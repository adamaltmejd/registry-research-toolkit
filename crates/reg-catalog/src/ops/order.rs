//! `order` and its download: a project's order manifest. A project the
//! supported-version decision or the structural validator rejects is
//! `project_invalid`, with validate's issues. Then, per binding in declaration
//! order, availability and representation slicing are [`resolve_binding`]'s, the
//! one pass `validate` reads too; a steward artifact matches each slice against its
//! compiled holdings and gates on full coverage, a catalog artifact serves each
//! slice by its canonical column (the global fallback). Any finding blocks the
//! whole order (`order_blocked`, every finding in accumulation order); there is
//! never a partial manifest. On a steward artifact each output file then gains the
//! held row keys it lacks ([`held_keys`]), and every entry of a file whose request
//! is narrower than its table's held period the table's period-column row filter
//! ([`row_filters`]).
//!
//! The manifest's bytes are `reg_core::project::to_json_pretty`'s, the encoding the
//! steward-side extractor reads; `order`'s `data` is the same document.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;

use reg_core::project::{
    Binding, ExtractFormat, ProjectData, Source, project_hash, to_json_pretty,
};
use reg_core::{
    Fqid, Interval, ValidationResult, gaps, intersect, is_letter, merge, quote, render,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use utoipa::ToSchema;

use super::family::{VariantFamily, variant_family};
use super::refs;
use super::validate::{self, Finding, Slice, held_representations, resolve_binding};
use super::{Params, Raw, Server};
use crate::held::{no_column_of, one_representation, serves, serves_at_by};
use crate::{Catalog, Code, Error, Scope};

/// The order manifest contract version.
const VERSION: u8 = 3;

/// The order manifest: machine-written here, machine-read by the steward-side
/// extract system, never hand-edited.
#[derive(Serialize, ToSchema)]
#[schema(as = OrderManifest)]
pub struct Manifest {
    /// The manifest contract version, 3.
    version: u8,
    /// The file format the extract runner writes every file in: the project's
    /// `extract_format`, `dta` when it sets none.
    extract_format: ExtractFormat,
    provenance: Provenance,
    /// One per resolved logical-to-physical binding, per source and binding in
    /// declaration order, then by table, edition and column; then each file's
    /// held row keys no binding ordered (`key`), by table, variant, edition, member
    /// index and column.
    entries: Vec<Entry>,
    /// Every availability clip, also when nothing blocked.
    clips: Vec<Clip>,
}

/// Which project, against which catalog, for which deployment: the manifest is
/// self-contained offline.
#[derive(Serialize, ToSchema)]
#[schema(as = OrderProvenance)]
struct Provenance {
    /// `steward_holdings` (a steward's compiled holdings grounded the entries) or
    /// `global_fallback` (canonical resolution alone, blank `table`).
    mode: &'static str,
    steward: String,
    /// The project's `name`, never blank: a blank name blocks the order.
    project_name: String,
    project_schema_version: String,
    project_reg_meta_version: String,
    /// SHA-256 of the project's canonical JSON.
    project_hash: String,
    catalog_schema_version: String,
    catalog_generation_id: String,
    /// `catalog` or `steward`.
    artifact_kind: &'static str,
}

/// One resolved binding: what was asked for and what the steward delivers.
#[derive(Serialize, ToSchema)]
#[schema(as = OrderEntry)]
struct Entry {
    /// The project source's name.
    source: String,
    logical: Logical,
    /// The days this entry serves: the request clipped by availability, a column
    /// window and a year or month member. It does not choose the output file; the
    /// table's edition does.
    requested_period: String,
    physical: Physical,
    /// A held row key column of the output file that no binding ordered (its
    /// variant's panel entity or time key, [`IS_KEY`]; a linking id comes only when
    /// ordered), carried so every file joins on its row key; absent on an ordered entry.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    #[schema(required = false)]
    key: bool,
    /// The output file's row filter: its table's period column and the ranges its
    /// entries request, the same on every entry of the file; absent when the table
    /// declares no period column or the file requests its whole held period.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(required = false)]
    filter: Option<Filter>,
    /// A projection's key cell its parent also serves (#1634): kept only when
    /// the projection contributes a cell of its own to the same file.
    #[serde(skip)]
    shared_key: bool,
    /// The bound variant: the file's held keys are the keys of each variant its entries bind.
    #[serde(skip)]
    variant_id: i64,
    /// `requested_period`'s days, empty for `_default`.
    #[serde(skip)]
    days: Vec<Interval>,
}

/// The project-side coordinate of an entry.
#[derive(Clone, Serialize, ToSchema)]
#[schema(as = LogicalCoordinate)]
struct Logical {
    provider: String,
    register: String,
    variant: String,
    /// The binding's variable FQID.
    variable: String,
    /// The canonical delivery column the slice resolved to.
    representation: String,
}

/// The steward-side coordinate of an entry; in the global fallback `table` is
/// blank, `column` the canonical column and `edition` the requested period.
#[derive(Clone, Serialize, ToSchema)]
#[schema(as = PhysicalCoordinate)]
struct Physical {
    /// The output file's name without its extension, the same on every entry of the
    /// file: `<provider>_<register>[_<variant>][_<placement>][_<period>]` of the
    /// table's own variant (crates/DESIGN.md, "Extraction keeps delivery topology").
    file: String,
    /// The table's edition, rendered as a period: one file per table, whatever
    /// periods and variants its entries request.
    edition: String,
    table: String,
    column: String,
    /// The table's disjoint-partition label; absent when it has none.
    #[serde(skip_serializing_if = "Option::is_none")]
    partition: Option<String>,
    /// The table's projection token (#1634): the table is a declared projection
    /// of another table's delivery, extracted as its own file; absent otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    projection: Option<String>,
    /// The column's wide-pivot member index; absent for a plain column.
    #[serde(skip_serializing_if = "Option::is_none")]
    member: Option<Member>,
    /// The name a `dta` order writes the column under, with `column` as the
    /// variable's label, when `column` is not a Stata variable name: its first 25
    /// characters made a name, `_` and 6 hex digits of its SHA-256; absent otherwise,
    /// and on every `parquet` or `csv` order.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(required = false)]
    stata_name: Option<String>,
}

/// A file's row filter (crates/DESIGN.md, "Order manifest"): keep the rows whose
/// `column` value falls in any of `ranges`. A range's bounds are inclusive ISO dates;
/// the extract runner reads `column` per `encoding` (`date`, `year`, `yyyymm` or
/// `yyyymmdd`, as `holding_table.period_encoding`) and keeps a row whose value's
/// year, month or day lies inside a range.
#[derive(Clone, Serialize, ToSchema)]
#[schema(as = OrderRowFilter)]
struct Filter {
    column: String,
    encoding: String,
    /// The file's requested days, ascending and disjoint, joined with OR.
    ranges: Vec<FilterRange>,
}

/// One requested range of a row filter. A bound at the table's held edge cuts
/// nothing and is absent, so a range may be open-ended on either side.
#[derive(Clone, Serialize, ToSchema)]
#[schema(as = OrderFilterRange)]
struct FilterRange {
    /// The first requested day, `YYYY-MM-DD`; absent from the table's first held day.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(required = false)]
    from: Option<String>,
    /// The last requested day, `YYYY-MM-DD`; absent through the table's last held day.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(required = false)]
    to: Option<String>,
}

/// A wide-pivot member index (#1495): one of N physical columns that together
/// deliver one representation. A position member serves the table's whole edition;
/// a year or month member serves only that period, so it is ordered only when the
/// slice reaches it. Members order by index, so `DIA2` precedes `DIA10`.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
#[schema(as = PivotMember)]
enum Member {
    Position(i64),
    Year(i64),
    /// `YYYY-MM`.
    Month(String),
}

impl Member {
    /// From `holding_mapping`'s member columns; the builder writes a year as its
    /// whole calendar year and a month as its whole month.
    fn from_columns(position: Option<i64>, lo: Option<String>, hi: Option<String>) -> Option<Self> {
        if let Some(position) = position {
            return Some(Self::Position(position));
        }
        let (lo, hi) = (lo?, hi?);
        let year = &lo[..4];
        if lo.ends_with("-01-01") && hi == format!("{year}-12-31") {
            return Some(Self::Year(year.parse().expect("an ISO year")));
        }
        Some(Self::Month(lo[..7].to_owned()))
    }
}

/// An availability clip: the binding asked for `requested_period` and is ordered
/// for `ordered_period`, where it is available.
#[derive(Serialize, ToSchema)]
#[schema(as = ClipReport)]
struct Clip {
    source: String,
    variable: String,
    requested_period: String,
    ordered_period: String,
}

/// One blocking finding of `order_blocked`; `source`, `variable` and `period` say
/// what it is about, null for a whole-project finding.
#[derive(Serialize, ToSchema)]
#[schema(as = OrderBlocking)]
pub(super) struct Blocking {
    code: &'static str,
    message: String,
    #[schema(required = true)]
    source: Option<String>,
    #[schema(required = true)]
    variable: Option<String>,
    #[schema(required = true)]
    period: Option<String>,
}

pub fn order(server: &Server, _scope: Scope, params: &Params) -> Result<Value, Error> {
    Ok(serde_json::to_value(materialize(server, params)?).expect("Manifest serializes"))
}

/// The manifest download: [`order`]'s document as the exact bytes, an attachment
/// named `order_<slug>_<YYYY-MM-DD>.json` after the project and the server's UTC date.
pub fn manifest(server: &Server, _scope: Scope, params: &Params) -> Result<Raw, Error> {
    let manifest = materialize(server, params)?;
    let filename = download_filename(&manifest.provenance.project_name, utc_date());
    Ok(Raw {
        bytes: to_json_pretty(&manifest).into_bytes(),
        headers: vec![(
            "content-disposition",
            format!("attachment; filename=\"{filename}\""),
        )],
    })
}

/// The longest project slug in a download name.
const SLUG_MAX: usize = 60;

/// `order_<slug>_<date>.json`. The slug is the name through `fold_search` (case and
/// accents folded: å, ä, é to a, a, e), with each run of other characters as one `-`,
/// trimmed and capped at [`SLUG_MAX`]; a name with no ASCII letter or digit (a blank
/// one never reaches here) is `project`. It is ASCII, so the header needs no
/// `filename*`.
fn download_filename(name: &str, (year, month, day): (i64, u32, u32)) -> String {
    let mut slug = String::new();
    for c in reg_core::fold_search(name).chars() {
        if c.is_ascii_alphanumeric() {
            slug.push(c);
        } else if !slug.ends_with('-') {
            slug.push('-');
        }
    }
    slug.truncate(SLUG_MAX);
    let slug = slug.trim_matches('-');
    let slug = if slug.is_empty() { "project" } else { slug };
    format!("order_{slug}_{year:04}-{month:02}-{day:02}.json")
}

/// Today's UTC date, from the system clock (Hinnant's `civil_from_days`).
fn utc_date() -> (i64, u32, u32) {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let z = i64::try_from(secs / 86_400).expect("a day count") + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = u32::try_from(doy - (153 * mp + 2) / 5 + 1).expect("a day");
    let month = u32::try_from(if mp < 10 { mp + 3 } else { mp - 9 }).expect("a month");
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

fn materialize(server: &Server, params: &Params) -> Result<Manifest, Error> {
    let project = validate::project(params)?.map_err(invalid)?;
    let catalog = &server.catalog;
    // Provenance is checked before anything resolves; no deployment rewrites it.
    let steward = catalog.name();
    if project.steward != steward {
        return Err(blocked(vec![Blocking::whole(
            "steward_mismatch",
            validate::steward_mismatch(&project.steward, steward),
        )]));
    }
    // An unnamed project is a valid editable draft too, but the manifest's
    // `provenance.project_name` is never blank (the extract runner refuses a blank
    // one), so it is the first finding of the one pass, not a refusal alone.
    let mut findings = Vec::new();
    if project.name.trim().is_empty() {
        findings.push(Blocking::whole(
            "project_unnamed",
            "project has no name — set `name`: the order manifest carries it as \
             provenance.project_name, which must not be blank"
                .into(),
        ));
    }
    if project.sources.iter().all(|s| s.bindings.is_empty()) {
        findings.push(Blocking::whole(
            "project_empty",
            "project binds no variables — an empty project is a valid editable draft but \
             cannot produce a header-only manifest"
                .into(),
        ));
        return Err(blocked(findings));
    }
    let conn = catalog.connect()?;
    let mut order = Order {
        conn: &conn,
        steward: catalog.is_steward(),
        entries: Vec::new(),
        clips: Vec::new(),
        findings,
    };
    for source in &project.sources {
        order.source(source)?;
    }
    if !order.findings.is_empty() {
        return Err(blocked(order.findings));
    }
    // A projection's shared key cells ride along only with a cell of its own anywhere
    // in the same output file (the table's edition, under any variant, from any
    // source or period): a projection file whose ordered cells its parent all serves is not
    // emitted, so no file holds only keys its parent's file also holds.
    let own: BTreeSet<(String, String)> = order
        .entries
        .iter()
        .filter(|e| e.physical.projection.is_some() && !e.shared_key)
        .map(placement)
        .collect();
    order
        .entries
        .retain(|e| !e.shared_key || own.contains(&placement(e)));
    // Only a file that kept an ordered entry gains its keys, so no file holds keys
    // alone. The global fallback has no physical table to hold one.
    if order.steward {
        let keys = held_keys(&conn, &order.entries)?;
        order.entries.extend(keys);
        row_filters(&conn, &mut order.entries)?;
    }
    file_names(&conn, order.steward, &mut order.entries)?;
    let extract_format = project.extract_format();
    if extract_format == ExtractFormat::Dta && order.steward {
        stata_names(&mut order.entries);
    }
    Ok(Manifest {
        version: VERSION,
        extract_format,
        provenance: provenance(catalog, &project),
        entries: order.entries,
        clips: order.clips,
    })
}

/// The output file an entry lands in: its table, for the table's edition
/// (crates/DESIGN.md, "Extraction keeps delivery topology"). `table` is the unique
/// `physical_id`, so its partition and projection token are facts of it; neither the
/// entry's variant nor its clipped `requested_period` splits the file. Only a steward
/// artifact groups by it: the global fallback has no table.
fn placement(entry: &Entry) -> (String, String) {
    (entry.physical.table.clone(), entry.physical.edition.clone())
}

/// Sets every entry's `physical.file` (crates/DESIGN.md, "Extraction keeps delivery
/// topology"): `<provider>_<register>_<variant>`, the variant left out when it is
/// `_default`, then the partition or projection token, then the edition when it is one
/// period (a year, month or date; a multi-period table's file holds every period). On a
/// steward artifact the coordinate is the table's own variant: the variant that maps
/// the most of the table's columns, ties to the first by provider, register and
/// variant slug, so a table several variants share has one name whatever the order
/// asks for. A name more than one table of the catalog would take gains the table's
/// `physical_id` in slug spelling, so the name stays a fact of the table, never of the
/// order. In the global fallback the coordinate is the entry's own.
fn file_names(conn: &Connection, steward: bool, entries: &mut [Entry]) -> Result<(), Error> {
    let tables = if steward {
        steward_file_names(conn)?
    } else {
        BTreeMap::new()
    };
    for e in entries.iter_mut() {
        let p = &e.physical;
        e.physical.file = if steward {
            tables[&p.table].clone()
        } else {
            let l = &e.logical;
            let one_period = !["..", ","].iter().any(|s| p.edition.contains(s));
            file_stem(
                &[&l.provider, &l.register, &l.variant],
                p.partition.as_deref().or(p.projection.as_deref()),
                one_period.then_some(p.edition.as_str()),
            )
        };
    }
    Ok(())
}

/// Every steward table's file name, by `physical_id` ([`file_names`]).
fn steward_file_names(conn: &Connection) -> Result<BTreeMap<String, String>, Error> {
    let mut stmt = conn.prepare_cached(
        "WITH counts AS (SELECT hc.table_id, hm.variant_id, \
         COUNT(DISTINCT hm.column_id) AS n FROM holding_mapping hm \
         JOIN holding_column hc USING(column_id) GROUP BY hc.table_id, hm.variant_id), \
         ranked AS (SELECT c.table_id, p.slug AS provider, r.slug AS register, \
         rv.slug AS variant, ROW_NUMBER() OVER (PARTITION BY c.table_id \
         ORDER BY c.n DESC, p.slug, r.slug, rv.slug) AS rank FROM counts c \
         JOIN register_variant rv ON rv.register_variant_id = c.variant_id \
         JOIN register r ON r.register_id = rv.register_id \
         JOIN provider p ON p.provider_id = r.provider_id) \
         SELECT ht.physical_id, k.provider, k.register, k.variant, \
         coalesce(ht.partition, ht.projection), ht.edition_json FROM ranked k \
         JOIN holding_table ht USING(table_id) WHERE k.rank = 1",
    )?;
    let rows = stmt.query_map([], |row| {
        let edition: Option<String> = row.get(5)?;
        // One period: a year (a JSON integer) or a month or date token, not `_default`
        // and not a range or list. simplify: the token is named as authored, so a
        // half-year `2020-H1` (which `physical.edition` renders `VT2020`) keeps its
        // spelling; render it through the period grammar if a steward authors one.
        let period = edition
            .as_deref()
            .and_then(|e| serde_json::from_str::<Value>(e).ok())
            .and_then(|e| match e {
                Value::Number(n) => Some(n.to_string()),
                Value::String(s) if s != "_default" => Some(s),
                _ => None,
            });
        let coordinate: [String; 3] = [row.get(1)?, row.get(2)?, row.get(3)?];
        let placement: Option<String> = row.get(4)?;
        let stem = file_stem(
            &[&coordinate[0], &coordinate[1], &coordinate[2]],
            placement.as_deref(),
            period.as_deref(),
        );
        Ok((row.get::<_, String>(0)?, stem))
    })?;
    let stems = rows.collect::<Result<BTreeMap<_, _>, _>>()?;
    // Case-insensitive, as the runner's clash check: MONA writes to a Windows file system.
    let mut uses: BTreeMap<String, usize> = BTreeMap::new();
    for stem in stems.values() {
        *uses.entry(stem.to_lowercase()).or_default() += 1;
    }
    Ok(stems
        .into_iter()
        .map(|(table, stem)| {
            let name = if uses[&stem.to_lowercase()] > 1 {
                format!("{stem}_{}", slug(&table))
            } else {
                stem
            };
            (table, name)
        })
        .collect())
}

/// `<provider>_<register>[_<variant>][_<placement>][_<period>]`, `_default` left out.
fn file_stem(coordinate: &[&str; 3], placement: Option<&str>, period: Option<&str>) -> String {
    let [provider, register, variant] = *coordinate;
    [Some(provider), Some(register)]
        .into_iter()
        .chain([(variant != "_default").then_some(variant), placement])
        .chain([period.filter(|p| *p != "_default")])
        .flatten()
        .collect::<Vec<_>>()
        .join("_")
}

/// A `physical_id` in slug spelling: lowercase, each run of other characters one `-`.
fn slug(physical_id: &str) -> String {
    physical_id
        .to_lowercase()
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

/// The held row keys each output file lacks (crates/DESIGN.md, "Order manifest"):
/// every column of the file's table that maps a key of a variant the file's entries
/// bind ([`IS_KEY`]) and that no entry of the file already extracts, whatever variable
/// ordered it. A key entry takes the source of its variant's first entry in the file and
/// the union of the file's requested days, narrowed to a year or month member's or a column window's own
/// period (a member or window the union does not reach is not added); a
/// year-independent file's keys request `_default`. An auxiliary table's carried key
/// (`holding_key`, never a served mapping) comes along the same way. Ordered by file
/// (table, edition), then member index and column, after every ordered entry.
fn held_keys(conn: &Connection, entries: &[Entry]) -> Result<Vec<Entry>, Error> {
    let mut stmt = conn.prepare_cached(&format!(
        "SELECT hc.name, p.slug || '/' || r.slug || '/' || v.slug, \
         hm.representation_canonical, hm.member_position, hm.member_lo, hm.member_hi, \
         COALESCE(hm.member_lo, hm.window_lo), COALESCE(hm.member_hi, hm.window_hi) \
         FROM holding_mapping hm JOIN holding_column hc USING(column_id) \
         JOIN holding_table ht USING(table_id) \
         JOIN variable v ON v.variable_id = hm.variable_id \
         JOIN register r ON r.register_id = v.register_id \
         JOIN provider p ON p.provider_id = r.provider_id \
         WHERE ht.physical_id = ?1 AND hm.variant_id = ?2 AND {IS_KEY} \
         UNION ALL \
         SELECT hc.name, p.slug || '/' || r.slug || '/' || v.slug, \
         hm.representation_canonical, NULL, NULL, NULL, NULL, NULL \
         FROM holding_key hm JOIN holding_column hc USING(column_id) \
         JOIN holding_table ht USING(table_id) \
         JOIN variable v ON v.variable_id = hm.variable_id \
         JOIN register r ON r.register_id = v.register_id \
         JOIN provider p ON p.provider_id = r.provider_id \
         WHERE ht.physical_id = ?1 AND hm.variant_id = ?2 AND {IS_KEY} \
         ORDER BY 1, 2, 3"
    ))?;
    let mut keys = Vec::new();
    for file in files(entries).into_values() {
        let year_independent = file.variants[0].requested_period == "_default";
        let union = merge(file.days);
        let mut found: Vec<Entry> = Vec::new();
        for first in file.variants {
            let rows = stmt.query_map(params![first.physical.table, first.variant_id], |row| {
                let own: (Option<String>, Option<String>) = (row.get(6)?, row.get(7)?);
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    Member::from_columns(row.get(3)?, row.get(4)?, row.get(5)?),
                    own.0.zip(own.1),
                ))
            })?;
            for row in rows {
                let (column, variable, representation, member, own) = row?;
                if file.columns.contains(column.as_str())
                    || found.iter().any(|k| k.physical.column == column)
                {
                    continue;
                }
                let days = match (&own, year_independent) {
                    (_, true) => Vec::new(),
                    // simplify: no golden has an identifier pivot member or windowed key
                    // column; add an `order/` case when a steward holds one.
                    (Some(own), false) => union.iter().filter_map(|d| intersect(d, own)).collect(),
                    (None, false) => union.clone(),
                };
                if !year_independent && days.is_empty() {
                    continue;
                }
                found.push(Entry {
                    source: first.source.clone(),
                    logical: Logical {
                        variable,
                        representation,
                        ..first.logical.clone()
                    },
                    requested_period: if year_independent {
                        "_default".into()
                    } else {
                        render(&days)
                    },
                    physical: Physical {
                        column,
                        member,
                        ..first.physical.clone()
                    },
                    key: true,
                    filter: None,
                    shared_key: false,
                    variant_id: first.variant_id,
                    days,
                });
            }
        }
        found.sort_by(|a, b| {
            (&a.physical.member, &a.physical.column).cmp(&(&b.physical.member, &b.physical.column))
        });
        keys.extend(found);
    }
    Ok(keys)
}

/// An output file: the first entry of each variant it holds, in entry order, its
/// physical columns and every day its entries request.
struct File<'a> {
    variants: Vec<&'a Entry>,
    columns: BTreeSet<&'a str>,
    days: Vec<Interval>,
}

/// The entries grouped by output file ([`placement`]), in file order.
fn files(entries: &[Entry]) -> BTreeMap<(String, String), File<'_>> {
    let mut files: BTreeMap<(String, String), File> = BTreeMap::new();
    for e in entries {
        let file = files.entry(placement(e)).or_insert_with(|| File {
            variants: Vec::new(),
            columns: BTreeSet::new(),
            days: Vec::new(),
        });
        if !file.variants.iter().any(|v| v.variant_id == e.variant_id) {
            file.variants.push(e);
        }
        file.columns.insert(&e.physical.column);
        file.days.extend(e.days.iter().cloned());
    }
    files
}

/// Each file's row filter (crates/DESIGN.md, "Order manifest"), set on every entry of
/// the file, held keys included: when its table declares a period column and the union
/// of its entries' requested days leaves part of the table's held period out, the
/// union's ranges, each without a bound at the table's held edge. A year-independent
/// file requests no days and is never filtered.
fn row_filters(conn: &Connection, entries: &mut [Entry]) -> Result<(), Error> {
    let mut stmt = conn.prepare_cached(
        "SELECT ht.period_column, ht.period_encoding, hp.lo, hp.hi \
         FROM holding_table ht JOIN holding_period hp USING(table_id) \
         WHERE ht.physical_id = ? AND ht.period_column IS NOT NULL",
    )?;
    let mut filters = BTreeMap::new();
    for (file, File { days, .. }) in files(entries) {
        if days.is_empty() {
            continue;
        }
        let mut column = None;
        let mut held = Vec::new();
        for row in stmt.query_map(params![file.0], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                (row.get(2)?, row.get(3)?),
            ))
        })? {
            let (name, encoding, period) = row?;
            column = Some((name, encoding));
            held.push(period);
        }
        let Some((column, encoding)) = column else {
            continue;
        };
        let held = merge(held);
        let union = merge(days);
        if gaps(&held, union.clone()).is_empty() {
            continue;
        }
        let first = &held.first().expect("a held period").0;
        let last = &held.last().expect("a held period").1;
        let ranges = union
            .into_iter()
            .map(|(lo, hi)| FilterRange {
                from: (lo > *first).then_some(lo),
                to: (hi < *last).then_some(hi),
            })
            .collect();
        filters.insert(
            file,
            Filter {
                column,
                encoding,
                ranges,
            },
        );
    }
    for e in entries.iter_mut() {
        e.filter = filters.get(&placement(e)).cloned();
    }
    Ok(())
}

/// Sets `physical.stata_name` on every entry whose column is not a Stata variable
/// name, held keys included, so a `dta` order writes it under [`stata_alias`].
fn stata_names(entries: &mut [Entry]) {
    for e in entries.iter_mut() {
        if !is_stata_name(&e.physical.column) {
            e.physical.stata_name = Some(stata_alias(&e.physical.column));
        }
    }
}

/// The Stata variable name a `.dta` file holds a column under when its own name is
/// not one (crates/DESIGN.md, "Order manifest"): the name's first 25 characters, each
/// that is not a letter, ASCII digit or `_` made `_` (and `_` put first when it would
/// start with a digit or `str` and a digit), then `_` and the first 6 hex digits of
/// the SHA-256 of the name's UTF-8 bytes. A function of the name alone, so a column
/// takes one alias in every table, order and catalog generation, and two names that
/// share their first 25 characters differ in the hash.
fn stata_alias(name: &str) -> String {
    let mut alias: String = name
        .chars()
        .map(|c| {
            if is_letter(c) || c.is_ascii_digit() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let str_digit = alias
        .strip_prefix("str")
        .is_some_and(|rest| rest.starts_with(|c: char| c.is_ascii_digit()));
    if str_digit || alias.starts_with(|c: char| c.is_ascii_digit()) {
        alias.insert(0, '_');
    }
    let mut alias: String = alias.chars().take(25).collect();
    alias.push('_');
    for byte in &Sha256::digest(name.as_bytes())[..3] {
        let _ = write!(alias, "{byte:02x}");
    }
    debug_assert!(is_stata_name(&alias), "{alias} is a Stata name");
    alias
}

/// Whether `name` is a variable name a `.dta` file (format 118) can hold: Stata's rule
/// (stricter than `ReadStat`, which writes any non-ASCII name; keep it so, Stata reads
/// the file), which `stata_name_ok` in `reg_mona_runner/swecov_extract.R` repeats. 1 to
/// 32 characters; a letter (general category L*, PCRE `\p{L}`) or `_` first, then letters, ASCII digits and `_`; not one of
/// Stata's reserved words, nor `str` followed by a digit.
fn is_stata_name(name: &str) -> bool {
    const RESERVED: [&str; 20] = [
        "_all", "_b", "byte", "_coef", "_cons", "double", "float", "if", "in", "int", "long", "_n",
        "_N", "_pi", "_pred", "_rc", "_skip", "strL", "using", "with",
    ];
    let mut chars = name.chars();
    chars.next().is_some_and(|c| is_letter(c) || c == '_')
        && chars.all(|c| is_letter(c) || c.is_ascii_digit() || c == '_')
        && name.chars().count() <= 32
        && !RESERVED.contains(&name)
        && !name
            .strip_prefix("str")
            .is_some_and(|rest| rest.starts_with(|c: char| c.is_ascii_digit()))
}

/// `project_invalid`: validate's issues for a project rejected before the catalog.
fn invalid(rejected: ValidationResult) -> Error {
    let message = match rejected.issues.as_slice() {
        [issue] if issue.code == "unsupported_schema_version" => issue.message.clone(),
        issues => format!(
            "cannot materialize an order for a structurally invalid project: {}",
            issues
                .iter()
                .filter(|i| i.level == reg_core::IssueLevel::Error)
                .map(|i| format!("{}@{}", i.code, i.path))
                .collect::<Vec<_>>()
                .join("; ")
        ),
    };
    let mut validation = serde_json::to_value(rejected).expect("serializes");
    Error::new(
        Code::ProjectInvalid,
        message,
        vec![validation["issues"].take()],
    )
}

/// `order_blocked`: every finding, and one line naming each (today's
/// `blocked_message`), read inside an error envelope and a banner alike.
fn blocked(findings: Vec<Blocking>) -> Error {
    let count = findings.len();
    let listed = findings
        .iter()
        .map(|f| {
            let parts: Vec<&str> = [&f.source, &f.variable, &f.period]
                .into_iter()
                .flatten()
                .map(String::as_str)
                .collect();
            let locator = if parts.is_empty() {
                String::new()
            } else {
                format!("[{}] ", parts.join(" "))
            };
            format!("{}: {locator}{}", f.code, f.message)
        })
        .collect::<Vec<_>>()
        .join("; ");
    let plural = if count == 1 { "" } else { "s" };
    let message = format!("order blocked by {count} finding{plural}: {listed}");
    let findings = serde_json::to_value(findings).expect("findings serialize");
    Error::new(Code::OrderBlocked, message, vec![findings])
}

impl Blocking {
    fn whole(code: &'static str, message: String) -> Self {
        Self {
            code,
            message,
            source: None,
            variable: None,
            period: None,
        }
    }
}

fn provenance(catalog: &Catalog, project: &ProjectData) -> Provenance {
    let steward = catalog.is_steward();
    Provenance {
        mode: if steward {
            "steward_holdings"
        } else {
            "global_fallback"
        },
        steward: catalog.name().to_owned(),
        project_name: project.name.clone(),
        project_schema_version: project.schema_version.clone(),
        project_reg_meta_version: project.reg_meta_version.clone(),
        project_hash: project_hash(project),
        catalog_schema_version: catalog.manifest("schema_version").to_owned(),
        catalog_generation_id: catalog.generation().to_owned(),
        artifact_kind: if steward { "steward" } else { "catalog" },
    }
}

/// The materializer's accumulators over one project.
struct Order<'a> {
    conn: &'a Connection,
    steward: bool,
    entries: Vec<Entry>,
    clips: Vec<Clip>,
    findings: Vec<Blocking>,
}

impl Order<'_> {
    fn source(&mut self, source: &Source) -> Result<(), Error> {
        let requested = match source.period.intervals() {
            Ok(requested) => requested,
            Err(reason) => {
                self.findings.push(Blocking {
                    source: Some(source.name.clone()),
                    ..Blocking::whole(
                        "period_not_orderable",
                        format!(
                            "source {} has no orderable period: {reason}",
                            quote(&source.name)
                        ),
                    )
                });
                return Ok(());
            }
        };
        // The source period is the extraction scope and every column's request,
        // over the family of the source's variant; the availability clip narrows
        // each column to its own years inside it.
        let family = variant_family(self.conn, &source.register_variant)?;
        for binding in &source.bindings {
            self.binding(source, binding, requested.as_deref(), family.as_ref())?;
        }
        Ok(())
    }

    fn finding(
        &mut self,
        source: &Source,
        binding: &Binding,
        code: &'static str,
        message: String,
        period: Option<String>,
    ) {
        self.findings.push(Blocking {
            code,
            message,
            source: Some(source.name.clone()),
            variable: Some(binding.variable.clone()),
            period,
        });
    }

    /// The binding's variable, or its `variable_unresolved` finding, which names the
    /// period `_default` for a year-independent request (`requested` none) and no
    /// period for a dated one, as today's.
    fn variable(
        &mut self,
        source: &Source,
        binding: &Binding,
        requested: Option<&[Interval]>,
    ) -> Result<Option<i64>, Error> {
        let detail = match binding.variable.parse() {
            Ok(Fqid::Variable {
                provider,
                register,
                variable,
            }) => {
                let slugs = [provider, register, variable];
                if let Some(id) = refs::variable_id(self.conn, Scope::Reference, &slugs)? {
                    return Ok(Some(id));
                }
                // Today's message carries an empty detail here (the frozen reader
                // formats an error whose str() is empty): a Rust-only fix.
                format!("FQID does not resolve to any row: {}", binding.variable)
            }
            Ok(_) => "not a binding FQID".to_owned(),
            Err(err) => err.to_string(),
        };
        let message = format!(
            "column {} does not resolve against the catalog at {}: {detail}",
            quote(&binding.variable),
            source.register_variant
        );
        let period = requested.is_none().then(|| "_default".to_owned());
        self.finding(source, binding, "variable_unresolved", message, period);
        Ok(None)
    }

    fn binding(
        &mut self,
        source: &Source,
        binding: &Binding,
        requested: Option<&[Interval]>,
        family: Option<&VariantFamily>,
    ) -> Result<(), Error> {
        let Some(id) = self.variable(source, binding, requested)? else {
            return Ok(());
        };
        let pin = binding.representation.as_deref();
        let resolution = resolve_binding(self.conn, source, binding, pin, id, family, requested)?;
        if let Some(ordered) = &resolution.clip {
            self.clips.push(Clip {
                source: source.name.clone(),
                variable: binding.variable.clone(),
                requested_period: resolution.requested_period.clone(),
                ordered_period: ordered.clone(),
            });
        }
        if let Some(Finding {
            code,
            message,
            period,
        }) = &resolution.finding
        {
            self.finding(source, binding, code, message.clone(), period.clone());
        }
        if let (true, Some(requested), Some(family)) = (self.steward, requested, family)
            && self.windows_block(source, binding, &resolution, id, family, requested)?
        {
            return Ok(());
        }
        if resolution.finding.is_some() {
            return Ok(());
        }
        // An unblocked resolution reached states, so the variant exists.
        let family = family.expect("a resolved binding has a variant");
        match requested {
            None => self.year_independent(source, binding, &resolution, (id, family.named)),
            Some(_) => self.dated(source, binding, &resolution.slices, id, family),
        }
    }

    /// On a steward artifact, each held physical edition a family member's share of
    /// the request reaches must overlap a state window of that member the binding
    /// is applicable in, for each representation its column maps; every one that
    /// does not is a `column_window_unavailable` finding, even where the
    /// availability clip removes it from the request.
    fn windows_block(
        &mut self,
        source: &Source,
        binding: &Binding,
        resolution: &validate::Resolution,
        id: i64,
        family: &VariantFamily,
        requested: &[Interval],
    ) -> Result<bool, Error> {
        let pin = binding.representation.as_deref();
        // A pin can clip away a differently spelled state sharing the physical
        // mapping, so applicability reads the unpinned resolution.
        let unpinned;
        let applicability = if pin.is_some() {
            unpinned = resolve_binding(
                self.conn,
                source,
                binding,
                None,
                id,
                Some(family),
                Some(requested),
            )?;
            &unpinned
        } else {
            resolution
        };
        let mut blocked = false;
        for (variant, requested) in family.split(requested) {
            blocked |=
                self.member_windows_block(source, binding, applicability, id, variant, &requested)?;
        }
        Ok(blocked)
    }

    /// [`Self::windows_block`] at one member, over its share of the request.
    fn member_windows_block(
        &mut self,
        source: &Source,
        binding: &Binding,
        applicability: &validate::Resolution,
        id: i64,
        variant: i64,
        requested: &[Interval],
    ) -> Result<bool, Error> {
        let pin = binding.representation.as_deref();
        let pinned = pin
            .map(|pin| canonical(self.conn, id, variant, pin))
            .transpose()?;
        let held = held_representations(self.conn, id, variant)?;
        let mut blocked = false;
        for m in matches(self.conn, id, variant, pinned.as_deref(), "intervals", None)? {
            let physical: Vec<Interval> = m
                .served()
                .iter()
                .flat_map(|b| requested.iter().filter_map(|r| intersect(b, r)))
                .collect();
            if physical.is_empty() {
                continue;
            }
            let mapped = representations(self.conn, m.table_id, &m.column, id, variant)?;
            for (column, &one) in mapped.iter().filter(|(c, one)| {
                held.contains(*c) && pinned.as_ref().is_none_or(|p| p == *c || **one)
            }) {
                let windows = applicability.states.iter().filter_map(|s| {
                    let e = &s.emitted;
                    match (&e.canonical_column, &e.valid_from, &e.valid_to) {
                        (Some(c), Some(from), Some(to))
                            if (one || c == column)
                                && e.register_variant_id == variant
                                && e.period_scope == "intervals" =>
                        {
                            Some((from.clone(), to.clone()))
                        }
                        _ => None,
                    }
                });
                let applicable = windows
                    .into_iter()
                    .any(|w| physical.iter().any(|p| intersect(p, &w).is_some()));
                if !applicable {
                    blocked = true;
                    let message = format!(
                        "{}: table {} column {} has no applicable column window for \
                         representation {}",
                        m.source_ref,
                        quote(&m.table),
                        quote(&m.column),
                        quote(column)
                    );
                    let period = render(&merge(physical.clone()));
                    self.finding(
                        source,
                        binding,
                        "column_window_unavailable",
                        message,
                        Some(period),
                    );
                }
            }
        }
        Ok(blocked)
    }

    /// A `_default` request: the year-independent column, at a year-independent
    /// steward table (or as itself in the global fallback).
    fn year_independent(
        &mut self,
        source: &Source,
        binding: &Binding,
        resolution: &validate::Resolution,
        (id, variant): (i64, i64),
    ) -> Result<(), Error> {
        let column = resolution
            .states
            .iter()
            .filter_map(|s| s.emitted.delivery_column_name.clone())
            .min()
            .expect("an unblocked year-independent resolution has a column");
        // `_default` resolves the named variant exactly.
        let named = source.register_variant.rsplit('/').next().expect("a slug");
        let mut located: Vec<Located> = if self.steward {
            let canonical = canonical(self.conn, id, variant, &column)?;
            matches(
                self.conn,
                id,
                variant,
                Some(&canonical),
                "year_independent",
                None,
            )?
            .into_iter()
            .map(|m| Located {
                table: m.table,
                column: m.column,
                placement: m.placement,
            })
            .collect()
        } else {
            vec![Located {
                table: String::new(),
                column: column.clone(),
                placement: Placement::default(),
            }]
        };
        // Stable: plain columns keep their table-and-name order; members order by
        // index, not name.
        located
            .sort_by(|a, b| (&a.table, &a.placement.member).cmp(&(&b.table, &b.placement.member)));
        if located.is_empty() {
            let message = "no year-independent steward table maps this delivery column";
            let period = Some("_default".to_owned());
            self.finding(source, binding, "mapping_missing", message.into(), period);
            return Ok(());
        }
        for Located {
            table,
            column: physical,
            placement,
        } in located
        {
            self.entries.push(Entry {
                source: source.name.clone(),
                logical: logical(source, binding, named, &column),
                requested_period: "_default".into(),
                physical: Physical {
                    file: String::new(),
                    edition: "_default".into(),
                    table,
                    column: physical,
                    partition: placement.partition,
                    projection: placement.projection,
                    member: placement.member,
                    stata_name: None,
                },
                key: false,
                filter: None,
                shared_key: placement.shared_key,
                variant_id: variant,
                days: Vec::new(),
            });
        }
        Ok(())
    }

    /// A dated request: each slice matched at its family member against the
    /// steward's holdings (or served by its canonical column in the global
    /// fallback), gated on full coverage.
    fn dated(
        &mut self,
        source: &Source,
        binding: &Binding,
        slices: &[Slice],
        id: i64,
        family: &VariantFamily,
    ) -> Result<(), Error> {
        let variable = quote(&binding.variable);
        // Per (table, physical column, representation, member), in
        // first-contribution order: its edition, partition and the requested days it
        // serves.
        let mut contributions: Vec<Contribution> = Vec::new();
        let mut blocked = false;
        let mut held: BTreeMap<i64, BTreeSet<String>> = BTreeMap::new();
        for s in slices {
            let (lo, hi, column, variant) = (&s.lo, &s.hi, &s.column, s.variant);
            let at = coordinate(source, family.slug(variant));
            let slice = s.days();
            let mut covered = Vec::new();
            if self.steward {
                let canonical = canonical(self.conn, id, variant, column)?;
                if !held_at(&mut held, self.conn, id, variant)?.contains(&canonical) {
                    blocked = true;
                    let message = format!(
                        "no steward table maps {at} {variable} representation {}; a missing \
                         logical-to-physical mapping blocks the order",
                        quote(column)
                    );
                    let period = Some(render(std::slice::from_ref(&slice)));
                    self.finding(source, binding, "mapping_missing", message, period);
                    continue;
                }
                let bounds = Some((lo.as_str(), hi.as_str()));
                for m in matches(
                    self.conn,
                    id,
                    variant,
                    Some(&canonical),
                    "intervals",
                    bounds,
                )? {
                    let overlaps: Vec<Interval> = m
                        .served()
                        .iter()
                        .filter_map(|b| intersect(b, &slice))
                        .collect();
                    if overlaps.is_empty() {
                        continue;
                    }
                    covered.extend(overlaps.iter().cloned());
                    let key = (m.table, m.column, column.clone(), variant);
                    contribute(&mut contributions, key, overlaps, (m.periods, m.placement));
                }
            } else {
                // The global fallback: canonical resolution is the topology, and a
                // slice covers itself.
                covered.push(slice.clone());
                contribute(
                    &mut contributions,
                    (String::new(), column.clone(), column.clone(), variant),
                    vec![slice.clone()],
                    Held::default(),
                );
            }
            for gap in gaps(std::slice::from_ref(&slice), covered) {
                blocked = true;
                let gap = render(std::slice::from_ref(&gap));
                let message = format!(
                    "no steward edition delivers {at} {variable} representation {} for {gap}; \
                     the whole order is blocked until every requested subperiod inside \
                     availability is covered",
                    quote(column)
                );
                self.finding(source, binding, "coverage_gap", message, Some(gap));
            }
        }
        if blocked {
            return Ok(());
        }
        if !self.steward {
            // A fallback entry's edition is its requested period.
            for c in &mut contributions {
                c.edition = merge(c.days.clone());
            }
        }
        // Stable: ties keep first-contribution order, as today's sort does. A
        // member orders by its index, not its column name.
        contributions.sort_by(|a, b| a.order().cmp(&b.order()));
        for c in contributions {
            let (table, physical, column, variant) = c.key;
            let days = merge(c.days);
            self.entries.push(Entry {
                source: source.name.clone(),
                logical: logical(source, binding, family.slug(variant), &column),
                requested_period: render(&days),
                physical: Physical {
                    file: String::new(),
                    edition: render(&c.edition),
                    table,
                    column: physical,
                    partition: c.placement.partition,
                    projection: c.placement.projection,
                    member: c.placement.member,
                    stata_name: None,
                },
                key: false,
                filter: None,
                shared_key: c.placement.shared_key,
                variant_id: variant,
                days,
            });
        }
        Ok(())
    }
}

/// One entry in the making: `(table, physical column, representation, member)`, the
/// table's edition, the column's placement, and the requested days it serves.
struct Contribution {
    key: (String, String, String, i64),
    edition: Vec<Interval>,
    placement: Placement,
    days: Vec<Interval>,
}

/// Where a held column lands in the extract beyond its table: the table's partition
/// or projection token, the column's member index, and whether it is a projection's
/// key cell its parent also serves.
#[derive(Default)]
struct Placement {
    partition: Option<String>,
    projection: Option<String>,
    member: Option<Member>,
    shared_key: bool,
}

/// A year-independent held column.
struct Located {
    table: String,
    column: String,
    placement: Placement,
}

impl Contribution {
    /// Table, edition, member index, then column: a member orders by its index, not
    /// its column name.
    fn order(&self) -> (&String, &Vec<Interval>, &Option<Member>, &String) {
        (
            &self.key.0,
            &self.edition,
            &self.placement.member,
            &self.key.1,
        )
    }
}

/// A held column's table edition and placement.
type Held = (Vec<Interval>, Placement);

fn contribute(
    contributions: &mut Vec<Contribution>,
    key: (String, String, String, i64),
    days: Vec<Interval>,
    (edition, placement): Held,
) {
    match contributions.iter_mut().find(|c| c.key == key) {
        Some(c) => {
            c.days.extend(days);
            c.edition = edition;
            c.placement = placement;
        }
        None => contributions.push(Contribution {
            key,
            edition,
            placement,
            days,
        }),
    }
}

/// The logical coordinate of an entry at family member `variant` of its source.
fn logical(source: &Source, binding: &Binding, variant: &str, representation: &str) -> Logical {
    let parts: Vec<&str> = source.register_variant.split('/').collect();
    let [provider, register, _] = parts[..] else {
        unreachable!("the structural validator admits only 3-part register variants")
    };
    Logical {
        provider: provider.to_owned(),
        register: register.to_owned(),
        variant: variant.to_owned(),
        variable: binding.variable.clone(),
        representation: representation.to_owned(),
    }
}

/// `<provider>/<register>/<variant>` of family member `variant` of the source.
fn coordinate(source: &Source, variant: &str) -> String {
    let (register, _) = source
        .register_variant
        .rsplit_once('/')
        .expect("a 3-part register variant");
    format!("{register}/{variant}")
}

/// The days of `slices` (a resolution's windows, each at its member) the steward
/// holds: what `Order::dated` serves rather than blocks as a `coverage_gap`
/// or `mapping_missing`. `validate` measures a source's delivered days by it.
pub(super) fn held_days(
    conn: &Connection,
    id: i64,
    slices: &[Slice],
) -> Result<Vec<Interval>, Error> {
    let mut held: BTreeMap<i64, BTreeSet<String>> = BTreeMap::new();
    let mut days = Vec::new();
    for slice in slices {
        let variant = slice.variant;
        let canonical = canonical(conn, id, variant, &slice.column)?;
        if !held_at(&mut held, conn, id, variant)?.contains(&canonical) {
            continue;
        }
        let (lo, hi) = (slice.lo.as_str(), slice.hi.as_str());
        let slice = slice.days();
        let bounds = Some((lo, hi));
        for m in matches(conn, id, variant, Some(&canonical), "intervals", bounds)? {
            days.extend(m.served().iter().filter_map(|b| intersect(b, &slice)));
        }
    }
    Ok(merge(days))
}

/// The held representations of variable `id` at `variant`, read once per member.
fn held_at<'a>(
    held: &'a mut BTreeMap<i64, BTreeSet<String>>,
    conn: &Connection,
    id: i64,
    variant: i64,
) -> Result<&'a BTreeSet<String>, Error> {
    if let std::collections::btree_map::Entry::Vacant(e) = held.entry(variant) {
        e.insert(held_representations(conn, id, variant)?);
    }
    Ok(&held[&variant])
}

/// `column`'s one spelling at the variable and variant (today's
/// `Catalog.canonical_delivery_column`): the canonical column of a state or window
/// of its `fold_identity` fold, else `column` itself.
fn canonical(conn: &Connection, id: i64, variant: i64, column: &str) -> Result<String, Error> {
    Ok(conn
        .prepare_cached(
            "SELECT canonical_column FROM expanded_state WHERE variable_id = ? \
             AND register_variant_id = ? AND fold_identity(canonical_column) = fold_identity(?) \
             LIMIT 1",
        )?
        .query_row(params![id, variant, column], |row| row.get(0))
        .optional()?
        .unwrap_or_else(|| column.to_owned()))
}

/// A held physical location of a binding: a table's column, with the table's
/// identity, partition, scope source and edition periods, the column's member
/// index, and the period a year or month member or a column window (#1645) serves.
struct Match {
    table_id: i64,
    table: String,
    column: String,
    source_ref: String,
    periods: Vec<Interval>,
    placement: Placement,
    own_period: Option<Interval>,
    /// Where the column serves a spelling through its one representation: the
    /// windows SCB delivered that mapped representation, which it does not serve.
    beside: Vec<Interval>,
}

impl Match {
    /// The periods the column serves: a year or month member's or a column window's
    /// own period (inside one edition segment, as the builder holds), else the
    /// table's edition, less where a one-representation fallback's own spelling was
    /// delivered (`inventory._fallbacks`: per instant, never over a whole slice).
    /// Nothing outside a window is served or borrowed.
    fn served(&self) -> Vec<Interval> {
        let served = self
            .own_period
            .clone()
            .map_or_else(|| self.periods.clone(), |period| vec![period]);
        if self.beside.is_empty() {
            return served;
        }
        gaps(&merge(served), self.beside.clone())
    }
}

/// Whether a mapping `hm` of table `ht` is one its declared parent also serves
/// (#1634): `ht` is a projection whose parent has a mapping of the same `(variable,
/// variant)` serving `hm`'s canonical representation over `hm`'s bounds
/// ([`serves_at_by`]: its own, or its one representation where SCB did not deliver
/// that beside it), at the same position, or neither at a position, over every
/// instant `hm` serves. A position member is served only by the parent's member of
/// that position, and a plain mapping, a year or month member or a windowed mapping
/// only by a parent mapping that is not a position member. A plain parent mapping
/// serves the whole shared edition; a year or month member or a column window (#1645)
/// serves its own period, so it covers a projection member or window inside it, or a
/// plain projection mapping whose edition lies inside it, and the parent serves
/// nothing outside it. This is the builder's `_parent_serves`, so the order drops
/// exactly the mappings the inventory admitted as served. Only the declared pair is
/// compared: an undeclared overlap is the builder's refusal, never deduplicated here.
fn parent_serves() -> String {
    const LO: &str = "COALESCE(hm.member_lo, hm.window_lo, \
         (SELECT MIN(p.lo) FROM holding_period p WHERE p.table_id = ht.table_id))";
    const HI: &str = "COALESCE(hm.member_hi, hm.window_hi, \
         (SELECT MAX(p.hi) FROM holding_period p WHERE p.table_id = ht.table_id))";
    format!(
        "EXISTS (SELECT 1 FROM holding_table pt \
         JOIN holding_column pc ON pc.table_id = pt.table_id \
         JOIN holding_mapping pm ON pm.column_id = pc.column_id \
         WHERE pt.physical_id = ht.projection_of AND pm.variable_id = hm.variable_id \
         AND pm.variant_id = hm.variant_id \
         AND {} \
         AND pm.member_position IS hm.member_position \
         AND (COALESCE(pm.member_lo, pm.window_lo) IS NULL OR (\
              COALESCE(pm.member_lo, pm.window_lo) <= {LO} \
              AND COALESCE(pm.member_hi, pm.window_hi) >= {HI})))",
        serves_at_by("pm", "pc", "hm.representation_canonical", LO, HI)
    )
}

/// Whether mapping `hm` binds a row key of its variant: the bound variant's
/// `panel_entity_key` or `panel_time_key`, each a bare slug or a JSON list of slugs
/// (decoded as `show`'s `panel_key` does). Any other identifier is a linking id,
/// carried only when ordered (maintainer ruling 2026-10-23); a variant that declares no
/// panel key has no row key here. This decides what [`held_keys`] auto-adds; [`kept`]
/// also keeps an ordered identifier ([`IS_IDENTIFIER`]).
// simplify: the `panel_time_key` sentinel `period` passes through as a slug, so a
// variable slugged `period` would count as a time key; exclude the sentinel if such a
// variable is ever bound as a panel key.
const IS_KEY: &str = "EXISTS (SELECT 1 FROM variable v JOIN register_variant rv \
     ON rv.register_variant_id = hm.variant_id WHERE v.variable_id = hm.variable_id \
     AND (v.slug IN (SELECT value FROM json_each(CASE WHEN rv.panel_entity_key LIKE '[%' \
              THEN rv.panel_entity_key ELSE json_array(rv.panel_entity_key) END)) \
          OR v.slug IN (SELECT value FROM json_each(CASE WHEN rv.panel_time_key LIKE '[%' \
              THEN rv.panel_time_key ELSE json_array(rv.panel_time_key) END))))";

/// Whether mapping `hm` binds an identifier variable: a row key or a linking id.
const IS_IDENTIFIER: &str = "EXISTS (SELECT 1 FROM variable v \
     WHERE v.variable_id = hm.variable_id AND v.is_identifier = 1)";

/// An ordered projection mapping its parent serves is dropped unless it is a row key
/// or an identifier: the parent alone serves a shared value cell, while a projection
/// may sit at another grain (EKB's household file beside its person file), so a
/// shared id emitted from the parent alone would leave the projection's rows
/// unjoinable on it. This only filters ordered matches; it adds nothing.
fn kept() -> String {
    format!("({IS_KEY} OR {IS_IDENTIFIER} OR NOT {})", parent_serves())
}

/// The held locations of the variable under the variant in tables of `scope`
/// (today's `Holdings.matches`), mapped as `representation` when given and in a
/// table with a period overlapping `bounds` when given, where a year or month
/// member's own period must overlap them too; ordered by table and column. A column
/// window is not pre-filtered: the caller clips `served()` to the slice.
// One Match per (table, column) carries the column's member or window: the builder
// refuses a member column with a second mapping (`inventory_member_invalid`) and a
// windowed (variant, variable) mapped twice in one column (`inventory_window_invalid`),
// so under one variable and variant it is unambiguous.
fn matches(
    conn: &Connection,
    id: i64,
    variant: i64,
    representation: Option<&str>,
    scope: &str,
    bounds: Option<(&str, &str)>,
) -> Result<Vec<Match>, Error> {
    let (lo, hi) = bounds.unzip();
    // Over a slice, a one-representation fallback is admitted here and clipped per
    // instant to where its own spelling is not delivered (`Match::beside`), as the
    // builder's `_fallbacks` places it; `held::serves_at` would judge the whole slice
    // and drop the years the inventory holds.
    let may_serve = format!(
        "(hm.representation_canonical = ?4 OR ({} AND {}))",
        one_representation("hm", "hc"),
        no_column_of("hc", "?4"),
    );
    let mut stmt = conn.prepare_cached(&format!(
        "SELECT DISTINCT ht.table_id, ht.physical_id, hc.name, ht.partition, ht.source_ref, \
         hp.lo, hp.hi, hm.member_position, hm.member_lo, hm.member_hi, ht.projection, \
         {parent_serves}, \
         COALESCE(hm.member_lo, hm.window_lo), COALESCE(hm.member_hi, hm.window_hi), \
         hm.representation_canonical \
         FROM holding_mapping hm JOIN holding_column hc USING(column_id) \
         JOIN holding_table ht USING(table_id) LEFT JOIN holding_period hp USING(table_id) \
         WHERE hm.variable_id = ?1 AND hm.variant_id = ?2 AND ht.scope = ?3 \
         AND (?4 IS NULL OR CASE WHEN ?5 IS NULL THEN {serves} ELSE {may_serve} END) \
         AND (?5 IS NULL OR EXISTS (SELECT 1 FROM holding_period p \
              WHERE p.table_id = ht.table_id AND p.lo <= ?6 AND p.hi >= ?5)) \
         AND (?5 IS NULL OR hm.member_lo IS NULL OR (hm.member_lo <= ?6 AND hm.member_hi >= ?5)) \
         AND {kept} \
         ORDER BY ht.physical_id, hc.name, hp.lo, hp.hi",
        kept = kept(),
        parent_serves = parent_serves(),
        serves = serves("?4"),
    ))?;
    let mut beside = conn.prepare_cached(
        "SELECT valid_from, valid_to FROM expanded_state WHERE variable_id = ? \
         AND register_variant_id = ? AND canonical_column = ? AND valid_from IS NOT NULL",
    )?;
    let rows = stmt.query_map(params![id, variant, scope, representation, lo, hi], |row| {
        let period: (Option<String>, Option<String>) = (row.get(5)?, row.get(6)?);
        let (own_lo, own_hi): (Option<String>, Option<String>) = (row.get(12)?, row.get(13)?);
        let mapped: String = row.get(14)?;
        Ok((
            Match {
                table_id: row.get(0)?,
                table: row.get(1)?,
                column: row.get(2)?,
                source_ref: row.get(4)?,
                periods: Vec::new(),
                placement: Placement {
                    partition: row.get(3)?,
                    projection: row.get(10)?,
                    member: Member::from_columns(row.get(7)?, row.get(8)?, row.get(9)?),
                    // A kept mapping its parent serves is a key.
                    shared_key: row.get(11)?,
                },
                own_period: own_lo.zip(own_hi),
                beside: Vec::new(),
            },
            period,
            mapped,
        ))
    })?;
    let mut out: Vec<Match> = Vec::new();
    for row in rows {
        let (mut m, period, mapped) = row?;
        let found = out
            .iter()
            .position(|o| (o.table_id, &o.column) == (m.table_id, &m.column));
        let index = if let Some(index) = found {
            index
        } else {
            if representation.is_some_and(|r| r != mapped) {
                m.beside = beside
                    .query_map(params![id, variant, mapped], |row| {
                        Ok((row.get(0)?, row.get(1)?))
                    })?
                    .collect::<rusqlite::Result<_>>()?;
            }
            out.push(m);
            out.len() - 1
        };
        if let (Some(lo), Some(hi)) = period {
            out[index].periods.push((lo, hi));
        }
    }
    Ok(out)
}

/// The representations a held table column maps the variable under the variant to,
/// each with whether it is its table's one representation, which may also serve
/// another spelling ([`one_representation`]).
fn representations(
    conn: &Connection,
    table_id: i64,
    column: &str,
    id: i64,
    variant: i64,
) -> Result<BTreeMap<String, bool>, Error> {
    Ok(conn
        .prepare_cached(&format!(
            "SELECT hm.representation_canonical, {one} FROM holding_mapping hm \
             JOIN holding_column hc USING(column_id) JOIN holding_table ht USING(table_id) \
             WHERE hc.table_id = ? AND hc.name = ? AND hm.variable_id = ? AND hm.variant_id = ? \
             AND {kept}",
            one = one_representation("hm", "hc"),
            kept = kept(),
        ))?
        .query_map(params![table_id, column, id, variant], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .collect::<rusqlite::Result<_>>()?)
}
