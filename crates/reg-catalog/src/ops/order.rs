//! `order` and its download: a project's order manifest. A project the
//! supported-version decision or the structural validator rejects is
//! `project_invalid`, with validate's issues. Then, per binding in declaration
//! order, availability and representation slicing are [`resolve_binding`]'s, the
//! one pass `validate` reads too; a steward artifact matches each slice against its
//! compiled holdings and gates on full coverage, a catalog artifact serves each
//! slice by its canonical column (the global fallback). Any finding blocks the
//! whole order (`order_blocked`, every finding in accumulation order); there is
//! never a partial manifest.
//!
//! The manifest's bytes are `reg_core::project::to_json_pretty`'s, the encoding the
//! steward-side extractor reads; `order`'s `data` is the same document.

use std::collections::BTreeSet;

use reg_core::project::{Binding, ProjectData, Source, project_hash, to_json_pretty};
use reg_core::{Fqid, Interval, ValidationResult, gaps, intersect, merge, quote, render};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

use super::refs;
use super::states::variant_id;
use super::validate::{self, Finding, held_representations, resolve_binding};
use super::{Params, Raw, Server};
use crate::{Catalog, Code, Error, Scope};

/// The order manifest contract version.
const VERSION: u8 = 1;

/// The order manifest: machine-written here, machine-read by the steward-side
/// extract system, never hand-edited.
#[derive(Serialize, ToSchema)]
#[schema(as = OrderManifest)]
pub struct Manifest {
    /// The manifest contract version, 1.
    version: u8,
    provenance: Provenance,
    /// One per resolved logical-to-physical binding, per source and binding in
    /// declaration order, then by table, edition and column.
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
    /// The availability-clipped period this table serves.
    requested_period: String,
    physical: Physical,
}

/// The project-side coordinate of an entry.
#[derive(Serialize, ToSchema)]
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
#[derive(Serialize, ToSchema)]
#[schema(as = PhysicalCoordinate)]
struct Physical {
    /// The table's edition, rendered as a period.
    edition: String,
    table: String,
    column: String,
    /// The table's disjoint-partition label; absent when it has none.
    #[serde(skip_serializing_if = "Option::is_none")]
    partition: Option<String>,
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

/// The manifest download: [`order`]'s document as the exact bytes, an attachment.
pub fn manifest(server: &Server, _scope: Scope, params: &Params) -> Result<Raw, Error> {
    Ok(Raw {
        bytes: to_json_pretty(&materialize(server, params)?).into_bytes(),
        headers: vec![(
            "content-disposition",
            "attachment; filename=\"order.json\"".to_owned(),
        )],
    })
}

fn materialize(server: &Server, params: &Params) -> Result<Manifest, Error> {
    let project = validate::project(params)?.map_err(invalid)?;
    let catalog = &server.catalog;
    // Provenance is checked before anything resolves; no deployment rewrites it.
    let steward = catalog.name();
    if project.steward.as_str() != steward {
        let wanted = quote(project.steward.as_str());
        return Err(blocked(vec![Blocking::whole(
            "steward_mismatch",
            format!(
                "this project's steward is {wanted} and this deployment serves the {} \
                 steward; order the project from the deployment that serves {wanted}. Its \
                 steward is provenance — no deployment rewrites it to match its own",
                quote(steward)
            ),
        )]));
    }
    if project.sources.iter().all(|s| s.bindings.is_empty()) {
        return Err(blocked(vec![Blocking::whole(
            "project_empty",
            "project binds no variables — an empty project is a valid editable draft but \
             cannot produce a header-only manifest"
                .into(),
        )]));
    }
    let conn = catalog.connect()?;
    let mut order = Order {
        conn: &conn,
        steward: catalog.is_steward(),
        entries: Vec::new(),
        clips: Vec::new(),
        findings: Vec::new(),
    };
    for source in &project.sources {
        order.source(source)?;
    }
    if !order.findings.is_empty() {
        return Err(blocked(order.findings));
    }
    Ok(Manifest {
        version: VERSION,
        provenance: provenance(catalog, &project),
        entries: order.entries,
        clips: order.clips,
    })
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

/// A binding's resolved identity: the variable and the source's variant of its
/// register, when the source names one.
struct Ids {
    variable: i64,
    variant: Option<i64>,
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
        for binding in &source.bindings {
            self.binding(source, binding, requested.as_deref())?;
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

    /// The binding's variable and variant, or its `variable_unresolved` finding, which
    /// names the period `_default` for a year-independent request (`requested` none)
    /// and no period for a dated one, as today's.
    fn ids(
        &mut self,
        source: &Source,
        binding: &Binding,
        requested: Option<&[Interval]>,
    ) -> Result<Option<Ids>, Error> {
        let detail = match binding.variable.parse() {
            Ok(Fqid::Variable {
                provider,
                register,
                variable,
            }) => {
                let slugs = [provider, register, variable];
                if let Some(id) = refs::variable_id(self.conn, Scope::Reference, &slugs)? {
                    let slug = source.register_variant.rsplit('/').next().expect("a slug");
                    return Ok(Some(Ids {
                        variable: id,
                        variant: variant_id(self.conn, id, slug)?,
                    }));
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
    ) -> Result<(), Error> {
        let Some(ids) = self.ids(source, binding, requested)? else {
            return Ok(());
        };
        let pin = binding.representation.as_deref();
        let resolution = resolve_binding(
            self.conn,
            source,
            binding,
            pin,
            ids.variable,
            ids.variant,
            requested,
        )?;
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
        if let (true, Some(requested), Some(variant)) = (self.steward, requested, ids.variant)
            && self.windows_block(
                source,
                binding,
                &resolution,
                ids.variable,
                variant,
                requested,
            )?
        {
            return Ok(());
        }
        if resolution.finding.is_some() {
            return Ok(());
        }
        // An unblocked resolution reached states, so the variant exists.
        let variant = ids.variant.expect("a resolved binding has a variant");
        let ids = (ids.variable, variant);
        match requested {
            None => self.year_independent(source, binding, &resolution, ids),
            Some(_) => self.dated(source, binding, &resolution.slices, ids),
        }
    }

    /// On a steward artifact, each held physical edition the request reaches must
    /// overlap a state window the binding is applicable in, for each representation
    /// its column maps; every one that does not is a `column_window_unavailable`
    /// finding, even where the availability clip removes it from the request.
    fn windows_block(
        &mut self,
        source: &Source,
        binding: &Binding,
        resolution: &validate::Resolution,
        id: i64,
        variant: i64,
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
                Some(variant),
                Some(requested),
            )?;
            &unpinned
        } else {
            resolution
        };
        let pinned = pin
            .map(|pin| canonical(self.conn, id, variant, pin))
            .transpose()?;
        let held = held_representations(self.conn, id, variant)?;
        let mut blocked = false;
        for m in matches(self.conn, id, variant, pinned.as_deref(), "intervals", None)? {
            let physical: Vec<Interval> = m
                .periods
                .iter()
                .flat_map(|b| requested.iter().filter_map(|r| intersect(b, r)))
                .collect();
            if physical.is_empty() {
                continue;
            }
            let mapped = representations(self.conn, m.table_id, &m.column, id, variant)?;
            for column in mapped
                .iter()
                .filter(|c| held.contains(*c) && pinned.as_ref().is_none_or(|p| p == *c))
            {
                let windows = applicability.states.iter().filter_map(|s| {
                    let e = &s.emitted;
                    match (&e.canonical_column, &e.valid_from, &e.valid_to) {
                        (Some(c), Some(from), Some(to))
                            if c == column && e.period_scope == "intervals" =>
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
        let located: Vec<(String, String, Option<String>)> = if self.steward {
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
            .map(|m| (m.table, m.column, m.partition))
            .collect()
        } else {
            vec![(String::new(), column.clone(), None)]
        };
        if located.is_empty() {
            let message = "no year-independent steward table maps this delivery column";
            let period = Some("_default".to_owned());
            self.finding(source, binding, "mapping_missing", message.into(), period);
            return Ok(());
        }
        for (table, physical, partition) in located {
            self.entries.push(Entry {
                source: source.name.clone(),
                logical: logical(source, binding, &column),
                requested_period: "_default".into(),
                physical: Physical {
                    edition: "_default".into(),
                    table,
                    column: physical,
                    partition,
                },
            });
        }
        Ok(())
    }

    /// A dated request: each slice matched against the steward's holdings (or served
    /// by its canonical column in the global fallback), gated on full coverage.
    fn dated(
        &mut self,
        source: &Source,
        binding: &Binding,
        slices: &[(String, String, String)],
        (id, variant): (i64, i64),
    ) -> Result<(), Error> {
        let variable = quote(&binding.variable);
        let at = &source.register_variant;
        // Per (table, physical column, representation), in first-contribution order:
        // its edition, partition and the requested days it serves.
        let mut contributions: Vec<Contribution> = Vec::new();
        let mut blocked = false;
        let held = if self.steward {
            held_representations(self.conn, id, variant)?
        } else {
            BTreeSet::new()
        };
        for (lo, hi, column) in slices {
            let slice = (lo.clone(), hi.clone());
            let mut covered = Vec::new();
            if self.steward {
                let canonical = canonical(self.conn, id, variant, column)?;
                if !held.contains(&canonical) {
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
                        .periods
                        .iter()
                        .filter_map(|b| intersect(b, &slice))
                        .collect();
                    if overlaps.is_empty() {
                        continue;
                    }
                    covered.extend(overlaps.iter().cloned());
                    let key = (m.table, m.column, column.clone());
                    contribute(&mut contributions, key, overlaps, m.periods, m.partition);
                }
            } else {
                // The global fallback: canonical resolution is the topology, and a
                // slice covers itself.
                covered.push(slice.clone());
                let key = (String::new(), column.clone(), column.clone());
                contribute(
                    &mut contributions,
                    key,
                    vec![slice.clone()],
                    Vec::new(),
                    None,
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
        // Stable: ties keep first-contribution order, as today's sort does.
        contributions.sort_by(|a, b| {
            (&a.key.0, &a.edition, &a.key.1).cmp(&(&b.key.0, &b.edition, &b.key.1))
        });
        for c in contributions {
            let (table, physical, column) = c.key;
            self.entries.push(Entry {
                source: source.name.clone(),
                logical: logical(source, binding, &column),
                requested_period: render(&merge(c.days)),
                physical: Physical {
                    edition: render(&c.edition),
                    table,
                    column: physical,
                    partition: c.partition,
                },
            });
        }
        Ok(())
    }
}

/// One entry in the making: `(table, physical column, representation)`, the table's
/// edition and partition, and the requested days it serves.
struct Contribution {
    key: (String, String, String),
    edition: Vec<Interval>,
    partition: Option<String>,
    days: Vec<Interval>,
}

fn contribute(
    contributions: &mut Vec<Contribution>,
    key: (String, String, String),
    days: Vec<Interval>,
    edition: Vec<Interval>,
    partition: Option<String>,
) {
    match contributions.iter_mut().find(|c| c.key == key) {
        Some(c) => {
            c.days.extend(days);
            c.edition = edition;
            c.partition = partition;
        }
        None => contributions.push(Contribution {
            key,
            edition,
            partition,
            days,
        }),
    }
}

fn logical(source: &Source, binding: &Binding, representation: &str) -> Logical {
    let mut parts = source.register_variant.split('/').map(str::to_owned);
    let mut next = || parts.next().expect("a 3-part register variant");
    Logical {
        provider: next(),
        register: next(),
        variant: next(),
        variable: binding.variable.clone(),
        representation: representation.to_owned(),
    }
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
/// identity, partition, scope source and edition periods.
struct Match {
    table_id: i64,
    table: String,
    column: String,
    partition: Option<String>,
    source_ref: String,
    periods: Vec<Interval>,
}

/// The held locations of the variable under the variant in tables of `scope`
/// (today's `Holdings.matches`), mapped as `representation` when given and in a
/// table with a period overlapping `bounds` when given; ordered by table and column.
fn matches(
    conn: &Connection,
    id: i64,
    variant: i64,
    representation: Option<&str>,
    scope: &str,
    bounds: Option<(&str, &str)>,
) -> Result<Vec<Match>, Error> {
    let (lo, hi) = bounds.unzip();
    let mut stmt = conn.prepare_cached(
        "SELECT DISTINCT ht.table_id, ht.physical_id, hc.name, ht.partition, ht.source_ref, \
         hp.lo, hp.hi \
         FROM holding_mapping hm JOIN holding_column hc USING(column_id) \
         JOIN holding_table ht USING(table_id) LEFT JOIN holding_period hp USING(table_id) \
         WHERE hm.variable_id = ?1 AND hm.variant_id = ?2 AND ht.scope = ?3 \
         AND (?4 IS NULL OR hm.representation_canonical = ?4) \
         AND (?5 IS NULL OR EXISTS (SELECT 1 FROM holding_period p \
              WHERE p.table_id = ht.table_id AND p.lo <= ?6 AND p.hi >= ?5)) \
         ORDER BY ht.physical_id, hc.name, hp.lo, hp.hi",
    )?;
    let rows = stmt.query_map(params![id, variant, scope, representation, lo, hi], |row| {
        let period: (Option<String>, Option<String>) = (row.get(5)?, row.get(6)?);
        Ok((
            Match {
                table_id: row.get(0)?,
                table: row.get(1)?,
                column: row.get(2)?,
                partition: row.get(3)?,
                source_ref: row.get(4)?,
                periods: Vec::new(),
            },
            period,
        ))
    })?;
    let mut out: Vec<Match> = Vec::new();
    for row in rows {
        let (m, period) = row?;
        let index = out
            .iter()
            .position(|o| (o.table_id, &o.column) == (m.table_id, &m.column))
            .unwrap_or_else(|| {
                out.push(m);
                out.len() - 1
            });
        if let (Some(lo), Some(hi)) = period {
            out[index].periods.push((lo, hi));
        }
    }
    Ok(out)
}

/// The representations a held table column maps the variable under the variant to.
fn representations(
    conn: &Connection,
    table_id: i64,
    column: &str,
    id: i64,
    variant: i64,
) -> Result<BTreeSet<String>, Error> {
    Ok(conn
        .prepare_cached(
            "SELECT hm.representation_canonical FROM holding_mapping hm \
             JOIN holding_column hc USING(column_id) \
             WHERE hc.table_id = ? AND hc.name = ? AND hm.variable_id = ? AND hm.variant_id = ?",
        )?
        .query_map(params![table_id, column, id, variant], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?)
}
