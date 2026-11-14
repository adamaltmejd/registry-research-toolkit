//! Link and emit: resolved registers into one SQLite file (`schema.sql`).
//! The emitter decides nothing. Row ids follow natural-key order, indexes come after
//! data, and the file is written with `VACUUM INTO`, so two builds from the same pins give the same bytes.
//!
//! It fills the identity, coverage, release, variable, segment, occurrence, gap,
//! value-set, relation, document and search tables (`search.rs`);
//! including curated winners and their statement evidence. Findings are not published:
//! they are a build output (`findings.rs`).

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use reg_frame::ValueSet;
use reg_resolve::Resolved;
use reg_resolve::provenance::{FinalValue, Origin, Subject as ProvenanceSubject};

use crate::docs::Document;
use crate::error::Result;
use crate::link::{self, Entity};
use rusqlite::Connection;
use rusqlite::types::{ToSqlOutput, ValueRef};

const SCHEMA: &str = include_str!("schema.sql");

/// One row of the input table.
pub struct InputRow {
    pub name: String,
    pub repo: String,
    pub commit: String,
    pub path: String,
    pub sha256: String,
    pub bytes: i64,
}

pub struct ProviderRow {
    pub pid: String,
    pub name: String,
    pub url: Option<String>,
}

pub struct StatementRow {
    pub hash: String,
    pub kind: String,
    pub file: String,
    pub line: usize,
    pub evidence: Option<String>,
    pub caveat: Option<String>,
}

impl StatementRow {
    #[must_use]
    pub fn for_locator<'a>(rows: &'a [Self], at: &reg_frame::Locator) -> Option<&'a Self> {
        rows.iter()
            .find(|s| s.file == at.input && format!("line={}", s.line) == at.at)
    }
}

/// Everything the file holds, linked.
pub struct Catalog {
    pub set_presentation: Vec<reg_resolve::curation::apply::SetPresentation>,
    pub external_relations: Vec<crate::external::Relation>,
    pub retired: Vec<crate::retired::Retired>,
    pub tags: BTreeMap<String, crate::sets::Tag>,
    pub meta: BTreeMap<&'static str, String>,
    /// Sorted by name.
    pub inputs: Vec<InputRow>,
    pub providers: Vec<ProviderRow>,
    pub registers: Vec<Resolved>,
    /// Value sets by hash, with the (input index, name) pairs that named them.
    pub valuesets: BTreeMap<String, (ValueSet, BTreeSet<(usize, String)>)>,
    pub sentinels: BTreeMap<String, BTreeSet<String>>,
    /// A frame file (locator input) → its input index.
    pub file_input: BTreeMap<String, usize>,
    /// The link stage's output (`link.rs`).
    pub relations: Vec<link::Relation>,
    pub notices: Vec<link::Notice>,
    pub breaks: Vec<link::Break>,
    /// Documents in hash order; a document's id is its position, from 1.
    pub docs: Vec<Document>,
    pub doc_links: Vec<link::DocLink>,
    pub statements: Vec<StatementRow>,
    pub provenance: reg_resolve::provenance::Finalized,
    pub classifications: Vec<reg_resolve::curation::apply::Classification>,
}

impl Catalog {
    /// A register's set's presentation statements. Label, description and axes may each
    /// come from its own statement; lock refuses two statements stating one field.
    #[must_use]
    pub fn set_presentation(
        &self,
        ri: usize,
        slug: &str,
    ) -> Vec<&reg_resolve::curation::apply::SetPresentation> {
        let r = &self.registers[ri].register;
        self.set_presentation
            .iter()
            .filter(|set| {
                set.provider == r.provider && set.register == r.register && set.slug == slug
            })
            .collect()
    }
}

/// A cell before interning: text, folded names and spellings are resolved to row ids
/// once every body is known, so their ids too follow sort order.
#[derive(Clone)]
enum Cell {
    Null,
    Int(i64),
    Str(String),
    Blob(Vec<u8>),
    Text(String),
    Fold(String),
    Spelling(String),
}

fn opt_text(s: Option<&String>) -> Cell {
    s.map_or(Cell::Null, |s| Cell::Text(s.clone()))
}

fn opt_int(v: Option<i64>) -> Cell {
    v.map_or(Cell::Null, Cell::Int)
}

fn opt_str(s: Option<&String>) -> Cell {
    s.map_or(Cell::Null, |s| Cell::Str(s.clone()))
}

/// The catalog's one name for a resolved dtype; unknown (`unspecified`) is NULL.
fn published_dtype(dtype: Option<&str>) -> Cell {
    dtype
        .and_then(|t| reg_frame::dtype_info(t).expect("validated dtype").published)
        .map_or(Cell::Null, |t| Cell::Str(t.to_owned()))
}

fn flag(b: Option<bool>) -> Cell {
    opt_int(b.map(i64::from))
}

/// The day number of an inclusive start.
pub(crate) fn start(iso: &str) -> i64 {
    reg_core::iso_day_number(iso).expect("resolved periods are ISO days")
}

/// The half-open end of an inclusive end: the day after.
pub(crate) fn end(iso: &str) -> i64 {
    start(iso) + 1
}

/// The half-open hull of a table's coverage spans.
pub(crate) fn hull(spans: &[reg_core::Interval]) -> (i64, i64) {
    spans.iter().fold((i64::MAX, i64::MIN), |(lo, hi), p| {
        (lo.min(start(&p.0)), hi.max(end(&p.1)))
    })
}

pub(crate) fn pid_of_register(r: &Resolved) -> String {
    format!("{}/{}", r.register.provider, r.public)
}

pub(crate) fn pid_of_table(r: &Resolved, t: usize) -> String {
    let base = pid_of_register(r);
    match &r.tables[t].slug {
        Some(slug) => format!("{base}/{slug}"),
        None => base,
    }
}

pub(crate) fn pid_of_variable(r: &Resolved, v: usize) -> String {
    format!("{}/{}", pid_of_register(r), r.variables[v].slug)
}

#[derive(Default)]
struct Rows {
    tables: BTreeMap<&'static str, Vec<Vec<Cell>>>,
}

impl Rows {
    fn push(&mut self, table: &'static str, row: Vec<Cell>) {
        self.tables.entry(table).or_default().push(row);
    }

    /// The `ident` row of a public id: its kind's row `id`, its register, and its
    /// folded `name`.
    fn ident(&mut self, pid: String, kind: &str, id: i64, register: Cell, name: &str) {
        self.push(
            "ident",
            vec![
                Cell::Str(pid),
                Cell::Str(kind.into()),
                Cell::Int(id),
                register,
                Cell::Str(reg_core::fold_identity(name)),
            ],
        );
    }
}

fn write_provenance(
    rows: &mut Rows,
    subject: &ProvenanceSubject,
    subject_id: i64,
    values: &BTreeMap<&ProvenanceSubject, Vec<&FinalValue>>,
    statements: &BTreeMap<&str, i64>,
) -> Result<()> {
    let kind = match subject {
        ProvenanceSubject::Register(_) => "register",
        ProvenanceSubject::Set(..) => "set",
        ProvenanceSubject::Table(..) => "table",
        ProvenanceSubject::Release(..) => "release",
        ProvenanceSubject::Occurrence(..) => "occurrence",
        ProvenanceSubject::Variable(..) => "variable",
        ProvenanceSubject::ValueSet(_) => "valueset",
        ProvenanceSubject::ClassVersion(..) => "class_version",
    };
    for value in values.get(subject).into_iter().flatten() {
        let (prov, statement, rule) = match &value.origin {
            Origin::Curated(hash) => (
                "curated",
                Some(
                    *statements
                        .get(hash.as_str())
                        .ok_or_else(|| format!("unknown curation statement {hash}"))?,
                ),
                None,
            ),
            Origin::Rule(code) => ("rule", None, Some(*code)),
        };
        rows.push(
            "override",
            vec![
                Cell::Str(kind.into()),
                Cell::Int(subject_id),
                Cell::Str(value.field.clone()),
                Cell::Str(prov.into()),
                opt_int(statement),
                rule.map_or(Cell::Null, |code| Cell::Str(code.into())),
                // A winning type publishes under the catalog's name, like the
                // occurrence it set; the losing source value stays as stated evidence.
                if value.field == "type" {
                    published_dtype(value.value.as_deref())
                } else {
                    opt_str(value.value.as_ref())
                },
                opt_str(value.source_value.as_ref()),
            ],
        );
    }
    Ok(())
}

/// Write `catalog` to `out`: build a scratch file next to it, compact it into a staged
/// file, run the schema's post-write checks on that and only then replace `out`. A
/// failed build leaves `out` as it was.
///
/// # Errors
/// A SQLite failure, or a post-write check that returns rows.
pub fn write(catalog: &Catalog, out: &Path) -> Result<()> {
    let name = out
        .file_name()
        .map_or_else(String::new, |n| n.to_string_lossy().into_owned());
    let scratch = out.with_file_name(format!(".{name}.scratch"));
    let staged = out.with_file_name(format!(".{name}.staged"));
    staged_into(&scratch, &staged, || {
        let t = std::time::Instant::now();
        stage(catalog, &scratch, &staged)?;
        crate::timing::done("write", t);
        let t = std::time::Instant::now();
        crate::verify::catalog(&staged, None)?;
        crate::timing::done("verify", t);
        std::fs::rename(&staged, out).map_err(|e| format!("{}: {e}", out.display()))?;
        Ok(())
    })
}

/// Run `build`, which writes `staged` through `scratch`, checks it and moves it into
/// place; then remove whichever of the two is left, so a failure leaves the output as it
/// was.
pub(crate) fn staged_into(
    scratch: &Path,
    staged: &Path,
    build: impl FnOnce() -> Result<()>,
) -> Result<()> {
    let result = build();
    for path in [scratch, staged] {
        if path.exists() {
            let _ = std::fs::remove_file(path);
        }
    }
    result
}

#[allow(clippy::too_many_lines)]
fn stage(catalog: &Catalog, scratch: &Path, staged: &Path) -> Result<()> {
    let mut rows = Rows::default();
    let statement_id = sorted_ids(catalog.statements.iter().map(|s| s.hash.as_str()));
    let mut external = catalog.external_relations.clone();
    external.sort();
    external.dedup();
    for (i, relation) in external.iter().enumerate() {
        rows.push(
            "holding_relation",
            vec![
                Cell::Int(id(i)),
                Cell::Str("derived_from".into()),
                Cell::Str(relation.subject_kind.clone()),
                Cell::Str(relation.subject_pid.clone()),
                Cell::Str(relation.object_kind.clone()),
                Cell::Str(relation.object_pid.clone()),
                opt_int(relation.start.as_deref().map(start)),
                opt_int(relation.end.as_deref().map(end)),
                Cell::Int(statement_id[relation.statement.as_str()]),
                opt_str(relation.caveat.as_ref()),
            ],
        );
    }
    let mut provenance_by_subject = BTreeMap::<_, Vec<_>>::new();
    for value in &catalog.provenance.values {
        provenance_by_subject
            .entry(&value.subject)
            .or_default()
            .push(value);
    }
    let curated_locators: BTreeSet<_> = catalog
        .statements
        .iter()
        .map(|s| reg_frame::Locator {
            input: s.file.clone(),
            at: format!("line={}", s.line),
        })
        .collect();
    for statement in &catalog.statements {
        rows.push(
            "statement",
            vec![
                Cell::Int(statement_id[statement.hash.as_str()]),
                Cell::Str(statement.hash.clone()),
                Cell::Str(statement.kind.clone()),
                Cell::Str(statement.file.clone()),
                Cell::Int(count(statement.line)),
                opt_text(statement.evidence.as_ref()),
                opt_text(statement.caveat.as_ref()),
            ],
        );
    }

    for (key, value) in &catalog.meta {
        rows.push(
            "meta",
            vec![Cell::Str((*key).to_owned()), Cell::Str(value.clone())],
        );
    }
    for (i, input) in catalog.inputs.iter().enumerate() {
        rows.push(
            "input",
            vec![
                Cell::Int(id(i)),
                Cell::Str(input.name.clone()),
                Cell::Str(input.repo.clone()),
                Cell::Str(input.commit.clone()),
                Cell::Str(input.path.clone()),
                Cell::Str(input.sha256.clone()),
                Cell::Int(input.bytes),
            ],
        );
    }

    // Row ids by natural key.
    let provider_id: BTreeMap<&str, i64> =
        sorted_ids(catalog.providers.iter().map(|p| p.pid.as_str()));
    let register_id: BTreeMap<String, i64> =
        sorted_ids(catalog.registers.iter().map(pid_of_register));
    let mut table_keys = Vec::new();
    let mut variable_keys = Vec::new();
    let mut segment_keys = Vec::new();
    let mut occurrence_keys = Vec::new();
    let mut release_keys = Vec::new();
    for (ri, r) in catalog.registers.iter().enumerate() {
        for t in 0..r.tables.len() {
            table_keys.push((pid_of_table(r, t), (ri, t)));
        }
        for (x, release) in r.releases.iter().enumerate() {
            release_keys.push((
                (
                    pid_of_table(r, release.table),
                    release.release.release.clone(),
                ),
                (ri, x),
            ));
        }
        for (v, variable) in r.variables.iter().enumerate() {
            variable_keys.push((pid_of_variable(r, v), (ri, v)));
            for (s, segment) in variable.segments.iter().enumerate() {
                segment_keys.push((
                    (
                        pid_of_variable(r, v),
                        pid_of_table(r, segment.table),
                        start(&segment.period.0),
                    ),
                    (ri, v, s),
                ));
            }
        }
        for (o, occ) in r.occurrences.iter().enumerate() {
            let x = &occ.occurrence;
            occurrence_keys.push((
                (
                    pid_of_table(r, occ.table),
                    occ.period.as_ref().map(|p| start(&p.0)),
                    reg_core::fold_identity(&x.column),
                    x.source_key.clone(),
                    x.label.clone(),
                    x.locator.clone(),
                    // Preserve existing row order; use full source identity on ties.
                    reg_resolve::OccurrenceIdentity::new(
                        x,
                        occ.stated.clone().or_else(|| occ.period.clone()),
                    ),
                ),
                (ri, o),
            ));
        }
    }
    let table_id = index(table_keys, "table ids")?;
    let variable_id = index(variable_keys, "variable ids")?;
    let segment_id = index(segment_keys, "segment keys")?;
    let class_id = sorted_ids(
        catalog
            .classifications
            .iter()
            .map(|c| (c.family.as_str(), c.version.as_str())),
    );

    let classification_id = |fqid: &str| {
        let reg_core::Fqid::ClassVersion { family, version } =
            fqid.parse().expect("validated classification")
        else {
            unreachable!("exact version")
        };
        class_id[&(family.as_str(), version.as_str())]
    };
    let occurrence_id = index(occurrence_keys, "occurrence keys")?;
    let release_id = index(release_keys, "release keys")?;
    let valueset_id: BTreeMap<&str, i64> = sorted_ids(catalog.valuesets.keys().map(String::as_str));

    for p in &catalog.providers {
        let pid = provider_id[p.pid.as_str()];
        rows.push(
            "provider",
            vec![
                Cell::Int(pid),
                Cell::Str(p.pid.clone()),
                Cell::Text(p.name.clone()),
                opt_str(p.url.as_ref()),
            ],
        );
        rows.ident(p.pid.clone(), "provider", pid, Cell::Null, &p.pid);
    }

    let input_of = |locator: &reg_frame::Locator| {
        catalog
            .file_input
            .get(&locator.input)
            .copied()
            .ok_or_else(|| format!("{}: no manifest input read this file", locator.input))
    };
    for (ri, r) in catalog.registers.iter().enumerate() {
        let reg = &r.register;
        let reg_pid = pid_of_register(r);
        let register_row = register_id[&reg_pid];
        write_provenance(
            &mut rows,
            &ProvenanceSubject::Register(ri),
            register_row,
            &provenance_by_subject,
            &statement_id,
        )?;
        let pid = *provider_id
            .get(reg.provider.as_str())
            .ok_or_else(|| format!("provider {:?} is not in the manifest", reg.provider))?;
        rows.push(
            "register",
            vec![
                Cell::Int(register_row),
                Cell::Int(pid),
                Cell::Str(reg_pid.clone()),
                opt_text(reg.name.as_ref()),
                opt_text(reg.description.as_ref()),
                Cell::Int(start(&r.coverage.0)),
                Cell::Int(end(&r.coverage.1)),
            ],
        );
        rows.ident(
            reg_pid.clone(),
            "register",
            register_row,
            Cell::Null,
            &r.public,
        );

        for (t, table) in r.tables.iter().enumerate() {
            let tid = table_id[&(ri, t)];
            write_provenance(
                &mut rows,
                &ProvenanceSubject::Table(ri, t),
                tid,
                &provenance_by_subject,
                &statement_id,
            )?;
            let (lo, hi) = hull(&table.spans);
            rows.push(
                "tbl",
                vec![
                    Cell::Int(tid),
                    Cell::Int(register_row),
                    Cell::Str(pid_of_table(r, t)),
                    opt_text(table.table.name.as_ref()),
                    opt_text(table.table.description.as_ref()),
                    Cell::Str(
                        reg_resolve::role_value(
                            "table",
                            table.table.role.as_deref().unwrap_or("data"),
                        )?
                        .into(),
                    ),
                    Cell::Int(i64::from(table.slug.is_none())),
                    Cell::Int(i64::from(table.timeless)),
                    if table.timeless {
                        Cell::Null
                    } else {
                        opt_str(table.table.cadence.as_ref())
                    },
                    Cell::Int(lo),
                    Cell::Int(hi),
                ],
            );
            for span in &table.spans {
                rows.push(
                    "tbl_span",
                    vec![
                        Cell::Int(tid),
                        Cell::Int(start(&span.0)),
                        Cell::Int(end(&span.1)),
                    ],
                );
            }
            // The implicit table shares the register's id, which `ident` already holds.
            if let Some(slug) = &table.slug {
                rows.ident(
                    pid_of_table(r, t),
                    "table",
                    tid,
                    Cell::Int(register_row),
                    slug,
                );
            }
        }

        for (x, release) in r.releases.iter().enumerate() {
            let rid = release_id[&(ri, x)];
            let rel = &release.release;
            let period = rel.from.as_ref().zip(rel.to.as_ref());
            write_provenance(
                &mut rows,
                &ProvenanceSubject::Release(ri, x),
                rid,
                &provenance_by_subject,
                &statement_id,
            )?;
            let asserted = curated_locators.contains(&rel.locator);
            rows.push(
                "release",
                vec![
                    Cell::Int(rid),
                    Cell::Int(table_id[&(ri, release.table)]),
                    Cell::Str(
                        if asserted
                            || provenance_by_subject
                                .get(&ProvenanceSubject::Release(ri, x))
                                .is_some_and(|values| {
                                    values.iter().any(|v| {
                                        v.field == "period"
                                            && matches!(v.origin, Origin::Curated(_))
                                    })
                                })
                        {
                            "curated"
                        } else if period.is_some() {
                            "parsed"
                        } else {
                            "unparsed"
                        }
                        .into(),
                    ),
                    opt_int(period.map(|p| start(p.0))),
                    opt_int(period.map(|p| end(p.1))),
                    opt_str(rel.grain.as_ref().filter(|_| period.is_some())),
                    Cell::Str(rel.edition.clone()),
                    Cell::Str(rel.qualifier.clone().unwrap_or_default()),
                    flag(release.primary),
                    Cell::Int(i64::from(release.edition_disagreement)),
                    Cell::Text(rel.name.clone()),
                    opt_text(rel.description.as_ref()),
                    Cell::Str(rel.release.clone()),
                    if asserted {
                        Cell::Null
                    } else {
                        Cell::Int(id(input_of(&rel.locator)?))
                    },
                    if asserted {
                        Cell::Null
                    } else {
                        Cell::Str(format!("{}:{}", rel.locator.input, rel.locator.at))
                    },
                ],
            );
            for (ord, population) in rel.populations.iter().enumerate() {
                rows.push(
                    "release_population",
                    vec![
                        Cell::Int(rid),
                        Cell::Int(count(ord)),
                        Cell::Text(population.name.clone()),
                        opt_text(population.definition.as_ref()),
                        opt_int(population.day.as_deref().map(start)),
                    ],
                );
            }
        }

        for (v, variable) in r.variables.iter().enumerate() {
            let vid = variable_id[&(ri, v)];
            rows.push(
                "variable",
                vec![
                    Cell::Int(vid),
                    Cell::Int(register_row),
                    Cell::Str(pid_of_variable(r, v)),
                    Cell::Text(variable.label.clone()),
                    opt_text(variable.definition.as_ref()),
                    opt_text(variable.description.as_ref()),
                    Cell::Int(start(&variable.hull.0)),
                    Cell::Int(end(&variable.hull.1)),
                ],
            );
            rows.ident(
                pid_of_variable(r, v),
                "variable",
                vid,
                Cell::Int(register_row),
                &variable.slug,
            );
            write_provenance(
                &mut rows,
                &ProvenanceSubject::Variable(ri, v),
                vid,
                &provenance_by_subject,
                &statement_id,
            )?;
            for run in &variable.texts {
                rows.push(
                    "variable_text",
                    vec![
                        Cell::Int(vid),
                        Cell::Int(start(&run.period.0)),
                        Cell::Int(end(&run.period.1)),
                        Cell::Text(run.label.clone()),
                        opt_text(run.definition.as_ref()),
                        opt_text(run.operational_definition.as_ref()),
                        opt_text(run.unit.as_ref()),
                    ],
                );
            }
            let folds: BTreeSet<String> = variable
                .spellings
                .iter()
                .map(|s| reg_core::fold_identity(s))
                .collect();
            for f in folds {
                rows.push(
                    "alias",
                    vec![Cell::Fold(f), Cell::Int(vid), Cell::Str("delivered".into())],
                );
            }
            for (s, segment) in variable.segments.iter().enumerate() {
                rows.push(
                    "segment",
                    vec![
                        Cell::Int(segment_id[&(ri, v, s)]),
                        Cell::Int(vid),
                        Cell::Int(table_id[&(ri, segment.table)]),
                        Cell::Fold(reg_core::fold_identity(&segment.spelling)),
                        Cell::Int(start(&segment.period.0)),
                        Cell::Int(end(&segment.period.1)),
                        Cell::Spelling(segment.spelling.clone()),
                        published_dtype(segment.dtype.as_deref()),
                        opt_int(segment.length),
                        opt_str(segment.format.as_ref()),
                        opt_int(segment.valueset.as_deref().map(|h| valueset_id[h])),
                        opt_int(segment.classification.as_deref().map(classification_id)),
                        Cell::Str(segment.coded.into()),
                        flag(segment.sensitive),
                        flag(segment.identifier),
                        Cell::Str(segment.role.into()),
                        Cell::Str(segment.state.into()),
                    ],
                );
            }
        }

        for (o, occ) in r.occurrences.iter().enumerate() {
            let oid = occurrence_id[&(ri, o)];
            let x = &occ.occurrence;
            write_provenance(
                &mut rows,
                &ProvenanceSubject::Occurrence(ri, o),
                oid,
                &provenance_by_subject,
                &statement_id,
            )?;
            let override_mask = catalog
                .provenance
                .occurrence_masks
                .get(&(ri, o))
                .copied()
                .unwrap_or_default();
            rows.push(
                "occurrence",
                vec![
                    Cell::Int(oid),
                    Cell::Int(table_id[&(ri, occ.table)]),
                    opt_int(occ.release.map(|x| release_id[&(ri, x)])),
                    opt_int(occ.period.as_ref().map(|p| start(&p.0))),
                    opt_int(occ.period.as_ref().map(|p| end(&p.1))),
                    if x.column.is_empty() {
                        Cell::Null
                    } else {
                        Cell::Spelling(x.column.clone())
                    },
                    Cell::Str(x.source_key.clone()),
                    opt_text(x.label.as_ref()),
                    opt_text(x.definition.as_ref()),
                    opt_text(x.operational_definition.as_ref()),
                    opt_text(x.unit.as_ref()),
                    published_dtype(x.dtype.as_deref()),
                    opt_int(x.length),
                    opt_str(x.format.as_ref()),
                    opt_int(x.valueset.as_deref().map(|h| valueset_id[h])),
                    opt_int(x.classification.as_deref().map(classification_id)),
                    Cell::Str(reg_resolve::coded(x).into()),
                    flag(x.sensitive),
                    flag(x.identifier),
                    Cell::Str(reg_resolve::role(x, x.identifier).into()),
                    opt_int(occ.variable.map(|v| variable_id[&(ri, v)])),
                    opt_int(
                        occ.variable
                            .zip(occ.segment)
                            .map(|(v, s)| segment_id[&(ri, v, s)]),
                    ),
                    Cell::Str(occ.status.into()),
                    Cell::Int(override_mask),
                ],
            );
            for pv in &x.population_valuesets {
                rows.push(
                    "occurrence_population_valueset",
                    vec![
                        Cell::Int(oid),
                        Cell::Int(count(pv.population)),
                        Cell::Int(valueset_id[pv.valueset.as_str()]),
                    ],
                );
            }
        }

        for gap in &r.gaps {
            let (kind, subject) = match gap.variable {
                Some(v) => ("variable", variable_id[&(ri, v)]),
                None => ("table", table_id[&(ri, gap.table)]),
            };
            rows.push(
                "gap",
                vec![
                    Cell::Str(kind.into()),
                    Cell::Int(subject),
                    Cell::Int(table_id[&(ri, gap.table)]),
                    Cell::Int(start(&gap.period.0)),
                    Cell::Int(end(&gap.period.1)),
                    Cell::Str(gap.reason.into()),
                ],
            );
        }
    }

    // The link stage's entities, by row id and public id.
    let row_of = |e: Entity| match e {
        Entity::Register(ri) => register_id[&pid_of_register(&catalog.registers[ri])],
        Entity::Table(ri, t) => table_id[&(ri, t)],
        Entity::Release(ri, x) => release_id[&(ri, x)],
        Entity::Variable(ri, v) => variable_id[&(ri, v)],
    };
    let pid_of = |e: Entity| e.pid(&catalog.registers);
    let mut memberships = BTreeMap::<(Option<usize>, String), BTreeSet<(usize, usize)>>::new();
    for (ri, register) in catalog.registers.iter().enumerate() {
        for (variable, resolved) in register.variables.iter().enumerate() {
            for (scope, names) in [(Some(ri), &resolved.sets), (None, &resolved.tags)] {
                for name in names {
                    memberships
                        .entry((scope, name.clone()))
                        .or_default()
                        .insert((ri, variable));
                }
            }
        }
    }
    for (i, ((scope, name), members)) in memberships.into_iter().enumerate() {
        let (kind, pid, register) = scope.map_or_else(
            || ("tag", format!("tag/{name}"), Cell::Null),
            |ri| {
                let reg_pid = pid_of_register(&catalog.registers[ri]);
                (
                    "set",
                    format!("set/{reg_pid}/{name}"),
                    Cell::Int(register_id[&reg_pid]),
                )
            },
        );
        let presentation = scope.map_or_default(|ri| catalog.set_presentation(ri, &name));
        let (label, description) = if scope.is_none() {
            let tag = catalog
                .tags
                .get(&name)
                .expect("tag vocabulary checked at load");
            (tag.label.clone(), Cell::Text(tag.definition.clone()))
        } else {
            (
                presentation
                    .iter()
                    .find_map(|s| s.label.clone())
                    .unwrap_or_else(|| name.clone()),
                presentation
                    .iter()
                    .find_map(|s| s.description.clone())
                    .map_or(Cell::Null, Cell::Text),
            )
        };
        let set_id = id(i);
        rows.push(
            "var_set",
            vec![
                Cell::Int(set_id),
                Cell::Str(kind.into()),
                Cell::Str(pid.clone()),
                register.clone(),
                Cell::Text(label),
                description,
            ],
        );
        if let Some(ri) = scope {
            rows.ident(pid, "set", set_id, register, &name);
            write_provenance(
                &mut rows,
                &ProvenanceSubject::Set(ri, name.clone()),
                set_id,
                &provenance_by_subject,
                &statement_id,
            )?;
        }
        if let Some(axes) = presentation.iter().find(|s| !s.axes.is_empty()) {
            for (ord, axis) in axes.axes.iter().enumerate() {
                rows.push(
                    "var_set_axis",
                    vec![
                        Cell::Int(set_id),
                        Cell::Str(axis.id.clone()),
                        Cell::Int(count(ord)),
                        Cell::Text(axis.label.clone()),
                    ],
                );
            }
        }
        for (ord, (ri, v)) in members.into_iter().enumerate() {
            rows.push(
                "var_set_member",
                vec![
                    Cell::Int(set_id),
                    Cell::Int(variable_id[&(ri, v)]),
                    Cell::Int(i64::try_from(ord).expect("set member ordinal fits i64")),
                ],
            );
            if scope.is_some() {
                for (axis, facet) in &catalog.registers[ri].variables[v].facets {
                    rows.push(
                        "var_set_member_facet",
                        vec![
                            Cell::Int(set_id),
                            Cell::Int(variable_id[&(ri, v)]),
                            Cell::Str(axis.clone()),
                            Cell::Str(facet.value.clone()),
                            Cell::Text(facet.label.as_ref().unwrap_or(&facet.value).clone()),
                        ],
                    );
                }
            }
        }
    }
    // Relation ids follow (kind, subject id, object id, at); the entity kinds break the
    // tie of an implicit table, whose id is its register's.
    let mut relations: Vec<_> = catalog
        .relations
        .iter()
        .map(|rel| {
            let at = rel.at.as_deref().map(start);
            (
                (
                    rel.kind,
                    pid_of(rel.subject),
                    rel.subject.kind(),
                    pid_of(rel.object),
                    rel.object.kind(),
                    at,
                ),
                rel,
            )
        })
        .collect();
    relations.sort_by(|a, b| a.0.cmp(&b.0));
    for (i, ((kind, _, subject_kind, _, object_kind, at), rel)) in relations.into_iter().enumerate()
    {
        rows.push(
            "relation",
            vec![
                Cell::Int(id(i)),
                Cell::Str(kind.into()),
                Cell::Str(subject_kind.into()),
                Cell::Int(row_of(rel.subject)),
                Cell::Str(object_kind.into()),
                Cell::Int(row_of(rel.object)),
                opt_int(at),
                opt_int(rel.end.as_deref().map(end)),
                Cell::Str(
                    if rel.statement.is_some() {
                        "curated"
                    } else {
                        "source"
                    }
                    .into(),
                ),
                opt_int(rel.statement.as_deref().map(|hash| statement_id[hash])),
                opt_text(rel.note.as_ref()),
            ],
        );
    }
    // Series-break ids follow (subject kind, subject pid, day, text, statement).
    let mut breaks: Vec<_> = catalog
        .breaks
        .iter()
        .map(|b| {
            (
                (
                    b.subject.kind(),
                    pid_of(b.subject),
                    b.at.as_deref().map(start),
                    b.text.clone(),
                    b.statement.clone(),
                ),
                b,
            )
        })
        .collect();
    breaks.sort_by(|a, b| a.0.cmp(&b.0));
    for (i, ((subject_kind, _, at, ..), b)) in breaks.into_iter().enumerate() {
        rows.push(
            "series_break",
            vec![
                Cell::Int(id(i)),
                Cell::Str(subject_kind.into()),
                Cell::Int(row_of(b.subject)),
                opt_int(at),
                opt_text(b.text.as_ref()),
                Cell::Str(
                    if b.statement.is_some() {
                        "curated"
                    } else {
                        "source"
                    }
                    .into(),
                ),
                opt_int(b.statement.as_deref().map(|hash| statement_id[hash])),
            ],
        );
    }
    // A value set's classification comes from curation only (D29): the version whose
    // content it is, or the one version its classified segments are curated to when its
    // codes, sentinels aside, are that version's. Containment checks a link, never makes
    // one; anything ambiguous stays unlinked and the segments keep their own versions.
    let mut bound: BTreeMap<&str, BTreeSet<i64>> = BTreeMap::new();
    for segment in catalog
        .registers
        .iter()
        .flat_map(|r| &r.variables)
        .flat_map(|v| &v.segments)
    {
        if let (Some(hash), Some(class)) = (&segment.valueset, &segment.classification) {
            bound
                .entry(hash.as_str())
                .or_default()
                .insert(classification_id(class));
        }
    }
    let mut content: BTreeMap<&str, Vec<i64>> = BTreeMap::new();
    let mut codes: BTreeMap<i64, BTreeSet<&str>> = BTreeMap::new();
    for class in &catalog.classifications {
        let cid = class_id[&(class.family.as_str(), class.version.as_str())];
        content
            .entry(class.valueset.as_str())
            .or_default()
            .push(cid);
        if let Some((set, _)) = catalog.valuesets.get(&class.valueset) {
            codes.insert(cid, set.items.iter().map(|i| i.code.as_str()).collect());
        }
    }
    let class_link = |hash: &str, set: &ValueSet| -> Option<(i64, &'static str)> {
        let own = content.get(hash).map_or(&[][..], Vec::as_slice);
        match bound
            .get(hash)
            .map(|b| b.iter().copied().collect::<Vec<_>>())
            .as_deref()
        {
            Some(&[cid]) if own.contains(&cid) => Some((cid, "equal")),
            Some(&[cid]) => {
                let sentinels = catalog.sentinels.get(hash);
                set.items
                    .iter()
                    .filter(|i| !sentinels.is_some_and(|s| s.contains(&i.code)))
                    .all(|i| codes.get(&cid).is_some_and(|c| c.contains(i.code.as_str())))
                    .then_some((cid, "subset"))
            }
            None => match own {
                &[cid] => Some((cid, "equal")),
                _ => None,
            },
            Some(_) => None,
        }
    };

    for (hash, (set, names)) in &catalog.valuesets {
        let vid = valueset_id[hash.as_str()];
        let link = class_link(hash, set);
        write_provenance(
            &mut rows,
            &ProvenanceSubject::ValueSet(hash.clone()),
            vid,
            &provenance_by_subject,
            &statement_id,
        )?;
        let ord: BTreeMap<&str, usize> = set
            .items
            .iter()
            .enumerate()
            .map(|(i, item)| (item.code.as_str(), i))
            .collect();
        let parent = |i: usize| {
            set.items[i]
                .parent
                .as_deref()
                .and_then(|p| ord.get(p).copied())
        };
        let level = |mut i: usize| {
            let mut depth = 1;
            while let Some(p) = parent(i) {
                depth += 1;
                i = p;
                if depth > set.items.len() {
                    break;
                }
            }
            depth
        };
        let hierarchical = (0..set.items.len()).any(|i| parent(i).is_some());
        rows.push(
            "valueset",
            vec![
                Cell::Int(vid),
                Cell::Str(hash.clone()),
                Cell::Int(count(set.items.len())),
                Cell::Int(i64::from(hierarchical)),
                opt_int(link.map(|l| l.0)),
                link.map_or(Cell::Null, |l| Cell::Str(l.1.into())),
            ],
        );
        for (i, item) in set.items.iter().enumerate() {
            rows.push(
                "vs_item",
                vec![
                    Cell::Int(vid),
                    Cell::Int(count(i)),
                    Cell::Text(item.code.clone()),
                    opt_text(item.label.as_ref()),
                    Cell::Int(count(level(i))),
                    opt_int(parent(i).map(count)),
                    Cell::Int(i64::from(
                        catalog
                            .sentinels
                            .get(hash)
                            .is_some_and(|codes| codes.contains(&item.code)),
                    )),
                ],
            );
        }
        for (input, name) in names {
            rows.push(
                "valueset_name",
                vec![
                    Cell::Int(vid),
                    Cell::Int(id(*input)),
                    Cell::Text(name.clone()),
                ],
            );
        }
    }

    let family_id = sorted_ids(catalog.classifications.iter().map(|c| c.family.as_str()));
    for (family, family_row) in &family_id {
        let facts = reg_resolve::curation::apply::Classification::family_facts(
            &catalog.classifications,
            family,
        );
        rows.ident(
            format!("class/{family}"),
            "class_family",
            *family_row,
            Cell::Null,
            family,
        );
        rows.push(
            "class_family",
            vec![
                Cell::Int(*family_row),
                Cell::Str(format!("class/{family}")),
                Cell::Str("classification".into()),
                Cell::Text(facts.name.into()),
                facts.publisher.map_or(Cell::Null, |s| Cell::Text(s.into())),
                facts
                    .description
                    .map_or(Cell::Null, |s| Cell::Text(s.into())),
            ],
        );
    }
    for class in &catalog.classifications {
        let cid = class_id[&(class.family.as_str(), class.version.as_str())];
        rows.ident(
            format!("class/{}/{}", class.family, class.version),
            "class_version",
            cid,
            Cell::Null,
            &class.version,
        );
        let vid = valueset_id.get(class.valueset.as_str()).ok_or_else(|| {
            format!(
                "classification {}/{} names missing value set",
                class.family, class.version
            )
        })?;
        rows.push(
            "class_version",
            vec![
                Cell::Int(cid),
                Cell::Int(family_id[class.family.as_str()]),
                Cell::Str(format!("class/{}/{}", class.family, class.version)),
                Cell::Text(class.name.as_deref().unwrap_or(&class.version).into()),
                Cell::Int(start(&class.period.0)),
                Cell::Int(end(&class.period.1)),
                Cell::Int(*vid),
                class
                    .part
                    .as_deref()
                    .map_or(Cell::Null, |s| Cell::Text(s.into())),
            ],
        );
        write_provenance(
            &mut rows,
            &ProvenanceSubject::ClassVersion(class.family.clone(), class.version.clone()),
            cid,
            &provenance_by_subject,
            &statement_id,
        )?;
    }

    let mut successions = Vec::new();
    for class in &catalog.classifications {
        for target in &class.succeeds {
            let reg_core::Fqid::ClassVersion { family, version } = target
                .parse()
                .map_err(|e: reg_core::GrammarError| e.to_string())?
            else {
                return Err(format!("invalid classification predecessor {target}").into());
            };
            let subject = class_id[&(class.family.as_str(), class.version.as_str())];
            let object = class_id[&(family.as_str(), version.as_str())];
            let statement = StatementRow::for_locator(&catalog.statements, &class.locator)
                .ok_or("classification inventory statement missing")?;
            successions.push((subject, object, statement_id[statement.hash.as_str()]));
        }
    }
    successions.sort_unstable();
    for (index, (subject, object, statement)) in successions.into_iter().enumerate() {
        rows.push(
            "relation",
            vec![
                Cell::Int(id(catalog.relations.len() + index)),
                Cell::Str("continues".into()),
                Cell::Str("class_version".into()),
                Cell::Int(subject),
                Cell::Str("class_version".into()),
                Cell::Int(object),
                Cell::Null,
                Cell::Null,
                Cell::Str("curated".into()),
                Cell::Int(statement),
                Cell::Null,
            ],
        );
    }

    // Documents. A section's id is its position in (document, ord) order.
    let mut sections = Vec::new();
    for (d, document) in catalog.docs.iter().enumerate() {
        let doc = &document.doc;
        rows.push(
            "doc",
            vec![
                Cell::Int(id(d)),
                Cell::Str(doc.hash.clone()),
                Cell::Str(doc.kind.clone()),
                Cell::Text(doc.title.clone()),
                opt_text(doc.document_title.as_ref()),
                Cell::Str(doc.licence.clone()),
                Cell::Int(i64::try_from(doc.bytes)?),
                opt_str(doc.url.as_ref()),
                Cell::Int(id(input_of(&doc.locator)?)),
                document.content.clone().map_or(Cell::Null, Cell::Blob),
            ],
        );
        for (ord, section) in doc.sections.iter().enumerate() {
            let section_id = id(sections.len());
            sections.push(section);
            rows.push(
                "doc_section",
                vec![
                    Cell::Int(section_id),
                    Cell::Int(id(d)),
                    Cell::Int(count(ord)),
                    opt_str(section.heading.as_ref()),
                    opt_int(section.page.map(i64::from)),
                    Cell::Str(section.body.clone()),
                ],
            );
        }
    }
    // doc_link rows in primary-key order; a document's first row names the register its
    // search row carries.
    let mut doc_links: Vec<_> = catalog
        .doc_links
        .iter()
        .map(|l| {
            let register = register_id[&pid_of_register(&catalog.registers[l.subject.register()])];
            (
                id(l.doc),
                l.subject.kind(),
                row_of(l.subject),
                l.page,
                register,
            )
        })
        .collect();
    doc_links.sort_unstable();
    let mut doc_register = BTreeMap::new();
    for &(doc, kind, subject, page, register) in &doc_links {
        doc_register.entry(doc).or_insert(register);
        rows.push(
            "doc_link",
            vec![
                Cell::Int(doc),
                Cell::Str(kind.into()),
                Cell::Int(subject),
                Cell::Int(i64::from(page)),
                Cell::Str("source".into()),
            ],
        );
    }

    let search = crate::search::items(
        catalog,
        &crate::search::Ids {
            register: &register_id,
            table: &table_id,
            variable: &variable_id,
            doc_register: &doc_register,
            valueset: &valueset_id,
        },
    )?;
    for (i, item) in search.iter().enumerate() {
        rows.push(
            "search_item",
            vec![
                Cell::Int(id(i)),
                Cell::Str(item.kind.into()),
                opt_int(item.entity_id),
                opt_text(item.pair.as_ref().map(|p| &p.0)),
                opt_text(item.pair.as_ref().and_then(|p| p.1.as_ref())),
                opt_int(item.register_id),
                opt_int(item.span.map(|s| s.0)),
                opt_int(item.span.map(|s| s.1)),
                Cell::Str(item.sort_key.clone()),
            ],
        );
        for &valueset in &item.valuesets {
            rows.push(
                "search_code_valueset",
                vec![Cell::Int(id(i)), Cell::Int(valueset)],
            );
        }
    }

    // Interning, in sort order.
    let mut texts = BTreeSet::new();
    let mut folds = BTreeSet::new();
    let mut spellings = BTreeSet::new();
    for cell in rows.tables.values().flatten().flatten() {
        match cell {
            Cell::Text(s) => {
                texts.insert(s.clone());
            }
            Cell::Fold(s) => {
                folds.insert(s.clone());
            }
            Cell::Spelling(s) => {
                folds.insert(reg_core::fold_identity(s));
                spellings.insert(s.clone());
            }
            _ => {}
        }
    }
    let text_id: BTreeMap<&str, i64> = sorted_ids(texts.iter().map(String::as_str));
    let fold_id: BTreeMap<&str, i64> = sorted_ids(folds.iter().map(String::as_str));
    let spelling_id: BTreeMap<&str, i64> = sorted_ids(spellings.iter().map(String::as_str));
    for (body, i) in &text_id {
        rows.push(
            "text",
            vec![
                Cell::Int(*i),
                Cell::Str("sv".into()),
                Cell::Str((*body).to_owned()),
            ],
        );
    }
    for (folded, i) in &fold_id {
        rows.push(
            "name_fold",
            vec![Cell::Int(*i), Cell::Str((*folded).to_owned())],
        );
    }
    for (exact, i) in &spelling_id {
        rows.push(
            "spelling",
            vec![
                Cell::Int(*i),
                Cell::Str((*exact).to_owned()),
                Cell::Int(fold_id[reg_core::fold_identity(exact).as_str()]),
            ],
        );
    }

    // Write.
    for path in [scratch, staged] {
        if path.exists() {
            std::fs::remove_file(path).map_err(|e| format!("{}: {e}", path.display()))?;
        }
    }
    let mut db = Connection::open(scratch)?;
    db.execute_batch(&format!(
        // The cache and temp-store settings are connection state, not file content:
        // `VACUUM INTO` writes the same bytes; they spare index builds the disk (#2196).
        "PRAGMA journal_mode = OFF; PRAGMA synchronous = OFF; PRAGMA auto_vacuum = NONE;
         PRAGMA cache_size = -1048576; PRAGMA temp_store = MEMORY;
         PRAGMA user_version = {};",
        reg_core::CATALOG_SCHEMA_VERSION
    ))?;
    let schema = if external.is_empty() {
        SCHEMA.to_owned()
    } else {
        format!("{SCHEMA}\n{}", include_str!("holdings/relations.sql"))
    };
    let statements = crate::verify::statements(&schema);
    let (indexes, others): (Vec<&String>, Vec<&String>) = statements
        .iter()
        .partition(|s| s.starts_with("CREATE INDEX") || s.starts_with("CREATE UNIQUE INDEX"));
    for statement in &others {
        db.execute_batch(statement)
            .map_err(|e| format!("{statement}: {e}"))?;
    }
    let live: BTreeSet<&str> = [("ident", 0), ("var_set", 2)]
        .into_iter()
        .flat_map(|(table, column)| {
            rows.tables
                .get(table)
                .into_iter()
                .flatten()
                .filter_map(move |row| match &row[column] {
                    Cell::Str(pid) => Some(pid.as_str()),
                    _ => None,
                })
        })
        .collect();
    for retired in &catalog.retired {
        if live.contains(retired.pid.as_str()) {
            return Err(retired.live_error().into());
        }
    }
    for retired in &catalog.retired {
        rows.push(
            "ident_retired",
            vec![
                Cell::Str(retired.pid.clone()),
                Cell::Str(retired.kind.clone()),
                Cell::Str(retired.version.clone()),
                Cell::Str(retired.reason.clone()),
            ],
        );
    }
    // Tables fill in schema order, which declares each table after those it references
    // (except `valueset.class_version_id`, deferred to the one commit).
    // Row order within a table does not reach the file: `VACUUM INTO` rewrites every
    // table in key order.
    let order: Vec<&str> = others
        .iter()
        .filter_map(|s| {
            let table = s.strip_prefix("CREATE TABLE ")?;
            table
                .trim_prefix("IF NOT EXISTS ")
                .split_whitespace()
                .next()
        })
        .collect();
    if let Some(table) = rows.tables.keys().find(|t| !order.contains(t)) {
        return Err(format!("{table}: not a table of schema.sql").into());
    }
    let tx = db.transaction()?;
    for table in order {
        let Some(table_rows) = rows.tables.get(table) else {
            continue;
        };
        let width = table_rows[0].len();
        let placeholders = vec!["?"; width].join(",");
        let mut insert = tx.prepare(&format!("INSERT INTO {table} VALUES ({placeholders})"))?;
        // One row's values at a time: a whole table of owned copies held the
        // largest tables twice (#2196).
        for row in table_rows {
            let values = row.iter().map(|cell| {
                ToSqlOutput::Borrowed(match cell {
                    Cell::Null => ValueRef::Null,
                    Cell::Int(i) => ValueRef::Integer(*i),
                    Cell::Str(s) => ValueRef::Text(s.as_bytes()),
                    Cell::Blob(b) => ValueRef::Blob(b),
                    Cell::Text(s) => ValueRef::Integer(text_id[s.as_str()]),
                    Cell::Fold(s) => ValueRef::Integer(fold_id[s.as_str()]),
                    Cell::Spelling(s) => ValueRef::Integer(spelling_id[s.as_str()]),
                })
            });
            insert
                .execute(rusqlite::params_from_iter(values))
                .map_err(|e| format!("{table}: {e}"))?;
        }
    }
    tx.commit()?;
    for statement in indexes {
        db.execute_batch(statement)?;
    }
    // Contentless FTS5: the rows go in rowid order, then each index is merged. The
    // description index holds only described items, with the description after the body.
    let tx = db.transaction()?;
    for table in ["search", "search_description"] {
        let mut insert = tx.prepare(&format!(
            "INSERT INTO {table}(rowid, name, label, body) VALUES (?1, ?2, ?3, ?4)"
        ))?;
        for (i, item) in search.iter().enumerate() {
            let body: Cow<'_, str> = match (table, item.description.is_empty()) {
                ("search", _) => item.body.as_str().into(),
                (_, true) => continue,
                _ => format!("{}\n{}", item.body, item.description).into(),
            };
            insert
                .execute((id(i), &item.name, &item.label, &body))
                .map_err(|e| format!("{table}: {e}"))?;
        }
        tx.execute(
            &format!("INSERT INTO {table}({table}) VALUES ('optimize')"),
            [],
        )?;
    }
    // External-content FTS5 over doc_section, in rowid order.
    {
        let mut insert =
            tx.prepare("INSERT INTO doc_fts(rowid, heading, body) VALUES (?1, ?2, ?3)")?;
        for (i, section) in sections.iter().enumerate() {
            insert
                .execute((id(i), &section.heading, &section.body))
                .map_err(|e| format!("doc_fts: {e}"))?;
        }
    }
    tx.execute("INSERT INTO doc_fts(doc_fts) VALUES ('optimize')", [])?;
    tx.commit()?;
    db.execute_batch("ANALYZE;")?;
    db.execute("VACUUM INTO ?1", [staged.to_string_lossy().as_ref()])?;
    Ok(())
}

/// Row ids 1.. for `keys` in sort order.
fn sorted_ids<K: Ord, I: IntoIterator<Item = K>>(keys: I) -> BTreeMap<K, i64> {
    let set: BTreeSet<K> = keys.into_iter().collect();
    set.into_iter()
        .enumerate()
        .map(|(i, k)| (k, id(i)))
        .collect()
}

/// Row ids 1.. for entities in sort order of their natural keys, which must be
/// unique.
fn index<K: Ord + std::fmt::Debug, E: Ord + Copy>(
    mut keys: Vec<(K, E)>,
    what: &str,
) -> Result<BTreeMap<E, i64>> {
    keys.sort();
    if let Some(w) = keys.windows(2).find(|w| w[0].0 == w[1].0) {
        return Err(format!("duplicate {what}: {:?}", w[0].0).into());
    }
    Ok(keys
        .into_iter()
        .enumerate()
        .map(|(i, (_, e))| (e, id(i)))
        .collect())
}

fn id(index: usize) -> i64 {
    count(index) + 1
}

pub(crate) fn count(n: usize) -> i64 {
    i64::try_from(n).expect("row counts fit in i64")
}
