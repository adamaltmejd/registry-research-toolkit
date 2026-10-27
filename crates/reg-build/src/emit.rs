//! Link and emit: resolved registers into one SQLite file, schema v4 (`schema.sql`).
//! The emitter decides nothing. Row ids follow natural-key order, indexes come after
//! data, and the file is written with `VACUUM INTO`, so two builds from the same pins give the same bytes.
//!
//! It fills the identity, coverage, release, variable, segment, occurrence, gap,
//! value-set, relation, finding and search tables (`search.rs`); classifications,
//! documents and curation statements stay empty.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use reg_frame::ValueSet;
use reg_resolve::{Resolved, Subject};

use crate::link::{self, Entity};
use rusqlite::Connection;
use rusqlite::types::{ToSql, ToSqlOutput};

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

/// Everything the file holds, linked.
pub struct Catalog {
    pub meta: BTreeMap<&'static str, String>,
    /// Sorted by name.
    pub inputs: Vec<InputRow>,
    pub providers: Vec<ProviderRow>,
    pub registers: Vec<Resolved>,
    /// Value sets by hash, with the (input index, name) pairs that named them.
    pub valuesets: BTreeMap<String, (ValueSet, BTreeSet<(usize, String)>)>,
    /// A frame file (locator input) → its input index.
    pub file_input: BTreeMap<String, usize>,
    /// The link stage's output (`link.rs`).
    pub relations: Vec<link::Relation>,
    pub notices: Vec<link::Notice>,
}

/// A cell before interning: text, folded names and spellings are resolved to row ids
/// once every body is known, so their ids too follow sort order.
#[derive(Clone)]
enum Cell {
    Null,
    Int(i64),
    Str(String),
    Text(String),
    Fold(String),
    Spelling(String),
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Value {
    Null,
    Int(i64),
    Str(String),
}

impl ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match self {
            Value::Null => Ok(ToSqlOutput::from(rusqlite::types::Null)),
            Value::Int(i) => Ok(ToSqlOutput::from(*i)),
            Value::Str(s) => Ok(ToSqlOutput::from(s.as_str())),
        }
    }
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
    format!("{}/{}", r.register.provider, r.register.register)
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
}

/// Write `catalog` to `out`: build a scratch file next to it, compact it into a staged
/// file, run the schema's post-write checks on that and only then replace `out`. A
/// failed build leaves `out` as it was.
///
/// # Errors
/// A SQLite failure, or a post-write check that returns rows.
pub fn write(catalog: &Catalog, out: &Path) -> Result<(), String> {
    let name = out
        .file_name()
        .map_or_else(String::new, |n| n.to_string_lossy().into_owned());
    let scratch = out.with_file_name(format!(".{name}.scratch"));
    let staged = out.with_file_name(format!(".{name}.staged"));
    let result = stage(catalog, &scratch, &staged)
        .and_then(|()| crate::verify::catalog(&staged, None))
        .and_then(|()| {
            std::fs::rename(&staged, out).map_err(|e| format!("{}: {e}", out.display()))
        });
    for path in [&scratch, &staged] {
        if path.exists() {
            let _ = std::fs::remove_file(path);
        }
    }
    result
}

#[allow(clippy::too_many_lines)]
fn stage(catalog: &Catalog, scratch: &Path, staged: &Path) -> Result<(), String> {
    let mut rows = Rows::default();
    let sql = |e: rusqlite::Error| e.to_string();

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
                ),
                (ri, o),
            ));
        }
    }
    let table_id = index(table_keys, "table ids")?;
    let variable_id = index(variable_keys, "variable ids")?;
    let segment_id = index(segment_keys, "segment keys")?;
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
        rows.push(
            "ident",
            vec![
                Cell::Str(p.pid.clone()),
                Cell::Str("provider".into()),
                Cell::Int(pid),
                Cell::Null,
                Cell::Str(reg_core::fold_identity(&p.pid)),
            ],
        );
    }

    let input_of = |locator: &reg_frame::Locator| {
        catalog
            .file_input
            .get(&locator.input)
            .copied()
            .ok_or_else(|| format!("{}: no manifest input read this file", locator.input))
    };
    let mut findings = Vec::new();
    for (ri, r) in catalog.registers.iter().enumerate() {
        let reg = &r.register;
        let reg_pid = pid_of_register(r);
        let register_row = register_id[&reg_pid];
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
        rows.push(
            "ident",
            vec![
                Cell::Str(reg_pid.clone()),
                Cell::Str("register".into()),
                Cell::Int(register_row),
                Cell::Null,
                Cell::Str(reg_core::fold_identity(&reg.register)),
            ],
        );

        for (t, table) in r.tables.iter().enumerate() {
            let tid = table_id[&(ri, t)];
            let (lo, hi) = hull(&table.spans);
            rows.push(
                "tbl",
                vec![
                    Cell::Int(tid),
                    Cell::Int(register_row),
                    Cell::Str(pid_of_table(r, t)),
                    opt_text(table.table.name.as_ref()),
                    opt_text(table.table.description.as_ref()),
                    Cell::Str("data".into()),
                    Cell::Int(i64::from(table.slug.is_none())),
                    Cell::Int(i64::from(table.timeless)),
                    Cell::Null,
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
                rows.push(
                    "ident",
                    vec![
                        Cell::Str(pid_of_table(r, t)),
                        Cell::Str("table".into()),
                        Cell::Int(tid),
                        Cell::Int(register_row),
                        Cell::Str(reg_core::fold_identity(slug)),
                    ],
                );
            }
        }

        for (x, release) in r.releases.iter().enumerate() {
            let rid = release_id[&(ri, x)];
            let rel = &release.release;
            let period = rel.from.as_ref().zip(rel.to.as_ref());
            rows.push(
                "release",
                vec![
                    Cell::Int(rid),
                    Cell::Int(table_id[&(ri, release.table)]),
                    Cell::Str(
                        if period.is_some() {
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
                    Cell::Text(rel.name.clone()),
                    opt_text(rel.description.as_ref()),
                    Cell::Null,
                    Cell::Str(rel.release.clone()),
                    Cell::Int(id(input_of(&rel.locator)?)),
                    Cell::Str(format!("{}:{}", rel.locator.input, rel.locator.at)),
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
                    Cell::Int(start(&variable.hull.0)),
                    Cell::Int(end(&variable.hull.1)),
                ],
            );
            rows.push(
                "ident",
                vec![
                    Cell::Str(pid_of_variable(r, v)),
                    Cell::Str("variable".into()),
                    Cell::Int(vid),
                    Cell::Int(register_row),
                    Cell::Str(reg_core::fold_identity(&variable.slug)),
                ],
            );
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
                        opt_str(segment.dtype.as_ref()),
                        Cell::Null,
                        Cell::Null,
                        opt_int(segment.valueset.as_deref().map(|h| valueset_id[h])),
                        Cell::Null,
                        Cell::Str(segment.coded.into()),
                        flag(segment.sensitive),
                        flag(segment.identifier),
                        Cell::Null,
                        Cell::Str(segment.role.into()),
                        Cell::Str("unknown".into()),
                        Cell::Str(segment.state.into()),
                    ],
                );
            }
        }

        for (o, occ) in r.occurrences.iter().enumerate() {
            let oid = occurrence_id[&(ri, o)];
            let x = &occ.occurrence;
            let clamped = occ.stated.is_some() && occ.status == "used";
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
                    opt_str(x.dtype.as_ref()),
                    opt_int(x.length),
                    Cell::Null,
                    opt_int(x.valueset.as_deref().map(|h| valueset_id[h])),
                    Cell::Null,
                    Cell::Str(reg_resolve::coded(x).into()),
                    flag(x.sensitive),
                    flag(x.identifier),
                    Cell::Null,
                    Cell::Str(reg_resolve::role(x, x.identifier).into()),
                    Cell::Str("unknown".into()),
                    opt_int(occ.variable.map(|v| variable_id[&(ri, v)])),
                    opt_int(
                        occ.variable
                            .zip(occ.segment)
                            .map(|(v, s)| segment_id[&(ri, v, s)]),
                    ),
                    Cell::Str(occ.status.into()),
                    Cell::Int(if clamped { 1 << 16 } else { 0 }),
                ],
            );
            for locator in std::iter::once(&x.locator).chain(&x.also) {
                rows.push(
                    "occurrence_locator",
                    vec![
                        Cell::Int(oid),
                        Cell::Int(id(input_of(locator)?)),
                        Cell::Str(format!("{}:{}", locator.input, locator.at)),
                    ],
                );
            }
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
            if clamped {
                let render = |p: &reg_core::Interval| reg_core::render(std::slice::from_ref(p));
                rows.push(
                    "override",
                    vec![
                        Cell::Str("occurrence".into()),
                        Cell::Int(oid),
                        Cell::Str("period".into()),
                        Cell::Str("rule".into()),
                        Cell::Null,
                        Cell::Str("T1".into()),
                        opt_str(occ.period.as_ref().map(render).as_ref()),
                        opt_str(occ.stated.as_ref().map(render).as_ref()),
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

        for f in &r.findings {
            let (kind, subject) = match f.subject {
                Subject::Register => ("register", register_row),
                Subject::Release(x) => ("release", release_id[&(ri, x)]),
                Subject::Variable(v) => ("variable", variable_id[&(ri, v)]),
                Subject::Segment(v, s) => ("segment", segment_id[&(ri, v, s)]),
                Subject::Occurrence(o) => ("occurrence", occurrence_id[&(ri, o)]),
            };
            let period = f.period.as_ref().map(|p| (start(&p.0), end(&p.1)));
            findings.push((
                (reg_pid.clone(), f.code, kind, subject, period),
                (register_row, f.severity, f.message.clone()),
            ));
        }
    }

    // The link stage's entities, by row id and public id.
    let row_of = |e: Entity| match e {
        Entity::Register(ri) => register_id[&pid_of_register(&catalog.registers[ri])],
        Entity::Table(ri, t) => table_id[&(ri, t)],
        Entity::Release(ri, x) => release_id[&(ri, x)],
        Entity::Variable(ri, v) => variable_id[&(ri, v)],
    };
    let pid_of = |e: Entity| match e {
        Entity::Register(ri) => pid_of_register(&catalog.registers[ri]),
        Entity::Table(ri, t) => pid_of_table(&catalog.registers[ri], t),
        Entity::Release(ri, x) => {
            let r = &catalog.registers[ri];
            let release = &r.releases[x];
            format!(
                "{}/{}",
                pid_of_table(r, release.table),
                release.release.release
            )
        }
        Entity::Variable(ri, v) => pid_of_variable(&catalog.registers[ri], v),
    };
    for notice in &catalog.notices {
        let r = &catalog.registers[notice.subject.register()];
        let reg_pid = pid_of_register(r);
        let period = notice.at.as_deref().map(|at| (start(at), end(at)));
        findings.push((
            (
                reg_pid.clone(),
                "S0:source-notice",
                notice.subject.kind(),
                row_of(notice.subject),
                period,
            ),
            (register_id[&reg_pid], "notice", notice.text.clone()),
        ));
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
                Cell::Null,
                Cell::Str("source".into()),
                Cell::Null,
                opt_text(rel.note.as_ref()),
            ],
        );
    }
    findings.sort();
    for (i, ((_, code, kind, subject, period), (register_row, severity, message))) in
        findings.into_iter().enumerate()
    {
        rows.push(
            "finding",
            vec![
                Cell::Int(id(i)),
                Cell::Str(code.into()),
                Cell::Str(severity.into()),
                Cell::Str(kind.into()),
                Cell::Int(subject),
                Cell::Int(register_row),
                opt_int(period.map(|p| p.0)),
                opt_int(period.map(|p| p.1)),
                Cell::Text(message),
                Cell::Null,
                Cell::Null,
            ],
        );
    }

    for (hash, (set, names)) in &catalog.valuesets {
        let vid = valueset_id[hash.as_str()];
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
                Cell::Null,
                Cell::Null,
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
                    Cell::Int(0),
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

    let search = crate::search::items(
        catalog,
        &crate::search::Ids {
            register: &register_id,
            table: &table_id,
            variable: &variable_id,
        },
    )?;
    for (i, item) in search.iter().enumerate() {
        rows.push(
            "search_item",
            vec![
                Cell::Int(id(i)),
                Cell::Str(item.kind.into()),
                opt_int(item.entity_id),
                Cell::Null,
                Cell::Null,
                opt_int(item.register_id),
                opt_int(item.span.map(|s| s.0)),
                opt_int(item.span.map(|s| s.1)),
                Cell::Str(item.sort_key.clone()),
            ],
        );
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
    let mut db = Connection::open(scratch).map_err(sql)?;
    db.execute_batch(
        "PRAGMA journal_mode = OFF; PRAGMA synchronous = OFF; PRAGMA auto_vacuum = NONE;",
    )
    .map_err(sql)?;
    let statements: Vec<String> = SCHEMA
        .lines()
        .map(|line| line.split_once("--").map_or(line, |(code, _)| code))
        .collect::<Vec<_>>()
        .join("\n")
        .split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    let (indexes, others): (Vec<&String>, Vec<&String>) = statements
        .iter()
        .partition(|s| s.starts_with("CREATE INDEX") || s.starts_with("CREATE UNIQUE INDEX"));
    for statement in &others {
        db.execute_batch(statement)
            .map_err(|e| format!("{statement}: {e}"))?;
    }
    // Tables fill in schema order, which declares each table after those it references.
    // Row order within a table does not reach the file: `VACUUM INTO` rewrites every
    // table in key order.
    let order: Vec<&str> = others
        .iter()
        .filter_map(|s| s.strip_prefix("CREATE TABLE ")?.split_whitespace().next())
        .collect();
    if let Some(table) = rows.tables.keys().find(|t| !order.contains(t)) {
        return Err(format!("{table}: not a table of schema.sql"));
    }
    for table in order {
        let Some(table_rows) = rows.tables.get(table) else {
            continue;
        };
        let values: Vec<Vec<Value>> = table_rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|cell| match cell {
                        Cell::Null => Value::Null,
                        Cell::Int(i) => Value::Int(*i),
                        Cell::Str(s) => Value::Str(s.clone()),
                        Cell::Text(s) => Value::Int(text_id[s.as_str()]),
                        Cell::Fold(s) => Value::Int(fold_id[s.as_str()]),
                        Cell::Spelling(s) => Value::Int(spelling_id[s.as_str()]),
                    })
                    .collect()
            })
            .collect();
        let width = values[0].len();
        let tx = db.transaction().map_err(sql)?;
        {
            let placeholders = vec!["?"; width].join(",");
            let mut insert = tx
                .prepare(&format!("INSERT INTO {table} VALUES ({placeholders})"))
                .map_err(sql)?;
            for row in &values {
                insert
                    .execute(rusqlite::params_from_iter(row))
                    .map_err(|e| format!("{table}: {e}"))?;
            }
        }
        tx.commit().map_err(sql)?;
    }
    for statement in indexes {
        db.execute_batch(statement).map_err(sql)?;
    }
    // Contentless FTS5: the rows go in rowid order, then the index is merged.
    let tx = db.transaction().map_err(sql)?;
    {
        let mut insert = tx
            .prepare("INSERT INTO search(rowid, name, label, body) VALUES (?1, ?2, ?3, ?4)")
            .map_err(sql)?;
        for (i, item) in search.iter().enumerate() {
            insert
                .execute((id(i), &item.name, &item.label, &item.body))
                .map_err(|e| format!("search: {e}"))?;
        }
    }
    tx.execute("INSERT INTO search(search) VALUES ('optimize')", [])
        .map_err(sql)?;
    tx.commit().map_err(sql)?;
    db.execute_batch("ANALYZE;").map_err(sql)?;
    db.execute("VACUUM INTO ?1", [staged.to_string_lossy().as_ref()])
        .map_err(sql)?;
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
) -> Result<BTreeMap<E, i64>, String> {
    keys.sort();
    if let Some(w) = keys.windows(2).find(|w| w[0].0 == w[1].0) {
        return Err(format!("duplicate {what}: {:?}", w[0].0));
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
