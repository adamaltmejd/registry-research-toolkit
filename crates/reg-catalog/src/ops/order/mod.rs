//! `validate` and `order`: the order check of design v4 §9 over the v4 catalog
//! (crates/DESIGN.md, "The logical order"). One function, [`check`], answers both
//! operations, which is what makes their results identical.
//!
//! The check, per source: resolve the table (`refs::table`) and the bindings' ids; check
//! the period against the table (`_default` exactly on a timeless table); follow the
//! table's dated `continues` successors, each day going to the table whose `at_start`
//! window holds it, and report an undated one (`table_succession_undated`, S5) instead
//! of following it; then per binding clip the request to the variable's segments
//! (`window.rs`), split it by form and collect the window findings. Panels compare their
//! members' row-key kinds last.
//!
//! Findings are ordered per source, then per binding (the source's own findings first),
//! then by code and period start; panel findings come last. A finding's `path` is the
//! field it is about (`/sources/0/table`, `/sources/0/period`, `/sources/0/edition`,
//! `/sources/0/bindings/1/variable`, `/sources/0/bindings/1/classification`), the
//! binding for a window finding (`/sources/0/bindings/1`), the source for an undated
//! succession and the panel for a panel finding.

mod rows;
mod window;

use std::collections::BTreeSet;

use reg_core::project::{ProjectData, Source};
use reg_core::{Interval, IssueLevel, ValidationIssue, overlap};
use rusqlite::{Connection, OptionalExtension};
use serde::Serialize;
use utoipa::ToSchema;

use super::{Operation, Period, Server, iso_day, refs};
use crate::{Code, Error};

/// Written by step 03.
pub const OPERATIONS: &[Operation] = &[];

/// `shape.Issue`: a project issue or an order finding, located in the project.
#[derive(Clone, Serialize, ToSchema)]
pub struct Issue {
    level: IssueLevel,
    code: String,
    /// An RFC 6901 pointer into the project; empty for the whole document.
    path: String,
    message: String,
    /// The project source's name.
    #[schema(required = true)]
    source: Option<String>,
    #[schema(required = true)]
    variable: Option<String>,
    #[schema(required = true)]
    table: Option<String>,
    #[schema(required = true)]
    period: Option<Period>,
    /// The successor a `variable_replaced` finding names, or the successor table of an
    /// undated succession (`table_succession_undated`).
    #[schema(required = true)]
    successor_fqid: Option<String>,
}

impl Issue {
    /// Whether the issue blocks: an `error`.
    #[must_use]
    pub fn blocks(&self) -> bool {
        self.level == IssueLevel::Error
    }

    /// The ordering within one anchor (a source, a binding, a panel): code, then period
    /// start.
    fn key(&self) -> (String, Option<String>) {
        (
            self.code.clone(),
            self.period.as_ref().and_then(|p| p.start.clone()),
        )
    }
}

/// A structural issue, whose located fields are `null`.
impl From<ValidationIssue> for Issue {
    fn from(issue: ValidationIssue) -> Self {
        Self {
            level: issue.level,
            code: issue.code.to_owned(),
            path: issue.path,
            message: issue.message,
            source: None,
            variable: None,
            table: None,
            period: None,
            successor_fqid: issue.successor_fqid,
        }
    }
}

/// `shape.LogicalOrder` (crates/DESIGN.md, "The logical order").
#[derive(Serialize, ToSchema)]
pub struct LogicalOrder {
    catalog: OrderCatalog,
    project: OrderProject,
    /// Per source, per binding in declaration order, then by period start.
    entries: Vec<OrderEntry>,
    /// Per source, then by period start.
    row_keys: Vec<OrderRowKeys>,
    /// Every availability clip, also when nothing blocks.
    clips: Vec<OrderClip>,
    /// The non-blocking findings (level `warning` or `info`).
    findings: Vec<Issue>,
}

/// The catalog generation an order was checked against (`meta`).
#[derive(Serialize, ToSchema)]
pub struct OrderCatalog {
    generation_id: String,
    catalog_version: String,
    schema_version: String,
}

/// The project an order was checked for.
#[derive(Serialize, ToSchema)]
pub struct OrderProject {
    name: String,
    schema_version: String,
    reg_meta_version: String,
    /// `reg_core::project::project_hash` of the document.
    project_hash: String,
}

/// One merged segment of one binding in one table.
#[derive(Serialize, ToSchema)]
pub struct OrderEntry {
    /// The project source's name.
    source: String,
    /// The binding's index in the source.
    binding: usize,
    provider: String,
    register: String,
    /// The table that serves the window.
    table: String,
    variable: String,
    /// As the binding states it.
    #[schema(required = true)]
    display_name: Option<String>,
    /// The clipped request over the merged segment; `{start: null, end: null}` for a
    /// timeless table.
    period: Period,
    /// The merged segment's exact spelling.
    spelling: String,
    #[serde(rename = "type")]
    #[schema(required = true)]
    dtype: Option<String>,
    #[schema(required = true)]
    length: Option<i64>,
    #[schema(required = true)]
    value_set: Option<OrderValueSet>,
    /// A class version's public id.
    #[schema(required = true)]
    classification: Option<String>,
    /// Only when the source names one.
    #[serde(skip_serializing_if = "Option::is_none")]
    edition: Option<String>,
    /// Only when the source names one.
    #[serde(skip_serializing_if = "Option::is_none")]
    qualifier: Option<String>,
}

/// An entry's value set by its content hash.
#[derive(Serialize, ToSchema)]
pub struct OrderValueSet {
    hash: String,
    n_items: i64,
}

/// A serving table's row-key columns over a period.
#[derive(Serialize, ToSchema)]
pub struct OrderRowKeys {
    source: String,
    table: String,
    period: Period,
    columns: Vec<RowKeyColumn>,
}

/// A row-key column: its variable and the spelling it is delivered under.
#[derive(Serialize, ToSchema)]
pub struct RowKeyColumn {
    variable: String,
    spelling: String,
}

/// An availability clip of one binding.
#[derive(Serialize, ToSchema)]
pub struct OrderClip {
    source: String,
    variable: String,
    requested: Vec<Period>,
    ordered: Vec<Period>,
}

/// A resolved table with what the check reads of it.
struct Table {
    id: i64,
    pid: String,
    register: String,
    provider: String,
    timeless: bool,
    /// The end of its coverage hull (a day number).
    end: i64,
}

/// A table that serves part of a source: its window (inclusive ISO days) and the
/// requested days in it.
struct Serving {
    table: Table,
    requested: Vec<Interval>,
}

/// An undated table succession the request would need: (successor pid, continued
/// table pid, the requested days past the continued table's coverage end).
type Undated = (String, String, Interval);

/// What one source contributes.
#[derive(Default)]
struct Checked {
    findings: Vec<Issue>,
    entries: Vec<OrderEntry>,
    row_keys: Vec<OrderRowKeys>,
    clips: Vec<OrderClip>,
    /// The serving tables, when the table and period resolved (for the panels).
    served: Option<Vec<Serving>>,
}

/// The order check of a structurally valid project: the logical order with its
/// non-blocking findings, or every finding (the blocking ones at level `error`) when
/// something blocks.
///
/// # Errors
///
/// `internal_error` when a catalog query fails; never for a finding.
pub fn check(
    server: &Server,
    project: &ProjectData,
) -> Result<Result<LogicalOrder, Vec<Issue>>, Error> {
    let conn = server.catalog.connect()?;
    let mut findings = Vec::new();
    let mut entries = Vec::new();
    let mut row_keys = Vec::new();
    let mut clips = Vec::new();
    let mut members = Vec::new();
    for (index, source) in project.sources.iter().enumerate() {
        let checked = check_source(server, &conn, index, source)?;
        findings.extend(checked.findings);
        entries.extend(checked.entries);
        row_keys.extend(checked.row_keys);
        clips.extend(checked.clips);
        members.push((source.name.as_str(), checked.served));
    }
    for (index, panel) in project.panels.iter().enumerate() {
        findings.extend(rows::panel(&conn, index, panel, &members)?);
    }
    if findings.iter().any(Issue::blocks) {
        return Ok(Err(findings));
    }
    Ok(Ok(LogicalOrder {
        catalog: OrderCatalog {
            generation_id: server.catalog.generation().to_owned(),
            catalog_version: server.catalog.meta("catalog_version").to_owned(),
            schema_version: server.catalog.meta("schema_version").to_owned(),
        },
        project: OrderProject {
            name: project.name.clone(),
            schema_version: project.schema_version.clone(),
            reg_meta_version: project.reg_meta_version.clone(),
            project_hash: reg_core::project::project_hash(project),
        },
        entries,
        row_keys,
        clips,
        findings,
    }))
}

/// One source: its own findings, then each binding's.
fn check_source(
    server: &Server,
    conn: &Connection,
    index: usize,
    source: &Source,
) -> Result<Checked, Error> {
    let at = |field: &str| format!("/sources/{index}/{field}");
    let issue = |level, code: &str, path: String, message: String| Issue {
        level,
        code: code.to_owned(),
        path,
        message,
        source: Some(source.name.clone()),
        variable: None,
        table: None,
        period: None,
        successor_fqid: None,
    };
    let mut own = Vec::new();
    let table = match table_ref(conn, &source.table)? {
        Ok(table) => Some(table),
        Err((code, message)) => {
            own.push(issue(IssueLevel::Error, code, at("table"), message));
            None
        }
    };
    let table_pid = table.as_ref().map(|t| t.pid.clone());
    let located = |mut issue: Issue| {
        issue.table.clone_from(&table_pid);
        issue
    };
    let requested = match requested(server, source, table.as_ref()) {
        Ok(days) => days,
        Err((code, message)) => {
            own.push(located(issue(
                IssueLevel::Error,
                code,
                at("period"),
                message,
            )));
            None
        }
    };
    let mut checked = Checked::default();
    let (Some(table), Some(requested)) = (table, requested) else {
        // Every finding at once: the bindings' ids still resolve.
        own.sort_by_key(Issue::key);
        checked.findings = own;
        for (b, binding) in source.bindings.iter().enumerate() {
            let mut found = window::refs(conn, index, b, source, binding, table_pid.as_deref())?.1;
            found.sort_by_key(Issue::key);
            checked.findings.extend(found);
        }
        return Ok(checked);
    };
    let (serving, undated) = serving(server, conn, table, &requested)?;
    for (successor, continued, past) in undated {
        let mut finding = issue(
            IssueLevel::Info,
            "table_succession_undated",
            format!("/sources/{index}"),
            format!(
                "{continued} is continued by {successor} on no stated day, so {} is not \
                 followed into {successor}",
                span(server, &past)
            ),
        );
        finding.table = Some(continued);
        finding.period = Some(period(server, &past));
        finding.successor_fqid = Some(successor);
        own.push(finding);
    }
    own.extend(
        editions(conn, index, source, &serving)?
            .into_iter()
            .map(|(code, path, message)| located(issue(IssueLevel::Error, code, path, message))),
    );
    own.sort_by_key(Issue::key);
    checked.findings = own;
    let table_pid = table_pid.unwrap_or_default();
    let scope = window::Scope {
        server,
        conn,
        index,
        source,
        table: &table_pid,
        serving: &serving,
        requested: &requested,
    };
    for (b, binding) in source.bindings.iter().enumerate() {
        let outcome = window::binding(&scope, b, binding)?;
        let mut found = outcome.findings;
        found.sort_by_key(Issue::key);
        checked.findings.extend(found);
        checked.entries.extend(outcome.entries);
        checked.clips.extend(outcome.clip);
    }
    checked.row_keys = rows::row_keys(server, conn, source, &serving)?;
    checked.served = Some(serving);
    Ok(checked)
}

/// The requested days (`None` when the table did not resolve), or the finding that
/// refuses the period: `period_not_orderable`, or `_default` anywhere but on a timeless
/// table and a dated period on one.
fn requested(
    server: &Server,
    source: &Source,
    table: Option<&Table>,
) -> Result<Option<Vec<Interval>>, (&'static str, String)> {
    let intervals = source.period.intervals().map_err(|reason| {
        (
            "period_not_orderable",
            format!("The period is not orderable: {reason}."),
        )
    })?;
    match (table, intervals) {
        (None, _) => Ok(None),
        (Some(t), None) if !t.timeless => Err((
            "period_default_on_dated_table",
            format!(
                "\"_default\" is a timeless table's period; {} is dated.",
                t.pid
            ),
        )),
        (Some(t), Some(_)) if t.timeless => Err((
            "period_on_timeless_table",
            format!("{} is timeless; its period is \"_default\".", t.pid),
        )),
        (Some(_), None) => Ok(Some(vec![(
            iso_day(server.open.0),
            iso_day(server.open.1 - 1),
        )])),
        (Some(_), Some(intervals)) => Ok(Some(intervals)),
    }
}

/// The table a source names, or the finding that refuses it: `fqid_unresolved`,
/// `retired_ref` ("retired in <version>") or `table_is_register` listing the tables.
fn table_ref(
    conn: &Connection,
    value: &str,
) -> Result<Result<Table, (&'static str, String)>, Error> {
    let id = match refs::table(conn, value) {
        Ok(id) => id,
        Err(err) => return refused(conn, value, "table", &err).map(Err),
    };
    let table = conn.query_row(
        "SELECT t.pid, r.pid, p.pid, t.timeless, t.end
         FROM tbl t JOIN register r ON r.id = t.register_id JOIN provider p ON p.id = r.provider_id
         WHERE t.id = ?",
        [id],
        |row| {
            Ok(Table {
                id,
                pid: row.get(0)?,
                register: row.get(1)?,
                provider: row.get(2)?,
                timeless: row.get(3)?,
                end: row.get(4)?,
            })
        },
    )?;
    Ok(Ok(table))
}

/// The finding for a ref `refs` refused, naming what was expected (`what`); a catalog
/// failure stays an error.
fn refused(
    conn: &Connection,
    value: &str,
    what: &str,
    err: &Error,
) -> Result<(&'static str, String), Error> {
    match err.kind() {
        Code::TableIsRegister => {
            let tables: Vec<String> = conn
                .prepare(
                    "SELECT t.pid FROM tbl t JOIN register r ON r.id = t.register_id
                     WHERE r.pid = ? ORDER BY t.pid",
                )?
                .query_map([value], |row| row.get(0))?
                .collect::<rusqlite::Result<_>>()?;
            Ok((
                "table_is_register",
                format!(
                    "{value} is a register; name one of its tables: {}.",
                    tables.join(", ")
                ),
            ))
        }
        Code::RetiredRef => {
            let retired_in: String = conn.query_row(
                "SELECT retired_in FROM ident_retired WHERE pid = ?",
                [value],
                |row| row.get(0),
            )?;
            Ok((
                "retired_ref",
                format!("{value} was retired in catalog {retired_in}."),
            ))
        }
        Code::NotFound | Code::InvalidRef | Code::AmbiguousRef => Ok((
            "fqid_unresolved",
            format!("{value} names no {what} in the catalog."),
        )),
        _ => Err(Error::new(
            Code::InternalError,
            format!("Resolving {value:?} failed."),
            vec![],
        )),
    }
}

/// The tables that serve `requested` from `table`: the table itself, then its dated
/// `continues` successors, followed transitively; each day goes to the table whose
/// `at_start` window holds it. Also each undated succession whose successor the request
/// would need (it reaches past the table's coverage end inside the table's window), as
/// [`Undated`]; it is not followed (S5): no day is inferred from coverage (v4 §2.1).
fn serving(
    server: &Server,
    conn: &Connection,
    table: Table,
    requested: &[Interval],
) -> Result<(Vec<Serving>, Vec<Undated>), Error> {
    let mut chain = vec![(server.open.0, table)];
    let mut seen = BTreeSet::from([chain[0].1.id]);
    let mut undated = Vec::new();
    let mut next = 0;
    let mut successors = conn.prepare(
        "SELECT r.subject_id, r.at_start, t.pid FROM relation r JOIN tbl t ON t.id = r.subject_id
         WHERE r.kind = 'continues' AND r.subject_kind = 'table' AND r.object_kind = 'table'
           AND r.object_id = ?
         ORDER BY r.at_start IS NULL, r.at_start, t.pid",
    )?;
    while next < chain.len() {
        let rows: Vec<(i64, Option<i64>, String)> = successors
            .query_map([chain[next].1.id], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .collect::<rusqlite::Result<_>>()?;
        for (subject, at_start, pid) in rows {
            match at_start {
                None => undated.push((chain[next].1.id, pid)),
                Some(at) if seen.insert(subject) => {
                    if let Ok(Ok(successor)) = table_ref(conn, &pid) {
                        chain.push((at, successor));
                    }
                }
                Some(_) => {}
            }
        }
        next += 1;
    }
    chain.sort_by_key(|(from, _)| *from);
    let ends: Vec<i64> = chain
        .iter()
        .skip(1)
        .map(|(from, _)| *from)
        .chain([server.open.1])
        .collect();
    let mut serving = Vec::new();
    let mut gone = Vec::new();
    for ((from, table), to) in chain.into_iter().zip(ends) {
        if from >= to {
            continue;
        }
        let window = (iso_day(from), iso_day(to - 1));
        for (object, successor) in &undated {
            if *object == table.id && table.end < to {
                let past = overlap(
                    requested,
                    &[(iso_day(table.end.max(from)), window.1.clone())],
                );
                if let (Some(first), Some(last)) = (past.first(), past.last()) {
                    let past = (first.0.clone(), last.1.clone());
                    gone.push((successor.clone(), table.pid.clone(), past));
                }
            }
        }
        let part = overlap(requested, &[window]);
        if !part.is_empty() {
            serving.push(Serving {
                table,
                requested: part,
            });
        }
    }
    Ok((serving, gone))
}

/// `edition_not_found` per source period: the named edition, or qualifier, has no
/// release of a serving table in the source's period (`(code, path, message)`).
fn editions(
    conn: &Connection,
    index: usize,
    source: &Source,
    serving: &[Serving],
) -> Result<Vec<(&'static str, String, String)>, Error> {
    let mut found = Vec::new();
    let table = serving.first().map_or("", |s| s.table.pid.as_str());
    let exists = |column: &str, value: &str| -> Result<bool, Error> {
        for s in serving {
            for (lo, hi) in &s.requested {
                let hit: Option<i64> = conn
                    .query_row(
                        &format!(
                            "SELECT 1 FROM release WHERE table_id = ?1 AND {column} = ?2
                               AND start IS NOT NULL AND start <= ?3 AND end > ?4 LIMIT 1"
                        ),
                        rusqlite::params![s.table.id, value, day(hi), day(lo)],
                        |row| row.get(0),
                    )
                    .optional()?;
                if hit.is_some() {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    };
    if let Some(edition) = source.edition
        && !exists("edition", edition.as_str())?
    {
        found.push((
            "edition_not_found",
            format!("/sources/{index}/edition"),
            format!(
                "{table} has no {} release in the source's period.",
                edition.as_str()
            ),
        ));
    }
    if let Some(qualifier) = &source.qualifier
        && !exists("qualifier", qualifier)?
    {
        found.push((
            "edition_not_found",
            format!("/sources/{index}/qualifier"),
            format!("{table} has no release with qualifier {qualifier:?} in the source's period."),
        ));
    }
    Ok(found)
}

/// The day number of an ISO day.
fn day(iso: &str) -> i64 {
    reg_core::iso_day_number(iso).expect("an ISO day")
}

/// The inclusive ISO interval of the day numbers `[start, end)`.
fn interval(start: i64, end: i64) -> Interval {
    (iso_day(start), iso_day(end - 1))
}

/// A result period of an inclusive ISO interval.
fn period(server: &Server, (lo, hi): &Interval) -> Period {
    server.period(day(lo), day(hi) + 1)
}

/// A period as messages write it, `[2019-01-01, 2020-01-01)`, `open` for a sentinel.
fn span(server: &Server, interval: &Interval) -> String {
    let p = period(server, interval);
    format!(
        "[{}, {})",
        p.start.as_deref().unwrap_or("open"),
        p.end.as_deref().unwrap_or("open")
    )
}
