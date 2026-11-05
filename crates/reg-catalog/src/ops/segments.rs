//! `segments`: a variable's segments, the reader's main row (schema §1.2), with each
//! segment's occurrences when `evidence` is asked for.

use std::collections::HashSet;

use rusqlite::{Connection, OptionalExtension};
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

use super::shape::{self, Release, ValueSetRef};
use super::show_evidence;
use super::{
    Cache, Operation, Param, Params, Period, Server, Type, component, cursor, days, flag, limit,
    refs,
};
use crate::{Error, rows};

pub const OPERATIONS: &[Operation] = &[Operation {
    name: "segments",
    paths: &["/api/segments/{ref}"],
    tool: Some("segments"),
    description: "A variable's columns over time: per table, column name and period one row, in \
        time order, with the provider's wording and the form (type, value set, flags) where \
        stated. A change of form or wording starts a new row. On a data holder's catalog, also \
        its columns and files over the years it holds. A table ref with `variable` (up to \
        200 variables) lists those variables' rows in that table, in one list. \
        `evidence=true` adds the deliveries behind each row and the curated values that \
        replaced the provider's.",
    params: &[
        Param::required("ref", Type::Ref),
        Param::optional("variable", Type::Strings),
        Param::optional("period", Type::Period),
        Param::optional("table", Type::Ref),
        Param::optional("value_set_name", Type::String),
        Param::optional("evidence", Type::Boolean),
        Param::optional("limit", Type::Limit),
        Param::optional("cursor", Type::Cursor),
    ],
    cache: Cache::Minute,
    run: segments,
    result: component::<SegmentPage>,
}];

#[derive(Serialize, ToSchema)]
pub struct SegmentPage {
    items: Vec<Segment>,
    #[schema(required = true)]
    next_cursor: Option<String>,
}

/// One row of the variable's per-period list: a column of one table over one period
/// with one form and one provider wording. A fact the catalog does not state is left
/// out, never `null`: no type, no value set, `coded` unknown, and `state` clear (the
/// default).
#[derive(Clone, Serialize, ToSchema)]
pub struct Segment {
    /// The variable's fqid.
    variable: String,
    /// The table's fqid.
    table: String,
    /// The table's name, as `show` gives it.
    table_name: String,
    /// The column name it is delivered under; on a data holder's catalog, the holder's column.
    column: String,
    period: Period,
    /// On a data holder's catalog, the file the holder has the column in.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    file: Option<String>,
    /// The provider's definition over this period; a change of wording starts a row.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    wording: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    dtype: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    length: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    value_set: Option<ValueSetRef>,
    /// A classification version's ref.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    classification: Option<String>,
    /// `yes` or `no`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    coded: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    sensitive: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    identifier: Option<bool>,
    role: String,
    /// Present only when the column is ambiguous; absent means clear.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    state: Option<String>,
    /// Present only when `evidence` is asked for.
    #[serde(skip_serializing_if = "Option::is_none")]
    evidence: Option<Vec<Occurrence>>,
}

/// One delivery behind this row.
#[derive(Clone, Serialize, ToSchema)]
pub struct Occurrence {
    #[schema(required = true)]
    release: Option<Release>,
    period: Period,
    spelling: String,
    #[schema(required = true)]
    label: Option<String>,
    source_key: String,
    status: String,
    locators: Vec<Locator>,
    overrides: Vec<Override>,
}

#[derive(Clone, Serialize, ToSchema)]
pub struct Locator {
    pub(super) input: String,
    pub(super) locator: String,
}

/// A curated value that replaced the provider's.
#[derive(Clone, Serialize, ToSchema)]
pub struct Override {
    field: String,
    #[schema(required = true)]
    value: Option<String>,
    prov: String,
    /// Where the curated value is written.
    #[schema(required = true)]
    statement: Option<StatementLocation>,
    #[schema(required = true)]
    rule: Option<String>,
    #[schema(required = true)]
    source_value: Option<String>,
}

#[derive(Clone, Serialize, ToSchema)]
pub struct StatementLocation {
    pub(super) file: String,
    pub(super) line: i64,
}

fn segments(server: &Server, params: &Params) -> Result<Value, Error> {
    let conn = server.catalog.connect_for_ref(params["ref"])?;
    let given = params["ref"];
    let hit = refs::resolve(&conn, given)?;
    let named = params.list("variable");
    // A variable ref is its own list; a table ref lists the named variables' rows in
    // that table, so a project's table reads its columns in one request (#2232).
    let (variables, table) = match (hit.kind.as_str(), named.is_empty()) {
        ("variable", true) => {
            let table = table_pid(&conn, params, &hit)?;
            (vec![hit.pid], table)
        }
        // A variable ref names its variable; a second list would contradict it.
        ("variable", false) => return Err(Error::invalid_parameter("variable")),
        (_, false) => {
            // The ref is the table: a `table` narrowing could only contradict it.
            if params.get("table").is_some() {
                return Err(Error::invalid_parameter("table"));
            }
            let id = refs::table(&conn, given)?;
            let pid: String =
                conn.query_row("SELECT pid FROM tbl WHERE id = ?", [id], |row| row.get(0))?;
            (variable_pids(server, named)?, Some(pid))
        }
        (kind, true) => {
            return Err(Error::invalid_ref(
                given,
                format!("{given:?} is a {kind}, not a variable."),
            ));
        }
    };
    let evidence = flag(params, "evidence")?;
    let limit = limit(params)?;
    let offset = match params.get("cursor") {
        Some(&token) => {
            cursor::decode(
                token,
                server.catalog.generation(),
                &cursor::context(params),
                usize::MAX,
            )?
            .0
        }
        None => 0,
    };
    // simplify: the whole list is built, then paged; a variable has tens of rows and
    // a table read at most 200 variables' rows in one table. Page in SQL again if a
    // list reaches thousands.
    let origins = origins(server, conn)?;
    let value_set_name = params.get("value_set_name").copied();
    let mut rows = Vec::new();
    for variable in &variables {
        rows.extend(listed(
            &origins,
            variable,
            table.as_deref(),
            value_set_name,
        )?);
    }
    let mut rows: Vec<Row> = match days(params, "period")? {
        Some((first, end)) => rows
            .into_iter()
            .filter(|row| row.start < end && first < row.end)
            .collect(),
        None => rows,
    };
    sort(&mut rows);
    let mut page: Vec<Row> = rows.into_iter().skip(offset).take(limit + 1).collect();
    let next_cursor = cursor::next(&mut page, limit, |_| {
        Some(cursor::encode(
            server.catalog.generation(),
            &cursor::context(params),
            offset + limit,
            "",
        ))
    });
    let items = served(server, &origins, page, evidence)?;
    Ok(serde_json::to_value(SegmentPage { items, next_cursor }).expect("a page serializes"))
}

/// The origins a variable's list reads: `catalog` (the catalog's own connection), or on
/// a data holder's catalog the catalog and its holdings.
pub(super) fn origins(server: &Server, catalog: Connection) -> Result<Vec<Connection>, Error> {
    Ok(match server.catalog.connect_holdings()? {
        Some(extra) => vec![server.catalog.connect()?, extra],
        None => vec![catalog],
    })
}

/// A variable's whole list as `segments` serves it unnarrowed: every row, unpaged, in
/// the same order and shape, without evidence.
pub(super) fn all(
    server: &Server,
    origins: &[Connection],
    variable: &str,
) -> Result<Vec<Segment>, Error> {
    let mut rows = listed(origins, variable, None, None)?;
    sort(&mut rows);
    served(server, origins, rows, false)
}

/// The variables `values` names, each resolved in its own origin (as a ref is), in
/// pid order without repeats.
fn variable_pids(server: &Server, values: &[&str]) -> Result<Vec<String>, Error> {
    // simplify: a connection per named variable (up to 200); resolve against the
    // open origins if table reads show in timing.
    let mut pids = values
        .iter()
        .map(|&value| {
            let hit = refs::resolve(&server.catalog.connect_for_ref(value)?, value)?;
            if hit.kind != "variable" {
                return Err(Error::invalid_ref(
                    value,
                    format!("{value:?} is a {}, not a variable.", hit.kind),
                ));
            }
            Ok(hit.pid)
        })
        .collect::<Result<Vec<_>, Error>>()?;
    pids.sort_unstable();
    pids.dedup();
    Ok(pids)
}

/// The list's order: by start day, then table, column, file and variable (a table
/// read's variables can share the rest).
fn sort(rows: &mut [Row]) {
    rows.sort_by(|a, b| {
        (
            a.start,
            &a.segment.table,
            &a.segment.column,
            &a.segment.file,
            &a.segment.variable,
        )
            .cmp(&(
                b.start,
                &b.segment.table,
                &b.segment.column,
                &b.segment.file,
                &b.segment.variable,
            ))
    });
}

/// The rows as served: each with its period and value set, and its deliveries when
/// `evidence` is asked for.
fn served(
    server: &Server,
    origins: &[Connection],
    rows: Vec<Row>,
    evidence: bool,
) -> Result<Vec<Segment>, Error> {
    let names = refs::names(server, rows.iter().map(|row| row.segment.table.as_str()))?;
    let mut items = Vec::with_capacity(rows.len());
    for Row {
        id,
        origin,
        valueset_id,
        start,
        end,
        mut segment,
    } in rows
    {
        let conn = &origins[origin];
        segment.table_name = names.of(refs::Named::Table, &segment.table)?;
        segment.period = server.period(start, end);
        segment.value_set = valueset_id
            .map(|valueset| shape::value_set_ref(conn, valueset))
            .transpose()?;
        segment.evidence = match (evidence, id) {
            (true, Some(id)) => Some(occurrences(server, conn, id)?),
            (true, None) => Some(vec![]),
            (false, _) => None,
        };
        items.push(segment);
    }
    Ok(items)
}

/// The variable's rows over every origin, before the `period` filter and paging. A
/// steward's supplemental occurrences of a reference variable are its forms in the
/// holdings origin under the same public id, so both are read and each row keeps its
/// origin; in steward mode the rows are then the held placements.
fn listed(
    origins: &[Connection],
    variable: &str,
    table: Option<&str>,
    value_set_name: Option<&str>,
) -> Result<Vec<Row>, Error> {
    let mut rows = Vec::new();
    let mut runs = Vec::new();
    let mut named = HashSet::new();
    for (origin, conn) in origins.iter().enumerate() {
        let Some(id) = conn
            .query_row("SELECT id FROM variable WHERE pid = ?", [variable], |row| {
                row.get::<_, i64>(0)
            })
            .optional()?
        else {
            continue;
        };
        rows.extend(segment_rows(conn, origin, id, table)?);
        runs.extend(show_evidence::wordings(conn, id)?);
        if let Some(name) = value_set_name {
            named.extend(
                named_value_sets(conn, name)?
                    .into_iter()
                    .map(|v| (origin, v)),
            );
        }
    }
    runs.sort_by_key(|run| std::cmp::Reverse((run.start, run.end)));
    if let [_, extra] = origins {
        rows = held_rows(extra, variable, table, &rows)?;
    }
    if value_set_name.is_some() {
        rows.retain(|row| {
            row.valueset_id
                .is_some_and(|valueset| named.contains(&(row.origin, valueset)))
        });
    }
    for row in &mut rows {
        variable.clone_into(&mut row.segment.variable);
    }
    Ok(with_wording(&runs, rows))
}

/// The public id of the `table` narrowing, checked to be in the variable's register.
fn table_pid(
    conn: &Connection,
    params: &Params,
    variable: &refs::Entity,
) -> Result<Option<String>, Error> {
    let Some(&table) = params.get("table") else {
        return Ok(None);
    };
    let id = refs::table(conn, table)?;
    let register = |sql: &str, id: i64| conn.query_row(sql, [id], |row| row.get::<_, i64>(0));
    if register("SELECT register_id FROM tbl WHERE id = ?", id)?
        != register("SELECT register_id FROM variable WHERE id = ?", variable.id)?
    {
        let given = params["ref"];
        return Err(Error::invalid_ref(
            table,
            format!("{table:?} is not a table of {given:?}'s register."),
        ));
    }
    Ok(Some(conn.query_row(
        "SELECT pid FROM tbl WHERE id = ?",
        [id],
        |row| row.get(0),
    )?))
}

/// The value sets in `conn` named `name`.
fn named_value_sets(conn: &Connection, name: &str) -> Result<Vec<i64>, Error> {
    Ok(rows(
        conn,
        "SELECT n.valueset_id FROM valueset_name n JOIN text x ON x.id = n.name_text
         WHERE x.body = ?",
        [name],
        |row| row.get(0),
    )?)
}

/// One row of the list before paging: its segment (none for a steward row no
/// catalog segment describes), value set id and `[start, end)` days.
#[derive(Clone)]
struct Row {
    id: Option<i64>,
    /// Index of the database origin the row's ids belong to.
    origin: usize,
    valueset_id: Option<i64>,
    start: i64,
    end: i64,
    segment: Segment,
}

/// The variable's catalog segments in one origin, in `table` when given.
fn segment_rows(
    conn: &Connection,
    origin: usize,
    variable: i64,
    table: Option<&str>,
) -> Result<Vec<Row>, Error> {
    let sql = "SELECT s.id, t.pid, s.start, s.end, sp.exact, s.dtype, s.length, s.format,
                s.valueset_id, cv.pid, s.coded, s.sensitive, s.identifier,
                s.role, s.state
         FROM segment s
           JOIN tbl t ON t.id = s.table_id
           JOIN spelling sp ON sp.id = s.spelling_id
           LEFT JOIN class_version cv ON cv.id = s.class_version_id
         WHERE s.variable_id = ?1 AND (?2 IS NULL OR t.pid = ?2)";
    let stated = |value: String, unstated: &str| (value != unstated).then_some(value);
    Ok(rows(
        conn,
        sql,
        rusqlite::params![variable, table],
        |row| {
            Ok(Row {
                id: Some(row.get(0)?),
                origin,
                valueset_id: row.get(8)?,
                start: row.get(2)?,
                end: row.get(3)?,
                segment: Segment {
                    // Set by `listed`, which has the pid.
                    variable: String::new(),
                    table: row.get(1)?,
                    // Named per page in `segments`, after paging.
                    table_name: String::new(),
                    column: row.get(4)?,
                    period: Period {
                        start: None,
                        end: None,
                    },
                    file: None,
                    wording: None,
                    dtype: row.get(5)?,
                    length: row.get(6)?,
                    format: row.get(7)?,
                    value_set: None,
                    classification: row.get(9)?,
                    coded: stated(row.get(10)?, "unknown"),
                    sensitive: row.get(11)?,
                    identifier: row.get(12)?,
                    role: row.get(13)?,
                    state: stated(row.get(14)?, "clear"),
                    evidence: None,
                },
            })
        },
    )?)
}

/// Steward mode: one row per held file and column over the years held, never the
/// catalog's wider periods (maintainer 2026-11-04). A placement takes its facts from
/// the catalog segments it overlaps in its table, preferring the same column name
/// among those; held years no segment covers state only the role.
fn held_rows(
    extra: &Connection,
    variable: &str,
    table: Option<&str>,
    catalog: &[Row],
) -> Result<Vec<Row>, Error> {
    let placements = rows(
        extra,
        "SELECT m.table_pid, c.name, m.start, m.end, h.physical_name, m.role
         FROM holding_mapping m
           JOIN holding_column c ON c.id = m.column_id
           JOIN holding_table h ON h.id = c.holding_id
         WHERE m.variable_pid = ?1 AND (?2 IS NULL OR m.table_pid = ?2)",
        rusqlite::params![variable, table],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
            ))
        },
    )?;
    let mut out = Vec::new();
    for (table, column, start, end, file, role) in placements {
        let in_table: Vec<&Row> = catalog
            .iter()
            .filter(|r| r.segment.table == table)
            .collect();
        let same_name = |r: &&Row| r.segment.column.eq_ignore_ascii_case(&column);
        let overlapping: Vec<&Row> = in_table
            .iter()
            .copied()
            .filter(|r| r.start < end && start < r.end)
            .collect();
        // The same column name wins per interval: a segment named otherwise stays
        // where no same-named one overlaps it (a rename the file did not take).
        let candidates: Vec<&Row> = overlapping
            .iter()
            .copied()
            .filter(|r| {
                same_name(r)
                    || !overlapping
                        .iter()
                        .any(|o| same_name(o) && o.start < r.end && r.start < o.end)
            })
            .collect();
        let mut parts: Vec<Row> = candidates
            .iter()
            .map(|r| Row {
                start: r.start.max(start),
                end: r.end.min(end),
                ..(*r).clone()
            })
            .collect();
        // Held years no catalog segment covers state only the role: another
        // period's form would be a guess.
        parts.sort_by_key(|part| part.start);
        let covered: Vec<_> = parts.iter().map(|part| (part.start, part.end)).collect();
        let gaps = reg_core::days::minus(&[(start, end)], &covered);
        parts.extend(
            gaps.into_iter()
                .map(|(start, end)| bare(&table, &column, &role, start, end)),
        );
        for mut part in parts {
            part.segment.table.clone_from(&table);
            part.segment.column.clone_from(&column);
            part.segment.file = Some(file.clone());
            out.push(part);
        }
    }
    Ok(out)
}

/// A held row that states only its column and role.
fn bare(table: &str, column: &str, role: &str, start: i64, end: i64) -> Row {
    Row {
        id: None,
        origin: 0,
        valueset_id: None,
        start,
        end,
        segment: Segment {
            variable: String::new(),
            table: table.to_owned(),
            table_name: String::new(),
            column: column.to_owned(),
            period: Period {
                start: None,
                end: None,
            },
            file: None,
            wording: None,
            dtype: None,
            length: None,
            format: None,
            value_set: None,
            classification: None,
            coded: None,
            sensitive: None,
            identifier: None,
            role: role.to_owned(),
            state: None,
            evidence: None,
        },
    }
}

/// The rows cut where the provider's wording changes, each carrying the wording in
/// force: the latest used occurrence's in the row's table that states one, else (a
/// steward table the catalog has no occurrence in) any table's on that day.
fn with_wording(runs: &[show_evidence::Wording], list: Vec<Row>) -> Vec<Row> {
    let mut out = Vec::new();
    for row in list {
        let table = &row.segment.table;
        let own = runs.iter().any(|run| &run.table == table);
        let wording_at = |day: i64| {
            runs.iter()
                .filter(|run| !own || &run.table == table)
                .find(|run| run.start <= day && day < run.end && run.text.is_some())
                .and_then(|run| run.text.clone())
        };
        let mut cuts: Vec<i64> = runs
            .iter()
            .flat_map(|run| [run.start, run.end])
            .filter(|day| row.start < *day && *day < row.end)
            .collect();
        cuts.sort_unstable();
        cuts.dedup();
        let mut start = row.start;
        let mut wording = wording_at(start);
        for cut in cuts {
            let next = wording_at(cut);
            if next != wording {
                let mut part = row.clone();
                part.start = start;
                part.end = cut;
                part.segment.wording = wording;
                out.push(part);
                start = cut;
                wording = next;
            }
        }
        let mut part = row;
        part.start = start;
        part.segment.wording = wording;
        out.push(part);
    }
    out
}

/// A segment's occurrences, in time, then source key and spelling order.
fn occurrences(server: &Server, conn: &Connection, segment: i64) -> Result<Vec<Occurrence>, Error> {
    let rows = rows(
        conn,
        "SELECT o.id, o.release_id, o.start, o.end, sp.exact, l.body, o.source_key, o.status
             FROM occurrence o
               JOIN spelling sp ON sp.id = o.spelling_id
               LEFT JOIN text l ON l.id = o.label_text
             WHERE o.segment_id = ?
             ORDER BY o.start, o.source_key, sp.exact, o.id",
        [segment],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
            ))
        },
    )?;
    let mut locators = conn.prepare(
        "SELECT i.name, l.locator
         FROM occurrence_locator l JOIN input i ON i.id = l.input_id
         WHERE l.occurrence_id = ? ORDER BY i.name, l.locator",
    )?;
    let mut overrides = conn.prepare(
        "SELECT o.field, o.value, o.prov, o.rule_code, o.source_value, s.file, s.line
         FROM override o LEFT JOIN statement s ON s.id = o.statement_id
         WHERE o.subject_kind = 'occurrence' AND o.subject_id = ?
         ORDER BY o.field",
    )?;
    rows.into_iter()
        .map(
            |(id, release, start, end, spelling, label, source_key, status)| {
                Ok(Occurrence {
                    release: release
                        .map(|release| shape::release(server, conn, release))
                        .transpose()?,
                    period: server.period(start, end),
                    spelling,
                    label,
                    source_key,
                    status,
                    locators: locators
                        .query_map([id], |row| {
                            Ok(Locator {
                                input: row.get(0)?,
                                locator: row.get(1)?,
                            })
                        })?
                        .collect::<rusqlite::Result<_>>()?,
                    overrides: overrides
                        .query_map([id], |row| {
                            Ok(Override {
                                field: row.get(0)?,
                                value: row.get(1)?,
                                prov: row.get(2)?,
                                statement: row
                                    .get::<_, Option<String>>(5)?
                                    .zip(row.get::<_, Option<i64>>(6)?)
                                    .map(|(file, line)| StatementLocation { file, line }),
                                rule: row.get(3)?,
                                source_value: row.get(4)?,
                            })
                        })?
                        .collect::<rusqlite::Result<_>>()?,
                })
            },
        )
        .collect()
}
