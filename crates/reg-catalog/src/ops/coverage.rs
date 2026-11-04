//! `coverage` (schema.md §2.1, "Coverage"): for a register, a table or a variable, the
//! periods it is delivered over, the gaps inside them and, per table, the releases.

use std::collections::BTreeMap;

use rusqlite::Connection;
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

use super::shape::{self, Release};
use super::{Cache, Operation, Param, Params, Period, Server, Type, component, refs};
use crate::{Error, rows};

pub const OPERATIONS: &[Operation] = &[Operation {
    name: "coverage",
    paths: &["/api/coverage/{ref}"],
    tool: Some("coverage"),
    description: "The periods a register, a table or a variable is delivered over: per \
        table its spans, gaps with their reasons and releases.",
    params: &[Param::required("ref", Type::Ref)],
    cache: Cache::Minute,
    run: coverage,
    result: component::<Coverage>,
}];

/// `shape.Coverage`.
#[derive(Serialize, ToSchema)]
pub struct Coverage {
    fqid: String,
    kind: String,
    /// The entity's hull.
    period: Period,
    tables: Vec<CoverageTable>,
}

#[derive(Serialize, ToSchema)]
pub struct CoverageTable {
    fqid: String,
    name: String,
    spans: Vec<Period>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    gaps: Vec<CoverageGap>,
    /// Absent for a variable ref (releases belong to tables) and in steward mode.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    releases: Vec<Release>,
}

#[derive(Serialize, ToSchema)]
pub struct CoverageGap {
    period: Period,
    reason: String,
}

/// `[start, end)` day numbers.
type Span = (i64, i64);

/// `spans` (in start order) with those that touch or overlap merged.
fn merge(spans: impl IntoIterator<Item = Span>) -> Vec<Span> {
    let mut out: Vec<Span> = Vec::new();
    for (start, end) in spans {
        match out.last_mut() {
            Some(last) if start <= last.1 => last.1 = last.1.max(end),
            _ => out.push((start, end)),
        }
    }
    out
}

fn periods(server: &Server, spans: &[Span]) -> Vec<Period> {
    spans.iter().map(|&(s, e)| server.period(s, e)).collect()
}

/// The `(start, end)` rows of `sql` for `params`, in the query's order.
fn spans(conn: &Connection, sql: &str, params: impl rusqlite::Params) -> Result<Vec<Span>, Error> {
    Ok(rows(conn, sql, params, |row| {
        Ok((row.get(0)?, row.get(1)?))
    })?)
}

fn gaps(
    server: &Server,
    conn: &Connection,
    sql: &str,
    params: impl rusqlite::Params,
) -> Result<Vec<CoverageGap>, Error> {
    Ok(rows(conn, sql, params, |row| {
        Ok(CoverageGap {
            period: server.period(row.get(0)?, row.get(1)?),
            reason: row.get(2)?,
        })
    })?)
}

fn coverage(server: &Server, params: &Params) -> Result<Value, Error> {
    let value = params["ref"];
    let conn = server.catalog.connect_for_ref(params["ref"])?;
    let hit = refs::resolve(&conn, value)?;
    if let Some(extra) = server.catalog.connect_holdings()? {
        return held(server, &conn, &extra, hit);
    }
    let (hull_sql, tables_sql) = match hit.kind.as_str() {
        "table" => (
            "SELECT start, end FROM tbl WHERE id = ?",
            "SELECT t.id, t.pid, x.body FROM tbl t JOIN text x ON x.id = t.name_text
             WHERE t.id = ?",
        ),
        "register" => (
            "SELECT start, end FROM register WHERE id = ?",
            "SELECT t.id, t.pid, x.body FROM tbl t JOIN text x ON x.id = t.name_text
             WHERE t.register_id = ? ORDER BY t.pid",
        ),
        "variable" => (
            "SELECT start, end FROM variable WHERE id = ?",
            "SELECT DISTINCT t.id, t.pid, x.body
             FROM segment s JOIN tbl t ON t.id = s.table_id JOIN text x ON x.id = t.name_text
             WHERE s.variable_id = ? ORDER BY t.pid",
        ),
        kind => {
            return Err(Error::invalid_ref(
                value,
                format!("{value:?} is a {kind}, which coverage does not take."),
            ));
        }
    };
    let hull: Span = conn.query_row(hull_sql, [hit.id], |row| Ok((row.get(0)?, row.get(1)?)))?;
    let rows: Vec<(i64, String, String)> = rows(&conn, tables_sql, [hit.id], |row| {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
    })?;
    let variable = hit.kind == "variable";
    let mut tables = Vec::new();
    for (id, fqid, name) in rows {
        let (table_spans, table_gaps, releases) = if variable {
            (
                merge(spans(
                    &conn,
                    "SELECT start, end FROM segment
                     WHERE variable_id = ? AND table_id = ? ORDER BY start",
                    [hit.id, id],
                )?),
                gaps(
                    server,
                    &conn,
                    "SELECT start, end, reason FROM gap
                     WHERE subject_kind = 'variable' AND subject_id = ? AND table_id = ?
                     ORDER BY start",
                    [hit.id, id],
                )?,
                vec![],
            )
        } else {
            (
                spans(
                    &conn,
                    "SELECT start, end FROM tbl_span WHERE table_id = ? ORDER BY start",
                    [id],
                )?,
                gaps(
                    server,
                    &conn,
                    "SELECT start, end, reason FROM gap
                     WHERE subject_kind = 'table' AND subject_id = ? ORDER BY start",
                    [id],
                )?,
                shape::releases(server, &conn, id)?,
            )
        };
        tables.push(CoverageTable {
            fqid,
            name,
            spans: periods(server, &table_spans),
            gaps: table_gaps,
            releases,
        });
    }
    Ok(serde_json::to_value(Coverage {
        fqid: hit.pid,
        kind: hit.kind,
        period: server.period(hull.0, hull.1),
        tables,
    })
    .expect("Coverage serializes"))
}

/// Steward mode: the years the steward holds, per table, from its placements; the
/// catalog's wider spans, gaps and releases are not the steward's (maintainer
/// 2026-11-04). Placements are matched by public ids, never by a row id of `conn`.
fn held(
    server: &Server,
    conn: &Connection,
    extra: &Connection,
    hit: refs::Entity,
) -> Result<Value, Error> {
    let filter = match hit.kind.as_str() {
        "variable" => "m.variable_pid = ?1",
        "table" => "m.table_pid = ?1",
        "register" => "(m.table_pid = ?1 OR substr(m.table_pid, 1, length(?1) + 1) = ?1 || '/')",
        kind => {
            return Err(Error::invalid_ref(
                &hit.pid,
                format!("{:?} is a {kind}, which coverage does not take.", hit.pid),
            ));
        }
    };
    let placements: Vec<(String, Span)> = rows(
        extra,
        &format!(
            "SELECT m.table_pid, m.start, m.end FROM holding_mapping m
             WHERE {filter} ORDER BY m.table_pid, m.start, m.end"
        ),
        [&hit.pid],
        |row| Ok((row.get(0)?, (row.get(1)?, row.get(2)?))),
    )?;
    let mut by_table: BTreeMap<String, Vec<Span>> = BTreeMap::new();
    for (table, span) in placements {
        by_table.entry(table).or_default().push(span);
    }
    let name = |pid: &str| -> Result<String, Error> {
        let sql = "SELECT x.body FROM tbl t JOIN text x ON x.id = t.name_text WHERE t.pid = ?";
        match conn.query_row(sql, [pid], |row| row.get(0)) {
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                Ok(extra.query_row(sql, [pid], |row| row.get(0))?)
            }
            other => Ok(other?),
        }
    };
    let mut hull: Option<Span> = None;
    let mut tables = Vec::with_capacity(by_table.len());
    for (fqid, spans) in by_table {
        let spans = merge(spans);
        for &(start, end) in &spans {
            hull = Some(hull.map_or((start, end), |(s, e)| (s.min(start), e.max(end))));
        }
        tables.push(CoverageTable {
            name: name(&fqid)?,
            fqid,
            spans: periods(server, &spans),
            gaps: vec![],
            releases: vec![],
        });
    }
    let (start, end) = hull.ok_or_else(|| {
        Error::new(
            crate::Code::NotFound,
            format!("Nothing is named {:?}.", hit.pid),
            vec![hit.pid.clone().into()],
        )
    })?;
    Ok(serde_json::to_value(Coverage {
        fqid: hit.pid,
        kind: hit.kind,
        period: server.period(start, end),
        tables,
    })
    .expect("Coverage serializes"))
}
