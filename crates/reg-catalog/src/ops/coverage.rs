//! `coverage` (schema.md §2.1, "Coverage"): for a register, a table or a variable, the
//! periods it is delivered over, the gaps inside them and, per table, the releases.

use std::collections::BTreeMap;

use rusqlite::Connection;
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

use super::shape::{self, Release};
use super::{Cache, Operation, Param, Params, Period, Server, Type, component, refs};
use crate::{Code, Error};

pub const OPERATIONS: &[Operation] = &[Operation {
    name: "coverage",
    paths: &["/api/coverage/{ref}"],
    tool: Some("coverage"),
    description: "The periods a register, a table or a variable is delivered over: per \
        table its spans, gaps with their reasons and releases; for a variable also the \
        periods of each delivered column name.",
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
    /// A variable's folded spellings with the periods each was delivered under;
    /// `null` for a register or a table.
    #[schema(required = true)]
    columns: Option<Vec<CoverageColumn>>,
}

#[derive(Serialize, ToSchema)]
pub struct CoverageTable {
    fqid: String,
    name: String,
    spans: Vec<Period>,
    gaps: Vec<CoverageGap>,
    /// Empty for a variable ref: releases belong to tables.
    releases: Vec<Release>,
}

#[derive(Serialize, ToSchema)]
pub struct CoverageGap {
    period: Period,
    reason: String,
}

#[derive(Serialize, ToSchema)]
pub struct CoverageColumn {
    spelling: String,
    spans: Vec<Period>,
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
    Ok(conn
        .prepare(sql)?
        .query_map(params, |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?)
}

fn gaps(
    server: &Server,
    conn: &Connection,
    sql: &str,
    params: impl rusqlite::Params,
) -> Result<Vec<CoverageGap>, Error> {
    Ok(conn
        .prepare(sql)?
        .query_map(params, |row| {
            Ok(CoverageGap {
                period: server.period(row.get(0)?, row.get(1)?),
                reason: row.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?)
}

fn coverage(server: &Server, params: &Params) -> Result<Value, Error> {
    let value = params["ref"];
    let conn = server.catalog.connect()?;
    let hit = refs::resolve(&conn, value)?;
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
            return Err(Error::new(
                Code::InvalidRef,
                format!("{value:?} is a {kind}, which coverage does not take."),
                vec![value.into()],
            ));
        }
    };
    let hull: Span = conn.query_row(hull_sql, [hit.id], |row| Ok((row.get(0)?, row.get(1)?)))?;
    let rows: Vec<(i64, String, String)> = conn
        .prepare(tables_sql)?
        .query_map([hit.id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<rusqlite::Result<_>>()?;
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
    let columns = variable
        .then(|| columns(server, &conn, hit.id))
        .transpose()?;
    Ok(serde_json::to_value(Coverage {
        fqid: hit.pid,
        kind: hit.kind,
        period: server.period(hull.0, hull.1),
        tables,
        columns,
    })
    .expect("Coverage serializes"))
}

/// A variable's columns: per folded spelling, its segments in every table merged,
/// ordered by first span start, then spelling.
fn columns(
    server: &Server,
    conn: &Connection,
    variable_id: i64,
) -> Result<Vec<CoverageColumn>, Error> {
    let mut by_spelling: BTreeMap<String, Vec<Span>> = BTreeMap::new();
    let mut stmt = conn.prepare(
        "SELECT f.folded, s.start, s.end
         FROM segment s JOIN name_fold f ON f.id = s.fold_id
         WHERE s.variable_id = ? ORDER BY s.start",
    )?;
    let rows = stmt.query_map([variable_id], |row| {
        Ok((row.get::<_, String>(0)?, (row.get(1)?, row.get(2)?)))
    })?;
    for row in rows {
        let (spelling, span) = row?;
        by_spelling.entry(spelling).or_default().push(span);
    }
    let mut columns: Vec<(Vec<Span>, String)> = by_spelling
        .into_iter()
        .map(|(spelling, spans)| (merge(spans), spelling))
        .collect();
    columns.sort_by(|a, b| (a.0[0].0, &a.1).cmp(&(b.0[0].0, &b.1)));
    Ok(columns
        .into_iter()
        .map(|(spans, spelling)| CoverageColumn {
            spelling,
            spans: periods(server, &spans),
        })
        .collect())
}
