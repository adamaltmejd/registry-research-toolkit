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
    /// Accepted physical placements, absent in reference-only mode.
    #[serde(skip_serializing_if = "Option::is_none")]
    held: Option<Vec<HeldPlacement>>,
    /// Same canonical variable, documented in additional steward-attested periods.
    /// Reference spans above retain the global catalog's meaning.
    #[serde(skip_serializing_if = "Option::is_none")]
    steward_occurrences: Option<Vec<StewardOccurrence>>,
}

/// A source occurrence in the steward's supplemental semantics. Its identity
/// declaration and ordinary transcription locators both remain available.
#[derive(Serialize, ToSchema)]
pub struct StewardOccurrence {
    table: String,
    variable: String,
    period: Period,
    spelling: String,
    #[schema(required = true)]
    label: Option<String>,
    #[schema(required = true)]
    definition: Option<String>,
    #[serde(rename = "type")]
    #[schema(required = true)]
    dtype: Option<String>,
    coded: String,
    #[schema(required = true)]
    value_set: Option<shape::ValueSetRef>,
    locator: String,
    evidence: String,
    sources: Vec<super::segments::Locator>,
}

/// A physical placement's public semantic coordinates and held interval.
#[derive(Serialize, ToSchema)]
pub struct HeldPlacement {
    table: String,
    variable: String,
    physical_table: String,
    column: String,
    period: Period,
    role: String,
    carried: bool,
    #[schema(required = true)]
    caveat: Option<String>,
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
    let conn = server.catalog.connect_for_ref(params["ref"])?;
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
    let (held, steward_occurrences) = if let Some(extra) = server.catalog.connect_holdings()? {
        (
            Some(held(server, &extra, &hit.pid, &hit.kind, &tables)?),
            Some(reference_occurrences(server, &extra, &hit.pid, &hit.kind)?),
        )
    } else {
        (None, None)
    };
    Ok(serde_json::to_value(Coverage {
        fqid: hit.pid,
        kind: hit.kind,
        period: server.period(hull.0, hull.1),
        tables,
        columns,
        held,
        steward_occurrences: steward_occurrences.filter(|rows| !rows.is_empty()),
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

/// Query holdings by public ids, never by a row id from the semantic connection.
fn held(
    server: &Server,
    conn: &Connection,
    pid: &str,
    kind: &str,
    tables: &[CoverageTable],
) -> Result<Vec<HeldPlacement>, Error> {
    let mut out = Vec::new();
    let mut query = conn.prepare(
        "SELECT m.table_pid, m.variable_pid, t.physical_name, c.name, m.start, m.end,
                m.role, m.carried, m.caveat
         FROM holding_mapping m
           JOIN holding_column c ON c.id = m.column_id
           JOIN holding_table t ON t.id = c.holding_id
         WHERE (?1 IS NULL OR m.table_pid = ?1) AND (?2 IS NULL OR m.variable_pid = ?2)
         ORDER BY m.table_pid, m.variable_pid, m.start, m.end, t.physical_name, c.ordinal",
    )?;
    // A steward occurrence can attest a canonical variable in a table whose
    // reference metadata omits that membership; global coverage stays unchanged.
    let routes: Vec<_> = if kind == "variable" {
        vec![None]
    } else {
        tables.iter().map(|t| Some(t.fqid.as_str())).collect()
    };
    for table in routes {
        let variable = (kind == "variable").then_some(pid);
        let rows = query.query_map(rusqlite::params![table, variable], |r| {
            Ok(HeldPlacement {
                table: r.get(0)?,
                variable: r.get(1)?,
                physical_table: r.get(2)?,
                column: r.get(3)?,
                period: server.period(r.get(4)?, r.get(5)?),
                role: r.get(6)?,
                carried: r.get(7)?,
                caveat: r.get(8)?,
            })
        })?;
        out.extend(rows.collect::<rusqlite::Result<Vec<_>>>()?);
    }
    Ok(out)
}

/// Only qualified public ids select this origin; supplemental row ids stay local.
fn reference_occurrences(
    server: &Server,
    conn: &Connection,
    pid: &str,
    kind: &str,
) -> Result<Vec<StewardOccurrence>, Error> {
    let mut query = conn.prepare(
        "SELECT t.pid,v.pid,o.start,o.end,sp.exact,l.body,d.body,
       o.dtype,o.coded,o.valueset_id,x.locator,x.evidence,o.id
       FROM holding_reference_occurrence x JOIN tbl t ON t.pid=x.table_pid
       JOIN variable v ON v.pid=x.variable_pid JOIN register r ON r.id=v.register_id
       JOIN occurrence o ON o.variable_id=v.id AND o.table_id=t.id
       JOIN spelling sp ON sp.id=o.spelling_id
       LEFT JOIN text l ON l.id=o.label_text LEFT JOIN text d ON d.id=o.definition_text
       WHERE o.status='used' AND ((?2='variable' AND v.pid=?1)
         OR (?2='table' AND t.pid=?1) OR (?2='register' AND r.pid=?1))
       ORDER BY t.pid,v.pid,o.start,o.end,o.source_key",
    )?;
    let rows = query
        .query_map([pid, kind], |r| {
            Ok((
                StewardOccurrence {
                    table: r.get(0)?,
                    variable: r.get(1)?,
                    period: server.period(r.get(2)?, r.get(3)?),
                    spelling: r.get(4)?,
                    label: r.get(5)?,
                    definition: r.get(6)?,
                    dtype: r.get(7)?,
                    coded: r.get(8)?,
                    value_set: None,
                    locator: r.get(10)?,
                    evidence: r.get(11)?,
                    sources: vec![],
                },
                r.get::<_, Option<i64>>(9)?,
                r.get::<_, i64>(12)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut sources = conn.prepare(
        "SELECT i.name,l.locator FROM occurrence_locator l
      JOIN input i ON i.id=l.input_id WHERE l.occurrence_id=? ORDER BY i.name,l.locator",
    )?;
    let mut out = Vec::new();
    for (mut occurrence, valueset, id) in rows {
        occurrence.value_set = valueset
            .map(|id| shape::value_set_ref(conn, id))
            .transpose()?;
        occurrence.sources = sources
            .query_map([id], |r| {
                Ok(super::segments::Locator {
                    input: r.get(0)?,
                    locator: r.get(1)?,
                })
            })?
            .collect::<rusqlite::Result<_>>()?;
        out.push(occurrence);
    }
    Ok(out)
}
