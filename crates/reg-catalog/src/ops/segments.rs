//! `segments`: a variable's segments, the reader's main row (schema §1.2), with each
//! segment's occurrences when `evidence` is asked for.

use rusqlite::types::Value as Sql;
use rusqlite::{Connection, params_from_iter};
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
    description: "A variable's columns over time: per table, column name and period one \
        row, in time order, with the provider's wording and the form (type, value set, \
        flags) where stated. A change of form or wording starts a row. On a steward \
        catalog, the held columns and files over the years held. With `evidence`, each \
        row's source occurrences and the rule overrides that changed them.",
    params: &[
        Param::required("ref", Type::Ref),
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

/// `Page<Segment>`.
#[derive(Serialize, ToSchema)]
pub struct SegmentPage {
    items: Vec<Segment>,
    #[schema(required = true)]
    next_cursor: Option<String>,
}

/// `shape.Segment`: one row of the variable's one per-period list, a column of one
/// table over one period with one form and one provider wording. A fact the catalog
/// does not state is absent, never `null`: no type, no value set, `coded` or
/// `temporality` unknown, and `state` clear (the default) are all left out.
#[derive(Clone, Serialize, ToSchema)]
pub struct Segment {
    /// The table's fqid.
    table: String,
    /// The column name it is delivered under; in steward mode, the held column's.
    column: String,
    period: Period,
    /// In steward mode, the file the steward holds the column in.
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
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    id_kind: Option<String>,
    role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    temporality: Option<String>,
    /// Present only when the column is ambiguous; absent means clear.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    state: Option<String>,
    /// Present only when `evidence` is asked for.
    #[serde(skip_serializing_if = "Option::is_none")]
    evidence: Option<Vec<Occurrence>>,
}

/// One occurrence behind a segment (`shape.Segment.evidence`).
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

/// A non-source winner over one field of an occurrence.
#[derive(Clone, Serialize, ToSchema)]
pub struct Override {
    field: String,
    #[schema(required = true)]
    value: Option<String>,
    prov: String,
    /// Authoring location of the curated statement.
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
    let variable = refs::resolve(&conn, given)?;
    if variable.kind != "variable" {
        return Err(Error::invalid_ref(
            given,
            format!("{given:?} is a {}, not a variable.", variable.kind),
        ));
    }
    let (filter, args) = narrowing(&conn, params, &variable)?;
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
    // simplify: the whole list is built, then paged; a variable has tens of rows.
    // Page in SQL again if a variable's list reaches thousands.
    let mut rows = segment_rows(&conn, &filter, args)?;
    if let Some(extra) = server.catalog.connect_holdings()? {
        rows = held_rows(&extra, &variable.pid, params.get("table").copied(), &rows)?;
    }
    let rows = with_wording(&conn, variable.id, rows)?;
    let mut rows: Vec<Row> = match days(params, "period")? {
        Some((first, end)) => rows
            .into_iter()
            .filter(|row| row.start < end && first < row.end)
            .collect(),
        None => rows,
    };
    rows.sort_by(|a, b| {
        (
            a.start,
            &a.segment.table,
            &a.segment.column,
            &a.segment.file,
        )
            .cmp(&(
                b.start,
                &b.segment.table,
                &b.segment.column,
                &b.segment.file,
            ))
    });
    let mut page: Vec<Row> = rows.into_iter().skip(offset).take(limit + 1).collect();
    let next_cursor = cursor::next(&mut page, limit, |_| {
        Some(cursor::encode(
            server.catalog.generation(),
            &cursor::context(params),
            offset + limit,
            "",
        ))
    });
    let mut items = Vec::with_capacity(page.len());
    for Row {
        id,
        valueset_id,
        start,
        end,
        mut segment,
    } in page
    {
        segment.period = server.period(start, end);
        segment.value_set = valueset_id
            .map(|valueset| shape::value_set_ref(&conn, valueset))
            .transpose()?;
        segment.evidence = match (evidence, id) {
            (true, Some(id)) => Some(occurrences(server, &conn, id)?),
            (true, None) => Some(vec![]),
            (false, _) => None,
        };
        items.push(segment);
    }
    Ok(serde_json::to_value(SegmentPage { items, next_cursor }).expect("a page serializes"))
}

/// The `WHERE` terms and arguments the variable, `table` and `value_set_name` give;
/// `period` applies after steward rows replace the catalog's periods.
fn narrowing(
    conn: &Connection,
    params: &Params,
    variable: &refs::Entity,
) -> Result<(Vec<String>, Vec<Sql>), Error> {
    let given = params["ref"];
    let mut filter = vec!["s.variable_id = ?".to_owned()];
    let mut args = vec![Sql::Integer(variable.id)];
    if let Some(&table) = params.get("table") {
        let id = refs::table(conn, table)?;
        let register = |sql: &str, id: i64| conn.query_row(sql, [id], |row| row.get::<_, i64>(0));
        if register("SELECT register_id FROM tbl WHERE id = ?", id)?
            != register("SELECT register_id FROM variable WHERE id = ?", variable.id)?
        {
            return Err(Error::invalid_ref(
                table,
                format!("{table:?} is not a table of {given:?}'s register."),
            ));
        }
        filter.push("s.table_id = ?".to_owned());
        args.push(Sql::Integer(id));
    }
    if let Some(&name) = params.get("value_set_name") {
        filter.push(
            "EXISTS (SELECT 1 FROM valueset_name n JOIN text x ON x.id = n.name_text
               WHERE n.valueset_id = s.valueset_id AND x.body = ?)"
                .to_owned(),
        );
        args.push(Sql::Text(name.to_owned()));
    }
    Ok((filter, args))
}

/// One row of the list before paging: its segment (none for a steward row no
/// catalog segment describes), value set id and `[start, end)` days.
#[derive(Clone)]
struct Row {
    id: Option<i64>,
    valueset_id: Option<i64>,
    start: i64,
    end: i64,
    segment: Segment,
}

/// Every catalog segment `filter` and `args` select.
fn segment_rows(conn: &Connection, filter: &[String], args: Vec<Sql>) -> Result<Vec<Row>, Error> {
    let sql = format!(
        "SELECT s.id, t.pid, s.start, s.end, sp.exact, s.dtype, s.length, s.format,
                s.valueset_id, cv.pid, s.coded, s.sensitive, s.identifier, ik.slug,
                s.role, s.temporality, s.state
         FROM segment s
           JOIN tbl t ON t.id = s.table_id
           JOIN spelling sp ON sp.id = s.spelling_id
           LEFT JOIN class_version cv ON cv.id = s.class_version_id
           LEFT JOIN id_kind ik ON ik.id = s.id_kind_id
         WHERE {}",
        filter.join(" AND ")
    );
    let stated = |value: String, unstated: &str| (value != unstated).then_some(value);
    Ok(rows(conn, &sql, params_from_iter(args), |row| {
        Ok(Row {
            id: Some(row.get(0)?),
            valueset_id: row.get(8)?,
            start: row.get(2)?,
            end: row.get(3)?,
            segment: Segment {
                table: row.get(1)?,
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
                id_kind: row.get(13)?,
                role: row.get(14)?,
                temporality: stated(row.get(15)?, "unknown"),
                state: stated(row.get(16)?, "clear"),
                evidence: None,
            },
        })
    })?)
}

/// Steward mode: one row per held file and column over the years held, never the
/// catalog's wider periods (maintainer 2026-11-04). A placement takes its facts from
/// the catalog segments it overlaps in its table, preferring the same column name;
/// with none, from the table's or the variable's first segment, else only its role.
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
        let named: Vec<&Row> = in_table
            .iter()
            .copied()
            .filter(|r| r.segment.column.eq_ignore_ascii_case(&column))
            .collect();
        let candidates = if named.is_empty() { in_table } else { named };
        let mut parts: Vec<Row> = candidates
            .iter()
            .filter(|r| r.start < end && start < r.end)
            .map(|r| Row {
                start: r.start.max(start),
                end: r.end.min(end),
                ..(*r).clone()
            })
            .collect();
        if parts.is_empty() {
            let template = candidates.first().copied().or_else(|| catalog.first());
            parts.push(match template {
                Some(r) => Row {
                    start,
                    end,
                    ..r.clone()
                },
                None => Row {
                    id: None,
                    valueset_id: None,
                    start,
                    end,
                    segment: Segment {
                        table: table.clone(),
                        column: column.clone(),
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
                        id_kind: None,
                        role,
                        temporality: None,
                        state: None,
                        evidence: None,
                    },
                },
            });
        }
        for mut part in parts {
            part.segment.table.clone_from(&table);
            part.segment.column.clone_from(&column);
            part.segment.file = Some(file.clone());
            out.push(part);
        }
    }
    Ok(out)
}

/// The rows cut where the provider's wording changes, each carrying the wording in
/// force: the latest used occurrence's in the row's table that states one, else (a
/// steward table the catalog has no occurrence in) any table's on that day.
fn with_wording(conn: &Connection, variable: i64, list: Vec<Row>) -> Result<Vec<Row>, Error> {
    let runs = show_evidence::wordings(conn, variable)?;
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
    Ok(out)
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
