//! `segments`: a variable's segments, the reader's main row (schema §1.2), with each
//! segment's occurrences when `evidence` is asked for.

use rusqlite::types::Value as Sql;
use rusqlite::{Connection, params_from_iter};
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

use super::shape::{self, Release, ValueSetRef};
use super::{
    Cache, Operation, Param, Params, Period, Server, Type, component, cursor, flag, limit, period,
    refs,
};
use crate::{Code, Error};

pub const OPERATIONS: &[Operation] = &[Operation {
    name: "segments",
    paths: &["/api/segments/{ref}"],
    tool: Some("segments"),
    description: "A variable's segments: its maximal runs of one form per table, in time \
        order, each with its spelling, type, value set, flags and state. With `evidence`, \
        each segment's source occurrences and the rule overrides that changed them.",
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

/// `shape.Segment`.
#[derive(Serialize, ToSchema)]
pub struct Segment {
    /// The table's fqid.
    table: String,
    period: Period,
    spelling: String,
    #[serde(rename = "type")]
    #[schema(required = true)]
    dtype: Option<String>,
    #[schema(required = true)]
    length: Option<i64>,
    #[schema(required = true)]
    format: Option<String>,
    /// `null` when the segment has no value set.
    #[schema(required = true)]
    value_set: Option<ValueSetRef>,
    /// A classification version's ref.
    #[schema(required = true)]
    classification: Option<String>,
    coded: String,
    #[schema(required = true)]
    sensitive: Option<bool>,
    #[schema(required = true)]
    identifier: Option<bool>,
    #[schema(required = true)]
    id_kind: Option<String>,
    role: String,
    temporality: String,
    state: String,
    /// Present only when `evidence` is asked for.
    #[serde(skip_serializing_if = "Option::is_none")]
    evidence: Option<Vec<Occurrence>>,
}

/// One occurrence behind a segment (`shape.Segment.evidence`).
#[derive(Serialize, ToSchema)]
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

#[derive(Serialize, ToSchema)]
pub struct Locator {
    input: String,
    locator: String,
}

/// A non-source winner over one field of an occurrence.
#[derive(Serialize, ToSchema)]
pub struct Override {
    field: String,
    #[schema(required = true)]
    value: Option<String>,
    prov: String,
    /// `{file, line}` of a curated statement; always `null` in C.
    // simplify: curated overrides name their statement once D emits statements; type
    // and fill this then.
    #[schema(required = true, value_type = Option<Object>)]
    statement: Option<()>,
    #[schema(required = true)]
    rule: Option<String>,
    #[schema(required = true)]
    source_value: Option<String>,
}

fn segments(server: &Server, params: &Params) -> Result<Value, Error> {
    let conn = server.catalog.connect()?;
    let given = params["ref"];
    let variable = refs::resolve(&conn, given)?;
    if variable.kind != "variable" {
        return Err(Error::new(
            Code::InvalidRef,
            format!("{given:?} is a {}, not a variable.", variable.kind),
            vec![given.into()],
        ));
    }
    let (mut filter, mut args) = narrowing(&conn, params, &variable)?;
    let evidence = flag(params, "evidence")?;
    let limit = limit(params)?;
    if let Some(&token) = params.get("cursor") {
        let (_, position) = cursor::decode(
            token,
            server.catalog.generation(),
            &cursor::context(params),
            usize::MAX,
        )?;
        let (start, table) = position
            .split_once('\u{1f}')
            .and_then(|(start, table)| Some((start.parse::<i64>().ok()?, table)))
            .ok_or_else(|| cursor::invalid("Cursor is malformed."))?;
        filter.push("(s.start, t.pid) > (?, ?)".to_owned());
        args.push(Sql::Integer(start));
        args.push(Sql::Text(table.to_owned()));
    }
    args.push(Sql::Integer(
        i64::try_from(limit + 1).expect("a limit fits i64"),
    ));
    let mut rows = segment_rows(server, &conn, &filter, args)?;
    let next_cursor = if rows.len() > limit {
        rows.truncate(limit);
        // A keyset cursor restarts from its position; the offset only has to be valid.
        rows.last().map(|(_, _, _, (start, table))| {
            cursor::encode(
                server.catalog.generation(),
                &cursor::context(params),
                limit,
                &format!("{start}\u{1f}{table}"),
            )
        })
    } else {
        None
    };
    let mut items = Vec::with_capacity(rows.len());
    for (id, valueset_id, mut segment, _) in rows {
        segment.value_set = valueset_id
            .map(|valueset| shape::value_set_ref(&conn, valueset))
            .transpose()?;
        segment.evidence = evidence
            .then(|| occurrences(server, &conn, id))
            .transpose()?;
        items.push(segment);
    }
    Ok(serde_json::to_value(SegmentPage { items, next_cursor }).expect("a page serializes"))
}

/// The `WHERE` terms and arguments the variable, `table`, `period` and
/// `value_set_name` give.
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
            return Err(Error::new(
                Code::InvalidRef,
                format!("{table:?} is not a table of {given:?}'s register."),
                vec![table.into()],
            ));
        }
        filter.push("s.table_id = ?".to_owned());
        args.push(Sql::Integer(id));
    }
    if let Some(requested) = period(params, "period")? {
        let (first, last) = requested.iso_bounds();
        let day = |iso: &str| reg_core::iso_day_number(iso).expect("a period bound is a date");
        filter.push("s.start < ? AND ? < s.end".to_owned());
        args.push(Sql::Integer(day(&last) + 1));
        args.push(Sql::Integer(day(&first)));
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

/// One segment row with its value set id and its keyset position (start day, table pid).
type Row = (i64, Option<i64>, Segment, (i64, String));

/// The page of segment rows `filter` and `args` select, one past `limit` when more follow.
fn segment_rows(
    server: &Server,
    conn: &Connection,
    filter: &[String],
    args: Vec<Sql>,
) -> Result<Vec<Row>, Error> {
    let sql = format!(
        "SELECT s.id, t.pid, s.start, s.end, sp.exact, s.dtype, s.length, s.format,
                s.valueset_id, cv.pid, s.coded, s.sensitive, s.identifier, ik.slug,
                s.role, s.temporality, s.state
         FROM segment s
           JOIN tbl t ON t.id = s.table_id
           JOIN spelling sp ON sp.id = s.spelling_id
           LEFT JOIN class_version cv ON cv.id = s.class_version_id
           LEFT JOIN id_kind ik ON ik.id = s.id_kind_id
         WHERE {}
         ORDER BY s.start, t.pid
         LIMIT ?",
        filter.join(" AND ")
    );
    Ok(conn
        .prepare(&sql)?
        .query_map(params_from_iter(args), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<i64>>(8)?,
                Segment {
                    table: row.get(1)?,
                    period: server.period(row.get(2)?, row.get(3)?),
                    spelling: row.get(4)?,
                    dtype: row.get(5)?,
                    length: row.get(6)?,
                    format: row.get(7)?,
                    value_set: None,
                    classification: row.get(9)?,
                    coded: row.get(10)?,
                    sensitive: row.get(11)?,
                    identifier: row.get(12)?,
                    id_kind: row.get(13)?,
                    role: row.get(14)?,
                    temporality: row.get(15)?,
                    state: row.get(16)?,
                    evidence: None,
                },
                // The keyset position: the start day and the table pid.
                (row.get::<_, i64>(2)?, row.get::<_, String>(1)?),
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

/// A segment's occurrences, in time, then source key and spelling order.
fn occurrences(server: &Server, conn: &Connection, segment: i64) -> Result<Vec<Occurrence>, Error> {
    let rows = conn
        .prepare(
            "SELECT o.id, o.release_id, o.start, o.end, sp.exact, l.body, o.source_key, o.status
             FROM occurrence o
               JOIN spelling sp ON sp.id = o.spelling_id
               LEFT JOIN text l ON l.id = o.label_text
             WHERE o.segment_id = ?
             ORDER BY o.start, o.source_key, sp.exact, o.id",
        )?
        .query_map([segment], |row| {
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
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut locators = conn.prepare(
        "SELECT i.name, l.locator
         FROM occurrence_locator l JOIN input i ON i.id = l.input_id
         WHERE l.occurrence_id = ? ORDER BY i.name, l.locator",
    )?;
    let mut overrides = conn.prepare(
        "SELECT field, value, prov, rule_code, source_value
         FROM override WHERE subject_kind = 'occurrence' AND subject_id = ?
         ORDER BY field",
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
                                statement: None,
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
