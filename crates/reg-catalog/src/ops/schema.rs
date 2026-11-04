//! `schema` and `diff` (schema.md §2.1, "Schema of a table" and "Schema diff"): one
//! line per (table, variable, folded spelling) over a period, and two periods' lines
//! joined on variable.

use std::collections::BTreeMap;

use rusqlite::{Connection, OptionalExtension};
use serde::Serialize;
use serde_json::{Value, json};
use utoipa::ToSchema;

use super::shape::{ValueSetRef, value_set_ref};
use super::{Cache, Operation, Param, Params, Period, Server, Type, component, cursor, refs};
use crate::{Error, rows};

pub const OPERATIONS: &[Operation] = &[
    Operation {
        name: "schema",
        paths: &["/api/schema/{ref}"],
        tool: Some("schema"),
        description: "The columns of a table, a register or a variable's tables over a period, \
            with type, length, value set and role.",
        params: &[
            Param::required("ref", Type::Ref),
            Param::optional("period", Type::Period),
            Param::optional("table", Type::Ref),
            Param::optional("limit", Type::Limit),
            Param::optional("cursor", Type::Cursor),
        ],
        cache: Cache::Minute,
        run: schema,
        result: component::<SchemaPage>,
    },
    Operation {
        name: "diff",
        paths: &["/api/diff/{ref}"],
        tool: Some("schema"),
        description: "Compare a table's or register's columns at two periods: added, removed \
            and changed (name, type, length, value set).",
        params: &[
            Param::required("ref", Type::Ref),
            Param::required("from", Type::Period),
            Param::required("to", Type::Period),
            Param::optional("table", Type::Ref),
        ],
        cache: Cache::Minute,
        run: diff,
        result: component::<Diff>,
    },
];

#[derive(Serialize, ToSchema)]
pub struct SchemaPage {
    items: Vec<SchemaLine>,
    #[schema(required = true)]
    next_cursor: Option<String>,
}

/// One variable under one column name in one table.
#[derive(Serialize, ToSchema)]
pub struct SchemaLine {
    table: String,
    variable: String,
    /// The variable's label.
    name: String,
    spelling: String,
    #[schema(required = true)]
    r#type: Option<String>,
    #[schema(required = true)]
    length: Option<i64>,
    #[schema(required = true)]
    value_set: Option<ValueSetRef>,
    role: String,
    state: String,
    /// Its period, cut to the requested one.
    period: Period,
}

#[derive(Serialize, ToSchema)]
pub struct Diff {
    fqid: String,
    /// The request's `from`.
    from: String,
    /// The request's `to`.
    to: String,
    tables: Vec<DiffTable>,
}

#[derive(Serialize, ToSchema)]
pub struct DiffTable {
    table: String,
    added: Vec<SchemaLine>,
    removed: Vec<SchemaLine>,
    changed: Vec<DiffChanged>,
    summary: DiffSummary,
}

#[derive(Serialize, ToSchema)]
pub struct DiffChanged {
    variable: String,
    changes: Vec<DiffChange>,
}

/// One field that differs between the two periods: `spelling`, `type`, `length` or
/// `value_set`.
#[derive(Serialize, ToSchema)]
pub struct DiffChange {
    field: &'static str,
    #[schema(value_type = Value)]
    from: Value,
    #[schema(value_type = Value)]
    to: Value,
}

#[derive(Serialize, ToSchema)]
pub struct DiffSummary {
    added: usize,
    removed: usize,
    changed: usize,
    unchanged: usize,
}

/// What a ref's lines are read from: its tables by pid, each with its local ids in
/// every origin that has segments for it.
struct Scope {
    fqid: String,
    tables: Vec<ScopeTable>,
}

struct ScopeTable {
    pid: String,
    /// (origin, table id, variable id for a variable ref).
    locals: Vec<(usize, i64, Option<i64>)>,
}

fn invalid_ref(value: &str, kind: &str, operation: &str) -> Error {
    Error::invalid_ref(
        value,
        format!("{value:?} is a {kind}, which {operation} does not take."),
    )
}

/// The database origins a ref's lines are read from: the reference catalog and, in
/// steward mode, the holdings, whose supplemental segments share public ids.
fn origins(server: &Server, conn: Connection) -> Result<Vec<Connection>, Error> {
    Ok(match server.catalog.connect_holdings()? {
        Some(extra) => vec![server.catalog.connect()?, extra],
        None => vec![conn],
    })
}

/// The tables of `value`: a table, a register's tables (implicit included) or, when
/// `variable_ok`, the tables a variable's segments are in, over every origin; in
/// steward mode only held ones. `table` keeps one of them.
fn scope(
    server: &Server,
    conn: &Connection,
    origins: &[Connection],
    value: &str,
    table: Option<&str>,
    variable_ok: bool,
    operation: &str,
) -> Result<Scope, Error> {
    let hit = refs::resolve(conn, value)?;
    let (local, sql) = match hit.kind.as_str() {
        "table" => (
            "SELECT id FROM tbl WHERE pid = ?",
            "SELECT id, pid FROM tbl WHERE id = ?",
        ),
        "register" => (
            "SELECT id FROM register WHERE pid = ?",
            "SELECT id, pid FROM tbl WHERE register_id = ? ORDER BY pid",
        ),
        "variable" if variable_ok => (
            "SELECT id FROM variable WHERE pid = ?",
            "SELECT DISTINCT t.id, t.pid FROM segment s JOIN tbl t ON t.id = s.table_id
             WHERE s.variable_id = ? ORDER BY t.pid",
        ),
        kind => return Err(invalid_ref(value, kind, operation)),
    };
    // The narrowing table may be the steward's own: the first origin naming it wins,
    // and the ref's origin's error stands when none does.
    let keep = table
        .map(|table| {
            let pid = |db: &Connection| -> Result<String, Error> {
                let id = refs::table(db, table)?;
                Ok(db.query_row("SELECT pid FROM tbl WHERE id = ?", [id], |row| row.get(0))?)
            };
            origins
                .iter()
                .find_map(|db| pid(db).ok())
                .map_or_else(|| pid(conn), Ok)
        })
        .transpose()?;
    let mut tables: BTreeMap<String, Vec<(usize, i64, Option<i64>)>> = BTreeMap::new();
    for (origin, db) in origins.iter().enumerate() {
        let Some(id) = db
            .query_row(local, [&hit.pid], |row| row.get::<_, i64>(0))
            .optional()?
        else {
            continue;
        };
        let variable = (hit.kind == "variable").then_some(id);
        for (table, pid) in rows(db, sql, [id], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })? {
            if server.catalog.serves("table", &pid) {
                tables
                    .entry(pid)
                    .or_default()
                    .push((origin, table, variable));
            }
        }
    }
    if let (Some(table), Some(pid)) = (table, &keep) {
        if !tables.contains_key(pid) {
            return Err(Error::invalid_ref(
                table,
                format!("{table:?} is not one of the tables of {value:?}."),
            ));
        }
        tables.retain(|t, _| t == pid);
    }
    Ok(Scope {
        fqid: hit.pid,
        tables: tables
            .into_iter()
            .map(|(pid, locals)| ScopeTable { pid, locals })
            .collect(),
    })
}

/// One (table, variable, folded spelling) over the request: the fields are those of
/// its latest segment inside the request.
struct Line {
    /// Index of the database origin `valueset_id` belongs to.
    origin: usize,
    table: String,
    variable: String,
    name: String,
    folded: String,
    spelling: String,
    dtype: Option<String>,
    length: Option<i64>,
    valueset_id: Option<i64>,
    role: String,
    state: String,
    /// First segment start to last segment end, clipped to the request.
    start: i64,
    end: i64,
}

/// The lines of `tables` over `[from, to)`, ordered by (table pid, variable pid,
/// start, spelling). In steward mode each segment is first clipped to the years its
/// variable is held in its table, and one held in none of them is dropped, so an
/// unheld segment never supplies a line's facts (crates/DESIGN.md, "A steward
/// deployment serves its holdings only").
fn lines(
    server: &Server,
    origins: &[Connection],
    tables: &[ScopeTable],
    (from, to): (i64, i64),
) -> Result<Vec<Line>, Error> {
    let extra = server.catalog.connect_holdings()?;
    let mut held = extra
        .as_ref()
        .map(|extra| {
            extra.prepare(
                "SELECT min(start), max(end) FROM holding_mapping
                 WHERE table_pid = ?1 AND variable_pid = ?2 AND start < ?4 AND ?3 < end",
            )
        })
        .transpose()?;
    let mut segments: Vec<Line> = Vec::new();
    for table in tables {
        for &(origin, id, variable) in &table.locals {
            let mut stmt = origins[origin].prepare_cached(
                "SELECT v.pid, l.body, f.folded, sp.exact, s.dtype, s.length, s.valueset_id,
                        s.role, s.state, s.start, s.end
                 FROM segment s
                   JOIN variable v ON v.id = s.variable_id
                   JOIN text l ON l.id = v.label_text
                   JOIN name_fold f ON f.id = s.fold_id
                   JOIN spelling sp ON sp.id = s.spelling_id
                 WHERE s.table_id = ?1 AND (?2 IS NULL OR s.variable_id = ?2)
                   AND s.start < ?4 AND ?3 < s.end",
            )?;
            let found = stmt.query_map(rusqlite::params![id, variable, from, to], |row| {
                Ok(Line {
                    origin,
                    table: table.pid.clone(),
                    variable: row.get(0)?,
                    name: row.get(1)?,
                    folded: row.get(2)?,
                    spelling: row.get(3)?,
                    dtype: row.get(4)?,
                    length: row.get(5)?,
                    valueset_id: row.get(6)?,
                    role: row.get(7)?,
                    state: row.get(8)?,
                    start: row.get::<_, i64>(9)?.max(from),
                    end: row.get::<_, i64>(10)?.min(to),
                })
            })?;
            for segment in found {
                let mut segment = segment?;
                if let Some(held) = held.as_mut() {
                    let (first, last): (Option<i64>, Option<i64>) = held.query_row(
                        rusqlite::params![
                            segment.table,
                            segment.variable,
                            segment.start,
                            segment.end
                        ],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )?;
                    let (Some(first), Some(last)) = (first, last) else {
                        continue;
                    };
                    segment.start = segment.start.max(first);
                    segment.end = segment.end.min(last);
                }
                segments.push(segment);
            }
        }
    }
    segments.sort_by(|a, b| {
        (&a.table, &a.variable, &a.folded, a.start).cmp(&(
            &b.table,
            &b.variable,
            &b.folded,
            b.start,
        ))
    });
    let mut out: Vec<Line> = Vec::new();
    for segment in segments {
        // A group's segments are now adjacent and in start order: a later one
        // replaces the fields and extends the end.
        match out.last_mut() {
            Some(last)
                if last.table == segment.table
                    && last.variable == segment.variable
                    && last.folded == segment.folded =>
            {
                *last = Line {
                    start: last.start,
                    end: last.end.max(segment.end),
                    ..segment
                };
            }
            _ => out.push(segment),
        }
    }
    out.sort_by(|a, b| {
        (&a.table, &a.variable, a.start, &a.spelling).cmp(&(
            &b.table,
            &b.variable,
            b.start,
            &b.spelling,
        ))
    });
    Ok(out)
}

impl Line {
    fn shape(self, server: &Server, origins: &[Connection]) -> Result<SchemaLine, Error> {
        let conn = &origins[self.origin];
        Ok(SchemaLine {
            table: self.table,
            variable: self.variable,
            name: self.name,
            spelling: self.spelling,
            r#type: self.dtype,
            length: self.length,
            value_set: self
                .valueset_id
                .map_or(Ok(None), |id| value_set_ref(conn, id).map(Some))?,
            role: self.role,
            state: self.state,
            period: server.period(self.start, self.end),
        })
    }
}

const SEPARATOR: char = '\u{1f}';

/// A cursor's position: the key of the last line returned.
fn position(text: &str) -> Result<(String, String, i64, String), Error> {
    let mut parts = text.split(SEPARATOR);
    match (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) {
        (Some(table), Some(variable), Some(start), Some(spelling), None) => start
            .parse()
            .map(|start| {
                (
                    table.to_owned(),
                    variable.to_owned(),
                    start,
                    spelling.to_owned(),
                )
            })
            .map_err(|_| cursor::invalid("Cursor is malformed.")),
        _ => Err(cursor::invalid("Cursor is malformed.")),
    }
}

fn schema(server: &Server, params: &Params) -> Result<Value, Error> {
    let limit = super::limit(params)?;
    let request = super::days(params, "period")?.unwrap_or((i64::MIN, i64::MAX));
    // The cursor is checked before the ref: a cursor from another generation is stale
    // whatever its ref now resolves to.
    let context = cursor::context(params);
    let (offset, after) = match params.get("cursor") {
        Some(token) => {
            let (offset, text) =
                cursor::decode(token, server.catalog.generation(), &context, usize::MAX)?;
            (offset, Some(position(&text)?))
        }
        None => (0, None),
    };
    let conn = server.catalog.connect_for_ref(params["ref"])?;
    let origins = origins(server, server.catalog.connect_for_ref(params["ref"])?)?;
    let scope = scope(
        server,
        &conn,
        &origins,
        params["ref"],
        params.get("table").copied(),
        true,
        "schema",
    )?;
    // simplify: every line of the ref is built, then paged in memory; page in SQL if a
    // register's lines pass a few thousand.
    let mut rest: Vec<Line> = lines(server, &origins, &scope.tables, request)?
        .into_iter()
        .filter(|l| {
            after
                .as_ref()
                .is_none_or(|(table, variable, start, spelling)| {
                    (
                        l.table.as_str(),
                        l.variable.as_str(),
                        l.start,
                        l.spelling.as_str(),
                    ) > (table.as_str(), variable.as_str(), *start, spelling.as_str())
                })
        })
        .take(limit + 1)
        .collect();
    let next_cursor = cursor::next(&mut rest, limit, |last| {
        Some(cursor::encode(
            server.catalog.generation(),
            &context,
            offset + limit,
            &format!(
                "{}{SEPARATOR}{}{SEPARATOR}{}{SEPARATOR}{}",
                last.table, last.variable, last.start, last.spelling
            ),
        ))
    });
    let items = rest
        .into_iter()
        .map(|l| l.shape(server, &origins))
        .collect::<Result<_, _>>()?;
    Ok(serde_json::to_value(SchemaPage { items, next_cursor }).expect("SchemaPage serializes"))
}

/// One line per variable at a period: the one with the latest start.
fn by_variable(lines: Vec<Line>) -> BTreeMap<String, Line> {
    let mut map = BTreeMap::new();
    for line in lines {
        map.insert(line.variable.clone(), line);
    }
    map
}

/// The changes between a variable's lines at the two periods, in the order spelling,
/// type, length, value set.
fn changes(origins: &[Connection], from: &Line, to: &Line) -> Result<Vec<DiffChange>, Error> {
    let mut out = Vec::new();
    if from.spelling != to.spelling {
        out.push(DiffChange {
            field: "spelling",
            from: json!(from.spelling),
            to: json!(to.spelling),
        });
    }
    if from.dtype != to.dtype {
        out.push(DiffChange {
            field: "type",
            from: json!(from.dtype),
            to: json!(to.dtype),
        });
    }
    if from.length != to.length {
        out.push(DiffChange {
            field: "length",
            from: json!(from.length),
            to: json!(to.length),
        });
    }
    // A value set is content-addressed: compare hashes, since the two lines' ids may
    // belong to different origins whose names for the same set differ.
    let set = |line: &Line| -> Result<Value, Error> {
        line.valueset_id.map_or(Ok(Value::Null), |id| {
            Ok(
                serde_json::to_value(value_set_ref(&origins[line.origin], id)?)
                    .expect("ValueSetRef serializes"),
            )
        })
    };
    let (before, after) = (set(from)?, set(to)?);
    if before.get("hash") != after.get("hash") {
        out.push(DiffChange {
            field: "value_set",
            from: before,
            to: after,
        });
    }
    Ok(out)
}

fn diff(server: &Server, params: &Params) -> Result<Value, Error> {
    let from = super::days(params, "from")?.expect("a required parameter");
    let to = super::days(params, "to")?.expect("a required parameter");
    let conn = server.catalog.connect_for_ref(params["ref"])?;
    let origins = origins(server, server.catalog.connect_for_ref(params["ref"])?)?;
    let scope = scope(
        server,
        &conn,
        &origins,
        params["ref"],
        params.get("table").copied(),
        false,
        "diff",
    )?;
    let mut tables = Vec::new();
    for table in &scope.tables {
        let one = std::slice::from_ref(table);
        let before = by_variable(lines(server, &origins, one, from)?);
        let mut after = by_variable(lines(server, &origins, one, to)?);
        let (mut removed, mut changed, mut unchanged) = (Vec::new(), Vec::new(), 0);
        for (variable, line) in before {
            match after.remove(&variable) {
                None => removed.push(line.shape(server, &origins)?),
                Some(now) => {
                    let differences = changes(&origins, &line, &now)?;
                    if differences.is_empty() {
                        unchanged += 1;
                    } else {
                        changed.push(DiffChanged {
                            variable,
                            changes: differences,
                        });
                    }
                }
            }
        }
        let added = after
            .into_values()
            .map(|line| line.shape(server, &origins))
            .collect::<Result<Vec<_>, _>>()?;
        let summary = DiffSummary {
            added: added.len(),
            removed: removed.len(),
            changed: changed.len(),
            unchanged,
        };
        tables.push(DiffTable {
            table: table.pid.clone(),
            added,
            removed,
            changed,
            summary,
        });
    }
    Ok(serde_json::to_value(Diff {
        fqid: scope.fqid,
        from: params["from"].to_owned(),
        to: params["to"].to_owned(),
        tables,
    })
    .expect("Diff serializes"))
}
