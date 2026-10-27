//! `schema` and `diff` (schema.md §2.1, "Schema of a table" and "Schema diff"): one
//! line per (table, variable, folded spelling) over a period, and two periods' lines
//! joined on variable.

use std::collections::BTreeMap;

use rusqlite::Connection;
use serde::Serialize;
use serde_json::{Value, json};
use utoipa::ToSchema;

use super::shape::{ValueSetRef, value_set_ref};
use super::{Cache, Operation, Param, Params, Period, Server, Type, component, cursor, refs};
use crate::{Code, Error};

pub const OPERATIONS: &[Operation] = &[
    Operation {
        name: "schema",
        paths: &["/api/schema/{ref}"],
        tool: Some("schema"),
        description: "The columns of a table, a register's tables or one variable's tables \
            over a period: one line per variable and delivered spelling, with its type, \
            length, value set and role.",
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
        description: "A table's or a register's schema at two periods, joined on variable: \
            what was added, removed or changed (spelling, type, length, value set). A \
            rename is a changed spelling.",
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

/// `Page<SchemaLine>`.
#[derive(Serialize, ToSchema)]
pub struct SchemaPage {
    items: Vec<SchemaLine>,
    #[schema(required = true)]
    next_cursor: Option<String>,
}

/// `shape.SchemaLine`: one variable's delivery under one folded spelling in one table.
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
    /// The line's segments merged, clipped to the request.
    period: Period,
}

/// `shape.Diff`.
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

/// What a ref's lines are read from: its tables by pid, and for a variable ref the
/// variable.
struct Scope {
    fqid: String,
    tables: Vec<(i64, String)>,
    variable: Option<i64>,
}

fn invalid_ref(value: &str, kind: &str, operation: &str) -> Error {
    Error::new(
        Code::InvalidRef,
        format!("{value:?} is a {kind}, which {operation} does not take."),
        vec![value.into()],
    )
}

/// The tables of `value`: a table, a register's tables (implicit included) or, when
/// `variable_ok`, the tables a variable's segments are in. `table` keeps one of them.
fn scope(
    conn: &Connection,
    value: &str,
    table: Option<&str>,
    variable_ok: bool,
    operation: &str,
) -> Result<Scope, Error> {
    let hit = refs::resolve(conn, value)?;
    let (sql, variable) = match hit.kind.as_str() {
        "table" => ("SELECT id, pid FROM tbl WHERE id = ?", None),
        "register" => (
            "SELECT id, pid FROM tbl WHERE register_id = ? ORDER BY pid",
            None,
        ),
        "variable" if variable_ok => (
            "SELECT DISTINCT t.id, t.pid FROM segment s JOIN tbl t ON t.id = s.table_id
             WHERE s.variable_id = ? ORDER BY t.pid",
            Some(hit.id),
        ),
        kind => return Err(invalid_ref(value, kind, operation)),
    };
    let mut tables: Vec<(i64, String)> = conn
        .prepare(sql)?
        .query_map([hit.id], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    if let Some(table) = table {
        let id = refs::table(conn, table)?;
        if !tables.iter().any(|(t, _)| *t == id) {
            return Err(Error::new(
                Code::InvalidRef,
                format!("{table:?} is not one of the tables of {value:?}."),
                vec![table.into()],
            ));
        }
        tables.retain(|(t, _)| *t == id);
    }
    Ok(Scope {
        fqid: hit.pid,
        tables,
        variable,
    })
}

/// One (table, variable, folded spelling) over the request: the fields are those of
/// its latest segment inside the request.
struct Line {
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

/// The lines of `tables` (of `variable` when given) over `[from, to)`, ordered by
/// (table pid, variable pid, start, spelling).
fn lines(
    conn: &Connection,
    tables: &[(i64, String)],
    variable: Option<i64>,
    (from, to): (i64, i64),
) -> Result<Vec<Line>, Error> {
    let mut stmt = conn.prepare(
        "SELECT v.pid, l.body, f.folded, sp.exact, s.dtype, s.length, s.valueset_id, s.role,
                s.state, s.start, s.end
         FROM segment s
           JOIN variable v ON v.id = s.variable_id
           JOIN text l ON l.id = v.label_text
           JOIN name_fold f ON f.id = s.fold_id
           JOIN spelling sp ON sp.id = s.spelling_id
         WHERE s.table_id = ?1 AND (?2 IS NULL OR s.variable_id = ?2)
           AND s.start < ?4 AND ?3 < s.end
         ORDER BY v.pid, f.folded, s.start",
    )?;
    let mut out: Vec<Line> = Vec::new();
    for (id, pid) in tables {
        let rows = stmt.query_map(rusqlite::params![id, variable, from, to], |row| {
            Ok(Line {
                table: pid.clone(),
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
        for row in rows {
            let row = row?;
            // The rows of one group are adjacent and in start order: a later segment
            // replaces the fields and extends the end.
            match out.last_mut() {
                Some(last)
                    if last.table == row.table
                        && last.variable == row.variable
                        && last.folded == row.folded =>
                {
                    *last = Line {
                        start: last.start,
                        ..row
                    };
                }
                _ => out.push(row),
            }
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
    fn shape(self, server: &Server, conn: &Connection) -> Result<SchemaLine, Error> {
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
    let conn = server.catalog.connect()?;
    let scope = scope(
        &conn,
        params["ref"],
        params.get("table").copied(),
        true,
        "schema",
    )?;
    // simplify: every line of the ref is built, then paged in memory; page in SQL if a
    // register's lines pass a few thousand.
    let mut rest: Vec<Line> = lines(&conn, &scope.tables, scope.variable, request)?
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
    let next_cursor = (rest.len() > limit).then(|| {
        rest.truncate(limit);
        let last = rest.last().expect("a page holds at least one line");
        cursor::encode(
            server.catalog.generation(),
            &context,
            offset + rest.len(),
            &format!(
                "{}{SEPARATOR}{}{SEPARATOR}{}{SEPARATOR}{}",
                last.table, last.variable, last.start, last.spelling
            ),
        )
    });
    let items = rest
        .into_iter()
        .map(|l| l.shape(server, &conn))
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
fn changes(conn: &Connection, from: &Line, to: &Line) -> Result<Vec<DiffChange>, Error> {
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
    // A value set is content-addressed, so equal ids are equal hashes.
    if from.valueset_id != to.valueset_id {
        let set = |id: Option<i64>| -> Result<Value, Error> {
            id.map_or(Ok(Value::Null), |id| {
                Ok(serde_json::to_value(value_set_ref(conn, id)?).expect("ValueSetRef serializes"))
            })
        };
        out.push(DiffChange {
            field: "value_set",
            from: set(from.valueset_id)?,
            to: set(to.valueset_id)?,
        });
    }
    Ok(out)
}

fn diff(server: &Server, params: &Params) -> Result<Value, Error> {
    let from = super::days(params, "from")?.expect("a required parameter");
    let to = super::days(params, "to")?.expect("a required parameter");
    let conn = server.catalog.connect()?;
    let scope = scope(
        &conn,
        params["ref"],
        params.get("table").copied(),
        false,
        "diff",
    )?;
    let mut tables = Vec::new();
    for table in &scope.tables {
        let one = std::slice::from_ref(table);
        let before = by_variable(lines(&conn, one, None, from)?);
        let mut after = by_variable(lines(&conn, one, None, to)?);
        let (mut removed, mut changed, mut unchanged) = (Vec::new(), Vec::new(), 0);
        for (variable, line) in before {
            match after.remove(&variable) {
                None => removed.push(line.shape(server, &conn)?),
                Some(now) => {
                    let differences = changes(&conn, &line, &now)?;
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
            .map(|line| line.shape(server, &conn))
            .collect::<Result<Vec<_>, _>>()?;
        let summary = DiffSummary {
            added: added.len(),
            removed: removed.len(),
            changed: changed.len(),
            unchanged,
        };
        tables.push(DiffTable {
            table: table.1.clone(),
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
