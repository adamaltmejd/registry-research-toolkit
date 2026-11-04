//! Refs (`operations.toml`'s `ref` type): a public id or a bare name, resolved through
//! `ident`. Every operation that takes a ref calls [`resolve`]; one that expects a
//! table calls [`table`].

use reg_core::Fqid;
use rusqlite::{Connection, OptionalExtension, Row};

use crate::{Code, Error, rows};

/// A resolved ref: `ident`'s row.
pub struct Entity {
    /// `ident.kind`: `provider`, `register`, `table`, `variable`, ...
    pub kind: String,
    /// The row id in the kind's table.
    pub id: i64,
    pub pid: String,
}

fn entity(row: &Row) -> rusqlite::Result<Entity> {
    Ok(Entity {
        kind: row.get(0)?,
        id: row.get(1)?,
        pid: row.get(2)?,
    })
}

/// Resolve `value`: validate slash-separated public ids before their exact lookup,
/// with `set/` identifying only sets; then, for a ref without a `/`, the
/// non-set `ident` rows whose `name_fold` is its `fold_identity`, where several are
/// `ambiguous_ref`. So an exact pid never turns ambiguous. Otherwise a retired pid is
/// `retired_ref`, a malformed ref with a `/` `invalid_ref` and anything else
/// `not_found`.
///
/// # Errors
///
/// `ambiguous_ref`, `retired_ref`, `invalid_ref` or `not_found`.
pub fn resolve(conn: &Connection, value: &str) -> Result<Entity, Error> {
    let set_ref = is_set_ref(value)?;
    if let Some(hit) = conn
        .query_row(
            "SELECT kind, entity_id, pid FROM ident WHERE pid = ?",
            [value],
            entity,
        )
        .optional()?
    {
        if (hit.kind == "set") != set_ref {
            return Err(invalid_ref(value));
        }
        return Ok(hit);
    }
    if !value.contains('/') {
        let mut found: Vec<Entity> = rows(
            conn,
            "SELECT kind, entity_id, pid FROM ident
                      WHERE name_fold = ? AND kind <> 'set' ORDER BY pid",
            [reg_core::fold_identity(value)],
            entity,
        )?;
        match found.len() {
            0 => {}
            1 => return Ok(found.remove(0)),
            n => {
                let candidates = found
                    .iter()
                    .map(|e| candidate(conn, e))
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                return Err(Error::new(
                    Code::AmbiguousRef,
                    format!("{n} entities are named {value:?}."),
                    vec![value.into(), candidates.into()],
                ));
            }
        }
    }
    if let Some((kind, retired_in, reason)) = conn
        .query_row(
            "SELECT kind, retired_in, reason FROM ident_retired WHERE pid = ?",
            [value],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?
    {
        return Err(Error::new(
            Code::RetiredRef,
            format!("{value:?} was retired in catalog {retired_in} ({reason})."),
            vec![value.into(), kind.into(), retired_in.into(), reason.into()],
        ));
    }
    Err(Error::new(
        Code::NotFound,
        format!("Nothing is named {value:?}."),
        vec![value.into()],
    ))
}

/// An `ambiguous_ref` candidate `{fqid, kind, name}`. A variable's or class version's
/// name is its label.
fn candidate(conn: &Connection, e: &Entity) -> rusqlite::Result<serde_json::Value> {
    let table = match e.kind.as_str() {
        "provider" => "provider",
        "register" => "register",
        "table" => "tbl",
        "variable" => "variable",
        "class_family" => "class_family",
        "class_version" => "class_version",
        _ => "var_set",
    };
    let column = if matches!(table, "variable" | "class_version") {
        "label_text"
    } else {
        "name_text"
    };
    let name: String = conn.query_row(
        &format!("SELECT x.body FROM {table} t JOIN text x ON x.id = t.{column} WHERE t.id = ?"),
        [e.id],
        |row| row.get(0),
    )?;
    Ok(serde_json::json!({"fqid": &e.pid, "kind": &e.kind, "name": name}))
}

/// A table ref: a `tbl.pid`, or a ref that resolves to a table. A register with an
/// implicit table resolves to that table, which has the register's pid; one with
/// explicit tables is `table_is_register`, listing their pids.
///
/// # Errors
///
/// [`resolve`]'s; `table_is_register`; `invalid_ref` for a ref of another kind.
pub fn table(conn: &Connection, value: &str) -> Result<i64, Error> {
    let set_ref = is_set_ref(value)?;
    let by_pid = |pid: &str| {
        conn.query_row("SELECT id FROM tbl WHERE pid = ?", [pid], |row| row.get(0))
            .optional()
    };
    if !set_ref && let Some(id) = by_pid(value)? {
        return Ok(id);
    }
    let hit = resolve(conn, value)?;
    match hit.kind.as_str() {
        "table" => Ok(hit.id),
        "register" => {
            if let Some(id) = by_pid(&hit.pid)? {
                return Ok(id);
            }
            let tables: Vec<String> = rows(
                conn,
                "SELECT pid FROM tbl WHERE register_id = ? ORDER BY pid",
                [hit.id],
                |row| row.get(0),
            )?;
            Err(Error::new(
                Code::TableIsRegister,
                format!("{value:?} is a register; name one of its tables."),
                vec![value.into(), tables.into()],
            ))
        }
        kind => Err(Error::invalid_ref(
            value,
            format!("{value:?} is a {kind}, not a table."),
        )),
    }
}

/// A namespaced set id can only address a set. Bare names resolve non-set entities;
/// malformed public ids cannot bypass the grammar via `ident`.
fn is_set_ref(value: &str) -> Result<bool, Error> {
    match value.parse::<Fqid>() {
        Ok(Fqid::Set { .. }) => Ok(true),
        Err(_) if value.contains('/') => Err(invalid_ref(value)),
        _ => Ok(false),
    }
}

fn invalid_ref(value: &str) -> Error {
    Error::invalid_ref(value, format!("{value:?} is not a public id."))
}
