//! Refs (`operations.toml`'s `ref` type): a public id or a bare name, resolved through
//! `ident`. Every operation that takes a ref calls [`resolve`]; one that expects a
//! table calls [`table`].

use reg_core::Fqid;
use rusqlite::{Connection, OptionalExtension, Row};

use crate::{Code, Error};

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

/// Resolve `value`: an exact `ident.pid` first; then, for a ref without a `/`, the
/// `ident` rows whose `name_fold` is its `fold_identity`, where several are
/// `ambiguous_ref`. So an exact pid never turns ambiguous. Otherwise a retired pid is
/// `retired_ref`, a malformed ref with a `/` `invalid_ref` and anything else
/// `not_found`.
///
/// # Errors
///
/// `ambiguous_ref`, `retired_ref`, `invalid_ref` or `not_found`.
pub fn resolve(conn: &Connection, value: &str) -> Result<Entity, Error> {
    if let Some(hit) = conn
        .query_row(
            "SELECT kind, entity_id, pid FROM ident WHERE pid = ?",
            [value],
            entity,
        )
        .optional()?
    {
        return Ok(hit);
    }
    if !value.contains('/') {
        let mut found: Vec<Entity> = conn
            .prepare("SELECT kind, entity_id, pid FROM ident WHERE name_fold = ? ORDER BY pid")?
            .query_map([reg_core::fold_identity(value)], entity)?
            .collect::<rusqlite::Result<_>>()?;
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
    if value.contains('/') && !well_formed(value) {
        return Err(Error::new(
            Code::InvalidRef,
            format!("{value:?} is not a public id."),
            vec![value.into()],
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
    let by_pid = |pid: &str| {
        conn.query_row("SELECT id FROM tbl WHERE pid = ?", [pid], |row| row.get(0))
            .optional()
    };
    if let Some(id) = by_pid(value)? {
        return Ok(id);
    }
    let hit = resolve(conn, value)?;
    match hit.kind.as_str() {
        "table" => Ok(hit.id),
        "register" => {
            if let Some(id) = by_pid(&hit.pid)? {
                return Ok(id);
            }
            let tables: Vec<String> = conn
                .prepare("SELECT pid FROM tbl WHERE register_id = ? ORDER BY pid")?
                .query_map([hit.id], |row| row.get(0))?
                .collect::<rusqlite::Result<_>>()?;
            Err(Error::new(
                Code::TableIsRegister,
                format!("{value:?} is a register; name one of its tables."),
                vec![value.into(), tables.into()],
            ))
        }
        kind => Err(Error::new(
            Code::InvalidRef,
            format!("{value:?} is a {kind}, not a table."),
            vec![value.into()],
        )),
    }
}

/// Whether `value` is in the public-id grammar (`reg_core::Fqid`).
fn well_formed(value: &str) -> bool {
    value.parse::<Fqid>().is_ok()
}
