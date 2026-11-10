//! Refs (`operations.toml`'s `ref` type): a public id or a bare name, resolved through
//! `ident`. Every operation that takes a ref calls [`resolve`]; one that expects a
//! table calls [`table`].

use std::collections::{BTreeMap, BTreeSet};

use reg_core::Fqid;
use rusqlite::{Connection, OptionalExtension, Row};

use super::Server;
use crate::{Code, Error, rows};

/// The kinds [`names`] names. An implicit table shares its register's id, so a name is
/// asked for by kind.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Named {
    Register,
    Table,
    Variable,
    ClassVersion,
}

/// The display names [`names`] read, by kind and public id.
pub struct Names(BTreeMap<(Named, String), String>);

impl Names {
    #[must_use]
    pub fn get(&self, kind: Named, fqid: &str) -> Option<String> {
        self.0.get(&(kind, fqid.to_owned())).cloned()
    }

    /// The name of an id the response already resolved, which every such id has
    /// (`name_text` and `label_text` are `NOT NULL`).
    ///
    /// # Errors
    ///
    /// `internal_error` when it has none.
    pub fn of(&self, kind: Named, fqid: &str) -> Result<String, Error> {
        self.get(kind, fqid)
            .ok_or_else(|| Error::internal(format!("{fqid:?} has no name in the catalog.")))
    }
}

/// The display names of the registers, tables, variables and classification versions
/// among `fqids`, as `show` gives them: a register's or a table's name, a variable's or
/// a version's label. In steward mode an
/// id the reference catalog defines keeps its reference name (an overlay's is its
/// slug), as `show` does. One query per origin however many ids, so a list response
/// names its rows with one call, never a read per row.
///
/// # Errors
///
/// `catalog_unavailable` for an unreadable file; `internal_error` when a query fails.
pub fn names<'a>(
    server: &Server,
    fqids: impl IntoIterator<Item = &'a str>,
) -> Result<Names, Error> {
    let mut wanted: Vec<&str> = fqids.into_iter().collect();
    wanted.sort_unstable();
    wanted.dedup();
    if wanted.is_empty() {
        return Ok(Names(BTreeMap::new()));
    }
    let mut names = origin_names(&server.catalog.connect()?, &wanted)?;
    let found: BTreeSet<&str> = names.keys().map(|(_, fqid)| fqid.as_str()).collect();
    let rest: Vec<&str> = wanted.into_iter().filter(|f| !found.contains(f)).collect();
    if !rest.is_empty()
        && let Some(extra) = server.catalog.connect_holdings()?
    {
        names.extend(origin_names(&extra, &rest)?);
    }
    Ok(Names(names))
}

/// [`names`] in one origin: one query over the four kinds.
fn origin_names(
    conn: &Connection,
    fqids: &[&str],
) -> Result<BTreeMap<(Named, String), String>, Error> {
    Ok(rows(
        conn,
        "SELECT 0, r.pid, n.body FROM json_each(?1) x
           JOIN register r ON r.pid = x.value JOIN text n ON n.id = r.name_text
         UNION ALL
         SELECT 1, t.pid, n.body FROM json_each(?1) x
           JOIN tbl t ON t.pid = x.value JOIN text n ON n.id = t.name_text
         UNION ALL
         SELECT 2, v.pid, n.body FROM json_each(?1) x
           JOIN variable v ON v.pid = x.value JOIN text n ON n.id = v.label_text
         UNION ALL
         SELECT 3, c.pid, n.body FROM json_each(?1) x
           JOIN class_version c ON c.pid = x.value JOIN text n ON n.id = c.label_text",
        [serde_json::Value::from(fqids).to_string()],
        |row| {
            let kind = match row.get::<_, i64>(0)? {
                0 => Named::Register,
                1 => Named::Table,
                2 => Named::Variable,
                _ => Named::ClassVersion,
            };
            Ok(((kind, row.get(1)?), row.get(2)?))
        },
    )?
    .into_iter()
    .collect())
}

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
    Err(Error::not_found(value))
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

/// `invalid_ref` for `value`, a ref of kind `kind` where the operation wants another:
/// `"{value} is a {kind}, {expected}."`.
#[must_use]
pub fn wrong_kind(value: &str, kind: &str, expected: &str) -> Error {
    Error::invalid_ref(value, format!("{value:?} is a {kind}, {expected}."))
}

/// [`resolve`] `value`, refusing a ref whose kind is not one of `kinds` as
/// [`wrong_kind`] with `expected`.
///
/// # Errors
///
/// [`resolve`]'s; `invalid_ref` for another kind.
pub fn expect(
    conn: &Connection,
    value: &str,
    kinds: &[&str],
    expected: &str,
) -> Result<Entity, Error> {
    let entity = resolve(conn, value)?;
    if kinds.contains(&entity.kind.as_str()) {
        Ok(entity)
    } else {
        Err(wrong_kind(value, &entity.kind, expected))
    }
}

/// A set's members this deployment serves, as `(variable row id, fqid)` in fqid
/// order: the variables a set ref stands for in `graph` and `docs_related`, which
/// answer it with the union of its members' answers, so a group page reads its
/// members in one request (#2232).
///
/// # Errors
///
/// `not_found` in steward mode when the steward serves none of them, as `show` answers
/// such a set.
pub fn set_members(
    server: &Server,
    conn: &Connection,
    set: &Entity,
) -> Result<Vec<(i64, String)>, Error> {
    let mut members: Vec<(i64, String)> = rows(
        conn,
        "SELECT v.id, v.pid FROM var_set_member m JOIN variable v ON v.id = m.variable_id
         WHERE m.set_id = ? ORDER BY v.pid",
        [set.id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    members.retain(|(_, fqid)| server.catalog.serves("variable", fqid));
    if server.catalog.steward().is_some() && members.is_empty() {
        return Err(Error::not_found(&set.pid));
    }
    Ok(members)
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
        kind => Err(wrong_kind(value, kind, "not a table")),
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
