//! Artifact verification: the one authority for a written catalog file's invariants
//! (schema v4 §4.2, §4.3). `emit` runs it before replacing the output; `reg-build verify`
//! runs it on any file.

use std::path::Path;

use reg_frame::Item;
use rusqlite::{Connection, OpenFlags};
use sha2::{Digest, Sha256};

const CHECKS: &str = include_str!("checks.sql");

/// Every invariant of a written catalog file (schema v4 §4.2, §4.3). Read-only.
///
/// # Errors
/// The first failure, naming the check: `integrity_check: …`, `post-write check failed:\n<sql>`,
/// `valueset <hash>: content hashes to <actual>`, or `previous release: <sql>`.
pub fn catalog(path: &Path, _previous: Option<&Path>) -> Result<(), String> {
    let sql = |e: rusqlite::Error| e.to_string();
    let db = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .map_err(sql)?;
    let integrity: String = db
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .map_err(sql)?;
    if integrity != "ok" {
        return Err(format!("integrity_check: {integrity}"));
    }
    let checks = CHECKS
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    for statement in checks.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        if has_rows(&db, statement)? {
            return Err(format!("post-write check failed:\n{statement}"));
        }
    }
    valuesets(&db)?;
    Ok(())
}

fn has_rows(db: &Connection, statement: &str) -> Result<bool, String> {
    let mut stmt = db
        .prepare(statement)
        .map_err(|e| format!("{statement}: {e}"))?;
    let mut found = stmt.query([]).map_err(|e| e.to_string())?;
    Ok(found.next().map_err(|e| e.to_string())?.is_some())
}

/// Each value set's items hash to its address, the way `reg_frame::ValueSet::new` does.
fn valuesets(db: &Connection) -> Result<(), String> {
    let sql = |e: rusqlite::Error| e.to_string();
    let mut sets = db
        .prepare("SELECT id, hash FROM valueset ORDER BY id")
        .map_err(sql)?;
    let mut items = db
        .prepare(
            "SELECT c.body, l.body, i.parent_ord
               FROM vs_item i
               JOIN text c ON c.id = i.code_text
               LEFT JOIN text l ON l.id = i.label_text
              WHERE i.valueset_id = ?1 ORDER BY i.ord",
        )
        .map_err(sql)?;
    let sets = sets
        .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))
        .map_err(sql)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sql)?;
    for (id, hash) in sets {
        let rows = items
            .query_map([id], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    r.get::<_, Option<u32>>(2)?,
                ))
            })
            .map_err(sql)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql)?;
        let list: Vec<Item> = rows
            .iter()
            .map(|(code, label, parent)| Item {
                code: code.clone(),
                label: label.clone(),
                parent: parent
                    .and_then(|p| rows.get(p as usize))
                    .map(|p| p.0.clone()),
            })
            .collect();
        let actual = reg_frame::hex(&Sha256::digest(reg_frame::canonical_line(&list)));
        if actual != hash {
            return Err(format!("valueset {hash}: content hashes to {actual}"));
        }
    }
    Ok(())
}
