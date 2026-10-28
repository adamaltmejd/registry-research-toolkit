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
/// `valueset <hash>: content hashes to <actual>`, `doc <hash>: content hashes to <actual>`, or `previous release: <sql>`.
pub fn catalog(path: &Path, previous: Option<&Path>) -> Result<(), String> {
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
    schema_version(&db)?;
    types(&db)?;
    valuesets(&db)?;
    pdfs(&db)?;
    if let Some(previous) = previous {
        compare(&db, previous)?;
    }
    Ok(())
}

/// Use the same closed family map as resolution, including the F6 exemption.
fn types(db: &Connection) -> Result<(), String> {
    let sql = |e: rusqlite::Error| e.to_string();
    let mut stated = db
        .prepare("SELECT dtype FROM occurrence UNION SELECT dtype FROM segment")
        .map_err(sql)?;
    for dtype in stated
        .query_map([], |r| r.get::<_, Option<String>>(0))
        .map_err(sql)?
    {
        if let Some(dtype) = dtype.map_err(sql)?
            && reg_frame::type_family(&dtype).is_none()
        {
            return Err(format!("unmapped catalog dtype {dtype:?}"));
        }
    }
    let mut pairs = db
        .prepare(
            "SELECT o.id, o.dtype, s.dtype FROM occurrence o JOIN segment s ON s.id = o.segment_id
         WHERE s.state <> 'ambiguous' AND o.dtype IS NOT s.dtype
           AND NOT (s.dtype IS NULL AND EXISTS (
             SELECT 1 FROM finding f
             WHERE f.subject_kind = 'segment' AND f.subject_id = s.id
               AND f.code = 'F6:source-conflict' AND f.severity = 'review'
               AND f.start = s.start AND f.end = s.end))",
        )
        .map_err(sql)?;
    let mut rows = pairs.query([]).map_err(sql)?;
    while let Some(row) = rows.next().map_err(sql)? {
        let occurrence: Option<String> = row.get(1).map_err(sql)?;
        let segment: Option<String> = row.get(2).map_err(sql)?;
        if occurrence.as_deref().and_then(reg_frame::type_family)
            != segment.as_deref().and_then(reg_frame::type_family)
        {
            return Err(format!(
                "occurrence {}: type family disagrees with its segment",
                row.get::<_, i64>(0).map_err(sql)?
            ));
        }
    }
    Ok(())
}

/// `PRAGMA user_version` and `meta.schema_version` both equal the one constant.
fn schema_version(db: &Connection) -> Result<(), String> {
    let sql = |e: rusqlite::Error| e.to_string();
    let user: i64 = db
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .map_err(sql)?;
    let meta: Option<String> = db
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |r| r.get(0),
        )
        .map(Some)
        .or_else(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => Ok(None),
            e => Err(e),
        })
        .map_err(sql)?;
    let expected = reg_core::CATALOG_SCHEMA_VERSION;
    if user != i64::from(expected) || meta.as_deref() != Some(expected.to_string().as_str()) {
        return Err(format!(
            "schema version: user_version {user}, meta {}, expected {expected}",
            meta.as_deref().unwrap_or("(none)")
        ));
    }
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

/// Each stored pdf hashes to its address.
fn pdfs(db: &Connection) -> Result<(), String> {
    let sql = |e: rusqlite::Error| e.to_string();
    let mut stmt = db
        .prepare("SELECT hash, content FROM doc WHERE kind = 'pdf' ORDER BY id")
        .map_err(sql)?;
    let mut rows = stmt.query([]).map_err(sql)?;
    while let Some(row) = rows.next().map_err(sql)? {
        let hash: String = row.get(0).map_err(sql)?;
        let actual = reg_frame::hex(&Sha256::digest(
            row.get_ref(1)
                .map_err(sql)?
                .as_blob()
                .map_err(|e| e.to_string())?,
        ));
        if actual != hash {
            return Err(format!("doc {hash}: content hashes to {actual}"));
        }
    }
    Ok(())
}

/// Against the previous release, every id survives or is retired, and retirement is
/// permanent (schema v4 §4.3).
fn compare(db: &Connection, previous: &Path) -> Result<(), String> {
    let fail = |what: String| format!("previous release: {what}");
    // `#`, `?` and `%` are URI syntax: encode every byte but unreserved ones and `/`.
    let encoded: String = previous
        .to_string_lossy()
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
                char::from(b).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect();
    let uri = format!("file:{encoded}?mode=ro");
    db.execute("ATTACH DATABASE ?1 AS prev", [uri])
        .map_err(|e| fail(format!("{}: {e}", previous.display())))?;
    let version = |schema: &str| -> Result<Option<String>, String> {
        db.query_row(
            &format!("SELECT value FROM {schema}.meta WHERE key = 'schema_version'"),
            [],
            |r| r.get(0),
        )
        .map(Some)
        .or_else(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => Ok(None),
            e => Err(fail(format!("{}: {e}", previous.display()))),
        })
    };
    let current = version("main")?.ok_or_else(|| "meta has no schema_version".to_owned())?;
    match version("prev")? {
        Some(p) if p == current => {}
        other => {
            return Err(fail(format!(
                "schema {} is not comparable",
                other.as_deref().unwrap_or("(none)")
            )));
        }
    }
    for statement in [
        "SELECT p.pid FROM prev.ident p
          WHERE NOT EXISTS (SELECT 1 FROM main.ident c WHERE c.pid = p.pid AND c.kind = p.kind)
            AND NOT EXISTS (SELECT 1 FROM main.ident_retired r WHERE r.pid = p.pid)",
        "SELECT pid FROM prev.ident_retired EXCEPT SELECT pid FROM main.ident_retired",
    ] {
        let first: Option<String> = db
            .query_row(statement, [], |r| r.get(0))
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                e => Err(e.to_string()),
            })?;
        if let Some(pid) = first {
            return Err(fail(format!("{statement}; e.g. {pid}")));
        }
    }
    Ok(())
}
