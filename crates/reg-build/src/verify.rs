//! Artifact verification: the one authority for a written catalog file's invariants
//! (§4.2, §4.3). `emit` runs it before replacing the output; `reg-build verify`
//! runs it on any file.

use std::path::Path;

use crate::error::Result;
use reg_frame::Item;
use rusqlite::{Connection, OpenFlags, OptionalExtension};

const CHECKS: &str = include_str!("checks.sql");

/// Every invariant of a written catalog file (§4.2, §4.3). Read-only.
///
/// # Errors
/// The first failure, naming the check: `integrity_check: …`, `post-write check failed:\n<sql>`,
/// `valueset <hash>: content hashes to <actual>`, `doc <hash>: content hashes to <actual>`, or `previous release: <sql>`.
pub fn catalog(path: &Path, previous: Option<&Path>) -> Result<()> {
    let db = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )?;
    semantic(&db)?;
    schema_version(&db)?;
    if let Some(previous) = previous {
        compare(&db, previous)?;
    }
    Ok(())
}

/// Shared semantic-table checks for a base catalog and a supplemental catalog.
pub(crate) fn semantic(db: &Connection) -> Result<()> {
    let integrity: String = db.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
    if integrity != "ok" {
        return Err(format!("integrity_check: {integrity}").into());
    }
    let checks = CHECKS
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    for statement in checks.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        if has_rows(db, statement)? {
            return Err(format!("post-write check failed:\n{statement}").into());
        }
    }
    types(db)?;
    valuesets(db)?;
    pdfs(db)?;
    Ok(())
}

/// Use the same closed family map as resolution, including the F6 exemption.
fn types(db: &Connection) -> Result<()> {
    let mut stated = db.prepare("SELECT dtype FROM occurrence UNION SELECT dtype FROM segment")?;
    for dtype in stated.query_map([], |r| r.get::<_, Option<String>>(0))? {
        if let Some(dtype) = dtype?
            && reg_frame::dtype_info(&dtype).is_none()
        {
            return Err(format!("unmapped catalog dtype {dtype:?}").into());
        }
    }
    let mut pairs = db.prepare(
        "SELECT o.id, o.dtype, s.dtype, s.state, EXISTS (
             SELECT 1 FROM finding f
             WHERE f.subject_kind = 'segment' AND f.subject_id = s.id
               AND f.code = 'F6:source-conflict' AND f.severity = 'review'
               AND f.start = s.start AND f.end = s.end)
         FROM occurrence o JOIN segment s ON s.id = o.segment_id
         WHERE o.dtype IS NOT s.dtype",
    )?;
    let mut rows = pairs.query([])?;
    while let Some(row) = rows.next()? {
        let occurrence: Option<String> = row.get(1)?;
        let segment: Option<String> = row.get(2)?;
        let family = |dtype: Option<&str>| {
            dtype.and_then(|t| reg_frame::dtype_info(t).expect("validated dtype").family)
        };
        let (own, published) = (family(occurrence.as_deref()), family(segment.as_deref()));
        if own.is_some() && segment.is_some() && published.is_none() {
            return Err(format!(
                "occurrence {}: known type beneath a published unknown type",
                row.get::<_, i64>(0)?
            )
            .into());
        }
        if own.is_some()
            && own == published
            && reg_frame::widest_dtype(occurrence.as_deref().into_iter().chain(segment.as_deref()))
                != segment.as_deref()
        {
            return Err(format!(
                "occurrence {}: segment type is narrower than its source",
                row.get::<_, i64>(0)?
            )
            .into());
        }
        let state: String = row.get(3)?;
        let exempt: bool = row.get(4)?;
        if state != "ambiguous"
            && own.is_some()
            && own != published
            && !(segment.is_none() && exempt)
        {
            return Err(format!(
                "occurrence {}: type family disagrees with its segment",
                row.get::<_, i64>(0)?
            )
            .into());
        }
    }
    Ok(())
}

/// `PRAGMA user_version` and `meta.schema_version` both equal the one constant.
fn schema_version(db: &Connection) -> Result<()> {
    let user: i64 = db.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    let meta: Option<String> = db
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |r| r.get(0),
        )
        .optional()?;
    let expected = reg_core::CATALOG_SCHEMA_VERSION;
    if user != i64::from(expected) || meta.as_deref() != Some(expected.to_string().as_str()) {
        return Err(format!(
            "schema version: user_version {user}, meta {}, expected {expected}",
            meta.as_deref().unwrap_or("(none)")
        )
        .into());
    }
    Ok(())
}

fn has_rows(db: &Connection, statement: &str) -> Result<bool> {
    let mut stmt = db
        .prepare(statement)
        .map_err(|e| format!("{statement}: {e}"))?;
    let mut found = stmt.query([])?;
    Ok(found.next()?.is_some())
}

/// Each value set's items hash to its address, the way `reg_frame::ValueSet::new` does.
fn valuesets(db: &Connection) -> Result<()> {
    let mut sets = db.prepare("SELECT id, hash FROM valueset ORDER BY id")?;
    let mut items = db.prepare(
        "SELECT c.body, l.body, i.parent_ord
               FROM vs_item i
               JOIN text c ON c.id = i.code_text
               LEFT JOIN text l ON l.id = i.label_text
              WHERE i.valueset_id = ?1 ORDER BY i.ord",
    )?;
    let sets = sets
        .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    for (id, hash) in sets {
        let rows = items
            .query_map([id], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    r.get::<_, Option<u32>>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
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
        let actual = reg_core::sha256_hex(reg_frame::canonical_line(&list));
        if actual != hash {
            return Err(format!("valueset {hash}: content hashes to {actual}").into());
        }
    }
    Ok(())
}

/// Each stored pdf hashes to its address.
fn pdfs(db: &Connection) -> Result<()> {
    let mut stmt = db.prepare("SELECT hash, content FROM doc WHERE kind = 'pdf' ORDER BY id")?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let hash: String = row.get(0)?;
        let actual = reg_core::sha256_hex(row.get_ref(1)?.as_blob()?);
        if actual != hash {
            return Err(format!("doc {hash}: content hashes to {actual}").into());
        }
    }
    Ok(())
}

/// Ids published from v1 survive on overlapping targets or are permanently retired.
/// Earlier releases report these changes without refusing them.
fn compare(db: &Connection, previous: &Path) -> Result<()> {
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
    let version = |schema: &str| -> Result<Option<String>> {
        Ok(db
            .query_row(
                &format!("SELECT value FROM {schema}.meta WHERE key = 'schema_version'"),
                [],
                |r| r.get(0),
            )
            .optional()
            .map_err(|e| fail(format!("{}: {e}", previous.display())))?)
    };
    let current = version("main")?.ok_or_else(|| "meta has no schema_version".to_owned())?;
    match version("prev")? {
        Some(p) if p == current => {}
        other => {
            return Err(fail(format!(
                "schema {} is not comparable",
                other.as_deref().unwrap_or("(none)")
            ))
            .into());
        }
    }
    let catalog_version: String = db
        .query_row(
            "SELECT value FROM prev.meta WHERE key = 'catalog_version'",
            [],
            |r| r.get(0),
        )
        .map_err(|e| fail(e.to_string()))?;
    let pre_v1 = catalog_version
        .split('.')
        .next()
        .and_then(|major| major.parse::<u64>().ok())
        .is_none_or(|major| major == 0);
    let mut changes = Vec::new();
    for (change, statement) in [
        ("removed id", "SELECT p.pid FROM prev.ident p
          WHERE NOT EXISTS (SELECT 1 FROM main.ident c WHERE c.pid = p.pid AND c.kind = p.kind)
            AND NOT EXISTS (SELECT 1 FROM main.ident_retired r WHERE r.pid = p.pid AND r.kind = p.kind)"),
        ("removed id", "SELECT p.pid FROM prev.var_set p
          WHERE p.kind = 'tag' AND NOT EXISTS (SELECT 1 FROM main.var_set c WHERE c.pid = p.pid AND c.kind = p.kind)
            AND NOT EXISTS (SELECT 1 FROM main.ident_retired r WHERE r.pid = p.pid AND r.kind = p.kind)"),
        ("removed retirement", "SELECT pid FROM prev.ident_retired EXCEPT SELECT pid FROM main.ident_retired"),
    ] {
        let mut stmt = db.prepare(statement)?;
        let pids = stmt.query_map([], |r| r.get::<_, String>(0))?;
        for pid in pids {
            let pid = pid?;
            if !pre_v1 {
                return Err(fail(format!("{change} {pid}")).into());
            }
            changes.push((pid.clone(), format!("{change} {pid}")));
        }
    }
    for moved in crate::id_targets::moved(db, "prev", db, "main").map_err(|e| fail(e.into()))? {
        if !pre_v1 {
            return Err(fail(format!(
                "moved id {}; retire the published id and mint a new one",
                moved.text()
            ))
            .into());
        }
        changes.push((moved.pid.clone(), format!("moved id {}", moved.text())));
    }
    if pre_v1 {
        changes.sort();
        for (_, change) in &changes {
            println!("previous release (pre-v1): {change}");
        }
        println!("previous release (pre-v1): id changes: {}", changes.len());
    }
    Ok(())
}
