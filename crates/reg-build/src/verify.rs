//! Artifact verification: the one authority for a written catalog file's invariants
//! (§4.2, §4.3). `emit` runs it before replacing the output; `reg-build verify`
//! runs it on any file.

use std::collections::{BTreeMap, BTreeSet};
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
    for statement in &statements(CHECKS) {
        if has_rows(db, statement)? {
            return Err(format!("post-write check failed:\n{statement}").into());
        }
    }
    types(db)?;
    valuesets(db)?;
    pdfs(db)?;
    Ok(())
}

/// One untyped segment's own occurrences, for the F6 check in [`types`].
#[derive(Default)]
struct Own {
    families: BTreeSet<&'static str>,
    /// Label key, value set, classification, source key: one entry when they agree.
    forms: BTreeSet<OwnForm>,
    /// An occurrence spelled other than the segment's folded spelling.
    off_spelling: bool,
}

type OwnForm = (Option<String>, Option<i64>, Option<i64>, String);

/// Use the same closed family map as resolution, including the F6 exemption. The
/// catalog holds only published names, never a reader or curation alias (#2071).
fn types(db: &Connection) -> Result<()> {
    let mut stated = db.prepare(
        "SELECT dtype FROM occurrence UNION SELECT dtype FROM segment
         UNION SELECT value FROM override WHERE field = 'type'",
    )?;
    for dtype in stated.query_map([], |r| r.get::<_, Option<String>>(0))? {
        if let Some(dtype) = dtype?
            && reg_frame::dtype_info(&dtype).and_then(|d| d.published) != Some(dtype.as_str())
        {
            return Err(format!("unpublished catalog dtype {dtype:?}").into());
        }
    }
    let family = |dtype: Option<&str>| {
        dtype.and_then(|t| reg_frame::dtype_info(t).expect("validated dtype").family)
    };
    // The F6 source conflict, as resolution decides it (reg-resolve, `type_conflicts`): an
    // untyped segment whose own occurrences state two or more known families but agree on
    // label (`label_key`), value set, classification and source key, each spelled as the
    // segment's folded spelling. A segment's occurrences are exactly the used ones
    // resolution segmented; any other disagreement leaves the segment ambiguous.
    let mut owns = BTreeMap::<i64, Own>::new();
    let mut stated = db.prepare(
        "SELECT o.segment_id, o.dtype, t.body, o.valueset_id, o.class_version_id,
                o.source_key, sp.fold_id IS NOT s.fold_id
         FROM occurrence o JOIN segment s ON s.id = o.segment_id
         LEFT JOIN text t ON t.id = o.label_text
         LEFT JOIN spelling sp ON sp.id = o.spelling_id
         WHERE s.dtype IS NULL",
    )?;
    let mut rows = stated.query([])?;
    while let Some(row) = rows.next()? {
        let own = owns.entry(row.get(0)?).or_default();
        if let Some(f) = family(row.get::<_, Option<String>>(1)?.as_deref()) {
            own.families.insert(f);
        }
        let label: Option<String> = row.get(2)?;
        own.forms.insert((
            label.as_deref().map(reg_core::label_key),
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
        ));
        own.off_spelling |= row.get::<_, bool>(6)?;
    }
    let conflicts: BTreeSet<i64> = owns
        .into_iter()
        .filter(|(_, own)| own.families.len() > 1 && own.forms.len() == 1 && !own.off_spelling)
        .map(|(segment, _)| segment)
        .collect();
    let mut pairs = db.prepare(
        "SELECT o.id, o.dtype, s.dtype, s.state, s.id
         FROM occurrence o JOIN segment s ON s.id = o.segment_id
         WHERE o.dtype IS NOT s.dtype",
    )?;
    let mut rows = pairs.query([])?;
    while let Some(row) = rows.next()? {
        let occurrence: Option<String> = row.get(1)?;
        let segment: Option<String> = row.get(2)?;
        // A published unknown is NULL (the alias check above refuses `unspecified`); a
        // known type beneath it fails the family check below unless ambiguity or an F6
        // conflict left it blank.
        let (own, published) = (family(occurrence.as_deref()), family(segment.as_deref()));
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
        let conflict = conflicts.contains(&row.get::<_, i64>(4)?);
        if state != "ambiguous" && own.is_some() && own != published && !conflict {
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

/// The statements of a SQL file: `--` comments dropped to the end of their line and the
/// text split at `;`, both outside string literals; trimmed, empty ones skipped.
pub(crate) fn statements(sql: &str) -> Vec<String> {
    let (mut out, mut statement, mut quoted) = (Vec::new(), String::new(), false);
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '-' if !quoted && chars.peek() == Some(&'-') => {
                while chars.next_if(|&c| c != '\n').is_some() {}
            }
            ';' if !quoted => out.push(std::mem::take(&mut statement)),
            _ => {
                quoted ^= c == '\'';
                statement.push(c);
            }
        }
    }
    out.push(statement);
    out.iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
}

fn has_rows(db: &Connection, statement: &str) -> Result<bool> {
    let mut stmt = db
        .prepare(statement)
        .map_err(|e| format!("{statement}: {e}"))?;
    Ok(stmt.exists([])?)
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
    // `schema_version` already equals the constant, so `current` parses.
    let current = version("main")?
        .and_then(|v| v.parse::<u32>().ok())
        .ok_or_else(|| "meta has no schema_version".to_owned())?;
    // An older schema stays comparable while the id queries below still run on it; a
    // newer one is never assumed to be.
    let prev_text = version("prev")?;
    let prev = match prev_text.as_deref().map(str::parse::<u32>) {
        Some(Ok(p)) if p <= current => p,
        _ => {
            return Err(fail(format!(
                "schema {} is not comparable",
                prev_text.as_deref().unwrap_or("(none)")
            ))
            .into());
        }
    };
    // A query error on the same schema is a broken file, not a schema gap.
    let incomparable = |e: crate::error::Error| {
        if prev == current {
            fail(e.into())
        } else {
            fail(format!("schema {prev} is not comparable: {e}"))
        }
    };
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
    if prev != current {
        println!(
            "previous release: cross-schema comparison: previous schema {prev}, current schema {current}"
        );
    }
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
        let pids = crate::error::rows(db, statement, [], |r| r.get::<_, String>(0))
            .map_err(incomparable)?;
        for pid in pids {
            if !pre_v1 {
                return Err(fail(format!("{change} {pid}")).into());
            }
            changes.push((pid.clone(), format!("{change} {pid}")));
        }
    }
    for moved in crate::id_targets::moved(db, "prev", db, "main").map_err(incomparable)? {
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
