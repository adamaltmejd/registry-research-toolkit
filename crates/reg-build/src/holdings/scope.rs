//! A holdings DB carries only what the builder declares (release step 8e).
//!
//! The holdings asset is public (maintainer 2026-11-03), so it has no disclosure
//! review. What it must not carry is background data: steward-repo notes, raw census or
//! storage exports, inventory working files. The pair verifier enforces that here, in
//! the released `reg-build`, because a steward repository verifies its own pair with
//! that binary alone (the steward-repository move, R15.4; this supersedes the post-F
//! Q16 choice of an xtask command).
//!
//! - Schema: every table (FTS shadow tables included) and column must equal what
//!   executing the builder's DDL yields in SQLite. An extra or missing table or column
//!   fails. `holding_relation` is optional: the builder emits it only when the steward
//!   declares external relations.
//! - Meta: every key must be in [`META_KEYS`]. The allowlist and the size bounds are
//!   kept here by hand, so a new key or a raised bound is a reviewed diff in this file.
//! - Rows: tables the steward build must leave empty ([`EMPTY_TABLES`]) have no rows. The
//!   semantic part keeps the catalog schema, but its source locators point into steward
//!   working files.
//! - Size: every TEXT, BLOB or untyped column of every table stays under its bound, so a
//!   raw payload in an allowed column is caught.
//! - Text: no `text` body names a steward working file (`*.json`, `*.jsonl`, `*.toml`)
//!   or a census row (`census:row=`). The reader serves these bodies as labels,
//!   definitions and notes, so a provenance suffix in a transcription cell reaches the
//!   public site (0.72.0: "Evidence: reference-occurrence-evidence.jsonl;
//!   census:row=489; ...").
//!
//! A refusal names each failing item (a table, a column, a key or a text id), never row
//! contents.

use std::collections::{BTreeMap, BTreeSet};

use rusqlite::Connection;

use crate::error::Result;

/// The builder's DDL, in the order it creates the tables.
const DDL: [&str; 3] = [
    include_str!("../schema.sql"),
    include_str!("relations.sql"),
    include_str!("schema.sql"),
];

const OPTIONAL_TABLES: [&str; 1] = ["holding_relation"];

/// Every meta key a holdings DB carries: the base catalog's keys plus the steward's.
const META_KEYS: [&str; 30] = [
    "schema_version",          // base: schema version
    "builder_version",         // base: builder provenance
    "catalog_version",         // base: release label
    "generation_id",           // base: content digest
    "curation_digest",         // base: curation digest
    "sqlite_version",          // base: SQLite that built the file
    "period_grammar",          // base: period grammar the reader parses
    "delivery_name_grammar",   // base: delivery-name grammar
    "open_start",              // base: open-interval sentinel
    "open_end",                // base: open-interval sentinel
    "semantic_schema_version", // holdings: schema of the semantic build
    "semantic_generation",     // holdings: provenance of the semantic build
    "semantic_sha256",         // holdings: digest of the semantic build
    "catalog_generation",      // holdings: the paired base catalog
    "catalog_sha256",          // holdings: the paired base catalog
    "owner_kind",              // holdings: owner of the file
    "owner_id",                // holdings: owner of the file
    "steward_repo",            // holdings: provenance, steward repository
    "steward_commit",          // holdings: provenance, steward commit
    "steward_config",          // holdings: provenance, steward config path
    "compiler_digest",         // holdings: provenance, compiler build
    "input_manifest_sha256",   // holdings: provenance, digest of the pinned input list
    "steward",                 // holdings: steward presentation, shown in the SPA
    "scope",                   // holdings: the steward's public scope document
    "logo",                    // holdings: steward logo, shown in the SPA
    "census_tables",           // holdings: aggregate count
    "census_columns",          // holdings: aggregate count
    "mapped_columns",          // holdings: aggregate count
    "unmatched_columns",       // holdings: aggregate count
    "storage_extra_columns",   // holdings: aggregate count
];

/// Size bounds in bytes, set from the 0.71.0 preview DB's maxima with margin: no ordinary
/// TEXT column exceeds 918; a raw export is larger than any of these.
const DEFAULT_BOUND: i64 = 2048;
const BOUNDS: [(&str, i64); 4] = [
    // FTS5 index pages (preview max 4 053), page size 4 096 by default.
    ("doc_fts_data.block", 16_384),
    ("search_data.block", 16_384),
    ("search_description_data.block", 16_384),
    ("search_exact_data.block", 16_384), // exact-match ranking index (#2167)
];
/// Bounded per key, below.
const PER_KEY: &str = "meta.value";
const META_BOUND: i64 = 4096; // preview max 1 755 (`logo`)
const META_BOUNDS: [(&str, i64); 1] = [
    ("scope", 120_000), // the steward's public scope document (preview 81 974)
];

/// Catalog-schema tables a steward build emits empty: rows would name working files.
const EMPTY_TABLES: [&str; 1] = ["occurrence_locator"];

type Columns = BTreeMap<String, BTreeSet<(String, String)>>;

/// Tables with their (column, upper-cased type) sets, and the names of the virtual tables.
fn schema_of(con: &Connection) -> rusqlite::Result<(Columns, BTreeSet<String>)> {
    let tables: Vec<(String, String)> = con
        .prepare(
            "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT GLOB 'sqlite_*'",
        )?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<Result<_, _>>()?;
    let mut cols = Columns::new();
    for (t, _) in &tables {
        let set = con
            .prepare(&format!("PRAGMA table_xinfo(\"{t}\")"))?
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?.to_uppercase(),
                ))
            })?
            .collect::<Result<_, _>>()?;
        cols.insert(t.clone(), set);
    }
    let virtual_tables = tables
        .into_iter()
        .filter(|(_, sql)| sql.starts_with("CREATE VIRTUAL"))
        .map(|(t, _)| t)
        .collect();
    Ok((cols, virtual_tables))
}

fn expected_schema() -> Result<Columns> {
    let con = Connection::open_in_memory()?;
    con.execute_batch(&DDL.join("\n"))
        .map_err(|e| format!("the builder's DDL: {e}"))?;
    Ok(schema_of(&con)?.0)
}

/// Refuses with one `holdings_content_<class>: <item>` line per failing item.
pub(super) fn check(con: &Connection) -> Result<()> {
    let want = expected_schema()?;
    let (have, virtual_tables) = schema_of(con)?;

    let extra_tables = have
        .keys()
        .filter(|t| !want.contains_key(*t))
        .cloned()
        .collect();
    let missing_tables = want
        .keys()
        .filter(|t| !have.contains_key(*t) && !OPTIONAL_TABLES.contains(&t.as_str()))
        .cloned()
        .collect();
    let (mut extra_columns, mut missing_columns) = (Vec::new(), Vec::new());
    for (t, have_cols) in &have {
        let Some(want_cols) = want.get(t) else {
            continue;
        };
        let named = |(c, ty): &(String, String)| format!("{t}.{c} {ty}");
        extra_columns.extend(have_cols.difference(want_cols).map(named));
        missing_columns.extend(want_cols.difference(have_cols).map(named));
    }
    let mut too_big = oversize_columns(con, &have, &virtual_tables)?;

    let meta: Vec<(String, Option<i64>)> = if have.contains_key("meta") {
        crate::error::rows(
            con,
            "SELECT key, length(CAST(value AS BLOB)) FROM meta",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?
    } else {
        Vec::new()
    };
    let unknown_keys = meta
        .iter()
        .filter(|(k, _)| !META_KEYS.contains(&k.as_str()))
        .map(|(k, _)| k.clone())
        .collect();
    for (k, n) in &meta {
        let bound = META_BOUNDS
            .iter()
            .find(|(key, _)| key == k)
            .map_or(META_BOUND, |(_, b)| *b);
        if let Some(n) = n.filter(|n| *n > bound) {
            too_big.push(format!("meta.{k} ({n} > {bound} bytes)"));
        }
    }

    let mut filled = Vec::new();
    for t in EMPTY_TABLES {
        if have.contains_key(t)
            && con.query_row(&format!("SELECT EXISTS(SELECT 1 FROM \"{t}\")"), [], |r| {
                r.get::<_, bool>(0)
            })?
        {
            filled.push(t.to_owned());
        }
    }

    let leaked = if have.contains_key("text") {
        working_references(con)?
    } else {
        Vec::new()
    };

    let mut items: [(&str, Vec<String>); 8] = [
        ("extra_table", extra_tables),
        ("missing_table", missing_tables),
        ("extra_column", extra_columns),
        ("missing_column", missing_columns),
        ("meta_key", unknown_keys),
        ("oversize", too_big),
        ("nonempty_table", filled),
        ("working_file", leaked),
    ];
    let mut lines = Vec::new();
    for (class, names) in &mut items {
        names.sort();
        lines.extend(
            names
                .iter()
                .map(|name| format!("holdings_content_{class}: {name}")),
        );
    }
    if lines.is_empty() {
        Ok(())
    } else {
        Err(lines.join("\n").into())
    }
}

/// TEXT, BLOB and untyped columns over their bound, with the size found.
fn oversize_columns(
    con: &Connection,
    have: &Columns,
    virtual_tables: &BTreeSet<String>,
) -> rusqlite::Result<Vec<String>> {
    let mut too_big = Vec::new();
    for (t, cols) in have {
        if virtual_tables.contains(t) {
            // Their rows live in the shadow tables, checked here too.
            continue;
        }
        for (c, ty) in cols {
            let column = format!("{t}.{c}");
            if !matches!(ty.as_str(), "TEXT" | "BLOB" | "") || column == PER_KEY {
                continue;
            }
            let bound = BOUNDS
                .iter()
                .find(|(name, _)| *name == column)
                .map_or(DEFAULT_BOUND, |(_, b)| *b);
            let n: Option<i64> = con.query_row(
                &format!("SELECT max(length(CAST(\"{c}\" AS BLOB))) FROM \"{t}\""),
                [],
                |r| r.get(0),
            )?;
            if let Some(n) = n.filter(|n| *n > bound) {
                too_big.push(format!("{column} ({n} > {bound} bytes)"));
            }
        }
    }
    Ok(too_big)
}

/// `text` rows naming a working file, by text id and the reference's kind, never the body.
fn working_references(con: &Connection) -> rusqlite::Result<Vec<String>> {
    let mut stmt = con.prepare("SELECT id, body FROM text")?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))?;
    let mut leaked = Vec::new();
    for row in rows {
        let (id, body) = row?;
        if let Some(kind) = working_reference(&body) {
            leaked.push(format!("text:{id} ({kind})"));
        }
    }
    Ok(leaked)
}

/// Regex `\w`: alphanumeric or underscore.
fn is_word(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}
/// The first steward working-file name (`[\w./-]+\.(jsonl?|toml)\b`) or census row
/// locator (`census:row=`) in `body`, as the reported kind: `json`, `jsonl`, `toml` or
/// `census:row`. Leftmost match first; at one start, the file name wins and its last
/// valid extension counts, as a backtracking regex would pick them.
fn working_reference(body: &str) -> Option<&'static str> {
    let chars: Vec<char> = body.chars().collect();
    let in_name = |c: char| is_word(c) || matches!(c, '.' | '/' | '-');
    // The extension starting at `at`, followed by a word boundary.
    let extension = |at: usize| {
        ["jsonl", "json", "toml"].into_iter().find(|ext| {
            let end = at + ext.len();
            end <= chars.len()
                && chars[at..end].iter().copied().eq(ext.chars())
                && chars.get(end).is_none_or(|c| !is_word(*c))
        })
    };
    let census: Vec<char> = "census:row=".chars().collect();
    // A name run that held no file name at its first character holds none later on.
    let mut run_end = 0;
    for start in 0..chars.len() {
        if start >= run_end && in_name(chars[start]) {
            run_end = chars[start..]
                .iter()
                .position(|c| !in_name(*c))
                .map_or(chars.len(), |n| start + n);
            // `[\w./-]+` takes at least one character before the dot.
            if let Some(ext) = (start + 1..run_end)
                .rev()
                .filter(|&dot| chars[dot] == '.')
                .find_map(|dot| extension(dot + 1))
            {
                return Some(ext);
            }
        }
        if chars[start..].starts_with(&census) {
            return Some("census:row");
        }
    }
    None
}
