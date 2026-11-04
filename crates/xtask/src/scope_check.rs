//! `cargo xtask scope-check <reg_meta_swecov.db>`: a holdings DB carries only what the
//! builder declares.
//!
//! The SWECOV catalog is public (maintainer 2026-11-03), so the paired holdings asset has
//! no disclosure review. What a release must not carry is background data: steward-repo
//! notes, raw census or storage exports, inventory working files. This check enforces
//! that mechanically. It lives here, not in `reg-build`, so the gate on the public asset
//! stays independent of the code that writes it (post-F ruling Q16).
//!
//! - Schema: every table (FTS shadow tables included) and column must equal what
//!   executing the builder's DDL yields in SQLite. An extra or missing table or column
//!   fails. `holding_relation` is optional: the builder emits it only when the steward
//!   declares external relations. The DDL is the builder's own source text, compiled in
//!   with `include_str!` (it is the declaration this check holds the file to); the meta
//!   allowlist and the size bounds below are kept here by hand, so a new key or a raised
//!   bound is a reviewed diff in this file.
//! - Meta: every key must be in [`META_KEYS`].
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
//! The DB is opened read-only. Output is one summary line and the failing items' names,
//! never row contents. Exit 1 on any failure, 2 on a usage or open error.

use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{Connection, OpenFlags};

const EXIT_FAIL: u8 = 1;
const EXIT_USAGE: u8 = 2;

const USAGE: &str = "usage: cargo xtask scope-check <reg_meta_swecov.db>";

/// The builder's DDL, in the order it creates the tables.
const DDL: [&str; 3] = [
    include_str!("../../reg-build/src/schema.sql"),
    include_str!("../../reg-build/src/holdings/relations.sql"),
    include_str!("../../reg-build/src/holdings/schema.sql"),
];
/// `holding_reference_occurrence` is created from a Rust string, not a `.sql` file.
const REFERENCES_RS: &str = include_str!("../../reg-build/src/holdings/references.rs");
const INLINE_DDL_START: &str = "\"CREATE TABLE holding_reference_occurrence ";

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

#[must_use]
pub fn main(args: &[String]) -> u8 {
    let [db] = args else {
        eprintln!("{USAGE}");
        return EXIT_USAGE;
    };
    match check(db) {
        Ok(failing) => {
            if failing {
                EXIT_FAIL
            } else {
                0
            }
        }
        Err(e) => {
            eprintln!("scope check: {e}");
            EXIT_USAGE
        }
    }
}

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

fn expected_schema() -> Result<Columns, String> {
    let inline = REFERENCES_RS
        .find(INLINE_DDL_START)
        .and_then(|start| {
            let ddl = &REFERENCES_RS[start + 1..];
            ddl.find(";\"").map(|end| &ddl[..=end])
        })
        .ok_or("holding_reference_occurrence DDL not found in holdings/references.rs")?;
    let con = Connection::open_in_memory().map_err(|e| e.to_string())?;
    con.execute_batch(&[DDL.join("\n").as_str(), inline].join("\n"))
        .map_err(|e| format!("the builder's DDL: {e}"))?;
    Ok(schema_of(&con).map_err(|e| e.to_string())?.0)
}

/// Prints the report; `Ok(true)` when anything fails.
fn check(path: &str) -> Result<bool, String> {
    let con = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|e| format!("{path}: {e}"))?;
    let want = expected_schema()?;
    let sql = |e: rusqlite::Error| format!("{path}: {e}");
    let (have, virtual_tables) = schema_of(&con).map_err(sql)?;

    let extra_tables: Vec<String> = have
        .keys()
        .filter(|t| !want.contains_key(*t))
        .cloned()
        .collect();
    let missing_tables: Vec<String> = want
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
    let mut too_big = oversize_columns(&con, &have, &virtual_tables).map_err(sql)?;

    let meta: Vec<(String, Option<i64>)> = if have.contains_key("meta") {
        con.prepare("SELECT key, length(CAST(value AS BLOB)) FROM meta")
            .and_then(|mut s| {
                s.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
                    .collect::<Result<_, _>>()
            })
            .map_err(sql)?
    } else {
        Vec::new()
    };
    let unknown_keys: Vec<String> = meta
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
            && con
                .query_row(&format!("SELECT EXISTS(SELECT 1 FROM \"{t}\")"), [], |r| {
                    r.get::<_, bool>(0)
                })
                .map_err(sql)?
        {
            filled.push(t.to_owned());
        }
    }

    let leaked = if have.contains_key("text") {
        working_references(&con).map_err(sql)?
    } else {
        Vec::new()
    };

    let items = [
        ("extra table", extra_tables),
        ("missing table", missing_tables),
        ("extra column", extra_columns),
        ("missing column", missing_columns),
        ("unknown meta key", unknown_keys),
        ("oversize value", too_big),
        ("non-empty table", filled),
        ("working-file reference", leaked),
    ];
    let columns = have.values().map(BTreeSet::len).sum::<usize>();
    let counts = format!(
        "tables={} columns={columns} meta_keys={}",
        have.len(),
        meta.len()
    );
    Ok(report(&counts, items))
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

/// Prints the summary line and each failing item's name; `true` when any fails.
fn report(counts: &str, mut items: [(&str, Vec<String>); 8]) -> bool {
    let failing: usize = items.iter().map(|(_, v)| v.len()).sum();
    let per_class: Vec<String> = items
        .iter()
        .map(|(label, v)| format!("{}s={}", label.replace(' ', "_"), v.len()))
        .collect();
    println!(
        "scope check {}: {counts} {}",
        if failing > 0 { "FAIL" } else { "ok" },
        per_class.join(" ")
    );
    for (label, names) in &mut items {
        names.sort();
        for name in names.iter() {
            println!("  {label}: {name}");
        }
    }
    failing > 0
}

/// Python's `\w` on a `str`: alphanumeric or underscore.
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
