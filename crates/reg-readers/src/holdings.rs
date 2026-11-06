//! Metadata-only physical census and storage export. Errors identify input roles and
//! source rows, never reproduce private table names, column names or source values.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::Deserialize;

use crate::{ReadError, config, error};

/// Reader vocabulary, evidenced exact-name renames and literal prefix rewrites. Both
/// affect matching only; the artifact retains exact trimmed physical identifiers. An
/// optional schema header keys tables by (schema, table) instead of table alone.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    #[serde(default = "table_header")]
    census_table: String,
    #[serde(default = "column_prefix")]
    census_columns: String,
    census_schema: Option<String>,
    #[serde(default)]
    rename: Vec<Rename>,
    #[serde(default)]
    rewrite: Vec<Rewrite>,
}

fn table_header() -> String {
    "Table".to_owned()
}

fn column_prefix() -> String {
    "V".to_owned()
}

/// One exact physical name matched as one catalog name (`catalog`) or as one catalog
/// variable (`variable`, an FQID), exactly one of them. Looked up on the trimmed
/// physical name before any rewrite; a renamed column skips every rewrite.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Rename {
    physical: String,
    catalog: Option<String>,
    variable: Option<String>,
    evidence: String,
}

/// What a census column is matched against in its routed tables.
#[derive(Clone)]
pub enum Match {
    /// A name folded against each segment's documented spelling.
    Spelling(String),
    /// A catalog variable: its segments match whatever their spelling, so the period
    /// picks the spelling (`FAMID` → `FAMID90` in 1990, `FAMID91` in 1991, …).
    Variable(String),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Rewrite {
    prefix: String,
    replacement: String,
    evidence: String,
}

/// One accounted physical table; no observation records are read.
pub struct Table {
    /// The census schema cell; `Some` exactly when the reader config names a schema
    /// header, and then the table's identity is (schema, name).
    pub schema: Option<String>,
    pub name: String,
    pub source_row: usize,
    pub columns: Vec<Column>,
}

pub struct ReadOutput {
    pub tables: Vec<Table>,
    /// Storage coordinates absent from the admitted census (including extra tables).
    pub storage_extra_columns: usize,
    /// Every `[[rename]]` target, in config order, for the compiler to resolve.
    pub rename_targets: Vec<Match>,
}

/// A census column and every exact storage candidate. Missing/multiple candidates
/// are retained so the compiler can refuse them only when the column is orderable.
pub struct Column {
    pub name: String,
    pub ordinal: i64,
    pub match_name: Match,
    pub storage: Vec<Storage>,
}

#[derive(Clone)]
pub struct Storage {
    pub schema_name: String,
    pub table_type: String,
    pub data_type: String,
    pub length: Option<i64>,
    pub precision: Option<i64>,
    pub scale: Option<i64>,
}

fn at(role: &str, row: usize, message: &str) -> ReadError {
    error(role, format!("row={row}: {message}"))
}

fn reader(path: &Path, role: &str) -> Result<csv::Reader<std::fs::File>, ReadError> {
    csv::Reader::from_path(path).map_err(|_| error(role, "cannot open CSV"))
}

fn headers(csv: &mut csv::Reader<std::fs::File>, role: &str) -> Result<Vec<String>, ReadError> {
    let headers: Vec<String> = csv
        .headers()
        .map_err(|_| error(role, "cannot read CSV header"))?
        .iter()
        .map(|s| s.trim().trim_start_matches('\u{feff}').to_owned())
        .collect();
    if headers.iter().collect::<BTreeSet<_>>().len() != headers.len() {
        return Err(error(role, "duplicate CSV header"));
    }
    Ok(headers)
}

fn storage(path: &Path) -> Result<BTreeMap<(String, String), Vec<Storage>>, ReadError> {
    const ROLE: &str = "storage_export";
    const REQUIRED: [&str; 9] = [
        "table_schema",
        "table_name",
        "table_type",
        "column_name",
        "data_type",
        "character_maximum_length",
        "numeric_precision",
        "numeric_scale",
        "ordinal_position",
    ];
    let mut csv = reader(path, ROLE)?;
    let names = headers(&mut csv, ROLE)?;
    let indices: Vec<usize> = REQUIRED
        .iter()
        .map(|required| {
            names
                .iter()
                .position(|h| h == required)
                .ok_or_else(|| error(ROLE, format!("missing CSV header {required}")))
        })
        .collect::<Result<_, _>>()?;
    let mut out: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for (index, record) in csv.records().enumerate() {
        let row = index + 2;
        let record = record.map_err(|_| at(ROLE, row, "malformed CSV record"))?;
        let value = |n: usize| record[indices[n]].trim();
        if (0..5).any(|n| value(n).is_empty()) {
            return Err(at(ROLE, row, "missing required storage identifier/type"));
        }
        let number = |n: usize| -> Result<Option<i64>, ReadError> {
            let value = value(n);
            if value.is_empty() || value.eq_ignore_ascii_case("null") {
                Ok(None)
            } else {
                value
                    .parse()
                    .map(Some)
                    .map_err(|_| at(ROLE, row, "invalid numeric storage fact"))
            }
        };
        if number(8)?.is_none_or(|n| n <= 0) {
            return Err(at(ROLE, row, "ordinal_position must be positive"));
        }
        let length = number(5)?;
        let precision = number(6)?;
        let scale = number(7)?;
        if !matches!(value(2), "BASE TABLE" | "VIEW")
            || length.is_some_and(|n| n < -1)
            || precision.is_some_and(|n| n <= 0)
            || scale.is_some_and(|n| n < 0)
            || precision.zip(scale).is_some_and(|(p, s)| s > p)
        {
            return Err(at(
                ROLE,
                row,
                "invalid storage shape, length or precision/scale",
            ));
        }
        out.entry((value(1).to_owned(), value(3).to_owned()))
            .or_default()
            .push(Storage {
                schema_name: value(0).to_owned(),
                table_type: value(2).to_owned(),
                data_type: value(4).to_owned(),
                length,
                precision,
                scale,
            });
    }
    Ok(out)
}

/// Read the physical census and storage metadata using an explicit reader config.
/// Storage tables outside the census are never admitted as holdings.
///
/// # Errors
/// Malformed CSV/config, duplicate census coordinates, invalid ordinals or empty
/// rewrite evidence. No error contains a private source row or identifier.
pub fn read(
    census: &Path,
    storage_export: &Path,
    config_path: &Path,
) -> Result<ReadOutput, ReadError> {
    let config: Config = config(config_path)?;
    if config.census_table.is_empty()
        || config.census_columns.is_empty()
        || config.census_schema.as_ref().is_some_and(String::is_empty)
        || config
            .rewrite
            .iter()
            .any(|r| r.prefix.is_empty() || r.evidence.trim().is_empty())
    {
        return Err(error(
            "holdings_reader",
            "empty header/prefix or rewrite evidence",
        ));
    }
    let (renames, rename_targets) = renames(&config.rename)?;
    let facts = storage(storage_export)?;
    let mut csv = reader(census, "census")?;
    let names = headers(&mut csv, "census")?;
    let position = |header: &str, missing: &str| {
        names
            .iter()
            .position(|h| h == header)
            .ok_or_else(|| error("census", missing))
    };
    let table = position(&config.census_table, "missing table header")?;
    let schema_column = config
        .census_schema
        .as_deref()
        .map(|header| position(header, "missing schema header"))
        .transpose()?;
    let columns = column_headers(&names, &config.census_columns)?;
    let mut tables = BTreeMap::new();
    let mut matched_storage = BTreeSet::new();
    for (row, record) in (2..).zip(csv.records()) {
        let record = record.map_err(|_| at("census", row, "malformed CSV record"))?;
        let name = record[table].trim();
        let schema = schema_column.map(|index| record[index].trim().to_owned());
        if name.is_empty() || schema.as_ref().is_some_and(String::is_empty) {
            return Err(at("census", row, "missing table identifier"));
        }
        let mut seen = BTreeSet::new();
        let mut physical = Vec::new();
        for &(index, ordinal) in &columns {
            let column = record[index].trim();
            if column.is_empty() {
                continue;
            }
            if !seen.insert(column) {
                return Err(at("census", row, "duplicate physical column"));
            }
            let match_name = match_name(column, &renames, &config.rewrite);
            if matches!(&match_name, Match::Spelling(name) if name.is_empty()) {
                return Err(at(
                    "census",
                    row,
                    "rewrite removed entire column identifier",
                ));
            }
            physical.push(Column {
                name: column.to_owned(),
                ordinal,
                match_name,
                // Every schema's facts; the compiler keeps the authored schema's.
                storage: facts
                    .get(&(name.to_owned(), column.to_owned()))
                    .cloned()
                    .unwrap_or_default(),
            });
            matched_storage.insert((schema.clone(), name.to_owned(), column.to_owned()));
        }
        if tables
            .insert(
                (schema.clone(), name.to_owned()),
                Table {
                    schema,
                    name: name.to_owned(),
                    source_row: row,
                    columns: physical,
                },
            )
            .is_some()
        {
            return Err(at("census", row, "duplicate physical table"));
        }
    }
    Ok(ReadOutput {
        tables: tables.into_values().collect(),
        storage_extra_columns: storage_coordinates(&facts, schema_column.is_some())
            .difference(&matched_storage)
            .count(),
        rename_targets,
    })
}

/// The numbered physical-column headers as (header index, ordinal), by ordinal.
fn column_headers(names: &[String], prefix: &str) -> Result<Vec<(usize, i64)>, ReadError> {
    let mut columns = Vec::new();
    let mut ordinals = BTreeSet::new();
    for (index, name) in names.iter().enumerate() {
        if let Some(suffix) = name.strip_prefix(prefix) {
            let ordinal = suffix
                .parse::<i64>()
                .map_err(|_| error("census", "invalid column ordinal header"))?;
            if ordinal <= 0 || !ordinals.insert(ordinal) {
                return Err(error("census", "duplicate or nonpositive column ordinal"));
            }
            columns.push((index, ordinal));
        }
    }
    if columns.is_empty() {
        return Err(error("census", "no physical column headers"));
    }
    columns.sort_by_key(|&(_, ordinal)| ordinal);
    Ok(columns)
}

/// Every distinct storage coordinate, (schema, table, column) for a schema-keyed census
/// and (table, column) otherwise.
fn storage_coordinates(
    facts: &BTreeMap<(String, String), Vec<Storage>>,
    keyed: bool,
) -> BTreeSet<(Option<String>, String, String)> {
    facts
        .iter()
        .flat_map(|((table, column), storages)| {
            storages.iter().map(move |s| {
                (
                    keyed.then(|| s.schema_name.clone()),
                    table.clone(),
                    column.clone(),
                )
            })
        })
        .collect()
}

/// Admit `[[rename]]` as an exact physical → target map, and every target in config
/// order. Errors locate entries by their 1-based position in the config, never by name.
fn renames(entries: &[Rename]) -> Result<(BTreeMap<&str, Match>, Vec<Match>), ReadError> {
    const ROLE: &str = "holdings_reader";
    let mut out = BTreeMap::new();
    let mut targets = Vec::new();
    for (n, entry) in (1..).zip(entries) {
        let target = match (&entry.catalog, &entry.variable) {
            (Some(catalog), None) if !catalog.trim().is_empty() => {
                Some(Match::Spelling(catalog.clone()))
            }
            (None, Some(variable)) if !variable.is_empty() && variable.trim() == variable => {
                Some(Match::Variable(variable.clone()))
            }
            _ => None,
        };
        let Some(target) = target.filter(|_| {
            !entry.physical.is_empty()
                && entry.physical.trim() == entry.physical
                && !entry.evidence.trim().is_empty()
        }) else {
            return Err(error(
                ROLE,
                format!(
                    "census_rename_invalid: rename {n}: empty or untrimmed name, not exactly one of catalog and variable, or empty evidence"
                ),
            ));
        };
        if out
            .insert(entry.physical.as_str(), target.clone())
            .is_some()
        {
            return Err(error(
                ROLE,
                format!("census_rename_duplicate: rename {n}: physical name already renamed"),
            ));
        }
        targets.push(target);
    }
    // Renames apply once, never transitively; a catalog name that is itself a renamed
    // physical name reads as a chain, so it refuses rather than guessing the intent. A
    // variable target is an FQID, never a physical name.
    if let Some(n) = (1..)
        .zip(&targets)
        .find(|(_, t)| matches!(t, Match::Spelling(name) if out.contains_key(name.as_str())))
        .map(|(n, _)| n)
    {
        return Err(error(
            ROLE,
            format!(
                "census_rename_chain: rename {n}: catalog name is also a renamed physical name"
            ),
        ));
    }
    Ok((out, targets))
}

/// What a physical column is matched by: its `[[rename]]` target when it has one, and
/// then no rewrite applies; otherwise its name after every `[[rewrite]]` in config order.
fn match_name(column: &str, renames: &BTreeMap<&str, Match>, rewrites: &[Rewrite]) -> Match {
    if let Some(target) = renames.get(column) {
        return target.clone();
    }
    let mut name = column.to_owned();
    for rewrite in rewrites {
        if let Some(rest) = name.strip_prefix(&rewrite.prefix) {
            name = format!("{}{rest}", rewrite.replacement);
        }
    }
    Match::Spelling(name)
}
