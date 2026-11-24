//! Metadata-only physical holdings from the pinned storage export (the nine
//! `INFORMATION_SCHEMA.COLUMNS` fields). Errors identify input roles and source rows,
//! never reproduce private table names, column names or source values.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::Deserialize;

use reg_core::manifest::StorageFamily;

use crate::{ReadError, config, error};

/// Reader vocabulary, evidenced exact-name renames and literal prefix rewrites. Both
/// affect matching only; the artifact retains exact trimmed physical identifiers.
/// `schema_keyed` keys tables by (schema, table) instead of table alone.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    schema_keyed: bool,
    #[serde(default)]
    rename: Vec<Rename>,
    #[serde(default)]
    rewrite: Vec<Rewrite>,
    #[serde(default)]
    lopnr: Vec<Lopnr>,
}

/// The physical columns that hold an SCB löpnummer (a key SCB pseudonymised: person,
/// firm and the like), by name prefix, ASCII case ignored as SQL Server does. The order
/// marks them and the runner writes them as int64 (#2648). A holder's own serial is
/// not one, whatever its name.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Lopnr {
    prefix: String,
    evidence: String,
}

impl Lopnr {
    fn marks(&self, column: &str) -> bool {
        column
            .get(..self.prefix.len())
            .is_some_and(|head| head.eq_ignore_ascii_case(&self.prefix))
    }
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

/// What a physical column is matched against in its routed tables.
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

/// One held physical table; no observation records are read.
pub struct Table {
    /// The table's identity schema: `Some` exactly when the reader config is
    /// `schema_keyed`, and then the table's identity is (schema, name).
    pub schema: Option<String>,
    pub name: String,
    /// The table's schema in the export, keyed or not. An unkeyed table name is in one
    /// schema only, so this is always defined.
    pub storage_schema: String,
    /// `BASE TABLE` or `VIEW`.
    pub table_type: String,
    /// The export row of the table's first column row.
    pub source_row: usize,
    /// By `ordinal_position`.
    pub columns: Vec<Column>,
}

pub struct ReadOutput {
    pub tables: Vec<Table>,
    /// Every `[[rename]]` target, in config order, for the compiler to resolve.
    pub rename_targets: Vec<Match>,
}

/// A physical column with its storage facts.
pub struct Column {
    pub name: String,
    pub ordinal: i64,
    pub match_name: Match,
    pub storage: Storage,
    /// An SCB löpnummer by the config's `[[lopnr]]`.
    pub lopnr: bool,
}

pub struct Storage {
    pub data_type: String,
    pub length: Option<i64>,
    pub precision: Option<i64>,
    pub scale: Option<i64>,
}

const ROLE: &str = "storage_export";

fn at(row: usize, message: &str) -> ReadError {
    error(ROLE, format!("row={row}: {message}"))
}

/// The nine `INFORMATION_SCHEMA.COLUMNS` fields, by the position the reader uses them.
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

/// One admitted export row.
struct Fact {
    schema: String,
    table: String,
    table_type: String,
    column: String,
    ordinal: i64,
    storage: Storage,
}

/// Read the held physical tables from the storage export using an explicit reader
/// config. Every table in the export is held: selection happens in the steward's export
/// step, content decisions in its authored topology.
///
/// # Errors
/// Malformed CSV/config, invalid storage facts, a duplicate column or ordinal within a
/// table, a table whose rows disagree on its type, an unkeyed table name in several
/// schemas, or empty rewrite evidence. No error contains a private source row or
/// identifier.
pub fn read(storage_export: &Path, config_path: &Path) -> Result<ReadOutput, ReadError> {
    let config: Config = config(config_path)?;
    if config
        .rewrite
        .iter()
        .any(|r| r.prefix.is_empty() || r.evidence.trim().is_empty())
    {
        return Err(error("holdings_reader", "empty rewrite prefix or evidence"));
    }
    if config
        .lopnr
        .iter()
        .any(|l| l.prefix.is_empty() || l.evidence.trim().is_empty())
    {
        return Err(error("holdings_reader", "empty lopnr prefix or evidence"));
    }
    let mut lopnr_used = vec![false; config.lopnr.len()];
    let (renames, rename_targets) = renames(&config.rename)?;
    let mut csv =
        csv::Reader::from_path(storage_export).map_err(|_| error(ROLE, "cannot open CSV"))?;
    let indices = indices(&mut csv)?;
    let mut tables: BTreeMap<(Option<String>, String), Table> = BTreeMap::new();
    for (row, record) in (2..).zip(csv.records()) {
        let record = record.map_err(|_| at(row, "malformed CSV record"))?;
        let fact = fact(&record, &indices, row)?;
        let identity = config.schema_keyed.then(|| fact.schema.clone());
        let table = tables
            .entry((identity.clone(), fact.table.clone()))
            .or_insert_with(|| Table {
                schema: identity,
                name: fact.table.clone(),
                storage_schema: fact.schema.clone(),
                table_type: fact.table_type.clone(),
                source_row: row,
                columns: Vec::new(),
            });
        if table.storage_schema != fact.schema {
            return Err(at(
                row,
                "table name in several schemas; the reader config must be schema_keyed",
            ));
        }
        if table.table_type != fact.table_type {
            return Err(at(row, "table_type differs within one table"));
        }
        if table
            .columns
            .iter()
            .any(|c| c.name == fact.column || c.ordinal == fact.ordinal)
        {
            return Err(at(row, "duplicate physical column or ordinal_position"));
        }
        let match_name = match_name(&fact.column, &renames, &config.rewrite);
        if matches!(&match_name, Match::Spelling(name) if name.is_empty()) {
            return Err(at(row, "rewrite removed entire column identifier"));
        }
        let mut lopnr = false;
        for (used, l) in lopnr_used.iter_mut().zip(&config.lopnr) {
            if l.marks(&fact.column) {
                (*used, lopnr) = (true, true);
            }
        }
        // The runner converts integers, reals and text; a date or binary is no löpnummer.
        if lopnr
            && !matches!(
                StorageFamily::of(&fact.storage.data_type),
                Some(StorageFamily::Integer | StorageFamily::Real | StorageFamily::Text)
            )
        {
            return Err(at(
                row,
                "holdings_lopnr_storage: a [[lopnr]] column is stored neither as a number nor as text",
            ));
        }
        table.columns.push(Column {
            name: fact.column,
            ordinal: fact.ordinal,
            match_name,
            storage: fact.storage,
            lopnr,
        });
    }
    if let Some(n) = lopnr_used.iter().position(|used| !used) {
        return Err(error(
            "holdings_reader",
            format!("holdings_lopnr_unused: lopnr {n} marks no held column"),
        ));
    }
    let mut tables: Vec<Table> = tables.into_values().collect();
    for table in &mut tables {
        table.columns.sort_by_key(|c| c.ordinal);
    }
    Ok(ReadOutput {
        tables,
        rename_targets,
    })
}

/// The header positions of [`REQUIRED`], refusing a duplicate or missing header.
fn indices(csv: &mut csv::Reader<std::fs::File>) -> Result<Vec<usize>, ReadError> {
    let names: Vec<String> = csv
        .headers()
        .map_err(|_| error(ROLE, "cannot read CSV header"))?
        .iter()
        .map(|s| s.trim().trim_start_matches('\u{feff}').to_owned())
        .collect();
    if names.iter().collect::<BTreeSet<_>>().len() != names.len() {
        return Err(error(ROLE, "duplicate CSV header"));
    }
    REQUIRED
        .iter()
        .map(|required| {
            names
                .iter()
                .position(|h| h == required)
                .ok_or_else(|| error(ROLE, format!("missing CSV header {required}")))
        })
        .collect()
}

/// One export row, trimmed and checked.
fn fact(record: &csv::StringRecord, indices: &[usize], row: usize) -> Result<Fact, ReadError> {
    let value = |n: usize| record[indices[n]].trim();
    if (0..5).any(|n| value(n).is_empty()) {
        return Err(at(row, "missing required storage identifier/type"));
    }
    let number = |n: usize| -> Result<Option<i64>, ReadError> {
        let value = value(n);
        if value.is_empty() || value.eq_ignore_ascii_case("null") {
            Ok(None)
        } else {
            value
                .parse()
                .map(Some)
                .map_err(|_| at(row, "invalid numeric storage fact"))
        }
    };
    let Some(ordinal) = number(8)?.filter(|&n| n > 0) else {
        return Err(at(row, "ordinal_position must be positive"));
    };
    let length = number(5)?;
    let precision = number(6)?;
    let scale = number(7)?;
    if !matches!(value(2), "BASE TABLE" | "VIEW")
        || length.is_some_and(|n| n < -1)
        || precision.is_some_and(|n| n <= 0)
        || scale.is_some_and(|n| n < 0)
        || precision.zip(scale).is_some_and(|(p, s)| s > p)
    {
        return Err(at(row, "invalid storage shape, length or precision/scale"));
    }
    Ok(Fact {
        schema: value(0).to_owned(),
        table: value(1).to_owned(),
        table_type: value(2).to_owned(),
        column: value(3).to_owned(),
        ordinal,
        storage: Storage {
            data_type: value(4).to_owned(),
            length,
            precision,
            scale,
        },
    })
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
                    "holdings_rename_invalid: rename {n}: empty or untrimmed name, not exactly one of catalog and variable, or empty evidence"
                ),
            ));
        };
        if out
            .insert(entry.physical.as_str(), target.clone())
            .is_some()
        {
            return Err(error(
                ROLE,
                format!("holdings_rename_duplicate: rename {n}: physical name already renamed"),
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
                "holdings_rename_chain: rename {n}: catalog name is also a renamed physical name"
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
