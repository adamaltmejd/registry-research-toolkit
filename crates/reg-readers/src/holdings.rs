//! Metadata-only physical holdings from the pinned storage export (the nine
//! `INFORMATION_SCHEMA.COLUMNS` fields). Errors identify input roles and source rows,
//! never reproduce private table names, column names or source values.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::Deserialize;

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
    #[serde(default)]
    packed: Vec<Packed>,
}

/// The physical columns that hold an SCB löpnummer (a key SCB pseudonymised: person,
/// firm and the like), selected as in [`Selector`]. The order marks them and the runner
/// writes them as int64 (#2648). A holder's own serial is not one, whatever its name.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Lopnr {
    prefix: Option<String>,
    name: Option<String>,
    schema: Option<String>,
    table: Option<String>,
    column: Option<String>,
    evidence: String,
}

/// The SQL storage an SCB löpnummer may have: exactly what the runner converts, its
/// `LOPNR_TYPES` and `lopnr_storage_ok` in `reg_mona_runner/extract.R`. Single precision
/// (`real`, `float(1..24)`) is refused: above 2^24 it has already merged distinct keys.
fn lopnr_storage(storage: &Storage) -> bool {
    match storage.data_type.to_ascii_lowercase().as_str() {
        "tinyint" | "smallint" | "int" | "bigint" | "decimal" | "numeric" | "char" | "varchar"
        | "nchar" | "nvarchar" | "text" | "ntext" => true,
        "float" => storage.precision.is_none_or(|p| p > 24),
        _ => false,
    }
}

impl Lopnr {
    fn selector(&self) -> Selector<'_> {
        Selector {
            prefix: self.prefix.as_deref(),
            name: self.name.as_deref(),
            schema: self.schema.as_deref(),
            table: self.table.as_deref(),
            column: self.column.as_deref(),
        }
    }

    fn valid(&self) -> bool {
        self.selector().valid() && !self.evidence.trim().is_empty()
    }
}

/// A physical column that holds several codes of one variable joined by `separator`
/// (PAR's `DIAGNOS`), selected as in [`Selector`]. A code filter on it matches any of
/// the codes (#2604).
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Packed {
    prefix: Option<String>,
    name: Option<String>,
    schema: Option<String>,
    table: Option<String>,
    column: Option<String>,
    separator: String,
    evidence: String,
}

impl Packed {
    fn selector(&self) -> Selector<'_> {
        Selector {
            prefix: self.prefix.as_deref(),
            name: self.name.as_deref(),
            schema: self.schema.as_deref(),
            table: self.table.as_deref(),
            column: self.column.as_deref(),
        }
    }

    /// The separator is one printable ASCII character that is not a letter or digit, as
    /// the runner and `reg_core::manifest::packed_separator` admit.
    fn valid(&self) -> bool {
        self.selector().valid()
            && !self.evidence.trim().is_empty()
            && reg_core::manifest::packed_separator(&self.separator)
    }
}

/// The columns a `[[lopnr]]` or `[[packed]]` entry marks, in exactly one form: a column
/// name `prefix`, an exact column `name`, or one column of one table (`table` and
/// `column`, the full path, which neither misses a column nor marks another table's,
/// with `schema` where the table name is in several schemas). ASCII case is ignored as
/// SQL Server does.
struct Selector<'a> {
    prefix: Option<&'a str>,
    name: Option<&'a str>,
    schema: Option<&'a str>,
    table: Option<&'a str>,
    column: Option<&'a str>,
}

impl Selector<'_> {
    /// Exactly one non-empty form; `schema` only in the full path.
    fn valid(&self) -> bool {
        match (self.prefix, self.name, self.schema, self.table, self.column) {
            (Some(v), None, None, None, None) | (None, Some(v), None, None, None) => !v.is_empty(),
            (None, None, s, Some(t), Some(c)) => {
                s.is_none_or(|s| !s.is_empty()) && !t.is_empty() && !c.is_empty()
            }
            _ => false,
        }
    }

    fn marks(&self, fact: &Fact) -> bool {
        let column = fact.column.as_str();
        match (self.prefix, self.name, self.table, self.column) {
            (Some(prefix), ..) => column
                .get(..prefix.len())
                .is_some_and(|head| head.eq_ignore_ascii_case(prefix)),
            (_, Some(name), ..) => column.eq_ignore_ascii_case(name),
            (.., Some(t), Some(c)) => {
                self.schema
                    .is_none_or(|s| fact.schema.eq_ignore_ascii_case(s))
                    && fact.table.eq_ignore_ascii_case(t)
                    && column.eq_ignore_ascii_case(c)
            }
            _ => false,
        }
    }

    /// Whether the entry marks the export row's column, keeping in `seen` the schema of
    /// its first mark. A full path without `schema` that marks its table in a second
    /// schema is refused: it would mark two tables (`holdings_mark_ambiguous`).
    fn mark(&self, seen: &mut Option<String>, fact: &Fact, row: usize) -> Result<bool, ReadError> {
        if !self.marks(fact) {
            return Ok(false);
        }
        if self.table.is_some()
            && self.schema.is_none()
            && seen.as_ref().is_some_and(|s| *s != fact.schema)
        {
            return Err(at(
                row,
                "holdings_mark_ambiguous: a [[lopnr]] or [[packed]] table and column is in several schemas; give its schema",
            ));
        }
        seen.get_or_insert_with(|| fact.schema.clone());
        Ok(true)
    }
}

/// The SQL storage a packed column may have: text the runner matches with `LIKE`.
fn packed_storage(storage: &Storage) -> bool {
    matches!(
        storage.data_type.to_ascii_lowercase().as_str(),
        "char" | "varchar" | "nchar" | "nvarchar"
    )
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
    /// The 1-based position of the first `[[lopnr]]` entry that marks the column as a
    /// löpnummer. Diagnostics name this entry; a later entry marking it too is not kept.
    pub lopnr: Option<usize>,
    /// The separator of a packed column by the config's `[[packed]]`.
    pub packed: Option<String>,
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
    marks_valid(&config)?;
    let mut lopnr_used: Vec<Option<String>> = vec![None; config.lopnr.len()];
    let mut packed_used: Vec<Option<String>> = vec![None; config.packed.len()];
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
        let mut lopnr = None;
        for (n, (used, l)) in (1..).zip(lopnr_used.iter_mut().zip(&config.lopnr)) {
            if l.selector().mark(used, &fact, row)? {
                lopnr.get_or_insert(n);
            }
        }
        if lopnr.is_some() && !lopnr_storage(&fact.storage) {
            return Err(at(
                row,
                "holdings_lopnr_storage: a [[lopnr]] column is stored neither as an integer, a decimal, a double-precision float nor as text",
            ));
        }
        let packed = packed(&config.packed, &mut packed_used, &fact, row)?;
        table.columns.push(Column {
            name: fact.column,
            ordinal: fact.ordinal,
            match_name,
            storage: fact.storage,
            lopnr,
            packed,
        });
    }
    if let Some(n) = packed_used.iter().position(Option::is_none) {
        return Err(error(
            "holdings_reader",
            format!(
                "holdings_packed_unused: packed {} marks no held column",
                n + 1
            ),
        ));
    }
    if let Some(n) = lopnr_used.iter().position(Option::is_none) {
        return Err(error(
            "holdings_reader",
            format!(
                "holdings_lopnr_unused: lopnr {} marks no held column",
                n + 1
            ),
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

/// Refuse a `[[lopnr]]` or `[[packed]]` entry with other than exactly one selector form,
/// an empty value or evidence, or (packed) a bad separator, naming it by position.
fn marks_valid(config: &Config) -> Result<(), ReadError> {
    if let Some(n) = config.lopnr.iter().position(|l| !l.valid()) {
        return Err(error(
            "holdings_reader",
            format!(
                "lopnr {}: not exactly one of prefix, name, or table with column (and an optional schema), or an empty value or evidence",
                n + 1
            ),
        ));
    }
    if let Some(n) = config.packed.iter().position(|p| !p.valid()) {
        return Err(error(
            "holdings_reader",
            format!(
                "packed {}: not exactly one of prefix, name, or table with column (and an optional schema), an empty value or evidence, or a separator that is not one printable ASCII character other than a letter or digit",
                n + 1
            ),
        ));
    }
    Ok(())
}

/// The separator the `[[packed]]` entries give the export row's column, marking each
/// entry that selects it as used.
fn packed(
    entries: &[Packed],
    used: &mut [Option<String>],
    fact: &Fact,
    row: usize,
) -> Result<Option<String>, ReadError> {
    let mut packed: Option<&str> = None;
    for (used, p) in used.iter_mut().zip(entries) {
        if p.selector().mark(used, fact, row)? {
            if packed.is_some_and(|s| s != p.separator) {
                return Err(at(
                    row,
                    "holdings_packed_conflict: two [[packed]] entries give one column different separators",
                ));
            }
            packed = Some(p.separator.as_str());
        }
    }
    if packed.is_some() && !packed_storage(&fact.storage) {
        return Err(at(
            row,
            "holdings_packed_storage: a [[packed]] column is stored otherwise than as char, varchar, nchar or nvarchar",
        ));
    }
    Ok(packed.map(str::to_owned))
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
