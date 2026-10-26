//! The transcription CSV: how a provider without a machine-readable export enters the
//! catalog. One directory per provider, UTF-8 CSV with a header row:
//!
//! - `registers.csv`: `register,name,description,from,to`, one row per register;
//! - `tables.csv` (optional): `register,table,name,description,from,to`;
//! - `<register>.csv`: one row per documented column, `table,column,label,definition,
//!   data_type,unit,codes,code_system,sensitive,identifier,from,to`.
//!
//! `table` names a table of `tables.csv`, several separated by `;` (one occurrence
//! each), or is blank for the register's one implicit table. Dates are inclusive ISO
//! days; a blank bound is unstated. `sensitive` and `identifier` are `true`, `false`
//! or blank (unstated). `codes` is inline (`M=Man; K=Kvinna`) or `@name`, naming
//! `codes/<name>.csv` (`code,label`) in the same directory. `code_system` names a code
//! system the transcription does not spell out; it stays evidence.

use std::collections::BTreeMap;
use std::path::Path;

use reg_frame::{Frame, Item, Locator, Occurrence, Record, Register, Table, ValueSet};
use serde::Deserialize;

use crate::{ReadError, ReadOutput, config, error, name_key, nonblank};

const REGISTERS: &[&str] = &["register", "name", "description", "from", "to"];
const TABLES: &[&str] = &["register", "table", "name", "description", "from", "to"];
const COLUMNS: &[&str] = &[
    "table",
    "column",
    "label",
    "definition",
    "data_type",
    "unit",
    "codes",
    "code_system",
    "sensitive",
    "identifier",
    "from",
    "to",
];
const CODES: &[&str] = &["code", "label"];

/// `curation/readers/transcription.toml`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Transcribed `data_type` → the frame's normalized type.
    pub types: BTreeMap<String, String>,
}

impl Config {
    /// # Errors
    /// An unreadable or invalid config file.
    pub fn load(path: &Path) -> Result<Self, ReadError> {
        config(path)
    }
}

/// Read the transcription directory `dir` (`rel`, relative to the inputs root) of
/// `provider`.
///
/// # Errors
/// A missing or malformed file, an unknown table, a malformed date, flag or code
/// list, each located at its file and row.
pub fn read(
    root: &Path,
    rel: &str,
    provider: &str,
    config: &Config,
) -> Result<ReadOutput, ReadError> {
    let mut out = ReadOutput::default();
    let mut records = Vec::new();
    let registers = rows(root, &format!("{rel}/registers.csv"), REGISTERS, &mut out)?;
    let tables_path = format!("{rel}/tables.csv");
    let tables = if root.join(&tables_path).is_file() {
        rows(root, &tables_path, TABLES, &mut out)?
    } else {
        Vec::new()
    };
    let mut declared: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (at, row) in &tables {
        let register = required(row, "register", at)?;
        let table = required(row, "table", at)?;
        declared
            .entry(register.clone())
            .or_default()
            .push(table.clone());
        records.push(Record::Table(Table {
            provider: provider.to_owned(),
            register,
            table,
            name: field(row, "name"),
            description: field(row, "description"),
            from: date(row, "from", at)?,
            to: date(row, "to", at)?,
            cadence: None,
            locator: at.clone(),
        }));
    }
    for (at, row) in &registers {
        let register = required(row, "register", at)?;
        records.push(Record::Register(Register {
            provider: provider.to_owned(),
            register: register.clone(),
            name: field(row, "name"),
            description: field(row, "description"),
            from: date(row, "from", at)?,
            to: date(row, "to", at)?,
            locator: at.clone(),
        }));
        let empty = Vec::new();
        let tables = declared.get(&register).unwrap_or(&empty);
        let path = format!("{rel}/{register}.csv");
        for (at, row) in rows(root, &path, COLUMNS, &mut out)? {
            let column = required(&row, "column", &at)?;
            let valueset = codes(root, rel, &row, &at, &mut out, &mut records)?;
            let targets: Vec<String> = match field(&row, "table") {
                None => vec![String::new()],
                Some(list) => list.split(';').map(|t| t.trim().to_owned()).collect(),
            };
            for table in targets {
                if table.is_empty() && field(&row, "table").is_some() {
                    return Err(error(
                        &at.input,
                        format!("{}: the table list has an empty entry", at.at),
                    ));
                }
                if !table.is_empty() && !tables.contains(&table) {
                    return Err(error(
                        &at.input,
                        format!("{}: table {table:?} is not in tables.csv", at.at),
                    ));
                }
                records.push(Record::Occurrence(Occurrence {
                    provider: provider.to_owned(),
                    register: register.clone(),
                    table,
                    release: None,
                    column: column.clone(),
                    source_key: name_key(&column),
                    from: date(&row, "from", &at)?,
                    to: date(&row, "to", &at)?,
                    label: field(&row, "label"),
                    definition: field(&row, "definition"),
                    operational_definition: None,
                    unit: field(&row, "unit"),
                    dtype: field(&row, "data_type")
                        .map(|t| config.types.get(&t).cloned().unwrap_or(t)),
                    length: None,
                    valueset: valueset.clone(),
                    population_valuesets: Vec::new(),
                    coded: None,
                    code_system: field(&row, "code_system"),
                    sensitive: flag(&row, "sensitive", &at)?,
                    identifier: flag(&row, "identifier", &at)?,
                    locator: at.clone(),
                    also: Vec::new(),
                    conflicts: Vec::new(),
                }));
            }
        }
    }
    out.frame = Frame::new(records);
    Ok(out)
}

type Row = BTreeMap<String, String>;

/// The data rows of one CSV, each with its locator (`row=N`, the header is row 1). A
/// header outside `known` is refused, so a misspelled column is not silently lost.
fn rows(
    root: &Path,
    rel: &str,
    known: &[&str],
    out: &mut ReadOutput,
) -> Result<Vec<(Locator, Row)>, ReadError> {
    let mut reader =
        csv::Reader::from_path(root.join(rel)).map_err(|e| error(rel, e.to_string()))?;
    let headers = reader
        .headers()
        .map_err(|e| error(rel, e.to_string()))?
        .clone();
    if let Some(unknown) = headers.iter().find(|h| !known.contains(h)) {
        return Err(error(
            rel,
            format!("unknown column {unknown:?}; expected {}", known.join(", ")),
        ));
    }
    if !out.files.iter().any(|f| f == rel) {
        out.files.push(rel.to_owned());
    }
    reader
        .records()
        .enumerate()
        .map(|(i, record)| {
            let record = record.map_err(|e| error(rel, e.to_string()))?;
            let row = headers
                .iter()
                .zip(record.iter())
                .map(|(h, v)| (h.to_owned(), v.to_owned()))
                .collect();
            let locator = Locator {
                input: rel.to_owned(),
                at: format!("row={}", i + 2),
            };
            Ok((locator, row))
        })
        .collect()
}

fn field(row: &Row, name: &str) -> Option<String> {
    row.get(name).and_then(|v| nonblank(v))
}

fn required(row: &Row, name: &str, at: &Locator) -> Result<String, ReadError> {
    field(row, name).ok_or_else(|| error(&at.input, format!("{}: {name} is blank", at.at)))
}

fn date(row: &Row, name: &str, at: &Locator) -> Result<Option<String>, ReadError> {
    match field(row, name) {
        Some(d) if reg_core::iso_day_number(&d).is_none() => Err(error(
            &at.input,
            format!("{}: {name} {d:?} is not an ISO date", at.at),
        )),
        d => Ok(d),
    }
}

fn flag(row: &Row, name: &str, at: &Locator) -> Result<Option<bool>, ReadError> {
    match field(row, name).as_deref() {
        None => Ok(None),
        Some("true") => Ok(Some(true)),
        Some("false") => Ok(Some(false)),
        Some(other) => Err(error(
            &at.input,
            format!("{}: {name} {other:?} is not true, false or blank", at.at),
        )),
    }
}

/// The row's `codes` as a value set record, returning its hash.
fn codes(
    root: &Path,
    rel: &str,
    row: &Row,
    at: &Locator,
    out: &mut ReadOutput,
    records: &mut Vec<Record>,
) -> Result<Option<String>, ReadError> {
    let Some(text) = field(row, "codes") else {
        return Ok(None);
    };
    let (items, name, locator) = if let Some(name) = text.strip_prefix('@') {
        let path = format!("{rel}/codes/{name}.csv");
        let mut items = Vec::new();
        for (at, row) in rows(root, &path, CODES, out)? {
            items.push(Item {
                code: unique(&items, required(&row, "code", &at)?, &at)?,
                label: field(&row, "label"),
                parent: None,
            });
        }
        let locator = Locator {
            input: path,
            at: String::new(),
        };
        (items, Some(name.to_owned()), locator)
    } else {
        let mut items = Vec::new();
        for pair in text.split(';') {
            let Some((code, label)) = pair.split_once('=') else {
                return Err(error(
                    &at.input,
                    format!("{}: codes entry {pair:?} is not code=label", at.at),
                ));
            };
            let Some(code) = nonblank(code) else {
                return Err(error(
                    &at.input,
                    format!("{}: codes entry {pair:?} has no code", at.at),
                ));
            };
            items.push(Item {
                code: unique(&items, code, at)?,
                label: nonblank(label),
                parent: None,
            });
        }
        (items, None, at.clone())
    };

    let set = ValueSet::new(items, name, locator);
    let hash = set.hash.clone();
    records.push(Record::ValueSet(set));
    Ok(Some(hash))
}

/// `code`, refused when `items` already states it.
fn unique(items: &[Item], code: String, at: &Locator) -> Result<String, ReadError> {
    if items.iter().any(|i| i.code == code) {
        return Err(error(
            &at.input,
            format!("{}: code {code:?} is stated twice", at.at),
        ));
    }
    Ok(code)
}
