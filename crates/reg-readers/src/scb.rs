//! The SCB metadata export: one directory of pipe-delimited, double-quoted CSVs in
//! CP1252, each with a header row.
//!
//! - `Registerinformation.csv`: one row per register version (a delivery), population
//!   and variable. A register (`RegisterId`) has variants (`RegVarID`, the tables) and
//!   versions (`RegVerID`, the releases); a version's name places it in time through
//!   the delivery-name grammar. Rows that differ only by population are one
//!   occurrence: the release lists its populations, the occurrence every row. A row
//!   with no `Kolumnnamn` documents the variable in that version without a column (a
//!   gap marker).
//!
//! Source keys are `v<VarId>`; release handles `r<RegVerID>`; table handles the
//! variant's name. A register's handle is its entry in the config's `[registers]`,
//! else `r<RegisterId>`, reported.

use std::collections::BTreeMap;
use std::path::Path;

use reg_frame::{Frame, Locator, Occurrence, Population, Record, Register, Release, Table};
use serde::Deserialize;

use crate::names::Grammar;
use crate::{ReadError, ReadOutput, config, error, nonblank};

const REGISTERINFORMATION: &str = "Registerinformation.csv";

/// `curation/readers/scb.toml`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// `RegisterId` → register handle.
    #[serde(default)]
    pub registers: BTreeMap<String, String>,
    /// `Datatyp` → the frame's normalized type.
    pub types: BTreeMap<String, String>,
    pub names: Grammar,
}

impl Config {
    /// # Errors
    /// An unreadable or invalid config file.
    pub fn load(path: &Path) -> Result<Self, ReadError> {
        config(path)
    }
}

/// Read the export directory `dir` (`rel`, relative to the inputs root) of `provider`.
///
/// # Errors
/// A missing file or column, a malformed CSV row, or a variant name two variants of
/// one register share, each located at its file and row.
#[allow(clippy::too_many_lines)]
pub fn read(
    root: &Path,
    rel: &str,
    provider: &str,
    config: &Config,
) -> Result<ReadOutput, ReadError> {
    let mut out = ReadOutput::default();
    let input = format!("{rel}/{REGISTERINFORMATION}");
    let mut reader = ::csv::ReaderBuilder::new()
        .delimiter(b'|')
        .from_path(root.join(&input))
        .map_err(|e| error(&input, e.to_string()))?;
    let header: Vec<String> = reader
        .byte_headers()
        .map_err(|e| error(&input, e.to_string()))?
        .iter()
        .map(decode)
        .collect();
    out.files.push(input.clone());
    let col = |name: &str| {
        header
            .iter()
            .position(|h| h == name)
            .ok_or_else(|| error(&input, format!("no column {name:?}")))
    };
    let register_id = col("RegisterId")?;
    let register_name = col("Registernamn")?;
    let register_purpose = col("Registersyfte")?;
    let variant_id = col("RegVarID")?;
    let variant_name = col("Registervariantnamn")?;
    let variant_title = col("Registervariantrubrik")?;
    let variant_description = col("Registervariantbeskrivning")?;
    let version_id = col("RegVerID")?;
    let version_name = col("Registerversionnamn")?;
    let version_description = col("Registerversionbeskrivning")?;
    let population_name = col("Populationnamn")?;
    let population_definition = col("Populationdefinition")?;
    let population_day = col("Populationdatum")?;
    let var_id = col("VarId")?;
    let var_name = col("Variabelnamn")?;
    let var_definition = col("Variabeldefinition")?;
    let var_operational = col("VariabelOperationell_definition")?;
    let unit = col("Mattenhet")?;
    let column = col("Kolumnnamn")?;
    let dtype = col("Datatyp")?;
    let length = col("Datalängd")?;

    let mut registers: BTreeMap<String, Register> = BTreeMap::new();
    // (RegisterId, RegVarID) → table; (RegisterId, folded variant name) → RegVarID.
    let mut tables: BTreeMap<(String, String), Table> = BTreeMap::new();
    let mut variant_names: BTreeMap<(String, String), String> = BTreeMap::new();
    let mut releases: BTreeMap<String, Release> = BTreeMap::new();
    let mut occurrences: BTreeMap<(String, String, String), Occurrence> = BTreeMap::new();
    for (i, record) in reader.byte_records().enumerate() {
        let row: Vec<String> = record
            .map_err(|e| error(&input, e.to_string()))?
            .iter()
            .map(decode)
            .collect();
        let locator = Locator {
            input: input.clone(),
            at: format!("row={}", i + 2),
        };
        let get = |c: usize| nonblank(&row[c]);
        let field = |c: usize, what: &str| {
            get(c).ok_or_else(|| error(&input, format!("{}: no {what}", locator.at)))
        };
        let rid = field(register_id, "RegisterId")?;
        let handle = match config.registers.get(&rid) {
            Some(h) => h.clone(),
            None => format!("r{rid}"),
        };
        if !registers.contains_key(&rid) {
            if !config.registers.contains_key(&rid) {
                out.warnings.push(format!(
                    "{}: RegisterId {rid} has no handle in [registers]; read as {handle}",
                    locator.at
                ));
            }
            registers.insert(
                rid.clone(),
                Register {
                    provider: provider.to_owned(),
                    register: handle.clone(),
                    name: get(register_name),
                    description: get(register_purpose),
                    from: None,
                    to: None,
                    locator: locator.clone(),
                },
            );
        }
        let vid = field(variant_id, "RegVarID")?;
        let table = get(variant_name).unwrap_or_else(|| vid.clone());
        let owner = variant_names
            .entry((rid.clone(), reg_core::fold_identity(&table)))
            .or_insert_with(|| vid.clone());
        if *owner != vid {
            return Err(error(
                &input,
                format!(
                    "{}: variants {owner} and {vid} of register {rid} share the name {table:?}",
                    locator.at
                ),
            ));
        }
        tables.entry((rid.clone(), vid)).or_insert_with(|| Table {
            provider: provider.to_owned(),
            register: handle.clone(),
            table: table.clone(),
            name: get(variant_title),
            description: get(variant_description),
            from: None,
            to: None,
            cadence: None,
            locator: locator.clone(),
        });
        let version = format!("r{}", field(version_id, "RegVerID")?);
        let release = releases.entry(version.clone()).or_insert_with(|| {
            let name = row[version_name].trim().to_owned();
            let parsed = config.names.parse(&name);
            let (from, to, grain) = match parsed.period {
                Some((from, to, grain)) => (Some(from), Some(to), Some(grain.to_owned())),
                None => (None, None, None),
            };
            Release {
                provider: provider.to_owned(),
                register: handle.clone(),
                table: table.clone(),
                release: version.clone(),
                name,
                description: get(version_description),
                from,
                to,
                grain,
                edition: parsed.edition.to_owned(),
                qualifier: parsed.qualifier,
                populations: Vec::new(),
                locator: locator.clone(),
            }
        });
        if let Some(name) = get(population_name) {
            let population = Population {
                name,
                definition: get(population_definition),
                // A range or no date states no one reference day.
                day: get(population_day).filter(|d| reg_core::iso_day_number(d).is_some()),
            };
            if !release.populations.contains(&population) {
                release.populations.push(population);
            }
        }
        let key = format!("v{}", field(var_id, "VarId")?);
        let spelling = row[column].trim().to_owned();
        match occurrences.entry((version.clone(), key.clone(), spelling.clone())) {
            std::collections::btree_map::Entry::Occupied(mut o) => {
                o.get_mut().also.push(locator);
            }
            std::collections::btree_map::Entry::Vacant(v) => {
                let literal = get(dtype);
                v.insert(Occurrence {
                    provider: provider.to_owned(),
                    register: handle,
                    table,
                    release: Some(version),
                    column: spelling,
                    source_key: key,
                    from: None,
                    to: None,
                    label: get(var_name),
                    definition: get(var_definition),
                    operational_definition: get(var_operational),
                    unit: get(unit),
                    dtype: literal.map(|t| config.types.get(&t).cloned().unwrap_or(t)),
                    // `-1` states no length.
                    length: get(length)
                        .and_then(|l| l.parse().ok())
                        .filter(|&l: &i64| l > 0),
                    valueset: None,
                    population_valuesets: Vec::new(),
                    coded: None,
                    code_system: None,
                    sensitive: None,
                    identifier: None,
                    locator,
                    also: Vec::new(),
                });
            }
        }
    }

    let records = registers
        .into_values()
        .map(Record::Register)
        .chain(tables.into_values().map(Record::Table))
        .chain(releases.into_values().map(Record::Release))
        .chain(occurrences.into_values().map(Record::Occurrence))
        .collect();
    out.frame = Frame::new(records);
    Ok(out)
}

/// CP1252 bytes as text. The five bytes CP1252 leaves undefined occur in the export as
/// DOS CP850 remnants and decode as CP850.
fn decode(bytes: &[u8]) -> String {
    const HIGH: [char; 32] = [
        '€', 'ü', '‚', 'ƒ', '„', '…', '†', '‡', 'ˆ', '‰', 'Š', '‹', 'Œ', 'ì', 'Ž', 'Å', 'É', '‘',
        '’', '“', '”', '•', '–', '—', '˜', '™', 'š', '›', 'œ', 'Ø', 'ž', 'Ÿ',
    ];
    bytes
        .iter()
        .map(|&b| match b {
            0x80..=0x9F => HIGH[usize::from(b - 0x80)],
            _ => char::from(b),
        })
        .collect()
}
