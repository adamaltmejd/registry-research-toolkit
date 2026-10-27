//! The SCB metadata export: a zip of pipe-delimited, double-quoted CSVs in CP1252,
//! each with a header row, read from the archive as pinned.
//!
//! - `Registerinformation.csv`: one row per register version (a delivery), population
//!   and variable. A register (`RegisterId`) has variants (`RegVarID`, the tables) and
//!   versions (`RegVerID`, the releases); a version's name places it in time through
//!   the delivery-name grammar. Rows that differ only by population are one
//!   occurrence: the release lists its populations, the occurrence every row. A row
//!   with no `Kolumnnamn` documents the variable in that version without a column (a
//!   gap marker).
//! - `Vardemangder.csv`: the value sets, one row per (`CVID`, item); an item
//!   (`ItemId`) is a code and label of a value-set version (`Värdemängdsversion`) at a
//!   level. Each row of `Registerinformation` names its `CVID`.
//! - `VardemangderValidDates.csv`: validity intervals per item. A set whose items are
//!   all dated keeps those valid in its release's period, unless that leaves none; any
//!   other set stands whole.
//! - `UnikaRegisterOchVariabler.csv`: per register, variant, variable and column name,
//!   whether the column is sensitive (always or sometimes) and an identifier.
//! - `Identifierare.csv`: the `VarId`s that are identifiers wherever they occur.
//! - `Timeseries.csv`: events (`Handelse`) on one entity (`Entitet`) named by `ID1`, and
//!   for a succession the entity `ID2` names. The config's `[timeseries]` maps each
//!   event to a notice or a succession and each entity to its endpoint kind; a
//!   succession becomes a `continues` link, or, where the config says so (between
//!   releases), a notice on the newer entity.
//!
//! A row's `VariabelRegister_Källa` names the register the variable is derived from:
//! one `derived_from` link to that register by name per (register, variable, text).
//!
//! An occurrence whose rows name different value sets states them per population, as
//! alternatives where one population's rows disagree; fields other rows disagree on are
//! recorded as conflicts. A `CVID` holding only an uncoded marker (an item whose code,
//! version and level are one of the config's `uncoded` names) states that the column
//! carries no codes.
//!
//! Register handles are `r<RegisterId>`, release handles `r<RegVerID>`, table handles
//! the variant's name and source keys `v<VarId>`. A register's public name is a curated
//! `bind`, not reader config.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Read;
use std::path::Path;

use reg_frame::{
    Endpoint, Frame, Item, Link, Locator, Notice, Occurrence, Population, PopulationValueSet,
    Record, Register, Release, Table, ValueSet,
};
use serde::Deserialize;

use crate::names::Grammar;
use crate::{ReadError, ReadOutput, config, error, nonblank};

const REGISTERINFORMATION: &str = "Registerinformation.csv";
const VARDEMANGDER: &str = "Vardemangder.csv";
const UNIKA: &str = "UnikaRegisterOchVariabler.csv";
const IDENTIFIERS: &str = "Identifierare.csv";
const VALID_DATES: &str = "VardemangderValidDates.csv";
const TIMESERIES: &str = "Timeseries.csv";

/// `curation/readers/scb.toml`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// `Datatyp` → the frame's normalized type.
    pub types: BTreeMap<String, String>,
    /// Value-set versions that mark a column as carrying no codes (a number, free
    /// text) rather than listing any.
    pub uncoded: BTreeSet<String>,
    pub names: Grammar,
    pub timeseries: Timeseries,
}

/// `[timeseries]`: how a `Timeseries.csv` row reads. A row whose event or entity is
/// not mapped is left out and counted.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Timeseries {
    /// `Handelse` → what the row states.
    pub events: BTreeMap<String, Event>,
    /// `Entitet` → the entity its ids name.
    pub entities: BTreeMap<String, Entity>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Event {
    /// A notice on `ID1`'s entity: `<Handelse>: <Beskrivning>`.
    Notice,
    /// `ID1`'s entity takes over from `ID2`'s.
    Replaces,
    /// `ID2`'s entity takes over from `ID1`'s.
    ReplacedBy,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entity {
    /// The endpoint kind, which fixes the id space: `register` a `RegisterId`, `table`
    /// a `RegVarID`, `release` a `RegVerID`, `variable` a `VarId`.
    pub kind: Kind,
    /// What a succession between two such entities becomes.
    pub succession: Succession,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Register,
    Table,
    Release,
    Variable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Succession {
    /// A `continues` link from the newer entity to the older.
    Link,
    /// A notice on the newer entity: `continues <older entity's name>`, then the
    /// description after `: ` when stated.
    Notice,
}

impl Config {
    /// # Errors
    /// An unreadable or invalid config file.
    pub fn load(path: &Path) -> Result<Self, ReadError> {
        config(path)
    }
}

/// Read the export archive at `path` (named `name` in locators) of `provider`.
///
/// # Errors
/// An unreadable archive, a missing member or column, a malformed CSV row, or a
/// variant name two variants of one register share, each located at its member and
/// row.
///
/// # Panics
/// Never: every `CVID` row belongs to an occurrence read before it.
#[allow(clippy::too_many_lines)]
pub fn read(
    path: &Path,
    name: &str,
    provider: &str,
    config: &Config,
) -> Result<ReadOutput, ReadError> {
    let mut out = ReadOutput::default();
    let input = format!("{name}/{REGISTERINFORMATION}");
    let file = std::fs::File::open(path).map_err(|e| error(name, e.to_string()))?;
    let mut archive = zip::ZipArchive::new(file).map_err(|e| error(name, e.to_string()))?;
    let member = archive
        .by_name(REGISTERINFORMATION)
        .map_err(|e| error(&input, e.to_string()))?;
    let (mut reader, col) = open(member, &input)?;
    out.files.push(input.clone());
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
    let source = col("VariabelRegister_Källa")?;

    let mut registers: BTreeMap<String, Register> = BTreeMap::new();
    // (RegisterId, RegVarID) → table; (RegisterId, folded variant name) → RegVarID.
    let mut tables: BTreeMap<(String, String), Table> = BTreeMap::new();
    let mut variant_names: BTreeMap<(String, String), String> = BTreeMap::new();
    let mut releases: BTreeMap<String, Release> = BTreeMap::new();
    let mut occurrences: BTreeMap<(String, String, String), Occurrence> = BTreeMap::new();
    let mut cvids: BTreeMap<(String, String, String), Rows> = BTreeMap::new();
    // (register handle, source key, source register text) → its first row.
    let mut derived: BTreeMap<(String, String, String), Locator> = BTreeMap::new();
    let cvid = col("CVID")?;
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
        let handle = format!("r{rid}");
        if !registers.contains_key(&rid) {
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
        let stated = tables
            .entry((rid.clone(), vid.clone()))
            .or_insert_with(|| Table {
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
        if reg_core::fold_identity(&stated.table) != reg_core::fold_identity(&table) {
            return Err(error(
                &input,
                format!(
                    "{}: variant {vid} of register {rid} is named {table:?} here and {:?} at {}",
                    locator.at, stated.table, stated.locator.at
                ),
            ));
        }
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
        let population = get(population_name).map(|name| {
            let population = Population {
                name,
                definition: get(population_definition),
                // A range or no date states no one reference day.
                day: get(population_day).filter(|d| reg_core::iso_day_number(d).is_some()),
            };
            if let Some(i) = release.populations.iter().position(|p| *p == population) {
                i
            } else {
                release.populations.push(population);
                release.populations.len() - 1
            }
        });
        let key = format!("v{}", field(var_id, "VarId")?);
        if let Some(text) = get(source) {
            derived
                .entry((handle.clone(), key.clone(), text))
                .or_insert_with(|| locator.clone());
        }
        let spelling = row[column].trim().to_owned();
        let literal = get(dtype);
        let occurrence = Occurrence {
            provider: provider.to_owned(),
            register: handle,
            table,
            release: Some(version.clone()),
            column: spelling.clone(),
            source_key: key.clone(),
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
            conflicts: Vec::new(),
        };
        if let Some(c) = get(cvid) {
            let c = c
                .parse()
                .map_err(|_| error(&input, format!("{}: CVID {c:?}", occurrence.locator.at)))?;
            cvids
                .entry((version.clone(), key.clone(), spelling.clone()))
                .or_default()
                .push((population, c));
        }
        match occurrences.entry((version, key, spelling)) {
            std::collections::btree_map::Entry::Occupied(mut o) => {
                // Another population's row: one occurrence. The first row's text and
                // type stand as provisional, and fields a row disagrees on are recorded.
                let first = o.get_mut();
                let differs = [
                    ("label", first.label != occurrence.label),
                    ("definition", first.definition != occurrence.definition),
                    (
                        "operational_definition",
                        first.operational_definition != occurrence.operational_definition,
                    ),
                    ("unit", first.unit != occurrence.unit),
                    ("dtype", first.dtype != occurrence.dtype),
                    ("length", first.length != occurrence.length),
                ];
                for (field, differs) in differs {
                    if differs && !first.conflicts.iter().any(|f| f == field) {
                        first.conflicts.push(field.to_owned());
                    }
                }
                first.conflicts.sort();
                first.also.push(occurrence.locator);
            }
            std::collections::btree_map::Entry::Vacant(v) => {
                v.insert(occurrence);
            }
        }
    }

    drop(reader);

    let needed: BTreeSet<u64> = cvids.values().flatten().map(|&(_, c)| c).collect();
    let validity = valid_dates(&mut archive, name, &mut out)?;
    let lists = members(&mut archive, name, &needed, &mut out)?;
    let vs_input = format!("{name}/{VARDEMANGDER}");
    let mut sets: BTreeMap<Vec<u32>, Option<ValueSet>> = BTreeMap::new();
    let mut duplicated: BTreeSet<String> = BTreeSet::new();
    let mut expired = 0;
    for (k, rows) in &cvids {
        let release = &releases[&k.0];
        let period = release.from.as_ref().zip(release.to.as_ref());
        let mut hashes: Vec<(Option<usize>, Option<String>)> = Vec::new();
        let mut uncoded = true;
        for &(population, c) in rows {
            let members = lists.cvids.get(&c).map_or(&[][..], Vec::as_slice);
            let marks_no_codes = !members.is_empty()
                && members
                    .iter()
                    .all(|&m| lists.items[m as usize].uncoded(&config.uncoded));
            uncoded &= marks_no_codes;
            if marks_no_codes {
                hashes.push((population, None));
                continue;
            }
            // Validity filters a set only when every item is dated and some item is
            // kept in a placed release. Otherwise the dates record when items entered the
            // metadata, not the codes (a 1996 survey's codes "valid from" 2008), and the
            // whole set stands.
            let stated: Option<Vec<&Vec<Validity>>> = members
                .iter()
                .map(|&m| lists.items[m as usize].id.and_then(|id| validity.get(&id)))
                .collect();
            let mut valid: Vec<u32> = match (stated, period) {
                (Some(stated), Some((lo, hi))) => members
                    .iter()
                    .zip(stated)
                    .filter(|(_, intervals)| {
                        intervals.iter().any(|(from, to)| {
                            from.as_deref().is_none_or(|f| f <= hi.as_str())
                                && to.as_deref().is_none_or(|t| t >= lo.as_str())
                        })
                    })
                    .map(|(&m, _)| m)
                    .collect(),
                _ => members.to_vec(),
            };
            if valid.is_empty() && !members.is_empty() {
                expired += 1;
                valid = members.to_vec();
            }
            let set = sets.entry(valid.clone()).or_insert_with(|| {
                if valid.is_empty() {
                    return None;
                }
                let items: Vec<Item> = valid
                    .iter()
                    .map(|&m| {
                        let item = &lists.items[m as usize];
                        Item {
                            code: item.code.clone(),
                            label: item.label.clone(),
                            parent: None,
                        }
                    })
                    .collect();
                let codes: BTreeSet<&str> = items.iter().map(|i| i.code.as_str()).collect();
                let repeats = codes.len() < items.len();
                let mut set = ValueSet::new(
                    items,
                    None,
                    Locator {
                        input: vs_input.clone(),
                        at: format!("CVID={c}"),
                    },
                );
                // Every item's Värdemängdsversion is a name of the set.
                set.names = valid
                    .iter()
                    .map(|&m| lists.items[m as usize].version.clone())
                    .collect();
                if repeats {
                    duplicated.insert(set.hash.clone());
                }
                Some(set)
            });
            hashes.push((population, set.as_ref().map(|s| s.hash.clone())));
        }
        let o = occurrences
            .get_mut(k)
            .expect("every CVID row has its occurrence");
        if uncoded {
            o.coded = Some(false);
            continue;
        }
        // Rows that name different sets give their populations alternatives (F6),
        // a population with several rows possibly more than one.
        let pairs: BTreeSet<(Option<usize>, Option<String>)> = hashes.into_iter().collect();
        let distinct: BTreeSet<&Option<String>> = pairs.iter().map(|(_, h)| h).collect();
        if distinct.len() == 1 {
            o.valueset = pairs.into_iter().next().and_then(|(_, h)| h);
        } else {
            for (population, hash) in pairs {
                match (population, hash) {
                    (Some(population), Some(valueset)) => {
                        o.population_valuesets.push(PopulationValueSet {
                            population,
                            valueset,
                        });
                    }
                    (None, Some(_)) => out.warnings.push(format!(
                        "{}: {} names a value set for a row without a population; left out",
                        o.locator.at, o.column
                    )),
                    // A population whose rows name no codes has no value set.
                    (_, None) => {}
                }
            }
        }
    }
    // Index lists that differ can name equal content (equal codes and labels under
    // other ItemIds): one value set per hash.
    let mut by_hash: BTreeMap<String, ValueSet> = BTreeMap::new();
    for set in sets.into_values().flatten() {
        match by_hash.entry(set.hash.clone()) {
            std::collections::btree_map::Entry::Occupied(mut o) => {
                o.get_mut().names.extend(set.names);
            }
            std::collections::btree_map::Entry::Vacant(v) => {
                v.insert(set);
            }
        }
    }
    if expired > 0 {
        out.warnings.push(format!(
            "{vs_input}: {expired} rows name a value set with no item valid in their release's period; their whole set stands"
        ));
    }
    if !duplicated.is_empty() {
        out.warnings.push(format!(
            "{vs_input}: {} value sets repeat a code across levels; the first item of each code stands",
            duplicated.len()
        ));
    }
    out.files.push(vs_input);
    out.files.push(format!("{name}/{VALID_DATES}"));
    flags(&mut archive, name, &registers, &mut occurrences, &mut out)?;
    let surface = Surface::new(provider, &registers, &tables, &releases, &occurrences);
    let events = timeseries(&mut archive, name, &config.timeseries, &surface, &mut out)?;
    let links = derived.into_iter().map(|((register, key, text), locator)| {
        Record::Link(Link {
            kind: "derived_from".to_owned(),
            subject: endpoint(provider, "variable", &register, None, Some(&key)),
            object: Endpoint {
                kind: "register".to_owned(),
                provider: provider.to_owned(),
                register: None,
                register_name: Some(text),
                table: None,
                key: None,
            },
            at: None,
            note: None,
            locator,
        })
    });

    let records = registers
        .into_values()
        .map(Record::Register)
        .chain(tables.into_values().map(Record::Table))
        .chain(releases.into_values().map(Record::Release))
        .chain(by_hash.into_values().map(Record::ValueSet))
        .chain(occurrences.into_values().map(Record::Occurrence))
        .chain(links)
        .chain(events)
        .collect();
    out.frame = Frame::new(records);
    Ok(out)
}

/// The sensitivity and identifier flags. `UnikaRegisterOchVariabler` states them per
/// register, variant, variable and column by name: a column is sensitive when it is
/// always or sometimes so. `Identifierare` adds its identifiers.
fn flags(
    archive: &mut zip::ZipArchive<std::fs::File>,
    name: &str,
    registers: &BTreeMap<String, Register>,
    occurrences: &mut BTreeMap<(String, String, String), Occurrence>,
    out: &mut ReadOutput,
) -> Result<(), ReadError> {
    type Key = (String, String, String, String);
    let names: BTreeMap<&str, &str> = registers
        .values()
        .filter_map(|r| Some((r.register.as_str(), r.name.as_deref()?)))
        .collect();
    let mut by_name: BTreeMap<Key, Vec<(String, String, String)>> = BTreeMap::new();
    for (k, o) in occurrences.iter() {
        if let (Some(&register), Some(label)) = (names.get(o.register.as_str()), &o.label) {
            let key = (
                register.to_owned(),
                reg_core::fold_identity(&o.table),
                label.clone(),
                reg_core::fold_identity(&o.column),
            );
            by_name.entry(key).or_default().push(k.clone());
        }
    }

    let input = format!("{name}/{UNIKA}");
    let member = archive
        .by_name(UNIKA)
        .map_err(|e| error(&input, e.to_string()))?;
    let (mut reader, col) = open(member, &input)?;
    let (register, variant, variable, column) = (
        col("Registernamn")?,
        col("Registervariantnamn")?,
        col("Variabelnamn")?,
        col("Kolumnnamn")?,
    );
    let (always, sometimes, identity) = (
        col("KansligVariabel")?,
        col("KansligVariabelIbland")?,
        col("Identitetsvariabel")?,
    );
    let mut unmatched = 0;
    let mut record = ::csv::ByteRecord::new();
    while reader
        .read_byte_record(&mut record)
        .map_err(|e| error(&input, e.to_string()))?
    {
        let text = |c: usize| nonblank(&decode(&record[c]));
        let flag = |c: usize| match text(c).as_deref() {
            Some("1") => Some(true),
            Some("0") => Some(false),
            _ => None,
        };
        // `Option<bool>` orders None < Some(false) < Some(true): `max` lets a stated
        // flag win over an unstated one and `true` over `false`.
        let sensitive = flag(always).max(flag(sometimes));
        let identifier = flag(identity);
        let key = (
            text(register).unwrap_or_default(),
            reg_core::fold_identity(&text(variant).unwrap_or_default()),
            text(variable).unwrap_or_default(),
            reg_core::fold_identity(&text(column).unwrap_or_default()),
        );
        let Some(keys) = by_name.get(&key) else {
            unmatched += 1;
            continue;
        };
        for k in keys {
            let o = occurrences.get_mut(k).expect("indexed occurrence");
            // A column stated twice is flagged if either statement flags it.
            o.sensitive = o.sensitive.max(sensitive);
            o.identifier = o.identifier.max(identifier);
        }
    }
    drop(reader);
    out.files.push(input.clone());
    if unmatched > 0 {
        out.warnings.push(format!(
            "{input}: {unmatched} rows name no documented column; their flags are left out"
        ));
    }

    identifiers(archive, name, occurrences, out)
}

/// `Identifierare`: the `VarId`s that are identifiers wherever they occur.
fn identifiers(
    archive: &mut zip::ZipArchive<std::fs::File>,
    name: &str,
    occurrences: &mut BTreeMap<(String, String, String), Occurrence>,
    out: &mut ReadOutput,
) -> Result<(), ReadError> {
    let input = format!("{name}/{IDENTIFIERS}");
    let member = archive
        .by_name(IDENTIFIERS)
        .map_err(|e| error(&input, e.to_string()))?;
    let (mut reader, col) = open(member, &input)?;
    let var = col("VarID")?;
    let mut keys = BTreeSet::new();
    let mut record = ::csv::ByteRecord::new();
    let mut row = 1;
    while reader
        .read_byte_record(&mut record)
        .map_err(|e| error(&input, e.to_string()))?
    {
        row += 1;
        let id = number(&record[var]).ok_or_else(|| error(&input, format!("row={row}: VarID")))?;
        keys.insert(format!("v{id}"));
    }
    let mut found = BTreeSet::new();
    for o in occurrences.values_mut() {
        if keys.contains(&o.source_key) {
            o.identifier = Some(true);
            found.insert(o.source_key.clone());
        }
    }
    out.files.push(input.clone());
    if found.len() < keys.len() {
        out.warnings.push(format!(
            "{input}: {} VarIDs Registerinformation does not document",
            keys.len() - found.len()
        ));
    }
    Ok(())
}

/// An endpoint of `provider`'s register `register` (a handle).
fn endpoint(
    provider: &str,
    kind: &str,
    register: &str,
    table: Option<&str>,
    key: Option<&str>,
) -> Endpoint {
    Endpoint {
        kind: kind.to_owned(),
        provider: provider.to_owned(),
        register: Some(register.to_owned()),
        register_name: None,
        table: table.map(str::to_owned),
        key: key.map(str::to_owned),
    }
}

/// The export's entities by their ids, for `Timeseries`. `RegVarID` and `RegVerID` are
/// unique across the export; a `VarId` is documented in each register that holds it.
struct Surface<'a> {
    provider: &'a str,
    /// `RegisterId` → register.
    registers: &'a BTreeMap<String, Register>,
    /// `RegVarID` → table.
    tables: BTreeMap<&'a str, &'a Table>,
    /// `r<RegVerID>` → release.
    releases: &'a BTreeMap<String, Release>,
    /// Source key → register handle → the first occurrence's label.
    variables: BTreeMap<&'a str, BTreeMap<&'a str, Option<&'a str>>>,
}

impl<'a> Surface<'a> {
    fn new(
        provider: &'a str,
        registers: &'a BTreeMap<String, Register>,
        tables: &'a BTreeMap<(String, String), Table>,
        releases: &'a BTreeMap<String, Release>,
        occurrences: &'a BTreeMap<(String, String, String), Occurrence>,
    ) -> Self {
        let mut variables: BTreeMap<&str, BTreeMap<&str, Option<&str>>> = BTreeMap::new();
        for o in occurrences.values() {
            variables
                .entry(o.source_key.as_str())
                .or_default()
                .entry(o.register.as_str())
                .or_insert(o.label.as_deref());
        }
        Surface {
            provider,
            registers,
            tables: tables
                .iter()
                .map(|((_, vid), t)| (vid.as_str(), t))
                .collect(),
            releases,
            variables,
        }
    }

    /// The entities `id` names as `kind`, each with its name: none for an id the export
    /// does not document, several for a `VarId` documented in several registers.
    fn ends(&self, kind: Kind, id: &str) -> Vec<(Endpoint, String)> {
        let p = self.provider;
        match kind {
            Kind::Register => self
                .registers
                .get(id)
                .map(|r| {
                    let name = r.name.clone().unwrap_or_else(|| r.register.clone());
                    (endpoint(p, "register", &r.register, None, None), name)
                })
                .into_iter()
                .collect(),
            Kind::Table => self
                .tables
                .get(id)
                .map(|t| {
                    let end = endpoint(p, "table", &t.register, Some(&t.table), None);
                    (end, t.table.clone())
                })
                .into_iter()
                .collect(),
            Kind::Release => self
                .releases
                .get(&format!("r{id}"))
                .map(|r| {
                    let end = endpoint(p, "release", &r.register, Some(&r.table), Some(&r.release));
                    (end, r.name.clone())
                })
                .into_iter()
                .collect(),
            Kind::Variable => {
                let key = format!("v{id}");
                self.variables
                    .get(key.as_str())
                    .into_iter()
                    .flatten()
                    .map(|(register, label)| {
                        let end = endpoint(p, "variable", register, None, Some(&key));
                        (end, label.map_or_else(|| key.clone(), str::to_owned))
                    })
                    .collect()
            }
        }
    }
}

/// `Timeseries`: notices and successions, as `[timeseries]` maps them.
#[allow(clippy::too_many_lines)]
fn timeseries(
    archive: &mut zip::ZipArchive<std::fs::File>,
    name: &str,
    config: &Timeseries,
    surface: &Surface,
    out: &mut ReadOutput,
) -> Result<Vec<Record>, ReadError> {
    let input = format!("{name}/{TIMESERIES}");
    let member = archive
        .by_name(TIMESERIES)
        .map_err(|e| error(&input, e.to_string()))?;
    let (mut reader, col) = open(member, &input)?;
    let (event, description, entity, id1, id2) = (
        col("Handelse")?,
        col("Beskrivning")?,
        col("Entitet")?,
        col("ID1")?,
        col("ID2")?,
    );
    let mut records = Vec::new();
    let mut unmapped: BTreeMap<(&str, String), usize> = BTreeMap::new();
    let (mut unknown, mut unpaired) = (0, 0);
    let mut record = ::csv::ByteRecord::new();
    let mut row = 1;
    while reader
        .read_byte_record(&mut record)
        .map_err(|e| error(&input, e.to_string()))?
    {
        row += 1;
        let text = |c: usize| decode(&record[c]).trim().to_owned();
        let locator = Locator {
            input: input.clone(),
            at: format!("row={row}"),
        };
        let (handelse, entitet) = (text(event), text(entity));
        let (Some(&ev), Some(&en)) = (config.events.get(&handelse), config.entities.get(&entitet))
        else {
            if !config.events.contains_key(&handelse) {
                *unmapped.entry(("Handelse", handelse)).or_default() += 1;
            }
            if !config.entities.contains_key(&entitet) {
                *unmapped.entry(("Entitet", entitet)).or_default() += 1;
            }
            continue;
        };
        let note = nonblank(&text(description));
        let first = surface.ends(en.kind, &text(id1));
        if first.is_empty() {
            unknown += 1;
            continue;
        }
        if ev == Event::Notice {
            let body = note.map_or_else(|| handelse.clone(), |n| format!("{handelse}: {n}"));
            for (subject, _) in first {
                records.push(Record::Notice(Notice {
                    subject,
                    at: None,
                    text: body.clone(),
                    locator: locator.clone(),
                }));
            }
            continue;
        }
        let second = surface.ends(en.kind, &text(id2));
        if second.is_empty() {
            unknown += 1;
            continue;
        }
        let (newer, older) = if ev == Event::Replaces {
            (first, second)
        } else {
            (second, first)
        };
        // One entity at each end pairs as stated; a `VarId` documented in several
        // registers pairs within each register that documents both.
        let pairs: Vec<_> = if newer.len() == 1 && older.len() == 1 {
            newer.into_iter().zip(older).collect()
        } else {
            newer
                .iter()
                .flat_map(|n| {
                    older
                        .iter()
                        .filter(|o| o.0.register == n.0.register)
                        .map(|o| (n.clone(), o.clone()))
                })
                .collect()
        };
        if pairs.is_empty() {
            unpaired += 1;
        }
        for ((subject, _), (object, older_name)) in pairs {
            records.push(match en.succession {
                Succession::Link => Record::Link(Link {
                    kind: "continues".to_owned(),
                    subject,
                    object,
                    at: None,
                    note: note.clone(),
                    locator: locator.clone(),
                }),
                Succession::Notice => Record::Notice(Notice {
                    subject,
                    at: None,
                    text: match &note {
                        Some(n) => format!("continues {older_name}: {n}"),
                        None => format!("continues {older_name}"),
                    },
                    locator: locator.clone(),
                }),
            });
        }
    }
    drop(reader);
    out.files.push(input.clone());
    for ((column, value), n) in unmapped {
        out.warnings.push(format!(
            "{input}: {n} rows of {column} {value:?}, which [timeseries] does not map; left out"
        ));
    }
    if unknown > 0 {
        out.warnings.push(format!(
            "{input}: {unknown} rows name an id Registerinformation does not document as their Entitet; left out"
        ));
    }
    if unpaired > 0 {
        out.warnings.push(format!(
            "{input}: {unpaired} successions name variables no register documents both of; left out"
        ));
    }
    Ok(records)
}

/// An item's validity: inclusive ISO days, unstated where blank.
type Validity = (Option<String>, Option<String>);

/// An occurrence's rows: each one's population (an index into its release's) and CVID.
type Rows = Vec<(Option<usize>, u64)>;

type Columns<'a> = Box<dyn Fn(&str) -> Result<usize, ReadError> + 'a>;

/// A CSV reader over an export member, and its column lookup by header name.
fn open<R: Read>(member: R, input: &str) -> Result<(::csv::Reader<R>, Columns<'_>), ReadError> {
    let mut reader = ::csv::ReaderBuilder::new()
        .delimiter(b'|')
        .from_reader(member);
    let header: Vec<String> = reader
        .byte_headers()
        .map_err(|e| error(input, e.to_string()))?
        .iter()
        .map(decode)
        .collect();
    let col = move |name: &str| {
        header
            .iter()
            .position(|h| h == name)
            .ok_or_else(|| error(input, format!("no column {name:?}")))
    };
    Ok((reader, Box::new(col)))
}

/// `ItemId` → its validity, inclusive ISO days, unstated where blank.
fn valid_dates(
    archive: &mut zip::ZipArchive<std::fs::File>,
    name: &str,
    out: &mut ReadOutput,
) -> Result<BTreeMap<u64, Vec<Validity>>, ReadError> {
    let input = format!("{name}/{VALID_DATES}");
    let member = archive
        .by_name(VALID_DATES)
        .map_err(|e| error(&input, e.to_string()))?;
    let (mut reader, col) = open(member, &input)?;
    let (item, from, to) = (col("ItemID")?, col("ValidFrom")?, col("ValidTo")?);
    let mut dates = BTreeMap::new();
    let mut unreadable = 0;
    let mut record = ::csv::ByteRecord::new();
    let mut row = 1;
    while reader
        .read_byte_record(&mut record)
        .map_err(|e| error(&input, e.to_string()))?
    {
        row += 1;
        let id =
            number(&record[item]).ok_or_else(|| error(&input, format!("row={row}: ItemID")))?;
        let day = |c: usize| nonblank(&decode(&record[c]));
        let (lo, hi) = (day(from), day(to));
        let valid = |d: &Option<String>| {
            d.as_deref()
                .is_none_or(|d| reg_core::iso_day_number(d).is_some())
        };
        if valid(&lo) && valid(&hi) {
            dates.entry(id).or_insert_with(Vec::new).push((lo, hi));
        } else {
            unreadable += 1;
        }
    }
    if unreadable > 0 {
        out.warnings.push(format!(
            "{input}: {unreadable} rows state a validity that is not an ISO day and are skipped; an item with no other row is undated"
        ));
    }
    Ok(dates)
}

/// One value-set item, interned by its `ItemId` and text: the export reuses an `ItemId`
/// for different codes, even within one `CVID`.
struct ItemText {
    id: Option<u64>,
    version: String,
    level: String,
    code: String,
    label: Option<String>,
}

impl ItemText {
    fn uncoded(&self, names: &BTreeSet<String>) -> bool {
        self.code == self.version && self.code == self.level && names.contains(&self.code)
    }
}

/// The items of the needed `CVID`s, each kept once, and each `CVID`'s item indices.
struct Lists {
    items: Vec<ItemText>,
    cvids: BTreeMap<u64, Vec<u32>>,
}

/// Stream `Vardemangder`, keeping the rows of the `needed` `CVID`s.
fn members(
    archive: &mut zip::ZipArchive<std::fs::File>,
    name: &str,
    needed: &BTreeSet<u64>,
    out: &mut ReadOutput,
) -> Result<Lists, ReadError> {
    let input = format!("{name}/{VARDEMANGDER}");
    let member = archive
        .by_name(VARDEMANGDER)
        .map_err(|e| error(&input, e.to_string()))?;
    let (mut reader, col) = open(member, &input)?;
    let (version, level, code, label, cvid, item) = (
        col("Värdemängdsversion")?,
        col("Värdemängdsnivå")?,
        col("Värdekod")?,
        col("Värdebenämning")?,
        col("CVID")?,
        col("ItemId")?,
    );
    let mut lists = Lists {
        items: Vec::new(),
        cvids: BTreeMap::new(),
    };
    // Keyed by the row's raw `ItemId` and text bytes, so text is decoded once per item.
    let mut index: HashMap<Vec<u8>, u32> = HashMap::new();
    let mut key = Vec::new();
    let mut without_id = 0;
    let mut record = ::csv::ByteRecord::new();
    let mut row = 1;
    while reader
        .read_byte_record(&mut record)
        .map_err(|e| error(&input, e.to_string()))?
    {
        row += 1;
        let c = number(&record[cvid]).ok_or_else(|| error(&input, format!("row={row}: CVID")))?;
        if !needed.contains(&c) {
            continue;
        }
        let text = |c: usize| decode(&record[c]).trim().to_owned();
        let id = if record[item].iter().all(u8::is_ascii_whitespace) {
            without_id += 1;
            None
        } else {
            Some(number(&record[item]).ok_or_else(|| error(&input, format!("row={row}: ItemId")))?)
        };
        key.clear();
        for c in [item, version, level, code, label] {
            key.extend_from_slice(record[c].trim_ascii());
            key.push(0);
        }
        let m = if let Some(&m) = index.get(key.as_slice()) {
            m
        } else {
            lists.items.push(ItemText {
                id,
                version: text(version),
                level: text(level),
                code: text(code),
                label: nonblank(&text(label)),
            });
            let m = u32::try_from(lists.items.len() - 1).expect("items fit in u32");
            index.insert(key.clone(), m);
            m
        };
        lists.cvids.entry(c).or_default().push(m);
    }
    if without_id > 0 {
        out.warnings.push(format!(
            "{input}: {without_id} rows have no ItemId; their items are told apart by text and undated"
        ));
    }
    let missing = needed
        .iter()
        .filter(|c| !lists.cvids.contains_key(c))
        .count();
    if missing > 0 {
        out.warnings.push(format!(
            "{input}: {missing} CVIDs Registerinformation names have no rows"
        ));
    }
    for members in lists.cvids.values_mut() {
        members.sort_unstable();
        members.dedup();
    }
    Ok(lists)
}

/// An unsigned decimal field.
fn number(bytes: &[u8]) -> Option<u64> {
    std::str::from_utf8(bytes).ok()?.trim().parse().ok()
}

/// CP1252 bytes as text. The five bytes CP1252 leaves undefined occur in the export as
/// DOS CP850 remnants and decode as CP850.
fn decode(bytes: &[u8]) -> String {
    let (text, _) = encoding_rs::WINDOWS_1252.decode_without_bom_handling(bytes);
    text.chars()
        .map(|c| match c {
            '\u{81}' => 'ü',
            '\u{8D}' => 'ì',
            '\u{8F}' => 'Å',
            '\u{90}' => 'É',
            '\u{9D}' => 'Ø',
            c => c,
        })
        .collect()
}
