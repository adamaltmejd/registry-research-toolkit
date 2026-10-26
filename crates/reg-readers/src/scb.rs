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
//! - `VardemangderValidDates.csv`: validity per item; an item without one is always
//!   valid. An occurrence's value set keeps the items valid in its release's period.
//!
//! An occurrence whose populations name different value sets states them per
//! population. A `CVID` holding only a sentinel (an item whose code, version and level
//! are one of the config's `sentinels`) states that the column carries no codes.
//!
//! Register handles are `r<RegisterId>`, release handles `r<RegVerID>`, table handles
//! the variant's name and source keys `v<VarId>`. A register's public name is a curated
//! `bind`, not reader config.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::path::Path;

use reg_frame::{
    Frame, Item, Locator, Occurrence, Population, PopulationValueSet, Record, Register, Release,
    Table, ValueSet,
};
use serde::Deserialize;

use crate::names::Grammar;
use crate::{ReadError, ReadOutput, config, error, nonblank};

const REGISTERINFORMATION: &str = "Registerinformation.csv";
const VARDEMANGDER: &str = "Vardemangder.csv";
const VALID_DATES: &str = "VardemangderValidDates.csv";

/// `curation/readers/scb.toml`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// `Datatyp` → the frame's normalized type.
    pub types: BTreeMap<String, String>,
    /// Value-set versions that mark a column as carrying no codes (a number, free
    /// text) rather than listing any.
    pub sentinels: BTreeSet<String>,
    pub names: Grammar,
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

    let mut registers: BTreeMap<String, Register> = BTreeMap::new();
    // (RegisterId, RegVarID) → table; (RegisterId, folded variant name) → RegVarID.
    let mut tables: BTreeMap<(String, String), Table> = BTreeMap::new();
    let mut variant_names: BTreeMap<(String, String), String> = BTreeMap::new();
    let mut releases: BTreeMap<String, Release> = BTreeMap::new();
    let mut occurrences: BTreeMap<(String, String, String), Occurrence> = BTreeMap::new();
    let mut cvids: BTreeMap<(String, String, String), Rows> = BTreeMap::new();
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
                // Another population's row: one occurrence, so the first row's text
                // and type stand, and a row that differs is reported.
                let first = o.get_mut();
                let facts = |o: &Occurrence| {
                    (
                        o.label.clone(),
                        o.definition.clone(),
                        o.operational_definition.clone(),
                        o.unit.clone(),
                        o.dtype.clone(),
                        o.length,
                    )
                };
                if facts(first) != facts(&occurrence) {
                    out.warnings.push(format!(
                        "{}: {} differs from its other population's row at {}; the first stands",
                        occurrence.locator.at, occurrence.column, first.locator.at
                    ));
                }
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
    let mut contested = 0;
    let mut expired = 0;
    for (k, rows) in &cvids {
        let release = &releases[&k.0];
        let period = release.from.as_ref().zip(release.to.as_ref());
        let mut hashes: Vec<(Option<usize>, Option<String>)> = Vec::new();
        let mut sentinel = true;
        for &(population, c) in rows {
            let members = lists.cvids.get(&c).map_or(&[][..], Vec::as_slice);
            let marks_no_codes = !members.is_empty()
                && members
                    .iter()
                    .all(|&m| lists.items[m as usize].sentinel(&config.sentinels));
            sentinel &= marks_no_codes;
            if marks_no_codes {
                hashes.push((population, None));
                continue;
            }
            let mut valid: Vec<u32> = members
                .iter()
                .copied()
                .filter(|&m| {
                    // An item with no stated validity, or in an unplaced release, stays.
                    let Some(stated) = lists.items[m as usize].id.and_then(|id| validity.get(&id))
                    else {
                        return true;
                    };
                    period.is_none_or(|(lo, hi)| {
                        stated.iter().any(|(from, to)| {
                            from.as_deref().is_none_or(|f| f <= hi.as_str())
                                && to.as_deref().is_none_or(|t| t >= lo.as_str())
                        })
                    })
                })
                .collect();
            // Validity that leaves no item dates the metadata entry, not the codes
            // (a 1996 survey's codes "valid from" 2008): the whole set stands.
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
                let set = ValueSet::new(
                    items,
                    Some(lists.items[valid[0] as usize].version.clone()),
                    Locator {
                        input: vs_input.clone(),
                        at: format!("CVID={c}"),
                    },
                );
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
        if sentinel {
            o.coded = Some(false);
            continue;
        }
        // Rows of one population may name several CVIDs; the first row's set stands.
        let mut by_population: BTreeMap<Option<usize>, Option<String>> = BTreeMap::new();
        let mut conflict = false;
        for (population, hash) in hashes {
            let first = by_population
                .entry(population)
                .or_insert_with(|| hash.clone());
            conflict |= *first != hash;
        }
        contested += usize::from(conflict);
        let distinct: BTreeSet<&Option<String>> = by_population.values().collect();
        if distinct.len() == 1 {
            o.valueset = by_population.into_values().next().flatten();
        } else {
            for (population, hash) in by_population {
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
    if contested > 0 {
        out.warnings.push(format!(
            "{vs_input}: {contested} occurrences name differing value sets for one population; the first row's stands"
        ));
    }
    // Index lists that differ can name equal content (equal codes and labels under
    // other ItemIds): one value set per hash.
    let mut by_hash: BTreeMap<String, ValueSet> = BTreeMap::new();
    for set in sets.into_values().flatten() {
        by_hash.entry(set.hash.clone()).or_insert(set);
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

    let records = registers
        .into_values()
        .map(Record::Register)
        .chain(tables.into_values().map(Record::Table))
        .chain(releases.into_values().map(Record::Release))
        .chain(by_hash.into_values().map(Record::ValueSet))
        .chain(occurrences.into_values().map(Record::Occurrence))
        .collect();
    out.frame = Frame::new(records);
    Ok(out)
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
            "{input}: {unreadable} rows state a validity that is not an ISO day and are skipped; an item with no other row is always valid"
        ));
    }
    Ok(dates)
}

/// One value-set item, interned by its `ItemId`, or by its text when it has none.
struct ItemText {
    id: Option<u64>,
    version: String,
    level: String,
    code: String,
    label: Option<String>,
}

impl ItemText {
    fn sentinel(&self, sentinels: &BTreeSet<String>) -> bool {
        self.code == self.version && self.code == self.level && sentinels.contains(&self.code)
    }
}

/// The items of the needed `CVID`s, each kept once, and each `CVID`'s item indices.
struct Lists {
    items: Vec<ItemText>,
    cvids: BTreeMap<u64, Vec<u32>>,
}

/// Stream `Vardemangder`, keeping the rows of the `needed` `CVID`s. Text is decoded
/// once per item.
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
    let mut index: BTreeMap<u64, u32> = BTreeMap::new();
    let mut unnumbered: BTreeMap<[String; 4], u32> = BTreeMap::new();
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
        let mut push = |id: Option<u64>, [version, level, code, label]: [String; 4]| {
            lists.items.push(ItemText {
                id,
                version,
                level,
                code,
                label: nonblank(&label),
            });
            u32::try_from(lists.items.len() - 1).expect("items fit in u32")
        };
        let m = if record[item].iter().all(u8::is_ascii_whitespace) {
            without_id += 1;
            let texts = [text(version), text(level), text(code), text(label)];
            if let Some(&m) = unnumbered.get(&texts) {
                m
            } else {
                let m = push(None, texts.clone());
                unnumbered.insert(texts, m);
                m
            }
        } else {
            let id =
                number(&record[item]).ok_or_else(|| error(&input, format!("row={row}: ItemId")))?;
            if let Some(&m) = index.get(&id) {
                m
            } else {
                let m = push(
                    Some(id),
                    [text(version), text(level), text(code), text(label)],
                );
                index.insert(id, m);
                m
            }
        };
        lists.cvids.entry(c).or_default().push(m);
    }
    if without_id > 0 {
        out.warnings.push(format!(
            "{input}: {without_id} rows have no ItemId; their items are told apart by text and always valid"
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
