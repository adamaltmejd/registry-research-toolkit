//! SCB's LISA variable list: one XLSX workbook, a sheet per table (individual,
//! year-independent individual, firm, workplace). Header aliases and the sensitivity
//! vocabulary come from `curation/readers/lisa.toml`.
//!
//! Each sheet has a header row (the first with a column alias in any cell) and then
//! data rows: a column, its description, its years and, on some sheets, a sensitivity
//! flag, each column found by its header alias. A row without a column or a label is a section heading or a note and is
//! skipped (with a warning when it states years or sensitivity). A year cell holds
//! years and ranges (`1990-2024`, `2003-2008\n2013`); each range is one occurrence of
//! the column. A sheet with no year column, or a blank year cell, leaves the bounds
//! unstated. A blank-year row on a sheet with years that repeats a column of a sheet
//! without years (`Individ`'s year-independent block) is left to that sheet.
//!
//! Table handles are the sheet names and source keys the folded column name.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use calamine::{Data, Range, Reader, Xlsx, open_workbook};
use reg_frame::{Frame, Locator, Occurrence, Record, Register, Table};
use serde::Deserialize;

use crate::workbook::{cell, text};
use crate::{ReadError, ReadOutput, config, error, name_key};

/// `curation/readers/lisa.toml`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    headers: Headers,
    /// A sensitivity cell, lowercased → whether the column is sensitive.
    sensitive: BTreeMap<String, bool>,
    /// Sheets that must have a sensitivity column.
    sensitive_sheets: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Headers {
    column: Vec<String>,
    label: Vec<String>,
    years: Vec<String>,
    sensitive: Vec<String>,
}

impl Config {
    /// # Errors
    /// An unreadable or invalid config file.
    pub fn load(path: &Path) -> Result<Self, ReadError> {
        config(path)
    }
}

/// A data row: its 1-based sheet row, column, label, year cell and sensitivity flag.
struct Entry {
    row: usize,
    column: String,
    label: String,
    years: Option<String>,
    sensitive: Option<bool>,
}

/// A sheet: its title cell and that cell's address, the column-name column's letter,
/// whether it has a year column, and its data rows.
struct Sheet {
    name: String,
    title: Option<String>,
    title_at: String,
    column: String,
    years: bool,
    entries: Vec<Entry>,
}

/// Read the workbook at `rel` as register `register` of `provider`.
///
/// # Errors
/// An unreadable workbook, a sheet with no header row, or a sheet `lisa.toml` names
/// as stating sensitivity that has no sensitivity column.
pub fn read(
    root: &Path,
    rel: &str,
    provider: &str,
    register: &str,
    config: &Config,
) -> Result<ReadOutput, ReadError> {
    let mut workbook: Xlsx<_> = open_workbook(root.join(rel))
        .map_err(|e: calamine::XlsxError| error(rel, e.to_string()))?;
    let mut out = ReadOutput::default();
    out.files.push(rel.to_owned());
    let mut sheets = Vec::new();
    for name in workbook.sheet_names() {
        let range = workbook
            .worksheet_range(&name)
            .map_err(|e| error(rel, format!("sheet {name:?}: {e}")))?;
        sheets.push(sheet(rel, name, &range, config, &mut out.warnings)?);
    }
    // A sheet with years lists some year-independent columns again with a blank year
    // cell; the sheet without years is where they belong.
    let timeless: BTreeSet<String> = sheets
        .iter()
        .filter(|s| !s.years)
        .flat_map(|s| s.entries.iter().map(|e| name_key(&e.column)))
        .collect();
    let mut repeated: BTreeMap<String, usize> = BTreeMap::new();
    let mut records = vec![Record::Register(Register {
        provider: provider.to_owned(),
        register: register.to_owned(),
        name: None,
        description: None,
        from: None,
        to: None,
        locator: Locator {
            input: rel.to_owned(),
            at: String::new(),
        },
    })];
    for sheet in sheets {
        let at = |row: usize| Locator {
            input: rel.to_owned(),
            at: format!("{}!{}{row}", sheet.name, sheet.column),
        };
        records.push(Record::Table(Table {
            provider: provider.to_owned(),
            register: register.to_owned(),
            table: sheet.name.clone(),
            name: sheet.title.clone(),
            description: None,
            from: None,
            to: None,
            cadence: None,
            locator: Locator {
                input: rel.to_owned(),
                at: format!("{}!{}", sheet.name, sheet.title_at),
            },
        }));
        for entry in sheet.entries {
            if sheet.years && entry.years.is_none() && timeless.contains(&name_key(&entry.column)) {
                *repeated.entry(sheet.name.clone()).or_default() += 1;
                continue;
            }
            let periods = bounds(entry.years.as_deref()).unwrap_or_else(|| {
                out.warnings.push(format!(
                    "{}!A{}: years {:?} are not years or ranges; bounds left unstated",
                    sheet.name,
                    entry.row,
                    entry.years.clone().unwrap_or_default()
                ));
                vec![(None, None)]
            });
            for (from, to) in periods {
                records.push(Record::Occurrence(Occurrence {
                    provider: provider.to_owned(),
                    register: register.to_owned(),
                    table: sheet.name.clone(),
                    release: None,
                    column: entry.column.clone(),
                    source_key: name_key(&entry.column),
                    from,
                    to,
                    label: Some(entry.label.clone()),
                    definition: None,
                    operational_definition: None,
                    unit: None,
                    dtype: None,
                    length: None,
                    valueset: None,
                    population_valuesets: Vec::new(),
                    coded: None,
                    code_system: None,
                    sensitive: entry.sensitive,
                    identifier: None,
                    locator: at(entry.row),
                    also: Vec::new(),
                    conflicts: Vec::new(),
                }));
            }
        }
    }
    if !repeated.is_empty() {
        let counts: Vec<String> = repeated.iter().map(|(s, n)| format!("{s} {n}")).collect();
        out.warnings.push(format!(
            "rows with a blank year cell repeat a column a sheet without years lists; that sheet's row stands ({})",
            counts.join(", ")
        ));
    }
    out.frame = Frame::new(records);
    Ok(out)
}

/// One sheet's header and data rows.
fn sheet(
    rel: &str,
    name: String,
    range: &Range<Data>,
    config: &Config,
    warnings: &mut Vec<String>,
) -> Result<Sheet, ReadError> {
    // Rows and columns count from the sheet's first, not the used range's.
    let (first_row, first_col) = range
        .start()
        .map_or((0, 0), |(r, c)| (r as usize, c as usize));
    let offset = first_row + 1;
    let rows: Vec<&[Data]> = range.rows().collect();
    let alias = |c: Option<String>, aliases: &[String]| {
        c.is_some_and(|c| {
            aliases
                .iter()
                .any(|a| a.trim().to_lowercase() == c.trim().to_lowercase())
        })
    };
    let (header, column) = rows
        .iter()
        .enumerate()
        .find_map(|(i, r)| {
            r.iter()
                .position(|c| alias(text(c), &config.headers.column))
                .map(|c| (i, c))
        })
        .ok_or_else(|| error(rel, format!("sheet {name:?}: no header row")))?;
    let col = |aliases: &[String]| rows[header].iter().position(|c| alias(text(c), aliases));
    let (label, years, sensitive) = (
        col(&config.headers.label),
        col(&config.headers.years),
        col(&config.headers.sensitive),
    );
    if sensitive.is_none() && alias(Some(name.clone()), &config.sensitive_sheets) {
        return Err(error(rel, format!("sheet {name:?}: no sensitivity column")));
    }
    let mut entries = Vec::new();
    for (i, row) in rows.iter().enumerate().skip(header + 1) {
        let at = i + offset;
        let (Some(column), Some(label)) = (cell(row, Some(column)), cell(row, label)) else {
            // Headings and notes state no years and no sensitivity.
            if cell(row, years).is_some() || cell(row, sensitive).is_some() {
                warnings.push(format!("{name}!A{at}: a row with years or sensitivity but no column or label is skipped"));
            }
            continue;
        };
        let stated = cell(row, sensitive).map(|s| s.to_lowercase());
        let flag = stated
            .as_ref()
            .and_then(|s| config.sensitive.get(s).copied());
        if let (Some(s), None) = (&stated, flag) {
            warnings.push(format!(
                "{name}!A{at}: sensitivity {s:?} is not in lisa.toml"
            ));
        }
        entries.push(Entry {
            row: at,
            column,
            label,
            years: cell(row, years),
            sensitive: flag,
        });
    }
    Ok(Sheet {
        title: rows.first().and_then(|r| cell(r, Some(0))),
        title_at: format!("{}{offset}", letter(first_col)),
        column: letter(first_col + column),
        years: years.is_some(),
        name,
        entries,
    })
}

/// A 0-based column index as its spreadsheet letters (0 → `A`, 26 → `AA`).
fn letter(mut index: usize) -> String {
    let mut out = Vec::new();
    loop {
        out.push(b'A' + u8::try_from(index % 26).expect("below 26"));
        if index < 26 {
            break;
        }
        index = index / 26 - 1;
    }
    out.reverse();
    String::from_utf8(out).expect("ASCII")
}

/// The bounds a year cell states: one pair per year or range, a pair of unstated
/// bounds for a blank cell, `None` for a cell that is not years and ranges.
fn bounds(cell: Option<&str>) -> Option<Vec<(Option<String>, Option<String>)>> {
    let Some(cell) = cell else {
        return Some(vec![(None, None)]);
    };
    Some(
        ranges(cell)?
            .into_iter()
            .map(|(from, to)| (Some(format!("{from}-01-01")), Some(format!("{to}-12-31"))))
            .collect(),
    )
}

/// A year cell's years and ranges (`1990-2021 2024`), each as (first, last) year;
/// `None` when a token is neither.
fn ranges(cell: &str) -> Option<Vec<(u32, u32)>> {
    let year = |t: &str| t.parse::<u32>().ok().filter(|_| t.len() == 4);
    cell.split_whitespace()
        .map(|token| match token.split_once('-') {
            Some((from, to)) => Some((year(from)?, year(to)?)).filter(|(f, t)| f <= t),
            None => year(token).map(|y| (y, y)),
        })
        .collect()
}
