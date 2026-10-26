//! SCB's LISA variable list: one XLSX workbook, a sheet per table (individual,
//! year-independent individual, firm, workplace). Header aliases and the sensitivity
//! vocabulary come from `curation/readers/lisa.toml`.
//!
//! Each sheet has a header row (the first whose first cell is a column alias) and then
//! data rows: a column, its description, its years and, on some sheets, a sensitivity
//! flag. A row with only its first cell filled is a section heading and is skipped.
//! A year cell holds years and ranges (`1990-2024`, `2003-2008\n2013`); each range is
//! one occurrence of the column. A sheet with no year column, or a blank year cell,
//! leaves the bounds unstated.
//!
//! Table handles are the sheet names and source keys the folded column name.

use std::collections::BTreeMap;
use std::path::Path;

use calamine::{Data, Reader, Xlsx, open_workbook};
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

/// Read the workbook at `rel` as register `register` of `provider`.
///
/// # Errors
/// An unreadable workbook, or a sheet with no header row.
#[allow(clippy::too_many_lines)]
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
    for sheet in workbook.sheet_names() {
        let range = workbook
            .worksheet_range(&sheet)
            .map_err(|e| error(rel, format!("sheet {sheet:?}: {e}")))?;
        let rows: Vec<&[Data]> = range.rows().collect();
        let alias = |c: Option<String>, aliases: &[String]| {
            c.is_some_and(|c| aliases.iter().any(|a| a.eq_ignore_ascii_case(&c)))
        };
        let header = rows
            .iter()
            .position(|r| {
                alias(
                    cell(r, Some(0)).map(|c| c.to_lowercase()),
                    &config.headers.column,
                )
            })
            .ok_or_else(|| error(rel, format!("sheet {sheet:?}: no header row")))?;
        let col = |aliases: &[String]| {
            rows[header]
                .iter()
                .position(|c| alias(text(c).map(|c| c.to_lowercase()), aliases))
        };
        let (label, years, sensitive) = (
            col(&config.headers.label),
            col(&config.headers.years),
            col(&config.headers.sensitive),
        );
        let at = |row: usize| Locator {
            input: rel.to_owned(),
            at: format!("{sheet}!A{}", row + 1),
        };
        records.push(Record::Table(Table {
            provider: provider.to_owned(),
            register: register.to_owned(),
            table: sheet.clone(),
            name: rows.first().and_then(|r| cell(r, Some(0))),
            description: None,
            from: None,
            to: None,
            cadence: None,
            locator: at(0),
        }));
        for (i, row) in rows.iter().enumerate().skip(header + 1) {
            let (Some(column), Some(label)) = (cell(row, Some(0)), cell(row, label)) else {
                continue;
            };
            let stated = cell(row, sensitive).map(|s| s.to_lowercase());
            let flag = stated
                .as_ref()
                .and_then(|s| config.sensitive.get(s).copied());
            if let (Some(s), None) = (&stated, flag) {
                out.warnings.push(format!(
                    "{sheet}!A{}: sensitivity {s:?} is not in lisa.toml",
                    i + 1
                ));
            }
            let stated_years = cell(row, years);
            let periods = bounds(stated_years.as_deref()).unwrap_or_else(|| {
                out.warnings.push(format!(
                    "{sheet}!A{}: years {:?} are not years or ranges; bounds left unstated",
                    i + 1,
                    stated_years.unwrap_or_default()
                ));
                vec![(None, None)]
            });
            for (from, to) in periods {
                records.push(Record::Occurrence(Occurrence {
                    provider: provider.to_owned(),
                    register: register.to_owned(),
                    table: sheet.clone(),
                    release: None,
                    column: column.clone(),
                    source_key: name_key(&column),
                    from,
                    to,
                    label: Some(label.clone()),
                    definition: None,
                    operational_definition: None,
                    unit: None,
                    dtype: None,
                    length: None,
                    valueset: None,
                    population_valuesets: Vec::new(),
                    coded: None,
                    code_system: None,
                    sensitive: flag,
                    identifier: None,
                    locator: at(i),
                    also: Vec::new(),
                }));
            }
        }
    }
    out.frame = Frame::new(records);
    Ok(out)
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
