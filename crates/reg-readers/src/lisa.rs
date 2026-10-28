//! SCB's LISA variable list: one XLSX workbook, a sheet per table (individual,
//! year-independent individual, firm, workplace). Header aliases and the sensitivity
//! vocabulary come from `curation/readers/lisa.toml`.
//!
//! Each sheet has a header row (the first with a column alias in any cell) and then
//! data rows: a column, its description, its years and, on some sheets, a sensitivity
//! flag, each column found by its header alias. A row without a column or a label is a
//! section heading or a note, retained in a separate section map (with a warning when it states years or
//! sensitivity). A year cell holds years and ranges (`1990-2024`, `2003-2008\n2013`);
//! each range is one occurrence of the column. A sheet with no year column, or a blank
//! year cell, leaves the bounds unstated. A blank-year row on a sheet with years that
//! repeats a column of a sheet without years (`Individ`'s year-independent block) is
//! left to that sheet.
//!
//! A sheet with a source register column (`Grundregister`) gives one `derived_from`
//! link per (sheet, column, register name) its cells state, from the column's variable
//! to the register of that name.
//!
//! Table handles are the sheet names and source keys the folded column name.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use calamine::{Data, Range, Reader, Xlsx, open_workbook};
use reg_frame::{Frame, Locator, Occurrence, Record, Register, Table};
use serde::Deserialize;

use crate::workbook::{cell, text};
use crate::{
    ReadError, ReadOutput, Section, SectionKind, config, derived_from, error, name_key, nonblank,
};

mod styles;

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
    /// The register a column is derived from; not every sheet has the column.
    source_register: Vec<String>,
}

impl Config {
    /// # Errors
    /// An unreadable or invalid config file.
    pub fn load(path: &Path) -> Result<Self, ReadError> {
        config(path)
    }
}

/// A data row: its column-name cell's address, column, label, year cell, sensitivity
/// flag and source register cell.
struct Entry {
    section: usize,
    at: String,
    column: String,
    label: String,
    years: Option<String>,
    sensitive: Option<bool>,
    source: Option<String>,
}

/// A sheet: its title cell and that cell's address, whether it has a year column, and
/// its data rows.
struct Sheet {
    name: String,
    title: Option<String>,
    title_at: String,
    years: bool,
    entries: Vec<Entry>,
    sections: Vec<Section>,
}

/// Read the workbook at `rel` as register `register` of `provider`.
///
/// # Errors
/// An unreadable workbook, a sheet with no header row, or a sheet `lisa.toml` names
/// as stating sensitivity that has no sensitivity column.
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
    let styles = styles::Styles::load(&root.join(rel)).map_err(|e| error(rel, e))?;
    let mut out = ReadOutput::default();
    out.files.push(rel.to_owned());
    let mut sheets = Vec::new();
    for name in workbook.sheet_names() {
        let range = workbook
            .worksheet_range(&name)
            .map_err(|e| error(rel, format!("sheet {name:?}: {e}")))?;
        sheets.push(sheet(
            rel,
            name,
            &range,
            &styles,
            config,
            &mut out.warnings,
        )?);
    }
    // A sheet with years lists some year-independent columns again with a blank year
    // cell; the sheet without years is where they belong.
    let timeless: BTreeSet<String> = sheets
        .iter()
        .filter(|s| !s.years)
        .flat_map(|s| s.entries.iter().map(|e| name_key(&e.column)))
        .collect();
    let mut repeated: BTreeMap<String, usize> = BTreeMap::new();
    // (sheet, source key, source register text) → its first row.
    let mut derived: BTreeMap<(String, String, String), Locator> = BTreeMap::new();
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
    for mut sheet in sheets {
        let at = |at: &str| Locator {
            input: rel.to_owned(),
            at: at.to_owned(),
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
            locator: at(&sheet.title_at),
        }));
        for entry in sheet.entries {
            if sheet.years && entry.years.is_none() && timeless.contains(&name_key(&entry.column)) {
                *repeated.entry(sheet.name.clone()).or_default() += 1;
                continue;
            }
            sheet.sections[entry.section].rows.push(at(&entry.at));
            // A cell may name several registers, one per line or separated by `,` or `;`.
            for text in entry.source.iter().flat_map(|s| s.split([',', ';', '\n'])) {
                if let Some(text) = nonblank(text) {
                    derived
                        .entry((sheet.name.clone(), name_key(&entry.column), text))
                        .or_insert_with(|| at(&entry.at));
                }
            }
            let periods = bounds(entry.years.as_deref()).unwrap_or_else(|| {
                out.warnings.push(format!(
                    "{}: years {:?} are not years or ranges; bounds left unstated",
                    entry.at,
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
                    format: None,
                    valueset: None,
                    population_valuesets: Vec::new(),
                    coded: None,
                    code_system: None,
                    sensitive: entry.sensitive,
                    identifier: None,
                    locator: at(&entry.at),
                    also: Vec::new(),
                    conflicts: Vec::new(),
                }));
            }
        }
        out.sections.extend(sheet.sections);
    }
    records.extend(
        derived
            .into_iter()
            .map(|((_, key, text), locator)| derived_from(provider, register, key, text, locator)),
    );
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
#[allow(clippy::too_many_lines)]
fn sheet(
    rel: &str,
    name: String,
    range: &Range<Data>,
    styles: &styles::Styles,
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
    let (label, years, sensitive, source) = (
        col(&config.headers.label),
        col(&config.headers.years),
        col(&config.headers.sensitive),
        col(&config.headers.source_register),
    );
    if sensitive.is_none() && alias(Some(name.clone()), &config.sensitive_sheets) {
        return Err(error(rel, format!("sheet {name:?}: no sensitivity column")));
    }
    let at_column = letter(first_col + column);
    let mut entries = Vec::new();
    let mut sections: Vec<Section> = Vec::new();
    let mut current: Option<usize> = None;
    let mut previous_data: Option<(usize, bool)> = None;
    let mut direct_rows = 0;
    let body_size = rows
        .iter()
        .enumerate()
        .skip(header + 1)
        .find(|(_, r)| cell(r, Some(column)).is_some() && cell(r, label).is_some())
        .map(|(i, _)| styles.cell(&name, i + offset, first_col + column))
        .transpose()
        .map_err(|e| error(rel, e))?
        .map(|s| s.size);
    for (i, row) in rows.iter().enumerate().skip(header + 1) {
        let at = i + offset;
        let column_text = cell(row, Some(column));
        let label_text = cell(row, label);
        let locator = || Locator {
            input: rel.to_owned(),
            at: format!("{name}!{at_column}{at}"),
        };
        let (Some(column_text), Some(label_text)) = (column_text.clone(), label_text) else {
            // Only a lone text cell in the column-name column is a candidate.
            // Footnotes in the description column are outside the section layout.
            if let Some(text) =
                column_text.filter(|_| row.iter().filter(|c| text(c).is_some()).count() == 1)
            {
                let style = styles
                    .cell(&name, at, first_col + column)
                    .map_err(|e| error(rel, e))?;
                let next_undated = rows
                    .iter()
                    .skip(i + 1)
                    .find(|r| cell(r, Some(column)).is_some())
                    .is_some_and(|r| cell(r, label).is_some() && cell(r, years).is_none());
                // A smaller line closing an adjacent larger heading's open border
                // band is a continuation note, not another section.
                let previous = if i > header + 1
                    && sections
                        .last()
                        .is_some_and(|s| s.locator.at == format!("{name}!{at_column}{}", at - 1))
                {
                    Some(
                        styles
                            .cell(&name, at - 1, first_col + column)
                            .map_err(|e| error(rel, e))?,
                    )
                } else {
                    None
                };
                let continuation = previous.is_some_and(|previous| {
                    sections.last().is_some_and(|s| {
                        s.kind == SectionKind::Heading
                            && previous.size > style.size
                            && !previous.bottom
                            && style.bottom
                    })
                });
                let note = continuation
                    || (style.italic
                        && !style.bold
                        && body_size.is_some_and(|size| style.size.total_cmp(&size).is_eq())
                        && !next_undated);
                if !note {
                    if let Some(index) = current.filter(|_| direct_rows == 0) {
                        sections[index].kind = SectionKind::Parent;
                    }
                    current = Some(sections.len());
                    direct_rows = 0;
                }
                sections.push(Section {
                    text: Some(text),
                    kind: if note {
                        SectionKind::Note
                    } else {
                        SectionKind::Heading
                    },
                    locator: locator(),
                    rows: Vec::new(),
                });
            }
            if cell(row, years).is_some() || cell(row, sensitive).is_some() {
                warnings.push(format!("{name}!{at_column}{at}: a row with years or sensitivity but no column or label is skipped"));
            }
            continue;
        };
        let dated = cell(row, years).is_some();
        // The source switches back to annual rows without a heading cell. A new
        // explicit heading already starts its own scope and needs no reset.
        let annual = previous_data
            .is_some_and(|(section, undated)| undated && dated && current == Some(section));
        if current.is_none() || annual {
            current = Some(sections.len());
            direct_rows = 0;
            sections.push(Section {
                text: None,
                kind: SectionKind::Unheaded,
                locator: locator(),
                rows: Vec::new(),
            });
        }
        let section = current.expect("data has a section");
        direct_rows += 1;
        previous_data = Some((section, !dated));
        let stated = cell(row, sensitive).map(|s| s.to_lowercase());
        let flag = stated
            .as_ref()
            .and_then(|s| config.sensitive.get(s).copied());
        if let (Some(s), None) = (&stated, flag) {
            warnings.push(format!(
                "{name}!{at_column}{at}: sensitivity {s:?} is not in lisa.toml"
            ));
        }
        entries.push(Entry {
            section,
            at: format!("{name}!{at_column}{at}"),
            column: column_text,
            label: label_text,
            years: cell(row, years),
            sensitive: flag,
            source: cell(row, source),
        });
    }
    Ok(Sheet {
        title: rows.first().and_then(|r| cell(r, Some(0))),
        title_at: format!("{name}!{}{offset}", letter(first_col)),
        years: years.is_some(),
        name,
        entries,
        sections,
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
