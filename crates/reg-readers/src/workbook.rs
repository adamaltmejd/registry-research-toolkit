//! The Socialstyrelsen metadata template: one XLSX workbook per register, read through
//! calamine. Sheet roles, header aliases, the type table and the frequency table come
//! from `curation/readers/sos-workbook.toml`.
//!
//! - The dataset sheet states the register's name and description.
//! - The tables sheet states tables (deldatamängder) with their bounds and frequency.
//! - The variables sheet states one occurrence per row: the column, its table, label,
//!   definition, type, linkage flag, bounds and code list.
//! - Code-list sheets state codes; a variable binds one when its link cell names the
//!   sheet exactly, else an inline enumeration in its value-set cell is read
//!   conservatively, else the link text is kept as code-system evidence.
//!
//! Bounds are year (`2001`), month (`200101`), day (`20010101`, an ISO date or a date
//! cell) or a start with a trailing dash (`1999-`). A blank bound stays unstated
//! (resolve reads it as the table's bound); one the reader cannot interpret is left
//! unstated and reported.
//!
//! Known gaps: calamine exposes no number formats, so a zero-padded numeric code
//! (`001` stored as 1) loses its padding, undetected; a code list stated per period
//! is read as the union of its periods, keeping the first label of a repeated code,
//! and reported.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use calamine::{Data, Range, Reader, Xlsx, open_workbook};
use reg_frame::{Frame, Item, Locator, Occurrence, Record, Register, Table, ValueSet};
use serde::Deserialize;

use crate::{ReadError, ReadOutput, config, error, fold, month_end, name_key, nonblank};

/// `curation/readers/sos-workbook.toml`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    sheets: Sheets,
    dataset: Dataset,
    tables: TableHeaders,
    variables: VariableHeaders,
    codelists: CodelistHeaders,
    links: Links,
    types: BTreeMap<String, String>,
    cadence: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Sheets {
    variables: Vec<Vec<String>>,
    tables: Vec<Vec<String>>,
    dataset: Vec<Vec<String>>,
    codelist_prefix: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    value_header: String,
    name: String,
    description: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TableHeaders {
    table: Vec<String>,
    name: Vec<String>,
    description: Vec<String>,
    from: Vec<String>,
    to: Vec<String>,
    cadence: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct VariableHeaders {
    table: Vec<String>,
    column: Vec<String>,
    label: Vec<String>,
    definition: Vec<String>,
    codes: Vec<String>,
    link: Vec<String>,
    dtype: Vec<String>,
    identifier: Vec<String>,
    from: Vec<String>,
    to: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CodelistHeaders {
    code: Vec<String>,
    label: Vec<String>,
    period: Vec<String>,
    column: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Links {
    prefixes: Vec<String>,
}

impl Config {
    /// # Errors
    /// An unreadable or invalid config file.
    pub fn load(path: &Path) -> Result<Self, ReadError> {
        config(path)
    }
}

/// One code row of a code-list sheet: the variable it names, if any, and the code.
struct CodeRow {
    column: Option<String>,
    item: Item,
}

struct Context<'a> {
    rel: &'a str,
    provider: &'a str,
    register: &'a str,
    config: &'a Config,
    out: ReadOutput,
    records: Vec<Record>,
}

/// Read the workbook `rel` (relative to the inputs root) as `register` of `provider`.
///
/// # Errors
/// An unreadable workbook, a missing variables sheet, or a variables sheet with no
/// column header.
pub fn read(
    root: &Path,
    rel: &str,
    provider: &str,
    register: &str,
    config: &Config,
) -> Result<ReadOutput, ReadError> {
    let mut workbook: Xlsx<_> = open_workbook(root.join(rel))
        .map_err(|e: calamine::XlsxError| error(rel, e.to_string()))?;
    let names = workbook.sheet_names();
    let mut taken: BTreeSet<String> = BTreeSet::new();
    let mut role = |alternatives: &[Vec<String>]| {
        let found = alternatives.iter().find_map(|tokens| {
            names.iter().find(|name| {
                let norm = normalize_sheet(name);
                !taken.contains(name.as_str()) && tokens.iter().all(|t| norm.contains(t.as_str()))
            })
        });
        found
            .inspect(|name| {
                taken.insert((*name).clone());
            })
            .cloned()
    };
    let variables = role(&config.sheets.variables)
        .ok_or_else(|| error(rel, "no variables sheet (see sos-workbook.toml)"))?;
    let tables = role(&config.sheets.tables);
    let dataset = role(&config.sheets.dataset);
    let mut sheet = |name: &str| {
        workbook
            .worksheet_range(name)
            .map_err(|e| error(rel, format!("sheet {name:?}: {e}")))
    };

    let mut cx = Context {
        rel,
        provider,
        register,
        config,
        out: ReadOutput::default(),
        records: Vec::new(),
    };
    cx.out.files.push(rel.to_owned());

    let (name, description) = match &dataset {
        Some(name) => cx.dataset(&sheet(name)?),
        None => (None, None),
    };
    cx.records.push(Record::Register(Register {
        provider: provider.to_owned(),
        register: register.to_owned(),
        name,
        description,
        from: None,
        to: None,
        locator: Locator {
            input: rel.to_owned(),
            at: dataset.map(|d| format!("{d}!")).unwrap_or_default(),
        },
    }));

    if let Some(name) = &tables {
        cx.tables(name, &sheet(name)?);
    }

    let mut codelists: BTreeMap<String, Vec<CodeRow>> = BTreeMap::new();
    for name in names.iter().filter(|n| {
        n.to_lowercase()
            .starts_with(&config.sheets.codelist_prefix.to_lowercase())
    }) {
        let rows = cx.codelist(name, &sheet(name)?);
        codelists.insert(name.clone(), rows);
    }
    let sheets: BTreeSet<String> = names.iter().cloned().collect();
    cx.variables(&variables, &sheet(&variables)?, &codelists, &sheets)?;

    let mut out = cx.out;
    out.frame = Frame::new(cx.records);
    Ok(out)
}

impl Context<'_> {
    fn warn(&mut self, message: &str) {
        self.out.warnings.push(format!("{}: {message}", self.rel));
    }

    fn locator(&self, sheet: &str, row: u32) -> Locator {
        Locator {
            input: self.rel.to_owned(),
            at: format!("{sheet}!{row}"),
        }
    }

    /// The register's name and description from the dataset sheet's attribute rows.
    fn dataset(&self, range: &Range<Data>) -> (Option<String>, Option<String>) {
        let rows: Vec<&[Data]> = range.rows().collect();
        let Some((header, body)) = rows.split_first() else {
            return (None, None);
        };
        let header = lower_cells(header);
        let Some(value) = column(
            &header,
            std::slice::from_ref(&self.config.dataset.value_header),
        ) else {
            return (None, None);
        };
        let attribute = |key: &str| {
            body.iter().find_map(|row| {
                let attr = row.first().and_then(text)?;
                (attr.to_lowercase() == key).then(|| row.get(value).and_then(text))?
            })
        };
        (
            attribute(&self.config.dataset.name),
            attribute(&self.config.dataset.description),
        )
    }

    /// Table records from the tables sheet.
    fn tables(&mut self, sheet: &str, range: &Range<Data>) {
        let h = &self.config.tables;
        let Some((header, body)) = split_header(range) else {
            return;
        };
        let col = |aliases: &[String]| column(&header, aliases);
        let (table, name, description) = (col(&h.table), col(&h.name), col(&h.description));
        let (from, to, cadence) = (col(&h.from), col(&h.to), col(&h.cadence));
        let Some(table) = table else {
            self.warn(&format!("sheet {sheet:?} has no table-name header"));
            return;
        };
        for (row_number, row) in body {
            let Some(handle) = cell(row, Some(table)) else {
                continue;
            };
            let at = self.locator(sheet, row_number);
            let (lo, hi) = self.bounds(row, from, to, &at);
            let cadence = self.cadence(cell(row, cadence), &at);
            self.records.push(Record::Table(Table {
                provider: self.provider.to_owned(),
                register: self.register.to_owned(),
                table: handle,
                name: cell(row, name),
                description: cell(row, description),
                from: lo,
                to: hi,
                cadence,
                locator: at,
            }));
        }
    }

    fn cadence(&mut self, stated: Option<String>, at: &Locator) -> Option<String> {
        let stated = stated?;
        match self.config.cadence.get(&stated.to_lowercase()) {
            Some(c) if c.is_empty() => None,
            Some(c) => Some(c.clone()),
            None => {
                self.warn(&format!("{}: frequency {stated:?} has no cadence", at.at));
                None
            }
        }
    }

    /// The code rows of one code-list sheet, after its header row.
    fn codelist(&mut self, sheet: &str, range: &Range<Data>) -> Vec<CodeRow> {
        let h = &self.config.codelists;
        let mut rows = range.rows();
        let header = rows.by_ref().find_map(|row| {
            let header = lower_cells(row);
            column(&header, &h.code).map(|code| (header, code))
        });
        let Some((header, code)) = header else {
            self.warn(&format!("sheet {sheet:?} has no code column; not read"));
            return Vec::new();
        };
        let prefixed = |prefixes: &[String]| {
            header
                .iter()
                .position(|c| prefixes.iter().any(|p| c.starts_with(p.as_str())))
        };
        let (label, period, variable) = (
            prefixed(&h.label),
            prefixed(&h.period),
            column(&header, &h.column),
        );
        let mut periods = BTreeSet::new();
        let mut out = Vec::new();
        for row in rows {
            if let Some(p) = cell(row, period) {
                periods.insert(p);
            }
            let Some(code) = cell(row, Some(code)) else {
                continue;
            };
            out.push(CodeRow {
                column: cell(row, variable).map(|c| fold(&c)),
                item: Item {
                    code,
                    label: cell(row, label),
                    parent: None,
                },
            });
        }
        if periods.len() > 1 {
            self.warn(&format!(
                "sheet {sheet:?} states codes for {} periods; read as their union",
                periods.len()
            ));
        }
        self.records.push(Record::ValueSet(ValueSet::new(
            out.iter().map(|r| r.item.clone()).collect(),
            Some(sheet.to_owned()),
            Locator {
                input: self.rel.to_owned(),
                at: format!("{sheet}!"),
            },
        )));
        out
    }

    fn variables(
        &mut self,
        sheet: &str,
        range: &Range<Data>,
        codelists: &BTreeMap<String, Vec<CodeRow>>,
        sheets: &BTreeSet<String>,
    ) -> Result<(), ReadError> {
        let h = &self.config.variables;
        let Some((header, body)) = split_header(range) else {
            return Err(error(self.rel, format!("sheet {sheet:?} is empty")));
        };
        let col = |aliases: &[String]| column(&header, aliases);
        let Some(name_col) = col(&h.column) else {
            return Err(error(
                self.rel,
                format!("sheet {sheet:?} has no column-name header"),
            ));
        };
        let (table_col, label, definition) = (col(&h.table), col(&h.label), col(&h.definition));
        let (codes, link, dtype) = (col(&h.codes), col(&h.link), col(&h.dtype));
        let (identifier, from, to) = (col(&h.identifier), col(&h.from), col(&h.to));
        for (row_number, row) in body {
            let Some(column_name) = cell(row, Some(name_col)) else {
                continue;
            };
            let at = self.locator(sheet, row_number);
            let table = cell(row, table_col).unwrap_or_default();
            let (lo, hi) = self.bounds(row, from, to, &at);
            let link_text = cell(row, link);
            let pointer = link_text.as_deref().and_then(|t| self.pointer(t, sheets));
            let (valueset, code_system) = if let Some(target) = pointer {
                let rows = codelists.get(&target).map_or(&[][..], Vec::as_slice);
                (self.bind(&target, rows, &column_name), None)
            } else {
                let inline = cell(row, codes).and_then(|t| inline_codes(&t));
                let hash = inline.map(|items| {
                    let set = ValueSet::new(items, None, at.clone());
                    let hash = set.hash.clone();
                    self.records.push(Record::ValueSet(set));
                    hash
                });
                (hash, link_text)
            };
            self.records.push(Record::Occurrence(Occurrence {
                provider: self.provider.to_owned(),
                register: self.register.to_owned(),
                table,
                column: column_name.clone(),
                source_key: name_key(&column_name),
                from: lo,
                to: hi,
                label: cell(row, label),
                definition: cell(row, definition),
                unit: None,
                dtype: cell(row, dtype).map(|t| {
                    self.config
                        .types
                        .get(&t.to_lowercase())
                        .cloned()
                        .unwrap_or(t)
                }),
                length: None,
                valueset,
                code_system,
                sensitive: None,
                identifier: identifier.map(|_| cell(row, identifier).is_some()),
                locator: at,
            }));
        }
        Ok(())
    }

    /// The sheet a link cell names exactly: a configured prefix, the sheet name, then
    /// nothing, `!` or a cell reference.
    fn pointer(&self, link: &str, sheets: &BTreeSet<String>) -> Option<String> {
        let stripped = self
            .config
            .links
            .prefixes
            .iter()
            .find_map(|p| link.strip_prefix(p.as_str()))
            .unwrap_or(link)
            .trim();
        let (name, reference) = stripped.split_once('!').unwrap_or((stripped, ""));
        let is_reference = reference.is_empty()
            || reference
                .trim_start_matches(|c: char| c.is_ascii_alphabetic())
                .chars()
                .all(|c| c.is_ascii_digit())
                && reference.starts_with(|c: char| c.is_ascii_alphabetic());
        let name = name.trim().to_lowercase();
        sheets
            .iter()
            .find(|s| is_reference && s.to_lowercase() == name)
            .cloned()
    }

    /// The value set a variable binds from a code-list sheet: the rows naming the
    /// variable when any do, else the whole sheet.
    fn bind(&mut self, sheet: &str, rows: &[CodeRow], column: &str) -> Option<String> {
        if rows.is_empty() {
            return None;
        }
        let key = fold(column);
        let named: Vec<Item> = rows
            .iter()
            .filter(|r| r.column.as_deref() == Some(key.as_str()))
            .map(|r| r.item.clone())
            .collect();
        let items = if named.is_empty() {
            rows.iter().map(|r| r.item.clone()).collect()
        } else {
            named
        };
        let set = ValueSet::new(
            items,
            Some(sheet.to_owned()),
            Locator {
                input: self.rel.to_owned(),
                at: format!("{sheet}!"),
            },
        );
        let hash = set.hash.clone();
        self.records.push(Record::ValueSet(set));
        Some(hash)
    }

    /// A row's stated bounds as inclusive ISO days.
    fn bounds(
        &mut self,
        row: &[Data],
        from: Option<usize>,
        to: Option<usize>,
        at: &Locator,
    ) -> (Option<String>, Option<String>) {
        let mut parse = |index: Option<usize>, end: bool| {
            let text = cell(row, index)?;
            // `1999-`: a start with no end of its own.
            let stated = text.strip_suffix('-').unwrap_or(&text);
            let bound = bound(stated, end);
            if bound.is_none() {
                self.warn(&format!(
                    "{}: bound {text:?} is not a date; unstated",
                    at.at
                ));
            }
            bound
        };
        let (lo, hi) = (parse(from, false), parse(to, true));
        (lo, hi)
    }
}

/// A year, month or day bound as an ISO day: the first day of the stated period for a
/// start, the last for an end.
fn bound(text: &str, end: bool) -> Option<String> {
    if reg_core::iso_day_number(text).is_some() {
        return Some(text.to_owned());
    }
    if !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let num = |s: &str| s.parse::<u32>().ok();
    match text.len() {
        4 => Some(if end {
            format!("{text}-12-31")
        } else {
            format!("{text}-01-01")
        }),
        6 => {
            let (y, m) = (num(&text[..4])?, num(&text[4..])?);
            if end {
                month_end(y, m)
            } else {
                month_end(y, m).map(|_| format!("{y:04}-{m:02}-01"))
            }
        }
        8 => {
            let iso = format!("{}-{}-{}", &text[..4], &text[4..6], &text[6..]);
            reg_core::iso_day_number(&iso).map(|_| iso)
        }
        _ => None,
    }
}

/// A value-set cell read as an enumeration: two or more segments separated by `;` or
/// a newline, either all `code=label` or all bare codes, with distinct codes that
/// look like codes and labels that hide no further assignment. Anything else is
/// prose and yields `None`: a wrong reject leaves the variable without codes, a wrong
/// accept would state false ones.
fn inline_codes(text: &str) -> Option<Vec<Item>> {
    let segments: Vec<&str> = text
        .split([';', '\n'])
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if segments.len() < 2 {
        return None;
    }
    let assigned = segments.iter().filter(|s| s.contains('=')).count();
    let mut items = Vec::new();
    for segment in &segments {
        let (code, label) = if assigned == segments.len() {
            let (code, label) = segment.split_once('=')?;
            let label = nonblank(label)?;
            if label.contains('=') {
                return None;
            }
            (code.trim(), Some(label))
        } else if assigned == 0 {
            (*segment, None)
        } else {
            return None;
        };
        let is_code = !code.is_empty()
            && code
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "ÅÄÖåäö._-".contains(c))
            && !code
                .as_bytes()
                .windows(3)
                .any(|w| w[0].is_ascii_digit() && w[1] == b'-' && w[2].is_ascii_digit());
        if !is_code || items.iter().any(|i: &Item| i.code == code) {
            return None;
        }
        items.push(Item {
            code: code.to_owned(),
            label,
            parent: None,
        });
    }
    Some(items)
}

/// A sheet name lowercased with spaces, `_`, `-` and parentheses removed.
fn normalize_sheet(name: &str) -> String {
    name.to_lowercase()
        .chars()
        .filter(|c| !c.is_whitespace() && !"_-()".contains(*c))
        .collect()
}

/// A data row with its 1-based sheet row number.
type Row<'a> = (u32, &'a [Data]);

/// The header row (first row of the range, lowercased) and the data rows with their
/// 1-based sheet row numbers.
fn split_header(range: &Range<Data>) -> Option<(Vec<String>, Vec<Row<'_>>)> {
    let start = range.start().map_or(0, |(r, _)| r);
    let mut rows = range.rows();
    let header = lower_cells(rows.next()?);
    let body = (start + 2..).zip(rows).collect();
    Some((header, body))
}

fn lower_cells(row: &[Data]) -> Vec<String> {
    row.iter()
        .map(|c| text(c).unwrap_or_default().to_lowercase())
        .collect()
}

/// The index of the first header equal to one of `aliases`.
fn column(header: &[String], aliases: &[String]) -> Option<usize> {
    header.iter().position(|h| aliases.iter().any(|a| a == h))
}

fn cell(row: &[Data], index: Option<usize>) -> Option<String> {
    row.get(index?).and_then(text)
}

/// A cell as trimmed text: integral numbers without a decimal point, date cells as
/// ISO days (with the time when it is not midnight), errors and blanks as `None`.
fn text(cell: &Data) -> Option<String> {
    match cell {
        Data::String(s) | Data::DateTimeIso(s) => nonblank(s),
        Data::Int(i) => Some(i.to_string()),
        #[allow(clippy::cast_possible_truncation)]
        Data::Float(f) if f.fract() == 0.0 && f.abs() < 1e15 => Some((*f as i64).to_string()),
        Data::Float(f) => Some(f.to_string()),
        Data::Bool(b) => Some(b.to_string()),
        Data::DateTime(d) => {
            let (year, month, day, hour, min, sec, milli) = d.to_ymd_hms_milli();
            let date = format!("{year:04}-{month:02}-{day:02}");
            Some(if (hour, min, sec, milli) == (0, 0, 0, 0) {
                date
            } else {
                format!("{date}T{hour:02}:{min:02}:{sec:02}")
            })
        }
        Data::Error(_) | Data::DurationIso(_) | Data::Empty => None,
    }
}
