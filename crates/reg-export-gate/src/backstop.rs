//! Backstop: no value, text or number, has a personnummer or samordningsnummer
//! shape. A backstop only: a pattern scan cannot establish safety, and
//! organisationsnummer share the shape and also match.
//!
//! The shape is the R gate's pattern
//! `(?<![0-9])(?:19|20)?YYMMDD[-+]?NNNN(?![0-9])`, with MM 01–12 and DD 01–31
//! or 61–91 (day + 60), scanned by hand over runs of ASCII digits since the
//! `regex` crate has no lookaround. Because no digit may sit on either side, a
//! match is a whole digit run of 10, or 12 starting 19 or 20, or a run of 6, or
//! 8 starting 19 or 20, then one `-` or `+`, then a run of exactly 4.

use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, Float64Type, Int32Type, Int64Type};
use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType;

use crate::structure::Candidate;
use crate::{Code, ColumnType, Finding, Refusal};

fn two(d: &[u8]) -> u8 {
    (d[0] - b'0') * 10 + (d[1] - b'0')
}

/// YYMMDD: any year, a month 01–12, a day 01–31 or 61–91.
fn date(d: &[u8]) -> bool {
    let (month, day) = (two(&d[2..4]), two(&d[4..6]));
    (1..=12).contains(&month) && ((1..=31).contains(&day) || (61..=91).contains(&day))
}

fn century(d: &[u8]) -> bool {
    d.starts_with(b"19") || d.starts_with(b"20")
}

/// Whether a whole digit run is a 10- or 12-digit identifier without a separator.
fn whole(run: &[u8]) -> bool {
    match run.len() {
        10 => date(run),
        12 => century(run) && date(&run[2..]),
        _ => false,
    }
}

/// The YYMMDD of a run that can precede a separator, or `None`.
fn head(run: &[u8]) -> Option<&[u8]> {
    match run.len() {
        6 => Some(run),
        8 if century(run) => Some(&run[2..]),
        _ => None,
    }
}

/// Whether `text` holds a personnummer or samordningsnummer shape.
#[must_use]
pub fn identifier_shape(text: &str) -> bool {
    let bytes = text.as_bytes();
    let mut runs = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i].is_ascii_digit() {
            let start = i;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            runs.push((start, i));
        } else {
            i += 1;
        }
    }
    runs.iter().enumerate().any(|(k, &(start, end))| {
        let run = &bytes[start..end];
        whole(run)
            || head(run).is_some_and(|head| {
                date(head)
                    && matches!(bytes.get(end), Some(b'-' | b'+'))
                    && runs
                        .get(k + 1)
                        .is_some_and(|&(s, e)| s == end + 1 && e - s == 4)
            })
    })
}

/// A column's cells as the scan reads them: strings as written, integers in
/// full, dates as their day number, doubles by the whole-number part of a
/// finite value (an identifier stored as a number is whole; the digits of a
/// fraction would match by chance). Booleans are not scanned.
fn texts(array: &ArrayRef, ty: ColumnType) -> Vec<String> {
    match (ty, array.data_type()) {
        (ColumnType::String, _) => array
            .as_string::<i32>()
            .iter()
            .flatten()
            .map(str::to_owned)
            .collect(),
        (ColumnType::Integer, DataType::Int32) => array
            .as_primitive::<Int32Type>()
            .iter()
            .flatten()
            .map(|v| v.to_string())
            .collect(),
        (ColumnType::Integer, _) => array
            .as_primitive::<Int64Type>()
            .iter()
            .flatten()
            .map(|v| v.to_string())
            .collect(),
        (ColumnType::Date, _) => array
            .as_primitive::<Date32Type>()
            .iter()
            .flatten()
            .map(|v| v.to_string())
            .collect(),
        (ColumnType::Double, _) => array
            .as_primitive::<Float64Type>()
            .iter()
            .flatten()
            .filter(|v| v.is_finite())
            .map(|v| format!("{:.0}", v.trunc()))
            .collect(),
        (ColumnType::Boolean, _) => Vec::new(),
    }
}

pub(crate) fn check(candidates: &[Candidate]) -> Result<(), Refusal> {
    let mut findings = Vec::new();
    for cand in candidates {
        for (col, array) in cand.spec.columns.iter().zip(&cand.columns) {
            if texts(array, col.ty).iter().any(|t| identifier_shape(t)) {
                findings.push(Finding::column(
                    "identifier_pattern",
                    &cand.spec.name,
                    &col.name,
                ));
            }
        }
    }
    if findings.is_empty() {
        Ok(())
    } else {
        Err(Refusal {
            code: Code::Backstop,
            findings,
        })
    }
}
