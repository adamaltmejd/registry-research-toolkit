//! What a run writes, estimated before it writes anything: rows and bytes per file from
//! the spec and the simulated population (`generate`), and the sources whose rows per
//! person across all their years are too many (`compile`). The bytes are an estimate
//! from each column's kind, not the writers' exact output.

use std::fmt::Write as _;

use reg_core::project::ExtractFormat;
use serde::Serialize;

use crate::generate::Kind;
use crate::population::Population;
use crate::spec::{Attribute, Spec, SpecColumn, SpecFile, Unit};

/// A run estimated above this many bytes needs `--allow-large` (COS ruling, 2026-11-24;
/// a 10 000-person run of a long-COVID spec wrote 915 MB, #2651).
pub const MAX_BYTES: u64 = 2 << 30;

/// Rows per person across a source's years above which `compile` warns.
pub const MAX_ROWS_PER_PERSON: f64 = 50.0;

/// The expected size of one file.
#[derive(Debug, Clone, PartialEq)]
pub struct FileSize {
    pub name: String,
    pub rows: u64,
    pub bytes: u64,
}

/// The expected size of a run, from [`estimate`].
#[derive(Debug, Clone, PartialEq)]
pub struct Estimate {
    pub files: Vec<FileSize>,
}

impl Estimate {
    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        self.files.iter().map(|f| f.bytes).sum()
    }

    #[must_use]
    pub fn total_rows(&self) -> u64 {
        self.files.iter().map(|f| f.rows).sum()
    }

    /// The estimate as the lines `generate` prints: the total, then the largest files.
    #[must_use]
    pub fn summary(&self) -> String {
        let mut largest: Vec<&FileSize> = self.files.iter().collect();
        largest.sort_by(|a, b| b.bytes.cmp(&a.bytes).then_with(|| a.name.cmp(&b.name)));
        let mut text = format!(
            "estimated size: {} rows, {} in {} files",
            self.total_rows(),
            human(self.total_bytes()),
            self.files.len()
        );
        for f in largest.iter().take(5) {
            let _ = write!(text, "\n  {}: {} rows, {}", f.name, f.rows, human(f.bytes));
        }
        text
    }
}

/// The years a file holds a row in: its own, or one for a timeless file.
fn file_years(file: &SpecFile, last: i32) -> Vec<i32> {
    if file.years.is_empty() {
        vec![last]
    } else {
        file.years.clone()
    }
}

/// Expected rows and bytes per file for `persons` simulated into `population`.
/// `last` is the window's last year, where a timeless file is drawn.
#[must_use]
pub fn estimate(spec: &Spec, population: &Population, last: i32) -> Estimate {
    let files = spec
        .files
        .iter()
        .map(|file| {
            let units: u64 = file_years(file, last)
                .iter()
                .map(|&year| match file.unit {
                    Unit::Person => population
                        .persons
                        .iter()
                        .filter(|p| crate::generate::eligible(file, p, year))
                        .count() as u64,
                    Unit::Firm => population.firms.len() as u64,
                })
                .sum();
            #[allow(clippy::cast_precision_loss)]
            let units = units as f64;
            // The mean is the rate itself at or below 1, else 1 plus a Poisson of rate - 1.
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let rows = (units * file.rows_per_unit).round() as u64;
            let row_bytes: u64 = file
                .columns
                .iter()
                .map(|c| column_bytes(c, spec.format))
                .sum();
            FileSize {
                name: file.name.clone(),
                rows,
                bytes: rows * row_bytes,
            }
        })
        .collect();
    Estimate { files }
}

/// A column's bytes in one row of the format.
fn column_bytes(column: &SpecColumn, format: ExtractFormat) -> u64 {
    let Ok(kind) = Kind::of(column, format) else {
        return 0;
    };
    let text = || {
        let longest = column.codes.iter().map(|c| c.code.len()).max();
        let width = column
            .storage
            .as_ref()
            .and_then(|s| s.length)
            .or(column.length)
            .and_then(|l| u64::try_from(l).ok())
            .map_or(8, |l| l.clamp(1, 8));
        longest.map_or(width, |l| l as u64).max(1)
    };
    match format {
        // Digits and a delimiter.
        ExtractFormat::Csv => match kind {
            Kind::Int32 | Kind::Float32 => 7,
            Kind::Int64 | Kind::Decimal(..) | Kind::Float64 => 10,
            Kind::Bool => 2,
            Kind::Date => 11,
            Kind::DateTime => 20,
            Kind::Text => text() + 1,
        },
        // Dictionary and Snappy compression: measured at about half the fixed widths.
        ExtractFormat::Parquet => match kind {
            Kind::Int32 | Kind::Float32 | Kind::Date => 2,
            Kind::Int64 | Kind::Float64 | Kind::DateTime => 3,
            Kind::Decimal(..) => 5,
            Kind::Bool => 1,
            Kind::Text => text() + 1,
        },
        // Fixed width.
        ExtractFormat::Dta => match kind {
            Kind::Int32 | Kind::Float32 | Kind::Date => 4,
            Kind::Int64 | Kind::Float64 | Kind::DateTime => 8,
            Kind::Decimal(precision, _) => {
                if precision > 15 {
                    u64::from(precision) + 2
                } else {
                    8
                }
            }
            Kind::Bool => 1,
            Kind::Text => text(),
        },
    }
}

/// `1.4 GiB`, `915 MiB`, `12 KiB`.
#[must_use]
pub fn human(bytes: u64) -> String {
    const UNITS: [(&str, u64); 3] = [("GiB", 1 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)];
    for (unit, size) in UNITS {
        if bytes >= size {
            #[allow(clippy::cast_precision_loss)]
            return format!("{:.1} {unit}", bytes as f64 / size as f64);
        }
    }
    format!("{bytes} B")
}

/// A finding that does not stop `compile`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Warning {
    pub code: &'static str,
    pub at: String,
    pub message: String,
}

/// Person files whose expected rows per person across all their years pass
/// [`MAX_ROWS_PER_PERSON`].
#[must_use]
pub fn row_warnings(spec: &Spec) -> Vec<Warning> {
    let mut warnings = Vec::new();
    for file in spec.files.iter().filter(|f| f.unit == Unit::Person) {
        // A person is in the register for the years its age bounds admit, at most.
        let admitted = u32::from(file.population.age_max.unwrap_or(100))
            .saturating_sub(u32::from(file.population.age_min.unwrap_or(0)))
            + 1;
        let years = file.years.len().max(1).min(admitted as usize);
        #[allow(clippy::cast_precision_loss)]
        let per_person = file.rows_per_unit * years as f64;
        if per_person <= MAX_ROWS_PER_PERSON {
            continue;
        }
        let has_year = file
            .columns
            .iter()
            .any(|c| c.attribute == Some(Attribute::Year));
        let cause = if has_year {
            format!("the file holds {years} years")
        } else {
            // The year is written into no column, so nothing asks for these years: the
            // file repeats its yearly rate over every year its sources are held.
            format!(
                "the file has no year column, so its rate of {} rows per person and year repeats over {years} years",
                file.rows_per_unit
            )
        };
        warnings.push(Warning {
            code: "rows_per_person",
            at: file.name.clone(),
            message: format!(
                "expected {per_person:.0} rows per person ({cause}): order fewer years, or lower the \
                 source's rows_per_unit in the rules; generate refuses a run estimated above {} \
                 without --allow-large",
                human(MAX_BYTES)
            ),
        });
    }
    warnings
}
