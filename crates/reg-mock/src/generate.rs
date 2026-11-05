//! `generate`: [`Spec`] + seed + size → one table of cells per file. Values come from
//! the unit (keys, sex, age, county, dates of birth and death, the row's year) or are
//! drawn from the column's documented codes, else from its type. Writing is
//! [`crate::write`].

use reg_core::manifest::Encoding;
use reg_core::project::ColumnType;

use crate::Refusal;
use crate::population::{Demography, Exit, Population, day_in, days_from_civil, year_of};
use crate::rng::Rng;
use crate::spec::{Attribute, Spec, SpecColumn, SpecFile, Unit};

/// The default population size, and the size no run passes without `--allow-large`
/// (maintainer, 2026-11-05).
pub const DEFAULT_PERSONS: u64 = 10_000;
pub const MAX_PERSONS: u64 = 1_000_000;

/// One value. Days and milliseconds count from 1970-01-01 (UTC).
#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    Null,
    Int(i64),
    Real(f64),
    /// A decimal as an integer at its column's scale, so codes keep every digit.
    Decimal(i128),
    Text(String),
    Date(i64),
    DateTime(i64),
    Bool(bool),
}

/// How a column is written: the held SQL storage when known, else its type
/// (crates/reg-mock/DESIGN.md, "Write types").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Int32,
    Int64,
    /// Precision and scale.
    Decimal(u8, u8),
    Float64,
    Float32,
    Bool,
    Date,
    DateTime,
    Text,
}

impl Kind {
    /// # Errors
    ///
    /// The SQL type, when the runner writes no such storage.
    pub fn of(column: &SpecColumn) -> Result<Self, String> {
        let Some(storage) = &column.storage else {
            return Ok(match column.dtype {
                // An integer identifier may pass 2^31; a code or amount stays a long.
                ColumnType::Integer if column.identifier => Self::Int64,
                // A pseudonymised key (`lopnr`) is delivered as a number.
                ColumnType::Identifier => Self::Int64,
                ColumnType::Integer => Self::Int32,
                ColumnType::Decimal => Self::Float64,
                ColumnType::Boolean => Self::Bool,
                ColumnType::Date => Self::Date,
                ColumnType::Datetime => Self::DateTime,
                _ => Self::Text,
            });
        };
        let sql = storage
            .dtype
            .as_deref()
            .unwrap_or_default()
            .to_ascii_lowercase();
        let digits =
            |n: Option<i64>, default: u8| n.and_then(|n| u8::try_from(n).ok()).unwrap_or(default);
        Ok(match sql.as_str() {
            "tinyint" | "smallint" | "int" => Self::Int32,
            "bigint" => Self::Int64,
            "decimal" | "numeric" => {
                Self::Decimal(digits(storage.precision, 18), digits(storage.scale, 0))
            }
            "float" => Self::Float64,
            "real" => Self::Float32,
            "bit" => Self::Bool,
            "date" => Self::Date,
            "datetime" | "datetime2" | "smalldatetime" => Self::DateTime,
            "char" | "nchar" | "varchar" | "nvarchar" | "text" | "ntext" => Self::Text,
            _ => return Err(sql),
        })
    }
}

/// A generated file: its columns' kinds and its rows.
pub struct Table {
    pub kinds: Vec<Kind>,
    pub rows: Vec<Vec<Cell>>,
}

/// The years the population must cover: every file's and column's years, or the
/// demography's last year when every file is timeless.
fn window(spec: &Spec) -> (i32, i32) {
    let years = spec.files.iter().flat_map(|f| {
        f.years
            .iter()
            .chain(f.columns.iter().flat_map(|c| &c.years))
    });
    let (min, max) = years.fold((i32::MAX, i32::MIN), |(lo, hi), &y| (lo.min(y), hi.max(y)));
    if min > max { (2024, 2024) } else { (min, max) }
}

/// Generate every file of the spec.
///
/// # Errors
///
/// `size_too_large` past [`MAX_PERSONS`] unless `allow_large`; `code_not_numeric` when a
/// numeric column's code is not a number; `storage_unsupported` for a storage type the
/// runner does not write.
pub fn generate(
    spec: &Spec,
    seed: u64,
    persons: u64,
    allow_large: bool,
) -> Result<Vec<(String, Table)>, Refusal> {
    if persons > MAX_PERSONS && !allow_large {
        return Err(Refusal::new(
            "size_too_large",
            "--persons",
            format!("more than {MAX_PERSONS} persons needs --allow-large"),
        ));
    }
    let (first, last) = window(spec);
    let population = Population::simulate(&Demography::embedded(), seed, persons, first, last);
    let mut tables = Vec::new();
    for file in &spec.files {
        let kinds = file
            .columns
            .iter()
            .map(|c| {
                Kind::of(c).map_err(|sql| {
                    Refusal::new(
                        "storage_unsupported",
                        format!("{}/{}", file.name, c.name),
                        format!("the runner writes no {sql} column"),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let rows = rows(spec, file, &kinds, &population, seed, last)?;
        tables.push((file.name.clone(), Table { kinds, rows }));
    }
    Ok(tables)
}

/// One unit's row in one year.
struct Unit_<'a> {
    year: i32,
    person: Option<&'a crate::population::Person>,
    firm: Option<&'a crate::population::Firm>,
}

fn rows(
    spec: &Spec,
    file: &SpecFile,
    kinds: &[Kind],
    population: &Population,
    seed: u64,
    last: i32,
) -> Result<Vec<Vec<Cell>>, Refusal> {
    let mut rng = Rng::new(seed, &format!("file/{}", file.name));
    let mut columns: Vec<Rng> = file
        .columns
        .iter()
        .map(|c| Rng::new(seed, &format!("column/{}/{}", file.name, c.name)))
        .collect();
    let years = if file.years.is_empty() {
        vec![last]
    } else {
        file.years.clone()
    };
    let mut rows = Vec::new();
    for &year in &years {
        let units: Vec<Unit_<'_>> = match file.unit {
            Unit::Person => population
                .persons
                .iter()
                .filter(|p| eligible(file, p, year))
                .map(|p| Unit_ {
                    year,
                    person: Some(p),
                    firm: Some(&population.firms[firm_of(p.key, population.firms.len())]),
                })
                .collect(),
            Unit::Firm => population
                .firms
                .iter()
                .map(|f| Unit_ {
                    year,
                    person: None,
                    firm: Some(f),
                })
                .collect(),
        };
        for unit in &units {
            // At most one row a year below a mean of 1 (a register holding some of the
            // eligible); one and more above it (events such as diagnoses).
            let n = if file.rows_per_unit <= 1.0 {
                u64::from(rng.chance(file.rows_per_unit))
            } else {
                1 + rng.poisson(file.rows_per_unit - 1.0)
            };
            for _ in 0..n {
                let mut row = Vec::with_capacity(kinds.len());
                for ((column, kind), crng) in file.columns.iter().zip(kinds).zip(&mut columns) {
                    let rate = column.null_rate.unwrap_or(spec.null_rate);
                    row.push(cell(file, column, *kind, unit, rate, crng)?);
                }
                rows.push(row);
            }
        }
    }
    Ok(rows)
}

fn eligible(file: &SpecFile, person: &crate::population::Person, year: i32) -> bool {
    let age = year - person.birth_year();
    let present = if file.population.include_departed {
        person.entered <= year && age >= 0
    } else {
        person.resident(year)
    };
    present
        && file
            .population
            .age_min
            .is_none_or(|min| age >= i32::from(min))
        && file
            .population
            .age_max
            .is_none_or(|max| age <= i32::from(max))
}

/// A person's employer: a fixed firm per person.
fn firm_of(person_key: i64, firms: usize) -> usize {
    // simplify: one employer for life; a job-spell model replaces it if analyses need
    // employer changes.
    usize::try_from(person_key.unsigned_abs() % firms as u64).expect("below firms")
}

fn cell(
    file: &SpecFile,
    column: &SpecColumn,
    kind: Kind,
    unit: &Unit_<'_>,
    null_rate: f64,
    rng: &mut Rng,
) -> Result<Cell, Refusal> {
    let at = || format!("{}/{}", file.name, column.name);
    if !column.years.is_empty() && !column.years.contains(&unit.year) {
        return Ok(Cell::Null);
    }
    let keyed = column.identifier
        || matches!(
            column.attribute,
            Some(Attribute::PersonKey | Attribute::FirmKey | Attribute::Year)
        );
    if !keyed && rng.chance(null_rate) {
        let sentinels: Vec<&str> = column
            .codes
            .iter()
            .filter(|c| c.sentinel)
            .map(|c| c.code.as_str())
            .collect();
        if sentinels.is_empty() {
            return Ok(Cell::Null);
        }
        let code = sentinels[pick(rng, sentinels.len())];
        return from_text(code, kind).ok_or_else(|| not_numeric(at()));
    }
    if let Some(attribute) = column.attribute {
        return attribute_cell(column, kind, attribute, unit, rng).ok_or_else(|| not_numeric(at()));
    }
    // A value set of sentinels only documents no value: draw from it, invent none.
    let codes: Vec<&str> = column
        .codes
        .iter()
        .filter(|c| !c.sentinel || column.codes.iter().all(|c| c.sentinel))
        .map(|c| c.code.as_str())
        .collect();
    if !codes.is_empty() {
        let code = codes[pick(rng, codes.len())];
        return from_text(code, kind).ok_or_else(|| not_numeric(at()));
    }
    if column.identifier {
        // A key of its own kind, not joinable to persons or firms.
        let key = 700_000_001 + i64::try_from(rng.below(10_000_000)).expect("fits");
        return Ok(from_int(key, kind));
    }
    // The held storage's width is the delivery's; the catalog's otherwise.
    let length = column
        .storage
        .as_ref()
        .and_then(|s| s.length)
        .or(column.length);
    Ok(drawn(kind, length, unit.year, rng))
}

fn not_numeric(at: String) -> Refusal {
    Refusal::new(
        "code_not_numeric",
        at,
        "a numeric column's code or mapped value is not a number: fix the codes or the map \
         in the rules, or the column's type",
    )
}

fn pick(rng: &mut Rng, n: usize) -> usize {
    usize::try_from(rng.below(n as u64)).expect("below n")
}

/// The unit's value for an attribute, in the column's kind and code map.
fn attribute_cell(
    column: &SpecColumn,
    kind: Kind,
    attribute: Attribute,
    unit: &Unit_<'_>,
    rng: &mut Rng,
) -> Option<Cell> {
    let person = unit.person;
    let mapped = |value: &str| {
        column
            .map
            .get(value)
            .map_or(value, String::as_str)
            .to_owned()
    };
    let date = |day: i64| match kind {
        Kind::Date => Some(Cell::Date(day)),
        Kind::DateTime => Some(Cell::DateTime(day * 86_400_000)),
        _ => from_text(&reg_core::issue::iso_day(day).replace('-', ""), kind),
    };
    Some(match attribute {
        Attribute::PersonKey => from_int(person?.key, kind),
        Attribute::FirmKey => from_int(unit.firm?.key, kind),
        Attribute::Sex => {
            let sex = if person?.sex == 1 { "male" } else { "female" };
            let code = column
                .map
                .get(sex)
                .cloned()
                .unwrap_or_else(|| person.map_or(1, |p| p.sex).to_string());
            from_text(&code, kind)?
        }
        Attribute::BirthYear => from_text(&mapped(&person?.birth_year().to_string()), kind)?,
        Attribute::BirthDate => date(person?.birth)?,
        Attribute::Age => from_text(
            &mapped(&(unit.year - person?.birth_year()).to_string()),
            kind,
        )?,
        Attribute::County => {
            let county = person.map_or_else(
                || unit.firm.map(|f| f.county.as_str()),
                |p| Some(p.county.as_str()),
            )?;
            from_text(&mapped(county), kind)?
        }
        Attribute::Municipality => {
            let county = person.map_or_else(
                || unit.firm.map(|f| f.county.as_str()),
                |p| Some(p.county.as_str()),
            )?;
            // A code of the person's county (its first two digits), else any code.
            let codes: Vec<&str> = column
                .codes
                .iter()
                .filter(|c| !c.sentinel)
                .map(|c| c.code.as_str())
                .collect();
            let local: Vec<&str> = codes
                .iter()
                .copied()
                .filter(|c| c.len() == 4 && c.starts_with(county))
                .collect();
            let pool = if local.is_empty() { codes } else { local };
            if pool.is_empty() {
                // simplify: without a value set, the county seat (`<county>80` in every
                // county); embed SCB's municipality list if analyses need the spread.
                return from_text(&mapped(&format!("{county}80")), kind);
            }
            from_text(&mapped(pool[pick(rng, pool.len())]), kind)?
        }
        Attribute::DeathDate => match person?.exit {
            Some((day, Exit::Death)) if year_of(day) <= unit.year => date(day)?,
            _ => Cell::Null,
        },
        Attribute::Year => year_cell(column.encoding, kind, unit.year, rng)?,
    })
}

/// The row's year in the row filter's encoding: a whole year, or a day or month in it.
fn year_cell(encoding: Option<Encoding>, kind: Kind, year: i32, rng: &mut Rng) -> Option<Cell> {
    let day = day_in(year, rng);
    let iso = reg_core::issue::iso_day(day);
    match (encoding, kind) {
        (_, Kind::Date) => Some(Cell::Date(day)),
        (_, Kind::DateTime) => Some(Cell::DateTime(day * 86_400_000)),
        (Some(Encoding::Yyyymm), _) => from_text(&iso[..7].replace('-', ""), kind),
        (Some(Encoding::Yyyymmdd | Encoding::Date), _) => from_text(&iso.replace('-', ""), kind),
        _ => from_text(&year.to_string(), kind),
    }
}

fn from_int(n: i64, kind: Kind) -> Cell {
    match kind {
        Kind::Text => Cell::Text(n.to_string()),
        #[allow(clippy::cast_precision_loss)]
        Kind::Float64 | Kind::Float32 => Cell::Real(n as f64),
        Kind::Decimal(_, scale) => Cell::Decimal(i128::from(n) * 10i128.pow(u32::from(scale))),
        Kind::Bool => Cell::Bool(n != 0),
        _ => Cell::Int(n),
    }
}

/// A code in the column's kind, or `None` when a numeric kind's code is not a number.
fn from_text(code: &str, kind: Kind) -> Option<Cell> {
    Some(match kind {
        Kind::Text => Cell::Text(code.to_owned()),
        Kind::Int32 | Kind::Int64 => Cell::Int(code.trim().parse().ok()?),
        Kind::Float64 | Kind::Float32 => Cell::Real(code.trim().parse().ok()?),
        Kind::Decimal(_, scale) => Cell::Decimal(scaled(code.trim(), scale)?),
        Kind::Bool => Cell::Bool(matches!(code.trim(), "1" | "true" | "J" | "j")),
        Kind::Date | Kind::DateTime => {
            let digits: String = code.chars().filter(char::is_ascii_digit).collect();
            let (y, m, d) = (digits.get(..4)?, digits.get(4..6)?, digits.get(6..8)?);
            let day = days_from_civil(y.parse().ok()?, m.parse().ok()?, d.parse().ok()?);
            if kind == Kind::Date {
                Cell::Date(day)
            } else {
                Cell::DateTime(day * 86_400_000)
            }
        }
    })
}

/// A decimal code as an integer at `scale`, or `None` when it is not a decimal or has
/// more fractional digits than the scale.
fn scaled(code: &str, scale: u8) -> Option<i128> {
    let (negative, digits) = code.strip_prefix('-').map_or((false, code), |d| (true, d));
    let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    let all_digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    if whole.is_empty() && fraction.is_empty()
        || !all_digits(whole)
        || !all_digits(fraction)
        || fraction.len() > usize::from(scale)
    {
        return None;
    }
    let padded = format!("{whole}{fraction:0<width$}", width = usize::from(scale));
    let value: i128 = padded.parse().ok()?;
    Some(if negative { -value } else { value })
}

/// A value from the type alone: an amount, a short text, a day or time in the year.
fn drawn(kind: Kind, length: Option<i64>, year: i32, rng: &mut Rng) -> Cell {
    // A skewed amount (median about 150), as most uncoded register numbers are.
    let mut amount = || (5.0 + 1.5 * rng.normal()).exp();
    match kind {
        #[allow(clippy::cast_possible_truncation)]
        Kind::Int32 | Kind::Int64 => Cell::Int(amount().round().min(2_000_000_000.0) as i64),
        Kind::Decimal(precision, scale) => {
            // The largest value is one scaled unit below 10^(p - s): 0.999 for (3, 3).
            let max = 10i128.pow(u32::from(precision)) - 1;
            #[allow(clippy::cast_possible_truncation)]
            let units = (amount() * 10f64.powi(i32::from(scale))).round() as i128;
            Cell::Decimal(units.min(max))
        }
        Kind::Float64 | Kind::Float32 => Cell::Real(amount()),
        Kind::Bool => Cell::Bool(rng.chance(0.5)),
        Kind::Date => Cell::Date(day_in(year, rng)),
        Kind::DateTime => {
            let ms = i64::try_from(rng.below(86_400)).expect("fits") * 1000;
            Cell::DateTime(day_in(year, rng) * 86_400_000 + ms)
        }
        Kind::Text => {
            let len = length
                .and_then(|l| usize::try_from(l).ok())
                .unwrap_or(8)
                .clamp(1, 8);
            let text = (0..len)
                .map(|_| char::from(b'A' + u8::try_from(rng.below(26)).expect("below 26")))
                .collect();
            Cell::Text(text)
        }
    }
}
