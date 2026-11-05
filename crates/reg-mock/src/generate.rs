//! `generate`: [`Spec`] + seed + size → one table of cells per file. Values come from
//! the unit (keys, sex, age, county, dates of birth and death, the row's year) or are
//! drawn from the column's documented codes, else from its type. Writing is
//! [`crate::write`].

use reg_core::manifest::Encoding;
use reg_core::project::{ColumnType, ExtractFormat};

use crate::Refusal;
use crate::population::{Demography, Exit, Population, day_in, year_of};
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
    pub fn of(column: &SpecColumn, format: ExtractFormat) -> Result<Self, String> {
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
                ColumnType::Text => Self::Text,
                // Refused by compile; an edited spec may still hold them.
                ColumnType::Geometry | ColumnType::Uuid | ColumnType::Binary => {
                    return Err(format!("{:?}", column.dtype).to_ascii_lowercase());
                }
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
                let (precision, scale) = (digits(storage.precision, 18), digits(storage.scale, 0));
                // SQL Server's bounds, which Parquet's decimal128 shares.
                if !(1..=38).contains(&precision) || scale > precision {
                    return Err(format!("{sql}({precision}, {scale})"));
                }
                Self::Decimal(precision, scale)
            }
            // The runner writes money only as CSV text (its "Types per format").
            "money" if format == ExtractFormat::Csv => Self::Decimal(19, 4),
            "smallmoney" if format == ExtractFormat::Csv => Self::Decimal(10, 4),
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
/// The years a spec may hold.
const YEARS: (i32, i32) = (1900, 2100);

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
/// A refusal from [`crate::compile::check_spec`]; `size_too_large` past [`MAX_PERSONS`]
/// unless `allow_large`; `code_invalid` when a code does not fit its column's type;
/// `storage_unsupported` for a type or storage the runner does not write in the format.
pub fn generate(
    spec: &Spec,
    seed: u64,
    persons: u64,
    allow_large: bool,
) -> Result<Vec<(String, Table)>, Refusal> {
    // A spec is a file anyone can edit: check what compile checked.
    if let Some(refusal) = crate::compile::check_spec(spec).into_iter().next() {
        return Err(refusal);
    }
    if persons > MAX_PERSONS && !allow_large {
        return Err(Refusal::new(
            "size_too_large",
            "--persons",
            format!("more than {MAX_PERSONS} persons needs --allow-large"),
        ));
    }
    // The years the demography can be stretched to; outside them a spec is a typo.
    let years = spec.files.iter().flat_map(|f| {
        f.years
            .iter()
            .map(move |y| (y, f.name.clone()))
            .chain(f.columns.iter().flat_map(move |c| {
                c.years
                    .iter()
                    .map(move |y| (y, format!("{}/{}", f.name, c.name)))
            }))
    });
    for (year, at) in years {
        if !(YEARS.0..=YEARS.1).contains(year) {
            return Err(Refusal::new(
                "year_invalid",
                at,
                format!("a year outside {}–{}", YEARS.0, YEARS.1),
            ));
        }
    }
    let (first, last) = window(spec);
    let population = Population::simulate(&Demography::embedded(), seed, persons, first, last);
    // Every municipality code the spec holds: a unit's municipality is drawn from them
    // once, so files whose value sets differ still agree on it.
    let mut municipalities: Vec<&str> = spec
        .files
        .iter()
        .flat_map(|f| &f.columns)
        .filter(|c| c.attribute == Some(Attribute::Municipality))
        .flat_map(|c| &c.codes)
        .filter(|c| !c.sentinel)
        .map(|c| c.code.as_str())
        .collect();
    municipalities.sort_unstable();
    municipalities.dedup();
    let mut tables = Vec::new();
    for file in &spec.files {
        let kinds = file
            .columns
            .iter()
            .map(|c| {
                Kind::of(c, spec.format).map_err(|sql| {
                    Refusal::new(
                        "storage_unsupported",
                        format!("{}/{}", file.name, c.name),
                        format!("the runner writes no {sql} column"),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let rows = rows(spec, file, &kinds, &population, &municipalities, seed, last)?;
        tables.push((file.name.clone(), Table { kinds, rows }));
    }
    Ok(tables)
}

/// One unit's row in one year.
struct Unit_<'a> {
    seed: u64,
    municipalities: &'a [&'a str],
    year: i32,
    person: Option<&'a crate::population::Person>,
    firm: Option<&'a crate::population::Firm>,
}

fn rows(
    spec: &Spec,
    file: &SpecFile,
    kinds: &[Kind],
    population: &Population,
    municipalities: &[&str],
    seed: u64,
    last: i32,
) -> Result<Vec<Vec<Cell>>, Refusal> {
    let mut rng = Rng::new(seed, &format!("file/{}", file.name));
    let mut columns: Vec<Rng> = file
        .columns
        .iter()
        .map(|c| Rng::new(seed, &format!("column/{}/{}", file.name, c.name)))
        .collect();
    let pools: Vec<Pool<'_>> = file.columns.iter().map(Pool::of).collect();
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
                    seed,
                    municipalities,
                    year,
                    person: Some(p),
                    firm: Some(&population.firms[firm_of(p.key, population.firms.len())]),
                })
                .collect(),
            Unit::Firm => population
                .firms
                .iter()
                .map(|f| Unit_ {
                    seed,
                    municipalities,
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
                for (((column, kind), pool), crng) in
                    file.columns.iter().zip(kinds).zip(&pools).zip(&mut columns)
                {
                    let rate = column.null_rate.unwrap_or(spec.null_rate);
                    let value = cell(file, column, *kind, pool, unit, rate, crng)?;
                    // A supplied code or mapped value within the held storage: an
                    // integer in its range, a text no wider.
                    if let (Cell::Int(n), Some((min, max))) = (&value, int_range(column))
                        && !(min..=max).contains(n)
                    {
                        return Err(not_numeric(format!("{}/{}", file.name, column.name)));
                    }
                    let width = column.storage.as_ref().and_then(|s| s.length);
                    if let (Cell::Text(text), Some(width)) = (&value, width)
                        && width > 0
                        && text.chars().count() > usize::try_from(width).unwrap_or(usize::MAX)
                    {
                        return Err(not_numeric(format!("{}/{}", file.name, column.name)));
                    }
                    row.push(value);
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

/// A column's codes, split once per file: the codes drawn as values and the
/// sentinels drawn as missing.
struct Pool<'a> {
    codes: Vec<&'a str>,
    sentinels: Vec<&'a str>,
}

impl<'a> Pool<'a> {
    fn of(column: &'a SpecColumn) -> Self {
        let all_sentinel = column.codes.iter().all(|c| c.sentinel);
        // A value set of sentinels only documents no value: draw from it, invent none.
        let codes = column
            .codes
            .iter()
            .filter(|c| !c.sentinel || all_sentinel)
            .map(|c| c.code.as_str())
            .collect();
        let sentinels = column
            .codes
            .iter()
            .filter(|c| c.sentinel)
            .map(|c| c.code.as_str())
            .collect();
        Self { codes, sentinels }
    }
}

fn cell(
    file: &SpecFile,
    column: &SpecColumn,
    kind: Kind,
    pool: &Pool<'_>,
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
        if pool.sentinels.is_empty() {
            return Ok(Cell::Null);
        }
        let code = pool.sentinels[pick(rng, pool.sentinels.len())];
        return from_text(code, kind).ok_or_else(|| not_numeric(at()));
    }
    if let Some(attribute) = column.attribute {
        return attribute_cell(column, kind, attribute, unit, rng).ok_or_else(|| not_numeric(at()));
    }
    if !pool.codes.is_empty() {
        let code = pool.codes[pick(rng, pool.codes.len())];
        return from_text(code, kind).ok_or_else(|| not_numeric(at()));
    }
    if column.identifier {
        // A key of its own kind, not joinable to persons or firms.
        let key = 700_000_001 + i64::try_from(rng.below(10_000_000)).expect("fits");
        return from_int(key, kind).ok_or_else(|| not_numeric(at()));
    }
    // The held storage's width is the delivery's; the catalog's otherwise.
    let length = column
        .storage
        .as_ref()
        .and_then(|s| s.length)
        .or(column.length);
    // A narrow integer storage keeps the delivery's range, though it is written as int32.
    #[allow(clippy::cast_precision_loss)]
    let ceiling = int_range(column).map_or(2_000_000_000.0, |(_, max)| max as f64);
    Ok(drawn(kind, length, ceiling, unit.year, rng))
}

/// The held integer storage's range, which the runner writes as int32 or int64.
fn int_range(column: &SpecColumn) -> Option<(i64, i64)> {
    let sql = column
        .storage
        .as_ref()?
        .dtype
        .as_deref()?
        .to_ascii_lowercase();
    Some(match sql.as_str() {
        "tinyint" => (0, 255),
        "smallint" => (i64::from(i16::MIN), i64::from(i16::MAX)),
        "int" => (i64::from(i32::MIN), i64::from(i32::MAX)),
        _ => return None,
    })
}

fn not_numeric(at: String) -> Refusal {
    Refusal::new(
        "code_invalid",
        at,
        "a code or mapped value does not fit the column's type (a number, a calendar date, a boolean, \
         the storage's precision or width): fix the codes or the map in the rules, or the column's type",
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
        Attribute::PersonKey => from_int(person?.key, kind)?,
        Attribute::FirmKey => from_int(unit.firm?.key, kind)?,
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
            // A code of the person's county (its first two digits), else any code, from
            // every municipality code in the spec, so each file agrees; a column may then
            // hold a code its own value set lacks. simplify: filtered per row; key the
            // pool by county if 1M-person runs slow.
            let codes = unit.municipalities;
            let local: Vec<&str> = codes
                .iter()
                .copied()
                .filter(|c| c.len() == 4 && c.starts_with(county))
                .collect();
            let pool = if local.is_empty() {
                codes.to_vec()
            } else {
                local
            };
            if pool.is_empty() {
                // simplify: without a value set, the county seat (`<county>80` in every
                // county); embed SCB's municipality list if analyses need the spread.
                return from_text(&mapped(&format!("{county}80")), kind);
            }
            // One municipality per unit, the same in every file: drawn from the unit's
            // own stream, not the column's. simplify: a stream per row, cache it per
            // unit if --allow-large runs past the 1M cap get slow.
            let key = person.map_or_else(|| unit.firm.map(|f| f.key), |p| Some(p.key))?;
            let mut stable = Rng::new(unit.seed, &format!("municipality/{key}"));
            from_text(&mapped(pool[pick(&mut stable, pool.len())]), kind)?
        }
        Attribute::DeathDate => match person?.exit {
            Some((day, Exit::Death)) if year_of(day) <= unit.year => date(day)?,
            _ => Cell::Null,
        },
        Attribute::Year => year_cell(column.encoding, kind, unit.year, &mapped, rng)?,
    })
}

/// The row's year in the row filter's encoding: a whole year, or a day or month in it.
fn year_cell(
    encoding: Option<Encoding>,
    kind: Kind,
    year: i32,
    mapped: &dyn Fn(&str) -> String,
    rng: &mut Rng,
) -> Option<Cell> {
    let day = day_in(year, rng);
    let iso = reg_core::issue::iso_day(day);
    match (encoding, kind) {
        (_, Kind::Date) => Some(Cell::Date(day)),
        (_, Kind::DateTime) => Some(Cell::DateTime(day * 86_400_000)),
        (Some(Encoding::Yyyymm), _) => from_text(&mapped(&iso[..7].replace('-', "")), kind),
        (Some(Encoding::Yyyymmdd | Encoding::Date), _) => {
            from_text(&mapped(&iso.replace('-', "")), kind)
        }
        _ => from_text(&mapped(&year.to_string()), kind),
    }
}

/// An integer in the column's kind, or `None` when the kind cannot hold it: a decimal
/// storage too narrow, a boolean other than 0 or 1, a date.
fn from_int(n: i64, kind: Kind) -> Option<Cell> {
    Some(match kind {
        Kind::Text => Cell::Text(n.to_string()),
        #[allow(clippy::cast_precision_loss)]
        Kind::Float64 | Kind::Float32 => Cell::Real(n as f64),
        Kind::Decimal(precision, scale) => Cell::Decimal(
            i128::from(n)
                .checked_mul(10i128.checked_pow(u32::from(scale))?)
                .filter(|v| v.unsigned_abs() < 10u128.pow(u32::from(precision)))?,
        ),
        Kind::Bool => match n {
            0 => Cell::Bool(false),
            1 => Cell::Bool(true),
            _ => return None,
        },
        Kind::Int32 | Kind::Int64 => Cell::Int(n),
        Kind::Date | Kind::DateTime => return None,
    })
}

/// A code in the column's kind, or `None` when a numeric kind's code is not a number.
fn from_text(code: &str, kind: Kind) -> Option<Cell> {
    Some(match kind {
        Kind::Text => Cell::Text(code.to_owned()),
        Kind::Int32 | Kind::Int64 => Cell::Int(code.trim().parse().ok()?),
        Kind::Float64 | Kind::Float32 => Cell::Real(code.trim().parse().ok()?),
        Kind::Decimal(precision, scale) => Cell::Decimal(
            scaled(code.trim(), scale)
                .filter(|v| v.unsigned_abs() < 10u128.pow(u32::from(precision)))?,
        ),
        Kind::Bool => match code.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "j" | "ja" | "y" | "yes" => Cell::Bool(true),
            "0" | "false" | "n" | "nej" | "no" => Cell::Bool(false),
            _ => return None,
        },
        Kind::Date | Kind::DateTime => temporal(code.trim(), kind)?,
    })
}

/// A date code (`YYYY-MM-DD` or `YYYYMMDD`), with a time (` HH:MM:SS` or `T…`) in a
/// datetime column; `None` for anything else, such as a day the calendar lacks.
fn temporal(code: &str, kind: Kind) -> Option<Cell> {
    let (date, time) = code
        .split_once([' ', 'T'])
        .map_or((code, None), |(d, t)| (d, Some(t)));
    let iso = match date.len() {
        8 if date.bytes().all(|b| b.is_ascii_digit()) => {
            format!("{}-{}-{}", &date[..4], &date[4..6], &date[6..])
        }
        _ => date.to_owned(),
    };
    let day = reg_core::iso_day_number(&iso)?;
    if kind == Kind::Date {
        return time.is_none().then_some(Cell::Date(day));
    }
    let seconds = match time {
        None => 0,
        Some(time) => {
            let mut parts = time.split(':').map(|p| p.parse::<i64>().ok());
            let (h, m, s) = (
                parts.next()??,
                parts.next()??,
                parts.next().unwrap_or(Some(0))?,
            );
            if parts.next().is_some()
                || !(0..24).contains(&h)
                || !(0..60).contains(&m)
                || !(0..60).contains(&s)
            {
                return None;
            }
            h * 3600 + m * 60 + s
        }
    };
    Some(Cell::DateTime(day * 86_400_000 + seconds * 1000))
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
fn drawn(kind: Kind, length: Option<i64>, ceiling: f64, year: i32, rng: &mut Rng) -> Cell {
    // A skewed amount (median about 150), as most uncoded register numbers are.
    let mut amount = || (5.0 + 1.5 * rng.normal()).exp();
    match kind {
        #[allow(clippy::cast_possible_truncation)]
        Kind::Int32 | Kind::Int64 => Cell::Int(amount().round().min(ceiling) as i64),
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
