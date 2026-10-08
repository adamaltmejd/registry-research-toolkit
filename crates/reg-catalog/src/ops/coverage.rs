//! `coverage`: the calendar years a register or a variable is delivered in (today's
//! `get availability`), over the representations `states` emits for the whole
//! history ([`states::emitted`]), held-clipped in holdings scope. A year a dated
//! representation touches counts; an open-ended one counts only its opening year, and
//! a year-independent one none.

use std::collections::{BTreeMap, BTreeSet};

use rusqlite::Connection;
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

use super::refs::{self, Target, VAR_ID, fqid, invalid_kind};
use super::{Params, Server, states};
use crate::held::{self, Narrow};
use crate::{Code, Error, Scope};

/// A register's or a variable's coverage (the CLI's `get availability` data); the
/// ref's kind is in `fqid`.
#[derive(Serialize, ToSchema)]
#[serde(untagged)]
pub enum Coverage {
    Register(RegisterCoverage),
    Variable(VariableCoverage),
}

#[derive(Serialize, ToSchema)]
pub struct RegisterCoverage {
    fqid: String,
    register_name: String,
    #[serde(flatten)]
    span: Span,
    variant_count: usize,
    /// The register variants with a year in scope, in build order.
    variants: Vec<VariantYears>,
}

#[derive(Serialize, ToSchema)]
pub struct VariantYears {
    /// The variant's slug; null for the unslugged one.
    variant: Option<String>,
    variant_name: String,
    years: Vec<u16>,
}

#[derive(Serialize, ToSchema)]
pub struct VariableCoverage {
    fqid: String,
    variable_name: Option<String>,
    #[serde(flatten)]
    span: Span,
    register_count: usize,
    /// The variable's register: one entry.
    registers: Vec<RegisterYears>,
}

#[derive(Serialize, ToSchema)]
pub struct RegisterYears {
    register: Option<String>,
    register_name: String,
    /// SCB's numeric variable id; null for other providers.
    var_id: Option<i64>,
    #[serde(flatten)]
    span: Span,
    /// Per year, the columns delivered in it, sorted.
    aliases_by_year: BTreeMap<String, BTreeSet<String>>,
}

/// The years covered, their bounds and the years missing between the bounds.
#[derive(Clone, Serialize, ToSchema)]
pub struct Span {
    min_year: u16,
    max_year: u16,
    years: Vec<u16>,
    gaps: Vec<u16>,
}

impl Span {
    /// None for no years.
    fn of(years: &BTreeSet<u16>) -> Option<Self> {
        let (&min_year, &max_year) = (years.first()?, years.last()?);
        Some(Self {
            min_year,
            max_year,
            years: years.iter().copied().collect(),
            gaps: (min_year..=max_year)
                .filter(|y| !years.contains(y))
                .collect(),
        })
    }
}

/// The calendar years a representation's bounds span (today's `_years_in_range`):
/// none when year-independent, only the opening year when open-ended.
fn years(from: Option<&str>, to: Option<&str>) -> impl Iterator<Item = u16> {
    let year = |date: &str| date.get(..4).and_then(|y| y.parse::<u16>().ok());
    let span = match (from.and_then(year), to.and_then(year)) {
        (Some(lo), Some(9999)) => Some((lo, lo)),
        (Some(lo), Some(hi)) => Some((lo, hi)),
        _ => None,
    };
    span.into_iter().flat_map(|(lo, hi)| lo..=hi)
}

pub fn coverage(server: &Server, scope: Scope, params: &Params) -> Result<Value, Error> {
    let conn = server.catalog.connect()?;
    let value = params.get("ref").copied().unwrap_or_default();
    let coverage = match refs::resolve(&conn, scope, Some(value))? {
        Target::Register { id, provider, slug } => {
            register(&conn, scope, id, format!("{provider}/{slug}"))?.map(Coverage::Register)
        }
        Target::Variable { id } => variable(&conn, scope, id)?.map(Coverage::Variable),
        _ => return Err(invalid_kind(value)),
    };
    // Coverage counts calendar years, so a ref delivered only year-independently has
    // nothing to answer, as `diff` has nothing for a register without both periods.
    let coverage = coverage.ok_or_else(|| {
        Error::new(
            Code::NotFound,
            format!("{value:?} has no dated deliveries in this scope."),
            vec![value.into()],
        )
    })?;
    Ok(serde_json::to_value(coverage).expect("Coverage serializes"))
}

/// A representation's register variant and bounds.
type Bounds = (i64, Option<String>, Option<String>);

fn register(
    conn: &Connection,
    scope: Scope,
    register_id: i64,
    fqid: String,
) -> Result<Option<RegisterCoverage>, Error> {
    let register_name: String = conn.query_row(
        "SELECT name FROM register WHERE register_id = ?",
        [register_id],
        |row| row.get(0),
    )?;
    // The whole-history representations, as `states::emitted` reads them without
    // bounds: in reference every expanded row but `base_fallback`, read for the whole
    // register at once (scb/frida's 7k variables one by one took 1.6 s); in holdings
    // each held variable's, clipped to its held periods.
    let bounds: Vec<Bounds> = match scope {
        Scope::Reference => conn
            .prepare(
                "SELECT e.register_variant_id, e.valid_from, e.valid_to \
                 FROM expanded_state e JOIN variable v USING(variable_id) \
                 WHERE v.register_id = ? AND e.kind != 'base_fallback'",
            )?
            .query_map([register_id], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .collect::<rusqlite::Result<_>>()?,
        Scope::Holdings => {
            let sql = format!(
                "SELECT v.variable_id FROM variable v WHERE v.register_id = ? AND {}",
                held::variable(scope, "v.variable_id", Narrow::default())
            );
            let variables = conn
                .prepare(&sql)?
                .query_map([register_id], |row| row.get(0))?
                .collect::<rusqlite::Result<Vec<i64>>>()?;
            // simplify: one `emitted` read per held variable (SWECOV's scb/lisa
            // 0.56 s); clip the register's rows in one pass if a held register's
            // coverage passes ~1 s.
            let mut bounds = Vec::new();
            for variable_id in variables {
                for e in states::emitted(conn, scope, variable_id, None, None)? {
                    bounds.push((e.register_variant_id, e.valid_from, e.valid_to));
                }
            }
            bounds
        }
    };
    let mut by_variant: BTreeMap<i64, BTreeSet<u16>> = BTreeMap::new();
    for (variant, from, to) in bounds {
        let mut covered = years(from.as_deref(), to.as_deref()).peekable();
        if covered.peek().is_some() {
            by_variant.entry(variant).or_default().extend(covered);
        }
    }
    let all: BTreeSet<u16> = by_variant.values().flatten().copied().collect();
    let Some(span) = Span::of(&all) else {
        return Ok(None);
    };
    let mut stmt =
        conn.prepare("SELECT slug, name FROM register_variant WHERE register_variant_id = ?")?;
    let variants = by_variant
        .into_iter()
        .map(|(id, years)| {
            let (variant, variant_name) =
                stmt.query_row([id], |row| Ok((row.get(0)?, row.get(1)?)))?;
            Ok(VariantYears {
                variant,
                variant_name,
                years: years.into_iter().collect(),
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    Ok(Some(RegisterCoverage {
        fqid,
        register_name,
        span,
        variant_count: variants.len(),
        variants,
    }))
}

fn variable(
    conn: &Connection,
    scope: Scope,
    variable_id: i64,
) -> Result<Option<VariableCoverage>, Error> {
    let mut all = BTreeSet::new();
    let mut aliases_by_year: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for e in states::emitted(conn, scope, variable_id, None, None)? {
        for year in years(e.valid_from.as_deref(), e.valid_to.as_deref()) {
            all.insert(year);
            // A year a column-less state covers is listed without a column.
            let columns = aliases_by_year.entry(year.to_string()).or_default();
            columns.extend(e.delivery_column_name.clone());
        }
    }
    let Some(span) = Span::of(&all) else {
        return Ok(None);
    };
    let sql = format!(
        "SELECT p.slug, r.slug, v.slug, v.name, r.name, {VAR_ID} FROM variable v \
         JOIN register r USING(register_id) JOIN provider p USING(provider_id) \
         WHERE v.variable_id = ?"
    );
    let coverage = conn.query_row(&sql, [variable_id], |row| {
        let [provider, register, slug]: [Option<String>; 3] =
            [row.get(0)?, row.get(1)?, row.get(2)?];
        Ok(VariableCoverage {
            // A resolved ref names a variable with a FQID.
            fqid: fqid(&[provider.clone(), register.clone(), slug]).unwrap_or_default(),
            variable_name: row.get(3)?,
            span: span.clone(),
            register_count: 1,
            registers: vec![RegisterYears {
                register: fqid(&[provider, register]),
                register_name: row.get(4)?,
                var_id: row.get(5)?,
                span,
                aliases_by_year,
            }],
        })
    })?;
    Ok(Some(coverage))
}
