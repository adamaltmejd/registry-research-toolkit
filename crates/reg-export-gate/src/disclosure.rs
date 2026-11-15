//! Disclosure: every non-null count is at least the policy's minimum (null is
//! the only suppression marker), a statistic is null wherever the count it
//! rests on is, and no double holds NaN, whatever its role: a NaN is not null,
//! and its payload bits survive Parquet and the rewrite, so a cell that looks
//! suppressed could carry a value. Null-ness is Arrow's validity, which is
//! exactly what the rewrite writes. A policy may also bound a statistic: the
//! smallest support it may rest on, and the significant digits of a double.
//!
//! Check-only columns hold the true figures behind published ones. A count's
//! `units` is the true number of units behind it, a statistic's truth is the
//! units of its support (or the support itself when that is check-only), and a
//! share's numerator and denominator are true counts. The rules test those
//! truths beside the published figures, whose floors stay. Integer rules
//! compare in `i128`, so no sum or difference of two `int64` values wraps.

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int32Type, Int64Type};
use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType;

use crate::structure::Candidate;
use crate::{Code, Column, ColumnType, Finding, Policy, Refusal, Role};

/// An integer column's cells, widened; the structure check admitted only
/// `int32` and `int64` for an integer column.
fn integers(array: &ArrayRef) -> Vec<Option<i64>> {
    match array.data_type() {
        DataType::Int32 => array
            .as_primitive::<Int32Type>()
            .iter()
            .map(|v| v.map(i64::from))
            .collect(),
        DataType::Int64 => array.as_primitive::<Int64Type>().iter().collect(),
        other => unreachable!("structure admitted {other} as an integer"),
    }
}

/// Whether `v` is its own decimal rendering with `digits` significant digits, read
/// back: the double nearest that decimal. Formatting and parsing are both
/// correctly rounded, so this is the same test as C's `%.*e` and `strtod`, which
/// the R gate uses. A NaN is `nan_value`'s finding, an infinity holds no digits.
fn rounded(v: f64, digits: u32) -> bool {
    if !v.is_finite() {
        return true;
    }
    let precision = usize::try_from(digits - 1).expect("at most 17");
    format!("{v:.precision$e}").parse::<f64>() == Ok(v)
}

/// Whether `n` is below the minimum `min`.
fn below(n: i64, min: u64) -> bool {
    i128::from(n) < i128::from(min)
}

/// One decoded file: its columns by schema name.
struct Table<'a> {
    cand: &'a Candidate,
}

impl Table<'_> {
    fn spec(&self, name: &str) -> &Column {
        let at = self
            .cand
            .spec
            .position(name)
            .expect("the schema check found it");
        &self.cand.spec.columns[at]
    }

    fn integers(&self, name: &str) -> Vec<Option<i64>> {
        let at = self
            .cand
            .spec
            .position(name)
            .expect("the schema check found it");
        integers(&self.cand.columns[at])
    }
}

/// A count against its `units`: a non-null count has a non-null truth of at
/// least `min_cell_count`, and a person count lies within its declared
/// perturbation of its units, which are its truth.
fn count_truth(
    findings: &mut Vec<Finding>,
    file: &str,
    col: &Column,
    published: &[Option<i64>],
    units: &[Option<i64>],
    policy: &Policy,
) {
    let rows = || published.iter().zip(units).filter(|(p, _)| p.is_some());
    if rows().any(|(_, u)| u.is_none()) {
        findings.push(Finding::column("unsupported_count", file, &col.name));
    }
    if rows().any(|(_, u)| u.is_some_and(|u| below(u, policy.min_cell_count))) {
        findings.push(Finding::column("count_units_too_small", file, &col.name));
    }
    if col.role == Role::PersonCount {
        let d = i128::from(col.perturbation.unwrap_or(0));
        let beyond = rows().any(|(p, u)| match (p, u) {
            (Some(p), Some(u)) => (i128::from(*p) - i128::from(*u)).abs() > d,
            _ => false,
        });
        if beyond {
            findings.push(Finding::column(
                "count_perturbation_exceeded",
                file,
                &col.name,
            ));
        }
    }
}

/// A statistic or share against its support: null wherever the support or
/// its truth is null, and, under a minimum (the policy's, raised by the
/// column's own), resting on a published support and a truth of at least it.
fn statistic_support(
    findings: &mut Vec<Finding>,
    table: &Table,
    col: &Column,
    array: &ArrayRef,
    policy: &Policy,
) {
    let file = table.cand.spec.name.as_str();
    let support_name = col
        .support
        .as_deref()
        .expect("a statistic names its support");
    let support = table.integers(support_name);
    let support_spec = table.spec(support_name);
    // A check-only support is its own truth; a count's truth is its units.
    let truth = match (support_spec.role, &support_spec.units) {
        (Role::CheckOnly, _) => Some(support.clone()),
        (_, Some(units)) => Some(table.integers(units)),
        (_, None) => None,
    };
    let at = |v: &Option<Vec<Option<i64>>>, i: usize| v.as_ref().map(|t| t[i]);
    let rows = || (0..array.len()).filter(|&i| array.is_valid(i));
    if rows().any(|i| support[i].is_none() || at(&truth, i) == Some(None)) {
        findings.push(Finding::column("unsupported_statistic", file, &col.name));
    }
    // A published support is floored by `min_cell_count` as a count; a
    // check-only support is not, so its minimum is at least that floor.
    let floor = (support_spec.role == Role::CheckOnly).then_some(policy.min_cell_count);
    let min = [policy.statistic_min_support, col.min_support, floor]
        .into_iter()
        .flatten()
        .max();
    if let Some(min) = min {
        let small = rows().any(|i| {
            support[i].is_some_and(|n| below(n, min))
                || at(&truth, i).flatten().is_some_and(|n| below(n, min))
        });
        if small {
            findings.push(Finding::column(
                "statistic_support_too_small",
                file,
                &col.name,
            ));
        }
    }
}

/// A share's own rules: a non-null share has a non-null numerator `k` and
/// denominator `n`, and neither `k` nor its complement `n − k` is a small cell
/// (1 to `min_cell_count − 1`) or negative.
fn share_cells(
    findings: &mut Vec<Finding>,
    table: &Table,
    col: &Column,
    array: &ArrayRef,
    policy: &Policy,
) {
    let file = table.cand.spec.name.as_str();
    let k = table.integers(col.numerator.as_deref().expect("a share's numerator"));
    let n = table.integers(col.denominator.as_deref().expect("a share's denominator"));
    let min = i128::from(policy.min_cell_count);
    let small = |x: i128| x < 0 || (1..min).contains(&x);
    let rows = || (0..array.len()).filter(|&i| array.is_valid(i));
    if rows().any(|i| k[i].is_none() || n[i].is_none()) {
        findings.push(Finding::column("unsupported_share", file, &col.name));
    }
    let bad = rows().any(|i| match (k[i], n[i]) {
        (Some(k), Some(n)) => {
            let (k, n) = (i128::from(k), i128::from(n));
            small(k) || small(n - k)
        }
        _ => false,
    });
    if bad {
        findings.push(Finding::column("share_cell_too_small", file, &col.name));
    }
}

pub(crate) fn check(candidates: &[Candidate], policy: &Policy) -> Result<(), Refusal> {
    let mut findings = Vec::new();
    for cand in candidates {
        let table = Table { cand };
        let file = cand.spec.name.as_str();
        for (col, array) in cand.spec.columns.iter().zip(&cand.columns) {
            if col.ty == ColumnType::Double
                && array
                    .as_primitive::<Float64Type>()
                    .iter()
                    .flatten()
                    .any(f64::is_nan)
            {
                findings.push(Finding::column("nan_value", file, &col.name));
            }
            if col.role.is_count() {
                let published = integers(array);
                // A negative count is below any minimum.
                if published
                    .iter()
                    .flatten()
                    .any(|&v| below(v, policy.min_cell_count))
                {
                    findings.push(Finding::column("min_cell_count", file, &col.name));
                }
                if let Some(units) = &col.units {
                    let units = table.integers(units);
                    count_truth(&mut findings, file, col, &published, &units, policy);
                }
            } else if col.role.is_statistic() {
                statistic_support(&mut findings, &table, col, array, policy);
                if col.role == Role::Share {
                    share_cells(&mut findings, &table, col, array, policy);
                }
                if let Some(digits) = policy.statistic_significant_digits
                    && col.ty == ColumnType::Double
                    && array
                        .as_primitive::<Float64Type>()
                        .iter()
                        .flatten()
                        .any(|v| !rounded(v, digits))
                {
                    findings.push(Finding::column("statistic_not_rounded", file, &col.name));
                }
            }
        }
    }
    if findings.is_empty() {
        Ok(())
    } else {
        Err(Refusal {
            code: Code::Disclosure,
            findings,
        })
    }
}
