//! Disclosure: every non-null count is at least the policy's minimum (null is
//! the only suppression marker), a statistic is null wherever the count it
//! rests on is, and no double holds NaN, whatever its role: a NaN is not null,
//! and its payload bits survive Parquet and the rewrite, so a cell that looks
//! suppressed could carry a value. Null-ness is Arrow's validity, which is
//! exactly what the rewrite writes. A policy may also bound a statistic: the
//! smallest support it may rest on, and the significant digits of a double.

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int32Type, Int64Type};
use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType;

use crate::structure::Candidate;
use crate::{Code, ColumnType, Finding, Policy, Refusal, Role};

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

pub(crate) fn check(candidates: &[Candidate], policy: &Policy) -> Result<(), Refusal> {
    let mut findings = Vec::new();
    for cand in candidates {
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
                // A negative count is below any minimum.
                let below = |v: i64| u64::try_from(v).map_or(true, |v| v < policy.min_cell_count);
                if integers(array).into_iter().flatten().any(below) {
                    findings.push(Finding::column("min_cell_count", file, &col.name));
                }
            } else if col.role == Role::Statistic {
                let support = col
                    .support
                    .as_deref()
                    .expect("a statistic names its support");
                let at = cand
                    .spec
                    .columns
                    .iter()
                    .position(|c| c.name == support)
                    .expect("the schema check found the support");
                let support = &cand.columns[at];
                if (0..array.len()).any(|i| array.is_valid(i) && support.is_null(i)) {
                    findings.push(Finding::column("unsupported_statistic", file, &col.name));
                }
                if let Some(min) = policy.statistic_min_support {
                    let small = integers(support).into_iter().enumerate().any(|(i, n)| {
                        array.is_valid(i)
                            && n.is_some_and(|n| u64::try_from(n).map_or(true, |n| n < min))
                    });
                    if small {
                        findings.push(Finding::column(
                            "statistic_support_too_small",
                            file,
                            &col.name,
                        ));
                    }
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
