//! N1 proposes a logical type where an occurrence's documented code domain supports
//! another type than its normalized one (crates/DESIGN.md, "Logical types (N1)").
//! Review only: the proposed `set type` is never applied.

use std::collections::{BTreeMap, BTreeSet};

use reg_frame::ValueSet;
use reg_frame::statement::{SetFields, Statement};
use reg_resolve::{Finding, Resolved, Subject};

/// The closed pattern table over complete documented non-sentinel codes. Text whose
/// codes are all integers with no significant leading zero proposes `integer`; an
/// integer whose numeric codes include a non-integral literal proposes `decimal`.
/// Everything else proposes nothing: zero-padded or alphanumeric codes keep text, a
/// decimal's integer codes do not prove an uncoded amount is integral (D's 2026-10-28
/// ruling keeps integer/decimal switches to evidenced curation), and letters on a
/// numeric column are as often unflagged missing markers (`.`, `blank`) as codes.
fn proposal<'a>(
    family: &str,
    codes: impl Iterator<Item = &'a str> + Clone,
) -> Option<&'static str> {
    if codes.clone().next().is_none() {
        return None;
    }
    match family {
        "text" if codes.clone().all(integer) => Some("integer"),
        "integer"
            if codes.clone().all(|c| integer(c) || fraction(c)) && codes.clone().any(fraction) =>
        {
            Some("decimal")
        }
        _ => None,
    }
}

/// `0`, `7`, `-12`: no sign without digits, no leading zero, no `-0`.
fn integer(code: &str) -> bool {
    let digits = code.strip_prefix('-').unwrap_or(code);
    !digits.is_empty()
        && digits.bytes().all(|b| b.is_ascii_digit())
        && (digits == "0" || !digits.starts_with('0'))
        && code != "-0"
}

/// `1.5`, `-0.25`: a point and a non-zero fractional digit. A comma is not read as a
/// decimal separator: `1,5` may as well list two codes.
fn fraction(code: &str) -> bool {
    code.split_once('.').is_some_and(|(whole, part)| {
        (integer(whole) || whole == "-0")
            && part.bytes().all(|b| b.is_ascii_digit())
            && part.bytes().any(|b| b != b'0')
    })
}

pub(super) fn generate(
    registers: &mut [Resolved],
    sets: &BTreeMap<&str, &ValueSet>,
    sentinels: &BTreeMap<String, BTreeSet<String>>,
) -> Result<(), String> {
    for register in registers {
        for (index, occurrence) in register.occurrences.iter().enumerate() {
            let row = &occurrence.occurrence;
            // A classification binding governs its codes, and its sentinels bind after
            // the acknowledgement stage, so N1 leaves bound occurrences alone.
            if occurrence.status != "used"
                || row.coded == Some(false)
                || row.classification.is_some()
            {
                continue;
            }
            let (Some(dtype), Some(set)) = (
                row.dtype.as_deref(),
                row.valueset.as_deref().and_then(|hash| sets.get(hash)),
            ) else {
                continue;
            };
            let Some(family) = reg_frame::dtype_info(dtype).and_then(|d| d.family) else {
                continue;
            };
            let flagged = sentinels.get(&set.hash);
            let codes = set
                .items
                .iter()
                .map(|item| item.code.as_str())
                .filter(|code| !flagged.is_some_and(|s| s.contains(*code)));
            let Some(proposed) = proposal(family, codes.clone()) else {
                continue;
            };
            let Some(subject) = super::occurrence_address(register, occurrence) else {
                continue;
            };
            let evidence = format!(
                "N1: normalized type {dtype}; {} documented non-sentinel codes support {proposed}",
                codes.count()
            );
            let statement = Statement::Set {
                subject,
                fields: Box::new(SetFields {
                    dtype: Some(proposed.to_owned()),
                    ..SetFields::default()
                }),
                evidence: evidence.clone(),
                caveat: Some(
                    "Proposal only; a code list proves its codes, not every delivered value."
                        .into(),
                ),
            };
            register.findings.push(Finding {
                code: "N1:logical-type",
                severity: "review",
                subject: Subject::Occurrence(index),
                period: occurrence.period.clone(),
                message: format!("{evidence}\n{}", super::statement_toml(&statement)?),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::proposal;

    // The pattern table from crates/DESIGN.md, "Logical types (N1)". Fails if a
    // zero-padded or alphanumeric domain proposes a number, a decimal's integer codes
    // propose integer, or a comma is read as a decimal point.
    #[test]
    fn pattern_table() {
        let cases: &[(&str, &[&str], Option<&str>)] = &[
            ("text", &["1", "2", "10", "-1", "0"], Some("integer")),
            ("text", &["01", "02"], None),
            ("text", &["1", "A"], None),
            ("text", &["1.5"], None),
            ("text", &[], None),
            ("integer", &["1", "1.5"], Some("decimal")),
            ("integer", &["1", "2.0"], None),
            ("integer", &["1", "1,5"], None),
            ("integer", &["1", "."], None),
            ("integer", &["1", "SA"], None),
            ("decimal", &["1", "2"], None),
            ("decimal", &["0.5"], None),
            ("temporal", &["1"], None),
        ];
        for (family, codes, expected) in cases {
            assert_eq!(
                proposal(family, codes.iter().copied()),
                *expected,
                "{family} {codes:?}"
            );
        }
    }
}
