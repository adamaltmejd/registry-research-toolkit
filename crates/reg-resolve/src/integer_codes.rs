//! Integer-coded text publishes as integer (crates/DESIGN.md, "Integer-coded text";
//! maintainer 2026-11-04, #2071).
//!
//! A segment whose every covering occurrence is reader text (`char`) with a documented
//! code list of plain integers is an integer column. Curated > rule > source: curation
//! may state a type only under its published name, so curated text is `text`, which
//! this rule never reads. Provenance records each change as rule `integer-codes`.

use std::collections::BTreeMap;

use reg_frame::ValueSet;

use crate::{ResolvedOccurrence, Variable};

/// `0`, `7`, `-12`: no sign without digits, no leading zero, no `-0`. A zero-padded or
/// alphanumeric code keeps the column text; so does a decimal point or a comma.
fn integer_code(code: &str) -> bool {
    let digits = code.strip_prefix('-').unwrap_or(code);
    !digits.is_empty()
        && digits.bytes().all(|b| b.is_ascii_digit())
        && (digits == "0" || !digits.starts_with('0'))
        && code != "-0"
}

/// Every covering occurrence must qualify, so the rule never splits a segment into text
/// and integer years (G3). Row keys and identifiers stay text: a code list never covers
/// every unit id (G2). A classification governs its own codes. Sentinels are not
/// removed: a non-integer sentinel keeps the column text (stricter than N1 was, and no
/// 0.75.0 N1 review had one).
pub(crate) fn apply(
    variables: &mut [Variable],
    resolved: &mut [ResolvedOccurrence],
    valuesets: &BTreeMap<&str, &ValueSet>,
) {
    let mut covering = BTreeMap::<(usize, usize), Vec<usize>>::new();
    for (i, r) in resolved.iter().enumerate() {
        if let (Some(v), Some(s)) = (r.variable, r.segment) {
            covering.entry((v, s)).or_default().push(i);
        }
    }
    for ((v, s), members) in covering {
        let segment = &variables[v].segments[s];
        if segment.dtype.as_deref() != Some("char")
            || segment.role == "row_key"
            || segment.identifier == Some(true)
            || segment.classification.is_some()
        {
            continue;
        }
        let qualifies = members.iter().all(|&i| {
            let o = &resolved[i].occurrence;
            o.dtype.as_deref() == Some("char")
                && o.coded != Some(false)
                && o.classification.is_none()
                && o.identifier != Some(true)
                && o.valueset
                    .as_deref()
                    .and_then(|hash| valuesets.get(hash))
                    .is_some_and(|set| {
                        !set.items.is_empty()
                            && set.items.iter().all(|item| integer_code(&item.code))
                    })
        });
        if !qualifies {
            continue;
        }
        variables[v].segments[s].dtype = Some("integer".to_owned());
        for i in members {
            let o = &mut resolved[i];
            o.stated_dtype = o.occurrence.dtype.replace("integer".to_owned());
        }
    }
}
