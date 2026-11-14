//! Integer-coded text publishes as integer (crates/DESIGN.md, "Integer-coded text";
//! maintainer 2026-11-04, #2071).
//!
//! A segment whose every covering occurrence is reader text (`char`) with a documented
//! code list of plain integers is an integer column. Curated > rule > source: a `set` may
//! state a type only under its published name, so an override to text is `text`, which
//! this rule never reads. Provenance records each change as rule `integer-codes`.

use std::collections::{BTreeMap, BTreeSet};

use reg_core::next_iso_day;
use reg_frame::{ValueSet, widest_dtype};

use crate::{ResolvedOccurrence, Variable, same_form};

/// `0`, `7`, `-12`: no sign without digits, no leading zero, no `-0`. A zero-padded or
/// alphanumeric code keeps the column text; so does a decimal point or a comma.
fn integer_code(code: &str) -> bool {
    let digits = code.trim_prefix('-');
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
    let mut flipped = BTreeSet::new();
    let mut covering = BTreeMap::<(usize, usize), Vec<usize>>::new();
    for (i, r) in resolved.iter().enumerate() {
        if let (Some(v), Some(s)) = (r.variable, r.segment) {
            covering.entry((v, s)).or_default().push(i);
        }
    }
    for ((v, s), members) in covering {
        let segment = &variables[v].segments[s];
        if segment.dtype.as_deref() != Some("char")
            || segment.row_key
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
        flipped.insert((v, s));
        for i in members {
            let o = &mut resolved[i];
            o.stated_dtype = o.occurrence.dtype.replace("integer".to_owned());
        }
    }
    rejoin(variables, resolved, &flipped);
}

/// A converted segment can now match its integer neighbour, which segment building kept
/// apart only for the type: join them as segment building joins adjacent runs of one
/// form, and point the occurrences at the joined segment. Only a pair with a converted
/// member joins; any other boundary was drawn for another reason.
fn rejoin(
    variables: &mut [Variable],
    resolved: &mut [ResolvedOccurrence],
    flipped: &BTreeSet<(usize, usize)>,
) {
    for v in flipped.iter().map(|&(v, _)| v).collect::<BTreeSet<_>>() {
        let old = std::mem::take(&mut variables[v].segments);
        let mut index = Vec::with_capacity(old.len());
        let mut kept: Vec<(crate::Segment, bool)> = Vec::new();
        for (s, segment) in old.into_iter().enumerate() {
            let converted = flipped.contains(&(v, s));
            match kept.last_mut() {
                Some((prev, prev_converted))
                    if (converted || *prev_converted)
                        && prev.table == segment.table
                        && next_iso_day(&prev.period.1) == segment.period.0
                        && same_form(prev, &segment) =>
                {
                    prev.period.1 = segment.period.1;
                    prev.spelling = segment.spelling;
                    prev.dtype = widest_dtype(
                        prev.dtype
                            .as_deref()
                            .into_iter()
                            .chain(segment.dtype.as_deref()),
                    )
                    .map(str::to_owned);
                    prev.length = prev.length.max(segment.length);
                    *prev_converted = converted;
                }
                _ => kept.push((segment, converted)),
            }
            index.push(kept.len() - 1);
        }
        variables[v].segments = kept.into_iter().map(|(segment, _)| segment).collect();
        for r in resolved.iter_mut().filter(|r| r.variable == Some(v)) {
            if let Some(s) = r.segment {
                r.segment = Some(index[s]);
            }
        }
    }
}
