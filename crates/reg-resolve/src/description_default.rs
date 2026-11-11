//! The default variable description (crates/DESIGN.md, "One variable, one description";
//! #2322, maintainer 2026-11-10 and 2026-11-11).
//!
//! A variable without a curated description whose used occurrences state several
//! provider wordings, one of which contains every other, takes that wording as its
//! description. Curated > rule: this runs after curation, only where none applied.
//! Provenance records the value as rule `description-default`.

use std::cmp::Reverse;
use std::collections::BTreeMap;

use crate::Resolved;

pub const RULE: &str = "description-default";

/// Settle each undescribed variable's description where its wordings fold into one.
pub fn apply(resolved: &mut Resolved) {
    // Per variable, each wording key with its raw text from the latest occurrence
    // (then the smallest text), so the displayed spelling is deterministic.
    let mut wordings = BTreeMap::<usize, BTreeMap<String, (&reg_core::Interval, String)>>::new();
    for occurrence in &resolved.occurrences {
        let (Some(variable), Some(period), "used") =
            (occurrence.variable, &occurrence.period, occurrence.status)
        else {
            continue;
        };
        let row = &occurrence.occurrence;
        let Some(text) = reg_core::provider_wording(
            row.definition.as_deref(),
            row.operational_definition.as_deref(),
        ) else {
            continue;
        };
        let entry = wordings
            .entry(variable)
            .or_default()
            .entry(reg_core::wording_key(&text));
        entry
            .and_modify(|(at, kept)| {
                if (Reverse(*at), &*kept) > (Reverse(period), &text) {
                    *at = period;
                    kept.clone_from(&text);
                }
            })
            .or_insert((period, text.clone()));
    }
    for (variable, keys) in wordings {
        let target = &mut resolved.variables[variable];
        if target.description.is_some() || keys.len() < 2 {
            continue;
        }
        // Only the longest key can contain every other.
        if let Some((key, (_, text))) = keys.iter().max_by_key(|(key, _)| key.len())
            && keys.keys().all(|other| key.contains(other.as_str()))
        {
            target.description = Some(text.clone());
            target.default_description = true;
        }
    }
}
