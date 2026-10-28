//! F1: what changed in a variable's documentation, as notices at the day the new form
//! starts. A change is read between neighbours: `ambiguous` segments are not compared,
//! and a gap between two is not a change.

use std::collections::{BTreeMap, BTreeSet};

use reg_frame::ValueSet;

use crate::{
    Finding, ResolvedOccurrence, ResolvedRelease, ResolvedTable, Segment, Subject, Variable,
    label_key, placed,
};

#[allow(clippy::too_many_lines)]
pub(crate) fn changes(
    variables: &[Variable],
    resolved: &[ResolvedOccurrence],
    tables: &[ResolvedTable],
    releases: &[ResolvedRelease],
    valuesets: &BTreeMap<&str, &ValueSet>,
    findings: &mut Vec<Finding>,
) {
    let table_name = |t: usize| tables[t].table.name.clone().unwrap_or_default();
    // One finding per (variable, sub-kind, table or none, day the new form starts).
    let mut seen: BTreeSet<(usize, &'static str, Option<usize>, String)> = BTreeSet::new();
    let mut push =
        |v: usize, code: &'static str, table: Option<usize>, day: &str, message: String| {
            if seen.insert((v, code, table, day.to_owned())) {
                findings.push(Finding {
                    code,
                    severity: "notice",
                    subject: Subject::Variable(v),
                    period: Some((day.to_owned(), day.to_owned())),
                    message,
                });
            }
        };

    for (v, variable) in variables.iter().enumerate() {
        // Segments are held per table, in period order.
        for (i, pair) in variable.segments.windows(2).enumerate() {
            let (a, b) = (&pair[0], &pair[1]);
            if a.table != b.table || a.state == "ambiguous" || b.state == "ambiguous" {
                continue;
            }
            let day = b.period.0.as_str();
            let table = table_name(a.table);
            // Unstated or conflicting types do not state a family change.
            let families_differ = matches!(
                (super::family(a.dtype.as_deref()), super::family(b.dtype.as_deref())),
                (Some(x), Some(y)) if x != y
            );
            if families_differ {
                push(
                    v,
                    "F1:type-change",
                    Some(a.table),
                    day,
                    format!("{} → {} in {table}", shape(a), shape(b)),
                );
            }
            if a.valueset != b.valueset && a.coded != "no" && b.coded != "no" {
                // A side that states no set but whose occurrence states one set per
                // population is split, not emptied.
                let per_population = |side: &Segment, segment: usize| {
                    side.valueset.is_none()
                        && resolved.iter().any(|r| {
                            r.variable == Some(v)
                                && r.segment == Some(segment)
                                && !r.occurrence.population_valuesets.is_empty()
                        })
                };
                let split = if per_population(b, i + 1) {
                    Some("one set → per-population sets")
                } else if per_population(a, i) {
                    Some("per-population sets → one set")
                } else {
                    None
                };
                push(
                    v,
                    "F1:domain-change",
                    Some(a.table),
                    day,
                    format!("{} in {table}", domain(a, b, valuesets, split)),
                );
            }
        }

        for pair in variable.texts.windows(2) {
            let (a, b) = (&pair[0], &pair[1]);
            let day = b.period.0.as_str();
            if label_key(&a.label) != label_key(&b.label) {
                push(
                    v,
                    "F1:label-drift",
                    None,
                    day,
                    format!("{:?} → {:?}", a.label, b.label),
                );
            }
            let fields: Vec<String> = [
                ("definition", &a.definition, &b.definition),
                (
                    "operational definition",
                    &a.operational_definition,
                    &b.operational_definition,
                ),
            ]
            .into_iter()
            .filter(|(_, old, new)| fold(old.as_ref()) != fold(new.as_ref()))
            .map(|(name, old, new)| {
                let how = match (old, new) {
                    (None, _) => "added",
                    (_, None) => "removed",
                    _ => "changed",
                };
                format!("{name} {how}")
            })
            .collect();
            if !fields.is_empty() {
                push(v, "F1:definition-change", None, day, fields.join(", "));
            }
            if a.unit != b.unit {
                push(
                    v,
                    "F1:unit-change",
                    None,
                    day,
                    format!(
                        "{} → {}",
                        a.unit.as_deref().unwrap_or("no unit"),
                        b.unit.as_deref().unwrap_or("no unit")
                    ),
                );
            }
        }

        // Population: the primary releases carrying the variable, per delivery (table and
        // qualifier) in period order, so same-period deliveries are never compared;
        // undated releases and releases without populations are skipped.
        let mut carriers: BTreeMap<(usize, Option<String>), BTreeSet<usize>> = BTreeMap::new();
        for r in resolved {
            if r.variable == Some(v)
                && r.status == "used"
                && let Some(x) = r.release
                && releases[x].primary == Some(true)
                && !releases[x].release.populations.is_empty()
                && placed(&releases[x].release).is_some()
            {
                carriers
                    .entry((r.table, releases[x].release.qualifier.clone()))
                    .or_default()
                    .insert(x);
            }
        }
        for ((t, _), set) in carriers {
            let mut ordered: Vec<&ResolvedRelease> = set.iter().map(|&x| &releases[x]).collect();
            ordered.sort_by_key(|x| {
                (
                    placed(&x.release),
                    x.release.qualifier.clone(),
                    x.release.release.clone(),
                )
            });
            for pair in ordered.windows(2) {
                let (old, new) = (names(pair[0]), names(pair[1]));
                if old != new {
                    let list =
                        |s: &BTreeSet<&str>| s.iter().copied().collect::<Vec<_>>().join(", ");
                    push(
                        v,
                        "F1:population-change",
                        Some(t),
                        placed(&pair[1].release).expect("placed").0.as_str(),
                        format!("{} → {} in {}", list(&old), list(&new), table_name(t)),
                    );
                }
            }
        }
    }
}

/// A definition compares under the fold labels use: case and spacing.
fn fold(text: Option<&String>) -> Option<String> {
    text.map(|t| label_key(t))
}

fn names(x: &ResolvedRelease) -> BTreeSet<&str> {
    x.release
        .populations
        .iter()
        .map(|p| p.name.as_str())
        .collect()
}

/// A segment's type and any stated length.
fn shape(s: &Segment) -> String {
    let dtype = s.dtype.as_deref().unwrap_or("untyped");
    match s.length {
        Some(n) => format!("{dtype}({n})"),
        None => dtype.to_owned(),
    }
}

/// The codes a pair of value sets add, drop and relabel; a missing set has no codes.
fn domain(
    a: &Segment,
    b: &Segment,
    valuesets: &BTreeMap<&str, &ValueSet>,
    split: Option<&str>,
) -> String {
    if let Some(split) = split {
        return split.to_owned();
    }
    let codes = |s: &Segment| -> BTreeMap<&str, Option<&str>> {
        s.valueset
            .as_deref()
            .and_then(|h| valuesets.get(h))
            .map(|set| {
                set.items
                    .iter()
                    .map(|i| (i.code.as_str(), i.label.as_deref()))
                    .collect()
            })
            .unwrap_or_default()
    };
    let (old, new) = (codes(a), codes(b));
    let added = new.keys().filter(|c| !old.contains_key(*c)).count();
    let removed = old.keys().filter(|c| !new.contains_key(*c)).count();
    let relabelled = old
        .iter()
        .filter(|(c, label)| new.get(*c).is_some_and(|l| l != *label))
        .count();
    format!(
        "{} codes → {} codes: {added} added, {removed} removed, {relabelled} relabelled",
        old.len(),
        new.len()
    )
}
