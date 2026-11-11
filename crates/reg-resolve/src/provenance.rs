//! Final field provenance, decided before catalog emission.

use std::collections::BTreeMap;

use reg_core::Interval;
use reg_frame::ValueSet;
use serde_json::Value;

use crate::Resolved;
use crate::curation::apply::{Change, Classification};

/// A source-shaped application's change and the publication identity of the
/// statement that made it, before default resolution.
#[derive(Clone)]
pub struct Claim {
    pub change: Change,
    pub statement: String,
}

/// A final catalog subject, using positions in the resolved build rather than row ids.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Subject {
    Register(usize),
    Set(usize, String),
    Table(usize, usize),
    Release(usize, usize),
    Occurrence(usize, usize),
    Variable(usize, usize),
    ValueSet(String),
    ClassVersion(String, String),
}

/// The one authority that supplied the published field.
pub enum Origin {
    Curated(String),
    Rule(&'static str),
}

pub struct FinalValue {
    pub subject: Subject,
    pub field: String,
    pub value: Option<String>,
    pub source_value: Option<String>,
    pub origin: Origin,
}

#[derive(Default)]
pub struct Finalized {
    pub values: Vec<FinalValue>,
    /// Missing occurrences have the schema default mask, zero.
    pub occurrence_masks: BTreeMap<(usize, usize), i64>,
}

fn override_bit(field: &str) -> i64 {
    [
        "spelling",
        "type",
        "length",
        "format",
        "value_set",
        "classification",
        "coded",
        "sensitive",
        "identifier",
        "role",
        "label",
        "definition",
        "operational_definition",
        "unit",
        "period",
        "table",
        "variable",
    ]
    .iter()
    .position(|name| *name == field)
    .map_or(0, |bit| 1 << bit)
}

fn matches(
    claim: &Claim,
    provider: Option<&str>,
    register: Option<&str>,
    table: Option<&str>,
    period: Option<&Interval>,
) -> bool {
    let claim = &claim.change;
    claim
        .provider
        .as_deref()
        .is_none_or(|p| Some(p) == provider)
        && claim
            .register
            .as_deref()
            .is_none_or(|r| Some(r) == register)
        && claim.table.as_deref().is_none_or(|t| Some(t) == table)
        && claim
            .period
            .as_ref()
            .is_none_or(|p| period.is_some_and(|other| reg_core::intersect(p, other).is_some()))
}

fn field_values<'a>(
    claims: impl Iterator<Item = &'a Claim>,
    subject: &Subject,
    current: &Value,
    period: Option<&Interval>,
) -> Vec<FinalValue> {
    let mut winners = BTreeMap::<&str, (&Claim, Option<String>)>::new();
    for claim in claims {
        let field = reg_frame::statement::frame_field(&claim.change.field);
        let final_value = if field == "period" {
            period.map(|p| serde_json::to_string(p).expect("period serializes"))
        } else if let Some(value) = current.get(field) {
            crate::curation::apply::rendered(value)
        } else {
            claim.change.value.clone()
        };
        // Only a claim for the resolved value can own it. Shared code lists can
        // have equal claims; use one deterministic owner.
        if field != "period" && final_value != claim.change.value {
            continue;
        }
        let entry = winners
            .entry(&claim.change.field)
            .or_insert((claim, final_value.clone()));
        if (&claim.statement, &claim.change.locator) < (&entry.0.statement, &entry.0.change.locator)
        {
            *entry = (claim, final_value);
        }
    }
    winners
        .into_values()
        .map(|(claim, value)| FinalValue {
            subject: subject.clone(),
            field: claim.change.field.clone(),
            value,
            source_value: claim.change.source_value.clone(),
            origin: Origin::Curated(claim.statement.clone()),
        })
        .collect()
}

/// Finalize curated ownership, winning values, losing evidence and rule provenance.
/// The emitter maps these subjects to database row ids and writes the result unchanged.
///
/// # Panics
/// Only if a typed resolved/frame record stops serializing to its declared JSON shape.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn finalize(
    registers: &[Resolved],
    sets: &[&ValueSet],
    classes: &[Classification],
    claims: &[Claim],
) -> Finalized {
    let mut result = Finalized::default();
    let mut by_locator = BTreeMap::<_, Vec<_>>::new();
    let mut by_coordinates = BTreeMap::<_, Vec<_>>::new();
    for claim in claims {
        by_locator
            .entry((claim.change.kind.as_str(), &claim.change.locator))
            .or_default()
            .push(claim);
        by_coordinates
            .entry((
                claim.change.kind.as_str(),
                claim.change.provider.as_deref(),
                claim.change.register.as_deref(),
                claim.change.table.as_deref(),
            ))
            .or_default()
            .push(claim);
    }
    for (ri, resolved) in registers.iter().enumerate() {
        let register = &resolved.register;
        let candidates = by_coordinates.get(&(
            "register",
            Some(register.provider.as_str()),
            Some(register.register.as_str()),
            None,
        ));
        result.values.extend(field_values(
            candidates.into_iter().flatten().copied().filter(|c| {
                matches(
                    c,
                    Some(&register.provider),
                    Some(&register.register),
                    None,
                    Some(&resolved.coverage),
                )
            }),
            &Subject::Register(ri),
            &serde_json::to_value(register).expect("register serializes"),
            Some(&resolved.coverage),
        ));
        let set_claims = claims.iter().filter(|c| {
            c.change.kind == "set"
                && c.change.provider.as_deref() == Some(register.provider.as_str())
                && c.change.register.as_deref() == Some(register.register.as_str())
        });
        let mut by_set = BTreeMap::<&str, Vec<&Claim>>::new();
        for claim in set_claims {
            if let Some(slug) = claim.change.table.as_deref() {
                by_set.entry(slug).or_default().push(claim);
            }
        }
        for (slug, claims) in by_set {
            result.values.extend(field_values(
                claims.into_iter(),
                &Subject::Set(ri, slug.to_owned()),
                &serde_json::json!({}),
                None,
            ));
        }
        for (ti, table) in resolved.tables.iter().enumerate() {
            let table = &table.table;
            let period = reg_frame::bounds(table.from.as_deref(), table.to.as_deref());
            let candidates = by_coordinates.get(&(
                "table",
                Some(table.provider.as_str()),
                Some(table.register.as_str()),
                Some(table.table.as_str()),
            ));
            result.values.extend(field_values(
                candidates.into_iter().flatten().copied().filter(|c| {
                    matches(
                        c,
                        Some(&table.provider),
                        Some(&table.register),
                        Some(&table.table),
                        Some(&period),
                    )
                }),
                &Subject::Table(ri, ti),
                &serde_json::to_value(table).expect("table serializes"),
                Some(&period),
            ));
        }
        for (xi, release) in resolved.releases.iter().enumerate() {
            let release = &release.release;
            let period = release
                .from
                .as_ref()
                .zip(release.to.as_ref())
                .map(|(a, b)| (a.clone(), b.clone()));
            let candidates = by_coordinates.get(&(
                "release",
                Some(release.provider.as_str()),
                Some(release.register.as_str()),
                Some(release.table.as_str()),
            ));
            result.values.extend(field_values(
                candidates.into_iter().flatten().copied().filter(|c| {
                    c.change.release.as_deref() == Some(release.release.as_str())
                        && matches(
                            c,
                            Some(&release.provider),
                            Some(&release.register),
                            Some(&release.table),
                            period.as_ref(),
                        )
                }),
                &Subject::Release(ri, xi),
                &serde_json::to_value(release).expect("release serializes"),
                period.as_ref(),
            ));
        }
        for (oi, occurrence) in resolved.occurrences.iter().enumerate() {
            let source = &occurrence.occurrence;
            let subject = Subject::Occurrence(ri, oi);
            let candidates = std::iter::once(&source.locator)
                .chain(&source.also)
                .filter_map(|at| by_locator.get(&("occurrence", at)))
                .flatten()
                .copied()
                .filter(|claim| {
                    claim.change.identity.as_ref()
                        == Some(&crate::OccurrenceIdentity::new(
                            source,
                            occurrence
                                .stated
                                .clone()
                                .or_else(|| occurrence.period.clone()),
                        ))
                        && (occurrence.period.is_some() || claim.change.field != "period")
                });
            let mut current = serde_json::to_value(source).expect("occurrence serializes");
            current["variable"] = Value::String(occurrence.variable.map_or_else(
                || "none".to_owned(),
                |index| resolved.variables[index].slug.clone(),
            ));
            if let Some(variable) = occurrence.variable {
                let current =
                    serde_json::json!({"description": resolved.variables[variable].description});
                for mut value in field_values(
                    candidates
                        .clone()
                        .filter(|claim| claim.change.field == "description"),
                    &Subject::Variable(ri, variable),
                    &current,
                    None,
                ) {
                    value.source_value = None;
                    result.values.push(value);
                }
            }
            let mut values = field_values(
                candidates.filter(|claim| claim.change.field != "description"),
                &subject,
                &current,
                occurrence.period.as_ref(),
            );
            let mut mask = values
                .iter()
                .fold(0, |mask, value| mask | override_bit(&value.field));
            if occurrence.stated.is_some() && occurrence.status == "used" {
                if mask & override_bit("period") == 0 {
                    let render = |p: &Interval| reg_core::render(std::slice::from_ref(p));
                    values.push(FinalValue {
                        subject: subject.clone(),
                        field: "period".into(),
                        value: occurrence.period.as_ref().map(render),
                        source_value: occurrence.stated.as_ref().map(render),
                        origin: Origin::Rule("T1"),
                    });
                }
                mask |= override_bit("period");
            }
            if let Some(stated) = &occurrence.stated_dtype
                && mask & override_bit("type") == 0
            {
                values.push(FinalValue {
                    subject,
                    field: "type".into(),
                    value: occurrence.occurrence.dtype.clone(),
                    source_value: Some(stated.clone()),
                    origin: Origin::Rule("integer-codes"),
                });
                mask |= override_bit("type");
            }
            if mask != 0 {
                result.occurrence_masks.insert((ri, oi), mask);
            }
            for mut value in values {
                if matches!(value.field.as_str(), "sets" | "tags" | "facets") {
                    if let Some(variable) = occurrence.variable {
                        value.subject = Subject::Variable(ri, variable);
                        result.values.push(value);
                    }
                } else {
                    result.values.push(value);
                }
            }
        }
        for (vi, variable) in resolved.variables.iter().enumerate() {
            if variable.default_description {
                result.values.push(FinalValue {
                    subject: Subject::Variable(ri, vi),
                    field: "description".into(),
                    value: variable.description.clone(),
                    source_value: None,
                    origin: Origin::Rule(crate::description_default::RULE),
                });
            }
        }
    }
    // One variable-level membership value can be reached through several selected occurrences.
    result
        .values
        .sort_by(|a, b| (&a.subject, &a.field).cmp(&(&b.subject, &b.field)));
    result
        .values
        .dedup_by(|a, b| a.subject == b.subject && a.field == b.field);
    // Content-addressed sets may keep a source locator when an assertion introduces
    // the same code list elsewhere. Its asserted provenance follows the hash, not the
    // arbitrary first source locator retained by value-set interning.
    let asserted_sets = claims
        .iter()
        .filter(|c| c.change.kind == "valueset" && c.change.field == "hash")
        .filter_map(|c| {
            c.change
                .value
                .as_deref()
                .map(|hash| ((c.statement.as_str(), &c.change.locator), hash))
        })
        .collect::<BTreeMap<_, _>>();
    let mut sets_by_hash = BTreeMap::<_, Vec<_>>::new();
    for claim in claims.iter().filter(|c| c.change.kind == "valueset") {
        if let Some(hash) = asserted_sets.get(&(claim.statement.as_str(), &claim.change.locator)) {
            sets_by_hash.entry(*hash).or_default().push(claim);
        }
    }
    for set in sets {
        let candidates = sets_by_hash.get(set.hash.as_str());
        result.values.extend(field_values(
            candidates.into_iter().flatten().copied(),
            &Subject::ValueSet(set.hash.clone()),
            &serde_json::to_value(set).expect("value set serializes"),
            None,
        ));
    }
    for class in classes {
        let candidates = by_locator.get(&("class_version", &class.locator));
        result.values.extend(field_values(
            candidates
                .into_iter()
                .flatten()
                .copied()
                .filter(|c| matches(c, None, None, None, Some(&class.period))),
            &Subject::ClassVersion(class.family.clone(), class.version.clone()),
            &serde_json::json!(class.facts(classes)),
            Some(&class.period),
        ));
    }
    result
}
