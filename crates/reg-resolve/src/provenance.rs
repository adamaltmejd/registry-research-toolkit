//! Final field provenance, decided before catalog emission.

use std::collections::BTreeMap;

use reg_core::Interval;
use reg_frame::{Locator, ValueSet};
use serde_json::Value;

use crate::Resolved;
use crate::curation::apply::Classification;

/// A source-shaped application's claim, before default resolution.
#[derive(Clone)]
pub struct Claim {
    pub kind: String,
    pub locator: Locator,
    pub identity: Option<crate::OccurrenceIdentity>,
    pub provider: Option<String>,
    pub register: Option<String>,
    pub table: Option<String>,
    pub release: Option<String>,
    pub period: Option<Interval>,
    pub field: String,
    pub value: Option<String>,
    pub source_value: Option<String>,
    pub statement: String,
}

/// A final catalog subject, using positions in the resolved build rather than row ids.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Subject {
    Register(usize),
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
        "id_kind",
        "role",
        "temporality",
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
        let field = match claim.field.as_str() {
            "type" => "dtype",
            "spelling" => "column",
            "value_set" => "valueset",
            field => field,
        };
        let final_value = if field == "period" {
            period.map(|p| serde_json::to_string(p).expect("period serializes"))
        } else if let Some(value) = current.get(field) {
            match value {
                Value::Null => None,
                Value::String(s) => Some(s.clone()),
                value => Some(value.to_string()),
            }
        } else {
            claim.value.clone()
        };
        // Only a claim for the resolved value can own it. Shared code lists can
        // have equal claims; use one deterministic owner.
        if field != "period" && final_value != claim.value {
            continue;
        }
        let entry = winners
            .entry(&claim.field)
            .or_insert((claim, final_value.clone()));
        if (&claim.statement, &claim.locator) < (&entry.0.statement, &entry.0.locator) {
            *entry = (claim, final_value);
        }
    }
    winners
        .into_values()
        .map(|(claim, value)| FinalValue {
            subject: subject.clone(),
            field: claim.field.clone(),
            value,
            source_value: claim.source_value.clone(),
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
            .entry((claim.kind.as_str(), &claim.locator))
            .or_default()
            .push(claim);
        by_coordinates
            .entry((
                claim.kind.as_str(),
                claim.provider.as_deref(),
                claim.register.as_deref(),
                claim.table.as_deref(),
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
        for (ti, table) in resolved.tables.iter().enumerate() {
            let table = &table.table;
            let period = (
                table
                    .from
                    .clone()
                    .unwrap_or_else(|| reg_frame::OPEN_START.into()),
                table
                    .to
                    .clone()
                    .unwrap_or_else(|| reg_frame::OPEN_END.into()),
            );
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
                    c.release.as_deref() == Some(release.release.as_str())
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
                    claim.identity.as_ref()
                        == Some(&crate::OccurrenceIdentity::new(
                            source,
                            occurrence
                                .stated
                                .clone()
                                .or_else(|| occurrence.period.clone()),
                        ))
                        && (occurrence.period.is_some() || claim.field != "period")
                });
            let mut current = serde_json::to_value(source).expect("occurrence serializes");
            current["variable"] = Value::String(occurrence.variable.map_or_else(
                || "none".to_owned(),
                |index| resolved.variables[index].slug.clone(),
            ));
            let mut values =
                field_values(candidates, &subject, &current, occurrence.period.as_ref());
            let mut mask = values
                .iter()
                .fold(0, |mask, value| mask | override_bit(&value.field));
            if occurrence.stated.is_some() && occurrence.status == "used" {
                if mask & (1 << 16) == 0 {
                    let render = |p: &Interval| reg_core::render(std::slice::from_ref(p));
                    values.push(FinalValue {
                        subject,
                        field: "period".into(),
                        value: occurrence.period.as_ref().map(render),
                        source_value: occurrence.stated.as_ref().map(render),
                        origin: Origin::Rule("T1"),
                    });
                }
                mask |= 1 << 16;
            }
            if mask != 0 {
                result.occurrence_masks.insert((ri, oi), mask);
            }
            for mut value in values {
                if value.field == "sets" {
                    if let Some(variable) = occurrence.variable {
                        value.subject = Subject::Variable(ri, variable);
                        result.values.push(value);
                    }
                } else {
                    result.values.push(value);
                }
            }
        }
    }
    // One variable-level set value can be reached through several selected occurrences.
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
        .filter(|c| c.kind == "valueset" && c.field == "hash")
        .filter_map(|c| {
            c.value
                .as_deref()
                .map(|hash| ((c.statement.as_str(), &c.locator), hash))
        })
        .collect::<BTreeMap<_, _>>();
    let mut sets_by_hash = BTreeMap::<_, Vec<_>>::new();
    for claim in claims.iter().filter(|c| c.kind == "valueset") {
        if let Some(hash) = asserted_sets.get(&(claim.statement.as_str(), &claim.locator)) {
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
        result.values.extend(field_values(candidates.into_iter().flatten().copied().filter(|c| matches(c, None, None, None, Some(&class.period))), &Subject::ClassVersion(class.family.clone(), class.version.clone()), &serde_json::json!({"family":class.family,"version":class.version,"valueset":class.valueset}), Some(&class.period)));
    }
    result
}
