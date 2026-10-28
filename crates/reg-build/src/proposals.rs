//! Review-only candidates. Statements are serialized for review, never applied.

use std::collections::{BTreeMap, BTreeSet};

use reg_frame::ValueSet;
use reg_frame::address::{Address, Comparison, Guard, GuardField};
use reg_frame::statement::{Relation, SetFields, Statement};
use reg_resolve::curation::apply::Classification;
use reg_resolve::{Finding, Resolved, ResolvedOccurrence, Subject};

use crate::link::{Entity, Linked};

mod classification;

pub fn generate(
    registers: &mut [Resolved],
    classes: &[Classification],
    sets: &BTreeMap<&str, &ValueSet>,
    linked: &Linked,
) -> Result<(), String> {
    continuations(registers, linked)?;
    classification::generate(registers, classes, sets)
}

fn statement_toml(statement: &Statement) -> Result<String, String> {
    // The authoring loader expects an array of statements, not a bare statement.
    #[derive(serde::Serialize)]
    struct File<'a> {
        statement: [&'a Statement; 1],
    }
    statement.validate()?;
    toml::to_string_pretty(&File {
        statement: [statement],
    })
    .map_err(|e| e.to_string())
}

fn occurrence_address(r: &Resolved, o: &ResolvedOccurrence) -> Option<Address> {
    let row = &o.occurrence;
    let table = r.tables[o.table].slug.as_deref().unwrap_or(&r.public);
    let mut guards = vec![Guard {
        field: GuardField::Key,
        comparison: Comparison::Equal,
        value: row.source_key.clone(),
    }];
    if let Some(release) = &row.release {
        guards.push(Guard {
            field: GuardField::Release,
            comparison: Comparison::Equal,
            value: release.clone(),
        });
    }
    let periods = o
        .stated
        .as_ref()
        .or(o.period.as_ref())
        .map(|(lo, hi)| format!("{lo}..{hi}").parse())
        .transpose()
        .ok()?;
    let address = Address::Occurrence {
        table: table.to_owned(),
        spellings: vec![reg_core::fold_identity(&row.column)],
        periods,
        guards,
    };
    // The grammar cannot quote every possible source spelling or guard value.
    let parsed = address.to_string().parse::<Address>().ok()?;
    (parsed == address).then_some(address)
}

#[allow(clippy::too_many_lines)]
fn continuations(registers: &mut [Resolved], linked: &Linked) -> Result<(), String> {
    let addresses: Vec<Vec<Option<Vec<Address>>>> = registers
        .iter()
        .map(|r| {
            let mut subjects = vec![Some(Vec::new()); r.variables.len()];
            for o in &r.occurrences {
                if let Some(vi) = o.variable {
                    if let Some(address) = occurrence_address(r, o) {
                        if let Some(items) = &mut subjects[vi] {
                            items.push(address);
                        }
                    } else {
                        subjects[vi] = None;
                    }
                }
            }
            subjects
        })
        .collect();
    // Index evidence and end days together: only adjacent candidates are compared,
    // including renames. Avoid comparing every pair of catalog variables.
    let mut ends: BTreeMap<(String, String, String), BTreeSet<(usize, usize)>> = BTreeMap::new();
    for (ri, r) in registers.iter().enumerate() {
        for (vi, v) in r.variables.iter().enumerate() {
            for key in evidence_keys(v) {
                ends.entry((r.register.provider.clone(), v.hull.1.clone(), key))
                    .or_default()
                    .insert((ri, vi));
            }
        }
    }
    let existing: BTreeSet<_> = linked
        .relations
        .iter()
        .filter(|edge| edge.kind == "continues")
        .map(|edge| (edge.subject, edge.object))
        .collect();
    let mut findings = Vec::new();
    for (ri, r) in registers.iter().enumerate() {
        for (vi, v) in r.variables.iter().enumerate() {
            if v.hull.0 == reg_frame::OPEN_START {
                continue;
            }
            let Some(subjects) = addresses[ri][vi].as_ref().filter(|s| !s.is_empty()) else {
                continue;
            };
            let previous = reg_core::prev_iso_day(&v.hull.0);
            let mut candidates = BTreeSet::new();
            for key in evidence_keys(v) {
                if let Some(matches) =
                    ends.get(&(r.register.provider.clone(), previous.clone(), key))
                {
                    candidates.extend(matches);
                }
            }
            let mut ranked = Vec::new();
            for &(pi, pv) in &candidates {
                let predecessor = &registers[pi].variables[pv];
                if (ri == pi && v.source_key == predecessor.source_key)
                    || existing.contains(&(Entity::Variable(ri, vi), Entity::Variable(pi, pv)))
                {
                    continue;
                }
                let label = !v.label.is_empty()
                    && reg_core::label_key(&v.label) == reg_core::label_key(&predecessor.label);
                let spelling = v.fold == predecessor.fold;
                let before = predecessor
                    .segments
                    .iter()
                    .find(|s| s.period.1 == predecessor.hull.1);
                let after = v.segments.iter().find(|s| s.period.0 == v.hull.0);
                let values = before
                    .zip(after)
                    .is_some_and(|(a, b)| a.valueset.is_some() && a.valueset == b.valueset);
                let dtype = before.zip(after).is_some_and(|(a, b)| {
                    a.dtype
                        .as_deref()
                        .and_then(reg_frame::dtype_info)
                        .zip(b.dtype.as_deref().and_then(reg_frame::dtype_info))
                        .is_some_and(|(a, b)| a.family.is_some() && a.family == b.family)
                });
                let score = 4 * u8::from(label)
                    + 3 * u8::from(values)
                    + 2 * u8::from(spelling)
                    + u8::from(dtype);
                if score >= 4 {
                    ranked.push((
                        std::cmp::Reverse(score),
                        pi,
                        pv,
                        label,
                        values,
                        spelling,
                        dtype,
                    ));
                }
            }
            ranked.sort();
            for (rank, (std::cmp::Reverse(score), pi, pv, label, values, spelling, dtype)) in ranked
                .into_iter()
                .filter(|(_, pi, pv, ..)| {
                    registers[*pi].variables[*pv].pinned
                        || (ri == *pi
                            && addresses[*pi][*pv].as_ref().is_some_and(|s| !s.is_empty()))
                })
                .take(3)
                .enumerate()
            {
                let prior = &registers[pi];
                let predecessor = &prior.variables[pv];
                let evidence = format!(
                    "F3 candidate: adjacent documentation; label={label}, value_set={values}, spelling={spelling}, type_family={dtype}; score={score}"
                );
                let statements = if ri == pi && predecessor.pinned {
                    subjects
                        .iter()
                        .cloned()
                        .map(|subject| Statement::Set {
                            subject,
                            fields: Box::new(SetFields {
                                variable: Some(predecessor.slug.clone()),
                                ..SetFields::default()
                            }),
                            evidence: evidence.clone(),
                            caveat: Some(
                                "Review continuation evidence before assigning identity".into(),
                            ),
                        })
                        .collect::<Vec<_>>()
                } else {
                    let object = if predecessor.pinned {
                        format!(
                            "{}/{}/{}",
                            prior.register.provider, prior.public, predecessor.slug
                        )
                    } else if ri == pi {
                        let Some(address) =
                            addresses[pi][pv].as_ref().and_then(|items| items.first())
                        else {
                            continue;
                        };
                        address.to_string()
                    } else {
                        continue;
                    };
                    vec![Statement::Link {
                        subject: subjects[0].clone(),
                        relation: Relation::Continues,
                        object,
                        at: None,
                        evidence: evidence.clone(),
                        caveat: Some("Review continuation evidence before linking".into()),
                    }]
                };
                let proposed = statements
                    .iter()
                    .map(statement_toml)
                    .collect::<Result<BTreeSet<_>, _>>()?
                    .into_iter()
                    .collect::<Vec<_>>()
                    .join("\n");
                findings.push((
                    ri,
                    Finding {
                        code: "F3:continuation",
                        severity: "review",
                        subject: Subject::Variable(vi),
                        period: Some((v.hull.0.clone(), v.hull.0.clone())),
                        message: format!("candidate {}: {evidence}\n{}", rank + 1, proposed),
                    },
                ));
            }
        }
    }
    for (ri, finding) in findings {
        registers[ri].findings.push(finding);
    }
    Ok(())
}

fn evidence_keys(v: &reg_resolve::Variable) -> BTreeSet<String> {
    let mut keys = BTreeSet::from([format!("spelling:{}", v.fold)]);
    if !v.label.is_empty() {
        keys.insert(format!("label:{}", reg_core::label_key(&v.label)));
    }
    for segment in &v.segments {
        if let Some(hash) = &segment.valueset {
            keys.insert(format!("values:{hash}"));
        }
    }
    keys
}
