//! Acknowledgements retain findings and record exactly what the curator reviewed.

use std::collections::BTreeMap;

use reg_frame::address::Address;
use reg_frame::statement::{Severity, Statement};
use serde_json::{Value, json};

use super::Premise;
use crate::{Resolved, Subject};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Acknowledgement {
    pub statement: String,
    pub caveat: Option<String>,
}

/// Match a finding on the selected entity. Occurrence selectors can acknowledge
/// segment findings only when every occurrence supporting that segment is selected.
///
/// # Errors
/// The finding is absent at the selected subject or its severity differs.
#[allow(clippy::too_many_lines)]
pub fn review(
    resolved: &Resolved,
    statement: &Statement,
    premise: &mut Premise,
) -> Result<Vec<usize>, String> {
    let Statement::Ack {
        subject,
        finding,
        severity,
        ..
    } = statement
    else {
        return Ok(Vec::new());
    };
    let selected = |index: usize| {
        let row = &resolved.occurrences[index].occurrence;
        premise.subjects.iter().any(|p| {
            let text = |field| p.fields.get(field).and_then(Value::as_str);
            (p.locator == row.locator || row.also.contains(&p.locator))
                && text("kind") == Some("occurrence")
                && text("provider") == Some(row.provider.as_str())
                && text("register") == Some(row.register.as_str())
                && text("table") == Some(row.table.as_str())
                && text("spelling") == Some(row.column.as_str())
                && text("key") == Some(row.source_key.as_str())
                && text("release") == row.release.as_deref()
                && text("from") == row.from.as_deref()
                && text("to") == row.to.as_deref()
        })
    };
    let mut snapshots = Vec::new();
    let mut indices = Vec::new();
    for (index, current) in resolved.findings.iter().enumerate() {
        if current.code != finding {
            continue;
        }
        let matched = match (&current.subject, subject) {
            (Subject::Variable(v) | Subject::Segment(v, _), Address::Variable(slug)) => {
                resolved.variables[*v].slug == *slug
            }
            (Subject::Occurrence(o), Address::Variable(slug)) => resolved.occurrences[*o]
                .variable
                .is_some_and(|v| resolved.variables[v].slug == *slug),
            (Subject::Release(r), Address::Release { table, handle }) => {
                resolved.releases[*r].release.release == *handle
                    && resolved.tables[resolved.releases[*r].table]
                        .slug
                        .as_deref()
                        .unwrap_or(&resolved.public)
                        == table
                    && premise
                        .subjects
                        .iter()
                        .any(|p| p.locator == resolved.releases[*r].release.locator)
            }
            (Subject::Occurrence(o), Address::Occurrence { .. }) => selected(*o),
            (Subject::Segment(v, s), Address::Occurrence { .. }) => {
                let members = resolved
                    .occurrences
                    .iter()
                    .enumerate()
                    .filter(|(_, o)| o.variable == Some(*v) && o.segment == Some(*s))
                    .map(|(i, _)| i)
                    .collect::<Vec<_>>();
                !members.is_empty() && members.into_iter().all(selected)
            }
            (Subject::Register, Address::Occurrence { .. })
                if current.code == "F5:documented-without-column" =>
            {
                let key = &resolved.finding_keys[&index];
                let members = resolved
                    .occurrences
                    .iter()
                    .enumerate()
                    .filter(|(_, o)| o.status == "gap_marker" && o.occurrence.source_key == *key)
                    .map(|(i, _)| i)
                    .collect::<Vec<_>>();
                !members.is_empty() && members.into_iter().all(selected)
            }
            (Subject::Register, Address::Variable(slug))
                if current.code == "F5:documented-without-column" =>
            {
                resolved
                    .variables
                    .iter()
                    .any(|v| v.slug == *slug && v.source_key == resolved.finding_keys[&index])
            }
            _ => false,
        };
        if !matched {
            continue;
        }
        let expected = match severity {
            Severity::Notice => "notice",
            Severity::Review => "review",
        };
        if current.severity != expected {
            return Err(format!(
                "ack-severity: finding {finding:?} has severity {}, not {expected}",
                current.severity
            ));
        }
        let rows = resolved
            .occurrences
            .iter()
            .enumerate()
            .filter(|(occurrence_index, o)| match current.subject {
                Subject::Variable(v) => o.variable == Some(v),
                Subject::Segment(v, s) => o.variable == Some(v) && o.segment == Some(s),
                Subject::Occurrence(i) => *occurrence_index == i,
                Subject::Release(r) => o.release == Some(r),
                Subject::Register => resolved
                    .finding_keys
                    .get(&index)
                    .is_none_or(|key| o.occurrence.source_key == *key),
            })
            .map(|(_, o)| json!({"occurrence":o.occurrence,"period":o.period,"status":o.status}))
            .collect::<Vec<_>>();
        snapshots.push(json!({"code":current.code,"severity":current.severity,"period":current.period,"message":current.message,"sources":rows}));
        indices.push(index);
    }
    if indices.is_empty() {
        return Err(format!(
            "stale-ack: finding {finding:?} is not present at {subject}"
        ));
    }
    snapshots.sort_by_key(reg_frame::canonical_line);
    // The finding is a premise, never a replacement source fact or an extra subject.
    if let Some(first) = premise.subjects.first_mut() {
        first
            .fields
            .insert("acknowledged_findings".into(), Value::Array(snapshots));
    }
    Ok(indices)
}

pub fn apply(resolved: &mut Resolved, acknowledgements: &BTreeMap<usize, Acknowledgement>) {
    resolved.acknowledgements.extend(acknowledgements.clone());
}
