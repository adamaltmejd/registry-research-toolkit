//! V1 ranks inventory evidence without making classification bindings.

use std::collections::BTreeMap;

use reg_frame::ValueSet;
use reg_frame::statement::{SetFields, Statement};
use reg_resolve::curation::apply::Classification;
use reg_resolve::{Finding, Resolved, Subject};

struct Candidate {
    reference: String,
    exact: bool,
    shared: usize,
    union: usize,
    labels: usize,
}

fn rank(
    set: &ValueSet,
    classes: &[Classification],
    sets: &BTreeMap<&str, &ValueSet>,
    by_code: &BTreeMap<&str, Vec<(usize, Option<&str>)>>,
) -> Vec<Candidate> {
    let mut agreement: BTreeMap<usize, (usize, usize)> = BTreeMap::new();
    for item in &set.items {
        for &(class, label) in by_code.get(item.code.as_str()).into_iter().flatten() {
            let counts = agreement.entry(class).or_default();
            counts.0 += 1;
            counts.1 += usize::from(label.is_some() && label == item.label.as_deref());
        }
    }
    let mut candidates: Vec<_> = agreement
        .into_iter()
        .map(|(index, (shared, labels))| {
            let class = &classes[index];
            Candidate {
                reference: format!("class/{}/{}", class.family, class.version),
                exact: set.hash == class.valueset,
                shared,
                union: set.items.len() + sets[class.valueset.as_str()].items.len() - shared,
                labels,
            }
        })
        .collect();
    candidates.sort_by(|a, b| {
        b.exact
            .cmp(&a.exact)
            .then_with(|| (b.shared * a.union).cmp(&(a.shared * b.union)))
            .then_with(|| (b.labels * a.shared).cmp(&(a.labels * b.shared)))
            .then_with(|| a.reference.cmp(&b.reference))
    });
    candidates.truncate(3);
    candidates
}

pub(super) fn generate(
    registers: &mut [Resolved],
    classes: &[Classification],
    sets: &BTreeMap<&str, &ValueSet>,
) -> Result<(), String> {
    let mut by_code: BTreeMap<&str, Vec<(usize, Option<&str>)>> = BTreeMap::new();
    for (index, class) in classes.iter().enumerate() {
        if let Some(set) = sets.get(class.valueset.as_str()) {
            for item in &set.items {
                by_code
                    .entry(&item.code)
                    .or_default()
                    .push((index, item.label.as_deref()));
            }
        }
    }
    let mut ranked: BTreeMap<&str, Vec<Candidate>> = BTreeMap::new();
    for register in registers {
        for (index, occurrence) in register.occurrences.iter().enumerate() {
            let row = &occurrence.occurrence;
            if occurrence.status != "used"
                || row.classification.is_some()
                || row.coded == Some(false)
            {
                continue;
            }
            let Some(set) = row.valueset.as_deref().and_then(|hash| sets.get(hash)) else {
                continue;
            };
            let Some(subject) = super::occurrence_address(register, occurrence) else {
                continue;
            };
            let candidates = ranked
                .entry(&set.hash)
                .or_insert_with(|| rank(set, classes, sets, &by_code));
            if candidates.is_empty() {
                continue;
            }
            let mut message = String::from(
                "Classification-version proposals; review applicability before curation.",
            );
            for (rank, candidate) in candidates.iter().enumerate() {
                let evidence = format!(
                    "V1 candidate {}; exact value set: {}; shared codes: {}/{} union; matching nonblank labels: {}/{} shared; source code-system: {:?}; source value-set names: {:?}",
                    rank + 1,
                    candidate.exact,
                    candidate.shared,
                    candidate.union,
                    candidate.labels,
                    candidate.shared,
                    row.code_system,
                    set.names,
                );
                let statement = Statement::Set {
                    subject: subject.clone(),
                    fields: Box::new(SetFields {
                        classification: Some(candidate.reference.clone()),
                        ..SetFields::default()
                    }),
                    evidence: evidence.clone(),
                    caveat: Some(
                        "Proposal only; inventory existence does not establish applicability."
                            .into(),
                    ),
                };
                message.push_str("\n\n");
                message.push_str(&evidence);
                message.push('\n');
                message.push_str(&super::statement_toml(&statement)?);
            }
            register.findings.push(Finding {
                code: "V1:classification-version",
                severity: "review",
                subject: Subject::Occurrence(index),
                period: occurrence.period.clone(),
                message,
            });
        }
    }
    Ok(())
}
