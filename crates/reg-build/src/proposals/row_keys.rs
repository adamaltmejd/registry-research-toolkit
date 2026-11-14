//! K1: a data table with identifier columns and no curated row key. The finding lists
//! the identifiers; when exactly one of them is a unit id by exact name, it proposes
//! that column as the row key. `identifier` is evidence for the proposal only: a row
//! key exists only once curated.

use std::collections::BTreeSet;

use reg_frame::address::Address;
use reg_frame::statement::{SetFields, Statement};
use reg_resolve::{Finding, Resolved, Subject};

use super::statement_toml;

/// Variable slugs that name the unit a row describes (a person, firm or workplace).
/// Exact names only: a slug outside the list is never proposed.
const UNIT_IDS: &[&str] = &[
    "cfar-nummer",
    "eget-personnr",
    "iafid",
    "individnyckel",
    "lopnr",
    "lopnr-personnr",
    "lopnummer",
    "orgnr",
    "orgnr-12",
    "person-orgnr",
    "personnr",
    "personnummer-personnr-scb",
    "persid",
    "pnr",
];

pub(super) fn generate(registers: &mut [Resolved]) -> Result<(), String> {
    for r in registers.iter_mut() {
        let mut findings = Vec::new();
        for (t, table) in r.tables.iter().enumerate() {
            if table.table.role.as_deref() == Some("codelist") {
                continue;
            }
            if r.variables
                .iter()
                .any(|v| in_table(v, t).any(|s| s.row_key))
            {
                continue;
            }
            let identifiers: Vec<usize> = (0..r.variables.len())
                .filter(|&v| in_table(&r.variables[v], t).any(|s| s.identifier == Some(true)))
                .collect();
            if identifiers.is_empty() {
                continue;
            }
            let names = identifiers
                .iter()
                .map(|&v| r.variables[v].slug.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            let units: Vec<usize> = identifiers
                .iter()
                .copied()
                .filter(|&v| UNIT_IDS.contains(&r.variables[v].slug.as_str()))
                .collect();
            let proposed = match units[..] {
                [v] => {
                    let spellings: BTreeSet<String> = in_table(&r.variables[v], t)
                        .map(|s| reg_core::fold_identity(&s.spelling))
                        .collect();
                    let statement = Statement::Set {
                        subject: Address::Occurrence {
                            table: table.slug.clone().unwrap_or_else(|| r.public.clone()),
                            spellings: spellings.into_iter().collect(),
                            periods: None,
                            guards: Vec::new(),
                        },
                        fields: Box::new(SetFields {
                            row_key: Some(true),
                            ..SetFields::default()
                        }),
                        evidence: format!(
                            "K1: {} is the table's only unit id among its identifiers ({names})",
                            r.variables[v].slug
                        ),
                        caveat: None,
                    };
                    format!("\n{}", statement_toml(&statement)?)
                }
                _ => String::new(),
            };
            findings.push(Finding {
                code: "K1:row-key",
                severity: "review",
                subject: Subject::Table(t),
                period: None,
                message: format!("no curated row key; identifiers: {names}{proposed}"),
            });
        }
        r.findings.extend(findings);
    }
    Ok(())
}

fn in_table(v: &reg_resolve::Variable, t: usize) -> impl Iterator<Item = &reg_resolve::Segment> {
    v.segments.iter().filter(move |s| s.table == t)
}
