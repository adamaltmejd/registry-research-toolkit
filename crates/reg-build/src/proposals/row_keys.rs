//! K1: row-key coverage of a data table. A row key is curated per table and variable
//! (`<table>: var:<id>`), never inferred: `identifier` is evidence for review only.
//!
//! - `K1:row-key`: identifier columns and no row key. When exactly one identifier is a
//!   unit id by exact name, the finding proposes it.
//! - `K1:row-key-gap`: a row key, but periods where the table has identifiers and no key.
//! - `K1:row-key-partial`: a segment of a key variable that is not a key. Selection
//!   covers every occurrence of the variable in the table, so this should never fire.

use reg_core::{gaps, merge, render};
use reg_frame::address::Address;
use reg_frame::statement::{SetFields, Statement};
use reg_resolve::{Finding, Resolved, Segment, Subject, Variable};

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
            let keyed: Vec<usize> = (0..r.variables.len())
                .filter(|&v| in_table(&r.variables[v], t).any(|s| s.row_key))
                .collect();
            partial(r, t, &keyed, &mut findings);
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
            if !keyed.is_empty() {
                let periods = |vars: &[usize], keep: fn(&Segment) -> bool| {
                    merge(
                        vars.iter()
                            .flat_map(|&v| in_table(&r.variables[v], t))
                            .filter(|s| keep(s))
                            .map(|s| s.period.clone())
                            .collect(),
                    )
                };
                let unkeyed = gaps(
                    &periods(&identifiers, |s| s.identifier == Some(true)),
                    periods(&keyed, |s| s.row_key),
                );
                if !unkeyed.is_empty() {
                    findings.push(Finding {
                        code: "K1:row-key-gap",
                        severity: "review",
                        subject: Subject::Table(t),
                        period: None,
                        message: format!(
                            "no row key over {}; identifiers: {names}",
                            render(&unkeyed)
                        ),
                    });
                }
                continue;
            }
            let units: Vec<usize> = identifiers
                .iter()
                .copied()
                .filter(|&v| UNIT_IDS.contains(&r.variables[v].slug.as_str()))
                .collect();
            let proposed = match units[..] {
                [v] => {
                    let statement = Statement::Set {
                        subject: Address::TableVariable {
                            table: table.slug.clone().unwrap_or_else(|| r.public.clone()),
                            variable: r.variables[v].slug.clone(),
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

fn partial(r: &Resolved, t: usize, keyed: &[usize], findings: &mut Vec<Finding>) {
    for &v in keyed {
        for (s, segment) in r.variables[v].segments.iter().enumerate() {
            if segment.table == t && !segment.row_key {
                findings.push(Finding {
                    code: "K1:row-key-partial",
                    severity: "review",
                    subject: Subject::Segment(v, s),
                    period: Some(segment.period.clone()),
                    message: format!(
                        "{} is a row key of this table but not under {}",
                        r.variables[v].slug, segment.spelling
                    ),
                });
            }
        }
    }
}

fn in_table(v: &Variable, t: usize) -> impl Iterator<Item = &Segment> {
    v.segments.iter().filter(move |s| s.table == t)
}
