//! Opt-in variable succession. Each hop reuses the ordinary binding checks on a
//! bounded request; successor tables contribute their own row keys and panel kinds.

use std::collections::BTreeSet;

use reg_core::project::Binding;
use reg_core::{Interval, IssueLevel, gaps, merge, overlap};
use rusqlite::params;

use super::window::{self, Outcome, Scope};
use super::{Issue, OrderClip, Serving, day, interval, period, table_ref};
use crate::Error;

pub(super) fn binding(scope: &Scope, b: usize, binding: &Binding) -> Result<Outcome, Error> {
    let mut out = Outcome::default();
    let mut current = binding.clone();
    current.follow_successor = None;
    let mut lower = scope.server.open.0;
    let until = day(&scope.requested.last().expect("requested days").1) + 1;
    let mut seen = BTreeSet::new();
    let mut first = true;
    loop {
        let (id, end): (i64, i64) = scope.conn.query_row(
            "SELECT id, end FROM variable WHERE pid = ?",
            [&current.variable],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if !seen.insert(id) || (!first && end <= lower) {
            out.findings.push(refusal(
                scope,
                b,
                &current.variable,
                "ambiguous_window",
                "The successor chain does not advance in time.",
            ));
            break;
        }
        // Edges point from successor to predecessor. Do not filter by provenance:
        // source and curated relations have the same authority here.
        let successors: Vec<(String, Option<i64>)> = scope
            .conn
            .prepare(
                "SELECT v.pid, r.at_start FROM relation r
             JOIN variable v ON v.id = r.subject_id
             WHERE r.kind = 'continues' AND r.subject_kind = 'variable'
               AND r.object_kind = 'variable' AND r.object_id = ?1
               AND (r.at_start IS NULL OR r.at_start < ?2)
             ORDER BY v.pid, r.at_start",
            )?
            .query_map(params![id, until], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?;
        let needs_next = end < until && !successors.is_empty();
        let stop = if needs_next { end } else { until };
        let requested = overlap(scope.requested, &[interval(lower, stop.max(lower + 1))]);
        if lower < stop && !requested.is_empty() {
            materialize(scope, b, &current, id, first, &requested, &mut out)?;
        }
        if !needs_next {
            break;
        }
        let [(successor, Some(at))] = successors.as_slice() else {
            let mut finding = refusal(
                scope,
                b,
                &current.variable,
                "variable_replaced",
                "Following requires exactly one successor with a stated start day; curate the succession.",
            );
            if successors.len() == 1 {
                finding.successor_fqid = Some(successors[0].0.clone());
            }
            out.findings.push(finding);
            break;
        };
        lower = end.max(*at).max(lower);
        current.variable.clone_from(successor);
        first = false;
        if lower >= until {
            break;
        }
    }
    finish(scope, b, binding, &mut out);
    Ok(out)
}

fn materialize(
    scope: &Scope,
    b: usize,
    current: &Binding,
    id: i64,
    first: bool,
    requested: &[Interval],
    out: &mut Outcome,
) -> Result<(), Error> {
    let serving = if first {
        scope
            .serving
            .iter()
            .filter_map(|s| {
                let requested = overlap(&s.requested, requested);
                (!requested.is_empty()).then(|| Serving {
                    table: s.table.clone(),
                    requested,
                })
            })
            .collect()
    } else {
        tables(
            scope,
            b,
            id,
            &current.variable,
            requested,
            &mut out.findings,
        )?
    };
    let chain: Vec<i64> = if first {
        scope.chain.to_vec()
    } else {
        serving.iter().map(|s| s.table.id).collect()
    };
    if !first && serving.is_empty() {
        out.findings.push(refusal(
            scope,
            b,
            &current.variable,
            "outside_availability",
            "The successor has no orderable column over the requested period.",
        ));
    } else {
        let part = Scope {
            serving: &serving,
            chain: &chain,
            requested,
            table: serving
                .first()
                .map_or(scope.table, |s| s.table.pid.as_str()),
            ..*scope
        };
        let checked = window::binding(&part, b, current)?;
        out.findings.extend(checked.findings);
        out.entries.extend(checked.entries);
    }
    if !first {
        out.serving.extend(serving);
    }
    Ok(())
}

fn finish(scope: &Scope, b: usize, binding: &Binding, out: &mut Outcome) {
    out.entries
        .sort_by_key(|e| (e.period.start.clone(), e.table.clone(), e.variable.clone()));
    let ordered = merge(
        out.entries
            .iter()
            .map(|e| {
                (
                    e.period
                        .start
                        .clone()
                        .unwrap_or_else(|| super::iso_day(scope.server.open.0)),
                    super::iso_day(e.period.end.as_deref().map_or(scope.server.open.1, day) - 1),
                )
            })
            .collect(),
    );
    // Ordinary per-hop checks already report local clips; this records the full
    // binding, including any gap between the predecessor's end and the edge's date.
    if !gaps(scope.requested, ordered.clone()).is_empty() {
        out.clip = Some(OrderClip {
            source: scope.source.name.clone(),
            variable: binding.variable.clone(),
            requested: window::periods(scope.server, scope.requested),
            ordered: window::periods(scope.server, &ordered),
        });
        // Reconstruct once so local and between-hop gaps have identical findings.
        out.findings.retain(|f| f.code != "period_clipped");
        for days in gaps(scope.requested, ordered) {
            let mut finding = refusal(
                scope,
                b,
                &binding.variable,
                "period_clipped",
                "This part of the requested period is unavailable and is not ordered.",
            );
            finding.level = IssueLevel::Info;
            finding.period = Some(period(scope.server, &days));
            out.findings.push(finding);
        }
    }
}

/// Use the source's explicit table chain when it contains the successor. Across
/// registers (or outside that chain), require a unique serving table on every day.
fn tables(
    scope: &Scope,
    b: usize,
    variable: i64,
    pid: &str,
    requested: &[Interval],
    findings: &mut Vec<Issue>,
) -> Result<Vec<Serving>, Error> {
    // Explicit-chain membership must not change when the requested period changes.
    let in_source = window::in_chain(scope, variable)?;
    let rows: Vec<(String, i64, i64)> = scope
        .conn
        .prepare(
            "SELECT t.pid, s.start, s.end FROM segment s JOIN tbl t ON t.id = s.table_id
         WHERE s.variable_id = ? ORDER BY t.pid, s.start",
        )?
        .query_map([variable], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?
        .collect::<rusqlite::Result<_>>()?;
    let mut serving: Vec<Serving> = Vec::new();
    for (table, start, end) in rows {
        let days = overlap(requested, &[interval(start, end)]);
        if days.is_empty() {
            continue;
        }
        if let Some(last) = serving.last_mut().filter(|s| s.table.pid == table) {
            last.requested.extend(days);
            last.requested = merge(std::mem::take(&mut last.requested));
        } else if let Ok(table) = table_ref(scope.conn, &table)? {
            serving.push(Serving {
                table,
                requested: days,
            });
        }
    }
    if in_source {
        serving.retain(|s| scope.chain.contains(&s.table.id));
        for s in &mut serving {
            s.requested = scope
                .serving
                .iter()
                .filter(|original| original.table.id == s.table.id)
                .flat_map(|original| overlap(&original.requested, &s.requested))
                .collect();
            s.requested = merge(std::mem::take(&mut s.requested));
        }
        serving.retain(|s| !s.requested.is_empty());
    }
    for (i, s) in serving.iter().enumerate() {
        if serving[..i]
            .iter()
            .any(|other| !overlap(&other.requested, &s.requested).is_empty())
        {
            findings.push(refusal(scope, b, pid, "ambiguous_window",
                "The successor is available in multiple tables on the same requested days; choose an explicit source."));
            return Ok(vec![]);
        }
    }
    // Withheld occurrences have no variable id. Uncovered days cannot be
    // distinguished safely from withheld successor data, so fail closed.
    let covered = merge(serving.iter().flat_map(|s| s.requested.clone()).collect());
    for days in gaps(requested, covered) {
        let mut finding = refusal(
            scope,
            b,
            pid,
            "outside_availability",
            "The successor is not available over this requested window; curation is required.",
        );
        finding.period = Some(period(scope.server, &days));
        findings.push(finding);
    }
    Ok(serving)
}

fn refusal(scope: &Scope, b: usize, variable: &str, code: &str, message: &str) -> Issue {
    Issue {
        level: IssueLevel::Error,
        code: code.to_owned(),
        path: format!("/sources/{}/bindings/{b}", scope.index),
        message: message.to_owned(),
        source: Some(scope.source.name.clone()),
        variable: Some(variable.to_owned()),
        table: Some(scope.table.to_owned()),
        period: None,
        successor_fqid: None,
    }
}
