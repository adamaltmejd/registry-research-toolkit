//! Neighbouring owners (F2): one column several variables claim in one table and window
//! goes to the variable that holds the column alone in the nearest releases before and
//! after: the nearest sole holder on each side, across gaps (crates/DESIGN.md,
//! "Identity"; maintainer 2026-11-14). The losing listings are
//! duplicates and are shadowed. A run with no sole holder on either side, two different
//! ones, or a holder that does not claim the whole run stays ambiguous. The rule never
//! shadows a curated variable assignment, and skips a column in a table when any used
//! claimant there is marked `long_format`.
//!
//! In a table with concurrent releases a run is contested only among the claims of one
//! release (and claims of no release, which are in every release their days meet), as
//! ambiguity is (crates/DESIGN.md, "Concurrent releases"); the neighbours are still read
//! from the column's whole timeline.

use std::collections::{BTreeMap, BTreeSet};

use reg_core::{Interval, fold_identity, next_iso_day};

use crate::{
    Deliveries, Finding, ResolvedOccurrence, ResolvedTable, Subject, covers, pieces, span,
};

/// Shadow the losing listings of every contested run a neighbouring owner settles.
/// `groups` are the variables' member occurrences, in variable order. Returns the
/// shadowed listings.
pub(crate) fn apply(
    resolved: &mut [ResolvedOccurrence],
    groups: &[&Vec<usize>],
    pinned: &BTreeSet<usize>,
    tables: &[ResolvedTable],
    delivered: &Deliveries,
    findings: &mut Vec<Finding>,
) -> BTreeSet<usize> {
    // (table, fold) -> the used occurrences claiming it, with their group.
    let mut columns: BTreeMap<(usize, String), Vec<(usize, usize)>> = BTreeMap::new();
    for (g, members) in groups.iter().enumerate() {
        for &i in *members {
            let r = &resolved[i];
            if r.status == "used" && !r.occurrence.column.is_empty() {
                columns
                    .entry((r.table, fold_identity(&r.occurrence.column)))
                    .or_default()
                    .push((g, i));
            }
        }
    }
    let mut lost = BTreeSet::new();
    let mut settled = BTreeSet::new();
    for ((t, fold), claims) in &columns {
        if claims
            .iter()
            .map(|&(g, _)| g)
            .collect::<BTreeSet<_>>()
            .len()
            < 2
            // A long-format column (a code column picks each row's variable) has no
            // owner among its claimants.
            || claims
                .iter()
                .any(|&(_, i)| resolved[i].occurrence.long_format == Some(true))
        {
            continue;
        }
        let neighbours = holder_runs(resolved, claims);
        let lane_of: Vec<Option<usize>> = claims
            .iter()
            .map(|&(_, i)| delivered.of(tables, &resolved[i]))
            .collect();
        let lanes: BTreeSet<usize> = lane_of.iter().flatten().copied().collect();
        // Each release's claims with those of no release; one set where none has a release.
        let sets: Vec<Vec<(usize, usize)>> = if lanes.is_empty() {
            vec![claims.clone()]
        } else {
            lanes
                .iter()
                .map(|&lane| {
                    claims
                        .iter()
                        .zip(&lane_of)
                        .filter(|(_, l)| l.is_none_or(|l| l == lane))
                        .map(|(c, _)| *c)
                        .collect()
                })
                .collect()
        };
        for set in &sets {
            let column = Column {
                table: *t,
                fold,
                claims: set,
                neighbours: &neighbours,
            };
            column.settle(resolved, pinned, tables, findings, &mut lost, &mut settled);
        }
    }
    for &i in &lost {
        resolved[i].status = "shadowed";
    }
    lost
}

/// One column's claims within one release, and the whole column's runs for neighbours.
struct Column<'a> {
    table: usize,
    fold: &'a str,
    claims: &'a [(usize, usize)],
    neighbours: &'a [(Interval, BTreeSet<usize>)],
}

impl Column<'_> {
    fn settle(
        &self,
        resolved: &[ResolvedOccurrence],
        pinned: &BTreeSet<usize>,
        tables: &[ResolvedTable],
        findings: &mut Vec<Finding>,
        lost: &mut BTreeSet<usize>,
        settled: &mut BTreeSet<(usize, Interval)>,
    ) {
        let (fold, table) = (self.fold, &tables[self.table].table.table);
        let runs = holder_runs(resolved, self.claims);
        let sole =
            |(_, h): &(Interval, BTreeSet<usize>)| (h.len() == 1).then(|| *h.first().expect("one"));
        let mut k = 0;
        while k < runs.len() {
            if runs[k].1.len() < 2 {
                k += 1;
                continue;
            }
            let mut end = k + 1;
            while end < runs.len()
                && runs[end].1.len() > 1
                && next_iso_day(&runs[end - 1].0.1) == runs[end].0.0
            {
                end += 1;
            }
            let run: Interval = (runs[k].0.0.clone(), runs[end - 1].0.1.clone());
            let start = k;
            k = end;
            let before = self
                .neighbours
                .iter()
                .rev()
                .filter(|(p, _)| p.1 < run.0)
                .find_map(sole);
            let after = self
                .neighbours
                .iter()
                .filter(|(p, _)| p.0 > run.1)
                .find_map(sole);
            let owner = match (before, after) {
                (Some(a), Some(b)) if a != b => None,
                (a, b) => a.or(b),
            };
            let Some(owner) = owner else { continue };
            if !runs[start..end].iter().all(|(_, h)| h.contains(&owner)) {
                continue;
            }
            let losing: Vec<usize> = self
                .claims
                .iter()
                .filter(|&&(g, i)| {
                    g != owner && reg_core::intersect(span(&resolved[i]), &run).is_some()
                })
                .map(|&(_, i)| i)
                .collect();
            // A curated assignment is final; say which pin keeps the run ambiguous.
            if let Some(&i) = losing.iter().find(|&i| pinned.contains(i)) {
                if settled.insert((i, run.clone())) {
                    findings.push(Finding {
                        code: "F2:pinned-claim",
                        severity: "review",
                        subject: Subject::Occurrence(i),
                        period: Some(run.clone()),
                        message: format!(
                            "{fold} in table {table:?}: the neighbouring releases would give the \
                             column to another variable, but curation assigns this listing"
                        ),
                    });
                }
                continue;
            }
            // A losing listing that reaches past the run would take its sole days too.
            if losing.iter().all(|&i| covers(&run, span(&resolved[i])))
                && let Some(&first) = losing.first()
            {
                lost.extend(losing);
                if settled.insert((first, run.clone())) {
                    findings.push(Finding {
                        code: "F2:neighbour-owner",
                        severity: "notice",
                        subject: Subject::Occurrence(first),
                        period: Some(run.clone()),
                        message: format!(
                            "{fold} in table {table:?} is documented under several variables; the \
                             one that holds it in the neighbouring releases keeps it"
                        ),
                    });
                }
            }
        }
    }
}

/// The column's elementary pieces with the groups holding each; adjacent pieces with one
/// holder set merge, a gap breaks a run.
fn holder_runs(
    resolved: &[ResolvedOccurrence],
    claims: &[(usize, usize)],
) -> Vec<(Interval, BTreeSet<usize>)> {
    let spans: Vec<Interval> = claims
        .iter()
        .map(|&(_, i)| span(&resolved[i]).clone())
        .collect();
    let mut runs: Vec<(Interval, BTreeSet<usize>)> = Vec::new();
    for piece in pieces(&spans) {
        let holders: BTreeSet<usize> = claims
            .iter()
            .filter(|&&(_, i)| covers(span(&resolved[i]), &piece))
            .map(|&(g, _)| g)
            .collect();
        match runs.last_mut() {
            Some((p, h)) if *h == holders && next_iso_day(&p.1) == piece.0 => p.1 = piece.1,
            _ => runs.push((piece, holders)),
        }
    }
    runs
}
