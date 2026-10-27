//! Gaps (T5) and the cadence they are measured in.

use std::collections::{BTreeMap, BTreeSet};

use reg_core::{Interval, next_iso_day};

use crate::{
    Gap, ResolvedOccurrence, ResolvedRelease, ResolvedTable, Variable, covers, pieces, placed, span,
};

/// A table's cadence in months: the greatest common step between its primary releases'
/// starts, when every start is a whole number of months after the one before on the
/// same day of the month (or month end). `None` with fewer than two starts.
fn cadence(releases: &[Interval]) -> Option<u32> {
    let starts: BTreeSet<&str> = releases.iter().map(|r| r.0.as_str()).collect();
    let month = |s: &str| -> Option<u32> {
        Some(s.get(..4)?.parse::<u32>().ok()? * 12 + s.get(5..7)?.parse::<u32>().ok()?)
    };
    let mut step: Option<u32> = None;
    for (a, b) in starts.iter().zip(starts.iter().skip(1)) {
        let months = month(b)? - month(a)?;
        if months == 0 || reg_core::add_iso_months(a, months).as_deref() != Some(*b) {
            return None;
        }
        step = Some(step.map_or(months, |g| gcd(g, months)));
    }
    step
}

/// A cadence name's step in months; `None` for `day` and `irregular`.
fn months(cadence: &str) -> Option<u32> {
    CADENCES
        .iter()
        .find(|(n, _)| *n == cadence)
        .map(|&(_, m)| m)
}

/// The cadence name of a step in months; a step no name fits is `irregular`.
fn cadence_name(step: u32) -> &'static str {
    CADENCES
        .iter()
        .find(|&&(_, m)| m == step)
        .map_or("irregular", |&(n, _)| n)
}

/// The named cadences and their steps in months.
const CADENCES: [(&str, u32); 4] = [("year", 12), ("half", 6), ("quarter", 3), ("month", 1)];

/// Each span widened to cover at least its cadence step from its start.
fn stretch(step: Option<u32>, spans: Vec<Interval>) -> Vec<Interval> {
    let Some(step) = step else {
        return spans;
    };
    spans
        .into_iter()
        .map(|(from, to)| {
            let end = reg_core::add_iso_months(&from, step)
                .map_or_else(|| to.clone(), |next| reg_core::prev_iso_day(&next));
            let to = to.max(end);
            (from, to)
        })
        .collect()
}

fn gcd(a: u32, b: u32) -> u32 {
    if b == 0 { a } else { gcd(b, a % b) }
}

/// The smallest interval holding every one of `spans`.
fn hull(spans: &[Interval]) -> Option<Interval> {
    let from = spans.iter().map(|s| &s.0).min()?;
    let to = spans.iter().map(|s| &s.1).max()?;
    Some((from.clone(), to.clone()))
}

/// T5 in the tables with placed releases: the days between a table's used columns and
/// primary releases that no used column covers (under a release with no columns, or no
/// release), and each variable's days inside its hull there that a release covers
/// without its column. Where the table has a cadence, every column, release and marker
/// covers its whole cadence step, so a gap is a missing step.
#[allow(clippy::too_many_lines)]
pub(crate) fn gaps(
    tables: &mut [ResolvedTable],
    releases: &[ResolvedRelease],
    variables: &[Variable],
    resolved: &[ResolvedOccurrence],
) -> Vec<Gap> {
    let mut delivered: BTreeMap<usize, Vec<Interval>> = BTreeMap::new();
    for r in releases {
        if r.primary == Some(true) {
            delivered
                .entry(r.table)
                .or_default()
                .extend(placed(&r.release));
        }
    }
    // A stated cadence stands; otherwise the inferred one is recorded on the table.
    let steps: BTreeMap<usize, u32> = delivered
        .iter()
        .filter_map(|(&t, r)| {
            let step = if let Some(stated) = tables[t].table.cadence.as_deref() {
                months(stated)?
            } else {
                let step = cadence(r)?;
                tables[t].table.cadence = Some(cadence_name(step).to_owned());
                step
            };
            Some((t, step))
        })
        .collect();
    let stretch = |t: usize, spans: Vec<Interval>| stretch(steps.get(&t).copied(), spans);
    let delivered: BTreeMap<usize, Vec<Interval>> = delivered
        .into_iter()
        .map(|(t, r)| (t, stretch(t, r)))
        .collect();
    let mut out = Vec::new();
    for (&t, releases) in &delivered {
        // The table's days between its used columns and primary releases that no used
        // column covers. The hull is theirs, not the table's bound: a bound may be open on
        // a side, while a placed release is bounded.
        let covered = stretch(
            t,
            resolved
                .iter()
                .filter(|r| r.table == t && r.status == "used")
                .map(|r| span(r).clone())
                .collect(),
        );
        let Some(hull) = hull(&[covered.as_slice(), releases].concat()) else {
            continue;
        };
        let mut all = vec![hull.clone()];
        all.extend(covered.iter().cloned());
        all.extend(releases.iter().cloned());
        let holes = runs(pieces(&all).into_iter().filter_map(|piece| {
            if !covers(&hull, &piece) || covered.iter().any(|c| covers(c, &piece)) {
                return None;
            }
            let reason = if releases.iter().any(|r| covers(r, &piece)) {
                "release_without_column"
            } else {
                "no_release"
            };
            Some((piece, reason))
        }));
        out.extend(holes.into_iter().map(|(period, reason)| Gap {
            variable: None,
            table: t,
            period,
            reason,
        }));
    }
    let mut marked: BTreeMap<(usize, usize), Vec<Interval>> = BTreeMap::new();
    for r in resolved {
        if r.status == "gap_marker"
            && let (Some(v), Some(p)) = (r.variable, &r.period)
        {
            marked.entry((v, r.table)).or_default().push(p.clone());
        }
    }
    for (v, variable) in variables.iter().enumerate() {
        let in_tables: BTreeSet<usize> = variable.segments.iter().map(|s| s.table).collect();
        for t in in_tables {
            let Some(delivered) = delivered.get(&t) else {
                continue;
            };
            let segments = stretch(
                t,
                variable
                    .segments
                    .iter()
                    .filter(|s| s.table == t)
                    .map(|s| s.period.clone())
                    .collect(),
            );
            let hull = hull(&segments).expect("a segment");
            let marks = stretch(t, marked.get(&(v, t)).cloned().unwrap_or_default());
            let mut all = vec![hull.clone()];
            all.extend(segments.iter().cloned());
            all.extend(delivered.iter().cloned());
            all.extend(marks.iter().cloned());
            let runs = runs(pieces(&all).into_iter().filter_map(|piece| {
                if !covers(&hull, &piece) || segments.iter().any(|s| covers(s, &piece)) {
                    return None;
                }
                // Days no release covers are the table's gap, not the variable's.
                if marks.iter().any(|m| covers(m, &piece)) {
                    Some((piece, "documented_without_column"))
                } else if delivered.iter().any(|d| covers(d, &piece)) {
                    Some((piece, "release_without_column"))
                } else {
                    None
                }
            }));
            out.extend(runs.into_iter().map(|(period, reason)| Gap {
                variable: Some(v),
                table: t,
                period,
                reason,
            }));
        }
    }
    out
}

/// Ascending gap pieces joined into runs of one reason.
fn runs(
    pieces: impl IntoIterator<Item = (Interval, &'static str)>,
) -> Vec<(Interval, &'static str)> {
    let mut out: Vec<(Interval, &'static str)> = Vec::new();
    for (piece, reason) in pieces {
        match out.last_mut() {
            Some((p, r)) if *r == reason && next_iso_day(&p.1) == piece.0 => p.1 = piece.1,
            _ => out.push((piece, reason)),
        }
    }
    out
}
