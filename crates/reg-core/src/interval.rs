//! The interval algebra over inclusive ISO date intervals (`RUST_RUNTIME_SPEC.md`
//! package 3e.2): a project period, a state's
//! window, an availability clip and a co-delivery all expand, intersect, merge and
//! render through it, so they never disagree about bounds or spelling.
//!
//! Bounds compare as strings: `YYYY-MM-DD` orders chronologically, the open-ended
//! `9999-12-31` included. The oracle is `tests/interval/`.

use crate::{next_iso_day, period_token_for_bounds, prev_iso_day};

/// An inclusive `(lo, hi)` ISO date interval.
pub type Interval = (String, String);

/// The days `a` and `b` share, if any.
#[must_use]
pub fn intersect(a: &Interval, b: &Interval) -> Option<Interval> {
    let lo = a.0.as_str().max(b.0.as_str());
    let hi = a.1.as_str().min(b.1.as_str());
    (lo <= hi).then(|| (lo.to_owned(), hi.to_owned()))
}

/// `intervals` sorted and coalesced: overlapping and day-adjacent intervals join
/// (`..2018-12-31` and `2019-01-01..` are one window).
#[must_use]
pub fn merge(mut intervals: Vec<Interval>) -> Vec<Interval> {
    intervals.sort();
    let mut merged: Vec<Interval> = Vec::new();
    for (lo, hi) in intervals {
        match merged.last_mut() {
            Some(last) if lo <= next_iso_day(&last.1) => {
                if hi > last.1 {
                    last.1 = hi;
                }
            }
            _ => merged.push((lo, hi)),
        }
    }
    merged
}

/// The parts of `whole` (ascending, disjoint) that `covered` does not reach, the
/// order coverage gate's output. `covered` is merged first, so day-adjacent
/// contributions leave no phantom gap, and coverage reaching an interval's end
/// completes it without day arithmetic (the open-ended `9999-12-31` has no
/// successor).
#[must_use]
pub fn gaps(whole: &[Interval], covered: Vec<Interval>) -> Vec<Interval> {
    let covered = merge(covered);
    let mut out = Vec::new();
    for (lo, hi) in whole {
        let mut cursor = lo.clone();
        let mut complete = false;
        for (c_lo, c_hi) in &covered {
            if *c_hi < cursor || c_lo > hi {
                continue;
            }
            if *c_lo > cursor {
                out.push((cursor.clone(), prev_iso_day(c_lo)));
            }
            if c_hi >= hi {
                complete = true;
                break;
            }
            cursor = next_iso_day(c_hi);
        }
        if !complete && cursor <= *hi {
            out.push((cursor, hi.clone()));
        }
    }
    out
}

/// The intervals two ascending, disjoint lists share, ascending.
#[must_use]
pub fn overlap(a: &[Interval], b: &[Interval]) -> Vec<Interval> {
    a.iter()
        .flat_map(|x| b.iter().filter_map(|y| intersect(x, y)))
        .collect()
}

/// The period spelling of ascending, disjoint intervals, as a project period is
/// written: each interval as its coarsest token (`2019`, `2019-Q3`), else as a range
/// whose endpoints are a year when they fall on its first or last day
/// (`2019..2020-06-30`) and the ISO date otherwise; comma-joined.
#[must_use]
pub fn render(intervals: &[Interval]) -> String {
    intervals
        .iter()
        .map(|(lo, hi)| {
            let token = period_token_for_bounds(lo, hi);
            if !token.contains("..") {
                return token;
            }
            let start = if lo.ends_with("-01-01") { &lo[..4] } else { lo };
            let end = if hi.ends_with("-12-31") { &hi[..4] } else { hi };
            format!("{start}..{end}")
        })
        .collect::<Vec<_>>()
        .join(",")
}
