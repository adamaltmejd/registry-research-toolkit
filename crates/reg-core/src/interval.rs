//! The interval algebra over inclusive ISO date intervals: a project period, a state's
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

/// The smallest interval holding every one of `intervals`; `None` when there are none.
#[must_use]
pub fn hull<'a>(intervals: impl IntoIterator<Item = &'a Interval>) -> Option<Interval> {
    let mut intervals = intervals.into_iter();
    let (lo, hi) = intervals.next()?;
    let (lo, hi) = intervals.fold((lo, hi), |(a, b), (lo, hi)| (a.min(lo), b.max(hi)));
    Some((lo.clone(), hi.clone()))
}

/// The same algebra over half-open `[start, end)` day numbers, the form the catalog and
/// holdings store: touching spans join without day arithmetic.
pub mod days {
    /// A half-open `[start, end)` span of day numbers.
    pub type Span = (i64, i64);

    /// `spans` sorted and coalesced: overlapping and touching spans join, empty ones drop.
    #[must_use]
    pub fn merge(mut spans: Vec<Span>) -> Vec<Span> {
        spans.sort_unstable();
        let mut out: Vec<Span> = Vec::new();
        for (lo, hi) in spans {
            if lo >= hi {
                continue;
            }
            match out.last_mut() {
                Some(last) if lo <= last.1 => last.1 = last.1.max(hi),
                _ => out.push((lo, hi)),
            }
        }
        out
    }

    /// The days two lists share, merged.
    #[must_use]
    pub fn overlap(a: &[Span], b: &[Span]) -> Vec<Span> {
        merge(
            a.iter()
                .flat_map(|&(lo, hi)| b.iter().map(move |&(x, y)| (lo.max(x), hi.min(y))))
                // Drop empty pairs before collecting: fragmented lists would otherwise
                // allocate and sort every pair, not only the intersections.
                .filter(|&(lo, hi)| lo < hi)
                .collect(),
        )
    }

    /// The days of `whole` that `covered` (ascending by start) leaves out.
    #[must_use]
    pub fn minus(whole: &[Span], covered: &[Span]) -> Vec<Span> {
        let mut out = Vec::new();
        for &(mut lo, hi) in whole {
            for &(a, b) in covered {
                if b <= lo || a >= hi {
                    continue;
                }
                if a > lo {
                    out.push((lo, a));
                }
                lo = lo.max(b);
            }
            if lo < hi {
                out.push((lo, hi));
            }
        }
        out
    }

    /// Whether every span of `wanted` lies inside one span of `by`, merged.
    #[must_use]
    pub fn covered(wanted: &[Span], by: &[Span]) -> bool {
        let by = merge(by.to_vec());
        wanted
            .iter()
            .all(|(a, b)| by.iter().any(|(c, d)| c <= a && b <= d))
    }

    /// Release shadowing (crates/DESIGN.md, "Concurrent releases"): the days each of one
    /// variable's segments in one table serves for `lane`, aligned with `segments` (each a
    /// span and its release). With a lane (a release the source names, on days two
    /// releases share) that release's segments serve, a releaseless one where they do
    /// not reach and another release's not at all. Without one (an unshared day) every
    /// release's segments serve and a releaseless one gives way to them. Every reader of
    /// a concurrent table's segments for an order applies it: the order's columns and row
    /// keys, and reg-mock.
    #[must_use]
    pub fn shadow(segments: &[(Span, Option<&str>)], lane: Option<&str>) -> Vec<Vec<Span>> {
        let reads = |release: Option<&str>| lane.is_none() || release.is_none() || release == lane;
        let keyed = merge(
            segments
                .iter()
                .filter(|(_, release)| release.is_some() && reads(*release))
                .map(|(span, _)| *span)
                .collect(),
        );
        segments
            .iter()
            .map(|&(span, release)| match release {
                _ if !reads(release) => Vec::new(),
                Some(_) => vec![span],
                None => minus(&[span], &keyed),
            })
            .collect()
    }

    /// The days each of one variable's segments in one table serves a source over
    /// `requested`, aligned with `segments` and merged (crates/DESIGN.md, "Concurrent
    /// releases"). Days no two primaries share are read with no lane; each `shared` span
    /// (its days and the releases sharing them) is read once per release `named` among
    /// those sharing it, and serves nothing when none is (`release_ambiguous`). The
    /// order's `window::served` applies the same rule to its pieces; the SPA reads this.
    #[must_use]
    pub fn serve(
        segments: &[(Span, Option<&str>)],
        requested: &[Span],
        shared: &[(Span, Vec<&str>)],
        named: &[&str],
    ) -> Vec<Vec<Span>> {
        let mut out = vec![Vec::new(); segments.len()];
        let mut read = |region: &[Span], lane: Option<&str>| {
            let pieces: Vec<(usize, Span, Option<&str>)> = segments
                .iter()
                .enumerate()
                .flat_map(|(i, &(span, release))| {
                    overlap(&[span], region)
                        .into_iter()
                        .map(move |piece| (i, piece, release))
                })
                .collect();
            let spans: Vec<(Span, Option<&str>)> = pieces
                .iter()
                .map(|&(_, span, release)| (span, release))
                .collect();
            for ((i, _, _), kept) in pieces.iter().zip(shadow(&spans, lane)) {
                out[*i].extend(kept);
            }
        };
        let days: Vec<Span> = shared.iter().map(|(span, _)| *span).collect();
        read(&minus(&merge(requested.to_vec()), &merge(days)), None);
        for (span, sharing) in shared {
            let region = overlap(&[*span], requested);
            for name in sharing.iter().filter(|n| named.contains(n)) {
                read(&region, Some(name));
            }
        }
        out.into_iter().map(merge).collect()
    }

    /// The spans between consecutive distinct endpoints of `spans`, ascending,
    /// including those no span covers.
    #[must_use]
    pub fn slices(spans: impl IntoIterator<Item = Span>) -> Vec<Span> {
        let cuts: std::collections::BTreeSet<i64> =
            spans.into_iter().flat_map(|(lo, hi)| [lo, hi]).collect();
        let cuts: Vec<i64> = cuts.into_iter().collect();
        cuts.windows(2).map(|pair| (pair[0], pair[1])).collect()
    }
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
            // The year is what precedes the suffix: four digits for an ISO date, and
            // no byte slicing, so any string renders without a panic.
            let start = lo.trim_suffix("-01-01");
            let end = hi.trim_suffix("-12-31");
            format!("{start}..{end}")
        })
        .collect::<Vec<_>>()
        .join(",")
}
