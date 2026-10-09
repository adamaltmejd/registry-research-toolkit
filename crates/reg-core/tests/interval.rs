//! The interval algebra (the testing policy's algebra exception): frozen Python's
//! answers on hand-picked inputs (`tests/interval/golden.json`), plus one seeded
//! property loop.

mod common;

use std::fs;
use std::path::PathBuf;

use common::Rng;
use reg_core::{Interval, Period, gaps, intersect, merge, next_iso_day, overlap, render};
use serde_json::{Value, json};

fn pair(v: &Value) -> Interval {
    let day = |i: usize| v[i].as_str().expect("an ISO date").to_owned();
    (day(0), day(1))
}

fn list(v: &Value) -> Vec<Interval> {
    v.as_array().expect("a list").iter().map(pair).collect()
}

/// Each golden case is frozen Python's `reg_meta.inventory` answer (`_intersect`,
/// `_merge`, `_overlap`, `_render`) or `reg_meta.order` answer (`_gaps`). Fails when
/// the algebra stops joining day-adjacent intervals (a synthesized non-leap `02-29`
/// end counting as February's end), keeps a containment or an empty intersection,
/// renders an interval as another spelling than the coarsest token or year-ended
/// range, or leaves a phantom gap or misses a real one.
///
/// To check the golden against frozen Python, from the repository root:
/// `uv run python -c 'import json; from reg_meta.inventory import _intersect, _merge,
/// _overlap, _render; from reg_meta.order import _gaps; ops = {"intersect": lambda
/// c: _intersect(c["a"], c["b"]), "merge": lambda c: _merge(c["in"]), "overlap":
/// lambda c: _overlap(c["a"], c["b"]), "render": lambda c: _render(c["in"]), "gaps":
/// lambda c: _gaps(tuple(map(tuple, c["whole"])), list(map(tuple,
/// c["covered"])))}; cases =
/// json.load(open("crates/reg-core/tests/interval/golden.json")); print([c for c in
/// cases if json.loads(json.dumps(ops[c["op"]](c))) != c["out"]])'` prints `[]`.
/// Python renders a non-leap February as `YYYY-02-01..YYYY-02-28`, so no case holds
/// one (the Rust-only fix of `period_token_for_bounds`, pinned in `tests/grammar.rs`).
#[test]
fn golden() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/interval/golden.json");
    let cases: Vec<Value> = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
    for case in &cases {
        let actual = match case["op"].as_str().unwrap() {
            "intersect" => json!(intersect(&pair(&case["a"]), &pair(&case["b"]))),
            "merge" => json!(merge(list(&case["in"]))),
            "overlap" => json!(overlap(&list(&case["a"]), &list(&case["b"]))),
            "render" => json!(render(&list(&case["in"]))),
            "gaps" => json!(gaps(&list(&case["whole"]), list(&case["covered"]))),
            op => panic!("unknown op {op}"),
        };
        assert_eq!(actual, case["out"], "{case}");
    }
}

/// Every day from 2018 through 2021, in order.
fn days() -> Vec<String> {
    let mut days = vec!["2018-01-01".to_owned()];
    while days.last().unwrap() != "2021-12-31" {
        days.push(next_iso_day(days.last().unwrap()));
    }
    days
}

fn random_intervals(rng: &mut Rng, days: &[String]) -> Vec<Interval> {
    let last = u16::try_from(days.len() - 1).unwrap();
    (0..rng.range(0, 4))
        .map(|_| {
            let lo = rng.range(0, last);
            // Short and long intervals, so adjacency and containment both occur.
            let span = if rng.range(0, 1) == 0 { 40 } else { 800 };
            let hi = lo.saturating_add(rng.range(0, span));
            let (lo, hi) = (usize::from(lo), usize::from(hi.min(last)));
            (days[lo].clone(), days[hi].clone())
        })
        .collect()
}

fn covers(intervals: &[Interval], day: &str) -> bool {
    intervals
        .iter()
        .any(|(lo, hi)| lo.as_str() <= day && day <= hi.as_str())
}

/// Ascending, each interval ordered, and no two touching (a day apart or closer).
fn canonical(intervals: &[Interval]) -> bool {
    intervals.iter().all(|(lo, hi)| lo <= hi)
        && intervals.windows(2).all(|w| next_iso_day(&w[0].1) < w[1].0)
}

// Fails if `merge` loses, adds or leaves touching days, is not idempotent, if
// `overlap` or `intersect` keeps a day one side lacks or drops a shared one, if `gaps`
// keeps a covered day or drops an uncovered one, or if `render` spells a merged list as anything that does not parse back, piece by piece
// through the period grammar, to exactly its intervals.
#[test]
fn algebra_properties() {
    let days = days();
    let mut rng = Rng(0x1A7E_57A1);
    for _ in 0..2_000 {
        let (a, b) = (
            random_intervals(&mut rng, &days),
            random_intervals(&mut rng, &days),
        );
        let (ma, mb) = (merge(a.clone()), merge(b.clone()));
        assert!(canonical(&ma), "{a:?} merged to {ma:?}");
        assert_eq!(merge(ma.clone()), ma);
        let shared = overlap(&ma, &mb);
        let missing = gaps(&ma, b.clone());
        assert!(canonical(&merge(shared.clone())), "{shared:?}");
        for day in &days {
            assert_eq!(covers(&a, day), covers(&ma, day), "{day} in {a:?}");
            assert_eq!(
                covers(&shared, day),
                covers(&ma, day) && covers(&mb, day),
                "{day}: {ma:?} and {mb:?}"
            );
            assert_eq!(
                covers(&missing, day),
                covers(&ma, day) && !covers(&mb, day),
                "{day}: {ma:?} less {b:?}"
            );
        }
        if let (Some(x), Some(y)) = (a.first(), b.first()) {
            assert_eq!(intersect(x, y), intersect(y, x));
        }
        let rendered = render(&ma);
        let parsed: Vec<Interval> = if ma.is_empty() {
            Vec::new()
        } else {
            rendered
                .split(',')
                .map(|piece| piece.parse::<Period>().expect(piece).iso_bounds())
                .collect()
        };
        assert_eq!(parsed, ma, "{rendered}");
    }
}
