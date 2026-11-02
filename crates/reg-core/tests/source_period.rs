//! Source-period wire shaping and interval algebra against the pre-archive golden.

use reg_core::project::SourcePeriod;
use reg_core::render;
use serde_json::{Value, json};

// Fails if the wire shaping (`from_wire`, `to_wire`), the one-period structural check,
// the days a source period requests, its year spans or their render change: a list
// member read as a range, a grammar year left a string, an unsorted list accepted,
// day-adjacent members left apart.
#[test]
fn source_period_matches_corpus() {
    let cases: Vec<Value> = include_str!("project/source_period.jsonl")
        .lines()
        .map(|line| serde_json::from_str(line).expect("a source period case"))
        .collect();
    let failures: Vec<String> = cases
        .iter()
        .enumerate()
        .filter_map(|(i, case)| {
            let period = &case["period"];
            let mut got = serde_json::Map::new();
            got.insert("period".into(), period.clone());
            if let Some(wire) = case.get("from_wire") {
                let shaped = SourcePeriod::from_wire(wire.as_str().expect("a wire string"));
                got.insert("from_wire".into(), json!(shaped));
            }
            let wire = SourcePeriod::from_shape(period).and_then(|p| p.to_wire());
            got.insert("wire".into(), json!(wire));
            match SourcePeriod::from_value(period).map(|p| (p.intervals(), p.year_spans())) {
                Ok((Ok(intervals), years)) => {
                    if let Some(days) = &intervals {
                        got.insert("render".into(), json!(render(days)));
                    }
                    got.insert("intervals".into(), json!(intervals));
                    got.insert("years".into(), json!(years));
                }
                Err(_) | Ok((Err(_), _)) => {
                    got.insert("error".into(), json!("invalid_period"));
                }
            }
            // `from_wire` is the wire that shapes into `period`.
            let mut expected = case.as_object().expect("a case object").clone();
            expected.remove("note");
            if let Some(wire) = expected.get_mut("from_wire") {
                *wire = period.clone();
            }
            let got = Value::Object(got);
            (got != Value::Object(expected)).then(|| format!("line {}: got {got}", i + 1))
        })
        .collect();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
