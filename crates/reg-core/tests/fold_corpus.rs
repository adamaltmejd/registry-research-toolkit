//! The folds against the corpus in `conformance/cases/folds/` (see its README).

use std::fs;
use std::path::PathBuf;

use serde_json::Value;

/// A generated file holds every scalar its fold changes; fewer lines means a broken
/// or truncated corpus, not a passing one.
const MIN_GENERATED_CASES: usize = 3000;

struct Case {
    line: usize,
    op: String,
    input: String,
    expected: Option<String>,
}

fn read_cases(file: &str, default_op: Option<&str>) -> Vec<Case> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../conformance/cases/folds")
        .join(file);
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    text.lines()
        .enumerate()
        .map(|(i, line)| {
            let value: Value =
                serde_json::from_str(line).unwrap_or_else(|e| panic!("{file}:{}: {e}", i + 1));
            let field = |name: &str| value.get(name).unwrap_or(&Value::Null);
            let op = match (field("op").as_str(), default_op) {
                (Some(op), _) | (None, Some(op)) => op.to_owned(),
                (None, None) => panic!("{file}:{}: no op", i + 1),
            };
            let input = field("in")
                .as_str()
                .unwrap_or_else(|| panic!("{file}:{}: `in` is not a string", i + 1))
                .to_owned();
            let expected = match field("out") {
                Value::Null => None,
                Value::String(s) => Some(s.clone()),
                other => panic!("{file}:{}: `out` is {other}", i + 1),
            };
            Case {
                line: i + 1,
                op,
                input,
                expected,
            }
        })
        .collect()
}

fn apply(op: &str, input: &str) -> Option<String> {
    match op {
        "fold_identity" => Some(reg_core::fold_identity(input)),
        "fold_search" => Some(reg_core::fold_search(input)),
        "normalized_search_query" => Some(reg_core::normalized_search_query(input)),
        "fts_match_query" => reg_core::fts_match_query(input),
        other => panic!("unknown op {other}"),
    }
}

fn hex(s: Option<&str>) -> String {
    s.map_or_else(
        || "null".to_owned(),
        |s| {
            s.chars()
                .map(|c| format!("{:04X}", u32::from(c)))
                .collect::<Vec<_>>()
                .join(" ")
        },
    )
}

fn assert_cases(file: &str, cases: &[Case]) {
    let failures: Vec<String> = cases
        .iter()
        .filter_map(|case| {
            let actual = apply(&case.op, &case.input);
            (actual != case.expected).then(|| {
                format!(
                    "{file}:{} {}: in=[{}] expected=[{}] actual=[{}]",
                    case.line,
                    case.op,
                    hex(Some(&case.input)),
                    hex(case.expected.as_deref()),
                    hex(actual.as_deref()),
                )
            })
        })
        .collect();
    assert!(
        failures.is_empty(),
        "{} of {} cases differ:\n{}",
        failures.len(),
        cases.len(),
        failures[..failures.len().min(20)].join("\n"),
    );
}

fn assert_generated(op: &str) {
    let file = format!("{op}.jsonl");
    let cases = read_cases(&file, Some(op));
    assert!(
        cases.len() >= MIN_GENERATED_CASES,
        "{file}: {} cases",
        cases.len()
    );
    assert_cases(&file, &cases);
}

#[test]
fn fold_identity_matches_corpus() {
    assert_generated("fold_identity");
}

#[test]
fn fold_search_matches_corpus() {
    assert_generated("fold_search");
}

#[test]
fn normalized_search_query_matches_corpus() {
    assert_generated("normalized_search_query");
}

#[test]
fn fts_match_query_matches_corpus() {
    assert_generated("fts_match_query");
}

#[test]
fn unicode_17_cases_match() {
    let cases = read_cases("unicode17.jsonl", None);
    assert!(!cases.is_empty());
    assert_cases("unicode17.jsonl", &cases);
}
