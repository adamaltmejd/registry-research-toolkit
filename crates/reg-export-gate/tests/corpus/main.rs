//! Runs the shared case corpus (`reg_export_gate/tests/cases/*/case.json`)
//! against the gate: a candidate staging folder in, the result out. The corpus
//! is the oracle shared with the R gate; everything a case asserts is in its
//! JSON (`reg_export_gate/DESIGN.md`, "Case corpus").

mod materialize;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use reg_export_gate::{Finding, check};
use serde_json::Value;

const CASES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../reg_export_gate/tests/cases"
);

/// Why a case does not run yet, or `None`. The reasons go as the checks they
/// wait for land (crates/reg-export-gate/DESIGN.md, "Plan").
fn pending(case: &Value) -> Option<&'static str> {
    let expect = &case["expect"];
    match expect["code"].as_str() {
        Some("disclosure") => Some("disclosure check not built"),
        Some("backstop") => Some("backstop scan not built"),
        _ if case.get("output_before").is_some() => Some("clearing a prior export not built"),
        // A pass asserts the export, which needs every check and the rewrite.
        _ if expect["result"] == "pass" => Some("the rewrite and receipt not built"),
        _ => None,
    }
}

fn expected_finding(f: &Value) -> Finding {
    let text = |key: &str| f.get(key).and_then(Value::as_str).map(str::to_owned);
    Finding {
        file: text("file"),
        column: text("column"),
        position: f
            .get("position")
            .and_then(Value::as_u64)
            .map(|p| usize::try_from(p).expect("a position")),
        // Leaked once per distinct rule string in the corpus: a test process.
        rule: Box::leak(text("rule").expect("a rule").into_boxed_str()),
    }
}

/// Every entry of `dir` with its bytes (`None` for a folder).
fn snapshot(dir: &Path) -> BTreeMap<String, Option<Vec<u8>>> {
    std::fs::read_dir(dir)
        .expect("a staging folder")
        .map(|entry| {
            let path = entry.expect("an entry").path();
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            let bytes = path.is_file().then(|| std::fs::read(&path).expect("bytes"));
            (name, bytes)
        })
        .collect()
}

fn case_path(case_dir: &Path, path: &Value) -> Option<PathBuf> {
    path.as_str().map(|p| case_dir.join(p))
}

/// The case's failure, or `None` if it holds.
fn run(name: &str, case_dir: &Path, case: &Value) -> Option<String> {
    let work = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("corpus")
        .join(name);
    let _ = std::fs::remove_dir_all(&work);
    let staging = work.join("staging");
    materialize::materialize(
        &staging,
        case["candidate"].as_object().expect("a candidate"),
    );
    let before = snapshot(&staging);
    let schema = case_path(case_dir, &case["schema"]);
    let policy = case_path(case_dir, &case["policy"]);
    let result = check(&staging, schema.as_deref(), policy.as_deref());
    if snapshot(&staging) != before {
        return Some("staging changed".into());
    }
    let expect = &case["expect"];
    match (expect["result"].as_str(), result) {
        (Some("pass"), Ok(_)) => None,
        (Some("refused"), Err(refusal)) => {
            let code = expect["code"].as_str().expect("a code");
            let mut findings: Vec<Finding> = expect["findings"]
                .as_array()
                .expect("findings")
                .iter()
                .map(expected_finding)
                .collect();
            // Finding order is not part of the contract (the R runner sorts too).
            let key = |f: &Finding| (f.file.clone(), f.column.clone(), f.position, f.rule);
            findings.sort_by_key(key);
            let mut got = refusal.findings.clone();
            got.sort_by_key(key);
            (refusal.code.as_str() != code || got != findings).then(|| {
                format!(
                    "expected {code} {findings:?}, got {} {:?}",
                    refusal.code.as_str(),
                    refusal.findings
                )
            })
        }
        (want, got) => Some(format!("expected {want:?}, got {got:?}")),
    }
}

#[test]
fn corpus() {
    let mut cases: Vec<PathBuf> = std::fs::read_dir(CASES)
        .expect("the corpus")
        .map(|e| e.expect("a case").path())
        .collect();
    cases.sort();
    let mut ran = 0;
    let mut failures = Vec::new();
    for case_dir in &cases {
        let name = case_dir.file_name().unwrap().to_string_lossy().into_owned();
        let case: Value =
            serde_json::from_slice(&std::fs::read(case_dir.join("case.json")).expect("case.json"))
                .expect("JSON");
        if let Some(reason) = pending(&case) {
            println!("pending {name}: {reason}");
            continue;
        }
        ran += 1;
        if let Some(failure) = run(&name, case_dir, &case) {
            failures.push(format!("{name}: {failure}"));
        }
    }
    assert!(ran > 0, "no corpus case ran");
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
