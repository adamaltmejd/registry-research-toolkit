//! The project types, the structural validator and the two JSON encodings against
//! data oracles: the structural corpora ([`CORPORA`]), the project bodies of
//! `conformance/cases/{validate,order}/`, and files frozen Python wrote.

use std::fs;
use std::path::{Path, PathBuf};

use reg_core::project::{ProjectData, project_hash, to_json_pretty};
use reg_core::{ValidationResult, validate_structural};
use serde_json::Value;

fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn json(path: &Path) -> Value {
    serde_json::from_str(&read(path)).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn cases(dir: &Path) -> Vec<PathBuf> {
    let mut cases: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .map(|entry| entry.unwrap().path())
        .filter(|p| p.is_dir())
        .collect();
    cases.sort();
    cases
}

/// The structural corpora, from the repository root: frozen Python's, and this
/// crate's cases Python's lacks (the intentional differences named in
/// `src/structural.rs`, and shapes only the types need).
const CORPORA: [&str; 2] = [
    "reg_schema/test_corpus",
    "crates/reg-core/tests/project/corpus",
];

type IssueKey = (String, String, String, String);

fn result_keys(result: &ValidationResult) -> Vec<IssueKey> {
    let encoded = serde_json::to_value(result).unwrap();
    expected_keys(&encoded)
}

fn expected_keys(result: &Value) -> Vec<IssueKey> {
    let field = |issue: &Value, key: &str| issue[key].as_str().unwrap().to_owned();
    result["issues"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| {
            (
                field(i, "level"),
                field(i, "code"),
                field(i, "path"),
                field(i, "message"),
            )
        })
        .collect()
}

/// Every corpus case gives exactly its expected issues (level, code, path, message),
/// in emission order. Fails when a rule's code, path or message template
/// changes, or a rule is lost or added.
#[test]
fn structural_corpora() {
    for corpus in CORPORA.map(|c| repo().join(c)) {
        let cases = cases(&corpus);
        assert!(!cases.is_empty(), "no cases under {}", corpus.display());
        for case in cases {
            let actual = result_keys(&validate_structural(&json(&case.join("input.json"))));
            let expected = expected_keys(&json(&case.join("expected_ValidationResult.json")));
            assert_eq!(actual, expected, "{}", case.display());
        }
    }
}

/// Every project of the corpora that the validator accepts.
fn valid_projects() -> Vec<(String, Value)> {
    let mut projects = Vec::new();
    let mut add = |key: String, project: &Value| {
        if validate_structural(project).ok() {
            projects.push((key, project.clone()));
        }
    };
    for corpus in CORPORA {
        for case in cases(&repo().join(corpus)) {
            let key = format!("{corpus}/{}/input.json", name(&case));
            add(key, &json(&case.join("input.json")));
        }
    }
    for kind in ["validate", "order"] {
        for case in cases(&repo().join("conformance/cases").join(kind)) {
            let file = format!("conformance/cases/{kind}/{}/request.json", name(&case));
            let request = json(&repo().join(&file));
            if let Some(project) = request.get("project") {
                add(format!("{file}#/project"), project);
            }
            for (i, step) in request["requests"]
                .as_array()
                .into_iter()
                .flatten()
                .enumerate()
            {
                if let Some(body) = step.get("body") {
                    add(format!("{file}#/requests/{i}/body"), body);
                }
            }
        }
    }
    projects.sort_by(|a, b| a.0.cmp(&b.0));
    projects
}

fn name(case: &Path) -> &str {
    case.file_name().unwrap().to_str().unwrap()
}

/// Every accepted project deserializes into [`ProjectData`] and hashes as frozen
/// Python's `reg_meta.order._project_hash` did (`tests/project/hashes.json`, keyed by
/// file and JSON pointer). Fails when the types drop, add or rename a field, stop
/// writing absent optionals as `null` or expanding panel-member shorthand, or the
/// canonical encoding changes; and when a corpus gains an accepted project the
/// golden lacks.
///
/// To add a project's hash, from the repository root with frozen Python:
/// `uv run python -c 'import json, sys; from reg_meta.order import _project_hash;
/// from reg_schema import ProjectData;
/// print(_project_hash(ProjectData.model_validate(json.load(sys.stdin))))' < project.json`.
///
/// Revisit: regenerate from Rust once the pinned-Rust baseline lands (stage 4, D1).
#[test]
fn project_hashes() {
    let golden = json(&PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/project/hashes.json"));
    let golden = golden.as_object().unwrap();
    let projects = valid_projects();
    assert_eq!(
        projects.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
        golden.keys().map(String::as_str).collect::<Vec<_>>(),
        "accepted projects and hashes.json differ"
    );
    for (key, project) in projects {
        let typed = ProjectData::from_value(&project).unwrap_or_else(|e| panic!("{key}: {e:?}"));
        assert_eq!(project_hash(&typed), golden[&key], "{key}");
    }
}

/// Each committed manifest a validate or order case downloads (`"bytes"`) re-encodes
/// byte for byte, and its `project_hash` is the hash of the project that requested
/// it. Fails when the pretty encoding changes (key order, indent, escaping, trailing
/// newline).
#[test]
fn committed_manifest_bytes() {
    let mut seen = 0;
    for kind in ["validate", "order"] {
        for case in cases(&repo().join("conformance/cases").join(kind)) {
            let request = json(&case.join("request.json"));
            for (i, step) in json(&case.join("expected.json"))
                .as_array()
                .into_iter()
                .flatten()
                .enumerate()
            {
                let Some(file) = step.get("bytes").and_then(Value::as_str) else {
                    continue;
                };
                seen += 1;
                let bytes = read(&case.join(file));
                let manifest: Value = serde_json::from_str(&bytes).unwrap();
                assert_eq!(to_json_pretty(&manifest), bytes, "{}", case.display());
                let project = ProjectData::from_value(&request["requests"][i]["body"]).unwrap();
                assert_eq!(
                    manifest["provenance"]["project_hash"],
                    project_hash(&project),
                    "{}",
                    case.display()
                );
            }
        }
    }
    assert!(seen > 0, "no committed manifest bytes");
}

/// The validation result encodes as frozen Python's `reg_meta.semantic.validation_json`
/// wrote it for the same input (`tests/project/validation_result/`): `ok`, issues in
/// emission order, an explicit `successor_fqid: null`, non-ASCII as is. Fails when
/// the wire shape, the issue order or the encoding changes.
#[test]
fn validation_result_bytes() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/project/validation_result");
    let result = validate_structural(&json(&dir.join("input.json")));
    assert_eq!(to_json_pretty(&result), read(&dir.join("expected.json")));
}
