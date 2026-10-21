//! The project types, the structural validator and the project hash against data
//! oracles: the structural corpus ([`CORPUS`]), the project bodies of
//! `conformance/cases/{api,order}/`, and the hashes the retired Python reader wrote.
//! The manifest encoding is pinned by `conformance/test_order.py` (the server's
//! download against each committed `order.json`).

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use reg_core::project::{ProjectData, project_hash};
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

/// The structural corpus, from the repository root: the retired Python validator's
/// cases, plus the intentional differences named in `src/structural.rs` and shapes
/// only the types need.
const CORPUS: &str = "crates/reg-core/tests/project/corpus";

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
fn structural_corpus() {
    let cases = cases(&repo().join(CORPUS));
    assert!(!cases.is_empty(), "no cases under {CORPUS}");
    for case in cases {
        let actual = result_keys(&validate_structural(&json(&case.join("input.json"))));
        let expected = expected_keys(&json(&case.join("expected_ValidationResult.json")));
        assert_eq!(actual, expected, "{}", case.display());
    }
}

/// Every project of the corpus and the conformance cases that the validator accepts.
fn valid_projects() -> BTreeMap<String, Value> {
    let mut projects = BTreeMap::new();
    let mut add = |key: String, project: &Value| {
        if validate_structural(project).ok() {
            projects.insert(key, project.clone());
        }
    };
    for case in cases(&repo().join(CORPUS)) {
        let key = format!("{CORPUS}/{}/input.json", name(&case));
        add(key, &json(&case.join("input.json")));
    }
    for kind in ["api", "order"] {
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
    projects
}

fn name(case: &Path) -> &str {
    case.file_name().unwrap().to_str().unwrap()
}

/// Every project keyed in `tests/project/hashes.json` (file and JSON pointer) is still
/// accepted, deserializes into [`ProjectData`] and hashes to its recorded value: the
/// retired Python reader's `reg_meta.order._project_hash` for the entries it wrote.
/// Fails when the types drop, add or rename a field, stop writing absent optionals as
/// `null` or expanding panel-member shorthand, or the canonical encoding changes; and
/// when a keyed project is no longer accepted. A new accepted project needs no entry:
/// its only source would be the `project_hash` under test.
#[test]
fn project_hashes() {
    let golden = json(&PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/project/hashes.json"));
    let projects = valid_projects();
    for (key, hash) in golden.as_object().unwrap() {
        let project = projects
            .get(key)
            .unwrap_or_else(|| panic!("{key}: not an accepted project"));
        let typed = ProjectData::from_value(project).unwrap_or_else(|e| panic!("{key}: {e:?}"));
        assert_eq!(project_hash(&typed), *hash, "{key}");
    }
}
