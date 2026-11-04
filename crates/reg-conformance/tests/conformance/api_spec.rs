//! A corpus lint for the API spec (`conformance/api/operations.toml`, `errors.toml`):
//! every `api` case parses, names operations in the table and uses only catalogued
//! error codes, and every operation lists only catalogued codes. `api` runs the cases.

use std::collections::{BTreeSet, HashSet};
use std::path::Path;

use serde_json::Value;

use crate::common::{case_dirs, read_json, read_toml, run_cases};

fn names(table: &toml::Table, array: &str, key: &str) -> Vec<toml::Table> {
    table
        .get(array)
        .and_then(toml::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(toml::Value::as_table)
        .filter(|entry| entry.get(key).is_some())
        .cloned()
        .collect()
}

fn spec() -> (Vec<toml::Table>, BTreeSet<String>) {
    let api = reg_conformance::repo().join("conformance/api");
    let operations = names(
        &read_toml(&api.join("operations.toml")).unwrap(),
        "operation",
        "name",
    );
    let codes = names(
        &read_toml(&api.join("errors.toml")).unwrap(),
        "code",
        "code",
    )
    .iter()
    .filter_map(|entry| entry.get("code")?.as_str().map(str::to_owned))
    .collect();
    (operations, codes)
}

#[test]
fn operations_name_catalogued_codes() {
    let (operations, codes) = spec();
    let unknown: Vec<String> = operations
        .iter()
        .flat_map(|op| {
            op.get("errors")
                .and_then(toml::Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(toml::Value::as_str)
                .filter(|code| !codes.contains(*code))
                .map(|code| format!("{:?}: {code}", op.get("name")))
        })
        .collect();
    assert!(unknown.is_empty(), "uncatalogued codes: {unknown:?}");
}

#[test]
fn api_cases_are_well_formed() {
    let (operations, codes) = spec();
    let operations: HashSet<&str> = operations
        .iter()
        .filter_map(|op| op.get("name")?.as_str())
        .collect();
    let cases = case_dirs(&reg_conformance::cases().join("api"));
    run_cases("api_spec", &cases, |case| {
        well_formed(case, &operations, &codes)
    });
}

fn well_formed(
    case: &Path,
    operations: &HashSet<&str>,
    codes: &BTreeSet<String>,
) -> Result<(), String> {
    let request = read_json(&case.join("request.json"))?;
    let expected = read_json(&case.join("expected.json"))?;
    for step in request["requests"].as_array().ok_or("requests is a list")? {
        if !step["operation"]
            .as_str()
            .is_some_and(|op| operations.contains(op))
        {
            return Err(format!("unknown operation {}", step["operation"]));
        }
    }
    let oracles = expected
        .as_array()
        .cloned()
        .unwrap_or_else(|| vec![expected.clone()]);
    for oracle in &oracles {
        let code = oracle
            .pointer("/startup_error/code")
            .or_else(|| oracle.get("json").and_then(|json| json.get("/error/code")));
        if let Some(code) = code
            && !code.as_str().is_some_and(|c| codes.contains(c))
        {
            return Err(format!("uncatalogued code {code}"));
        }
    }
    for (index, oracle) in oracles.iter().enumerate() {
        let Some(members) = oracle.get("members") else {
            continue;
        };
        let keys: BTreeSet<&str> = members
            .as_object()
            .into_iter()
            .flatten()
            .map(|(k, _)| k.as_str())
            .collect();
        if keys != BTreeSet::from(["steps", "pointer", "equals"]) {
            return Err(format!("step {index}: members keys {keys:?}"));
        }
        // Evaluated with this step's oracle, so only earlier steps are answered.
        let steps = members["steps"].as_array().ok_or("members steps")?;
        if !steps.iter().all(|s| {
            s.as_u64()
                .is_some_and(|s| usize::try_from(s).is_ok_and(|s| s <= index))
        }) {
            return Err(format!(
                "step {index}: members steps {steps:?} reach later steps"
            ));
        }
        let equals = members["equals"].as_array().ok_or("members equals")?;
        let distinct: HashSet<String> = equals.iter().map(Value::to_string).collect();
        if distinct.len() != equals.len() {
            return Err(format!("step {index}: members equals repeats"));
        }
    }
    Ok(())
}
