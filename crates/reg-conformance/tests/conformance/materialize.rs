//! The `materialize` corpus: the MONA order file over HTTP, against hand-written case
//! data (crates/DESIGN.md, "From a logical order to physical files" and "MONA order
//! file").
//!
//! Each case under `cases/materialize/` holds a `request.json` (the project and a note
//! naming the regression it catches) and either `manifest.json`, the order file without
//! its generation ids and project hash, or `expected.json`, the findings that block it.
//! Every case runs on one steward artifact (`steward::materialize`): the holdings
//! fixture with the documented slot family, a long Part B header and a first-half
//! Parent amount. Write the expectation from the project and the fixture source before
//! serving it; a disagreement is a server bug or a wrong expectation, never a golden to
//! save. The MONA runner's refusal suite reads some of these goldens as orders
//! (`reg_mona_runner/tests`).

use std::path::Path;

use serde_json::{Value, json};

use crate::common::http_case::json_eq;
use crate::common::{case_dirs, client, read_json, run_cases, steward, without_nulls};

const ROUTE: &str = "/api/project/materialize";

#[test]
fn materialize_cases() {
    // Each case's note names the regression it catches. Also fails if the order file
    // stops naming the served artifact pair and project, or a blocked order stops
    // refusing with every located finding and no partial manifest.
    let cases: Vec<_> = case_dirs(&reg_conformance::cases().join("materialize"))
        .into_iter()
        .filter(|case| case.join("request.json").is_file())
        .collect();
    run_cases("materialize", &cases, check_case);
}

/// The answer's `findings` as `{"findings": [...]}` without null members.
fn findings(list: &Value) -> Value {
    json!({ "findings": without_nulls(list) })
}

fn check_case(case: &Path) -> Result<(), String> {
    let request = read_json(&case.join("request.json"))?;
    let project = Some(&request["project"]);
    let port = steward::materialize()?;
    let (status, answer) = client::json(port, "POST", ROUTE, &[], project)?;
    let (ordered, logical) = client::json(port, "POST", "/api/project/order", &[], project)?;
    let oracle = case.join("manifest.json");
    if oracle.is_file() {
        if status != 200 {
            return Err(format!("materialize: {status} {answer}"));
        }
        let mut data = answer["data"].clone();
        let provenance = &mut data["provenance"];
        let catalog = pop(&mut provenance["catalog"], "generation");
        if catalog != answer["meta"]["generation"] {
            return Err(format!("provenance.catalog.generation: {catalog}"));
        }
        for value in [
            pop(&mut provenance["holdings"], "generation"),
            pop(provenance, "base_generation"),
            pop(&mut provenance["project"], "project_hash"),
        ] {
            if !value.as_str().is_some_and(reg_core::is_sha256_hex) {
                return Err(format!("provenance digest: {value}"));
            }
        }
        let want = read_json(&oracle)?;
        if !json_eq(&data, &want) {
            return Err(format!("manifest.json differs: got {data}"));
        }
        // Fails if steward `order` drops the holdings stage's warnings (a
        // `mapping_caveat`) that the order file carries. `order` names them; the order
        // file carries the bare finding.
        if ordered != 200 {
            return Err(format!("order: {ordered} {logical}"));
        }
        let names = ["variable_name", "table_name", "successor_name"];
        let bare: Vec<Value> = logical["data"]["findings"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|finding| {
                let mut finding = finding.clone();
                if let Some(map) = finding.as_object_mut() {
                    map.retain(|key, _| !names.contains(&key.as_str()));
                }
                finding
            })
            .collect();
        if !json_eq(&Value::from(bare), &data["findings"]) {
            return Err(format!("order's findings differ: {logical}"));
        }
    } else {
        if status != 422 || answer["error"]["code"] != "order_blocked" {
            return Err(format!("expected order_blocked: {status} {answer}"));
        }
        if answer.get("data").is_some() {
            return Err(format!("a blocked order file carries data: {answer}"));
        }
        let want = read_json(&case.join("expected.json"))?;
        let got = findings(&answer["error"]["fields"]["findings"]);
        if !json_eq(&got, &want) {
            return Err(format!("expected.json differs: got {got}"));
        }
        // Also fails if steward `validate` stops running the holdings stage and passes a
        // project (an unheld variable, a contested cell) that materialize blocks with
        // the same located findings (#2194).
        let (validated, validation) =
            client::json(port, "POST", "/api/project/validate", &[], project)?;
        let data = &validation["data"];
        if validated != 200 || data["ok"] != false || !json_eq(&findings(&data["issues"]), &want) {
            return Err(format!("validate: {validated} {validation}"));
        }
        // Fails if steward `order` stops running the holdings stage and returns a
        // logical order the SPA reads as "Ready to order" for a project materialize
        // blocks (an unheld variable is `mapping_missing`).
        let error = &logical["error"];
        if ordered != 422
            || error["code"] != "order_blocked"
            || !json_eq(&findings(&error["fields"]["findings"]), &want)
        {
            return Err(format!("order: {ordered} {logical}"));
        }
    }
    Ok(())
}

/// Remove and return `object[key]`.
fn pop(object: &mut Value, key: &str) -> Value {
    object
        .as_object_mut()
        .and_then(|map| map.remove(key))
        .unwrap_or(Value::Null)
}

#[test]
fn unnamed_project_is_a_logical_draft_but_no_order_file() {
    // Fails if a whitespace-only name reaches the order file's provenance, or if
    // refusing it also refuses the logical order of the same draft.
    let mut project = read_json(
        &reg_conformance::cases().join("materialize/key-only/request.json"),
    )
    .unwrap()["project"]
        .take();
    project["name"] = json!(" \t ");
    let port = steward::materialize().unwrap();
    let (status, refused) = client::json(port, "POST", ROUTE, &[], Some(&project)).unwrap();
    assert_eq!(status, 422, "{refused}");
    assert_eq!(refused["error"]["code"], "project_unnamed");
    let (status, _) =
        client::json(port, "POST", "/api/project/order", &[], Some(&project)).unwrap();
    assert_eq!(status, 200);
}
