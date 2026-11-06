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

#[test]
fn an_order_draws_from_one_database() {
    // On a steward with two databases (`steward::spaces`: Part A copied into `aux`).
    // Fails if a project without `linkage` is not refused with `database_required`; if
    // a project merged from two databases' projects (demo/held 2020 is only in Pool,
    // in `main`) orders instead of refusing that binding with `not_in_database` naming
    // the database that holds it; or if placement ignores the chosen database (the
    // `aux` order then also takes main's Part A and Part B), or if the logical order's row
    // keys ignore it (aux's Part A is keyed on New, main's tables on Id).
    let port = steward::spaces().unwrap();
    let source = |name: &str, period: i64| {
        json!({"name": name, "table": "demo/held", "period": period,
               "bindings": [{"variable": "demo/held/value"}]})
    };
    let mut project = json!({
        "schema_version": reg_core::project::SCHEMA_VERSION,
        "reg_meta_version": "0.79.0",
        "name": "Two databases",
        "sources": [source("late", 2021)],
    });
    let codes = |findings: &Value| -> Vec<(String, String)> {
        findings
            .as_array()
            .unwrap()
            .iter()
            .filter(|f| f["level"] == "error")
            .map(|f| {
                (
                    f["code"].as_str().unwrap().into(),
                    f["path"].as_str().unwrap().into(),
                )
            })
            .collect()
    };
    for route in [ROUTE, "/api/project/order"] {
        let (status, refused) = client::json(port, "POST", route, &[], Some(&project)).unwrap();
        assert_eq!(status, 422, "{route}: {refused}");
        assert_eq!(
            codes(&refused["error"]["fields"]["findings"]),
            [("database_required".into(), "/linkage".into())],
            "{route}: {refused}"
        );
    }
    let (_, validated) =
        client::json(port, "POST", "/api/project/validate", &[], Some(&project)).unwrap();
    assert_eq!(
        codes(&validated["data"]["issues"]),
        [("database_required".into(), "/linkage".into())],
        "{validated}"
    );

    project["linkage"] = json!({"database": "aux"});
    project["sources"] = json!([source("late", 2021), source("early", 2020)]);
    let (status, refused) = client::json(port, "POST", ROUTE, &[], Some(&project)).unwrap();
    assert_eq!(status, 422, "{refused}");
    let findings = &refused["error"]["fields"]["findings"];
    assert_eq!(
        codes(findings),
        [("not_in_database".into(), "/sources/1/bindings/0".into())],
        "{refused}"
    );
    let message = findings[0]["message"].as_str().unwrap();
    assert!(message.contains("It is in: Main database."), "{message}");

    project["sources"] = json!([source("late", 2021)]);
    let (status, ordered) = client::json(port, "POST", ROUTE, &[], Some(&project)).unwrap();
    assert_eq!(status, 200, "{ordered}");
    let files: Vec<(&str, &str)> = ordered["data"]["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| (f["schema"].as_str().unwrap(), f["table"].as_str().unwrap()))
        .collect();
    assert_eq!(files, [("aux", "Part A")], "{ordered}");
    let (status, logical) =
        client::json(port, "POST", "/api/project/order", &[], Some(&project)).unwrap();
    assert_eq!(status, 200, "{logical}");
    let keys: Vec<&str> = logical["data"]["row_keys"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|k| k["columns"].as_array().unwrap())
        .map(|c| c["variable"].as_str().unwrap())
        .collect();
    assert_eq!(keys, ["demo/held/value"], "{logical}");
}

#[test]
fn readers_name_the_database_of_each_held_table() {
    // On `steward::spaces`, demo/held/value is in Pool, Part A and Part B (`main`) and in
    // the `aux` copy of Part A over 2021 only. Fails if coverage merges the two databases'
    // spans into one entry, lists them out of config order or drops `space`, or if a
    // segments row loses its database (crates/DESIGN.md, "Linkage spaces").
    let port = steward::spaces().unwrap();
    let get = |route: &str| {
        let (status, body) = client::json(port, "GET", route, &[], None).unwrap();
        assert_eq!(status, 200, "{route}: {body}");
        body
    };
    let coverage = get("/api/coverage/demo/held/value");
    let tables: Vec<(&str, &str, &Value)> = coverage["data"]["tables"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| {
            (
                t["fqid"].as_str().unwrap(),
                t["space"].as_str().unwrap(),
                &t["spans"],
            )
        })
        .collect();
    assert_eq!(tables.len(), 2, "{coverage}");
    assert_eq!(
        (tables[0].0, tables[0].1),
        ("demo/held", "main"),
        "{coverage}"
    );
    assert_eq!(
        tables[1],
        (
            "demo/held",
            "aux",
            &json!([{"start": "2021-01-01", "end": "2022-01-01"}])
        ),
        "{coverage}"
    );

    let segments = get("/api/segments/demo/held/value?limit=200");
    let held: std::collections::BTreeSet<(&str, &str)> = segments["data"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| (s["file"].as_str().unwrap(), s["space"].as_str().unwrap()))
        .collect();
    assert_eq!(
        held,
        [
            ("Part B", "main"),
            ("Pool", "main"),
            ("aux.Part A", "aux"),
            ("dbo.Part A", "main"),
        ]
        .into(),
        "{segments}"
    );
}
