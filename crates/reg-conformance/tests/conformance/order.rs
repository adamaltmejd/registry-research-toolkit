//! The `order` corpus: logical orders and located findings over HTTP, against
//! independently authored case data (conformance/README.md).
//!
//! A case that orders commits its `order.json`: the logical order's `data` without
//! `catalog.generation_id` and `project.project_hash`. A blocked case has none, and its
//! `expected.json` findings are the oracle.
//!
//! Recipe for a new case (a content decision, reviewed in the diff; never rewritten to
//! make a run pass): first write down, from the project and the fixture source alone,
//! the entries, clips, row keys and findings the order must hold, or the findings that
//! block it. Then serve the case's fixture with `reg-meta serve`, `POST
//! /api/project/order` with `request["project"]` and compare. A disagreement is a
//! server bug or a wrong expectation, never a golden to save.

use std::path::Path;

use serde_json::{Value, json};

use crate::common::http_case::{fixture_source, json_eq};
use crate::common::{
    case_dirs, client, error_level, read_json, run_cases, server, steward, without_nulls,
};

#[test]
fn order_cases() {
    // Fails when the logical order drifts from the written-down `order.json` (entries,
    // clips, row keys, findings, their order or messages), when it does not name the
    // served generation, when a blocked order stops refusing with these located
    // findings, or when `validate` disagrees with `order`. The classification ambiguity
    // pair fails if a pin filters unresolved-version windows away before the blocking
    // ambiguity check.
    let cases = case_dirs(&reg_conformance::cases().join("order"));
    run_cases("order", &cases, check_case);
}

fn check_case(case: &Path) -> Result<(), String> {
    let request = read_json(&case.join("request.json"))?;
    let artifact = reg_conformance::cached_reader_artifact(&fixture_source(&request))?;
    let port = server::server(artifact.parent().expect("the artifact directory"))?.port;
    let project = Some(&request["project"]);
    let (status, answer) = client::json(port, "POST", "/api/project/order", &[], project)?;
    let (validated, validation) =
        client::json(port, "POST", "/api/project/validate", &[], project)?;
    if validated != 200 {
        return Err(format!("validate: {validated} {validation}"));
    }
    let validation = &validation["data"];
    let errors = error_level(&validation["issues"]);
    let oracle = case.join("order.json");
    if oracle.is_file() {
        if status != 200 {
            return Err(format!("order: {status} {answer}"));
        }
        let mut data = answer["data"].clone();
        let served = take(&mut data, "catalog", "generation_id");
        if served != generation(&artifact)? {
            return Err(format!("generation_id: {served}"));
        }
        let hash = take(&mut data, "project", "project_hash");
        if !hash.as_str().is_some_and(reg_core::is_sha256_hex) {
            return Err(format!("project_hash: {hash}"));
        }
        let want = read_json(&oracle)?;
        if !json_eq(&data, &want) {
            return Err(format!("order.json differs: got {data}"));
        }
        if validation["ok"] != true || !errors.is_empty() {
            return Err(format!("validate refuses an ordered project: {validation}"));
        }
    } else {
        if status != 422 || answer["error"]["code"] != "order_blocked" {
            return Err(format!("expected order_blocked: {status} {answer}"));
        }
        let findings = &answer["error"]["fields"]["findings"];
        // The oracle spells an absent coordinate by omission.
        let got = json!({ "findings": without_nulls(findings) });
        let want = read_json(&case.join("expected.json"))?;
        if !json_eq(&got, &want) {
            return Err(format!("expected.json differs: got {got}"));
        }
        if validation["ok"] != false {
            return Err(format!("validate passes a blocked project: {validation}"));
        }
        if errors != error_level(findings) {
            return Err(format!(
                "validate's errors differ from order's blocking findings: {validation}"
            ));
        }
    }
    Ok(())
}

/// Remove and return `data[object][key]`.
fn take(data: &mut Value, object: &str, key: &str) -> Value {
    data.get_mut(object)
        .and_then(Value::as_object_mut)
        .and_then(|map| map.remove(key))
        .unwrap_or(Value::Null)
}

/// The artifact's `generation_id` meta value.
fn generation(artifact: &Path) -> Result<Value, String> {
    let conn =
        rusqlite::Connection::open_with_flags(artifact, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|e| e.to_string())?;
    conn.query_row(
        "SELECT value FROM meta WHERE key = 'generation_id'",
        [],
        |row| row.get::<_, String>(0),
    )
    .map(Value::from)
    .map_err(|e| format!("{}: {e}", artifact.display()))
}

#[test]
fn steward_order_carries_declared_keys_not_catalog_identifiers() {
    // Fails if catalog identifier roles auto-carry RelativeId or replace the declared
    // received Id; reference-only order cases cannot exercise the holdings key
    // authority. RelativeId is not bound: Pool holds no column of it, so a steward
    // order binding it is `mapping_missing` (the materialize corpus's blocked cases).
    let expected =
        read_json(&reg_conformance::holdings_case().join("order-key-requirements.json")).unwrap();
    let port = steward::paired().unwrap();
    let (status, answer) = client::json(
        port,
        "POST",
        "/api/project/order",
        &[],
        Some(&expected["project"]),
    )
    .unwrap();
    assert_eq!(status, 200, "{answer}");
    assert!(
        json_eq(&answer["data"]["row_keys"], &expected["row_keys"]),
        "{answer}"
    );
}

#[test]
fn steward_only_slot_orders_paired_and_is_unresolved_base_only() {
    // Fails if steward-mode order reads only the base catalog (the slot table gives
    // `fqid_unresolved`), if a steward-only source loses its declared row key, or if
    // base-only order starts admitting steward definitions.
    let expected =
        read_json(&reg_conformance::holdings_case().join("order-steward-slot.json")).unwrap();
    let project = Some(&expected["project"]);
    let port = steward::paired().unwrap();
    let (status, answer) = client::json(port, "POST", "/api/project/order", &[], project).unwrap();
    assert_eq!(status, 200, "{answer}");
    for (key, want) in expected["paired"].as_object().unwrap() {
        assert!(json_eq(&answer["data"][key], want), "{key}: {answer}");
    }
    let base =
        reg_conformance::cached_reader_artifact(&reg_conformance::holdings_case().join("base"))
            .unwrap();
    let port = server::server(base.parent().unwrap()).unwrap().port;
    let (status, blocked) = client::json(port, "POST", "/api/project/order", &[], project).unwrap();
    assert_eq!(status, 422, "{blocked}");
    let got = json!({ "findings": without_nulls(&blocked["error"]["fields"]["findings"]) });
    assert!(json_eq(&got, &expected["base_only"]), "{got}");
}

#[test]
fn steward_order_names_the_held_column() {
    // Fails if a steward order's entries take the catalog's spelling (Id) instead of
    // the column name the held file uses (Received_Id): the order cases run on the
    // base catalog, whose spellings are the files' own.
    let expected =
        read_json(&reg_conformance::holdings_case().join("order-held-spelling.json")).unwrap();
    let port = steward::paired().unwrap();
    let (status, answer) = client::json(
        port,
        "POST",
        "/api/project/order",
        &[],
        Some(&expected["project"]),
    )
    .unwrap();
    assert_eq!(status, 200, "{answer}");
    let got: Vec<Value> = answer["data"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| json!({"variable": e["variable"], "period": e["period"], "spelling": e["spelling"]}))
        .collect();
    assert!(json_eq(&json!(got), &expected["entries"]), "{answer}");
}
