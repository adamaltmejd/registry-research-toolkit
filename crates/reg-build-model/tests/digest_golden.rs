//! The guard digest basis: Rust's `evidence_sha256` of a prepared record equals the
//! Python builder's (`tests/digest/records.json`, written by `tests/digest/generate.py`).
//!
//! It fails if the reader decodes a payload differently from Python's reconstruction
//! (a serde default that drifts from the Pydantic one) or if the evidence encoding drifts
//! (a field dropped or kept against `evidence_position`, locators left unsorted, a JSON
//! escape that differs from Python's). Either would make every curation guard stale.

use std::path::PathBuf;

use reg_build_model::prepared::Records;
use reg_build_model::record_evidence_sha256;
use rusqlite::Connection;
use rusqlite::types::Value as SqlValue;
use serde_json::Value;

fn sql_value(value: &Value) -> SqlValue {
    match value {
        Value::Null => SqlValue::Null,
        Value::Number(number) => SqlValue::Integer(number.as_i64().expect("integer column")),
        Value::String(text) => SqlValue::Text(text.clone()),
        other => panic!("unexpected fixture cell {other}"),
    }
}

fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&text[at..at + 2], 16).expect("hex digest"))
        .collect()
}

/// Rebuild the fixture's prepared database at `path`.
fn rebuild(fixture: &Value, path: &PathBuf) {
    let conn = Connection::open(path).expect("fixture database");
    for statement in fixture["schema"].as_array().expect("schema") {
        conn.execute_batch(statement.as_str().expect("statement"))
            .expect("schema statement");
    }
    conn.pragma_update(
        None,
        "user_version",
        fixture["user_version"].as_i64().expect("version"),
    )
    .expect("user_version");
    // Parents before children: the schema's foreign keys are enforced.
    for table in ["payload", "occurrence", "locator", "delivered_cell"] {
        for row in fixture["rows"][table].as_array().expect("table rows") {
            let cells = row.as_array().expect("row");
            let values: Vec<SqlValue> = cells
                .iter()
                .enumerate()
                // `payload.digest` is the only BLOB column; the fixture writes it as hex.
                .map(|(position, cell)| {
                    if table == "payload" && position == 2 {
                        SqlValue::Blob(unhex(cell.as_str().expect("digest")))
                    } else {
                        sql_value(cell)
                    }
                })
                .collect();
            let placeholders = vec!["?"; values.len()].join(", ");
            conn.execute(
                &format!("INSERT INTO {table} VALUES ({placeholders})"),
                rusqlite::params_from_iter(values),
            )
            .expect("fixture row");
        }
    }
}

#[test]
fn record_digests_match_python() {
    let fixture: Value =
        serde_json::from_str(include_str!("digest/records.json")).expect("fixture");
    let path = std::env::temp_dir().join(format!(
        "reg-build-model-digest-{}.sqlite",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    rebuild(&fixture, &path);
    let mut actual = Vec::new();
    Records::open(&path)
        .expect("records")
        .for_each_record(None, |record| {
            actual.push(serde_json::json!({
                "record_id": record.record_id,
                "evidence_sha256": record_evidence_sha256(&record),
            }));
            Ok(())
        })
        .expect("read back");
    std::fs::remove_file(&path).expect("remove fixture database");
    assert_eq!(Value::Array(actual), fixture["expected"]);
}
