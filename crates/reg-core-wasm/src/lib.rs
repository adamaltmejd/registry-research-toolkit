//! `reg-core-wasm`: `reg-core` for the SPA, as a WebAssembly module
//! (`RUST_RUNTIME_SPEC.md` section 10, package 5.2).
//!
//! Thin glue: JSON text crosses the boundary in both directions, and every export is
//! total, so no input string can panic (a panic aborts the module). The SPA's only
//! importer is `reg_webapp/frontend/src/lib/reg_core.ts`.

use reg_core::project::{SCHEMA_VERSION, check};
use reg_core::{IssueLevel, ValidationIssue, ValidationResult};
use serde_json::Value;
use wasm_bindgen::prelude::wasm_bindgen;

/// The `project_data.json` `schema_version` this build reads, which a new draft takes.
#[wasm_bindgen]
#[must_use]
pub fn project_schema_version() -> String {
    SCHEMA_VERSION.into()
}

/// `reg_core::project::check` over JSON text, as the `{ok, issues}` JSON the server's
/// validate operation writes. Any JSON value is checked (a non-object root is
/// `invalid_root`); text that is not JSON is one `invalid_json` issue.
///
/// # Panics
///
/// Never on input: a validation result always encodes.
#[wasm_bindgen]
#[must_use]
pub fn check_project(json: &str) -> String {
    let result = match serde_json::from_str::<Value>(json) {
        Ok(raw) => check(&raw)
            .err()
            .unwrap_or(ValidationResult { issues: Vec::new() }),
        Err(e) => ValidationResult {
            issues: vec![ValidationIssue {
                level: IssueLevel::Error,
                code: "invalid_json",
                path: String::new(),
                message: format!("project_data.json is not valid JSON: {e}"),
                successor_fqid: None,
            }],
        },
    };
    serde_json::to_string(&result).expect("a validation result encodes")
}
