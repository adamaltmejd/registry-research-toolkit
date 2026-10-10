//! `reg-core-wasm`: `reg-core` for the SPA, as a WebAssembly module
//! (`RUST_RUNTIME_SPEC.md` section 10, packages 5.2 and 5.3).
//!
//! Thin glue: JSON text crosses the boundary in both directions, and every export is
//! total, so no input string can panic (a panic aborts the module). The SPA's only
//! importer is `reg_webapp/frontend/src/lib/reg_core.ts`.

use reg_core::project::{SCHEMA_VERSION, SourcePeriod, check};
use reg_core::{Interval, IssueLevel, Period, PeriodToken, ValidationIssue, ValidationResult};
use serde_json::{Value, json};
use wasm_bindgen::prelude::wasm_bindgen;

/// The `project_data.json` `schema_version` this build reads, which a new draft takes.
#[wasm_bindgen]
#[must_use]
pub fn project_schema_version() -> String {
    SCHEMA_VERSION.into()
}

/// `reg_core::project::check` over JSON text, as the `{ok, issues}` JSON the server's
/// validate operation writes. Any JSON value is checked (a non-object root is
/// `invalid_root`); text `serde_json` cannot read (not JSON, or nested past its
/// 128-level limit) is one `invalid_json` issue.
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
                // Not "invalid": from the SPA this is JSON that `JSON.parse` read but
                // serde_json refuses (nesting past its 128-level limit).
                message: format!("project_data.json could not be read: {e}"),
                successor_fqid: None,
            }],
        },
    };
    serde_json::to_string(&result).expect("a validation result encodes")
}

/// `text` through the period grammar: `{canonical, kind, bounds: [lo, hi], years:
/// [lo, hi]}`, the kind one of `conformance/cases/grammar/period.jsonl`'s, or
/// `{error}` with the error catalog code. Not trimmed.
#[wasm_bindgen]
#[must_use]
pub fn parse_period(text: &str) -> String {
    let parsed = match text.parse::<Period>() {
        Ok(period) => {
            let kind = match period {
                Period::Range { .. } => "range",
                Period::Token(token) => match token {
                    PeriodToken::Year(_) => "year",
                    PeriodToken::Month { .. } => "month",
                    PeriodToken::Day { .. } => "day",
                    PeriodToken::Term { .. } => "term",
                    PeriodToken::SchoolYear(_) => "school_year",
                    PeriodToken::Quarter { .. } => "quarter",
                    PeriodToken::Half { .. } => "half",
                },
            };
            let (lo, hi) = period.iso_bounds();
            let (first, last) = period.years();
            json!({
                "canonical": period.to_string(),
                "kind": kind,
                "bounds": [lo, hi],
                "years": [first, last],
            })
        }
        Err(e) => json!({"error": e.code()}),
    };
    parsed.to_string()
}

/// `reg_core::period_token_for_bounds`: the coarsest token whose bounds are exactly
/// `lo..hi`, else the explicit `lo..hi`. Total over any two strings.
#[wasm_bindgen]
#[must_use]
pub fn period_token_for_bounds(lo: &str, hi: &str) -> String {
    reg_core::period_token_for_bounds(lo, hi)
}

/// The `Source.period` JSON a `?period` wire spells (`SourcePeriod::from_wire`),
/// shaped, not validated.
///
/// # Panics
///
/// Never on input: a source period always encodes.
#[wasm_bindgen]
#[must_use]
pub fn source_period_from_wire(wire: &str) -> String {
    serde_json::to_string(&SourcePeriod::from_wire(wire)).expect("a source period encodes")
}

/// The `?period` wire of a `Source.period` JSON value as a JSON string, or `null`
/// when it has none (`SourcePeriod::to_wire`) or the value is not a source period's
/// shape. Shaped, not validated: an out-of-grammar token still has a wire.
#[wasm_bindgen]
#[must_use]
pub fn source_period_to_wire(json: &str) -> String {
    let wire = serde_json::from_str::<SourcePeriod>(json)
        .ok()
        .and_then(|period| period.to_wire());
    json!(wire).to_string()
}

/// The days a `Source.period` JSON value requests: `{intervals: [[lo, hi], ...]}`,
/// merged; `{intervals: null}` for the year-independent `"_default"`; or `{error:
/// "invalid_period", message}` when the period's structural check or its range
/// order refuses it.
#[wasm_bindgen]
#[must_use]
pub fn source_period_intervals(json: &str) -> String {
    let refused = |message: String| json!({"error": "invalid_period", "message": message});
    let answer = match checked_period(json) {
        Err(message) => refused(message),
        Ok(period) => match period.intervals() {
            Ok(intervals) => json!({ "intervals": intervals }),
            Err(message) => refused(message),
        },
    };
    answer.to_string()
}

/// The calendar-year spans of a `Source.period` JSON value
/// (`SourcePeriod::year_spans`): `[[lo, hi], ...]`, or `null` unless the period is
/// accepted and every endpoint is a year.
#[wasm_bindgen]
#[must_use]
pub fn source_period_years(json: &str) -> String {
    json!(checked_period(json).ok().and_then(|p| p.year_spans())).to_string()
}

/// `reg_core::merge` over a JSON list of `[lo, hi]` ISO intervals; `null` for text
/// that is not such a list.
#[wasm_bindgen]
#[must_use]
pub fn merge(json: &str) -> String {
    json!(intervals(json).map(reg_core::merge)).to_string()
}

/// `reg_core::overlap` of two JSON lists of ascending, disjoint ISO intervals;
/// `null` when either is not such a list.
#[wasm_bindgen]
#[must_use]
pub fn overlap(a: &str, b: &str) -> String {
    let shared = intervals(a)
        .zip(intervals(b))
        .map(|(a, b)| reg_core::overlap(&a, &b));
    json!(shared).to_string()
}

/// `reg_core::render` of a JSON list of ascending, disjoint ISO intervals, as a
/// JSON string; `null` for text that is not such a list.
#[wasm_bindgen]
#[must_use]
pub fn render(json: &str) -> String {
    json!(intervals(json).map(|i| reg_core::render(&i))).to_string()
}

/// The source period in JSON text once its structural check accepts it, so that
/// `SourcePeriod::intervals` cannot panic; else why not.
fn checked_period(json: &str) -> Result<SourcePeriod, String> {
    let value: Value = serde_json::from_str(json).map_err(|e| e.to_string())?;
    SourcePeriod::from_value(&value).map_err(|result| {
        result
            .issues
            .iter()
            .map(|issue| issue.message.as_str())
            .collect::<Vec<_>>()
            .join("; ")
    })
}

fn intervals(json: &str) -> Option<Vec<Interval>> {
    serde_json::from_str(json).ok()
}
