//! The order check of a binding's code filter (crates/DESIGN.md, "Row filters by code"):
//! the variable must be coded and not an identifier, its type must hold the entries, and
//! each entry must name a code of the value sets and classifications its served pieces
//! carry. The structural layer has already checked the filter's shape.

use std::collections::BTreeSet;

use reg_core::IssueLevel;
use reg_core::project::CatalogBinding;
use rusqlite::Connection;

use super::Issue;
use super::window::Coding;
use crate::Error;

/// The findings of `binding`'s filter over `coding`, the facts of every piece it is
/// served from (successor hops included). None without a filter or a served piece.
pub(super) fn check(
    conn: &Connection,
    composed: bool,
    (source, s, b): (&str, usize, usize),
    binding: &CatalogBinding,
    coding: &[Coding],
) -> Result<Vec<Issue>, Error> {
    let Some(filter) = &binding.filter else {
        return Ok(Vec::new());
    };
    if coding.is_empty() {
        return Ok(Vec::new());
    }
    let base = format!("/sources/{s}/bindings/{b}/filter");
    let issue = |level, code: &str, path: String, message: String| Issue {
        source: Some(source.to_owned()),
        variable: Some(binding.variable.clone()),
        ..Issue::new(level, code, path, message)
    };
    let variable = &binding.variable;
    if coding.iter().any(|c| c.coded == "no" || c.identifier) {
        return Ok(vec![issue(
            IssueLevel::Error,
            "filter_variable_not_coded",
            base,
            format!(
                "{variable} is not a coded variable, so its rows cannot be selected by code. \
                 Remove the filter."
            ),
        )]);
    }
    let types: BTreeSet<&str> = coding.iter().filter_map(|c| c.dtype.as_deref()).collect();
    if let Some(other) = types.iter().find(|t| !matches!(**t, "text" | "integer")) {
        return Ok(vec![issue(
            IssueLevel::Error,
            "filter_type",
            base,
            format!(
                "{variable} is {other}, and only text and integer variables can be selected \
                 by code. Remove the filter."
            ),
        )]);
    }
    let mut found = Vec::new();
    if types.contains("integer") {
        if !filter.prefixes.is_empty() {
            found.push(issue(
                IssueLevel::Error,
                "filter_type",
                format!("{base}/prefixes"),
                format!(
                    "{variable} is an integer, which is selected by exact codes only. \
                     List the codes instead of prefixes."
                ),
            ));
        }
        for (k, code) in filter.codes.iter().enumerate() {
            if code.parse::<i64>().is_err() {
                found.push(issue(
                    IssueLevel::Error,
                    "filter_type",
                    format!("{base}/codes/{k}"),
                    format!("{variable} is an integer, and {code:?} is not a whole number."),
                ));
            }
        }
    }
    let codes = known(conn, composed, coding)?;
    if codes.is_empty() {
        found.push(issue(
            IssueLevel::Warning,
            "filter_codes_unchecked",
            base,
            format!(
                "{variable} has no documented code list here, so the filter's codes are \
                 passed on unchecked."
            ),
        ));
        return Ok(found);
    }
    for (k, code) in filter.codes.iter().enumerate() {
        if !codes.contains(code) {
            found.push(issue(
                IssueLevel::Error,
                "filter_code_unknown",
                format!("{base}/codes/{k}"),
                format!(
                    "{code:?} is not a code of {variable} in the requested period. Check the \
                     codes with the values tool."
                ),
            ));
        }
    }
    for (k, prefix) in filter.prefixes.iter().enumerate() {
        if !codes.iter().any(|c| c.starts_with(prefix.as_str())) {
            found.push(issue(
                IssueLevel::Error,
                "filter_prefix_unknown",
                format!("{base}/prefixes/{k}"),
                format!(
                    "No code of {variable} in the requested period starts with {prefix:?}. \
                     Check the codes with the values tool."
                ),
            ));
        }
    }
    Ok(found)
}

/// Every code of the pieces' value sets and class versions, read by public identity
/// (hash, class version id) in the base and, in the composed view, the holdings database.
fn known(conn: &Connection, composed: bool, coding: &[Coding]) -> Result<BTreeSet<String>, Error> {
    let hashes: BTreeSet<&str> = coding
        .iter()
        .filter_map(|c| c.valueset.as_deref())
        .collect();
    let versions: BTreeSet<&str> = coding
        .iter()
        .filter_map(|c| c.class_version.as_deref())
        .collect();
    let dbs: &[&str] = if composed {
        &["main", "held"]
    } else {
        &["main"]
    };
    let mut codes = BTreeSet::new();
    for db in dbs {
        for (keys, sql) in [
            (
                &hashes,
                format!(
                    "SELECT t.body FROM {db}.valueset v JOIN {db}.vs_item i ON i.valueset_id = v.id \
                     JOIN {db}.text t ON t.id = i.code_text WHERE v.hash = ?1"
                ),
            ),
            (
                &versions,
                format!(
                    "SELECT t.body FROM {db}.class_version cv \
                     JOIN {db}.vs_item i ON i.valueset_id = cv.valueset_id \
                     JOIN {db}.text t ON t.id = i.code_text WHERE cv.pid = ?1"
                ),
            ),
        ] {
            let mut stmt = conn.prepare(&sql)?;
            for key in keys {
                for code in stmt.query_map([key], |row| row.get::<_, String>(0))? {
                    codes.insert(code?);
                }
            }
        }
    }
    Ok(codes)
}
