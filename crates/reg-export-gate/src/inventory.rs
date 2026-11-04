//! Inventory: staging holds exactly the schema's files. None missing, none
//! extra, no hidden files, folders, links or other non-regular entries, and no
//! receipt of its own. Links are seen on every platform, unlike the R gate on
//! Windows; the rewrite is still what guarantees only checked data leaves.

use std::path::Path;

use crate::{Code, Finding, RECEIPT, Refusal, Schema};

pub(crate) fn check(staging: &Path, schema: &Schema) -> Result<(), Refusal> {
    let internal = || Refusal::setup(Finding::rule("internal_error"));
    let mut present = Vec::new();
    for entry in std::fs::read_dir(staging).map_err(|_| internal())? {
        let entry = entry.map_err(|_| internal())?;
        // Not following links: a link is reported, never read through.
        let regular = entry.file_type().map_err(|_| internal())?.is_file();
        present.push((entry.file_name(), regular));
    }
    // Byte order, not the locale's, so a position means the same everywhere.
    present.sort_by(|a, b| a.0.as_encoded_bytes().cmp(b.0.as_encoded_bytes()));

    let expected: Vec<&str> = schema.files.iter().map(|f| f.name.as_str()).collect();
    let mut findings = Vec::new();
    for (i, (name, regular)) in present.iter().enumerate() {
        let bytes = name.as_encoded_bytes();
        // A name the schema declares (or the receipt's) is named; any other
        // is the author's and is located by position.
        let declared = name
            .to_str()
            .filter(|n| *n == RECEIPT || expected.contains(n));
        let rule = if bytes.starts_with(b".") {
            "hidden_file"
        } else if declared == Some(RECEIPT) {
            "reserved_name"
        } else if !regular {
            "not_a_file"
        } else if declared.is_none() {
            "extra_file"
        } else {
            continue;
        };
        findings.push(match declared {
            Some(n) => Finding::file(rule, n),
            None => Finding::position(rule, None, i + 1),
        });
    }
    for name in &expected {
        if !present.iter().any(|(p, _)| p.to_str() == Some(name)) {
            findings.push(Finding::file("missing_file", name));
        }
    }
    if findings.is_empty() {
        Ok(())
    } else {
        Err(Refusal {
            code: Code::Inventory,
            findings,
        })
    }
}
