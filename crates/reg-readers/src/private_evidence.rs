//! The private evidence file: curation evidence that names a data steward, kept in the
//! private inputs repository and cited from public curation as `private:ev-NNNN`
//! (`crates/DESIGN.md`, "Steward provenance"), and the steward names public curation
//! must not use. The builder only checks that every reference resolves; it never emits
//! the text.
//!
//! One TOML table of `ev-NNNN = "<evidence>"`.

use std::collections::BTreeMap;
use std::path::Path;

use crate::{ReadError, ReadOutput, error};

/// Steward names and the document and project names that identify a steward's
/// deliveries, compared whole-token and case-insensitively. An exact list, never a
/// pattern (maintainer ruling, 2026-11-05); a new steward adds its names here. They
/// live in this crate because only readers may name a provider.
const STEWARD_NAMES: [&str; 4] = ["SWECOV", "P1105", "Leveransrapport", "IFAU"];

/// The first steward name in `text`. Words split at every non-alphanumeric character,
/// so `P1105_LopNr` and `p1105-lopnr` match and a code `CP1105` does not.
#[must_use]
pub fn steward_name(text: &str) -> Option<&'static str> {
    text.split(|c: char| !c.is_alphanumeric()).find_map(|word| {
        STEWARD_NAMES
            .into_iter()
            .find(|name| word.eq_ignore_ascii_case(name))
    })
}

/// A reference key: `ev-` and at least four ASCII digits.
#[must_use]
pub fn is_key(key: &str) -> bool {
    key.strip_prefix("ev-")
        .is_some_and(|n| n.len() >= 4 && n.bytes().all(|b| b.is_ascii_digit()))
}

/// The file's keys, each with non-blank text.
///
/// # Errors
/// An unreadable file, invalid TOML, a malformed key or blank text.
pub fn keys(root: &Path, path: &str) -> Result<std::collections::BTreeSet<String>, ReadError> {
    let file = root.join(path);
    let text = std::fs::read_to_string(&file).map_err(|e| error(path, e.to_string()))?;
    // The file is private: a refusal names a line and the parser's message, never the
    // offending source line that `toml::de::Error`'s Display quotes.
    let entries: BTreeMap<String, String> =
        toml::from_str(&text).map_err(|e: toml::de::Error| {
            let line = crate::curation::Lines::new(&text).at(e.span().map_or(0, |s| s.start));
            error(&format!("{path}:{line}"), e.message())
        })?;
    for (key, text) in &entries {
        if !is_key(key) {
            return Err(error(
                path,
                format!("private-evidence-key: {key:?} is not ev-NNNN"),
            ));
        }
        if text.trim().is_empty() {
            return Err(error(path, format!("private-evidence-key: {key} is blank")));
        }
    }
    Ok(entries.into_keys().collect())
}

/// No frame: the input is read only so its pin and digest enter the input table.
///
/// # Errors
/// As [`keys`].
pub fn read(root: &Path, path: &str) -> Result<ReadOutput, ReadError> {
    keys(root, path)?;
    Ok(ReadOutput {
        files: vec![path.to_owned()],
        ..ReadOutput::default()
    })
}
