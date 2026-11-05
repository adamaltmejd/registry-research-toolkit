//! The private evidence file: curation evidence that names a data steward, kept in the
//! private inputs repository and cited from public curation as `private:ev-NNNN`
//! (`crates/DESIGN.md`, "Steward provenance"). The builder only checks that every
//! reference resolves; it never emits the text.
//!
//! One TOML table of `ev-NNNN = "<evidence>"`.

use std::collections::BTreeMap;
use std::path::Path;

use crate::{ReadError, ReadOutput, error};

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
    let entries: BTreeMap<String, String> = crate::config(&root.join(path))?;
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
