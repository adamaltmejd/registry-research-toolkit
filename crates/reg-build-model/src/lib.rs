//! `reg-build-model`: the Rust builder's neutral common form, the reader for the
//! Python-prepared source records it is normalized from, and the guard digest basis
//! (`crates/DESIGN.md`, "The builder model").
//!
//! - [`model`]: registers, tables, deliveries, columns and column → variable links. Rules
//!   and curation see only these.
//! - [`prepared`]: the v16 prepared records as delivered. Only normalize reads them.
//! - [`verbs`]: the neutral curation verbs as the converted tree states them.
//! - [`Evidence`]: a prepared record behind an opaque handle, with its `evidence_sha256`.

mod evidence;
pub mod model;
pub mod prepared;
pub mod verbs;

use std::fmt;
use std::path::Path;

pub use evidence::{Evidence, canonical_json, canonical_sha256, record_evidence_sha256};

/// A prepared root that cannot be read, or that violates its contract.
#[derive(Debug)]
pub struct Error(String);

impl Error {
    fn io(path: &Path, err: &std::io::Error) -> Self {
        Self(format!("{}: {err}", path.display()))
    }

    fn contract(message: String) -> Self {
        Self(message)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self(err.to_string())
    }
}

impl From<rusqlite::Error> for Error {
    fn from(err: rusqlite::Error) -> Self {
        Self(format!("prepared records database: {err}"))
    }
}
