//! Local mock data from a `project_data.json` (#707, crates/reg-mock/DESIGN.md): the
//! mock rules and spec contracts and `compile`, which `reg-meta mock` runs offline on
//! the researcher's machine. Reads no data rows: only catalog metadata and the rules.

pub mod compile;
pub mod generate;
pub mod input;
pub mod population;
mod rng;
pub mod rules;
pub mod spec;
pub mod write;

use serde::Serialize;

/// A located refusal: a stable `code`, where (`at`: a file, source, table or column),
/// and what to do. Never holds a data value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Refusal {
    pub code: &'static str,
    pub at: String,
    pub message: String,
}

impl Refusal {
    #[must_use]
    pub fn new(code: &'static str, at: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code,
            at: at.into(),
            message: message.into(),
        }
    }
}
