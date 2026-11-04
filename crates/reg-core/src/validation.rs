//! The project validation result (`conformance/api/operations.toml`, `validate`:
//! today's `{ok, issues}`), shared by the structural and the semantic layer.
//!
//! With the `openapi` feature [`IssueLevel`] carries the schema the server publishes
//! (through `reg-catalog`'s own `Validation` and `Issue`), so its comment is a plain one.

use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};

// How much an issue blocks. The structural layer emits only errors; 3e.2's semantic
// layer emits warnings and infos too.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "lowercase")]
pub enum IssueLevel {
    Error,
    Warning,
    Info,
}

/// One finding about the document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ValidationIssue {
    pub level: IssueLevel,
    /// A stable identifier, such as `period_outside_state_validity`.
    pub code: &'static str,
    /// An RFC 6901 JSON pointer into the document; empty for the whole document.
    pub path: String,
    pub message: String,
    /// The successor a `variable_replaced` finding names; null otherwise.
    pub successor_fqid: Option<String>,
}

/// Every issue a validation found, in the order the layers emitted them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationResult {
    pub issues: Vec<ValidationIssue>,
}

impl ValidationResult {
    /// No issue is an error. Warnings and infos do not block, so `ok` is not a clean
    /// bill of health.
    #[must_use]
    pub fn ok(&self) -> bool {
        self.issues.iter().all(|i| i.level != IssueLevel::Error)
    }
}

impl Serialize for ValidationResult {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut s = serializer.serialize_struct("ValidationResult", 2)?;
        s.serialize_field("ok", &self.ok())?;
        s.serialize_field("issues", &self.issues)?;
        s.end()
    }
}
