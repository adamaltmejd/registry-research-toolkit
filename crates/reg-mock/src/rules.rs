//! The mock rules (`mock_rules.json`): what the catalog cannot say, written by the
//! agent and reviewed by the researcher. Who each source holds, how many rows a unit
//! has, which columns are unit attributes, and the delivery's departures from the
//! catalog. Columns the catalog does not document are the project's (manual items), not
//! the rules'. Closed and versioned like the spec.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::spec::{Attribute, Code, Population, Unit};

/// The rules `version` this crate reads.
pub const RULES_VERSION: u32 = 2;

/// The default share of missing values.
pub const DEFAULT_NULL_RATE: f64 = 0.02;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rules {
    pub version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub null_rate: Option<f64>,
    /// One entry per project source, by its name.
    pub sources: BTreeMap<String, SourceRules>,
    /// A file name the compiled spec would use → the name the delivery uses.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub rename: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceRules {
    pub unit: Unit,
    #[serde(default)]
    pub population: Population,
    pub rows_per_unit: RowsPerUnit,
    /// By the column's name in the file.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub columns: BTreeMap<String, ColumnRules>,
}

/// Rows per eligible unit and year, with the reason the agent chose it, so a reviewer
/// can judge it (one for a yearly snapshot, more for events such as diagnoses).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowsPerUnit {
    pub mean: f64,
    pub reason: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnRules {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attribute: Option<Attribute>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub map: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub null_rate: Option<f64>,
    /// The delivery's codes, replacing the catalog's; a column entered by hand states
    /// its codes in the project instead (`codes_in_project`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codes: Option<Vec<Code>>,
}
