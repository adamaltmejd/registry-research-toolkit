//! The mock spec (`mock_spec.json`): everything `generate` reads, compiled from the
//! project, the saved catalog answers and the rules (crates/reg-mock/DESIGN.md). Closed
//! and versioned: an unknown field anywhere is a refusal.

use std::collections::BTreeMap;

use reg_core::manifest::{Encoding, Storage};
use reg_core::project::{ColumnType, ExtractFormat};
use serde::{Deserialize, Serialize};

/// The spec `version` this crate writes and reads.
pub const SPEC_VERSION: u32 = 2;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    pub version: u32,
    pub format: ExtractFormat,
    pub layout: Layout,
    pub project: SpecProject,
    /// The catalog generation the saved answers came from.
    pub catalog_generation: String,
    /// The share of values written missing (NULL, or a sentinel code where the column
    /// has one) unless a column states its own.
    pub null_rate: f64,
    pub files: Vec<SpecFile>,
}

/// `delivery`: the steward's order file named the files and columns, so they are the
/// runner's. `standard`: no delivery layout was known; files follow
/// [`crate::compile::standard_file_name`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Layout {
    Delivery,
    Standard,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecProject {
    pub name: String,
    pub hash: String,
}

/// One output file.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecFile {
    /// The file name without its extension.
    pub name: String,
    /// The project source it delivers (the first, when several share it).
    pub source: String,
    pub unit: Unit,
    #[serde(default)]
    pub population: Population,
    /// The expected rows per eligible unit and year.
    pub rows_per_unit: f64,
    /// The years the file holds, ascending; empty for a timeless table.
    #[serde(default)]
    pub years: Vec<i32>,
    pub columns: Vec<SpecColumn>,
}

/// What one row describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Unit {
    Person,
    Firm,
}

/// Which units a register holds in a year. Absent bounds mean no limit.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Population {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub age_min: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub age_max: Option<u8>,
    /// Also units dead or emigrated before the year (a cause-of-death register).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub include_departed: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecColumn {
    /// The column name in the file.
    pub name: String,
    /// A `.dta` file's name for it, when `name` is not a Stata name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stata_name: Option<String>,
    /// A `.dta` variable label: the full name of an aliased column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// The type values are drawn and written in, after the precedence of
    /// [`crate::compile::resolve_type`].
    #[serde(rename = "type")]
    pub dtype: ColumnType,
    /// The held SQL storage the runner maps the column from; absent when unknown or
    /// when the project overrides the type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<Storage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub length: Option<i64>,
    /// The documented codes a coded column draws from.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub codes: Vec<Code>,
    /// The unit attribute the value comes from, instead of a draw.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attribute: Option<Attribute>,
    /// The attribute's values as this column codes them (`female` → `2`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub map: BTreeMap<String, String>,
    /// How a [`Attribute::Year`] column writes the year: the order file's row filter
    /// encoding. Absent: the plain year.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encoding: Option<Encoding>,
    /// The years the column is held in, when fewer than its file's; it is missing in
    /// the file's other years.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub years: Vec<i32>,
    /// The catalog flags the column an identifier.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub identifier: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub null_rate: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Code {
    pub code: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// A missing-value code (`okänd`, `uppgift saknas`), drawn as a missing value.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub sentinel: bool,
}

/// A value a column takes from the unit rather than from a draw, so that a person's
/// age, sex and keys agree across every file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Attribute {
    PersonKey,
    FirmKey,
    Sex,
    BirthYear,
    BirthDate,
    Age,
    County,
    Municipality,
    DeathDate,
    /// The row's year, in the column's own encoding.
    Year,
}
