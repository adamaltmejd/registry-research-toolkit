//! Schema and policy loading (`reg_export_gate/DESIGN.md`, "Schema format" and
//! "Policy format"). The setup rules apply in the R gate's order: unreadable,
//! not the format, `format_version`, the reserved name, then the closed shape.

use std::collections::HashSet;
use std::path::Path;

use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{Finding, RECEIPT, Refusal};

const SCHEMA_FORMAT: &str = "reg-export-gate/schema";
const POLICY_FORMAT: &str = "reg-export-gate/policy";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnType {
    Integer,
    Double,
    String,
    Boolean,
    Date,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Dimension,
    Count,
    PersonCount,
    Statistic,
    /// A statistic that is a proportion of two true counts.
    Share,
    /// A true, unperturbed figure: checked, never exported.
    CheckOnly,
}

impl Role {
    #[must_use]
    pub fn is_count(self) -> bool {
        matches!(self, Role::Count | Role::PersonCount)
    }

    /// A statistic or a share: it rests on a support.
    #[must_use]
    pub fn is_statistic(self) -> bool {
        matches!(self, Role::Statistic | Role::Share)
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: ColumnType,
    pub role: Role,
    /// The count or check-only column a statistic or share rests on.
    #[serde(default)]
    pub support: Option<String>,
    /// The check-only column holding the true number of units behind a count.
    #[serde(default)]
    pub units: Option<String>,
    /// The most a person count's published figure may differ from its units.
    #[serde(default)]
    pub perturbation: Option<u64>,
    /// A share's check-only numerator.
    #[serde(default)]
    pub numerator: Option<String>,
    /// A share's check-only denominator.
    #[serde(default)]
    pub denominator: Option<String>,
    /// Raises the policy's `statistic_min_support` for this statistic or share.
    #[serde(default)]
    pub min_support: Option<u64>,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct File {
    pub name: String,
    pub columns: Vec<Column>,
    #[serde(default)]
    pub description: Option<String>,
}

impl File {
    /// The position of the column named `name`.
    #[must_use]
    pub fn position(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Schema {
    pub format: String,
    pub format_version: u64,
    pub name: String,
    pub version: u64,
    #[serde(default)]
    pub description: Option<String>,
    pub files: Vec<File>,
    /// SHA-256 of the file as read, for the receipt.
    #[serde(skip)]
    pub sha256: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaRef {
    pub name: String,
    pub version: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    pub format: String,
    pub format_version: u64,
    pub name: String,
    pub version: u64,
    #[serde(default)]
    pub description: Option<String>,
    pub schema: SchemaRef,
    pub min_cell_count: u64,
    /// The smallest support count a non-null statistic may rest on; absent, no rule.
    #[serde(default)]
    pub statistic_min_support: Option<u64>,
    /// The significant digits a non-null `double` statistic is rounded to; absent, no
    /// rule.
    #[serde(default)]
    pub statistic_significant_digits: Option<u32>,
    /// Every count and person count must name its `units`; absent, false.
    #[serde(default)]
    pub count_units_required: bool,
    /// SHA-256 of the file as read, for the receipt.
    #[serde(skip)]
    pub sha256: String,
}

/// `^[a-z0-9][a-z0-9_-]*$`
pub(crate) fn is_slug(s: &str) -> bool {
    let mut bytes = s.bytes();
    bytes
        .next()
        .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        && bytes.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
}

/// A slug ending in `.parquet`, so `receipt.json` can never be a data file.
pub(crate) fn is_file_name(s: &str) -> bool {
    s.strip_suffix(".parquet").is_some_and(is_slug)
}

/// `^[a-z][a-z0-9_]*$`
fn is_column_name(s: &str) -> bool {
    let mut bytes = s.bytes();
    bytes.next().is_some_and(|b| b.is_ascii_lowercase())
        && bytes.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

/// The file's basename, the only part of a toolkit path a finding names.
fn where_of(path: Option<&Path>) -> Option<String> {
    path.and_then(Path::file_name)
        .map(|name| name.to_string_lossy().into_owned())
}

fn setup(rule: &'static str, file: Option<&str>) -> Refusal {
    Refusal::setup(match file {
        Some(file) => Finding::file(rule, file),
        None => Finding::rule(rule),
    })
}

/// The file's bytes and parsed JSON, or `None` for a missing path, a folder,
/// unreadable bytes or invalid JSON.
fn read_json(path: Option<&Path>) -> Option<(Vec<u8>, Value)> {
    let path = path?;
    if path.is_dir() {
        return None;
    }
    let bytes = std::fs::read(path).ok()?;
    let doc = serde_json::from_slice(&bytes).ok()?;
    Some((bytes, doc))
}

/// The staged checks shared by schema and policy: the format string, then
/// `format_version` exactly the integer 1.
fn check_format(
    doc: &Value,
    format: &str,
    invalid: &'static str,
    version: &'static str,
    file: Option<&str>,
) -> Result<(), Refusal> {
    if doc.get("format").and_then(Value::as_str) != Some(format) {
        return Err(setup(invalid, file));
    }
    // `1.0` is a float in serde_json and not 1 here, as in jsonlite.
    if doc.get("format_version").and_then(Value::as_u64) != Some(1) {
        return Err(setup(version, file));
    }
    Ok(())
}

/// Deserialized from the bytes, not the parsed value, so a duplicate key is
/// refused rather than resolved to one of its values.
fn typed<T: DeserializeOwned>(bytes: &[u8]) -> Option<T> {
    serde_json::from_slice(bytes).ok()
}

/// Whether a column's role, type and fields fit together; `role_of` gives the
/// role of a column of the same file by name.
fn valid_column(col: &Column, role_of: &impl Fn(&str) -> Option<Role>) -> bool {
    let names = |name: &Option<String>, ok: fn(Role) -> bool| {
        name.as_deref().and_then(role_of).is_some_and(ok)
    };
    let check_only: fn(Role) -> bool = |r| r == Role::CheckOnly;
    let supports: fn(Role) -> bool = |r| r.is_count() || r == Role::CheckOnly;
    let units = col.units.is_none() || names(&col.units, check_only);
    let (support, perturbation, share_fields, min_support) = (
        col.support.is_some(),
        col.perturbation.is_some(),
        col.numerator.is_some() || col.denominator.is_some(),
        col.min_support.is_some(),
    );
    let statistic_fields = support || share_fields || min_support;
    match col.role {
        Role::Dimension | Role::CheckOnly => {
            // A check-only column is a true count.
            (col.role == Role::Dimension || col.ty == ColumnType::Integer)
                && !statistic_fields
                && col.units.is_none()
                && !perturbation
        }
        // A count is a whole number; its suppression marker is null. A row
        // count's units are not its truth, so no perturbation bound is defined
        // for it.
        Role::Count => col.ty == ColumnType::Integer && units && !statistic_fields && !perturbation,
        Role::PersonCount => {
            col.ty == ColumnType::Integer
                && units
                && (!perturbation || col.units.is_some())
                && !statistic_fields
        }
        Role::Statistic | Role::Share => {
            names(&col.support, supports)
                && col.min_support.is_none_or(|n| n >= 1)
                && col.units.is_none()
                && !perturbation
                && if col.role == Role::Share {
                    col.ty == ColumnType::Double
                        && names(&col.numerator, check_only)
                        && names(&col.denominator, check_only)
                } else {
                    !share_fields
                }
        }
    }
}

fn valid_columns(columns: &[Column]) -> bool {
    let mut names = HashSet::new();
    let unique = columns
        .iter()
        .all(|col| names.insert(col.name.as_str()) && is_column_name(&col.name));
    let role_of = |name: &str| columns.iter().find(|c| c.name == name).map(|c| c.role);
    // A check-only column no rule reads would be truth kept in staging for nothing.
    let referenced = |name: &str| {
        columns.iter().any(|c| {
            [&c.support, &c.units, &c.numerator, &c.denominator]
                .iter()
                .any(|r| r.as_deref() == Some(name))
        })
    };
    unique
        // At least one column is exported.
        && columns.iter().any(|col| col.role != Role::CheckOnly)
        && columns.iter().all(|col| {
            valid_column(col, &role_of) && (col.role != Role::CheckOnly || referenced(&col.name))
        })
}

pub(crate) fn load_schema(path: Option<&Path>) -> Result<Schema, Refusal> {
    let file = where_of(path);
    let file = file.as_deref();
    let (bytes, doc) = read_json(path).ok_or_else(|| setup("schema_unreadable", file))?;
    check_format(
        &doc,
        SCHEMA_FORMAT,
        "schema_invalid",
        "schema_version",
        file,
    )?;
    let declares_receipt = doc
        .get("files")
        .and_then(Value::as_array)
        .is_some_and(|files| {
            files
                .iter()
                .any(|f| f.get("name").and_then(Value::as_str) == Some(RECEIPT))
        });
    if declares_receipt {
        return Err(setup("schema_reserved_name", file));
    }
    let invalid = || setup("schema_invalid", file);
    let mut schema: Schema = typed(&bytes).ok_or_else(invalid)?;
    schema.sha256 = crate::sha256_hex(&bytes);
    let mut file_names = HashSet::new();
    let ok = is_slug(&schema.name)
        && schema.version >= 1
        && !schema.files.is_empty()
        && schema.files.iter().all(|f| {
            file_names.insert(f.name.as_str()) && is_file_name(&f.name) && valid_columns(&f.columns)
        });
    if !ok {
        return Err(invalid());
    }
    Ok(schema)
}

pub(crate) fn load_policy(path: Option<&Path>, schema: &Schema) -> Result<Policy, Refusal> {
    let file = where_of(path);
    let file = file.as_deref();
    if !path.is_some_and(Path::exists) {
        return Err(setup("policy_missing", file));
    }
    let (bytes, doc) = read_json(path).ok_or_else(|| setup("policy_unreadable", file))?;
    check_format(
        &doc,
        POLICY_FORMAT,
        "policy_invalid",
        "policy_version",
        file,
    )?;
    let invalid = || setup("policy_invalid", file);
    let mut policy: Policy = typed(&bytes).ok_or_else(invalid)?;
    policy.sha256 = crate::sha256_hex(&bytes);
    let ok = is_slug(&policy.name)
        && policy.version >= 1
        && policy.schema.version >= 1
        && policy.min_cell_count >= 1
        && policy.statistic_min_support.is_none_or(|n| n >= 1)
        // 17 digits name every double, so more would be no rule at all.
        && policy
            .statistic_significant_digits
            .is_none_or(|d| (1..=17).contains(&d));
    if !ok {
        return Err(invalid());
    }
    // A column minimum raises the policy's, never lowers or restates it.
    let lowers = |col: &Column| {
        col.min_support
            .zip(policy.statistic_min_support)
            .is_some_and(|(own, policy)| own <= policy)
    };
    if policy.schema.name != schema.name
        || policy.schema.version != schema.version
        || schema.files.iter().flat_map(|f| &f.columns).any(lowers)
    {
        return Err(setup("policy_schema_mismatch", file));
    }
    if policy.count_units_required {
        for f in &schema.files {
            let bare = f
                .columns
                .iter()
                .find(|c| c.role.is_count() && c.units.is_none());
            if let Some(col) = bare {
                return Err(Refusal::setup(Finding::column(
                    "units_required",
                    &f.name,
                    &col.name,
                )));
            }
        }
    }
    Ok(policy)
}
