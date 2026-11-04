//! The export gate's check (`reg_export_gate/DESIGN.md`; Rust specifics in this
//! crate's DESIGN.md). Findings name schema files, columns, positions and rules,
//! never a value or an author's name.

mod inventory;
mod schema;
mod structure;

use std::path::Path;

pub use schema::{Column, ColumnType, File, Policy, Role, Schema};
pub use structure::Candidate;

/// The receipt's reserved name: never a data file, never in staging.
pub const RECEIPT: &str = "receipt.json";

/// The refusal category: the one thing a failed check tells the agent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Code {
    Setup,
    Inventory,
    Structure,
}

impl Code {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Code::Setup => "setup",
            Code::Inventory => "inventory",
            Code::Structure => "structure",
        }
    }
}

/// One located finding. `file` and `column` are names the schema declares (or
/// the receipt's); an undeclared file or column is located by its 1-based
/// `position`, since its name is the author's and can be a value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Finding {
    pub file: Option<String>,
    pub column: Option<String>,
    pub position: Option<usize>,
    pub rule: &'static str,
}

impl Finding {
    pub(crate) fn rule(rule: &'static str) -> Self {
        Finding {
            file: None,
            column: None,
            position: None,
            rule,
        }
    }

    pub(crate) fn file(rule: &'static str, file: &str) -> Self {
        Finding {
            file: Some(file.to_owned()),
            ..Finding::rule(rule)
        }
    }

    pub(crate) fn column(rule: &'static str, file: &str, column: &str) -> Self {
        Finding {
            column: Some(column.to_owned()),
            ..Finding::file(rule, file)
        }
    }

    pub(crate) fn position(rule: &'static str, file: Option<&str>, position: usize) -> Self {
        Finding {
            file: file.map(str::to_owned),
            position: Some(position),
            ..Finding::rule(rule)
        }
    }
}

/// A refused check: its category and every finding of the step that refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refusal {
    pub code: Code,
    pub findings: Vec<Finding>,
}

impl Refusal {
    pub(crate) fn setup(finding: Finding) -> Self {
        Refusal {
            code: Code::Setup,
            findings: vec![finding],
        }
    }
}

/// What passed the check so far: the schema and policy in force and each
/// schema file decoded from the bytes read once.
#[derive(Debug)]
pub struct Checked {
    pub schema: Schema,
    pub policy: Policy,
    pub candidates: Vec<Candidate>,
}

/// Loads the schema and policy, then checks `staging`: inventory, then
/// structure. Setup stops at its first finding; each later step collects every
/// finding over all files, and the check stops at the first step with one.
///
/// # Errors
///
/// The refusal, with its code and findings.
pub fn check(
    staging: &Path,
    schema: Option<&Path>,
    policy: Option<&Path>,
) -> Result<Checked, Refusal> {
    if !staging.is_dir() {
        return Err(Refusal::setup(Finding::rule("staging_missing")));
    }
    let schema = schema::load_schema(schema)?;
    let policy = schema::load_policy(policy, &schema)?;
    inventory::check(staging, &schema)?;
    let candidates = structure::check(staging, &schema)?;
    Ok(Checked {
        schema,
        policy,
        candidates,
    })
}
