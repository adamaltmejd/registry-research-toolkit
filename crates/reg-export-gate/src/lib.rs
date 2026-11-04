//! The export gate (`reg_export_gate/DESIGN.md`; Rust specifics in this
//! crate's DESIGN.md). Findings name schema files, columns, positions and rules,
//! never a value or an author's name.

mod backstop;
mod disclosure;
mod export;
mod inventory;
mod schema;
mod structure;

use std::path::Path;

use sha2::{Digest, Sha256};

pub use backstop::identifier_shape;
pub use schema::{Column, ColumnType, File, Policy, Role, Schema, SchemaRef};
pub use structure::{MAX_DECODED_BYTES, MAX_FILE_BYTES, MAX_ROWS};

/// The receipt's reserved name: never a data file, never in staging.
pub const RECEIPT: &str = "receipt.json";
/// The gate version every receipt and report records.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The refusal category: the one thing a failed check tells the agent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Code {
    Setup,
    Inventory,
    Structure,
    Disclosure,
    Backstop,
}

impl Code {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Code::Setup => "setup",
            Code::Inventory => "inventory",
            Code::Structure => "structure",
            Code::Disclosure => "disclosure",
            Code::Backstop => "backstop",
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

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    Sha256::digest(bytes)
        .iter()
        .fold(String::new(), |mut hex, b| {
            let _ = write!(hex, "{b:02x}");
            hex
        })
}

fn run(
    staging: &Path,
    output: &Path,
    schema: Option<&Path>,
    policy: Option<&Path>,
) -> Result<usize, Refusal> {
    export::check_paths(staging, output)?;
    export::clear_output(output)?;
    let schema = schema::load_schema(schema)?;
    let policy = schema::load_policy(policy, &schema)?;
    inventory::check(staging, &schema)?;
    let candidates = structure::check(staging, &schema)?;
    disclosure::check(&candidates, &policy)?;
    backstop::check(&candidates)?;
    export::export(output, &schema, &policy, &candidates)?;
    Ok(candidates.len())
}

/// The whole gate over the complete candidate output in `staging`: the paths,
/// clearing a prior export in `output`, the schema and policy, then inventory,
/// structure, disclosure and the identifier backstop. Setup stops at its first
/// finding; each later step collects every finding over all files, and the
/// gate stops at the first step with one. Only when every file passes does it
/// write each checked table afresh, with `receipt.json`, as `output`. A refusal
/// after the paths check leaves no `output` folder (unless it held something
/// not the gate's to clear); a refusal of the paths themselves leaves `output`
/// as it was. Every run writes its report to `.diagnostics/` beside `output`
/// where that touches neither it nor staging. Staging is left as it was.
///
/// # Errors
///
/// The refusal, with its code and findings.
pub fn export_gate(
    staging: &Path,
    output: &Path,
    schema: Option<&Path>,
    policy: Option<&Path>,
) -> Result<usize, Refusal> {
    // `out/` names the same folder as `out`, but a trailing separator makes
    // the OS follow a link there; every step sees the plain spelling.
    let output = &output
        .file_name()
        .map_or_else(|| output.to_path_buf(), |n| export::parent(output).join(n));
    let result = run(staging, output, schema, policy);
    export::write_report(staging, output, result.as_ref().err());
    result
}
