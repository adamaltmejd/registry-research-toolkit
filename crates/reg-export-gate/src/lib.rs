//! The export gate (`reg_export_gate/DESIGN.md`; Rust specifics in this
//! crate's DESIGN.md). Findings name schema files, columns, positions and rules,
//! never a value or an author's name.

mod backstop;
mod config;
mod disclosure;
mod export;
mod inventory;
mod schema;
mod structure;

use std::path::Path;

use sha2::{Digest, Sha256};

pub use backstop::identifier_shape;
pub use config::CONFIG_DIR;
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

    /// The binary's exit status for a refusal of this category.
    #[must_use]
    pub fn exit_code(self) -> u8 {
        match self {
            Code::Setup => 10,
            Code::Inventory => 11,
            Code::Structure => 12,
            Code::Disclosure => 13,
            Code::Backstop => 14,
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
    apart_from: &[&Path],
) -> Result<usize, Refusal> {
    export::check_paths(staging, output, apart_from)?;
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

/// A panic is `internal_error`, with no message kept.
fn caught<T>(f: impl FnOnce() -> Result<T, Refusal>) -> Result<T, Refusal> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f))
        .unwrap_or_else(|_| Err(Refusal::setup(Finding::rule("internal_error"))))
}

fn gate(
    staging: &Path,
    output: &Path,
    schema: Option<&Path>,
    policy: Option<&Path>,
    apart_from: &[&Path],
) -> Result<usize, Refusal> {
    // `out/` names the same folder as `out`, but a trailing separator makes
    // the OS follow a link there; every step sees the plain spelling.
    let output = &output
        .file_name()
        .map_or_else(|| output.to_path_buf(), |n| export::parent(output).join(n));
    let result = caught(|| run(staging, output, schema, policy, apart_from));
    let _ = caught(|| {
        export::write_report(staging, output, apart_from, result.as_ref().err());
        Ok(())
    });
    result
}

/// The whole gate over the complete candidate output in `staging`: the paths,
/// clearing a prior export in `output`, the schema and policy, then inventory,
/// structure, disclosure and the identifier backstop. Setup stops at its first
/// finding; each later step collects every finding over all files, and the
/// gate stops at the first step with one. Only when every file passes does it
/// write each checked table afresh, with `receipt.json`, as `output`. A refusal
/// after the paths check leaves no `output` folder (unless it held something
/// not the gate's to clear); a refusal of the paths themselves leaves `output`
/// as it was. The report goes to `.diagnostics/` beside `output` wherever that
/// touches neither it nor staging, and only if staging resolves. Staging is
/// left as it was. A panic is `internal_error`.
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
    gate(staging, output, schema, policy, &[])
}

/// The gate in server mode: the schema, policy and output come from the
/// project's config in `config_dir` (the binary passes [`CONFIG_DIR`]), so
/// the pipeline names only the project and its staging folder. The config is
/// refused if it is missing, invalid or writable by anyone but its owner;
/// staging and output may not be, or lie inside, the config folder or the
/// destination. A config refusal writes no report: the output is not known
/// to be the deployer's yet.
///
/// # Errors
///
/// The refusal, with its code and findings.
pub fn check_project(config_dir: &Path, project: &str, staging: &Path) -> Result<usize, Refusal> {
    let project = caught(|| config::load(config_dir, project))?;
    gate(
        staging,
        &project.output,
        Some(&project.schema),
        Some(&project.policy),
        &[config_dir, &project.destination],
    )
}
