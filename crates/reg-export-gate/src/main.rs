//! `reg-export-gate`: the export gate's binary (this crate's DESIGN.md).
//!
//! - `check --staging DIR --output DIR [--schema FILE] [--policy FILE]`: the
//!   gate as the R gate's `export_gate()` runs it.
//! - `check --project NAME --staging DIR`: server mode; the schema, policy and
//!   output come from the project's config in [`CONFIG_DIR`].
//! - `export --project NAME --receipt SHA256`: the reviewer's export of the
//!   project's checked output to its destination.
//!
//! stdout gets one line, `pass` or `refused: <code>`. `check` prints nothing
//! else (findings go only to the report, since the pipeline reads its output);
//! `export`, which the reviewer runs, prints a refusal's findings on stderr.
//! Exit status: 0 pass, 2 usage, then the refusal's category (10 setup, 11
//! inventory, 12 structure, 13 disclosure, 14 backstop, 15 delivery).

use std::ffi::OsString;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use reg_export_gate::{CONFIG_DIR, Code, Refusal, check_project, export_gate, export_project};

/// Fixed text: an argument is never echoed, since the pipeline chose it.
const USAGE: &str = "usage:
  reg-export-gate check --staging DIR --output DIR [--schema FILE] [--policy FILE]
  reg-export-gate check --project NAME --staging DIR
  reg-export-gate export --project NAME --receipt SHA256
";

enum Command {
    Check {
        staging: PathBuf,
        output: PathBuf,
        schema: Option<PathBuf>,
        policy: Option<PathBuf>,
    },
    CheckProject {
        project: String,
        staging: PathBuf,
    },
    Export {
        project: String,
        receipt: String,
    },
}

/// 64 lowercase hex digits, as `sha256sum` prints them.
fn is_sha256(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// The command, or `None` for anything else: an unknown mode or flag, a flag
/// given twice, without a value or not of its mode, or flags of the two
/// `check` forms mixed.
fn parse(mut args: impl Iterator<Item = OsString>) -> Option<Command> {
    let mode = args.next()?;
    let (mut staging, mut output, mut schema, mut policy, mut project, mut receipt) =
        (None, None, None, None, None, None);
    while let Some(flag) = args.next() {
        let slot = match flag.to_str()? {
            "--staging" => &mut staging,
            "--output" => &mut output,
            "--schema" => &mut schema,
            "--policy" => &mut policy,
            "--project" => &mut project,
            "--receipt" => &mut receipt,
            _ => return None,
        };
        if slot.replace(args.next()?).is_some() {
            return None;
        }
    }
    if mode == "export" {
        if staging.is_some() || output.is_some() || schema.is_some() || policy.is_some() {
            return None;
        }
        let receipt = receipt?.into_string().ok().filter(|r| is_sha256(r))?;
        return Some(Command::Export {
            project: project?.into_string().ok()?,
            receipt,
        });
    }
    if mode != "check" || receipt.is_some() {
        return None;
    }
    let staging = PathBuf::from(staging?);
    match project {
        Some(project) if output.is_none() && schema.is_none() && policy.is_none() => {
            Some(Command::CheckProject {
                project: project.into_string().ok()?,
                staging,
            })
        }
        Some(_) => None,
        None => Some(Command::Check {
            staging,
            output: output?.into(),
            schema: schema.map(PathBuf::from),
            policy: policy.map(PathBuf::from),
        }),
    }
}

fn run(command: &Command) -> Result<usize, Refusal> {
    match command {
        Command::Check {
            staging,
            output,
            schema,
            policy,
        } => export_gate(staging, output, schema.as_deref(), policy.as_deref()),
        Command::CheckProject { project, staging } => {
            check_project(Path::new(CONFIG_DIR), project, staging)
        }
        Command::Export { project, receipt } => {
            export_project(Path::new(CONFIG_DIR), project, receipt).map(|_| 0)
        }
    }
}

/// One line per finding on stderr, for the reviewer: the rule, then where.
/// A finding never holds a value (`Finding`).
fn print_findings(refusal: &Refusal) {
    let mut err = std::io::stderr().lock();
    for f in &refusal.findings {
        let _ = write!(err, "{}", f.rule);
        if let Some(file) = &f.file {
            let _ = write!(err, " file={file}");
        }
        if let Some(column) = &f.column {
            let _ = write!(err, " column={column}");
        }
        if let Some(position) = f.position {
            let _ = write!(err, " position={position}");
        }
        let _ = writeln!(err);
    }
}

fn main() -> ExitCode {
    // Silent for every panic, before the library installs its decoder hook
    // (which defers to this one): the default hook prints the message, and a
    // message can quote a value.
    std::panic::set_hook(Box::new(|_| {}));
    let Some(command) = parse(std::env::args_os().skip(1)) else {
        let _ = std::io::stderr().write_all(USAGE.as_bytes());
        return ExitCode::from(2);
    };
    // The library catches its own panics; this catches the rest.
    let code = match std::panic::catch_unwind(|| run(&command)) {
        Ok(Ok(_)) => None,
        Ok(Err(refusal)) => {
            if matches!(command, Command::Export { .. }) {
                print_findings(&refusal);
            }
            Some(refusal.code)
        }
        Err(_) => Some(Code::Setup),
    };
    let line = code.map_or_else(|| "pass".to_owned(), |c| format!("refused: {}", c.as_str()));
    let _ = writeln!(std::io::stdout(), "{line}");
    ExitCode::from(code.map_or(0, Code::exit_code))
}
