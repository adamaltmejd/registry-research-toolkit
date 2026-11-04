//! `reg-export-gate`: the export gate's binary (this crate's DESIGN.md).
//!
//! - `check --staging DIR --output DIR [--schema FILE] [--policy FILE]`: the
//!   gate as the R gate's `export_gate()` runs it.
//! - `check --project NAME --staging DIR`: server mode; the schema, policy and
//!   output come from the project's config in [`CONFIG_DIR`].
//!
//! stdout gets one line, `pass` or `refused: <code>`, and nothing else is
//! printed: findings go only to the report. Exit status: 0 pass, 2 usage, then
//! the refusal's category (10 setup, 11 inventory, 12 structure, 13 disclosure,
//! 14 backstop).

use std::ffi::OsString;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use reg_export_gate::{CONFIG_DIR, Code, Refusal, check_project, export_gate};

/// Fixed text: an argument is never echoed, since the pipeline chose it.
const USAGE: &str = "usage:
  reg-export-gate check --staging DIR --output DIR [--schema FILE] [--policy FILE]
  reg-export-gate check --project NAME --staging DIR
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
}

/// The command, or `None` for anything else: an unknown mode or flag, a flag
/// given twice or without a value, or flags of the two forms mixed.
fn parse(mut args: impl Iterator<Item = OsString>) -> Option<Command> {
    if args.next()? != "check" {
        return None;
    }
    let (mut staging, mut output, mut schema, mut policy, mut project) =
        (None, None, None, None, None);
    while let Some(flag) = args.next() {
        let slot = match flag.to_str()? {
            "--staging" => &mut staging,
            "--output" => &mut output,
            "--schema" => &mut schema,
            "--policy" => &mut policy,
            "--project" => &mut project,
            _ => return None,
        };
        if slot.replace(args.next()?).is_some() {
            return None;
        }
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
        Ok(Err(refusal)) => Some(refusal.code),
        Err(_) => Some(Code::Setup),
    };
    let line = code.map_or_else(|| "pass".to_owned(), |c| format!("refused: {}", c.as_str()));
    let _ = writeln!(std::io::stdout(), "{line}");
    ExitCode::from(code.map_or(0, Code::exit_code))
}
