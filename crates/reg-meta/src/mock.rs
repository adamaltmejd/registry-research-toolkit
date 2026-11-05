//! `reg-meta mock`: local mock data, offline (crates/reg-mock/DESIGN.md).
//!
//! ```text
//! reg-meta mock compile --project FILE --rules FILE --answers DIR --out FILE
//! ```
//!
//! A refusal prints `{"refusals": [...]}` (every located refusal at once) on stderr and
//! exits 2; a usage error exits 64.

use std::path::{Path, PathBuf};

use reg_mock::Refusal;
use reg_mock::compile::{compile, load_answers};
use reg_mock::rules::Rules;
use serde_json::{Value, json};

const USAGE: &str = "reg-meta mock compile --project FILE --rules FILE --answers DIR --out FILE";

pub fn main(mut args: impl Iterator<Item = String>) -> ! {
    match args.next().as_deref() {
        Some("compile") => run_compile(args),
        _ => usage("mock: expected a subcommand"),
    }
}

fn usage(message: &str) -> ! {
    eprintln!("{message}\nusage: {USAGE}");
    std::process::exit(64);
}

fn refuse(refusals: &[Refusal]) -> ! {
    eprintln!("{}", json!({ "refusals": refusals }));
    std::process::exit(2);
}

fn run_compile(mut args: impl Iterator<Item = String>) -> ! {
    let (mut project, mut rules, mut answers, mut out) = (None, None, None, None);
    while let Some(flag) = args.next() {
        let slot = match flag.as_str() {
            "--project" => &mut project,
            "--rules" => &mut rules,
            "--answers" => &mut answers,
            "--out" => &mut out,
            _ => usage(&format!("mock compile: unknown flag {flag}")),
        };
        *slot = Some(PathBuf::from(
            args.next()
                .unwrap_or_else(|| usage(&format!("{flag} needs a value"))),
        ));
    }
    let (Some(project), Some(rules), Some(answers), Some(out)) = (project, rules, answers, out)
    else {
        usage("mock compile: --project, --rules, --answers and --out are required");
    };
    let project = read_json(&project);
    let rules: Rules = reg_mock::from_json(&read_json(&rules)).unwrap_or_else(|e| {
        refuse(&[Refusal::new(
            "rules_invalid",
            rules.display().to_string(),
            e,
        )])
    });
    let answers = load_answers(&answers).unwrap_or_else(|r| refuse(&[r]));
    let spec = compile(&project, &rules, &answers).unwrap_or_else(|r| refuse(&r));
    let mut text = serde_json::to_string_pretty(&spec).expect("the spec serializes");
    text.push('\n');
    std::fs::write(&out, text).unwrap_or_else(|e| {
        refuse(&[Refusal::new("io", out.display().to_string(), e.to_string())])
    });
    std::process::exit(0);
}

fn read_json(path: &Path) -> Value {
    let at = path.display().to_string();
    let bytes = std::fs::read(path)
        .unwrap_or_else(|e| refuse(&[Refusal::new("io", at.clone(), e.to_string())]));
    serde_json::from_slice(&bytes)
        .unwrap_or_else(|e| refuse(&[Refusal::new("json_invalid", at, e.to_string())]))
}
