//! `reg-mock`: local mock data, offline (`DESIGN.md`).
//!
//! ```text
//! reg-mock compile --project FILE --rules FILE --answers DIR --out FILE
//! reg-mock generate --spec FILE --out DIR [--seed N] [--persons N] [--allow-large]
//! ```
//!
//! `generate` writes one file per spec file into `DIR` in the spec's format, and
//! `mock_manifest.json`. `--seed` defaults to 1 and `--persons` to 10 000; more than
//! 1 000 000 persons needs `--allow-large`.
//!
//! A refusal prints `{"refusals": [...]}` (every located refusal at once) on stderr and
//! exits 2; a usage error exits 64.

use std::path::{Path, PathBuf};

use reg_mock::Refusal;
use reg_mock::compile::{compile, load_answers};
use reg_mock::rules::Rules;
use reg_mock::spec::{SPEC_VERSION, Spec};
use reg_mock::{generate, write};
use serde_json::{Value, json};

const USAGE: &str = "reg-mock compile --project FILE --rules FILE --answers DIR --out FILE\n       \
                     reg-mock generate --spec FILE --out DIR [--seed N] [--persons N] [--allow-large]";

fn main() {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("compile") => run_compile(args),
        Some("generate") => run_generate(args),
        _ => usage("expected a subcommand"),
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
            _ => usage(&format!("compile: unknown flag {flag}")),
        };
        *slot = Some(PathBuf::from(
            args.next()
                .unwrap_or_else(|| usage(&format!("{flag} needs a value"))),
        ));
    }
    let (Some(project), Some(rules), Some(answers), Some(out)) = (project, rules, answers, out)
    else {
        usage("compile: --project, --rules, --answers and --out are required");
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

fn run_generate(mut args: impl Iterator<Item = String>) -> ! {
    let (mut spec, mut out, mut seed, mut persons, mut allow_large) =
        (None, None, None, None, false);
    while let Some(flag) = args.next() {
        if flag == "--allow-large" {
            allow_large = true;
            continue;
        }
        let slot = match flag.as_str() {
            "--spec" => &mut spec,
            "--out" => &mut out,
            "--seed" => &mut seed,
            "--persons" => &mut persons,
            _ => usage(&format!("generate: unknown flag {flag}")),
        };
        *slot = Some(
            args.next()
                .unwrap_or_else(|| usage(&format!("{flag} needs a value"))),
        );
    }
    let (Some(spec), Some(out)) = (spec, out) else {
        usage("generate: --spec and --out are required");
    };
    let number = |value: Option<String>, flag: &str, default: u64| {
        value.map_or(default, |v| {
            v.parse()
                .unwrap_or_else(|_| usage(&format!("{flag} takes a whole number")))
        })
    };
    let seed = number(seed, "--seed", 1);
    let persons = number(persons, "--persons", generate::DEFAULT_PERSONS);
    let spec_path = PathBuf::from(spec);
    let spec: Spec = reg_mock::from_json(&read_json(&spec_path)).unwrap_or_else(|e| {
        refuse(&[Refusal::new(
            "spec_invalid",
            spec_path.display().to_string(),
            e,
        )])
    });
    if spec.version != SPEC_VERSION {
        refuse(&[Refusal::new(
            "spec_version",
            spec_path.display().to_string(),
            format!("spec version must be {SPEC_VERSION}: compile it again"),
        )]);
    }
    let refusals = reg_mock::compile::check_spec(&spec);
    if !refusals.is_empty() {
        refuse(&refusals);
    }
    let tables =
        generate::generate(&spec, seed, persons, allow_large).unwrap_or_else(|r| refuse(&[r]));
    write::write_all(&spec, &tables, Path::new(&out), seed, persons)
        .unwrap_or_else(|r| refuse(&[r]));
    std::process::exit(0);
}
