//! Only `reg-readers` may name a provider or a format (crates/DESIGN.md, "The catalog
//! builder").
//!
//! These words fail in the other builder crates and in the catalog reader and server,
//! which read only the catalog: providers, their template vocabulary and file formats.
//! Extend the crate list with each new reader.
//!
//! A word matches a whole run of word characters, case-insensitively, as the
//! `\b(...)\b` regex of the gate step this replaced did: every listed word is made of
//! word characters, so a bounded match is always a whole run.

use std::fs;
use std::path::{Path, PathBuf};

const NEUTRAL_CRATES: [&str; 5] = [
    "reg-frame",
    "reg-resolve",
    "reg-build",
    "reg-catalog",
    "reg-meta",
];
const WORDS: [&str; 19] = [
    "scb",
    "sos",
    "socialstyrelsen",
    "fk",
    "försäkringskassan",
    "fohm",
    "folkhälsomyndigheten",
    "lakemedelsverket",
    "läkemedelsverket",
    "pliktverket",
    "riksarkivet",
    "umu",
    "swecov",
    "xlsx",
    "calamine",
    "workbook",
    "kodlista",
    "variabelnamn",
    "värdemängd",
];
/// Words that also match with any suffix (`deldatamängder`, `datavyer`).
const PREFIXES: [&str; 2] = ["deldatamängd", "datavy"];

fn provider_word(line: &str) -> Option<&str> {
    line.split(|c: char| !(c.is_alphanumeric() || c == '_'))
        .find(|word| {
            let lower = word.to_lowercase();
            WORDS.contains(&lower.as_str()) || PREFIXES.iter().any(|p| lower.starts_with(p))
        })
}

fn files_under(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files_under(&path, out);
        } else {
            out.push(path);
        }
    }
}

#[test]
fn neutral_crates_name_no_provider() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut hits = Vec::new();
    for krate in NEUTRAL_CRATES {
        let mut files = Vec::new();
        files_under(&root.join("crates").join(krate).join("src"), &mut files);
        files.sort();
        for path in files {
            let text = fs::read_to_string(&path).unwrap();
            for (n, line) in text.lines().enumerate() {
                if let Some(word) = provider_word(line) {
                    let rel = path.strip_prefix(&root).unwrap().display();
                    hits.push(format!("{rel}:{}: {word}", n + 1));
                }
            }
        }
    }
    assert!(hits.is_empty(), "provider vocabulary:\n{}", hits.join("\n"));
}
