//! Test modules stay at or under 800 lines; an oversized module is split by contract
//! surface.
//!
//! Rust: every integration test file, a `.rs` file under a `tests/` directory. Python, until it is gone:
//! `test_*.py` and `conftest.py` under `tests/` or `conformance/`. Source fixtures and
//! support scripts are not test modules. Untracked, unignored files count too.

use std::fs;
use std::path::Path;
use std::process::Command;

const MAX_LINES: usize = 800;

fn is_test_module(path: &str) -> bool {
    let parts: Vec<&str> = path.split('/').collect();
    let (name, dirs) = parts.split_last().unwrap();
    if Path::new(name).extension().is_some_and(|ext| ext == "rs") {
        return dirs.contains(&"tests");
    }
    let python_test = name.starts_with("test_") || *name == "conftest.py";
    python_test && dirs.iter().any(|d| *d == "tests" || *d == "conformance")
}

#[test]
fn test_modules_stay_under_800_lines() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let out = Command::new("git")
        .arg("-C")
        .arg(&root)
        .args([
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "--",
            "*.py",
            "*.rs",
        ])
        .output()
        .unwrap();
    assert!(out.status.success(), "git ls-files failed");
    let names = String::from_utf8(out.stdout).unwrap();
    let files: Vec<&str> = names
        .lines()
        .filter(|name| is_test_module(name) && root.join(name).is_file())
        .collect();
    assert!(!files.is_empty(), "no test modules scanned");
    let oversized: Vec<String> = files
        .iter()
        .filter_map(|name| {
            let lines = fs::read_to_string(root.join(name)).unwrap().lines().count();
            (lines > MAX_LINES).then(|| format!("{name}: {lines}"))
        })
        .collect();
    assert!(
        oversized.is_empty(),
        "split by contract surface: {oversized:?}"
    );
}
