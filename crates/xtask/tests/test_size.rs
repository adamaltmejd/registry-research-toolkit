//! Test modules stay at or under 800 lines; an oversized module is split by contract
//! surface.
//!
//! Every integration test file, a `.rs` file under a `tests/` directory. Untracked,
//! unignored files count too.

use std::fs;
use std::path::Path;
use std::process::Command;

const MAX_LINES: usize = 800;

fn is_test_module(path: &str) -> bool {
    let parts: Vec<&str> = path.split('/').collect();
    let (_, dirs) = parts.split_last().unwrap();
    dirs.contains(&"tests")
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
