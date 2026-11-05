//! The repository holds no Python: no tracked `.py` file and no Python project files.
//! Tooling is Rust (`crates/xtask`), shell (`scripts/gate.sh`) or bun; a Python file would
//! bring back a toolchain that CI no longer installs. Untracked, unignored files count too.

use std::path::Path;
use std::process::Command;

#[test]
fn repository_holds_no_python() {
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
            "*pyproject.toml",
            "*uv.lock",
            "*.python-version",
        ])
        .output()
        .unwrap();
    assert!(out.status.success(), "git ls-files failed");
    let names = String::from_utf8(out.stdout).unwrap();
    // A deleted but still-indexed file is not in the tree.
    let python: Vec<&str> = names
        .lines()
        .filter(|name| root.join(name).exists())
        .collect();
    assert!(python.is_empty(), "Python in the repository: {python:?}");
}
