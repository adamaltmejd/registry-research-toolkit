//! `cargo xtask gh-issue`, the maintainer-author trust gate, run as a process.
//!
//! Each `tests/gh_issue/*.json` case gives the argv, the environment, the issue payload
//! `gh` holds (`null`: `gh` exits 1, a missing number) and the expected exit, stdout and
//! stderr substring. Its `why` names the regression that fails it. The one boundary faked
//! is `gh`: a sh script first on `PATH` that, like `gh`, answers only the `--json` fields
//! it is asked for, so a case fails if the gate stops requesting a field it surfaces.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use xtask::json::Json;

const FAKE_GH: &str = r#"#!/bin/sh
dir=$(dirname "$0")
[ -d "$dir/fields" ] || { echo "not found" >&2; exit 1; }
fields=""
while [ $# -gt 0 ]; do
  [ "$1" = --json ] && fields=$2
  shift
done
sep="{"
IFS=,
for f in $fields; do
  if [ -f "$dir/fields/$f" ]; then
    printf '%s"%s":' "$sep" "$f"
    cat "$dir/fields/$f"
    sep=","
  fi
done
[ "$sep" = "{" ] && printf '{'
printf '}\n'
"#;

/// A per-case directory under the system temp dir, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("xtask-gh-issue-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn str_of(json: &Json) -> &str {
    json.as_str().expect("a string")
}

/// Runs one case and returns its mismatches.
fn run_case(path: &Path) -> Vec<String> {
    let name = path.file_stem().unwrap().to_str().unwrap();
    let case: Json = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
    let field = |key: &str| case.get(key).unwrap_or_else(|| panic!("{name}: no {key}"));

    let tmp = TempDir::new(name);
    let gh = tmp.0.join("gh");
    fs::write(&gh, FAKE_GH).unwrap();
    fs::set_permissions(&gh, fs::Permissions::from_mode(0o755)).unwrap();
    if let Json::Object(members) = field("gh") {
        fs::create_dir(tmp.0.join("fields")).unwrap();
        for (key, value) in members {
            fs::write(tmp.0.join("fields").join(key), value.dumps()).unwrap();
        }
    }

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_xtask"));
    cmd.arg("gh-issue");
    let Json::Array(args) = field("args") else {
        panic!("{name}: args")
    };
    cmd.args(args.iter().map(str_of));
    cmd.env_remove("REGISTRY_MAINTAINER_LOGIN")
        .env_remove("GITHUB_REPOSITORY");
    let Json::Object(env) = field("env") else {
        panic!("{name}: env")
    };
    for (key, value) in env {
        cmd.env(key, str_of(value));
    }
    let path_var = std::env::var_os("PATH").unwrap_or_default();
    let mut paths = vec![tmp.0.clone()];
    paths.extend(std::env::split_paths(&path_var));
    cmd.env("PATH", std::env::join_paths(paths).unwrap());
    let out = cmd.output().unwrap();

    let mut errors = Vec::new();
    let Json::Number(exit) = field("exit") else {
        panic!("{name}: exit")
    };
    if out.status.code().map(i64::from) != exit.as_i64() {
        errors.push(format!(
            "{name}: exit {:?}, expected {exit}",
            out.status.code()
        ));
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    if stdout != str_of(field("stdout")) {
        errors.push(format!("{name}: stdout {stdout:?}"));
    }
    let stderr = String::from_utf8_lossy(&out.stderr);
    if let Some(needle) = case.get("stderr_contains")
        && !stderr.contains(str_of(needle))
    {
        errors.push(format!("{name}: stderr {stderr:?}"));
    }
    errors
}

#[test]
fn gh_issue_cases() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/gh_issue");
    let mut cases: Vec<PathBuf> = fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect();
    cases.sort();
    assert!(!cases.is_empty(), "no cases in {}", dir.display());
    let errors: Vec<String> = cases.iter().flat_map(|path| run_case(path)).collect();
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}
