//! A plugin is installed from its version string, so a change under `plugins/<name>/`
//! that keeps or lowers the version never reaches installed copies. Regression caught: a
//! plugin file edited (a skill, the schema, a manifest) while `version` in its
//! `plugin.json` files is not above the base's, as happened to 0.12.0 after #2601 to #2622.
//!
//! The base is the branch's own fork point, so a main that moved on does not charge the
//! branch with plugin changes it never made:
//! - with history, `git merge-base HEAD origin/main`;
//! - in a shallow clone (CI checks out the PR merge commit at depth 1) the first parent
//!   of HEAD, read from the commit object, which lists it even when the parent is
//!   absent, and fetched by SHA at depth 1. On a PR that parent is the pinned base; on a
//!   push to main it is the previous main commit.
//!
//! Where no base can be found, CI fails the test and a local run skips with the reason
//! printed.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn git(root: &Path, args: &[&str]) -> Output {
    Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()
        .expect("git runs")
}

fn git_stdout(root: &Path, args: &[&str]) -> Option<String> {
    let out = git(root, args);
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

/// `X.Y.Z` as numbers, so versions order by value (0.9.0 < 0.13.0).
fn triple(version: &str, what: &str) -> (u64, u64, u64) {
    let parts: Vec<u64> = version
        .split('.')
        .map(|p| {
            p.parse()
                .unwrap_or_else(|_| panic!("{what}: version {version} is not X.Y.Z"))
        })
        .collect();
    match parts[..] {
        [a, b, c] => (a, b, c),
        _ => panic!("{what}: version {version} is not X.Y.Z"),
    }
}

fn version_of(json: &[u8], what: &str) -> String {
    let value: serde_json::Value = serde_json::from_slice(json).unwrap();
    value["version"]
        .as_str()
        .unwrap_or_else(|| panic!("{what}: no string `version`"))
        .to_owned()
}

/// Every `<plugin>/.<host>-plugin/plugin.json`, sorted.
fn manifests(plugin: &Path) -> Vec<PathBuf> {
    let mut found: Vec<PathBuf> = fs::read_dir(plugin)
        .unwrap()
        .map(|e| e.unwrap().path().join("plugin.json"))
        .filter(|p| p.is_file())
        .collect();
    found.sort();
    found
}

/// The commit the branch forked from, as a SHA whose objects are present.
fn base_commit(root: &Path) -> Option<String> {
    if git_stdout(root, &["rev-parse", "--is-shallow-repository"]).as_deref() != Some("true") {
        return git_stdout(root, &["merge-base", "HEAD", "origin/main"]);
    }
    let head = git_stdout(root, &["cat-file", "-p", "HEAD"])?;
    let parent = head
        .lines()
        .take_while(|l| !l.is_empty())
        .find_map(|l| l.strip_prefix("parent "))?
        .to_owned();
    git(
        root,
        &["fetch", "--no-tags", "--depth=1", "origin", &parent],
    )
    .status
    .success()
    .then_some(parent)
}

#[test]
fn plugin_change_raises_the_version() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let plugins: Vec<PathBuf> = fs::read_dir(root.join("plugins"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.is_dir())
        .collect();
    assert!(!plugins.is_empty(), "no plugin under plugins/");

    let mut errors = Vec::new();
    let mut versions = Vec::new();
    for plugin in &plugins {
        let name = plugin.file_name().unwrap().to_string_lossy().into_owned();
        let claude = plugin.join(".claude-plugin/plugin.json");
        assert!(
            claude.is_file(),
            "{name}: missing .claude-plugin/plugin.json"
        );
        let version = version_of(&fs::read(&claude).unwrap(), &name);
        for file in manifests(plugin) {
            let other = version_of(&fs::read(&file).unwrap(), &name);
            if other != version {
                errors.push(format!(
                    "{name}: {} says {other}, .claude-plugin/plugin.json says {version}",
                    file.strip_prefix(&root).unwrap().display()
                ));
            }
        }
        versions.push((name, version));
    }

    if let Some(base) = base_commit(&root) {
        for (name, version) in &versions {
            let dir = format!("plugins/{name}");
            if git(&root, &["diff", "--quiet", &base, "--", &dir])
                .status
                .success()
            {
                continue;
            }
            let old = git(
                &root,
                &["show", &format!("{base}:{dir}/.claude-plugin/plugin.json")],
            );
            // A plugin absent from the base is new and starts at any version.
            if !old.status.success() {
                continue;
            }
            let old = version_of(&old.stdout, name);
            if triple(version, name) <= triple(&old, name) {
                errors.push(format!(
                    "{name}: files under {dir}/ differ from the base but version {version} \
                         is not above {old}; raise `version` in every plugin.json"
                ));
            }
        }
    } else {
        assert!(
            std::env::var_os("CI").is_none(),
            "no base commit: need origin/main in a full clone, or a fetchable first parent"
        );
        eprintln!("skipping the bump check: no base commit (no origin/main, not shallow)");
    }
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}
