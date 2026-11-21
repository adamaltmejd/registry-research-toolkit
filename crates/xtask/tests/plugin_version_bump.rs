//! A plugin is installed from its version string, so a change under `plugins/<name>/`
//! that keeps the version never reaches installed copies. Regression caught: a plugin
//! file edited (a skill, the schema, a manifest) while `version` in its `plugin.json`
//! files stays the one on `origin/main`, as happened to 0.12.0 after #2601 to #2622.
//!
//! The base is the tip of `origin/main`, not the merge base: CI checks out the
//! pull-request merge commit at depth 1, which holds no history, and that commit differs
//! from the main tip by exactly the PR's changes. A local branch behind main fails only
//! when it changes the plugin without bumping. If `origin/main` is absent the test
//! fetches it at depth 1; where that fails, CI fails the test and a local run skips with
//! the reason printed. On a push to main the diff is empty and the check passes.

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

/// True when `origin/main` is available, fetching it at depth 1 if needed.
fn have_base(root: &Path) -> bool {
    let present = || {
        git(
            root,
            &["rev-parse", "--verify", "-q", "refs/remotes/origin/main"],
        )
        .status
        .success()
    };
    if present() {
        return true;
    }
    let fetch = git(
        root,
        &[
            "fetch",
            "--no-tags",
            "--depth=1",
            "origin",
            "+refs/heads/main:refs/remotes/origin/main",
        ],
    );
    fetch.status.success() && present()
}

#[test]
fn plugin_change_carries_a_version_bump() {
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

    if have_base(&root) {
        for (name, version) in &versions {
            let dir = format!("plugins/{name}");
            if git(&root, &["diff", "--quiet", "origin/main", "--", &dir])
                .status
                .success()
            {
                continue;
            }
            let base = git(
                &root,
                &[
                    "show",
                    &format!("origin/main:{dir}/.claude-plugin/plugin.json"),
                ],
            );
            // A plugin absent from origin/main is new and starts at any version.
            if base.status.success() && version_of(&base.stdout, name) == *version {
                errors.push(format!(
                    "{name}: files under {dir}/ differ from origin/main but version stays \
                     {version}; bump `version` in every plugin.json"
                ));
            }
        }
    } else {
        assert!(
            std::env::var_os("CI").is_none(),
            "origin/main is not available and could not be fetched"
        );
        eprintln!("skipping the bump check: origin/main is not available");
    }
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}
