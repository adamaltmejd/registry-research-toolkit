//! A plugin is installed from its version string, so a change under `plugins/<name>/`
//! that keeps or lowers the version never reaches installed copies. Regression caught: a
//! plugin file edited (a skill, the schema, a manifest) while `version` in its
//! `plugin.json` files is not above the base's, as happened to 0.12.0 after #2601 to #2622.
//!
//! The base is the branch's own fork point, so a main that moved on does not charge the
//! branch with plugin changes it never made. One explicit source per environment:
//! - in CI (`GITHUB_EVENT_PATH` set; the checkout is shallow) the event's own base:
//!   `pull_request.base.sha`, or `before` for a push, fetched by SHA at depth 1. Other
//!   events and a push that creates a branch (`before` all zeros) skip;
//! - locally, `git merge-base HEAD origin/main`; without `origin/main` the test skips.
//!
//! A skip prints its reason. A base SHA that cannot be fetched fails the test, as does a
//! base manifest that exists but cannot be read.

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

/// The commit the branch forked from, as a SHA whose objects are present, or the reason
/// no base applies.
fn base_commit(root: &Path) -> Result<String, String> {
    let Some(event) = std::env::var_os("GITHUB_EVENT_PATH") else {
        return git_stdout(root, &["merge-base", "HEAD", "origin/main"])
            .ok_or_else(|| "no origin/main to take a merge base with".to_owned());
    };
    let event: serde_json::Value = serde_json::from_slice(&fs::read(&event).unwrap()).unwrap();
    let sha = event["pull_request"]["base"]["sha"]
        .as_str()
        .or_else(|| event["before"].as_str())
        .ok_or("the CI event is neither a pull request nor a push")?;
    if sha.bytes().all(|b| b == b'0') {
        return Err("the push creates a branch, so it has no previous commit".to_owned());
    }
    let present = || {
        git(root, &["cat-file", "-e", &format!("{sha}^{{commit}}")])
            .status
            .success()
    };
    let fetch = || {
        git(root, &["fetch", "--no-tags", "--depth=1", "origin", sha])
            .status
            .success()
    };
    assert!(
        present() || (fetch() && present()),
        "cannot fetch base {sha}"
    );
    Ok(sha.to_owned())
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
        triple(&version, &name);
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

    match base_commit(&root) {
        Err(reason) => eprintln!("skipping the bump check: {reason}"),
        Ok(base) => {
            for (name, version) in &versions {
                let dir = format!("plugins/{name}");
                if git(&root, &["diff", "--quiet", &base, "--", &dir])
                    .status
                    .success()
                {
                    continue;
                }
                let path = format!("{dir}/.claude-plugin/plugin.json");
                let listed = git(&root, &["ls-tree", &base, "--", &path]);
                assert!(
                    listed.status.success(),
                    "{name}: cannot list {path} in base {base}"
                );
                // A plugin absent from the base is new and starts at any version.
                if listed.stdout.is_empty() {
                    continue;
                }
                let old = git(&root, &["show", &format!("{base}:{path}")]);
                assert!(
                    old.status.success(),
                    "{name}: {path} exists in base {base} but cannot be read"
                );
                let old = version_of(&old.stdout, name);
                if triple(version, name) <= triple(&old, name) {
                    errors.push(format!(
                        "{name}: files under {dir}/ differ from the base but version {version} \
                         is not above {old}; raise `version` in every plugin.json"
                    ));
                }
            }
        }
    }
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}
