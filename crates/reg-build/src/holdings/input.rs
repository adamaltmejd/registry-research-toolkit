//! Closed steward input contract, pinned by the clean checkout; no legacy inventory is
//! read at runtime.
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Component, Path, PathBuf};
use std::process::Command;

use crate::error::Result;
use serde::{Deserialize, Serialize};

/// A steward checkout's identity: its `origin` URL, `HEAD` commit and config path. A
/// holdings build reads it from the checkout; `verify` reads it back from the artifact.
pub(super) struct Pin {
    pub repo: String,
    pub commit: String,
    pub config: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    pub owner: String,
    pub scope: String,
    pub curation_root: String,
    pub presentation: Presentation,
    pub inputs: Inputs,
    pub pins: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Presentation {
    pub steward: String,
    pub logo: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Inputs {
    pub reference_occurrences: Option<String>,
    pub census: String,
    pub storage_export: String,
    pub holdings: String,
    pub census_reader: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Topology {
    pub table: Vec<Table>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Table {
    pub physical_name: String,
    pub schema_name: Option<String>,
    pub disposition: String,
    pub delivery: Option<String>,
    #[serde(default)]
    #[serde(rename = "table_pids")]
    pub routes: Vec<String>,
    pub locator: String,
    pub reason: Option<String>,
    pub partition: Option<String>,
    pub projection_of: Option<String>,
    pub projection_token: Option<String>,
    pub period_column: Option<String>,
    pub period_encoding: Option<String>,
    #[serde(default)]
    pub column: Vec<Column>,
    #[serde(default)]
    pub key: Vec<RowKey>,
}

/// Reviewed entity/time components of a served shape, not every identifier.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct RowKey {
    pub entity: Vec<String>,
    #[serde(default)]
    pub time: Vec<String>,
    pub period: Option<String>,
    pub period_key: Option<String>,
    pub hold: Option<String>,
    pub follow_up: Option<String>,
    pub locator: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Column {
    pub name: String,
    pub slot_family: Option<String>,
    pub slot_kind: Option<String>,
    pub slot_index: Option<SlotIndex>,
    pub hold: Option<String>,
    pub unmap: Option<String>,
    pub follow_up: Option<String>,
    pub locator: String,
    #[serde(default)]
    pub mapping: Vec<Mapping>,
}

#[derive(Deserialize)]
#[serde(untagged)]
pub(super) enum SlotIndex {
    Number(i64),
    Text(String),
}

impl std::fmt::Display for SlotIndex {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Number(n) => n.fmt(formatter),
            Self::Text(s) => s.fmt(formatter),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Mapping {
    pub table_pid: String,
    pub variable_pid: String,
    pub delivery: Option<String>,
    pub caveat: Option<String>,
    pub locator: String,
    pub hold: Option<String>,
    pub follow_up: Option<String>,
    #[serde(default)]
    pub carried: bool,
}

/// Relative paths only, including after symlink resolution.
pub(super) fn path(root: &Path, relative: &str) -> Result<PathBuf> {
    let rel = Path::new(relative);
    if rel.as_os_str().is_empty() || rel.components().any(|p| !matches!(p, Component::Normal(_))) {
        return Err("holdings_input_path_invalid: expected a relative input path".into());
    }
    let full = root
        .join(rel)
        .canonicalize()
        .map_err(|_| "holdings_input_missing: declared input is absent".to_owned())?;
    let root = root.canonicalize()?;
    if !full.starts_with(root) {
        return Err("holdings_input_path_invalid: input escapes steward checkout".into());
    }
    Ok(full)
}

fn git(root: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()
        .map_err(|e| format!("holdings_pin_invalid: git: {e}"))?;
    if !output.status.success() {
        return Err("holdings_pin_invalid: cannot inspect supplied checkout".into());
    }
    String::from_utf8(output.stdout)
        .map(|s| s.trim().to_owned())
        .map_err(|_| "holdings_pin_invalid: invalid git output".into())
}

/// The pin of the checkout at `root` as it stands; `admit` then refuses a dirty tree.
pub(super) fn checkout(root: &Path, config: &str) -> Result<Pin> {
    Ok(Pin {
        repo: git(root, &["remote", "get-url", "origin"])?,
        commit: git(root, &["rev-parse", "HEAD"])?,
        config: config.to_owned(),
    })
}

#[allow(clippy::too_many_lines)]
pub(super) fn admit(root: &Path, pin: &Pin, owner: &str) -> Result<Config> {
    if pin.commit.len() != 40
        || !pin.commit.bytes().all(|b| b.is_ascii_hexdigit())
        || git(root, &["rev-parse", "HEAD"])? != pin.commit
        || git(root, &["remote", "get-url", "origin"])? != pin.repo
        || !git(root, &["status", "--porcelain", "--untracked-files=all"])?.is_empty()
    {
        return Err("holdings_pin_invalid: checkout must match the exact clean steward pin".into());
    }
    let config_path = path(root, &pin.config)?;
    let committed = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["show", &format!("{}:{}", pin.commit, pin.config)])
        .output()
        .map_err(|_| "holdings_pin_invalid: cannot inspect pinned config")?;
    if !committed.status.success()
        || committed.stdout != std::fs::read(&config_path).map_err(|_| "holdings_config_invalid")?
    {
        return Err("holdings_pin_invalid: config bytes differ from pinned commit".into());
    }
    let config: Config = crate::toml_file(&config_path)
        .map_err(|_| "holdings_config_invalid: malformed steward config".to_owned())?;
    if config.owner != owner || !reg_core::is_slug(owner) {
        return Err(
            "holdings_owner_invalid: configured owner differs from selected steward".into(),
        );
    }
    let curation = Path::new(&config.curation_root)
        .parent()
        .ok_or("holdings_input_path_invalid: curation_root has no parent")?
        .to_string_lossy()
        .into_owned();
    let mut consumed = vec![
        &config.scope,
        &curation,
        &config.presentation.steward,
        &config.presentation.logo,
        &config.inputs.census,
        &config.inputs.storage_export,
        &config.inputs.holdings,
        &config.inputs.census_reader,
    ];
    let manifest: crate::Manifest =
        crate::toml_file(&path(root, &format!("{curation}/inputs.toml"))?)
            .map_err(|_| "holdings_config_invalid: malformed supplemental manifest".to_owned())?;
    if manifest.input.iter().any(|input| input.release.is_some()) {
        return Err(
            "holdings_input_invalid: supplemental inputs must be inside the pinned checkout".into(),
        );
    }
    consumed.extend(config.inputs.reference_occurrences.iter());
    consumed.extend(manifest.input.iter().map(|input| &input.path));
    for rel in consumed {
        let full = path(root, rel)?;
        if full.is_dir() {
            let mut files = std::collections::BTreeSet::new();
            tree(&full, "", &mut files)?;
            if let Some(expected) = config.pins.get(rel) {
                if *expected != crate::digest(&full, &files)?.0 {
                    return Err("holdings_input_digest_mismatch: declared input changed".into());
                }
            } else {
                for file in &files {
                    check(root, &format!("{rel}/{file}"), &config.pins)?;
                }
            }
        } else {
            check(root, rel, &config.pins)?;
        }
    }
    Ok(config)
}

/// Recompute the digest of every path already in the config's `[pins]` and rewrite the
/// values that changed, in place: the step after a re-lock rewrites `curation.lock`
/// files. It adds and drops no pin, so what the steward pins stays a reviewed change.
/// Returns the number of pins rewritten.
pub(super) fn refresh_pins(root: &Path, config: &str) -> Result<usize> {
    // Only `[pins]`, with each value's byte span, so the rewrite keeps every other
    // byte (comments, spacing, line endings) as the steward wrote it.
    #[derive(Deserialize)]
    struct Spans {
        pins: BTreeMap<String, toml::Spanned<String>>,
    }
    let config_path = path(root, config)?;
    let _: Config = crate::toml_file(&config_path)
        .map_err(|_| "holdings_config_invalid: malformed steward config".to_owned())?;
    let mut text = std::fs::read_to_string(&config_path)
        .map_err(|_| "holdings_config_invalid: unreadable steward config")?;
    let spans: Spans = toml::from_str(&text)
        .map_err(|_| "holdings_config_invalid: malformed steward config".to_owned())?;
    let mut edits = Vec::new();
    for (rel, old) in &spans.pins {
        let full = path(root, rel)?;
        let new = if full.is_dir() {
            let mut files = std::collections::BTreeSet::new();
            tree(&full, "", &mut files)?;
            crate::digest(&full, &files)?.0
        } else {
            crate::sha256_file(&full)?.0
        };
        if new != *old.get_ref() {
            edits.push((old.span(), new));
        }
    }
    // Back to front, so an earlier span stays valid after a later one is rewritten.
    edits.sort_by_key(|(span, _)| std::cmp::Reverse(span.start));
    for (span, new) in &edits {
        text.replace_range(span.clone(), &format!("\"{new}\""));
    }
    std::fs::write(&config_path, text)
        .map_err(|e| format!("holdings_config_invalid: write {config}: {e}"))?;
    Ok(edits.len())
}

/// One digest over the pinned input list, so the artifact names its inputs without
/// carrying their paths. The per-file pins stay in the build report.
pub(super) fn manifest_digest(pins: &BTreeMap<String, String>) -> String {
    reg_core::sha256_hex(pin_lines(pins))
}

/// `path\tsha256\n` per pin, in path order.
pub(super) fn pin_lines(pins: &BTreeMap<String, String>) -> String {
    pins.iter().fold(String::new(), |mut lines, (path, sha)| {
        writeln!(lines, "{path}\t{sha}").expect("write to String");
        lines
    })
}

fn check(root: &Path, rel: &str, pins: &BTreeMap<String, String>) -> Result<()> {
    let expected = pins
        .get(rel)
        .ok_or("holdings_input_unpinned: consumed input has no hash")?;
    if expected.len() != 64 || crate::sha256_file(&path(root, rel)?)?.0 != *expected {
        return Err("holdings_input_digest_mismatch: declared input changed".into());
    }
    Ok(())
}

/// Inputs cannot redirect directory traversal through a symlink, even if Git tracks it.
fn tree(dir: &Path, prefix: &str, out: &mut std::collections::BTreeSet<String>) -> Result<()> {
    for entry in std::fs::read_dir(dir).map_err(|_| "holdings_input_missing")? {
        let entry = entry.map_err(|_| "holdings_input_missing")?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| "holdings_input_path_invalid")?;
        if name.starts_with('.') {
            continue;
        }
        let kind = entry.file_type().map_err(|_| "holdings_input_missing")?;
        if kind.is_symlink() {
            return Err("holdings_input_path_invalid: directory contains a symlink".into());
        }
        let rel = if prefix.is_empty() {
            name
        } else {
            format!("{prefix}/{name}")
        };
        if kind.is_dir() {
            tree(&entry.path(), &rel, out)?;
        } else {
            out.insert(rel);
        }
    }
    Ok(())
}
