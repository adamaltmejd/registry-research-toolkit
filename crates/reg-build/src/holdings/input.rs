//! Closed, pinned steward input contract; no legacy inventory is read at runtime.
use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
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
    pub series_root: String,
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
    // Conversion provenance only. These legacy paths never become runtime inputs.
    pub inventory: Option<String>,
    pub providers: Option<String>,
    pub slugs: Option<String>,
    pub delivery_docs: Option<String>,
    pub readers_root: Option<String>,
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
pub(super) fn path(root: &Path, relative: &str) -> Result<PathBuf, String> {
    let rel = Path::new(relative);
    if rel.as_os_str().is_empty() || rel.components().any(|p| !matches!(p, Component::Normal(_))) {
        return Err("holdings_input_path_invalid: expected a relative input path".into());
    }
    let full = root
        .join(rel)
        .canonicalize()
        .map_err(|_| "holdings_input_missing: declared input is absent".to_owned())?;
    let root = root.canonicalize().map_err(|e| e.to_string())?;
    if !full.starts_with(root) {
        return Err("holdings_input_path_invalid: input escapes steward checkout".into());
    }
    Ok(full)
}

fn git(root: &Path, args: &[&str]) -> Result<String, String> {
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

#[allow(clippy::too_many_lines)]
pub(super) fn admit(root: &Path, pin: &Pin, owner: &str) -> Result<Config, String> {
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
        &config.series_root,
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
    // Legacy migration paths are accepted as provenance, not implicit source authority.
    let _ = (
        &config.inputs.inventory,
        &config.inputs.providers,
        &config.inputs.slugs,
        &config.inputs.delivery_docs,
        &config.inputs.readers_root,
    );
    Ok(config)
}

fn check(root: &Path, rel: &str, pins: &BTreeMap<String, String>) -> Result<(), String> {
    let expected = pins
        .get(rel)
        .ok_or("holdings_input_unpinned: consumed input has no hash")?;
    if expected.len() != 64 || crate::sha256_file(&path(root, rel)?)?.0 != *expected {
        return Err("holdings_input_digest_mismatch: declared input changed".into());
    }
    Ok(())
}

/// Inputs cannot redirect directory traversal through a symlink, even if Git tracks it.
fn tree(
    dir: &Path,
    prefix: &str,
    out: &mut std::collections::BTreeSet<String>,
) -> Result<(), String> {
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
