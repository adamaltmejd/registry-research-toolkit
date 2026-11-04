//! Server mode's project config (this crate's DESIGN.md, "Server mode"). The
//! pipeline names a project; the deployer's config names its schema, policy,
//! output folder and destination, so the pipeline chooses none of them.

use std::path::{Component, Path, PathBuf};

use serde::Deserialize;
use serde_json::Value;

use crate::{Finding, Refusal};

/// Where server mode reads project configs: fixed in the binary, never taken
/// from the environment or the arguments, which the pipeline controls.
pub const CONFIG_DIR: &str = "/etc/reg-export-gate";
const PROJECT_FORMAT: &str = "reg-export-gate/project";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Raw {
    #[allow(dead_code, reason = "checked on the parsed value first")]
    format: String,
    #[allow(dead_code, reason = "checked on the parsed value first")]
    format_version: u64,
    schema: String,
    policy: String,
    output: PathBuf,
    destination: PathBuf,
}

/// A loaded project: the schema and policy resolved inside the config folder,
/// the output and destination absolute.
pub(crate) struct Project {
    pub schema: PathBuf,
    pub policy: PathBuf,
    pub output: PathBuf,
    pub destination: PathBuf,
    /// The config folder, resolved.
    pub dir: PathBuf,
}

/// The user that owns `path`; `None` off Unix or if it cannot be read.
#[cfg(unix)]
pub(crate) fn owner(path: &Path) -> Option<u32> {
    use std::os::unix::fs::MetadataExt;
    std::fs::symlink_metadata(path).ok().map(|m| m.uid())
}

#[cfg(not(unix))]
pub(crate) fn owner(_: &Path) -> Option<u32> {
    None
}

fn setup(rule: &'static str) -> Refusal {
    Refusal::setup(Finding::rule(rule))
}

/// `rel` under the config folder `dir` (resolved): relative, plain names only,
/// no link at any step (a link in a folder someone else can write could be
/// retargeted after the check), and a regular file (a FIFO would block the
/// read). `None` otherwise, or if it does not exist.
fn resolve(dir: &Path, rel: &str) -> Option<PathBuf> {
    let rel = Path::new(rel);
    let plain = rel.components().all(|c| matches!(c, Component::Normal(_)));
    if !plain || rel.as_os_str().is_empty() {
        return None;
    }
    let mut path = dir.to_path_buf();
    for part in rel.components() {
        path.push(part);
        let meta = std::fs::symlink_metadata(&path).ok()?;
        if meta.file_type().is_symlink() {
            return None;
        }
    }
    std::fs::symlink_metadata(&path)
        .is_ok_and(|m| m.is_file())
        .then_some(path)
}

/// Whether anyone but the owner of `dir` could write `path`: a group or other
/// write bit, or another owner (root aside), who could set one.
#[cfg(unix)]
fn writable_by_others(path: &Path, dir: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    let (Ok(meta), Ok(top)) = (std::fs::metadata(path), std::fs::metadata(dir)) else {
        return true;
    };
    meta.mode() & 0o022 != 0 || (meta.uid() != top.uid() && meta.uid() != 0)
}

/// Off Unix the mode bits are not there to read, so nothing passes.
#[cfg(not(unix))]
fn writable_by_others(_: &Path, _: &Path) -> bool {
    true
}

/// Whether anyone but its owner could write `file` (inside `dir`, reached
/// without links) or any folder from it up to `dir`: whoever can, can choose
/// the config.
fn writable(file: &Path, dir: &Path) -> bool {
    file.ancestors()
        .take_while(|a| a.starts_with(dir))
        .any(|a| writable_by_others(a, dir))
}

fn absolute(path: &Path) -> bool {
    path.is_absolute() && path.file_name().is_some()
}

/// Loads `<config_dir>/projects/<name>.json`. `config_missing` if the folder,
/// the name (not a slug) or the file is not there; `config_invalid` if it is
/// a regular file reached without links, not a closed format-1 project config,
/// its schema or policy is not such a file inside the config folder, or its output or destination is not absolute;
/// `config_writable` if anyone but the config folder's owner (or root) could
/// write the config, the schema or the policy, or a folder they are in up to
/// the config folder.
pub(crate) fn load(config_dir: &Path, name: &str) -> Result<Project, Refusal> {
    let dir = config_dir
        .canonicalize()
        .map_err(|_| setup("config_missing"))?;
    let rel = format!("projects/{name}.json");
    if !crate::schema::is_slug(name) || std::fs::symlink_metadata(dir.join(&rel)).is_err() {
        return Err(setup("config_missing"));
    }
    let invalid = || setup("config_invalid");
    let file = resolve(&dir, &rel).ok_or_else(invalid)?;
    let bytes = std::fs::read(&file).map_err(|_| invalid())?;
    let doc: Value = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    if doc.get("format").and_then(Value::as_str) != Some(PROJECT_FORMAT)
        || doc.get("format_version").and_then(Value::as_u64) != Some(1)
    {
        return Err(invalid());
    }
    // From the bytes, so a duplicate key is refused (as in `schema.rs`).
    let raw: Raw = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    if !absolute(&raw.output) || !absolute(&raw.destination) {
        return Err(invalid());
    }
    let schema = resolve(&dir, &raw.schema).ok_or_else(invalid)?;
    let policy = resolve(&dir, &raw.policy).ok_or_else(invalid)?;
    if [&file, &schema, &policy].iter().any(|f| writable(f, &dir)) {
        return Err(setup("config_writable"));
    }
    Ok(Project {
        schema,
        policy,
        output: raw.output,
        destination: raw.destination,
        dir,
    })
}
