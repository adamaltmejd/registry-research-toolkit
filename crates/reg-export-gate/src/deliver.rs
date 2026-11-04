//! `export`: the reviewer's step after inspecting a server-mode output folder
//! (this crate's DESIGN.md, "Export and delivery"). It re-derives every byte
//! it delivers from what it reads and checks, refuses anything a genuine gate
//! run would not have written, and delivers its own bytes, never the folder's.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::{self, owner};
use crate::export::{apart_from_roots, place, rewritten};
use crate::structure::{self, Outcome};
use crate::{Code, Finding, RECEIPT, Refusal, backstop, disclosure, schema, sha256_hex};

/// Hex digits of the receipt's hash that name the export's folder at the
/// destination.
const FOLDER_DIGITS: usize = 16;
fn delivery(rule: &'static str) -> Refusal {
    Refusal {
        code: Code::Delivery,
        findings: vec![Finding::rule(rule)],
    }
}

fn setup(rule: &'static str) -> Refusal {
    Refusal::setup(Finding::rule(rule))
}

/// A real folder (not a link) with no group or other write bit.
#[cfg(unix)]
fn owner_only(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::symlink_metadata(path).is_ok_and(|m| m.is_dir() && m.permissions().mode() & 0o022 == 0)
}

/// Off Unix the mode bits are not there to read, so nothing passes.
#[cfg(not(unix))]
fn owner_only(_: &Path) -> bool {
    false
}

/// Whether only `uid` could change what `dir` (resolved) names: it is owned by
/// `uid` (or root, if `root_ok`), and it and every folder above it have no
/// group or other write bit (a sticky folder above aside, which lets others
/// add entries but not move ours) and are owned by `uid` or root. Otherwise
/// someone else could move the folder aside and put their own or a link in
/// its place.
#[cfg(unix)]
fn reviewer_only(dir: &Path, uid: u32, root_ok: bool) -> bool {
    use std::os::unix::fs::MetadataExt;
    dir.ancestors().enumerate().all(|(i, a)| {
        std::fs::symlink_metadata(a).is_ok_and(|m| {
            let above = i > 0;
            let sticky = above && m.mode() & 0o1000 != 0;
            let owner = m.uid() == uid || ((above || root_ok) && m.uid() == 0);
            m.is_dir() && owner && (m.mode() & 0o022 == 0 || sticky)
        })
    })
}

#[cfg(not(unix))]
fn reviewer_only(_: &Path, _: u32, _: bool) -> bool {
    false
}

/// The user this process writes as: the owner of a folder it creates (and
/// removes) in `dir`. `std` has no `geteuid`, and this crate has no `unsafe`.
fn writer(dir: &Path) -> Result<u32, Refusal> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let probe = dir.join(format!(".export-gate-probe-{}-{nanos}", std::process::id()));
    std::fs::create_dir(&probe).map_err(|_| setup("export_not_writable"))?;
    let uid = owner(&probe);
    let _ = std::fs::remove_dir(&probe);
    uid.ok_or_else(|| setup("internal_error"))
}

/// The output folder's entries, each read once without following a link, or
/// `None` unless it is a real folder holding exactly `names`, all regular
/// files.
fn read_output(output: &Path, names: &[&str]) -> Option<HashMap<String, Outcome<Vec<u8>>>> {
    if !std::fs::symlink_metadata(output).ok()?.is_dir() {
        return None;
    }
    let mut files = HashMap::new();
    for entry in std::fs::read_dir(output).ok()? {
        let entry = entry.ok()?;
        let name = entry.file_name().into_string().ok()?;
        if !entry.file_type().ok()?.is_file() || !names.contains(&name.as_str()) {
            return None;
        }
        let bytes = structure::read(&output.join(&name));
        files.insert(name, bytes);
    }
    (files.len() == names.len()).then_some(files)
}

/// Whether `folder` is a real folder holding exactly `files`, each with the
/// same SHA-256.
fn holds(folder: &Path, files: &[(String, Vec<u8>)]) -> bool {
    if !std::fs::symlink_metadata(folder).is_ok_and(|m| m.is_dir()) {
        return false;
    }
    let Ok(entries) = std::fs::read_dir(folder) else {
        return false;
    };
    entries.count() == files.len()
        && files.iter().all(|(name, bytes)| {
            std::fs::read(folder.join(name)).is_ok_and(|b| sha256_hex(&b) == sha256_hex(bytes))
        })
}

/// Exports the project's output folder whose `receipt.json` has SHA-256
/// `receipt` (the one the reviewer inspected) to
/// `<destination>/<first 16 hex digits>/`, and gives that folder.
pub(crate) fn export(config_dir: &Path, name: &str, receipt: &str) -> Result<PathBuf, Refusal> {
    let project = config::load(config_dir, name)?;
    let destination = &project.destination;
    if !owner_only(destination) {
        return Err(delivery("destination_writable"));
    }
    match apart_from_roots(None, &project.output, &[&project.dir, destination]) {
        Some(true) => {}
        Some(false) => return Err(setup("paths_overlap")),
        None => return Err(setup("output_parent_missing")),
    }
    let schema = schema::load_schema(Some(&project.schema))?;
    let policy = schema::load_policy(Some(&project.policy), &schema)?;

    // The output, read once: exactly the schema's files and a receipt, the
    // receipt the one named.
    let mut names: Vec<&str> = schema.files.iter().map(|f| f.name.as_str()).collect();
    names.push(crate::RECEIPT);
    let mut files =
        read_output(&project.output, &names).ok_or_else(|| delivery("receipt_mismatch"))?;
    let Some(Outcome::Ok(receipt_bytes)) = files.remove(RECEIPT) else {
        return Err(delivery("receipt_mismatch"));
    };
    if sha256_hex(&receipt_bytes) != receipt {
        return Err(delivery("receipt_mismatch"));
    }
    let hashes: HashMap<String, String> = files
        .iter()
        .filter_map(|(n, o)| match o {
            Outcome::Ok(bytes) => Some((n.clone(), sha256_hex(bytes))),
            _ => None,
        })
        .collect();

    // Checked again under the configured schema and policy, then rewritten:
    // the fresh receipt must be the one read, and each file the one the
    // receipt lists, so the folder is byte for byte what the gate writes.
    let candidates =
        structure::check_read(&schema, |n| files.remove(n).unwrap_or(Outcome::Unreadable))?;
    disclosure::check(&candidates, &policy)?;
    backstop::check(&candidates)?;
    let fresh = rewritten(&schema, &policy, &candidates)?;
    let same = fresh.iter().all(|(n, bytes)| {
        if n == RECEIPT {
            *bytes == receipt_bytes
        } else {
            hashes.get(n) == Some(&sha256_hex(bytes))
        }
    });
    if !same {
        return Err(delivery("receipt_mismatch"));
    }

    // Only the reviewer may own the destination and the config; then the
    // gate's own bytes, re-hashed where they landed.
    let uid = writer(destination)?;
    let destination = &destination
        .canonicalize()
        .map_err(|_| delivery("destination_writable"))?;
    if !reviewer_only(destination, uid, false) {
        return Err(delivery("destination_writable"));
    }
    if !reviewer_only(&project.dir, uid, true) {
        return Err(setup("config_writable"));
    }
    let target = destination.join(&receipt[..FOLDER_DIGITS]);
    if std::fs::symlink_metadata(&target).is_ok() {
        // Exported before: re-verified, nothing written.
        return if holds(&target, &fresh) {
            Ok(target)
        } else {
            Err(delivery("hash_mismatch"))
        };
    }
    place(&target, &fresh)?;
    if !holds(&target, &fresh) {
        let _ = std::fs::remove_dir_all(&target);
        return Err(delivery("hash_mismatch"));
    }
    Ok(target)
}
