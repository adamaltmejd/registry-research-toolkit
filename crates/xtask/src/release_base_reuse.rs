//! `cargo xtask release-base-reuse <earlier evidence dir>`: may a release reuse an
//! earlier evidence base build?
//!
//! Run from the release checkout (clean, at the release commit). The release skill's
//! step 8 builds the real-seed base once per release; a later commit in the same release
//! reuses that base when nothing the base build reads has changed (maintainer ruling
//! 2026-11-04). The base is a deterministic function of the `reg-build` binary, the
//! curation tree (including the inputs pin, `curation/inputs.toml`), the toolchain and
//! the build flags, so a reuse is safe exactly when all of those are unchanged:
//!
//! - Builder closure: the workspace crates `reg-build` depends on (normal and build
//!   dependencies, not dev), read from `cargo metadata` at the release commit, so a new
//!   crate joins without editing this file. A new dependency edge changes a closure
//!   crate's `Cargo.toml`, which the diff sees. Every other member's `Cargo.toml` too:
//!   the `--workspace` build unifies features, so a reader crate's manifest can change
//!   what the builder links. Plus `curation/`, the root `Cargo.toml` and `Cargo.lock`,
//!   `.cargo/` and any `rust-toolchain*`.
//! - Toolchain: `rustc -vV` at the release checkout equals the recorded `rustc.txt`.
//! - Flags: the recorded `build-args.txt` has `--release` and `--catalog-version` equal
//!   to this release's `reg-meta` version and to the base's `meta.catalog_version`.
//! - Bytes: `catalog/reg_meta.db` still hashes to the recorded `base.sha256`.
//!
//! A step's result carries over only when the evidence's `passed` file lists it against
//! this base digest (each step appends `<step> <base sha>` once it passes); only 8b and
//! 8c can. Conformance (8d) runs the release commit's server, so it and the 8e
//! `base_sha` check always run at the release commit.
//!
//! The evidence directory must hold what step 8a records: `source-revision`,
//! `rustc.txt`, `build-args.txt`, `base.sha256` and `catalog/reg_meta.db`. A missing or
//! inconsistent record fails closed. Exit 0: reuse the base. Exit 1: rebuild it. Exit 2:
//! the records do not support a reuse; rebuild it.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;

use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde_json::Value;
use sha2::{Digest, Sha256};

const EXIT_REBUILD: u8 = 1;
const EXIT_REFUSED: u8 = 2;

const USAGE: &str = "usage: cargo xtask release-base-reuse <earlier evidence dir>";

const ALWAYS_BUILDER: [&str; 5] = [
    "curation",
    "Cargo.toml",
    "Cargo.lock",
    ".cargo",
    ":(glob)rust-toolchain*",
];

/// Step 8a's `reg-build build` value options.
const BUILD_VALUES: [&str; 6] = [
    "--inputs",
    "--assets",
    "--curation",
    "--out",
    "--catalog-version",
    "--report",
];

#[must_use]
pub fn main(args: &[String]) -> u8 {
    let [evidence] = args else {
        eprintln!("{USAGE}");
        return EXIT_REFUSED;
    };
    match run(Path::new(evidence)) {
        Ok(code) => code,
        Err(message) => {
            eprintln!("{message}");
            EXIT_REFUSED
        }
    }
}

/// A refusal of the records; the caller prints it with the rebuild advice.
struct Refused(String);

fn refused(message: impl Into<String>) -> Refused {
    Refused(message.into())
}

/// Stdout of a command that must succeed.
fn output(program: &str, args: &[&str]) -> Result<String, String> {
    let out = Command::new(program)
        .args(args)
        .output()
        .map_err(|e| format!("command failed: {program} {}: {e}", args.join(" ")))?;
    if !out.status.success() {
        return Err(format!(
            "command failed: {program} {}\n{}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim_end()
        ));
    }
    String::from_utf8(out.stdout).map_err(|e| format!("{program}: {e}"))
}

fn changed(built: &str, release: &str, paths: &[String]) -> Result<Vec<String>, String> {
    let mut args = vec!["diff", "--name-only", built, release, "--"];
    args.extend(paths.iter().map(String::as_str));
    Ok(output("git", &args)?
        .split_whitespace()
        .map(str::to_owned)
        .collect())
}

fn record(evidence: &Path, name: &str) -> Result<String, Refused> {
    let path = evidence.join(name);
    if !path.is_file() {
        return Err(refused(format!(
            "{} is missing: record it in step 8a",
            path.display()
        )));
    }
    fs::read_to_string(&path)
        .map(|s| s.trim().to_owned())
        .map_err(|e| refused(format!("{}: {e}", path.display())))
}

fn sha256(path: &Path) -> Result<String, String> {
    let mut file = fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut digest = Sha256::new();
    let mut buf = vec![0; 1 << 24];
    loop {
        let n = file
            .read(&mut buf)
            .map_err(|e| format!("{}: {e}", path.display()))?;
        if n == 0 {
            break;
        }
        digest.update(&buf[..n]);
    }
    Ok(reg_core::hex(&digest.finalize()))
}

fn first_word(text: &str) -> &str {
    text.split_whitespace().next().unwrap_or("")
}

fn open_ro(path: &Path) -> Result<Connection, String> {
    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|e| format!("{}: {e}", path.display()))
}

fn meta_value(conn: &Connection, key: &str) -> Result<Option<String>, String> {
    conn.query_row("SELECT value FROM meta WHERE key = ?", [key], |r| r.get(0))
        .optional()
        .map_err(|e| e.to_string())
}

fn read_toml(path: &Path) -> Result<toml::Table, String> {
    fs::read_to_string(path)
        .map_err(|e| format!("{}: {e}", path.display()))?
        .parse()
        .map_err(|e| format!("{}: {e}", path.display()))
}

/// Step 8a's `reg-build build` arguments, exactly: every value option once, plus
/// `--release`. Anything else (a subset, a diagnostic flag) is not a release build.
fn build_options(args: &[&str]) -> Result<BTreeMap<String, String>, Refused> {
    let Some((&"build", rest)) = args.split_first() else {
        return Err(refused("build-args.txt is not a `reg-build build` command"));
    };
    let mut options = BTreeMap::new();
    let mut rest = rest.iter();
    while let Some(&flag) = rest.next() {
        if flag == "--release" && !options.contains_key(flag) {
            options.insert(flag.to_owned(), String::new());
        } else if BUILD_VALUES.contains(&flag)
            && !options.contains_key(flag)
            && let Some(&value) = rest.as_slice().first()
        {
            rest.next();
            options.insert(flag.to_owned(), value.to_owned());
        } else {
            return Err(refused(format!(
                "build-args.txt has an unexpected argument {flag:?}"
            )));
        }
    }
    if options.len() != BUILD_VALUES.len() + 1 {
        return Err(refused(
            "build-args.txt is not step 8a's full release build",
        ));
    }
    Ok(options)
}

/// The built commit and base digest, once every record supports reusing it.
fn check_records(evidence: &Path, repo: &Path) -> Result<(String, String), Refused> {
    let built = record(evidence, "source-revision")?;
    let commit = format!("{built}^{{commit}}");
    if output("git", &["cat-file", "-e", &commit]).is_err() {
        return Err(refused(format!(
            "built commit {built} is not in this clone: git fetch it"
        )));
    }
    let recorded_rustc = record(evidence, "rustc.txt")?;
    if recorded_rustc != output("rustc", &["-vV"]).map_err(refused)?.trim() {
        return Err(refused("rustc -vV differs from the recorded rustc.txt"));
    }
    let args = record(evidence, "build-args.txt")?;
    let args: Vec<&str> = args.split('\n').collect();
    let version = read_toml(&repo.join("crates/reg-meta/Cargo.toml"))
        .map_err(refused)?
        .get("package")
        .and_then(|p| p.get("version"))
        .and_then(toml::Value::as_str)
        .ok_or_else(|| refused("crates/reg-meta/Cargo.toml has no package.version"))?
        .to_owned();
    let options = build_options(&args)?;
    if options["--catalog-version"] != version {
        return Err(refused(format!(
            "the recorded build's --catalog-version is not {version}"
        )));
    }
    if options["--curation"] != "curation" {
        return Err(refused(
            "the recorded build did not read this checkout's curation/",
        ));
    }
    // The catalog file itself is not resolved: only its directory names the evidence.
    let db = evidence.join("catalog/reg_meta.db");
    let out = Path::new(&options["--out"]);
    let out_dir = out.parent().unwrap_or(Path::new(""));
    let out_dir = if out_dir.as_os_str().is_empty() {
        Path::new(".")
    } else {
        out_dir
    };
    let resolved = resolve(out_dir).join(out.file_name().unwrap_or_default());
    if resolved != db {
        return Err(refused(
            "the recorded build did not write this evidence's catalog",
        ));
    }
    if !db.is_file() {
        return Err(refused(format!("{} is missing", db.display())));
    }
    let catalog_version = open_ro(&db)
        .and_then(|conn| meta_value(&conn, "catalog_version"))
        .map_err(refused)?;
    if catalog_version.as_deref() != Some(version.as_str()) {
        return Err(refused(format!(
            "{} meta.catalog_version is not {version}",
            db.display()
        )));
    }
    let recorded = first_word(&record(evidence, "base.sha256")?).to_owned();
    if sha256(&db).map_err(refused)? != recorded {
        return Err(refused(format!(
            "{} no longer matches base.sha256",
            db.display()
        )));
    }
    Ok((built, recorded))
}

/// Python's non-strict `Path.resolve()`: an absolute path with symlinks resolved as far
/// as the path exists.
fn resolve(path: &Path) -> PathBuf {
    let absolute = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
    let mut existing = absolute.as_path();
    let mut rest = Vec::new();
    loop {
        if let Ok(real) = existing.canonicalize() {
            return rest
                .iter()
                .rev()
                .fold(real, |p: PathBuf, part| p.join(part));
        }
        match (existing.parent(), existing.file_name()) {
            (Some(parent), Some(name)) => {
                rest.push(name.to_owned());
                existing = parent;
            }
            _ => return absolute,
        }
    }
}

/// Steps `passed` records against this base digest (`<step> <base sha>` lines).
fn passed_steps(evidence: &Path, base_sha: &str) -> BTreeSet<String> {
    let Ok(text) = fs::read_to_string(evidence.join("passed")) else {
        return BTreeSet::new();
    };
    text.lines()
        .filter_map(
            |line| match line.split_whitespace().collect::<Vec<_>>()[..] {
                [step, sha] if sha == base_sha => Some(step.to_owned()),
                _ => None,
            },
        )
        .collect()
}

/// Workspace package ids `root_name` reaches through normal and build deps. Fails
/// closed: an absent root or resolve node would shrink the builder paths to nothing.
fn workspace_closure(
    meta: &Value,
    members: &BTreeSet<String>,
    root_name: &str,
) -> Result<BTreeSet<String>, String> {
    let root = meta["packages"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|p| p["name"] == root_name && members.contains(p["id"].as_str().unwrap_or("")))
        .and_then(|p| p["id"].as_str())
        .map(str::to_owned)
        .ok_or_else(|| format!("{root_name} is not a workspace member"))?;
    let nodes: BTreeMap<&str, &Value> = meta["resolve"]["nodes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|n| Some((n["id"].as_str()?, n)))
        .collect();
    let mut seen = BTreeSet::new();
    let mut todo = vec![root];
    while let Some(id) = todo.pop() {
        if !seen.insert(id.clone()) {
            continue;
        }
        let node = nodes
            .get(id.as_str())
            .ok_or_else(|| format!("cargo metadata has no resolve node for {id}"))?;
        for dep in node["deps"].as_array().into_iter().flatten() {
            let pkg = dep["pkg"].as_str().unwrap_or("");
            let not_dev_only = dep["dep_kinds"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|k| k["kind"] != "dev");
            if members.contains(pkg) && not_dev_only {
                todo.push(pkg.to_owned());
            }
        }
    }
    Ok(seen)
}

fn run(evidence: &Path) -> Result<u8, String> {
    let evidence = resolve(evidence);
    let repo = PathBuf::from(output("git", &["rev-parse", "--show-toplevel"])?.trim());
    // Every pathspec below is repository-relative.
    std::env::set_current_dir(&repo).map_err(|e| format!("{}: {e}", repo.display()))?;
    if !output("git", &["status", "--porcelain"])?.is_empty() {
        eprintln!("refused: the release checkout is dirty");
        return Ok(EXIT_REFUSED);
    }
    let release = output("git", &["rev-parse", "HEAD"])?.trim().to_owned();
    let (built, base_sha) = match check_records(&evidence, &repo) {
        Ok(records) => records,
        Err(Refused(message)) => {
            eprintln!("refused: {message}; rebuild the base");
            return Ok(EXIT_REFUSED);
        }
    };

    let PathSets {
        builder_dirs,
        builder: builder_paths,
    } = PathSets::read(&repo)?;

    println!("built   {built} ({})", evidence.display());
    println!("release {release}");
    println!("builder closure: {}", builder_dirs.join(" "));
    let base = changed(&built, &release, &builder_paths)?;
    if !base.is_empty() {
        println!(
            "base: rebuild (builder input changed):\n  {}",
            base.join("\n  ")
        );
        return Ok(EXIT_REBUILD);
    }
    let passed = passed_steps(&evidence, &base_sha);
    println!("base: reuse");
    for step in ["8b", "8c"] {
        if passed.contains(step) {
            println!("{step}: carries over");
        } else {
            println!("{step}: run it (not passed in the evidence)");
        }
    }
    println!("always: 8d and the 8e base_sha check at the release commit");
    Ok(0)
}

/// The pathspecs each verdict diffs, from `cargo metadata` at the release commit.
struct PathSets {
    /// The builder closure's crate directories, for the report.
    builder_dirs: Vec<String>,
    /// What the base build reads.
    builder: Vec<String>,
}

impl PathSets {
    fn read(repo: &Path) -> Result<Self, String> {
        let meta: Value = serde_json::from_str(&output(
            "cargo",
            &["metadata", "--format-version", "1", "--locked"],
        )?)
        .map_err(|e| format!("cargo metadata: {e}"))?;
        let members: BTreeSet<String> = meta["workspace_members"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|m| m.as_str().map(str::to_owned))
            .collect();
        let mut dirs = BTreeMap::new();
        for p in meta["packages"].as_array().into_iter().flatten() {
            let Some(id) = p["id"].as_str().filter(|id| members.contains(*id)) else {
                continue;
            };
            let manifest = Path::new(p["manifest_path"].as_str().unwrap_or(""));
            let dir = manifest
                .parent()
                .and_then(|d| d.strip_prefix(repo).ok())
                .ok_or_else(|| format!("{} is outside {}", manifest.display(), repo.display()))?;
            dirs.insert(id.to_owned(), dir.display().to_string());
        }
        let builder = workspace_closure(&meta, &members, "reg-build")?;
        let sorted = |ids: Vec<&String>, suffix: &str| {
            let mut v: Vec<String> = ids
                .iter()
                .map(|i| format!("{}{suffix}", dirs[*i]))
                .collect();
            v.sort();
            v
        };
        let reader: Vec<&String> = members.difference(&builder).collect();
        let builder_dirs = sorted(builder.iter().collect(), "");
        // `--workspace` builds unify features across every member, so any member's
        // manifest can change what the builder links (e.g. a reader crate enabling a
        // feature of a shared dependency) without touching Cargo.lock.
        let manifests = sorted(reader, "/Cargo.toml");
        Ok(Self {
            builder: [
                builder_dirs.clone(),
                manifests,
                ALWAYS_BUILDER.map(str::to_owned).to_vec(),
            ]
            .concat(),
            builder_dirs,
        })
    }
}
