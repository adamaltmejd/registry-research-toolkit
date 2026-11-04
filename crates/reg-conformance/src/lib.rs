//! The conformance harness's artifacts (conformance/README.md): the fixture cache and
//! the synthetic holdings pair, both built by `target/debug/reg-build`, which the
//! caller has compiled. The harness never runs cargo.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

/// Points the fixture cache at another directory.
pub const FIXTURE_CACHE_ENV: &str = "REG_FIXTURE_CACHE";
/// A generation idle this long is pruned when a new one is created. Every lookup marks
/// its generation as used, so a returned path stays valid this long after its last
/// lookup.
pub const RETENTION: Duration = Duration::from_hours(6);

/// The repository root.
#[must_use]
pub fn repo() -> PathBuf {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    fs::canonicalize(&path).unwrap_or(path)
}

/// `conformance/cases`, the corpus.
#[must_use]
pub fn cases() -> PathBuf {
    repo().join("conformance/cases")
}

/// The directory of a fixture name under `cases/fixtures`.
#[must_use]
pub fn fixture(name: &str) -> PathBuf {
    cases().join("fixtures").join(name)
}

/// A workspace binary: `$<env_var>` when set, else `name` beside the running
/// executable in its cargo target (`<target>/debug/`, or its parent when the
/// executable is a test in `<target>/debug/deps/`), so a per-worktree
/// `CARGO_TARGET_DIR` works.
///
/// # Panics
/// When the running executable has no path.
#[must_use]
pub fn binary(name: &str, env_var: &str) -> PathBuf {
    if let Some(path) = std::env::var_os(env_var).filter(|v| !v.is_empty()) {
        return PathBuf::from(path);
    }
    let exe = std::env::current_exe().expect("the running executable's path");
    let mut dir = exe.parent().expect("the executable's directory");
    if dir.file_name().is_some_and(|n| n == "deps") {
        dir = dir.parent().expect("the target profile directory");
    }
    dir.join(name)
}

/// `reg-build`; `REG_BUILD_BIN` points at another build of it (a release build).
#[must_use]
pub fn reg_build() -> PathBuf {
    binary("reg-build", "REG_BUILD_BIN")
}

/// Every file under `dir`, relative to it and `/`-separated, in byte order; symlinks
/// are followed. `skip_dot` skips dotfiles, as `reg-build` does for a pinned directory.
fn files(
    dir: &Path,
    prefix: &str,
    skip_dot: bool,
    out: &mut BTreeSet<String>,
) -> Result<(), String> {
    for entry in fs::read_dir(dir).map_err(|e| format!("{}: {e}", dir.display()))? {
        let entry = entry.map_err(|e| e.to_string())?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if skip_dot && name.starts_with('.') {
            continue;
        }
        let rel = if prefix.is_empty() {
            name
        } else {
            format!("{prefix}/{name}")
        };
        let path = entry.path();
        if path.is_dir() {
            files(&path, &rel, skip_dot, out)?;
        } else if path.is_file() {
            out.insert(rel);
        }
    }
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String, String> {
    fs::read(path)
        .map(reg_core::sha256_hex)
        .map_err(|e| format!("{}: {e}", path.display()))
}

/// Content digest of a file or directory, independent of where it lives: every file's
/// relative path and bytes.
///
/// # Errors
/// When `root` or a file under it cannot be read.
pub fn tree_digest(root: &Path) -> Result<String, String> {
    if root.is_file() {
        return sha256_file(root);
    }
    let mut paths = BTreeSet::new();
    files(root, "", false, &mut paths)?;
    let lines = paths
        .iter()
        .map(|rel| Ok(format!("{rel}\0{}\n", sha256_file(&root.join(rel))?)))
        .collect::<Result<String, String>>()?;
    Ok(reg_core::sha256_hex(lines))
}

/// The artifact cache shared by worktrees and runs: `$REG_FIXTURE_CACHE`, else a
/// directory in the system temp directory (writable inside agent sandboxes, cleared on
/// reboot).
#[must_use]
pub fn fixture_cache_dir() -> PathBuf {
    std::env::var_os(FIXTURE_CACHE_ENV)
        .filter(|v| !v.is_empty())
        .map_or_else(
            || std::env::temp_dir().join("registry-research-toolkit-fixtures"),
            PathBuf::from,
        )
}

/// The directory of the generation keyed `build_inputs` (the `reg-build` binary's
/// digest) under `cache`, marked as used at `now`. Creating it prunes the generations
/// idle since before `now - RETENTION`, and nothing outside `generations/`.
///
/// # Errors
/// When the directory cannot be created or marked.
pub fn generation_dir(
    cache: &Path,
    build_inputs: &str,
    now: SystemTime,
) -> Result<PathBuf, String> {
    let generations = cache.join("generations");
    let generation = generations.join(build_inputs);
    let io = |e: std::io::Error| format!("{}: {e}", generation.display());
    if !generation.is_dir() {
        fs::create_dir_all(&generation).map_err(io)?;
        // simplify: every rebuilt binary starts a generation (~1 MB per fixture), so
        // ones idle for 6 hours are pruned; switch to size-based eviction if that
        // still grows.
        let cutoff = now - RETENTION;
        for stale in fs::read_dir(&generations).map_err(io)?.flatten() {
            let idle = stale
                .metadata()
                .and_then(|m| m.modified())
                .is_ok_and(|modified| modified < cutoff);
            if idle && stale.path().is_dir() {
                let _ = fs::remove_dir_all(stale.path());
            }
        }
    }
    fs::File::open(&generation)
        .and_then(|dir| dir.set_modified(now))
        .map_err(io)?;
    Ok(generation)
}

/// The cached, read-only `reg_meta.db` built from the fixture directory `source`,
/// built on first use.
///
/// A miss builds into a private staging directory beside the entry and renames it into
/// place, so a reader never sees a partial entry; when another builder of the same key
/// renamed first, its entry is kept and this copy discarded. A caller that mutates an
/// artifact copies it.
///
/// # Errors
/// When `reg-build` is not built, `source` is not a fixture or the build fails.
pub fn cached_reader_artifact(source: &Path) -> Result<PathBuf, String> {
    let reg_build = reg_build();
    if !reg_build.is_file() {
        return Err(format!(
            "{} is not built: run `cargo build -p reg-build`",
            reg_build.display()
        ));
    }
    let generation = generation_dir(
        &fixture_cache_dir(),
        &tree_digest(&reg_build)?,
        SystemTime::now(),
    )?;
    let entry = generation.join(tree_digest(source)?);
    let path = entry.join("reg_meta.db");
    if path.exists() {
        return Ok(path);
    }
    if !source.join("curation/inputs.toml").is_file() {
        return Err(format!(
            "{} is not a reg-build fixture (curation/inputs.toml)",
            source.display()
        ));
    }
    let staging = Scratch::in_dir(&generation, ".staging")?;
    let db = staging.path().join("reg_meta.db");
    run(Command::new(&reg_build)
        .arg("build")
        .arg("--inputs")
        .arg(source.join("inputs"))
        .arg("--curation")
        .arg(source.join("curation"))
        .arg("--out")
        .arg(&db)
        .arg("--report")
        .arg(staging.path().join("report.json")))
    .map_err(|e| format!("reg-build build {}: {e}", source.display()))?;
    fs::set_permissions(&db, fs::Permissions::from_mode(0o444)).map_err(|e| e.to_string())?;
    if let Err(e) = fs::rename(staging.path(), &entry)
        && !entry.is_dir()
    {
        return Err(format!("{}: {e}", entry.display()));
    }
    Ok(path)
}

/// A private directory, removed on drop.
#[derive(Debug)]
pub struct Scratch(PathBuf);

impl Scratch {
    /// A new directory in the system temp directory, named for `label`.
    ///
    /// # Errors
    /// When it cannot be created.
    pub fn new(label: &str) -> Result<Self, String> {
        Self::in_dir(&std::env::temp_dir(), &format!("reg-conformance-{label}"))
    }

    /// A new directory in `parent`, named `<prefix>-<pid>-<n>`.
    ///
    /// # Errors
    /// When it cannot be created.
    pub fn in_dir(parent: &Path, prefix: &str) -> Result<Self, String> {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!("{prefix}-{}-{n}", std::process::id()));
        // A crashed run with the same pid may have left one behind.
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).map_err(|e| format!("{}: {e}", path.display()))?;
        Ok(Self(path))
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Copy the tree `from` to `to`, files as plain copies.
///
/// # Errors
/// When a file cannot be read or written.
pub fn copy_tree(from: &Path, to: &Path) -> Result<(), String> {
    let mut paths = BTreeSet::new();
    files(from, "", false, &mut paths)?;
    for rel in paths {
        let target = to.join(&rel);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        fs::copy(from.join(&rel), &target).map_err(|e| format!("{rel}: {e}"))?;
    }
    Ok(())
}

/// Run `command` from the repository root with no stdin; its stdout, or its stderr as
/// the error when it fails.
///
/// # Errors
/// When the command cannot start or exits non-zero.
pub fn run(command: &mut Command) -> Result<String, String> {
    if command.get_current_dir().is_none() {
        command.current_dir(repo());
    }
    let output = command
        .stdin(Stdio::null())
        .output()
        .map_err(|e| format!("{}: {e}", command.get_program().display()))?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).trim().to_owned())
    }
}

/// The synthetic steward's sources, `conformance/cases/holdings`.
#[must_use]
pub fn holdings_case() -> PathBuf {
    cases().join("holdings")
}

/// A locked, pinned copy of the synthetic steward, ready for `reg-build holdings`.
#[derive(Debug)]
pub struct Holdings {
    /// The cached base artifact the holdings compile against (read-only, shared).
    pub base: PathBuf,
    /// The steward checkout: a git repository whose head pins its inputs.
    pub root: PathBuf,
    /// The `stewards.toml` naming that head.
    pub registry: PathBuf,
}

impl Holdings {
    /// `reg-build holdings` on this steward; the caller adds `--out`.
    #[must_use]
    pub fn command(&self) -> Command {
        let mut command = Command::new(reg_build());
        command
            .arg("holdings")
            .arg("--base")
            .arg(&self.base)
            .args(["--steward", "fixture", "--steward-repo"])
            .arg(&self.root)
            .arg("--registry")
            .arg(&self.registry);
        command
    }
}

/// Copy the synthetic steward into `directory`, lock its curation against the cached
/// base artifact and pin it. The private steward repository is never used.
///
/// # Errors
/// When a copy, the lock or a git step fails.
pub fn prepare_holdings(directory: &Path) -> Result<Holdings, String> {
    let root = directory.join("steward");
    copy_tree(&holdings_case().join("steward"), &root)?;
    let base = cached_reader_artifact(&holdings_case().join("base"))?;
    run(Command::new(reg_build())
        .arg("lock")
        .arg("--base")
        .arg(&base)
        .arg("--inputs")
        .arg(&root)
        .arg("--curation")
        .arg(root.join("curation")))?;
    let registry = directory.join("stewards.toml");
    pin_steward(&root, &registry)?;
    Ok(Holdings {
        base,
        root,
        registry,
    })
}

/// Pin the steward checkout at `root` (including a refusal case's mutations): write
/// `steward.toml` with the digest of every consumed input, commit it all in a fresh
/// git repository and name that commit in `registry`.
///
/// # Errors
/// When an input cannot be read or a git step fails.
pub fn pin_steward(root: &Path, registry: &Path) -> Result<(), String> {
    let mut roles = vec![
        "scope.toml",
        "series",
        "curation",
        "inputs/slots",
        "branding/steward.json",
        "branding/logo.svg",
        "census.csv",
        "storage.csv",
        "holdings.toml",
        "curation/readers/holdings.toml",
    ];
    let references = root.join("reference-occurrences.toml").exists();
    if references {
        // The classification reader's directory is pinned by file, as the compiler
        // checks an unpinned consumed directory.
        roles.extend([
            "reference-occurrences.toml",
            "inputs/reference",
            "inputs/reference/codes/kind.csv",
        ]);
    }
    let mut pins = String::new();
    for role in roles {
        let path = root.join(role);
        // `reg-build`'s digest of a consumed input: a file (or a directory of one
        // file) is its own SHA-256, more files hash `path\tsha256\n` lines.
        let digest = if path.is_dir() {
            let mut paths = BTreeSet::new();
            files(&path, "", true, &mut paths)?;
            if let [only] = paths.iter().collect::<Vec<_>>()[..] {
                sha256_file(&path.join(only))?
            } else {
                let lines = paths
                    .iter()
                    .map(|rel| Ok(format!("{rel}\t{}\n", sha256_file(&path.join(rel))?)))
                    .collect::<Result<String, String>>()?;
                reg_core::sha256_hex(lines)
            }
        } else {
            sha256_file(&path)?
        };
        writeln!(pins, "\"{role}\" = \"{digest}\"").expect("write to String");
    }
    let inputs = if references {
        "[inputs]\nreference_occurrences = \"reference-occurrences.toml\""
    } else {
        "[inputs]"
    };
    let config = format!(
        "owner = \"fixture\"\nscope = \"scope.toml\"\nseries_root = \"series\"\n\
         curation_root = \"curation/registers\"\n\n[presentation]\n\
         steward = \"branding/steward.json\"\nlogo = \"branding/logo.svg\"\n\n{inputs}\n\
         census = \"census.csv\"\nstorage_export = \"storage.csv\"\n\
         holdings = \"holdings.toml\"\ncensus_reader = \"curation/readers/holdings.toml\"\n\n\
         [pins]\n{pins}"
    );
    fs::write(root.join("steward.toml"), config).map_err(|e| e.to_string())?;
    let git = |args: &[&str]| run(Command::new("git").args(args).current_dir(root));
    git(&["init", "--quiet"])?;
    git(&["config", "user.name", "Synthetic fixture"])?;
    git(&["config", "user.email", "fixture@example.invalid"])?;
    git(&["config", "commit.gpgsign", "false"])?;
    git(&["add", "."])?;
    git(&[
        "commit",
        "--quiet",
        "--allow-empty",
        "-m",
        "Pin synthetic inputs",
    ])?;
    let commit = git(&["rev-parse", "HEAD"])?;
    if git(&["remote", "get-url", "origin"]).is_err() {
        git(&["remote", "add", "origin", "fixture"])?;
    }
    fs::write(
        registry,
        format!(
            "[fixture]\nrepo = \"fixture\"\ncommit = \"{}\"\nconfig = \"steward.toml\"\n",
            commit.trim()
        ),
    )
    .map_err(|e| e.to_string())
}

/// Build the synthetic paired catalog into `out`: the holdings as
/// `reg_meta_fixture.db` beside a copy of their base as `reg_meta.db`, served as
/// catalog `fixture`.
///
/// # Errors
/// When preparing or compiling the holdings fails.
pub fn build_paired(out: &Path) -> Result<(), String> {
    fs::create_dir_all(out).map_err(|e| format!("{}: {e}", out.display()))?;
    let scratch = Scratch::new("paired")?;
    let holdings = prepare_holdings(scratch.path())?;
    run(holdings
        .command()
        .arg("--out")
        .arg(out.join("reg_meta_fixture.db")))?;
    let base = out.join("reg_meta.db");
    fs::copy(&holdings.base, &base).map_err(|e| e.to_string())?;
    // The copy keeps the cache entry's read-only mode; the pair is the caller's.
    fs::set_permissions(&base, fs::Permissions::from_mode(0o644)).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn perturb(path: &Path) {
        let mut bytes = fs::read(path).unwrap();
        bytes.push(b'\n');
        fs::write(path, bytes).unwrap();
    }

    fn stamp(path: &Path, at: SystemTime) {
        fs::File::open(path).unwrap().set_modified(at).unwrap();
    }

    #[test]
    fn only_idle_generations_are_pruned() {
        // Fails if pruning reaches outside `generations/` or ignores the idle cutoff.
        let cache = Scratch::new("cache-prune").unwrap();
        let now = SystemTime::now();
        let idle = now - RETENTION - Duration::from_secs(60);
        let recent = now - RETENTION + Duration::from_secs(600);
        let stale = cache.path().join("generations/stale");
        let active = cache.path().join("generations/active");
        let neighbour = cache.path().join("neighbour");
        for (dir, at) in [(&stale, idle), (&active, recent), (&neighbour, idle)] {
            fs::create_dir_all(dir).unwrap();
            stamp(dir, at);
        }
        let created = generation_dir(cache.path(), "new", now).unwrap();
        assert!(created.starts_with(cache.path().join("generations")));
        assert_eq!(
            (stale.exists(), active.exists(), neighbour.exists()),
            (false, true, true)
        );
    }

    #[test]
    fn every_build_input_changes_the_key() {
        // Fails if a fixture file's path or bytes, or the `reg-build` binary's bytes,
        // drop out of the key, which would serve a stale artifact after that input
        // changes.
        let scratch = Scratch::new("cache-key").unwrap();
        let tree = scratch.path().join("fixture");
        copy_tree(&fixture("base"), &tree).unwrap();
        // A stand-in for the binary: the key reads only its bytes.
        let binary = scratch.path().join("reg-build");
        fs::write(&binary, b"binary").unwrap();

        // Content-addressed: a copy elsewhere keys the same as the checkout's.
        let baseline = tree_digest(&tree).unwrap();
        assert_eq!(baseline, tree_digest(&fixture("base")).unwrap());
        let binary_baseline = tree_digest(&binary).unwrap();
        perturb(&tree.join("inputs/frame/base.jsonl"));
        let edited = tree_digest(&tree).unwrap();
        fs::rename(
            tree.join("inputs/frame/base.jsonl"),
            tree.join("inputs/frame/moved.jsonl"),
        )
        .unwrap();
        let renamed = tree_digest(&tree).unwrap();
        perturb(&binary);
        assert_eq!(BTreeSet::from([&baseline, &edited, &renamed]).len(), 3);
        assert_ne!(tree_digest(&binary).unwrap(), binary_baseline);
    }
}
