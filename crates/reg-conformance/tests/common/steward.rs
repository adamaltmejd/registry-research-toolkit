//! The run's synthetic steward: locked once, then copied and pinned per use, so a
//! mutation case pays for its own compile only. The steward pairs are each built once
//! and served as catalog `fixture` by one shared server.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use reg_conformance::{
    Holdings, Scratch, cases, copy_tree, holdings_case, lock_holdings, pinned_holdings, run,
};

use super::server;

type Built = OnceLock<Result<PathBuf, String>>;

/// The run's directory for the session-built stewards, in the system temp dir: they
/// are git repositories, and the Claude Code sandbox refuses `.git/config` writes
/// inside the working tree. A static is never dropped, so the directory outlives the
/// run until the OS clears its temp dir; it holds synthetic data only.
fn run_dir() -> Result<&'static Path, String> {
    static DIR: OnceLock<Result<Scratch, String>> = OnceLock::new();
    DIR.get_or_init(|| Scratch::new("steward"))
        .as_ref()
        .map(Scratch::path)
        .map_err(Clone::clone)
}

/// An unpinned copy of the run's locked synthetic steward in `directory/steward`; its
/// root. `reg-build lock` writes the same lock for every copy, so it runs once per
/// run; a case that changes what the lock reads relocks its copy
/// (`reg_conformance::lock_steward`). A case mutates the copy, then pins it
/// (`reg_conformance::pinned_holdings`).
pub fn locked_copy(directory: &Path) -> Result<PathBuf, String> {
    static LOCKED: Built = OnceLock::new();
    let locked = LOCKED
        .get_or_init(|| lock_holdings(&run_dir()?.join("locked")))
        .clone()?;
    let root = directory.join("steward");
    copy_tree(&locked, &root)?;
    Ok(root)
}

/// A pinned, unmodified copy of the run's locked synthetic steward in `directory`.
pub fn holdings(directory: &Path) -> Result<Holdings, String> {
    locked_copy(directory)?;
    pinned_holdings(directory)
}

/// The unmodified synthetic pair: its steward checkout and its directory, which holds
/// the compiled `reg_meta_fixture.db` beside a link to its base as `reg_meta.db`.
pub struct Pair {
    pub dir: PathBuf,
    pub holdings: Holdings,
}

/// The unmodified synthetic pair, built once per run.
pub fn pair() -> Result<&'static Pair, String> {
    static PAIR: OnceLock<Result<Pair, String>> = OnceLock::new();
    PAIR.get_or_init(|| {
        let dir = run_dir()?.join("paired");
        locked_copy(&dir)?;
        let holdings = compile(&dir)?;
        Ok(Pair { dir, holdings })
    })
    .as_ref()
    .map_err(Clone::clone)
}

/// The steward copy in `directory/steward`, pinned and compiled into
/// `directory/reg_meta_fixture.db`, with its base linked as `reg_meta.db`: a pair
/// `reg-meta serve --catalog fixture` serves.
pub fn compile(directory: &Path) -> Result<Holdings, String> {
    let holdings = pinned_holdings(directory)?;
    run(holdings
        .command()
        .arg("--out")
        .arg(directory.join("reg_meta_fixture.db")))?;
    std::os::unix::fs::symlink(&holdings.base, directory.join("reg_meta.db"))
        .map_err(|e| e.to_string())?;
    Ok(holdings)
}

/// The server of the unmodified synthetic pair.
pub fn paired() -> Result<u16, String> {
    Ok(server::steward_server(&pair()?.dir, "fixture")?.port)
}

/// The server of the materialize cases' steward (`prepare_materialize`).
pub fn materialize() -> Result<u16, String> {
    static PAIR: Built = OnceLock::new();
    let dir = PAIR
        .get_or_init(|| {
            let dir = run_dir()?.join("materialize");
            prepare_materialize(&dir).map(|()| dir)
        })
        .clone()?;
    Ok(server::steward_server(&dir, "fixture")?.port)
}

pub fn read(path: &Path) -> Result<String, String> {
    fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))
}

pub fn write(path: &Path, text: &str) -> Result<(), String> {
    fs::write(path, text).map_err(|e| format!("{}: {e}", path.display()))
}

/// Replace `from` with `to` in the file at `path`: its first occurrence, or every one
/// when `all`. A missing `from` is an error, so a fixture edit cannot silently turn a
/// mutation into a no-op.
pub fn edit(path: &Path, from: &str, to: &str, all: bool) -> Result<(), String> {
    let text = read(path)?;
    if !text.contains(from) {
        return Err(format!("{}: no {from:?} to replace", path.display()));
    }
    let edited = if all {
        text.replace(from, to)
    } else {
        text.replacen(from, to, 1)
    };
    write(path, &edited)
}

/// Append `tail` to the file at `path`.
pub fn append(path: &Path, tail: &str) -> Result<(), String> {
    write(path, &(read(path)? + tail))
}

/// `text` with its one `anchor` followed by `insert`; a missing anchor is an error, so
/// a fixture edit cannot silently drop a splice.
fn after(text: &str, anchor: &str, insert: &str) -> Result<String, String> {
    let at = text
        .find(anchor)
        .ok_or(format!("no {anchor:?} to splice after"))?
        + anchor.len();
    Ok(format!("{}{insert}{}", &text[..at], &text[at..]))
}

/// `text` with its one `from` replaced by `to`.
fn replaced(text: &str, from: &str, to: &str) -> Result<String, String> {
    if !text.contains(from) {
        return Err(format!("no {from:?} to replace"));
    }
    Ok(text.replacen(from, to, 1))
}

/// Replace Pool's single Slot2 with the documented position family 1, 2, 10.
pub fn splice_slot_family(root: &Path) -> Result<(), String> {
    let slots = holdings_case().join("slot-family");
    for name in ["census.csv", "storage.csv"] {
        fs::copy(slots.join(name), root.join(name)).map_err(|e| format!("{name}: {e}"))?;
    }
    let path = root.join("holdings.toml");
    let text = read(&path)?;
    let column = "[[table.column]]";
    let start = text
        .find(&format!("{column}\nname = \"Slot2\"\n"))
        .ok_or("holdings.toml: no Slot2 column")?;
    let end = text[start + column.len()..]
        .find(column)
        .ok_or("holdings.toml: no column after Slot2")?
        + start
        + column.len();
    let family = read(&slots.join("columns.toml"))?;
    write(
        &path,
        &format!("{}{family}{}", &text[..start], &text[end..]),
    )
}

/// The materialize cases' steward in `directory`: the slot family, a long header on
/// Part B, a first-half Parent amount, a wholly contested Owners column and Visits,
/// whose row identifier moves between columns.
fn prepare_materialize(directory: &Path) -> Result<(), String> {
    let root = &locked_copy(directory)?;
    splice_slot_family(root)?;
    let extension = cases().join("materialize/fixture");
    let census = root.join("census.csv");
    let mut text = read(&census)?;
    text = replaced(
        &text,
        "Synthetic,Partition,Part B,Id,New,",
        "Synthetic,Partition,Part B,Id,New,Relative identifier as received in the delivery",
    )?;
    text = replaced(
        &text,
        "Synthetic,Rivals,Owners,Shared,Id,Unmatched,,",
        "Synthetic,Rivals,Owners,Shared,Id,Unmatched,Gamma,Rival",
    )?;
    text += &read(&extension.join("keyed-census.csv"))?;
    write(&census, &text)?;
    let storage = root.join("storage.csv");
    let mut text = read(&storage)?;
    for name in [
        "long-column-storage.csv",
        "contested-column-storage.csv",
        "keyed-storage.csv",
    ] {
        text += &read(&extension.join(name))?;
    }
    write(&storage, &text)?;
    let path = root.join("holdings.toml");
    let mut text = read(&path)?;
    for (anchor, name) in [
        ("locator = \"fixture:partition-b\"\n", "long-column.toml"),
        (
            "locator = \"fixture:interval-local-owners\"\n",
            "contested-column.toml",
        ),
        ("locator = \"fixture:parent\"\n", "partial-parent.toml"),
    ] {
        text = after(
            &text,
            anchor,
            &format!("\n{}", read(&extension.join(name))?),
        )?;
    }
    text.push('\n');
    text += &read(&extension.join("keyed.toml"))?;
    write(&path, &text)?;
    compile(directory).map(drop)
}

/// Split the steward copy at `root` into a schema-keyed census of two schemas: every
/// table in `dbo`, plus a copy of Part A (Id, New over 2021) in `aux`. It compiles only
/// once [`declare_spaces`] puts the two schemas in separate databases.
pub fn split_schemas(root: &Path) -> Result<(), String> {
    let census = root.join("census.csv");
    edit(&census, "Synthetic,", "dbo,", true)?;
    append(&census, "aux,Partition,Part A,Id,New,,,,,,,\n")?;
    append(
        &root.join("storage.csv"),
        "aux,Part A,BASE TABLE,Id,int,,10,0,1\naux,Part A,BASE TABLE,New,int,,10,0,2\n",
    )?;
    edit(
        &root.join("curation/readers/holdings.toml"),
        "census_columns = \"V\"\n",
        "census_columns = \"V\"\ncensus_schema = \"Category\"\n",
        false,
    )?;
    let topology = root.join("holdings.toml");
    edit(
        &topology,
        "[[table]]\n",
        "[[table]]\nschema_name = \"dbo\"\n",
        true,
    )?;
    append(
        &topology,
        "\n[[table]]\nschema_name = \"aux\"\nphysical_name = \"Part A\"\ndisposition = \"dated\"\n\
         delivery = \"2021\"\ntable_pids = [\"demo/held\"]\nlocator = \"fixture:aux-copy\"\n\n\
         [[table.key]]\nentity = [\"demo/held/value\"]\ntime = []\nperiod_key = \"year\"\n\
         locator = \"fixture:declared-entity-time-key\"\n",
    )
}

/// Declare the databases `main` (schema `dbo`) and `aux` (schema `aux`) in the pinned
/// steward at `root` and commit them: pinning rewrites `steward.toml`, so this follows
/// it.
pub fn declare_spaces(root: &Path) -> Result<(), String> {
    append(
        &root.join("steward.toml"),
        "\n[[space]]\nid = \"main\"\nname = \"Main database\"\nschemas = [\"dbo\"]\n\n\
         [[space]]\nid = \"aux\"\nname = \"Auxiliary database\"\nschemas = [\"aux\"]\n",
    )?;
    run(std::process::Command::new("git").arg("-C").arg(root).args([
        "commit",
        "--quiet",
        "-am",
        "Declare two spaces",
    ]))
    .map(drop)
}

/// The server of a steward with two databases ([`split_schemas`], [`declare_spaces`]),
/// built once per run.
pub fn spaces() -> Result<u16, String> {
    static PAIR: Built = OnceLock::new();
    let dir = PAIR
        .get_or_init(|| {
            let dir = run_dir()?.join("spaces");
            let root = locked_copy(&dir)?;
            split_schemas(&root)?;
            let holdings = pinned_holdings(&dir)?;
            declare_spaces(&root)?;
            run(holdings
                .command()
                .arg("--out")
                .arg(dir.join("reg_meta_fixture.db")))?;
            std::os::unix::fs::symlink(&holdings.base, dir.join("reg_meta.db"))
                .map_err(|e| e.to_string())?;
            Ok(dir)
        })
        .clone()?;
    Ok(server::steward_server(&dir, "fixture")?.port)
}
