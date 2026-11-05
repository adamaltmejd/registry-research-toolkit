//! The run's steward pairs, each built once and served as catalog `fixture` by one
//! shared server. They live in the run's directory beside the server logs: a static
//! is never dropped, so nothing would remove a scratch directory.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use reg_conformance::{build_paired, cases, holdings_case, pin_steward, prepare_holdings, run};

use super::server;

type Built = OnceLock<Result<PathBuf, String>>;

fn serve(
    pair: &'static Built,
    name: &str,
    build: fn(&Path) -> Result<(), String>,
) -> Result<u16, String> {
    let dir = pair
        .get_or_init(|| {
            let dir = server::log_dir().join(name);
            build(&dir).map(|()| dir)
        })
        .clone()?;
    Ok(server::steward_server(&dir, "fixture")?.port)
}

/// The server of the unmodified synthetic pair (`reg_conformance::build_paired`).
pub fn paired() -> Result<u16, String> {
    static PAIR: Built = OnceLock::new();
    serve(&PAIR, "paired", build_paired)
}

/// The server of the materialize cases' steward (`prepare_materialize`).
pub fn materialize() -> Result<u16, String> {
    static PAIR: Built = OnceLock::new();
    serve(&PAIR, "materialize", prepare_materialize)
}

fn read(path: &Path) -> Result<String, String> {
    fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))
}

fn write(path: &Path, text: &str) -> Result<(), String> {
    fs::write(path, text).map_err(|e| format!("{}: {e}", path.display()))
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
fn splice_slot_family(root: &Path) -> Result<(), String> {
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
    let holdings = prepare_holdings(directory)?;
    let root = &holdings.root;
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
    pin_steward(root, &directory.join("stewards.toml"))?;
    run(holdings
        .command()
        .arg("--out")
        .arg(directory.join("reg_meta_fixture.db")))?;
    std::os::unix::fs::symlink(&holdings.base, directory.join("reg_meta.db"))
        .map_err(|e| e.to_string())
}
