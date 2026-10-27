//! `reg-catalog`: the reader (`crates/DESIGN.md`, "Crates and run modes"). It opens and
//! admits one catalog artifact and defines the operation set ([`ops`]), which the
//! `reg-meta` binary serves.

mod error;
pub mod ops;

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OpenFlags};
use serde_json::Value;

pub use error::{Code, Error, Spec};

/// `contract_version` in `conformance/api/operations.toml`.
pub const CONTRACT_VERSION: &str = "5.0.0";
const DB_FILENAME: &str = "reg_meta.db";
/// The `meta` keys every catalog carries (`reg-build`'s `schema.sql` and `checks.sql`).
const REQUIRED_META: [&str; 10] = [
    "schema_version",
    "generation_id",
    "catalog_version",
    "builder_version",
    "sqlite_version",
    "open_start",
    "open_end",
    "curation_digest",
    "period_grammar",
    "delivery_name_grammar",
];

/// An admitted catalog: its `meta` has every required key, `schema_version` is
/// exactly [`reg_core::CATALOG_SCHEMA_VERSION`] and `generation_id` is 64 lowercase
/// hex. Publishability is `reg-build verify`'s, not the reader's.
pub struct Catalog {
    path: PathBuf,
    meta: BTreeMap<String, String>,
    mmap_size: i64,
}

impl Catalog {
    /// Open `dir/reg_meta.db` and admit it. `mmap_size` is each connection's
    /// `PRAGMA mmap_size` (`DEFAULT_MMAP_SIZE`; 0 disables memory-mapping).
    ///
    /// # Errors
    ///
    /// `db_not_found`, or `schema_incompatible` for a file that is not a readable
    /// catalog, another schema version, a missing `meta` key or a malformed
    /// `generation_id`.
    pub fn open(dir: &Path, mmap_size: i64) -> Result<Self, Error> {
        let path = dir.join(DB_FILENAME);
        if !path.is_file() {
            return Err(Error::new(
                Code::DbNotFound,
                format!("Database not found: {}", path.display()),
                vec![],
            ));
        }
        let supported = reg_core::CATALOG_SCHEMA_VERSION.to_string();
        let refuse = |version: Option<&str>, reason: String| {
            Error::new(
                Code::SchemaIncompatible,
                format!("{} is not an admissible catalog: {reason}", path.display()),
                vec![
                    version.map_or(Value::Null, Value::from),
                    supported.as_str().into(),
                ],
            )
        };
        let meta = read_meta(&path, mmap_size)
            .map_err(|err| refuse(None, format!("manifest is unreadable ({err})")))?;
        let version = meta.get("schema_version").map(String::as_str);
        if version != Some(supported.as_str()) {
            return Err(refuse(
                version,
                format!("schema version {version:?} is not {supported:?}"),
            ));
        }
        if let Some(key) = REQUIRED_META.iter().find(|key| !meta.contains_key(**key)) {
            return Err(refuse(version, format!("meta has no {key:?}")));
        }
        let generation = &meta["generation_id"];
        if generation.len() != 64
            || !generation
                .bytes()
                .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        {
            return Err(refuse(
                version,
                format!("generation_id {generation:?} is not 64 lowercase hex digits"),
            ));
        }
        Ok(Self {
            path,
            meta,
            mmap_size,
        })
    }

    /// A required `meta` value.
    #[must_use]
    pub fn meta(&self, key: &str) -> &str {
        &self.meta[key]
    }

    /// The admitted database file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub fn generation(&self) -> &str {
        self.meta("generation_id")
    }

    /// A fresh read-only connection; the server opens one per request.
    ///
    /// # Errors
    ///
    /// `catalog_unavailable` when the admitted file cannot be opened.
    pub fn connect(&self) -> Result<Connection, Error> {
        connect(&self.path, self.mmap_size).map_err(|err| unavailable(&self.path, &err))
    }
}

/// `catalog_unavailable` for a file the server admitted but cannot open now.
fn unavailable(path: &Path, err: &rusqlite::Error) -> Error {
    Error::new(
        Code::CatalogUnavailable,
        format!("{} cannot be read: {err}", path.display()),
        vec![],
    )
}

/// Lowercase hex of `bytes`: cursors and `ETag` digests.
#[must_use]
pub fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 * bytes.len());
    for byte in bytes {
        write!(out, "{byte:02x}").expect("write to String");
    }
    out
}

/// The default `PRAGMA mmap_size` (`serve --mmap-size`): memory-map the file (SQLite
/// caps the size at its compile-time maximum, about 2 GiB), since a connection per
/// request otherwise reads every page through its own small cache, and concurrent
/// requests spend most of their time in `pread`. On a cold Linux page cache, mmap
/// read-around reads several times more bytes than `pread`, so a small-memory host
/// with slow storage may serve faster with 0 (off).
pub const DEFAULT_MMAP_SIZE: i64 = 1 << 31;

/// `immutable=1`: the published files never change in place (they are replaced by
/// rename), so SQLite skips locking and never creates `-wal`/`-shm` sidecars, which
/// a read-only directory would refuse.
fn connect(path: &Path, mmap_size: i64) -> rusqlite::Result<Connection> {
    let mut uri = String::from("file:");
    for c in path.to_string_lossy().chars() {
        match c {
            '%' => uri.push_str("%25"),
            '?' => uri.push_str("%3f"),
            '#' => uri.push_str("%23"),
            c => uri.push(c),
        }
    }
    uri.push_str("?mode=ro&immutable=1");
    let conn = Connection::open_with_flags(
        uri,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.pragma_update(None, "mmap_size", mmap_size)?;
    Ok(conn)
}

fn read_meta(path: &Path, mmap_size: i64) -> rusqlite::Result<BTreeMap<String, String>> {
    let conn = connect(path, mmap_size)?;
    let mut stmt = conn.prepare("SELECT key, value FROM meta")?;
    stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect()
}
