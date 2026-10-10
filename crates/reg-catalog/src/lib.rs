//! `reg-catalog`: the reader (`RUST_RUNTIME_SPEC.md` sections 6 and 7). It opens and
//! admits one catalog artifact and defines the operation set ([`ops`]), which the
//! `reg-meta` binary serves.

mod docs;
mod error;
mod held;
pub mod ops;

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use rusqlite::functions::FunctionFlags;
use rusqlite::{Connection, OpenFlags};
use serde::Serialize;
use utoipa::ToSchema;

pub use docs::Docs;
pub use error::{Code, Error, Spec};

/// `contract_version` in `conformance/api/operations.toml`.
pub const CONTRACT_VERSION: &str = "4.0.0";
/// The schema gate: the artifact's major must equal this major and its minor be at
/// least this minor.
pub const SCHEMA: (u32, u32) = (10, 0);
const DB_FILENAME: &str = "reg_meta.db";

/// The read scope (section 7); every read takes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    Holdings,
    Reference,
}

impl Scope {
    /// The scope's name, as derived tables key their per-scope rows.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Holdings => "holdings",
            Self::Reference => "reference",
        }
    }
}

/// An admitted artifact: schema-compatible, publishable and, when named, the
/// selected catalog.
pub struct Catalog {
    path: PathBuf,
    manifest: BTreeMap<String, String>,
    mmap_size: i64,
}

impl Catalog {
    /// Open `dir/reg_meta.db` and admit it. `selected` is `--catalog NAME`: `global`
    /// for the catalog artifact, else a steward id. `mmap_size` is each connection's
    /// `PRAGMA mmap_size` (`DEFAULT_MMAP_SIZE`; 0 disables memory-mapping).
    ///
    /// # Errors
    ///
    /// `db_not_found`, `schema_incompatible`, `catalog_unpublishable` or
    /// `catalog_mismatch`.
    pub fn open(dir: &Path, selected: Option<&str>, mmap_size: i64) -> Result<Self, Error> {
        let path = dir.join(DB_FILENAME);
        if !path.is_file() {
            return Err(Error::new(
                Code::DbNotFound,
                format!("Database not found: {}", path.display()),
                vec![],
            ));
        }
        let manifest = read_manifest(&path, mmap_size).map_err(|err| {
            Error::new(
                Code::SchemaIncompatible,
                format!(
                    "Database manifest is unreadable in {}: {err}",
                    path.display()
                ),
                vec![serde_json::Value::Null, supported(SCHEMA).into()],
            )
        })?;
        let catalog = Self {
            path,
            manifest,
            mmap_size,
        };
        catalog.gate_schema()?;
        catalog.admit_identity()?;
        // `global` names the catalog artifact, never a steward that calls itself so.
        if let Some(name) = selected
            && (name != catalog.name() || (name == "global" && catalog.is_steward()))
        {
            return Err(Error::new(
                Code::CatalogMismatch,
                format!(
                    "Selected catalog {name:?} disagrees with the artifact's catalog {:?}.",
                    catalog.name()
                ),
                vec![name.into(), catalog.name().into()],
            ));
        }
        Ok(catalog)
    }

    fn gate_schema(&self) -> Result<(), Error> {
        let version = self.manifest("schema_version");
        if admits(SCHEMA, version) {
            return Ok(());
        }
        Err(Error::new(
            Code::SchemaIncompatible,
            format!(
                "Database schema {version:?} ({}) is not supported (supported: {}).",
                self.path.display(),
                supported(SCHEMA)
            ),
            vec![version.into(), supported(SCHEMA).into()],
        ))
    }

    /// Today's `validate_catalog_selection`, plus the `import_date` that `context`
    /// reports.
    fn admit_identity(&self) -> Result<(), Error> {
        let generation = self.manifest("generation_id");
        let publishable = matches!(
            self.manifest("catalog_artifact_kind"),
            "catalog" | "steward"
        ) && self.manifest("catalog_publishable") == "true"
            && self.manifest("catalog_completeness") == "complete"
            && generation.len() == 64
            && generation
                .bytes()
                .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
            && !self.manifest("import_date").is_empty()
            && (!self.is_steward() || is_slug(self.manifest("steward")));
        if publishable {
            Ok(())
        } else {
            Err(Error::new(
                Code::CatalogUnpublishable,
                "Selected artifact lacks a publishable catalog identity and generation.",
                vec![],
            ))
        }
    }

    /// A manifest value, or `""` when the key is absent.
    #[must_use]
    pub fn manifest(&self, key: &str) -> &str {
        self.manifest.get(key).map_or("", String::as_str)
    }

    #[must_use]
    pub fn generation(&self) -> &str {
        self.manifest("generation_id")
    }

    #[must_use]
    pub fn is_steward(&self) -> bool {
        self.manifest("catalog_artifact_kind") == "steward"
    }

    /// The catalog's name: `global`, or the steward id (an admitted slug).
    #[must_use]
    pub fn name(&self) -> &str {
        if self.is_steward() {
            self.manifest("steward")
        } else {
            "global"
        }
    }

    #[must_use]
    pub fn default_scope(&self) -> Scope {
        if self.is_steward() {
            Scope::Holdings
        } else {
            Scope::Reference
        }
    }

    /// The effective scope for a request's `scope` value.
    ///
    /// # Errors
    ///
    /// `invalid_parameter` for an unknown value; `scope_unavailable` for `holdings`
    /// on a catalog artifact.
    pub fn scope(&self, value: Option<&str>) -> Result<Scope, Error> {
        match value {
            None => Ok(self.default_scope()),
            Some("reference") => Ok(Scope::Reference),
            Some("holdings") if self.is_steward() => Ok(Scope::Holdings),
            Some("holdings") => Err(Error::new(
                Code::ScopeUnavailable,
                "Holdings scope requires a steward artifact; this artifact is a catalog.",
                vec!["holdings".into()],
            )),
            Some(_) => Err(Error::invalid_parameter("scope")),
        }
    }

    /// A fresh read-only connection with the `reg-core` folds as SQL functions
    /// (`fold_search`, `fold_identity`, `fts_term`) and `ln`; the server opens one per
    /// request.
    ///
    /// # Errors
    ///
    /// `catalog_unavailable` when the admitted file cannot be opened.
    pub fn connect(&self) -> Result<Connection, Error> {
        connect(&self.path, self.mmap_size)
            .and_then(with_folds)
            .map_err(|err| unavailable(&self.path, &err))
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

/// A schema gate: `version`'s major equals the gate's and its minor is at least the
/// gate's.
fn admits(gate: (u32, u32), version: &str) -> bool {
    let mut parts = version.split('.').map(str::parse::<u32>);
    matches!(
        (parts.next(), parts.next()),
        (Some(Ok(major)), Some(Ok(minor))) if major == gate.0 && minor >= gate.1
    )
}

fn supported(gate: (u32, u32)) -> String {
    format!("{}.{} or a later {}.x", gate.0, gate.1, gate.0)
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

/// Register the folds. Each maps SQL NULL to NULL; `fts_term(text, term)` is
/// today's `py_fts_term`: some search term of `text` starts with `term`.
fn with_folds(conn: Connection) -> rusqlite::Result<Connection> {
    let flags = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC;
    let unary = |name: &str, fold: fn(&str) -> String| {
        conn.create_scalar_function(name, 1, flags, move |ctx| {
            Ok(ctx.get::<Option<String>>(0)?.map(|s| fold(&s)))
        })
    };
    // SQLite's math `ln`, which the bundled build omits; the code arm ranks by it.
    conn.create_scalar_function("ln", 1, flags, |ctx| {
        Ok(ctx.get::<Option<f64>>(0)?.map(f64::ln))
    })?;
    unary("fold_search", reg_core::fold_search)?;
    unary("fold_identity", reg_core::fold_identity)?;
    conn.create_scalar_function("fts_term", 2, flags, |ctx| {
        let (text, term) = (ctx.get::<Option<String>>(0)?, ctx.get::<String>(1)?);
        Ok(text.is_some_and(|text| {
            reg_core::fts_terms(&text)
                .iter()
                .any(|token| token.starts_with(&term))
        }))
    })?;
    Ok(conn)
}

fn read_manifest(path: &Path, mmap_size: i64) -> rusqlite::Result<BTreeMap<String, String>> {
    let conn = connect(path, mmap_size)?;
    let mut stmt = conn.prepare("SELECT key, value FROM import_manifest")?;
    stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect()
}

/// A steward id is a slug: exactly what parses as a one-segment FQID.
fn is_slug(value: &str) -> bool {
    matches!(value.parse(), Ok(reg_core::Fqid::Provider { .. }))
}
