//! The documentation database, `reg_meta_docs.db` beside the catalog. It is optional:
//! without it the docs operations answer `docs_unavailable`; a present but
//! incompatible one refuses startup (`errors.toml`).

use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension};

use crate::{Code, Error, admits, connect, supported, unavailable};

/// The docs schema gate, as `reg_meta.doc_db.DOC_SCHEMA_VERSION`.
const DOC_SCHEMA: (u32, u32) = (1, 3);
const DOC_DB_FILENAME: &str = "reg_meta_docs.db";

/// An admitted docs database.
pub struct Docs {
    path: PathBuf,
}

impl Docs {
    /// Admit `dir/reg_meta_docs.db`; `None` when there is none.
    ///
    /// # Errors
    ///
    /// `doc_schema_incompatible` when its schema version is missing, unreadable or
    /// outside the gate.
    pub fn open(dir: &Path) -> Result<Option<Self>, Error> {
        let path = dir.join(DOC_DB_FILENAME);
        if !path.is_file() {
            return Ok(None);
        }
        let version: Option<String> = connect(&path)
            .and_then(|conn| {
                conn.query_row(
                    "SELECT value FROM doc_meta WHERE key = 'schema_version'",
                    [],
                    |row| row.get(0),
                )
                .optional()
            })
            .unwrap_or_default();
        if version.as_deref().is_some_and(|v| admits(DOC_SCHEMA, v)) {
            return Ok(Some(Self { path }));
        }
        Err(Error::new(
            Code::DocSchemaIncompatible,
            format!(
                "Docs database schema {version:?} ({}) is not supported (supported: {}).",
                path.display(),
                supported(DOC_SCHEMA)
            ),
            vec![],
        ))
    }

    /// A fresh read-only connection; the server opens one per request.
    ///
    /// # Errors
    ///
    /// `catalog_unavailable` when the admitted file cannot be opened.
    pub fn connect(&self) -> Result<Connection, Error> {
        connect(&self.path).map_err(|err| unavailable(&self.path, &err))
    }
}
