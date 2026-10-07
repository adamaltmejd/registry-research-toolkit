//! Catalog admission and the one spike operation: the variable FTS arm of search.

use std::cell::RefCell;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OpenFlags, params};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The reader's schema gate: same major, DB minor >= code minor (reg_meta.db).
pub const SCHEMA_VERSION: (u32, u32) = (9, 0);
pub const CONTRACT_VERSION: &str = "spike-0";

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ErrorDoc {
    pub code: String,
    pub class: String,
    pub message: String,
    pub remediation: String,
}

impl ErrorDoc {
    fn new(code: &str, class: &str, message: String, remediation: &str) -> Self {
        Self {
            code: code.into(),
            class: class.into(),
            message,
            remediation: remediation.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct Meta {
    pub contract_version: &'static str,
    pub generation_id: String,
    pub scope: &'static str,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct Envelope<T> {
    pub data: T,
    pub meta: Meta,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct SearchParams {
    /// Free-text query; each word is matched as a prefix.
    pub query: String,
    /// Maximum results (default 50, at most 1000).
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct VariableHit {
    pub fqid: Option<String>,
    pub variable_id: i64,
    pub var_id: Option<i64>,
    pub variable_name: String,
    pub register_name: String,
    pub rank: f64,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct SearchResult {
    pub items: Vec<VariableHit>,
    pub next_cursor: Option<String>,
}

pub struct Catalog {
    path: PathBuf,
    generation_id: String,
}

thread_local! {
    // rusqlite connections are not Sync; each worker thread keeps its own.
    static CONN: RefCell<Option<(PathBuf, Connection)>> = const { RefCell::new(None) };
}

fn open(path: &Path) -> rusqlite::Result<Connection> {
    // Same URI as reg_meta.db.open_db: read-only and immutable, so a WAL-mode
    // artifact never needs a -shm file (#283).
    let uri = format!("file:{}?mode=ro&immutable=1", path.display());
    Connection::open_with_flags(
        uri,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
}

impl Catalog {
    /// Open and admit: schema gate plus the publishable/complete identity checks.
    pub fn admit(path: PathBuf) -> Result<Self, ErrorDoc> {
        let db = path.join("reg_meta.db");
        if !db.is_file() {
            return Err(ErrorDoc::new(
                "db_not_found",
                "config",
                format!("No catalog at {}", db.display()),
                "Run `reg-meta fetch` or pass --db DIR.",
            ));
        }
        let conn = open(&db).map_err(|e| internal(e.to_string()))?;
        let manifest = |key: &str| -> Option<String> {
            conn.query_row(
                "SELECT value FROM import_manifest WHERE key = ?",
                [key],
                |r| r.get(0),
            )
            .ok()
        };
        let version = manifest("schema_version").unwrap_or_default();
        let mut parts = version.split('.').map(|p| p.parse::<u32>().ok());
        let (major, minor) = (parts.next().flatten(), parts.next().flatten());
        if major != Some(SCHEMA_VERSION.0) || minor.is_none_or(|m| m < SCHEMA_VERSION.1) {
            return Err(ErrorDoc::new(
                "schema_incompatible",
                "config",
                format!(
                    "Catalog schema {version:?} is incompatible with reader schema {}.{}.",
                    SCHEMA_VERSION.0, SCHEMA_VERSION.1
                ),
                "Fetch a catalog built for this reader.",
            ));
        }
        let generation_id = manifest("generation_id").unwrap_or_default();
        let ok_identity = generation_id.len() == 64
            && generation_id
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            && manifest("catalog_publishable").as_deref() == Some("true")
            && manifest("catalog_completeness").as_deref() == Some("complete");
        if !ok_identity {
            return Err(ErrorDoc::new(
                "catalog_not_admissible",
                "config",
                "Catalog manifest is not a complete, publishable generation.".into(),
                "Fetch a published catalog release.",
            ));
        }
        Ok(Self {
            path: db,
            generation_id,
        })
    }

    pub fn sqlite_version() -> &'static str {
        rusqlite::version()
    }

    fn with_conn<T>(
        &self,
        f: impl FnOnce(&Connection) -> Result<T, ErrorDoc>,
    ) -> Result<T, ErrorDoc> {
        CONN.with(|cell| {
            let mut slot = cell.borrow_mut();
            if slot.as_ref().is_none_or(|(p, _)| p != &self.path) {
                *slot = Some((
                    self.path.clone(),
                    open(&self.path).map_err(|e| internal(e.to_string()))?,
                ));
            }
            f(&slot.as_ref().unwrap().1)
        })
    }

    pub fn envelope<T>(&self, data: T) -> Envelope<T> {
        Envelope {
            data,
            meta: Meta {
                contract_version: CONTRACT_VERSION,
                generation_id: self.generation_id.clone(),
                scope: "reference",
            },
        }
    }

    /// Mirrors `queries._search_description_variables` in reference scope, without
    /// register/year filters: same MATCH expression, bm25 weights and ordering.
    pub fn search_variables(&self, params: &SearchParams) -> Result<SearchResult, ErrorDoc> {
        let limit = params.limit.unwrap_or(50);
        if limit == 0 || limit > 1000 {
            return Err(ErrorDoc::new(
                "usage_error",
                "usage",
                format!("limit must be 1..=1000, got {limit}"),
                "Pass a limit between 1 and 1000.",
            ));
        }
        let Some(fts) = reg_core_spike::fts_match_query(&params.query) else {
            return Ok(SearchResult {
                items: vec![],
                next_cursor: None,
            });
        };
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare_cached(
                    "SELECT vf.rowid AS variable_id, \
                     CASE WHEN vf.rowid < 4611686018427387904 AND vf.provider_key GLOB '[0-9]*' \
                     AND NOT vf.provider_key GLOB '*[^0-9]*' THEN CAST(vf.provider_key AS INTEGER) \
                     ELSE NULL END AS var_id, \
                     vf.name, bm25(variable_fts, 0.2, 0.2, 6.0, 4.0, 2.0, 1.0, 0.4) AS rank, \
                     r.name, r.slug, p.slug, v.slug \
                     FROM variable_fts vf \
                     JOIN register r ON vf.register_id = r.register_id \
                     JOIN provider p ON p.provider_id = r.provider_id \
                     JOIN variable v ON v.variable_id = vf.rowid \
                     WHERE variable_fts MATCH ? \
                     ORDER BY rank, vf.rowid LIMIT ?",
                )
                .map_err(|e| internal(e.to_string()))?;
            let items = stmt
                .query_map(params![fts, limit], |r| {
                    let slugs: (Option<String>, Option<String>, Option<String>) =
                        (r.get(6)?, r.get(5)?, r.get(7)?);
                    Ok(VariableHit {
                        fqid: match slugs {
                            (Some(p), Some(reg), Some(v)) => Some(format!("{p}/{reg}/{v}")),
                            _ => None,
                        },
                        variable_id: r.get(0)?,
                        var_id: r.get(1)?,
                        variable_name: r.get(2)?,
                        rank: r.get(3)?,
                        register_name: r.get(4)?,
                    })
                })
                .and_then(|rows| rows.collect::<Result<Vec<_>, _>>())
                .map_err(|e| internal(e.to_string()))?;
            Ok(SearchResult {
                items,
                next_cursor: None,
            })
        })
    }
}

fn internal(message: String) -> ErrorDoc {
    ErrorDoc::new(
        "internal_error",
        "internal",
        message,
        "Report this error to maintainers.",
    )
}
