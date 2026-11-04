//! An immutable base reference used by the ordinary curation premise/lock path.
//! Only supplemental-to-reference derivation is admitted; no identity relation or
//! successor edge can cross this boundary.
use crate::error::Result;
use reg_frame::Locator;
use reg_resolve::curation::SourcePremise;
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use std::collections::BTreeMap;
use std::path::Path;

pub(crate) struct Context {
    db: Connection,
    pub generation: String,
    pub sha256: String,
}

impl Context {
    pub fn open(path: &Path) -> Result<Self> {
        crate::verify::catalog(path, None)?;
        let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        let generation = db.query_row(
            "SELECT value FROM meta WHERE key='generation_id'",
            [],
            |r| r.get(0),
        )?;
        Ok(Self {
            db,
            generation,
            sha256: crate::sha256_file(path)?.0,
        })
    }
    pub fn kind(&self, pid: &str) -> Result<Option<String>> {
        Ok(self
            .db
            .query_row(
                "SELECT kind FROM ident WHERE pid=? AND kind IN ('register','variable')",
                [pid],
                |r| r.get(0),
            )
            .optional()?)
    }
    pub fn premise(&self, pid: &str) -> Result<Option<Vec<SourcePremise>>> {
        let Some(kind) = self.kind(pid)? else {
            return Ok(None);
        };
        Ok(Some(vec![SourcePremise {
            locator: Locator {
                input: "catalog".into(),
                at: pid.into(),
            },
            fields: BTreeMap::from([
                ("kind".into(), serde_json::json!(kind)),
                ("pid".into(), serde_json::json!(pid)),
                (
                    "catalog_generation".into(),
                    serde_json::json!(self.generation),
                ),
                ("catalog_sha256".into(), serde_json::json!(self.sha256)),
            ])
            .into(),
        }]))
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Relation {
    pub subject_kind: String,
    pub subject_pid: String,
    pub object_kind: String,
    pub object_pid: String,
    pub start: Option<String>,
    pub end: Option<String>,
    pub statement: String,
    pub caveat: Option<String>,
}
