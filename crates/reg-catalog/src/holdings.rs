//! Pair admission and database-origin selection for steward metadata. Row ids never
//! cross the two database boundaries; physical placements use public ids.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read as _;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;

use crate::{Catalog, Code, Error, connect, hex, read_meta};

pub(crate) struct Holdings {
    path: PathBuf,
    meta: BTreeMap<String, String>,
}

/// The selected steward and the identities of its admitted catalog/holdings pair.
#[derive(Clone, Serialize, ToSchema)]
pub struct Steward {
    pub owner: String,
    pub catalog_generation: String,
    pub holdings_generation: String,
    pub schema_version: String,
}

/// `Context.steward_config`: the steward's compiled presentation, logo and scope. The
/// server passes the admitted JSON through; this type is its API schema and the
/// admission check that keeps the schema true.
#[derive(ToSchema)]
#[expect(
    dead_code,
    reason = "schema only; the served value is the admitted JSON"
)]
pub struct StewardConfig {
    presentation: StewardPresentation,
    /// The logo's SVG markup.
    logo: String,
    /// The steward's `scope.toml` as JSON; its shape is the steward spec's, not this API's.
    #[schema(value_type = Object)]
    scope: serde_json::Value,
}

/// The steward's `steward.json`. Other keys pass through untyped.
#[derive(Deserialize, ToSchema)]
#[expect(dead_code, reason = "deserialized only to check the admitted JSON")]
pub struct StewardPresentation {
    id: String,
    name: String,
    long_name: Option<String>,
    /// Inline markdown: emphasis and `[text](https://…)` links.
    description: Option<String>,
    /// How a researcher hands in an order.
    order_instructions: Option<String>,
}

fn file_digest(path: &Path) -> std::io::Result<String> {
    let mut file = File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0_u8; 65_536];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    Ok(hex(&hash.finalize()))
}

impl Catalog {
    /// Admit the reference catalog and, for a non-global selection, its pinned
    /// `reg_meta_<steward>.db` partner. The owner comes from startup configuration.
    ///
    /// # Errors
    /// `invalid_parameter`, `db_not_found` or `schema_incompatible` before serving.
    pub fn open_selected(dir: &Path, selected: &str, mmap_size: i64) -> Result<Self, Error> {
        let mut catalog = Self::open(dir, mmap_size)?;
        if selected == "global" {
            return Ok(catalog);
        }
        if selected.is_empty()
            || !selected
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return Err(Error::invalid_parameter("--catalog"));
        }
        let path = dir.join(format!("reg_meta_{selected}.db"));
        if !path.is_file() {
            return Err(Error::new(
                Code::DbNotFound,
                "Selected holdings database is missing.",
                vec![],
            ));
        }
        let refuse = |reason: &str| {
            Error::new(
                Code::SchemaIncompatible,
                format!("Holdings pair is inadmissible: {reason}."),
                vec![
                    serde_json::Value::Null,
                    reg_core::HOLDINGS_SCHEMA_VERSION.to_string().into(),
                ],
            )
        };
        let meta =
            read_meta(&path, mmap_size).map_err(|_| refuse("unreadable holdings manifest"))?;
        let physical_schema: u32 = connect(&path, mmap_size)
            .and_then(|conn| conn.query_row("PRAGMA user_version", [], |r| r.get(0)))
            .map_err(|_| refuse("unreadable holdings schema"))?;
        if physical_schema != reg_core::HOLDINGS_SCHEMA_VERSION {
            return Err(refuse("physical schema version does not match"));
        }
        let expected_schema = reg_core::HOLDINGS_SCHEMA_VERSION.to_string();
        let semantic_schema = reg_core::CATALOG_SCHEMA_VERSION.to_string();
        for (key, expected) in [
            ("schema_version", expected_schema.as_str()),
            ("semantic_schema_version", semantic_schema.as_str()),
            ("owner_kind", "steward"),
            ("owner_id", selected),
            ("catalog_generation", catalog.meta("generation_id")),
        ] {
            if meta.get(key).map(String::as_str) != Some(expected) {
                return Err(refuse(&format!("{key} does not match")));
            }
        }
        let generation = meta
            .get("generation_id")
            .ok_or_else(|| refuse("missing generation_id"))?;
        if generation.len() != 64
            || !generation
                .bytes()
                .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        {
            return Err(refuse("malformed generation_id"));
        }
        if meta
            .get("steward")
            .and_then(|value| serde_json::from_str::<StewardPresentation>(value).ok())
            .is_none()
        {
            return Err(refuse(
                "steward must be a JSON object with string id and name",
            ));
        }
        if meta
            .get("scope")
            .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
            .is_none_or(|value| !value.is_object())
        {
            return Err(refuse("scope must be a JSON object"));
        }
        if meta.get("logo").is_none_or(|value| value.trim().is_empty()) {
            return Err(refuse("missing logo"));
        }
        let digest = file_digest(&catalog.path).map_err(|_| refuse("catalog cannot be hashed"))?;
        if meta.get("catalog_sha256") != Some(&digest) {
            return Err(refuse("catalog file digest does not match"));
        }
        let mut pair = Sha256::new();
        pair.update(catalog.meta("generation_id").as_bytes());
        pair.update(generation.as_bytes());
        catalog.generation = hex(&pair.finalize());
        catalog.holdings = Some(Holdings { path, meta });
        catalog.admit_identities()?;
        Ok(catalog)
    }

    fn admit_identities(&self) -> Result<(), Error> {
        let conn = self.connect()?;
        self.attach_holdings(&conn)?;
        let collision: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM main.ident b JOIN held.ident s ON s.pid = b.pid
             WHERE (b.kind != 'provider' OR s.kind != 'provider') AND NOT (
               b.kind=s.kind AND (
               (b.kind='variable' AND EXISTS(SELECT 1 FROM held.holding_reference_occurrence x WHERE x.variable_pid=b.pid))
               OR (b.kind='table' AND EXISTS(SELECT 1 FROM held.holding_reference_occurrence x WHERE x.table_pid=b.pid))
               OR (b.kind='register' AND EXISTS(SELECT 1 FROM held.holding_reference_occurrence x
                 JOIN held.variable v ON v.pid=x.variable_pid JOIN held.register r ON r.id=v.register_id WHERE r.pid=b.pid))
               -- An overlay: the steward adds its own variables to the same register or table.
               OR (b.kind='register' AND EXISTS(SELECT 1 FROM main.register br JOIN main.provider bp ON bp.id=br.provider_id
                 JOIN held.register hr ON hr.pid=br.pid JOIN held.provider hp ON hp.id=hr.provider_id
                 WHERE br.pid=b.pid AND hp.pid=bp.pid)
                 -- Its implicit table shares the pid and must match too.
                 AND NOT EXISTS(SELECT 1 FROM held.tbl ht JOIN held.register hr ON hr.id=ht.register_id
                   WHERE ht.pid=b.pid AND NOT EXISTS(SELECT 1 FROM main.tbl bt JOIN main.register br ON br.id=bt.register_id
                     WHERE bt.pid=ht.pid AND br.pid=hr.pid AND bt.timeless=ht.timeless)))
               OR (b.kind='table' AND EXISTS(SELECT 1 FROM main.tbl bt JOIN main.register br ON br.id=bt.register_id
                 JOIN held.tbl ht ON ht.pid=bt.pid JOIN held.register hr ON hr.id=ht.register_id
                 WHERE bt.pid=b.pid AND hr.pid=br.pid AND ht.timeless=bt.timeless)))))
             OR EXISTS(SELECT 1 FROM main.ident_retired b JOIN held.ident s ON s.pid = b.pid)
             OR EXISTS(SELECT 1 FROM main.ident b JOIN held.ident_retired s ON s.pid = b.pid)",
                [],
                |r| r.get(0),
            )
            .map_err(|_| Error::new(Code::SchemaIncompatible, "Supplemental identity index is unreadable.", vec![]))?;
        if collision {
            return Err(Error::new(
                Code::SchemaIncompatible,
                "Supplemental public id collides with the catalog.",
                vec![],
            ));
        }
        Ok(())
    }

    /// The admitted steward's identity; absent in reference-only mode.
    #[must_use]
    pub fn steward(&self) -> Option<Steward> {
        self.holdings.as_ref().map(|h| Steward {
            owner: h.meta["owner_id"].clone(),
            catalog_generation: h.meta["catalog_generation"].clone(),
            holdings_generation: h.meta["generation_id"].clone(),
            schema_version: h.meta["schema_version"].clone(),
        })
    }

    /// Compiled presentation/config data is served from the admitted artifact;
    /// runtime never reads the steward repository.
    pub(crate) fn steward_config(&self) -> Option<serde_json::Value> {
        self.holdings.as_ref().map(|h| serde_json::json!({
            "presentation": serde_json::from_str::<serde_json::Value>(&h.meta["steward"]).expect("admitted JSON"),
            "logo": h.meta["logo"],
            "scope": serde_json::from_str::<serde_json::Value>(&h.meta["scope"]).expect("admitted JSON"),
        }))
    }

    /// A read-only connection to the supplemental semantics and physical holdings.
    ///
    /// # Errors
    /// `catalog_unavailable` when the admitted partner is no longer readable.
    pub fn connect_holdings(&self) -> Result<Option<Connection>, Error> {
        self.holdings
            .as_ref()
            .map(|h| {
                connect(&h.path, self.mmap_size).map_err(|err| crate::unavailable(&h.path, &err))
            })
            .transpose()
    }

    /// Resolve the database origin before using any local row id. Exact public ids
    /// take precedence over names; a bare name in both origins stays ambiguous.
    ///
    /// # Errors
    /// The usual reference errors, or `catalog_unavailable` for an unreadable file.
    pub fn connect_for_ref(&self, value: &str) -> Result<Connection, Error> {
        let base = self.connect()?;
        let Some(extra) = self.connect_holdings()? else {
            return Ok(base);
        };
        let exact = |conn: &Connection| -> Result<bool, Error> {
            Ok(conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM ident WHERE pid = ?)",
                [value],
                |r| r.get(0),
            )?)
        };
        if exact(&base)? {
            return Ok(base);
        }
        if exact(&extra)? {
            return Ok(extra);
        }
        let first = crate::ops::refs::resolve(&base, value);
        let second = crate::ops::refs::resolve(&extra, value);
        match (first, second) {
            (Ok(a), Ok(b)) => Err(Error::new(
                Code::AmbiguousRef,
                "Name occurs in both catalog and steward metadata; use a public id.",
                vec![
                    value.into(),
                    serde_json::json!([
                        {"fqid": a.pid, "kind": a.kind}, {"fqid": b.pid, "kind": b.kind}
                    ]),
                ],
            )),
            (Err(first), Err(second)) => Err(
                if (second.kind() == Code::AmbiguousRef && first.kind() != Code::AmbiguousRef)
                    || (first.kind() == Code::NotFound && second.kind() == Code::RetiredRef)
                {
                    second
                } else {
                    first
                },
            ),
            (Err(err), _) | (_, Err(err)) if err.kind() == Code::AmbiguousRef => Err(err),
            (Ok(_), _) => Ok(base),
            (_, Ok(_)) => Ok(extra),
        }
    }

    /// Physical facts attached under an explicit database origin for public-id joins.
    pub(crate) fn attach_holdings(&self, conn: &Connection) -> Result<(), Error> {
        let holdings = self
            .holdings
            .as_ref()
            .ok_or_else(|| Error::invalid_parameter("scope"))?;
        conn.execute(
            "ATTACH DATABASE ? AS held",
            [crate::read_uri(&holdings.path)],
        )?;
        Ok(())
    }

    /// Select a content-addressed shared value set or document without exposing ids
    /// from the other database origin.
    pub(crate) fn connect_for_hash(&self, table: &str, hash: &str) -> Result<Connection, Error> {
        let base = self.connect()?;
        let exists = |conn: &Connection| -> Result<bool, Error> {
            Ok(conn
                .query_row(
                    &format!("SELECT 1 FROM {table} WHERE hash = ?"),
                    [hash],
                    |_| Ok(()),
                )
                .optional()?
                .is_some())
        };
        if exists(&base)? {
            return Ok(base);
        }
        if let Some(extra) = self.connect_holdings()?
            && exists(&extra)?
        {
            return Ok(extra);
        }
        Ok(base)
    }
}
