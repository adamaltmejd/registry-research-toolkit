//! The release tier (conformance/README.md, "Release checks"): black-box checks on a
//! real catalog that neither the fixture corpus nor `reg-build verify`/`diff` reach.
//! Release step 8f and integration.yml run it once per catalog.
//!
//! `REG_ARTIFACT_DIR` names the artifact directory (`reg_meta.db`). `REG_CATALOG=NAME`
//! also serves `reg_meta_NAME.db` beside it, as `--catalog NAME`. Without
//! `REG_ARTIFACT_DIR` the run uses the synthetic `base` fixture (`scripts/gate.sh
//! rust`), so the target stays runnable between releases. Run it after building
//! `reg-meta` (`$REG_META_BIN` for a release build), by name: `cargo test -p
//! reg-conformance --test release`.

#[path = "../common/mod.rs"]
#[allow(
    dead_code,
    reason = "the release checks use the server and client only"
)]
mod common;

mod held;
mod order;
mod search;

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use rusqlite::{Connection, OpenFlags};
use serde_json::Value;

use common::{client, server};

/// The artifact under test and the catalog it is served as.
pub(crate) struct Artifact {
    pub(crate) dir: PathBuf,
    /// The steward catalog name; `None` serves `reg_meta.db` alone.
    pub(crate) catalog: Option<String>,
    /// Whether the artifact is a real one (`REG_ARTIFACT_DIR`), not the fixture.
    pub(crate) real: bool,
}

/// The run's artifact, from the environment.
pub(crate) fn artifact() -> &'static Artifact {
    static ARTIFACT: OnceLock<Artifact> = OnceLock::new();
    ARTIFACT.get_or_init(|| {
        let catalog = std::env::var("REG_CATALOG")
            .ok()
            .filter(|c| !c.is_empty() && c != "global");
        if let Some(dir) = std::env::var_os("REG_ARTIFACT_DIR").filter(|d| !d.is_empty()) {
            return Artifact {
                dir: PathBuf::from(dir),
                catalog,
                real: true,
            };
        }
        assert!(
            catalog.is_none(),
            "REG_CATALOG needs REG_ARTIFACT_DIR: the fixture has no steward pair"
        );
        let db = reg_conformance::cached_reader_artifact(&reg_conformance::fixture("base"))
            .unwrap_or_else(|e| panic!("{e}"));
        Artifact {
            dir: db.parent().expect("the artifact directory").to_path_buf(),
            catalog: None,
            real: false,
        }
    })
}

impl Artifact {
    /// The reference catalog, read-only.
    pub(crate) fn base(&self) -> Connection {
        open(&self.dir.join("reg_meta.db"))
    }

    /// The steward's holdings file, read-only, when one is served.
    pub(crate) fn holdings(&self) -> Option<Connection> {
        let catalog = self.catalog.as_ref()?;
        Some(open(&self.dir.join(format!("reg_meta_{catalog}.db"))))
    }

    /// The port of the artifact's shared server.
    pub(crate) fn port(&self) -> u16 {
        match &self.catalog {
            None => server::server(&self.dir),
            Some(catalog) => server::steward_server(&self.dir, catalog),
        }
        .unwrap_or_else(|e| panic!("{e}"))
        .port
    }
}

/// A SQLite file opened read-only: a release's evidence is never written.
fn open(path: &Path) -> Connection {
    Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// Every row of `sql` over `params` on `conn`, through `row`.
pub(crate) fn rows<T>(
    conn: &Connection,
    sql: &str,
    params: impl rusqlite::Params,
    row: impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
) -> Vec<T> {
    conn.prepare_cached(sql)
        .and_then(|mut statement| statement.query_map(params, row)?.collect())
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// `GET target`'s document, which must answer 200.
pub(crate) fn get(port: u16, target: &str) -> Result<Value, String> {
    let (status, document) = client::json(port, "GET", target, &[], None)?;
    if status != 200 {
        return Err(format!(
            "GET {target}: {status} {}",
            document["error"]["code"]
        ));
    }
    Ok(document)
}

/// `path?query`, the pairs form-encoded.
pub(crate) fn target(path: &str, query: &[(&str, &str)]) -> String {
    let mut form = form_urlencoded::Serializer::new(String::new());
    form.extend_pairs(query);
    format!("{path}?{}", form.finish())
}
