//! `reg-meta serve --catalog fixture` admission of the synthetic pair: a damaged or
//! unpinned holdings file is refused at startup, before the server listens.

use std::fs;
use std::process::Command;

use reg_conformance::Scratch;
use serde_json::Value;

use crate::common::cli::{column, exec, execute};
use crate::common::server;
use crate::common::{client, steward};

#[test]
fn damaged_pair_is_refused_at_startup() {
    // A matching advertised generation cannot bypass the byte pin or let malformed
    // compiled presentation data panic during server startup. The positional-array
    // steward fails if admission reads the presentation through Serde's
    // struct-from-sequence form and serves an array where the OpenAPI promises an
    // object. Each damage starts from the valid meta.
    let pair = steward::pair().unwrap();
    let dir = Scratch::new("admission").unwrap();
    let db = dir.path().join("reg_meta_fixture.db");
    fs::copy(pair.dir.join("reg_meta_fixture.db"), &db).unwrap();
    std::os::unix::fs::symlink(&pair.holdings.base, dir.path().join("reg_meta.db")).unwrap();
    let mut argv = server::serve_argv(dir.path());
    argv.extend(["--catalog".to_owned(), "fixture".to_owned()]);
    let original = column(
        &db,
        "SELECT 'INSERT OR REPLACE INTO meta VALUES(' || quote(key) || ',' || \
         quote(value) || ');' FROM meta",
    )
    .concat();
    let refused = |env: &[(&str, &str)]| {
        let ran = exec(
            Command::new(server::reg_meta())
                .args(&argv)
                .envs(env.iter().copied()),
        );
        assert_eq!(ran.code, Some(10), "{}", ran.stderr);
        let error: Value = serde_json::from_str(&ran.stderr).unwrap();
        assert_eq!(error["code"], "schema_incompatible", "{error}");
        error
    };
    for damage in [
        format!(
            "UPDATE meta SET value='{}' WHERE key='catalog_sha256'",
            "0".repeat(64)
        ),
        "UPDATE meta SET value='{' WHERE key='scope'".to_owned(),
        "UPDATE meta SET value='[\"fixture\", \"Synthetic steward\", null, null, null]' \
         WHERE key='steward'"
            .to_owned(),
        "DELETE FROM meta WHERE key='logo'".to_owned(),
    ] {
        execute(&db, &format!("{original}{damage}"));
        refused(&[]);
    }
    // The image's build-time digest stands in for hashing the catalog file. The
    // mismatch fails if admission ignores REG_META_CATALOG_SHA256 and hashes the
    // (correctly pinned) file; the match fails if admission still hashes the file,
    // whose digest is not the pinned one.
    let pinned = "0".repeat(64);
    execute(&db, &original);
    let error = refused(&[("REG_META_CATALOG_SHA256", &pinned)]);
    let message = error["message"].as_str().unwrap();
    assert!(
        message.contains("catalog file digest does not match"),
        "{message}"
    );
    execute(
        &db,
        &format!("UPDATE meta SET value='{pinned}' WHERE key='catalog_sha256'"),
    );
    // Bound, not a temporary: the server stops when its handle drops.
    let pinned_server =
        server::start(dir.path(), &argv, &[("REG_META_CATALOG_SHA256", &pinned)]).unwrap();
    let (status, _) = client::json(pinned_server.port, "GET", "/api/context", &[], None).unwrap();
    assert_eq!(status, 200);
}
