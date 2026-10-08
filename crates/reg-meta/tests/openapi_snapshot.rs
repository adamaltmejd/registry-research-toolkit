//! The committed `openapi.json` equals the document `reg-meta serve` publishes. The
//! SPA generates its types for the Rust server from that file (`bun run gen:types`).
//!
//! Regenerate with `REG_META_BLESS=1 cargo test -p reg-meta --test openapi_snapshot`.

use std::path::Path;

use reg_catalog::ops;

/// Fails when an operation, parameter or result schema changes without the snapshot.
/// The version is fixed so a release bump leaves the snapshot alone.
#[test]
fn openapi_snapshot_is_current() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("openapi.json");
    let rendered = ops::openapi("0").to_pretty_json().unwrap() + "\n";
    if std::env::var_os("REG_META_BLESS").is_some() {
        std::fs::write(&path, &rendered).unwrap();
    }
    let committed = std::fs::read_to_string(&path).unwrap_or_default();
    assert!(
        committed == rendered,
        "{} is stale; regenerate it with REG_META_BLESS=1 cargo test -p reg-meta --test openapi_snapshot",
        path.display()
    );
}
