//! The conformance suite (conformance/README.md): one module per surface, each a
//! corpus runner over `conformance/cases/<surface>`. Run it after
//! `cargo build --workspace`, by name: `cargo test -p reg-conformance --test
//! conformance` (`scripts/gate.py rust`).

#[path = "../common/mod.rs"]
mod common;

mod api;
mod api_spec;
mod build_report;
mod holdings;
mod holdings_admission;
mod holdings_reference;
mod ids;
mod lock;
mod materialize;
mod mcp;
mod order;
