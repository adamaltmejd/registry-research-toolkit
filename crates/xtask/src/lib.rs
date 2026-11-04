//! Repository tooling behind `cargo xtask` (alias in `.cargo/config.toml`).
//!
//! The repo-policy lints live in this crate's tests, so `cargo test --workspace` runs
//! them on every change.

pub mod gh_issue;
pub mod json;
pub mod release_base_reuse;
pub mod scope_check;

pub const USAGE: &str = "usage: cargo xtask gh-issue view <n> [--comments]
       cargo xtask scope-check <reg_meta_swecov.db>
       cargo xtask release-base-reuse <earlier evidence dir>";
