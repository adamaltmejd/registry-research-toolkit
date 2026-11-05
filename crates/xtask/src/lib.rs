//! Repository tooling behind `cargo xtask` (alias in `.cargo/config.toml`).
//!
//! The repo-policy lints live in this crate's tests, so `cargo test --workspace` runs
//! them on every change.

pub mod gen_case_folding;
pub mod gh_issue;
pub mod json;
pub mod release_base_reuse;
pub mod search_eval;
pub mod usage;

pub const USAGE: &str = "usage: cargo xtask gh-issue view <n> [--comments]
       cargo xtask release-base-reuse <earlier evidence dir>
       cargo xtask gen-case-folding --source <CaseFolding.txt>
       cargo xtask search-eval [--url URL] [--limit N]
       cargo xtask usage volume|top|zero|perf|rollup [...]";
