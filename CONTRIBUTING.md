# Contributing

## Setup

Prerequisites: a stable **Rust** toolchain for `crates/`, [bun](https://bun.sh) for the
SPA, and for the gate `cargo-machete` and `shellcheck`.

```bash
git clone https://github.com/adamaltmejd/registry-research-toolkit.git
cd registry-research-toolkit
cargo install cargo-machete --locked
```

## Testing

```bash
bash scripts/gate.sh all   # the full gate before a PR
bash scripts/gate.sh rust  # the workspace build and conformance against the Rust server
cargo test --workspace     # the crates
```

Conformance checks on a real artifact run only with
`REG_ARTIFACT_DIR=... cargo test -p reg-conformance --test release` (see
`conformance/README.md`).

There are no git hooks: CI (`ci.yml`) runs the gate on every PR to main, and merges
require it green. Run the relevant steps locally before pushing.

## Linting

```bash
bash scripts/gate.sh lint   # shellcheck
cargo fmt --all             # fix Rust formatting
```

## Releasing

Use the `release` skill (`.agents/skills/release/SKILL.md`, `/release` in Claude Code).
It covers the version bump, the tag, building or copying forward the DB assets, and the
release workflows.
