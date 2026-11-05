# Registry Research Toolkit

Multi-package workspace for Swedish register research: catalog metadata, schema
validation, and project authoring. `ARCHITECTURE.md` has the cross-package design
(topology, dependency graph, repo-wide invariants, testing strategy); each
`<package>/DESIGN.md` has the package rationale (the catalog object model is in
`crates/DESIGN.md`). Remaining work lives in GitHub issues.

## Packages

- `crates/` — the Rust runtime, released as `reg_meta`: `reg-core` (the
  `project_data.json` contract and structural validator, the FQID and period grammars),
  `reg-catalog` (the catalog reader and its operations), `reg-meta` (the binary:
  `reg-meta serve` for the HTTP API and `/mcp`, `reg-meta mcp` for local stdio MCP),
  `reg-core-wasm` (`reg-core` for the SPA), and the catalog builder: `reg-frame`,
  `reg-readers`, `reg-resolve` and `reg-build`, with its reader config, curation and id
  ledgers under `curation/`. `xtask` is repository and release tooling
  (`cargo xtask gh-issue`, `scope-check`, `release-base-reuse`, `gen-case-folding`,
  `search-eval`) and the repo-policy lints; it is not released.
- `reg_webapp` — server deployment: the Rust server container and hosted MCP edge.
- `reg_mona_runner` — `swecov_extract.R`, the extract runner a SWECOV data manager runs
  inside MONA: order manifests in, one file per manifest file out, in the order's
  format: Stata `.dta`, Parquet or CSV (R: DBI, odbc, jsonlite, haven, arrow).
- `reg_export_gate` — the export gate: `export_gate.R`, the version-pinned check a MONA
  pipeline runs as its last step (inventory, structure, disclosure, identifier backstop;
  Parquet plus `receipt.json` out only on a pass), the toolkit's fixed schemas and
  policies, and the case corpus the future `reg-export-gate` Rust binary shares (R:
  jsonlite, arrow).

The `reg_monabundle` (MONA bundle build + runtime + PII scanner) and `mock_data_wizard`
(local mock-data generation) packages have been archived to branch
`archive/mona-subsystem` (tag `mona-subsystem-pre-rebuild`) and removed from `main`.
Local mock data from a `project_data.json` is tracked in #707, as a local skill.

## MONA constraint

[MONA](https://www.scb.se/mona) is Statistics Sweden's microdata platform. Agents are
not allowed on MONA. **PII must never leave MONA — only aggregate statistics are
exported.** This is a domain invariant that remains true regardless of the tooling
state.

# Governance

- `DESIGN.md` per package documents design rationale and constraints.
- No frozen specs or **permanent** implementation trackers — design decisions live in
  DESIGN.md, implementation history lives in git.
- **Exception**: each multi-PR refactor spanning weeks may keep one root-level tracker
  for cross-PR coordination. The current tracker is [SPA-REDESIGN.md](SPA-REDESIGN.md),
  the plain-language SPA redesign (gate: deleted in its last slice). Each tracker is
  **scoped and self-deleting**: it ships with an explicit completion gate (e.g. "deleted
  when stage X ships"), gets deleted at that gate, and never outlives the refactor it
  tracks. Per-package DESIGN.md notes for the same effort are still preferred where the
  scope is package-local.

# Maturity and compatibility

- Pre-v1, no external users — break things freely if it benefits the long-term design.
- Do not write migration code, shims, deprecation wrappers, or backwards-compatibility
  layers. If something needs to change, change it directly.
- Do not preserve old code "just in case." Dead code gets deleted.

# Coding principles

- Deterministic behavior with explicit seed/config.
- Fail fast with actionable errors and stable exit codes.
- Keep domain logic separate from IO/prompts/integrations.
- Validate JSON contracts at read/write boundaries.
- Avoid leaking sensitive row-level content.

## Reuse first, build last

Prefer, in order: no change → existing capability → stdlib/platform → installed dep →
minimal new code → new dep. Two repo-specific notes:

- **The failure mode here is leaf-helper duplication, not new-module inflation.** A new
  adapter/route legitimately needs a new module, but a small format-agnostic leaf inside
  it (a validator, a write loop, a clamp gate) gets re-pasted instead of hoisted. Before
  writing a leaf, check whether an internal capability already does it (`reg-core` and
  `reg-frame` are typical homes). Extend it; don't re-type it.
- **The ladder cuts both ways.** This repo *under*-uses libraries as often as it
  over-builds. "Installed dep solves it → use it" binds as hard as "don't add a dep for
  what a few lines do" — add one only when it removes more complexity than it adds.

**Never simplify away** the load-bearing guards: PII/MONA confinement, k-anonymity /
disclosure control, determinism / byte-identity, JSON-contract validation, fail-fast. A
shorter diff that drops one of these isn't simpler, it's broken.

Mark a deliberate shortcut with a `simplify:` comment naming its ceiling and upgrade
trigger
(`# simplify: O(n^2) scan, index it if the candidate set passes a few thousand`), so a
simplification reads as intent and a deferral can't silently rot.

# Testing policy

Most changes need no new test. Verify by running the existing suite or the real
pipeline; a check run once is not a permanent test. Tests in agentic development cost
more than they look: each one is code a later agent must keep green. Add or extend a
test only for a black-box behavior that is **not obvious from running the code** and
that the release's `reg-build verify` and `reg-build diff` would not surface, and only
when you can name the behavior it pins, a credible change that breaks it, and why no
existing boundary case already catches that failure. Two kinds are defects on sight:
**tautological** tests (the expected value comes from the code under test) and
**change-detector** tests (they break on a refactor that changes no boundary).

Tests pin **contracts and behavior**, never implementation. The toolkit is a
deterministic compiler (`reg-build`) that feeds an immutable artifact to stateless
readers (the `crates/` runtime, `reg_webapp`), so nearly every behavior is observable at
a boundary that outlives the code behind it. `ARCHITECTURE.md` → "Testing strategy"
names the boundaries, the oracles, the tiers and each package's time budget. The
release's own check is `reg-build verify` and `reg-build diff` against the previous
release (release skill, step 8). The rules below are what an agent must follow when
writing, changing or deleting a test. The `test-audit` skill applies them to a change
and to a sweep.

- **End-to-end and integration first.** A test drives the real pipeline, reader or
  server from a boundary and observes its output. A unit test is the exception: allowed
  only where it pins behavior a boundary case cannot reach well (a grammar, interval
  algebra, a pure fold), stated as input → expected output, and it must survive a
  rewrite of the code behind it.
- **Assert only at a named boundary**, one of the table in `ARCHITECTURE.md` →
  "Boundaries and oracles". Anything else is reached through one of those, not tested on
  its own. No private-name imports (`from x import _helper`) and no patching of internal
  modules; mock only process boundaries: network, clock, filesystem, subprocess.
- **Oracles are data.** Expected behavior lives in golden corpora and snapshot files
  (`input → expected`) readable without Python. Changing an expected file is a content
  decision reviewed in the diff, with the reason in the commit. Never weaken, regenerate
  or delete a test only to get a passing run.
- **Fixtures come from readable source** (TOML/JSON/CSV run through the real pipeline),
  never from Python literals of database rows. The synthetic artifact is the unit.
- **At most one case per behavior, at its hardest case, and only where no existing case
  covers it**: a second run, reordered input, an interval edge, a refusal beside its
  allowed twin. Not the first case that passes. Named by behavior; no integration test
  file over 800 lines, split by contract surface.
- **Expected values come from outside the code under test**, or the test is
  tautological: a golden, the source fixture, the spec, or agreement between two
  adapters. Derive them from the spec or fixture before implementing.
- **Every test can fail.** A new or changed test's comment names the regression that
  makes it fail. A refusal asserts the located error code, not only that something
  failed. A contract is asserted once, at the outermost boundary that reaches it; a twin
  is deleted.
- **Delete with the code.** A test that pinned deleted behavior is deleted, not
  retargeted. A refactor that changes no boundary changes no test; if it does, the
  boundary moved and the design note says so.
- **Determinism is a tested property**: byte-identical rebuilds and serializations.
- **Structural validation has one authority.** `reg-build verify` owns the published
  catalog's invariants; tests run it on the synthetic artifact, they do not re-derive
  its checks.
- **A bug fix adds no regression test.** The fix is verified once, by the run that
  shows it; a fixed bug is not a behavior. Add a case only when the fix establishes a
  product rule that meets the bar above, and then at most one, in the owning corpus.
- **Within budget.** Each package's suite stays inside its time budget in
  `ARCHITECTURE.md` → "Tiers". A check that costs more than its behavior is worth moves
  to a slower tier (real-seed or release gate) or goes.
- **Report what ran.** Name the checks you ran and the ones you did not; never claim an
  unrun check passed.

# Stack

- **Builder** (`crates/`, Rust): `reg-build` reads the pinned inputs into source frames,
  resolves each register with its curation and emits one SQLite file (DDL in
  `crates/reg-build/src/schema.sql`, `reg_core::CATALOG_SCHEMA_VERSION` gates
  compatibility, regenerate-not-migrate, no ORM).
- **Runtime** (`crates/`, Rust): reader, `project_data.json` validator, order
  materializer, HTTP API and MCP tools. There is no Python web backend.
- **Tests**: `cargo test` (crates), the `reg-conformance` harness (conformance), its
  `--test release` target for real-artifact checks. Build coverage is fully synthetic.
- **Tooling**: Rust (`crates/xtask`), shell (`scripts/gate.sh`) and bun for the SPA. The
  repository holds no Python; the xtask test `no_python` fails on any tracked `.py`
  file or Python project file.

# Run

`reg-meta serve --db <catalog-dir> --port 8000` serves the HTTP API and `/mcp`.
`reg_webapp/dev/dev.sh` runs it with the SPA (`reg_webapp/frontend/`) on free ports.

# Lint and test

`scripts/gate.sh` owns every check command and tool pin; CI calls its steps. There are
no git hooks: CI is the gate, so run the relevant step before pushing.

- `bash scripts/gate.sh all` — the full gate before a PR (`g0`, `rust`, `flows`,
  `frontend`). The R packages are not gated: their testthat suites run only by hand
  (`Rscript -e 'testthat::test_dir("<pkg>/tests/testthat")'`).
- `bash scripts/gate.sh lint` — panache (markdown) and shellcheck. The repo-policy
  lints (provider vocabulary, skill reads and aliases, test size, no Python) and the
  plugin schema drift check are `crates/xtask` tests, run by `crates`.
- `bash scripts/gate.sh g0 rust` — lint and crates, plus the conformance run against
  the Rust server.
- `bash scripts/gate.sh regen` — regenerate rebase-sensitive generated files; after a
  rebase, commit the diff separately.
- Heavy runs (gate steps, real-seed builds, parallel suites) are not locked: the
  Operator schedules them with the maintainer.
- Narrow runs: `cargo test -p <crate>`.
- Real-artifact conformance:
  `REG_ARTIFACT_DIR=<dir> [REG_CATALOG=swecov] cargo test -p reg-conformance --test release`
  (release step 8f).
- Local prerequisites: `cargo install cargo-machete --locked`, panache at the
  `PANACHE_VERSION` in `scripts/gate.sh` (`cargo install panache --version <pin>
  --locked`), shellcheck and bun.
- A new worktree needs no setup step: the gate and `dev.sh` install the SPA's packages
  on demand.
- The LISA register markdown (`catalog/docs/markdown/lisa/*.md` in the private inputs
  repository) is a build artifact — fix that repository's `tools/parse_lisa_docs.py`,
  not the output.

# Issue tracker

GitHub Issues is the **idea archive, not a build queue**. Before filing, **search open
AND closed issues** (`gh issue list --state all --search "<keywords>"`) for an existing
match: extend or comment on it rather than opening a duplicate.

Dogfooding sessions (log findings, file issues at the end) follow the
[dogfood](.agents/skills/dogfood/SKILL.md) skill.

**Title** — mirror the commit convention: `<type>(<package>): <imperative summary>`
(e.g. `feat(reg_meta): …`, `fix(reg_webapp): …`).

**Labels** — exactly **one area label** — a package (`reg_meta` for the Rust runtime and
builder, `reg_webapp`) or `cross-package` — plus a **type**: `enhancement`, `bug`, or
`documentation`.

**Ingestion trust gate** — this repo is public, so automation reads issue/PR content
**only** through `cargo xtask gh-issue`, a fail-closed maintainer-author trust gate:
issues/comments not authored by the maintainer are dropped rather than surfaced to a
model. Raw `gh issue view` / `gh issue list` without `--search` (and
`gh api .../issues`, `gh search issues`) model-reads in skill files are forbidden,
enforced by `crates/xtask/tests/skill_gh_reads.rs`.

# Git

- Develop on an isolated branch or worktree. A PR needs green CI and an independent
  review (a fresh agent that did not write the change counts) before it merges.
  Coordinate concurrent work and keep main to a single write path.
- Renovate (`renovate.json`) opens the routine dependency PRs: tools, CI and dev deps
  nightly; the builder's runtime deps and Cargo weekly, plus a separate weekly
  lockfile-maintenance PR (the next curation real-seed build proves the weekly ones). A
  red Renovate PR is fixed locally by an Operator session with upgrade-deps.
- Dependency repair and manual, larger upgrades use
  [upgrade-deps](.agents/skills/upgrade-deps/SKILL.md), releases use
  [release](.agents/skills/release/SKILL.md), and real-seed verification uses
  [build-db](.agents/skills/build-db/SKILL.md).
