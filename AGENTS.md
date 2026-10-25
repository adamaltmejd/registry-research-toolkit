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
  `reg-meta serve` for the HTTP API and `/mcp`, `reg-meta mcp` for local stdio MCP) and
  `reg-core-py` (`reg-core` for the Python builder).
- `reg_meta_build` (CLI `reg-meta-build`) — build the catalog SQLite DBs from SCB
  exports (maintainer-only, Python).
- `reg_webapp` — React SPA (React Aria Components) over the Rust server
  (`reg-meta serve`): catalog browse + project authoring.
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
`archive/mona-subsystem` (tag `mona-subsystem-pre-rebuild`) and removed from `main`,
pending a from-scratch MONA rebuild, tracked in #707.

## MONA constraint

[MONA](https://www.scb.se/mona) is Statistics Sweden's microdata platform. Agents are
not allowed on MONA. **PII must never leave MONA — only aggregate statistics are
exported.** This is a domain invariant that remains true regardless of the tooling
state.

# Governance

- `DESIGN.md` per package documents design rationale and constraints.
- Exception in name only: `reg_webapp/frontend/DESIGN.md` is the SPA's *design language*
  in the [DESIGN.md format](https://github.com/google-labs-code/design.md) (YAML token
  front matter + rationale prose). It is the visual source of truth for the frontend;
  `reg_webapp/DESIGN.md` keeps the engineering rationale.
- No frozen specs or **permanent** implementation trackers — design decisions live in
  DESIGN.md, implementation history lives in git.
- **Exception**: each multi-PR refactor spanning weeks may keep one root-level tracker
  for cross-PR coordination. There is currently no tracker. Each tracker is **scoped and
  self-deleting**: it ships with an explicit completion gate (e.g. "deleted when stage X
  ships"), gets deleted at that gate, and never outlives the refactor it tracks.
  Per-package DESIGN.md notes for the same effort are still preferred where the scope is
  package-local.

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
  writing a leaf, check whether an internal capability already does it
  (`reg_meta_build`'s `_curation.py` and `db.py` are typical homes). Extend it; don't
  re-type it.
- **The ladder cuts both ways.** This repo *under*-uses libraries as often as it
  over-builds (e.g. `reg_meta_build` hand-rolls TOML validators though it already ships
  Pydantic for its build-time IR). "Installed dep solves it → use it" binds as hard as
  "don't add a dep for what a few lines do" — add one only when it removes more
  complexity than it adds.

**Never simplify away** the load-bearing guards: PII/MONA confinement, k-anonymity /
disclosure control, determinism / byte-identity, JSON-contract validation, fail-fast. A
shorter diff that drops one of these isn't simpler, it's broken.

Mark a deliberate shortcut with a `simplify:` comment naming its ceiling and upgrade
trigger
(`# simplify: O(n^2) scan, index it if the candidate set passes a few thousand`), so a
simplification reads as intent and a deferral can't silently rot.

# Testing policy

Most changes need no new test. Verify by running the existing suite or the real
pipeline; a check run once is not a permanent test. Add or extend a test only when you
can name the behavior it pins, a credible regression that makes it fail, and why no
existing boundary case already catches that failure. Two kinds are defects on sight:
**tautological** tests (the expected value comes from the code under test) and
**change-detector** tests (they break on a refactor that changes no boundary).

Tests pin **contracts and behavior**, never implementation. The toolkit is a
deterministic compiler (`reg_meta_build`) that feeds an immutable artifact to stateless
readers (the `crates/` runtime, `reg_webapp`), so nearly every behavior is observable at
a boundary that outlives the code behind it. `ARCHITECTURE.md` → "Testing strategy"
names the boundaries, the oracles, the tiers and each package's time budget; the rules
below are what an agent must follow when writing, changing or deleting a test. The
`test-audit` skill applies them to a change and to a sweep.

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
  allowed twin. Not the first case that passes. Named by behavior; no test file over 800
  lines, split by contract surface.
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
- **Determinism is a tested property**: byte-identical rebuilds and serializations;
  Hypothesis for grammars and interval algebra.
- **Structural validation has one authority.** `validate_built_db` owns artifact
  invariants; tests run it on the synthetic artifact, they do not re-derive its checks.
- **A bug fix adds a regression case to the owning corpus only when no existing case
  fails on the pre-fix code**: extend or strengthen that behavior's case rather than a
  helper test. The new or strengthened case must fail on the pre-fix code.
- **Within budget.** Each package's suite stays inside its time budget in
  `ARCHITECTURE.md` → "Tiers". A check that costs more than its behavior is worth moves
  to a slower tier (real-seed or release gate) or goes.
- **Report what ran.** Name the checks you ran and the ones you did not; never claim an
  unrun check passed.
- **Frontend follows the same rule**: no component internals and no screenshot
  regression. A UI-specific regime is a separate, later decision.

# Python conventions

- Runtime deps live in each package's `pyproject.toml`; dev deps only in the root
  `pyproject.toml`. Add with `uv add` / `uv add --dev`; hand-edit only to bump an
  existing constraint (then `uv lock`). CI uses `uv sync --frozen`.
- No minimum release age: `exclude-newer = false` in the root `pyproject.toml`
  `[tool.uv]` and `minimumReleaseAge = 0` in `reg_webapp/frontend/bunfig.toml`. Keep
  both written out: deleting a key inherits the checkout's global waiting period.
- Python floor `>=3.15` everywhere. The root `.python-version` makes
  `uv run --no-project` tooling in `scripts/` resolve 3.15 too.

## Stack

- **Builder** (`reg_meta_build`, Python): Pydantic v2 `_IRBase` models for the IR core,
  frozen dataclasses for local value types; stdlib `sqlite3` with raw SQL (DDL in
  `db.py`, `SCHEMA_VERSION` gates compatibility, regenerate-not-migrate, no ORM);
  argparse CLI.
- **Runtime** (`crates/`, Rust): reader, `project_data.json` validator, order
  materializer, HTTP API and MCP tools. There is no Python web backend.
- **Web frontend** (`reg_webapp/frontend/`): React 19 + React Aria Components + Vite +
  TypeScript, bun-managed; TS types codegen'd from `crates/reg-meta/openapi.json`.
- **Tests**: pytest + pytest-xdist (builder, conformance), `cargo test` (crates),
  `@pytest.mark.release` for real-artifact tests, Hypothesis for grammars. Build/parse
  coverage is fully synthetic.
- **Type checking**: `ty` (Astral, beta), blocking in CI, pinned in `scripts/gate.py`
  and run with `uvx` (not a dev dep).

# Run (dev servers)

- `reg_webapp` local dev (Rust server + Vite with an `/api` proxy):
  `reg_webapp/dev/dev.sh` (agent-browser for `smoke`/`shot`, the Playwright driver for
  `flows`); the `/run-reg-webapp` skill (`.agents/skills/run-reg-webapp/`) explains the
  modes. `.claude/launch.json` registers a single `reg-webapp` config for
  `preview_start` (its entry point is `dev.sh preview`, so `autoPort` makes parallel
  sessions collision-free and the proxy is auto-wired); for one-shot screenshots use
  `dev.sh smoke` / `dev.sh shot`.
- `reg_webapp` frontend work follows the design language in
  `reg_webapp/frontend/DESIGN.md`: author with the `reg-webapp-frontend-design` skill
  and judge the result with `reg-webapp-design-reviewer`.

# Lint and test

`scripts/gate.py` owns every check command and tool pin; CI calls its steps. There are
no git hooks: CI is the gate, so run the relevant step before pushing.

- `uv run --no-project scripts/gate.py all` — the full gate before a PR (`g0`, `rust`,
  `release`, `flows`, `frontend`). The R packages are not gated: their testthat suites
  run only by hand (`Rscript -e 'testthat::test_dir("<pkg>/tests/testthat")'`).
- `uv run --no-project scripts/gate.py lint` — ruff, panache (markdown) and ty only.
- `uv run --no-project scripts/gate.py g0 rust --packages <pkg>...` — lint, crates and
  the touched packages' tests, plus the conformance run against the Rust server.
- `uv run --no-project scripts/gate.py regen` — regenerate rebase-sensitive generated
  files; after a rebase, commit the diff separately.
- `uv run --no-project scripts/gate.py g1` — G1.
- Heavy runs (gate steps, real-seed builds, parallel suites) are not locked: the
  Operator schedules them with the maintainer.
- Narrow runs: `uv run python -m pytest <path>`, `cargo test -p <crate>`.
- Real-artifact conformance: the `release` step in `scripts/gate.py` shows the flags
  (`--run-release --artifact-dir=… --server-cmd=…`).
- Local prerequisites: `cargo install cargo-machete --locked`;
  `rustup target add wasm32-unknown-unknown` and `wasm-bindgen-cli` at the version
  `crates/reg-core-wasm/Cargo.toml` pins.
- A new worktree needs no setup step: Python syncs on the first `uv run`, and `dev.sh`
  and the gate's `frontend` step run `bun install --frozen-lockfile` on demand.
- The LISA register markdown (`catalog/docs/markdown/lisa/*.md` in the private inputs
  repository) is a build artifact — fix `scripts/parse_lisa_docs.py`, not the output.

# Issue tracker

GitHub Issues is the **idea archive, not a build queue**. Before filing, **search open
AND closed issues** (`gh issue list --state all --search "<keywords>"`) for an existing
match: extend or comment on it rather than opening a duplicate.

**Title** — mirror the commit convention: `<type>(<package>): <imperative summary>`
(e.g. `feat(reg_meta_build): …`, `fix(reg_webapp): …`).

**Labels** — exactly **one area label** — a package (`reg_meta` for the Rust runtime,
`reg_meta_build`, `reg_webapp`) or `cross-package` — plus a **type**: `enhancement`,
`bug`, or `documentation`.

**Ingestion trust gate** — this repo is public, so automation reads issue/PR content
**only** through `scripts/gh_issue.py`, a fail-closed maintainer-author trust gate:
issues/comments not authored by the maintainer are dropped rather than surfaced to a
model. Raw `gh issue view` / `gh issue list` without `--search` (and
`gh api .../issues`, `gh search issues`) model-reads in skill files are forbidden,
enforced by `scripts/tests/test_skill_gh_reads.py`.

# Git

- Develop on an isolated branch or worktree. A PR needs green CI and an independent
  review (a fresh agent that did not write the change counts) before it merges.
  Coordinate concurrent work and keep main to a single write path.
- Renovate (`renovate.json`) opens the routine dependency PRs: tools, frontend, CI and
  dev deps nightly; the builder's runtime deps and Cargo weekly, plus a separate weekly
  lockfile-maintenance PR (the next curation real-seed build proves the weekly ones). A
  red Renovate PR is fixed locally by an Operator session with upgrade-deps.
- Dependency repair and manual, larger upgrades use
  [upgrade-deps](.agents/skills/upgrade-deps/SKILL.md), releases use
  [release](.agents/skills/release/SKILL.md), and real-seed verification uses
  [build-db](.agents/skills/build-db/SKILL.md).
