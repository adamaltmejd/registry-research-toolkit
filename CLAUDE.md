# Registry Research Toolkit

Multi-package workspace for Swedish register research: catalog metadata, schema
validation, and MONA bundle / mock-data generation. See `ARCHITECTURE.md` for the
cross-package design and `REFACTOR_SPEC.md` for the remaining (post-A5) work.

## Packages

- `reg_meta` (CLI `reg-meta`) — search and query registry metadata.
- `reg_meta_build` (CLI `reg-meta-build`) — build the reg_meta SQLite DBs from SCB
  exports (maintainer-only).
- `reg_schema` (library) — `project_data.json` schema and structural validator.
- `reg_monabundle` (library) — MONA bundle build + runtime + PII scanner.
- `reg_webapp` — FastAPI backend + Svelte SPA: catalog browse + project authoring.
- `mock_data_wizard` (CLI `mock-data-wizard`) — local mock-data generation; pending
  rename to `reg_mockdata` + reg_meta-dep removal (see `REFACTOR_SPEC.md`).

## MONA constraint

[MONA](https://www.scb.se/mona) is Statistics Sweden's microdata platform. Agents are
not allowed on MONA. **PII must never leave MONA — only aggregate statistics are
exported.** `mock_data_wizard` (post-refactor: `reg_monabundle` + `reg_mockdata`)
bridges agentic local work to MONA projects.

# Governance

- `DESIGN.md` per package documents design rationale and constraints.
- No frozen specs or **permanent** implementation trackers — design decisions live in
  DESIGN.md, implementation history lives in git.
- **Exception**: a multi-PR refactor spanning weeks may keep a single root-level tracker
  (currently `REFACTOR_SPEC.md`, scoping the remaining post-A5 work; the earlier
  `MIGRATION_PLAN.md` was retired once A5 shipped) for cross-PR coordination. The
  tracker is **scoped and self-deleting**: it ships with an explicit completion gate
  (e.g. "deleted when stage X ships"), gets deleted at that gate, and never outlives the
  refactor it tracks. Per-package DESIGN.md notes for the same effort are still
  preferred where the scope is package-local.

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

# Python conventions

- Runtime deps live in each package's `pyproject.toml`; dev deps live only in the
  workspace-root `pyproject.toml`.
- Add with `uv add` (runtime) / `uv add --dev` (dev) — don't hand-edit `pyproject.toml`
  for new deps; uv writes to the right PEP 735 group. Hand-editing is fine for bumping
  an existing constraint (then `uv lock`).
- One-off tools (not project deps): `uvx <tool>` — e.g.
  `uvx pre-commit run --all-files`.
- Refresh lockfile with `uv lock --upgrade`; CI uses `uv sync --frozen`.
- `requires-python` floor is bound to MONA's bundled Python — see
  `mock_data_wizard/DESIGN.md` "MONA Python runtime" before raising it.

## Stack

Current state (the Model A refactor through A5 has shipped). See `ARCHITECTURE.md` for
the cross-package invariants and each `<package>/DESIGN.md` for the detail;
`REFACTOR_SPEC.md` tracks the remaining work.

- **Library packages** (`reg_meta`, `reg_monabundle`, `reg_mockdata`, `reg_meta_build`):
  - Modeling: `@dataclass`. **No Pydantic on these library surfaces** — keeps them
    importable from any context (Jupyter, scripts, MONA bundle).
  - Database: stdlib `sqlite3` with raw SQL; DDL string in `db.py`; `SCHEMA_VERSION`
    constant gates compatibility; regenerate-not-migrate. **No SQLAlchemy/Alembic** — DB
    is read-mostly, single-backend; an ORM would add overhead with no benefit.
  - Analytical queries: DuckDB where needed.
  - CLI: argparse. No click/typer.
- **`reg_schema`** (authoring/validation surface — exception to the no-Pydantic rule):
  Pydantic v2. Reasons: (1) it's the canonical structural validator for
  `project_data.json` — Pydantic's declarative field/model validators are the right
  tool; (2) FastAPI in `reg_webapp/backend/` consumes `reg_schema` models directly as
  response models, killing the 1:1 wrapper drift surface; (3) `model_json_schema()`
  gives the SPA's TypeScript codegen a free, always-correct schema source. Runtime
  escape valve: the MONA bundle does **not** ship Pydantic; bundle-build runs the
  Pydantic validator as the gate, then converts validated `Source` → dataclass
  `LoadedSpec` (`reg_monabundle.runtime.spec`) which the bundle amalgamates instead. See
  `reg_schema/DESIGN.md` and `reg_monabundle/DESIGN.md` for the boundary.
- **Web backend** (`reg_webapp/backend/`): FastAPI + Pydantic REST. `reg_schema`
  Pydantic models are response models directly (no wrapper layer). For `reg_meta`
  (dataclass-based) responses, the backend defines per-endpoint Pydantic response
  wrappers — the only place 1:1 wrappers remain.
- **Web frontend** (`reg_webapp/frontend/`): Svelte 5 + Vite + TypeScript, bun-managed.
  TS types codegen'd from FastAPI's `openapi.json`.
- **Tests**: pytest + pytest-xdist; `@pytest.mark.integration` opts into
  Docker-requiring tests. Build/parse coverage is fully synthetic (no gitignored real
  SCB/SOS data) and runs the full structural validator
  (`validate_built_db(corpus=False)` — every invariant except the real-corpus volume
  gate). Real-corpus drift is surfaced by a maintainer's actual `build-db`, which
  validates by default with `corpus=True` (opt out with `--no-validate`).
- **Type checking**: `uvx ty check` (Astral, beta). Blocking in CI; runs latest via
  `uvx` so we don't chase version bumps. Not a dev dep — keep `pyproject.toml` clean.
- **MONA bundle runtime deps are expensive**: `reg_monabundle.runtime.*` amalgamates
  into a single file uploaded to MONA. Each added runtime dep must already be in MONA's
  WinPython env (see `mock_data_wizard/DESIGN.md`). Prefer stdlib for runner-bound code.

`mock_data_wizard`'s old local authoring path (editor/server/`ui` subcommand and the
`web/` SPA) is fully deleted, superseded by `reg_webapp`; `classify.py` was **moved** to
`reg_monabundle/runtime/classify.py` (not deleted — it backs the bundle's runtime
classification). Don't revive that path — extend the new packages.

# Run (dev servers)

- `reg_webapp` local dev (FastAPI :8000 + Vite :5173): the `/run-reg-webapp` skill
  (`reg_webapp/.claude/skills/run-reg-webapp/`) has the verified launch steps + a
  Playwright driver for smoke/screenshots; `.claude/launch.json` registers both servers
  for `preview_start` (names `reg-webapp-backend` / `reg-webapp-frontend`).

# Lint and test

- `uv run ruff check` — python lint
- `uv run ruff format --check` — python format check
- `uvx --from panache-cli==2.51.0 panache format --check .` — markdown format check
  (config in `.panache.toml`; drop `--check` to fix)
- `uvx --from panache-cli==2.51.0 panache lint --check .` — markdown lint
- `uv run python -m pytest` — all tests (pytest discovers per-package via root pyproject
  `testpaths`)
- `uv run python -m pytest reg_meta/` — narrow to a single package
- `reg_meta_build/docs/lisa/*.md` are build artifacts — fix
  `scripts/parse_lisa_docs.py`, not the output

# Git

- Never run `git commit --no-verify`, `git commit -n`, or `git push --no-verify`. If a
  pre-commit hook fails, fix the underlying issue rather than bypassing.

## PR merge gate

Green CI + one review is not sufficient to merge. Every PR must clear:

- **Real-data validation** when build-pipeline or DB content changed: run
  `reg-meta-build build-db --validate` against the real seed at
  `reg_meta_build/input_data/` (untracked; main checkout only), not just fixture tests.
- **Independent findings-only review** of the diff; fix every finding. Return findings
  to the maintainer — never post them as PR comments.
- **Bot reviews**: Codex + Copilot auto-review open PRs — address their inline comments,
  waiting \~10 min after open/push for reviews to land. Codex's clean verdict is a 👍
  reaction on the PR body from `chatgpt-codex-connector[bot]` with no review submitted;
  reactions are invisible to `gh pr view` — check
  `gh api repos/<owner>/<repo>/issues/<pr>/reactions`.
- **After merging**, verify the merge commit's tree matches the branch tip — the GitHub
  API can capture a stale head and silently drop just-pushed commits.

# Layout

For per-package design rationale, see `<package>/DESIGN.md` (the reg_meta object model
lives in `reg_meta/DESIGN.md`; per-provider source-delivery shapes in
`reg_meta_build/DESIGN.md`). For the cross-package design (topology, dependency graph,
repo-wide invariants), see `ARCHITECTURE.md`; for the remaining post-A5 work, see
`REFACTOR_SPEC.md`.
