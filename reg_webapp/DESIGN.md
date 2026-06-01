# reg_webapp — design

FastAPI backend + Svelte SPA that serves the reg_meta catalog. See
`REFACTOR_SPEC.md` §9–§9.6 for the authoritative webapp design; this file
records the package-local rationale.

## Layout

```text
reg_webapp/
  backend/                # uv workspace member (own pyproject, src-layout)
    src/reg_webapp/        # FastAPI app, routes, models, stewards loader
    scripts/gen_openapi.py # deterministic OpenAPI dumper
    openapi.json           # committed snapshot (canonical API contract)
    tests/                 # pytest, manifest-only fixture DB
  frontend/               # Svelte 5 + Vite + TS SPA (bun-managed)
    src/lib/api-types.ts   # codegen'd from ../backend/openapi.json
  stewards/               # per-steward config (sibling of backend/frontend)
    global/steward.toml    # identity only; no catalog → full universe
  DESIGN.md
```

`stewards/` is a sibling of `backend/` and `frontend/` (REFACTOR_SPEC §9): a
steward config is deployment data, not backend source. The loader resolves it
relative to the module (`stewards.STEWARDS_DIR`) so it works regardless of
cwd.

## The A5.1a / A5.1b split

A5.1 is split. **A5.1a (this scaffold)** stands up the package, the boot seam,
and the full toolchain (OpenAPI snapshot + TS codegen + CI gates) against a
single endpoint, `GET /api/context`. **A5.1b** adds the catalog domain models
(reg_meta dataclass → Pydantic response wrappers, §9.6) and the
`/api/catalog` browse endpoints.

A5.1a deliberately ships **no `{fqid:path}` catch-all route** — that arrives in
A5.1b with its per-segment slug-grammar and path-traversal guards (§9.5, §16).
A boot test (`test_boot.py`) asserts no `:path` converter is registered, so the
catch-all can't slip in unguarded.

## Boot seam (the reg_meta read-only DB)

The FastAPI lifespan opens reg_meta read-only through reg_meta's **own**
helpers, never a hardcoded path:

```python
db_path = reg_meta.db.db_path_from_args(None)   # REG_META_DB > XDG > platform
conn = reg_meta.db.open_db(db_path)             # mode=ro + _check_schema_compat
```

`open_db` already opens `mode=ro` and runs `_check_schema_compat` — a real
`SCHEMA_VERSION` assert vs the DB manifest. That is the **load-bearing** schema
gate (a wrong major / too-old minor raises at startup; `test_boot.py` covers
it). A5.1a needs only the manifest snapshot, so the boot connection is closed
once it's read and the parsed manifest is stashed on `app.state`; the keys
`/api/context` surfaces are validated at boot so a malformed DB fails fast.
A5.1a holds **no** long-lived query connection — a single `sqlite3` connection
from the lifespan can't be queried from FastAPI's sync-handler threadpool, so
A5.1b opens the mmap'd query connection (and picks its threading model) when it
adds the catalog index + `/api/catalog`.

A5 reads reg_meta read-only and ships no DDL, so there is **no
`SCHEMA_VERSION` bump** from this stage.

## Steward layering (§9.1)

A steward is `stewards/<id>/steward.toml` (identity/branding) plus an optional
`steward.project_data.json` (the catalog filter). The **`global`** steward
ships only `steward.toml` — the *absence* of the project file means
full-universe mode (no filter, reg_meta's whole catalog). The loader
(`stewards.load_steward`) detects that absence via `has_catalog_filter`.
A5.1a ships only `global`; loading and validating a steward catalog is A5.1b.

## §9.6 Pydantic boundary

reg_webapp defines its **own** webapp-local Pydantic response models
(`models.py`) — NOT reg_meta dataclasses (which stay plain dataclasses on the
library surface for import lightness) and NOT reg_schema models (those are used
directly only for project_data-shaped responses, A5.1b+). `ContextResponse`
is the first such wrapper: it composes steward identity, the reg_meta manifest
block, and package versions. No git sha — provenance needs no new dependency.

## OpenAPI snapshot + TS codegen (the drift gate)

`openapi.json` is committed and is the canonical contract. `gen_openapi.py`
dumps `create_app().openapi()` with `sort_keys=True` + a trailing newline so
the snapshot is byte-stable across machines. `app.openapi()` builds without the
lifespan (no DB needed), so the dumper runs offline. The SPA codegens
`src/lib/api-types.ts` from the snapshot via `openapi-typescript`. Two checks
keep these in lockstep: `test_openapi_snapshot.py` (in the always-run `test`
job) asserts the committed `openapi.json` equals a fresh render of the app, and
the `reg-webapp-frontend` CI job regenerates `api-types.ts` from the committed
snapshot and fails on any diff — so app, snapshot, TS types, and the committed
tree must agree.

## Frontend toolchain

Svelte 5 + Vite + TypeScript, bun-managed. **Biome** (`>=2.3.0`) is the single
formatter/linter — no prettier/eslint. Biome's experimental Svelte support
formats/lints the JS/CSS/HTML parts of `.svelte` but does **not** yet parse
Svelte control-flow (`{#if}` / `{#each}`), so:

- `.svelte` formatting is imperfect (accepted for A5.1a).
- `noUnusedVariables` / `noUnusedImports` are disabled for `.svelte` in
  `biome.json` — Biome can't see template-bound usage of `<script>`
  declarations and false-fires. **`svelte-check`** (the `check` script) is the
  authoritative type/template gate and does see template usage.
- The codegen'd `src/lib/api-types.ts` is excluded from Biome entirely
  (codegen output, never hand-formatted).

## Why CI uses a fixture DB, not a real asset

reg_meta 5.1.0 is unpublished — there's no `reg_meta/v*` release asset, so
`reg-meta update` in CI would fail or fetch a stale incompatible DB. Instead
the backend tests build a tiny **manifest-only** fixture DB (just
`import_manifest` with `schema_version` + `import_date`) in a tmp dir and point
the app at it via the highest-precedence `REG_META_DB` override.
`GET /api/context` reads only the manifest, so no reg_meta_build DDL is needed.
The real DB at the default path is the **local** boot smoke the maintainer/
orchestrator runs.
