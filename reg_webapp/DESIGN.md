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

A5.1 is split. **A5.1a (the scaffold)** stands up the package, the boot seam,
and the full toolchain (OpenAPI snapshot + TS codegen + CI gates) against a
single endpoint, `GET /api/context`. **A5.1b** adds the catalog domain models
(reg_meta dataclass → Pydantic response wrappers, §9.6) and the `/api/catalog`
browse endpoints; it is itself split into **A5.1b-i** (the reg_meta listing API
— `Catalog.list_providers` / `list_registers` / `list_bindings`) and
**A5.1b-ii** (this webapp catalog-browse half — the `/api/catalog` root + the
`{fqid:path}` catch-all + its §16 guard).

A5.1a deliberately shipped **no `{fqid:path}` catch-all route**. **A5.1b-ii owns
it**: `/api/catalog/{fqid:path}` lands here, behind the §16 per-segment guard
(below). The A5.1a boot test that asserted *no* `:path` converter is INVERTED in
A5.1b-ii — it now asserts the catalog catch-all IS present and declared LAST, and
that the §16 guard runs before any DB access.

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
it). The boot connection is closed once the manifest is read; the parsed manifest
AND the resolved `db_path` are stashed on `app.state` (the keys `/api/context`
surfaces are validated at boot so a malformed DB fails fast). The lifespan holds
**no** long-lived query connection — see the connection model below.

A5 reads reg_meta read-only and ships no DDL, so there is **no
`SCHEMA_VERSION` bump** from this stage.

## Catalog connection model (per-request open)

The catalog routes (`routes/catalog.py`) open a **fresh read-only connection
per request** from the boot-resolved `app.state.db_path`, via a FastAPI
dependency (`_catalog`) that `yield`s a `Catalog` and `close()`s the connection
in a `finally`. This is a deliberate decision, not an oversight:

- A single shared `sqlite3` connection is **not** concurrency-safe across
  FastAPI's sync-handler threadpool, even with `check_same_thread=False` —
  per-connection cursor state races between threads. So no long-lived shared
  connection, no lock, and **not** `check_same_thread=False`.
- The per-request connection is owned by the handling thread (`sqlite3`'s
  default `check_same_thread=True`), which is correct: one thread, one
  connection, opened and closed within the request.
- `open_db(db_path, check_schema=False)` skips the schema-compat re-check — the
  lifespan already ran it at boot, so re-checking per request is wasted work,
  not safety. (The mmap'd read-only open is cheap; reg_meta's DB is read-mostly
  and single-backend.)

## §16 FQID path guard (`catalog_fqid.py`)

The `{fqid:path}` catch-all is guarded by a single chokepoint,
`validate_fqid_path(raw_path)`, in its own module so it's unit-testable in
isolation and reusable by A5.2's suffixed routes. It runs **before** any
`Catalog` call — a malformed/traversal-shaped path returns **422 with zero SQL
executed** (pinned by a trace-hook test that counts statements == 0).

- Each `/`-split segment is validated by **delegating** to
  `reg_meta.fqid.validate_slug` (no second copy of the slug regex — single
  source of truth) plus the `class` / `_default` literals (§5.2).
- Starlette URL-decodes the path before the handler, so `%2e%2e` / `%2f` / `%00`
  arrive decoded and fail the per-segment check like any other non-slug char.
  (A raw `..` is collapsed by HTTP clients before it reaches the server, so the
  raw-dotdot reject is exercised at the unit layer; the app layer uses the
  percent-encoded forms.)
- **One carve-out:** a binding **leaf** of the form `slug@version` is split on
  the single `@` and each half validated separately. `@` is the only non-slug
  char admitted, only as the single leaf delimiter — a second `@`, or `@`
  anywhere but a 3-seg leaf, is a 422. In A5.1b-ii `@version` is
  **validated-but-not-narrowing**: the bare 3-seg FQID is handed to
  `parse`/`resolve` and the embedded leaf is the full history; the narrowing is
  A5.2's `?value_set_version`.
- The classification-root literal `class` (1 seg) is a reserved slug that
  `validate_slug` rejects, so the handler special-cases it **before** `parse` →
  lists all classifications (via `reg_meta.queries.list_classifications`, no new
  Catalog method).

## Catalog router structure (the A5.2 seam)

Catalog routes live in one `routes/catalog.py` APIRouter, declaring `/catalog`
then `/catalog/{fqid:path}` (the catch-all **last**). Starlette matches in
declaration order and the `{fqid:path}` converter greedy-consumes any suffix, so
A5.2's suffixed routes (`/states`, `/predecessors`, ...,
`/{provider}/{register}/variants`) must declare ABOVE the catch-all; an
ordering-comment seam marks the line. The router-ordering CI introspection test
is deferred to A5.2 (no suffixed routes exist yet). The validate→parse→Catalog
dispatch→Pydantic-map flow is factored into reusable helpers.

A register node's children include a `variants` **reference stub**
(`VariantsRef`, `available: false`) — a declared slot, not data — so A5.2's
variant browser has a stable place in the discriminated union before the
`/{provider}/{register}/variants` sub-resource exists.

## Deferred: the §9.2 in-memory catalog index

A5.1b-ii does **not** build the §9.2 in-memory index. The index is built from a
steward `project_data.json`, which the `global` deployment lacks, and its
consumers are A5.2's validate/authoring surface. Browse resolves/lists directly
against the DB per request (`Catalog.resolve` / `list_*`).

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
library surface for import lightness) and NOT reg_schema models (those enter
with `/api/project/*` in A5.2). `ContextResponse` is the first such wrapper.

The catalog response models (A5.1b-ii) are 1:1 wrappers of reg_meta's frozen
`Catalog` dataclasses. Each node model carries a `kind` `Literal` discriminator
(`provider` / `register` / `binding` / `classification` / `classification-root`
/ `root` / `variants-ref`); the catch-all returns a Pydantic discriminated union
(`Field(discriminator="kind")`) so `openapi-typescript` emits a clean tagged
union for A5.3. FQID fields serialize as plain `str` (`str(fqid)`), never nested
models, so the codegen'd TS sees flat string fields. The binding **leaf** embeds
the variable's FULL longitudinal record from one `Catalog.resolve` call (states,
value sets, and the variable-grain `same_as` / `replaced_by` / `related_to` /
`lineage` edges). `lineage_warnings` are **omitted** — `ResolvedVariable`
doesn't carry them; they arrive via A5.2's `/lineage_warnings` endpoint.

One gotcha: a `register` field on a `pydantic.BaseModel` shadows
`BaseModel.register` (a method) and warns. The edge-ref models name the Python
attribute `register_name` and `Field(alias="register")` it, so the wire/JSON key
(and OpenAPI schema property) stays `register` while the warning is gone — the
alias is also the canonical init param the mappers construct with.

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
the backend tests build fixture DBs in a tmp dir and point the app at them via
the highest-precedence `REG_META_DB` override:

- **`/api/context`** reads only `import_manifest`, so its fixture
  (`compatible_db` / `mismatched_db`) is just that one table.
- **`/api/catalog`** resolves/lists against the full reg_meta schema, so the
  `catalog_db` fixture builds a **slugged** DB via `reg_meta_build`'s
  `_slugged_db` helper (a `scb/lisa/kon` binding with a state + value set, a
  second `scb/rams` register, a `same_as` edge, and a `class/sun2020`
  classification), then stamps an `import_manifest` so the boot compat check
  passes. The backend `conftest.py` mirrors `reg_meta/tests/conftest.py`'s
  sys.path injection to import that bare-name helper.

The real DB at the default path is the **local** boot smoke the maintainer/
orchestrator runs.
