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

## A5.2a-ii — catalog-READ sub-endpoints + the `?period` query

A5.2a-ii fills the A5.1b-ii ordering seam with the 7 read sub-resources and adds
the `?period` query to the catch-all. (The project-WRITE endpoints —
`/api/project/*`, `/bundle`, `/kit` — are A5.2b; `/api/catalog-search` (FTS) is
deferred — noted below.)

- **The 6 binding-suffix routes** (`/states`, `/predecessors`, `/successors`,
  `/related`, `/lineage`, `/lineage_warnings`) plus the **register sub-resource**
  `/{provider}/{register}/variants` are ALL declared ABOVE the `{fqid:path}`
  catch-all (Starlette matches in declaration order; the catch-all
  greedy-consumes any suffix). A CI introspection test
  (`test_boot.py::test_suffixed_routes_declared_before_catch_all`, §9.5
  `routes_declared_before`) pins the order. The `variants` route is a FIXED 3-seg
  shape with a literal `variants` tail — explicit `{provider}`/`{register}`
  segments, NOT an `{fqid:path}` suffix.
- **Each suffixed route maps 1:1 to a `Catalog` accessor** and returns a thin
  `{binding, <list>}` envelope (or `{register, variants}`) so the SPA codegen
  sees one response type per endpoint. The suffixed routes are binding-only: a
  non-binding FQID raises reg_meta's `not_a_binding_fqid` (EXIT_USAGE) → **422**
  (a usage error, not a 500); an absent binding → 404.
- **`?period` on the catch-all.** On a binding leaf, `?period=...` returns
  `{binding, states: [...]}` — the `resolve_at` subset, **uniform with `/states`**
  (so codegen sees one state-list type). `?variant` narrows to one variant;
  `?value_set_version` narrows to one vintage. The period query is **ignored** on
  non-binding kinds (the register/provider/classification node resolves
  normally). An absent `?period` still returns the FULL embedded leaf.
- **`/lineage` shape.** Maps what reg_meta's `LineageEdge` carries
  (`consumer_state_id`, `source_state_id`, the validity intersection,
  `source_fqid`). The §9.5 *richer* per-source-state shape (embedding each source
  state's variant / value_set / column) is a possible reg_meta enhancement — NOT
  blocked on here. When reg_meta's `LineageEdge` grows those fields, the wrapper
  and `LineageResponse` widen; the endpoint contract (`lineage_edges`) is stable.

### The §16 query allow-list (`period_param.py`)

The second §16 chokepoint alongside `catalog_fqid.validate_fqid_path`. A thin
**syntactic** allow-list parsing `?period` / `?variant` / `?value_set_version`
into the polymorphic `reg_meta.catalog.Period` type **before any reg_meta lookup**
— a malformed value (SQLi probe, traversal, NUL, percent-encoded slash) returns
**422 with zero SQL AND zero connection opens** (wired as a pre-open `Depends`;
reg_meta's `resolve_at` / `_period_bounds` is the SEMANTIC backstop). Single
source of truth: the grammar is `reg_meta.fqid.is_period` / `validate_slug` —
not re-encoded here. FastAPI-free so it's unit-testable in isolation.

- **Period wire format** (§9.5): int year (`2020` → `int`), period token
  (`HT2020` / `2020-Q3` / `2020-08` / `2018-12-31` → `str`), range
  (`<from>..<to>`, literal `..` → `{"from","to"}` dict), `_default` sentinel. A
  bare year maps to `int` (the documented year arm); every other token to `str`.
- **`?variant` ADMITS `_default`** (a real `register_variant` slug, §5.1; 108 in
  the real DB) UNLIKE the path guard (which rejects `_default` because it's not a
  path segment). `?value_set_version` is the classification-slug /
  `value_set_version_label` grammar (the slug grammar) and does NOT admit
  `_default`.
- **`@version` vs `?value_set_version`.** The binding-leaf `@version` pin
  (parsed into `ValidatedFqidPath.value_set_version` by the path guard) and the
  `?value_set_version` query reconcile: both-present-and-equal or only-one uses
  it; both-present-and-DIFFERENT is **422** (ambiguous), regardless of `?period`.
  The reconciliation runs before the connection opens, so a conflict costs no SQL.

The connection model is UNCHANGED from A5.1b-ii (the LOCKED P1 guard): every new
DB-backed route opens its sqlite connection INSIDE the sync handler body via
`with _catalog_conn(request) as conn:` — NEVER a FastAPI generator `Depends`
(which is entered on a different threadpool thread → cross-thread
`ProgrammingError`). Each new DB-backed route gets its OWN `ThreadPoolExecutor`
concurrency smoke (the `TestClient` sequential default masks the bug).

## §9.4 ETag / Cache-Control (`etag.py` + `middleware.py`)

Every read endpoint (`/api/context`, the `/api/catalog` root + catch-all, the 7
sub-endpoints) carries `ETag: "<reg_meta_version>-<steward_id>-<sha256(body)[:16]>"`
and `Cache-Control: public, max-age=86400, must-revalidate`; a matching
`If-None-Match` yields a **304** with no body. The pure logic lives in `etag.py`
(`compute_etag` + `etag_matches`); an ASGI middleware (`ETagMiddleware`) wires it
DRY onto every GET read response.

- **`reg_meta_version`** is the INSTALLED `reg_meta.__version__` (the v1.x Model A
  package release), NOT the DB `schema_version` manifest (§9.5). `steward_id` is
  `app.state.steward.id`.
- **The body-hash** makes `If-None-Match` per-URL coherent — the `?period` /
  `?variant` query is part of the URL, so it's already part of the cache key
  (different periods are different ETags).
- **Middleware skips WRITE endpoints** via a method gate: only `GET`/`HEAD` reads
  are stamped, so A5.2b's POST endpoints pass through with no ETag (§9.4). It also
  skips non-200 responses — an error body isn't a cacheable representation.
- The **Cloudflare edge-cache round-trip** (§9.4) is a MAINTAINER task — we
  unit-test only the ETag/Cache-Control LOGIC + the 304 behavior, not the edge.

### Deferred from A5.2a-ii

`/api/catalog-search` (FTS over registers/variables, §9.5) is **deferred** — it's
a separate path delegating to reg_meta's FTS5 indexes, orthogonal to the
resolve/edge read surface this stage ships. The §9.2 in-memory index, steward
project-file load, rate limits, and the body-size cap remain A5.2b.

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

## SPA routing + production fallback (A5.3a)

The SPA (`frontend/`) browses the catalog read-only with **path-based routing**:
clean URLs mirror the API (`/catalog`, `/catalog/scb/lisa`,
`/catalog/scb/lisa/kon`, `/catalog/class/<slug>`). The router is hand-rolled —
no routing-library dep — in `src/lib/router.svelte.ts` (a `.svelte.ts` module so
its reactive `$state` route compiles): it reads `window.location.pathname`,
navigates via `history.pushState`, handles `popstate`, and intercepts internal
`<a>` clicks (the `link` action) so navigation doesn't full-reload.

- **Dev** serving Just Works: the Vite dev server's default `appType: 'spa'`
  rewrites unknown paths to `index.html`, and `vite.config.ts` proxies `/api` to
  the backend on `:8000`. Deep-linking to `/catalog/...` in `bun run dev` works.
- **Production** SPA fallback is a **deploy/maintainer task**, NOT backend code.
  The backend is a pure JSON API — `create_app` mounts no `StaticFiles` and
  serves no `index.html` (keeping `/api`, `/openapi.json`, `/docs` un-shadowed).
  The SPA is served by the edge (Cloudflare), which must rewrite a cold-load deep
  link to any non-`/api` path → `index.html` (a `_redirects` / 404-rewrite rule).
  This mirrors the existing "Cloudflare edge-cache gate is a maintainer task"
  pattern (§9.4 above); see the comment atop `router.svelte.ts`.

`gen:types` typing: A5.3a's fetch wrapper (`src/lib/api.ts`) types every response
off `components["schemas"][...]` from the codegen'd `api-types.ts`. The catch-all
returns the `kind`-discriminated `CatalogNode` union; A5.3a never sends `?period`,
so the `StatesResponse` arm (the period/states picker — A5.3b) is out of scope.
Components narrow on `kind` via `src/lib/catalog.ts` helpers (unit-tested).

## Frontend unit tests (Vitest)

`bun run test` runs **Vitest** (`vitest run`) — Vite-native, so it reuses
`vite.config.ts` and compiles `.svelte` / `.svelte.ts`. The env is `jsdom`
(`router.svelte.ts` reads `window` at module load; `api.ts` mocks `fetch`).
Tests live next to source as `*.test.ts` and cover the fetch-wrapper error path,
the `kind`-narrowing helpers, and route parsing. The `reg-webapp-frontend` CI job
runs `bun run test` alongside `svelte-check` + the codegen drift check. (Use
`bun run test`, not `bun test` — the latter is Bun's own runner, which doesn't
compile Svelte.)

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
