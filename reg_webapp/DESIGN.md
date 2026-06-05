# reg_webapp — design

FastAPI backend + Svelte SPA. The backend serves the reg_meta catalog read-only
and the project-authoring write surface (validate / order / bundle); the SPA is
the researcher's authoring client. This file records the package-local design
rationale. Cross-cutting topology (package tree, dependency graph, perf/bundle
budgets, version policy, testing-strategy overview) lives in the root
`ARCHITECTURE.md`; remaining/unbuilt work lives in `REFACTOR_SPEC.md`. The API
contract itself is the committed `backend/openapi.json` (the reference);
`models.py` + the route handlers are the response-shape reference.

## Why no auth — cost protection instead

The data is public-ish registry metadata; there is **no server-side user-private
state** (project files live in the browser, never on the server). "Auth" here is
really cost protection, on two axes: read GETs are edge-cacheable + ETag-
revalidated (cheap), and the actual-work POST endpoints carry an origin-side
body-size cap + per-IP rate limit. Real auth is a v2+ concern, layered on only if
a steward ever needs private data.

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

`stewards/` is a sibling of `backend/` and `frontend/`: a steward config is
deployment data, not backend source. The loader resolves it
relative to the module (`stewards.STEWARDS_DIR`) so it works regardless of
cwd.

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
**no** long-lived query connection — see the connection model below. The boot
also loads the steward and builds its in-memory catalog index (below), stashing
both on `app.state`.

The webapp reads reg_meta read-only and ships no DDL, so it owns no
`SCHEMA_VERSION` — the only schema gate is `open_db`'s boot compat check against
reg_meta's manifest.

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
isolation and reusable by the suffixed routes (`/states` etc.). It runs
**before** any `Catalog` call — a malformed/traversal-shaped path returns **422
with zero SQL executed** (pinned by a trace-hook test that counts statements
== 0).

- Each `/`-split segment is validated by **delegating** to
  `reg_meta.fqid.validate_slug` (no second copy of the slug regex — single
  source of truth). The only literal admitted beyond the slug grammar is `class`
  (§5.2's classification-root sentinel), and only at the **leading** position;
  in any other slot `class` 422s like any reserved token. `_default` is **never**
  a catalog path segment (variants are a register sub-resource, not an
  `/api/catalog/{fqid}` segment), so it is rejected too.
- Starlette URL-decodes the path before the handler, so `%2e%2e` / `%2f` / `%00`
  arrive decoded and fail the per-segment check like any other non-slug char.
  (A raw `..` is collapsed by HTTP clients before it reaches the server, so the
  raw-dotdot reject is exercised at the unit layer; the app layer uses the
  percent-encoded forms.)
- **No `@version` carve-out.** A binding leaf is a bare slug — the `@version`
  value-set-version pin is **retired** (the value set is determined by the
  resolved `(variable, variant, period)`, not pinned on the FQID), so `@` is just
  a non-slug character that 422s like any other. Browse narrowing to one vintage
  is the read-only `?value_set_version` query (below), not a path grammar.
- The classification-root literal `class` (1 seg) is a reserved slug that
  `validate_slug` rejects, so the handler special-cases it **before** `parse` →
  lists all classifications (via `reg_meta.queries.list_classifications`, no new
  Catalog method).

## Catalog router structure

Catalog routes live in one `routes/catalog.py` APIRouter, declaring `/catalog`,
then the suffixed routes, then `/catalog/{fqid:path}` (the catch-all **last**).
Starlette matches in **declaration order** and the `{fqid:path}` converter
greedy-consumes any suffix, so the suffixed routes must declare ABOVE the
catch-all or the catch-all swallows the suffix into `fqid` and the suffix handler
never fires. A CI router-introspection test
(`test_boot.py::test_suffixed_routes_declared_before_catch_all`) pins the order.
The suffix tokens (and `variants`) are also **reserved in the variable slot** of
the slug grammar (§5.2) at build time, so a variable slugged `states` can't
shadow a sub-endpoint. The validate→parse→Catalog-dispatch→Pydantic-map flow is
factored into reusable helpers.

The suffixed surface is six **binding-suffix routes** (`/states`,
`/predecessors`, `/successors`, `/related`, `/lineage`, `/lineage_warnings`),
each mapping 1:1 to a `Catalog` accessor and returning a thin `{binding, <list>}`
envelope so the SPA codegen sees one response type per endpoint; plus one
**register sub-resource** `/{provider}/{register}/variants` (a FIXED 3-seg shape
with a literal `variants` tail — explicit `{provider}`/`{register}` segments, NOT
an `{fqid:path}` suffix). The suffixed routes are binding-only: a non-binding
FQID raises reg_meta's `not_a_binding_fqid` (EXIT_USAGE) → **422** (a usage
error, not a 500); an absent binding → 404. A register node's children include a
`variants` reference (`VariantsRef`) so the variant browser has a stable slot in
the discriminated union without the variant being an FQID.

**The `?period` query** on the catch-all. On a binding leaf, `?period=...`
returns `{binding, states: [...]}` — the `resolve_at` subset, **uniform with
`/states`** (so codegen sees one state-list type). `?variant` narrows to one
variant; `?value_set_version` narrows to one vintage (a read-only browse filter
matched against `value_set_version_label` by `resolve_at`, **not** a path pin).
The period query is **ignored** on non-binding kinds (the register / provider /
classification node resolves normally). An absent `?period` still returns the
FULL embedded leaf.

**`/lineage` shape.** Maps what reg_meta's `LineageEdge` carries
(`consumer_state_id`, `source_state_id`, the validity intersection,
`source_fqid`). A richer per-source-state shape (embedding each source state's
variant / value_set / column) is a possible reg_meta enhancement — when
`LineageEdge` grows those fields, the wrapper and `LineageResponse` widen; the
endpoint contract (`lineage_edges`) is stable.

### The §16 query allow-list (`period_param.py`)

The second §16 chokepoint alongside `catalog_fqid.validate_fqid_path`. A thin
**syntactic** allow-list parsing `?period` / `?variant` / `?value_set_version`
into the polymorphic `reg_meta.catalog.Period` type **before any reg_meta lookup**
— a malformed value (SQLi probe, traversal, NUL, percent-encoded slash) returns
**422 with zero SQL AND zero connection opens** (wired as a pre-open `Depends`;
reg_meta's `resolve_at` / `_period_bounds` is the SEMANTIC backstop). Single
source of truth: the grammar is `reg_meta.fqid.is_period` / `validate_slug` —
not re-encoded here. FastAPI-free so it's unit-testable in isolation.

- **Period wire format**: int year (`2020` → `int`), period token
  (`HT2020` / `2020-Q3` / `2020-08` / `2018-12-31` → `str`), range
  (`<from>..<to>`, literal `..` → `{"from","to"}` dict), `_default` sentinel. A
  bare year maps to `int` (the documented year arm); every other token to `str`.
- **`?variant` ADMITS `_default`** (a real `register_variant` slug, §5.1) UNLIKE
  the path guard (which rejects `_default` because it's not a path segment).
  `?value_set_version` is the `value_set_version_label` grammar and does NOT admit
  `_default`; the `_none` sentinel selects the empty-label vintage (the empty
  string can't ride in a query without being indistinguishable from absent).

The connection model is the **LOCKED P1 guard**: every DB-backed route opens its
sqlite connection INSIDE the sync handler body via `with _catalog_conn(request)
as conn:` — NEVER a FastAPI generator `Depends` (which is entered on a different
threadpool thread → cross-thread `ProgrammingError`). Each DB-backed route gets
its OWN `ThreadPoolExecutor` concurrency smoke (the `TestClient` sequential
default masks the bug).

## ETag / Cache-Control (`etag.py` + `middleware.py`)

Every read endpoint (`/api/context`, the `/api/catalog` root + catch-all, the 7
sub-endpoints) carries `ETag: "<reg_meta_version>-<steward_id>-<sha256(body)[:16]>"`
and `Cache-Control: public, max-age=86400, must-revalidate`; a matching
`If-None-Match` yields a **304** with no body. The pure logic lives in `etag.py`
(`compute_etag` + `etag_matches`); an ASGI middleware (`ETagMiddleware`) wires it
DRY onto every GET read response.

- **`reg_meta_version`** is the INSTALLED `reg_meta.__version__` (the v1.x Model A
  package release), NOT the DB `schema_version` manifest. `steward_id` is
  `app.state.steward.id`.
- **The body-hash** makes `If-None-Match` per-URL coherent — the `?period` /
  `?variant` query is part of the URL, so it's already part of the cache key
  (different periods are different ETags).
- **Middleware skips WRITE endpoints** via a method gate: only `GET` reads are
  stamped, so the POST endpoints pass through with no ETag. It also skips non-200
  responses — an error body isn't a cacheable representation, and handing the
  client a validator for a transient error would be wrong.
- We unit-test only the ETag / Cache-Control LOGIC + the 304 behavior. The
  **edge** side (Cloudflare edge caching / DDoS shielding / edge rate-limits) is a
  deploy/maintainer concern and not backend code. Remaining: edge config — see
  `REFACTOR_SPEC.md`.

## Steward layering and the in-memory catalog index (`stewards.py` + `catalog_index.py`)

A steward is `stewards/<id>/steward.toml` (identity/branding, required) plus an
optional `steward.project_data.json` (the catalog filter). The **`global`**
steward ships only `steward.toml` — the *absence* of the project file means
full-universe mode (no filter, reg_meta's whole catalog). The loader
(`stewards.load_steward`) detects that absence via `has_catalog_filter`.

**Why reuse `project_data.json` as the catalog schema?** A steward catalog is
structurally identical to a researcher's project (same `reg_schema` validator) —
many `sources`, no `panels` — so the FQIDs on its columns *are* the catalog, with
no separate catalog schema to maintain. The webapp can validate both a project
and a catalog with the same structural + semantic validators, so consistency
comes for free.

The in-memory **`CatalogIndex`** is built once at boot (`load_catalog_index`,
with the boot connection) and held on `app.state` for the process lifetime. It is
the filter that scopes a steward deployment to a subset of reg_meta's universe.
It is an internal frozen `@dataclass` (never a response body — only response
models are Pydantic; webapp internals are dataclasses), carrying two maps derived
directly from the steward project's `sources[]`:

- `bindings_by_variant` — `register_variant` coordinate → frozenset of admitted
  binding FQIDs. Keys on the bare 3-segment binding FQID directly (no `@version`
  pin to normalize away — that grammar is retired).
- `period_range_by_register` — register FQID → best-effort `(lo, hi)` period span
  for UI hinting **only**, NOT a validity gate (the semantic validator's
  per-binding `period_outside_state_validity` is the gate; mixed period grammars
  don't sort cleanly as strings).

The `global` deployment (`has_catalog_filter=False`) has **no** index (`None`);
the catalog endpoints pass through to reg_meta's full universe.

**Steward-load drift downgrade.** Loading a steward catalog runs the same
`validate_semantic` (below) in **steward-caller** mode. A reg_meta-drift
resolution failure (`fqid_unresolved` / `value_set_missing` /
`period_outside_state_validity` / `binding_representation_unknown`) is downgraded
error → warning so the deployment **boots through** reg_meta evolving out from
under a steward's committed catalog: the affected bindings are DROPPED from the
index (unauthorable until the steward updates) and the warnings ride on
`/api/context` so the SPA can show a "catalog drift" banner. Because the
downgrade keeps `result.ok` True, the loader keys on the **warnings list**, not
`.ok`. A *structural* break in the committed catalog (malformed JSON, an
unexpected/typo'd field that survives structural but fails model construction) is
**not** drift — it's a misconfigured deployment, so it fails fast
(`StewardCatalogError`). A residual *semantic* error that survives the drift
downgrades (e.g. a still-ambiguous `binding_value_set_version_ambiguous`, which
stays an error because it's an author-time choice, not drift) also fails the boot
— don't admit a broken binding to the index and never surface it.

Adding a steward is a monorepo PR (drop a directory, register the hostname,
rebuild). `REG_WEBAPP_STEWARD` selects which steward a process serves;
`REG_WEBAPP_STEWARDS_DIR` overrides the on-disk root for a packaged
wheel/Docker image (the `stewards/` sibling doesn't exist there). Remaining: the
SPA catalog-authoring mode, a `reg-meta-build steward-diff` CLI, and per-steward
`extensions` — see `REFACTOR_SPEC.md`.

## Pydantic boundary

reg_webapp defines its **own** webapp-local Pydantic response models
(`models.py`) for the reg_meta dataclass surface — reg_meta stays plain
dataclasses on the library surface (import lightness), so the webapp wraps them
1:1. This is the **only** place a 1:1 Pydantic wrapper remains. For
`project_data`-related responses (`/api/project/*`) the webapp uses **`reg_schema`
Pydantic models directly** — no wrapper layer, eliminating that drift surface.
(The cross-package bundle boundary — validated `Source` → dataclass `LoadedSpec`,
which is where the Pydantic side hands off to the dataclass bundle runtime —
lives in `reg_monabundle/DESIGN.md`.)

The catalog response models are 1:1 wrappers of reg_meta's frozen `Catalog`
dataclasses. Each node model carries a `kind` `Literal` discriminator
(`provider` / `register` / `binding` / `classification` / `classification-root`
/ `root` / `variants-ref`); the catch-all returns a Pydantic discriminated union
(`Field(discriminator="kind")`) so `openapi-typescript` emits a clean tagged
union. FQID fields serialize as plain `str` (`str(fqid)`), never nested models,
so the codegen'd TS sees flat string fields. The binding **leaf** embeds the
variable's FULL longitudinal record from one `Catalog.resolve` call (states,
value sets, and the variable-grain `same_as` / `replaced_by` / `related_to` /
`lineage` edges). `lineage_warnings` are **omitted** — `ResolvedVariable` doesn't
carry them; they arrive via the `/lineage_warnings` endpoint.

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

- `.svelte` formatting is imperfect (an accepted tradeoff).
- `noUnusedVariables` / `noUnusedImports` are disabled for `.svelte` in
  `biome.json` — Biome can't see template-bound usage of `<script>`
  declarations and false-fires. **`svelte-check`** (the `check` script) is the
  authoritative type/template gate and does see template usage.
- The codegen'd `src/lib/api-types.ts` is excluded from Biome entirely
  (codegen output, never hand-formatted).

## SPA routing + production fallback

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
  This mirrors the "edge config is a maintainer task" pattern (ETag section
  above); see the comment atop `router.svelte.ts`.

The fetch wrapper (`src/lib/api.ts`) types every response off
`components["schemas"][...]` from the codegen'd `api-types.ts`, so the SPA and the
backend contract can't drift. The catch-all returns the `kind`-discriminated
`CatalogNode` union; components narrow on `kind` via `src/lib/catalog.ts` helpers
(unit-tested).

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

CI has no published `reg_meta/v*` DB release asset to pull, so `reg-meta update`
in CI would fail or fetch a stale incompatible DB. Instead the backend tests
build fixture DBs in a tmp dir and point the app at them via the
highest-precedence `REG_META_DB` override:

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

## Project-write surface (`routes/project.py` + `routes/bundle.py`)

Three POST endpoints: `/api/project/validate`, `/api/project/order`,
`/api/bundle`. All read the body as a **raw JSON dict** (not a typed param):
`/validate` must accept malformed specs to diagnose them, and `/bundle` must
preserve steward-namespaced blocks (`swecov` / `reg_mockdata`) that a typed
`extra="ignore"` body would silently drop. The raw body is documented in OpenAPI
as an open object (`additionalProperties: true`) so the SPA codegen sees a body
to send.

- **`/validate` status discipline.** A spec that FAILS validation is a
  *successful validation response* — **HTTP 200 with `ok=false` + the issues**.
  4xx is reserved for a malformed REQUEST (non-JSON, duplicate JSON keys, a
  too-deeply-nested or non-object body, an oversized body). It runs the §6.8.0
  three-layer composition (structural → namespaced-block → semantic) and returns
  the **concatenated** issue list; the DB-free layers run first, so a
  structurally-rejected body costs no DB hit. It also runs the build-side
  cross-block referential checks (orphan `column_options` keys /
  suppress_k-on-non-categorical), closing that half of the old
  `/validate`↔`/bundle` divergence — the one residual gap is `/bundle`'s step-4
  capability gates (e.g. a build-required `display_name`), which `/validate`
  defaults from reg_meta, so a `/validate`-clean spec can still 422 at `/bundle`.
- **`/order`** renders the steward's default order-export CSV (a `text/csv`
  download) and is the one documented exception to the "every route declares a
  `response_model`" lint (it returns raw bytes). Unlike `/validate`, it
  structurally **gates** first: you cannot render a provider order from an invalid
  spec → 422.
- **`/bundle`** embeds the raw dict into a single-file `.py` MONA bundle (a pure
  function of input → content-hash cacheable, no ETag). It is DB-free. A
  build-gate raise (bad input) → 422; a malformed body → 400.

**Connection model = per-request open ON ONE THREAD** (the locked cross-thread
guard). `/validate` and `/order` are `async` only to read the body off the wire;
the blocking work (structural parse + per-binding sqlite resolution) is offloaded
via `run_in_threadpool`, and the reg_meta connection opens on **that** worker
thread inside a `with`-block — NEVER a generator `Depends` (which can run on a
different AnyIO thread → `sqlite3.ProgrammingError`). `/bundle` is DB-free.

The order CSV cell values are passed through a **spreadsheet formula-injection**
guard (`_csv_safe`): a researcher-controlled `display_name` like
`=HYPERLINK(...)` would otherwise execute as a formula when the data provider
opens the manifest. A leading formula-trigger char (`=+-@\t\r`) is prefixed with
a single quote.

## Semantic validation (`semantic.py`)

The §6.8.3 reg_meta-backed validation layer. It lives in the webapp — NOT
`reg_schema` — because `reg_schema` is reg_meta-free by design (MONA-amalgamatable);
semantic rules need the live DB, so they belong where the DB is. `reg_schema`
lists these codes as defined-but-not-emitted on its own surface; this is their
home. The webapp invokes it (with `reg_schema`'s structural validator and the
owning packages' block validators) — `reg_schema` itself never imports the owning
packages. It emits the same frozen `reg_schema.ValidationIssue` shape the other
layers do, so composition is plain tuple concatenation. It takes a `Catalog` and
never opens a connection (the caller owns the connection's lifetime).

Rules, walking each source's `register_variant` + every binding:

- The `register_variant` coordinate resolves to a known variant; the binding
  `variable` (3-segment FQID) resolves to a known variable (following `same_as`
  links — `Catalog.resolve` does that). Unresolved → `fqid_unresolved` (error).
- The binding resolves to a covering `variable_state` at the source's variant AND
  period. None → `period_outside_state_validity` (error). A range / `_default`
  period crossing a state transition (sequential, non-overlapping states) →
  `binding_state_drifts_within_period` (info).
- The binding's `value_set` (a `class/<slug>` FQID) resolves to a known
  classification → else `value_set_missing` (error).

**Representation, not `@version`.** A FQID names one concept, but a concept may
carry several **co-existing delivery columns** at the same instant — parallel
representations (SSYK 3/4/5-digit, age brackets). When ≥2 distinct delivery
columns co-exist (overlapping validity windows) and the binding sets no
`representation`, the extract would pull more than one column →
`binding_value_set_version_ambiguous` (error); the author must pick one via
`Binding.representation` (the delivery column name; the SPA offers a chooser).
This is exactly the job the retired `@version` pin used to do, now keyed on the
delivery column. A `representation` reg_meta no longer delivers as a column →
`binding_representation_unknown` (error). Crucially, the co-existence test keys on
**overlapping** windows: distinct columns in *non*-overlapping windows are a
sequential rename (drift), NOT ambiguity, and must not demand a `representation`.
A separate defensive backstop (`binding_value_set_version_ambiguous` on ≥2
distinct `value_set_id`s on **one** column) should be unreachable against a clean
catalog — the reg_meta build enforces one value set per
`(variable, variant, period, delivery_column)`.

**Caller context — researcher vs steward.** The `caller` flag drives the level
mapping, NOT a different rule set. The **researcher** path
(`POST /api/project/validate`) keeps unresolved-FQID-class codes as blocking
**errors** (fix before extract). The **steward-catalog load** path (boot)
**downgrades** `fqid_unresolved`, `value_set_missing`,
`period_outside_state_validity`, and `binding_representation_unknown` from error
→ warning, so the deployment boots through reg_meta drift (those bindings drop
from the index; see the steward section). `binding_value_set_version_ambiguous`
deliberately stays strict on both paths — it's an author-time choice, not drift.

**Onboarding.** Stewards declare a subset of what reg_meta knows; data without an
FQID can't be authored (no `{display_name + type, no FQID}` escape hatch in v1).
New variables/registers/classifications onboard via slug-TOML PRs against
`reg_meta_build`; once the next reg_meta release lands, the steward adds them to
their catalog.

**Steward catalog filtering — `fqid_outside_steward_catalog`.** When a
researcher's project references an FQID outside the loaded steward catalog, the
intent is to emit `fqid_outside_steward_catalog` at level **warning** (not error):
this is also the deliberate "what would my project look like under steward X?"
feature — load a spec against another steward's deployment and the warnings
enumerate exactly which columns would be unavailable; the SPA offers a one-click
"drop out-of-scope columns" remediation. The `global` deployment never emits it
(no filter). The membership probe (`CatalogIndex.admits`) is built and
unit-tested. **Gap: it is not yet wired into `/api/project/validate`** —
`_semantic_issues` runs `validate_semantic` + the cross-block checks but never
consults the `CatalogIndex`, so the code is never emitted from a request today.
Remaining: wire `admits` into `/validate` — see `REFACTOR_SPEC.md`.

## Cost protection (`limits.py`)

Two stdlib-only ASGI middlewares (no `slowapi` dep) gate ONLY the write methods
(POST); read GETs flow through untouched (they have the cheaper edge-cache +
ETag axis). These are **origin-side** guards — Cloudflare fronts production with
the same budgets at the edge (remaining — see `REFACTOR_SPEC.md`); these catch
direct origin hits that bypass the edge.

- **`BodySizeLimitMiddleware`** — a **streaming** byte-count guard that 413s a
  body exceeding `MAX_BODY_BYTES` (1 MB). It counts bytes as they arrive rather
  than trusting `Content-Length` (absent on chunked transfers, and spoofable), so
  an oversized chunked/under-declared body is still caught even if the handler
  never reads it. 1 MB matches the bundle-output budget and is far above any
  plausible `project_data.json`.
- **`RateLimitMiddleware`** — an in-memory per-IP token bucket
  (`request.client.host`, ~`RATE_LIMIT_PER_MINUTE` req/min/IP → 429). **IP-only**
  by design: a session token would bucket per-browser (helpful behind NAT) but
  adds a fingerprinting surface for anonymous public data — layer it in only if a
  steward needs it. A missing client host buckets under one shared key (fail
  closed). Buckets are per-process (lost on restart, not shared across replicas) —
  sufficient as the origin backstop behind the edge limiter; a shared store
  (Redis) is a scale-out concern, not v1.

## Browser storage + project-file persistence (the SPA store)

Project files live in the **browser** during a session and as JSON in the user's
git repo for durability. There is **no server-side storage** — git is the durable
store, email/git-sharing handles collaboration (server-side projects are a
possible v2 feature). The authoring store (`project_store.svelte.ts`) is a
module-singleton Svelte 5 rune store holding one draft per session.

- **Autosave to IndexedDB** (`indexeddb_persistence.ts`) over the raw IndexedDB
  API (no `idb` dep — keeps the frontend dep surface lean) via a debounced
  (~500ms) `$effect`. **Graceful degradation is mandatory**: in private mode /
  disabled storage / quota failures, `save` resolves and `load` resolves `null`
  so the app keeps working in-memory — autosave NEVER rejects or crashes the
  effect.
- **Store-schema stamping + gate.** Each persisted draft is stamped with the
  store's own `storeSchemaVersion` (distinct from the project's `schema_version`);
  `load` restores only on a match, else discards the stale-schema draft. This is
  the store's record shape, bumped only when the persisted shape changes.
- **Project-file version gate.** Model A files carry `schema_version` MAJOR **2**
  (reg_schema `2.0.0`) and `reg_meta_version` of the form `reg_meta/v1.x.y`. The
  SPA **hard-rejects** a file whose `schema_version` major is **1** OR whose
  `reg_meta_version` is `reg_meta/v0.x` (pre-Model-A) with a blocking open-error —
  no migration, pre-v1 policy. (`schema_version` major 1 is the *rejected*
  pre-Model-A value, not Model A.) Any other version is a neutral no-op: the
  backend stays the canonical validator.
- **Unsaved-changes warning.** A `dirty` flag derives from the draft diverging
  from the last DOWNLOAD baseline (`lastDownloaded`); a `beforeunload` listener
  prompts on a tab/window close with a dirty draft. The store drives the write
  endpoints (validate / order / bundle download) through `lib/api.ts`; it is NOT
  a structural validator (the backend is canonical).

Note: v1 is **one draft per SPA session** (a single IndexedDB key), not a
multi-project list — opening a file replaces the current draft.

## API surface

The committed `backend/openapi.json` is the canonical contract; this table is the
orientation map. All endpoints are under `/api/`; read GETs are edge-cacheable,
write POSTs are not. Catalog browse paths use FQID segments directly.

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/context` | Deployment identity, branding, build info, catalog-drift warnings. |
| GET | `/api/catalog` | Top-level: every provider the steward exposes + the `class` root. |
| GET | `/api/catalog/{fqid}` | Single endpoint for every hierarchy node (`kind`-discriminated). On a binding leaf, embeds the variable's full longitudinal record. Optional `?period` / `?variant` / `?value_set_version` narrow a binding leaf to a `{binding, states}` subset (uniform with `/states`). |
| GET | `/api/catalog/{provider}/{register}/variants` | The register's variant browser. |
| GET | `/api/catalog/{fqid}/states` | Full state history for a binding. |
| GET | `/api/catalog/{fqid}/predecessors` | Inbound `variable_replaced_by` edges. |
| GET | `/api/catalog/{fqid}/successors` | Outbound `variable_replaced_by` edges. |
| GET | `/api/catalog/{fqid}/related` | `variable_related_to` edges (sibling-grain variables). |
| GET | `/api/catalog/{fqid}/lineage` | Materialized `variable_state_lineage` edges (consumer ← source). |
| GET | `/api/catalog/{fqid}/lineage_warnings` | Linker-emitted lineage coverage warnings. |
| POST | `/api/project/validate` | Three-layer validation; 200 + `ok` + issues. |
| POST | `/api/project/order` | Default order-export CSV download. |
| POST | `/api/bundle` | Build the MONA `.py` bundle from a spec. |

**Order-export CSV columns** (the v1 default; fixed order is the contract):
`provider,register,variant,variable,representation,period,display_name` — one row
per spec binding. `representation` is its OWN column (not folded into
`display_name`): a custom display name would otherwise hide which delivery column
the binding pinned, so the data provider couldn't tell representations apart.
`period` serializes via the catalog `?period` wire form (range →
`"<from>..<to>"`, snapshot → `"_default"`). Pluggable per-steward `order_template`s are remaining — see
`REFACTOR_SPEC.md`.

Remaining (not yet routes): `POST /api/kit` (kit-build — see `REFACTOR_SPEC.md`),
`/api/catalog-search` (FTS), and `/api/docs/*`.

## §16 input-validation gates (security boundary)

Two chokepoints reject hostile input **before** any DB lookup, each pinned by a
parametrized test asserting 422 **and zero SQL executed** (a SQLite trace hook
counts statements == 0):

- **`?period` canonicalization** (`period_param.py`) — the raw query is parsed
  into a typed `Period` against an allow-list of the canonical period forms
  before any reg_meta lookup. SQLi probes / traversal / NUL / URL-encoded slashes
  aren't period tokens, so they 422 and never touch SQL.
- **FQID route-segment validation** (`catalog_fqid.py`) — each `{fqid:path}`
  segment must match the slug grammar (or the leading `class` literal). The
  grammar excludes `.`, `..`, `%`, `\`, and any non-structural `/`, so canonical
  FQIDs cannot encode path traversal; Starlette URL-decodes first, so `%2e%2e` /
  `%2f` / `%00` fail the per-segment check. **`@version` is a 422, not a pin:**
  `scb/lisa/naringsgren@sni2007` is now an explicit *negative* case (the pin is
  retired), alongside `scb/lisa/naringsgren@bad/slug` and `…@@x`.

**Provenance confinement (route introspection).** No handler references the
provenance DB path — the route surface never exposes provenance, so there is no
path-confinement to enforce at the handler level. This is a property of the
endpoint set, re-checked when routes are added.

## Forward-looking open UX notes

These are unresolved UX questions, not built behavior — recorded so they aren't
re-discovered. The underlying data-layer lineage rationale lives in
`reg_meta` / `reg_meta_build` (the `variable_state_lineage` interval-overlap
edges); these are purely the authoring-UI presentation.

- **LISA composite-source presentation.** ~64% of LISA's variable slugs are
  sourced from RTB/RAMS/FastPak/IoT and carry inbound lineage edges. How the
  catalog UI surfaces that origin when a user authors a LISA variable list — hover
  tooltip, inline note, "see also" panel — is undecided. The data is present; the
  question is purely UX.
- **Per-steward repo autonomy.** v1 hosts every steward's config in this monorepo.
  Stewards versioning their own catalogs in their own repos (if IFAU/SWECOV ever
  run their own deployments) would reintroduce external-repo build wiring v1
  sheds — not needed until a steward asks.
- **Realign-patch lifecycle** (gated behind the unbuilt merged-mode realign
  flow). Whether the realign-review UI writes an accepted patch back into git
  automatically or just produces a new `project_data.json` the user replaces
  manually.
