# reg_webapp — design

FastAPI backend + Svelte SPA. The backend serves the reg_meta catalog read-only and the
project-authoring write surface (validate / order / bundle); the SPA is the researcher's
authoring client. This file records the package-local design rationale. Cross-cutting
topology (package tree, dependency graph, perf/bundle budgets, version policy,
testing-strategy overview) lives in the root `ARCHITECTURE.md`; remaining/unbuilt work
lives in `REFACTOR_SPEC.md`. The API contract itself is the committed
`backend/openapi.json` (the reference); `models.py` + the route handlers are the
response-shape reference.

## Why no auth — cost protection instead

The data is public-ish registry metadata; there is **no server-side user-private state**
(project files live in the browser, never on the server). "Auth" here is really cost
protection, on two axes: read GETs are edge-cacheable + ETag- revalidated (cheap), and
the actual-work POST endpoints carry an origin-side body-size cap + per-IP rate limit.
Real auth is a v2+ concern, layered on only if a steward ever needs private data.

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

`stewards/` is a sibling of `backend/` and `frontend/`: a steward config is deployment
data, not backend source. The loader resolves it relative to the module
(`stewards.STEWARDS_DIR`) so it works regardless of cwd.

## Boot seam (the reg_meta read-only DB)

The FastAPI lifespan opens reg_meta read-only through reg_meta's **own** helpers, never
a hardcoded path:

```python
db_path = reg_meta.db.db_path_from_args(None)   # REG_META_DB > XDG > platform
conn = reg_meta.db.open_db(db_path)             # mode=ro + _check_schema_compat
```

`open_db` already opens `mode=ro` and runs `_check_schema_compat` — a real
`SCHEMA_VERSION` assert vs the DB manifest. That is the **load-bearing** schema gate (a
wrong major / too-old minor raises at startup; `test_boot.py` covers it). The boot
connection is closed once the manifest is read; the parsed manifest AND the resolved
`db_path` are stashed on `app.state` (the keys `/api/context` surfaces are validated at
boot so a malformed DB fails fast). The lifespan holds **no** long-lived query
connection — see the connection model below. The boot also loads the steward and builds
its in-memory catalog index (below), stashing both on `app.state`.

The webapp reads reg_meta read-only and ships no DDL, so it owns no `SCHEMA_VERSION` —
the only schema gate is `open_db`'s boot compat check against reg_meta's manifest.

## Catalog connection model (per-request open)

The catalog routes (`routes/catalog.py`) open a **fresh read-only connection per
request** from the boot-resolved `app.state.db_path`, via the `_catalog_conn`
contextmanager used as a plain `with` block inside the sync handler body (NOT a FastAPI
`Depends`). It `yield`s a `sqlite3.Connection` that the handler wraps in a `Catalog`,
and `close()`s it in a `finally`. This is a deliberate decision, not an oversight:

- A single shared `sqlite3` connection is **not** concurrency-safe across FastAPI's
  sync-handler threadpool, even with `check_same_thread=False` — per-connection cursor
  state races between threads. So no long-lived shared connection, no lock, and **not**
  `check_same_thread=False`.
- The per-request connection is owned by the handling thread (`sqlite3`'s default
  `check_same_thread=True`), which is correct: one thread, one connection, opened and
  closed within the request.
- `open_db(db_path, check_schema=False)` skips the schema-compat re-check — the lifespan
  already ran it at boot, so re-checking per request is wasted work, not safety. (The
  read-only open is cheap; reg_meta's DB is read-mostly and single-backend.)

## §16 FQID path guard (`catalog_fqid.py`)

The `{fqid:path}` catch-all is guarded by a single chokepoint,
`validate_fqid_path(raw_path)`, in its own module so it's unit-testable in isolation and
reusable by the suffixed routes (`/states` etc.). It runs **before** any `Catalog` call
— a malformed/traversal-shaped path returns **422 with zero SQL executed** (pinned by a
trace-hook test that counts statements == 0).

- Each `/`-split segment is validated by **delegating** to `reg_meta.fqid.validate_slug`
  (no second copy of the slug regex — single source of truth). The only literal admitted
  beyond the slug grammar is `class` (the classification-root sentinel; see
  reg_meta/DESIGN.md → FQID grammar), and only at the **leading** position; in any other
  slot `class` 422s like any reserved token. `_default` is **never** a catalog path
  segment (variants are a register sub-resource, not an `/api/catalog/{fqid}` segment),
  so it is rejected too.
- Starlette URL-decodes the path before the handler, so `%2e%2e` / `%2f` / `%00` arrive
  decoded and fail the per-segment check like any other non-slug char. (A raw `..` is
  collapsed by HTTP clients before it reaches the server, so the raw-dotdot reject is
  exercised at the unit layer; the app layer uses the percent-encoded forms.)
- **No `@version` carve-out.** A binding leaf is a bare slug — the `@version`
  value-set-version pin is **retired** (the value set is determined by the resolved
  `(variable, variant, period)`, not pinned on the FQID), so `@` is just a non-slug
  character that 422s like any other. Browse narrowing to one vintage is the read-only
  `?value_set_version` query (below), not a path grammar.
- The classification-root literal `class` (1 seg) is a reserved slug that
  `validate_slug` rejects, so the handler special-cases it **before** `parse` → lists
  all classifications (via `reg_meta.queries.list_classifications`, no new Catalog
  method).

## Catalog router structure

Catalog routes live in one `routes/catalog.py` APIRouter, declaring `/catalog`, then the
suffixed routes, then `/catalog/{fqid:path}` (the catch-all **last**). Starlette matches
in **declaration order** and the `{fqid:path}` converter greedy-consumes any suffix, so
the suffixed routes must declare ABOVE the catch-all or the catch-all swallows the
suffix into `fqid` and the suffix handler never fires. A CI router-introspection test
(`test_boot.py::test_suffixed_routes_declared_before_catch_all`) pins the order. The
suffix tokens (and `variants`) are also **reserved in the variable slot** of the slug
grammar (see reg_meta/DESIGN.md → FQID grammar) at build time, so a variable slugged
`states` can't shadow a sub-endpoint. The validate→parse→Catalog-dispatch→Pydantic-map
flow is factored into reusable helpers.

The suffixed surface is six **binding-suffix routes** (`/states`, `/predecessors`,
`/successors`, `/related`, `/lineage`, `/lineage_warnings`), each mapping 1:1 to a
`Catalog` accessor and returning a thin `{binding, <list>}` envelope so the SPA codegen
sees one response type per endpoint; plus one **register sub-resource**
`/{provider}/{register}/variants` (a FIXED 3-seg shape with a literal `variants` tail —
explicit `{provider}`/`{register}` segments, NOT an `{fqid:path}` suffix). The suffixed
routes are binding-only: a non-binding FQID raises reg_meta's `not_a_binding_fqid`
(EXIT_USAGE) → **422** (a usage error, not a 500); an absent binding → 404. A register
node's children include a `variants` reference (`VariantsRef`) so the variant browser
has a stable slot in the discriminated union without the variant being an FQID.

**The `?period` query** on the catch-all. On a binding leaf, `?period=...` returns
`{binding, states: [...]}` — the `resolve_at` subset, **uniform with `/states`** (so
codegen sees one state-list type). The **#307 comma list form**
(`?period=2005..2010,2015..2020`, an interrupted series — #340) resolves **per
segment**, returning the compound-key-deduped union — keyed on
`(state_id, delivery_column_name, valid_from)` since a merged monthly-family variable
(#319) expands one annual state into 12 same-`state_id` per-month windows (keying on
`state_id` alone would collapse 11 of them): `parse_period_query` splits the wire into
segments and the handler calls `resolve_at` once per segment — `resolve_at` never sees
the list form (keeps the list grammar out of the separately-released reg_meta, mirroring
`semantic.py`'s per-segment iteration). `?variant` narrows to one variant;
`?value_set_version` narrows to one vintage (a read-only browse filter matched against
`value_set_version_label` by `resolve_at`, **not** a path pin). The period query is
**ignored** on non-binding kinds (the register / provider / classification node resolves
normally). An absent `?period` still returns the FULL embedded leaf.

**Concept groups (#303).** The register and classification-root responses carry a
`groups` list (`ConceptGroupModel`, mapped 1:1 from reg_meta's `ConceptGroupSummary` —
see reg_meta/DESIGN.md → Concept groups) ALONGSIDE the complete flat `children` list:
grouped members appear in both, so the contract stays additive and group-unaware
consumers keep working. The SPA folds client-side (`catalog.ts::foldGroupedRows`):
grouped leaves hide under one expandable `ConceptGroupRow` (a month×rank value matrix
for two facet axes, chips for one — months/vintages — and a plain member list for edge
groups), ungrouped leaves render as before, and the type-to-filter matches a group on
its label/key OR any member's name/FQID (`groupMatchesFilter`) so member searches still
surface the folded group. The CatalogPicker's variable list folds the same way (#322):
`ConceptGroupRow` takes an optional `onpick` that renders members as pick buttons
instead of catalogHref links, and the picker's `rankFilter` ranks a group row on
`groupFilterKeys` (the shared match set behind `groupMatchesFilter`); a picked member
rides the same derive-on-pick path as a leaf row. `foldGroupedRows` tolerates a stale
pre-`groups` edge-cached payload (#317) by degrading to the flat list.

**`/lineage` shape.** Maps what reg_meta's `LineageEdge` carries (`consumer_state_id`,
`source_state_id`, the validity intersection, `source_fqid`). A richer per-source-state
shape (embedding each source state's variant / value_set / column) is a possible
reg_meta enhancement — when `LineageEdge` grows those fields, the wrapper and
`LineageResponse` widen; the endpoint contract (`lineage_edges`) is stable.

### The §16 query allow-list (`period_param.py`)

The second §16 chokepoint alongside `catalog_fqid.validate_fqid_path`. A thin
**syntactic** allow-list parsing `?period` / `?variant` / `?value_set_version` into the
polymorphic `reg_meta.catalog.Period` type **before any reg_meta lookup** — a malformed
value (SQLi probe, traversal, NUL, percent-encoded slash) returns **422 with zero SQL
AND zero connection opens** (wired as a pre-open `Depends`; reg_meta's `resolve_at` /
`_period_bounds` is the SEMANTIC backstop). Single source of truth: the grammar is
`reg_meta.fqid.is_period` / `validate_slug` — not re-encoded here. FastAPI-free so it's
unit-testable in isolation.

- **Period wire format**: int year (`2020` → `int`), period token (`HT2020` / `2020-Q3`
  / `2020-08` / `2018-12-31` → `str`), range (`<from>..<to>`, literal `..` →
  `{"from","to"}` dict), `_default` sentinel, and the **#307 comma list**
  (`2005..2010,2015..2020` → one segment per member via `parse_period_query`; #340). A
  bare year maps to `int` (the documented year arm); every other token to `str`. List
  members follow the scalar grammar — no empty members, `_default` whole-value-only;
  order/overlap are deliberately NOT gated (the route's union is order-insensitive, and
  the sorted/disjoint rule belongs to the AUTHORED `Source.period`, enforced by
  reg_schema's structural validator).
- **`?variant` ADMITS `_default`** (a real `register_variant` slug, see
  reg_meta/DESIGN.md → Two-level variable model) UNLIKE the path guard (which rejects
  `_default` because it's not a path segment). `?value_set_version` is the
  `value_set_version_label` grammar and does NOT admit `_default`; the `_none` sentinel
  selects the empty-label vintage (the empty string can't ride in a query without being
  indistinguishable from absent).

The connection model is the **LOCKED P1 guard**: every DB-backed route opens its sqlite
connection INSIDE the sync handler body via `with _catalog_conn(request) as conn:` —
NEVER a FastAPI generator `Depends` (which is entered on a different threadpool thread →
cross-thread `ProgrammingError`). Each DB-backed route gets its OWN `ThreadPoolExecutor`
concurrency smoke (the `TestClient` sequential default masks the bug).

## Global catalog search (`routes/search.py` + `conn.py`)

`GET /api/search?q=&limit=` (#350) is the discovery surface consumed by the global
header omnibox (`SearchOmnibox.svelte`, shipped in this PR). It returns **typed result
groups** over the shipped FTS5 indexes, reusing reg_meta's concept-group-folded `search`
(`reg_meta.queries.search`, #322) — the webapp does NOT reimplement folding or FTS.

The SPA surface: a global `<SearchOmnibox>` in the app header routes to a shareable
`/search?q=` results page (`SearchView.svelte`) that renders the four typed groups with
navigation to catalog nodes. The router gained `search` and `doc` routes (query lives in
`?q=`, keyed on pathname so the page re-runs on every query change) and a
`router.replace()` method (mirrors the `?period` URL-as-single-source-of-truth pattern:
the omnibox syncs back to the URL, and the URL drives the view). `api.ts` gained
`search(q, limit?)` typed off the codegen'd contract. The **docs group** ships as a 5th
sibling group in `SearchView.svelte` (#394): it calls the DEDICATED `/api/docs/search`
endpoint via a SECOND, independent `asyncResource` — failure-isolated from the four main
groups (a docs failure, an absent index returning `ingested:false`, or an empty result
silently omits the docs section and never blanks the main groups). The `/doc/<filename>`
route (router `Route` union arm `{name:"doc",identifier}`) renders `DocView.svelte`:
title, register/variable/tags, a `source_url` seam (None today, link-ready), and a
bounded `excerpt`; 404 distinguishes "not ingested" vs "not found"; `snippet`/`excerpt`
are rendered as TEXT, never `{@html}`, and the full converted body is never fetched.

**The response contract is the point — designed to extend.** The body is
`{kind, query, groups: SearchGroup[]}`; each `SearchGroup` is a discriminated arm
(`group` literal) carrying its own `total_count` + typed `results`. Today: `registers`,
`variables` (leaf hits ⧺ folded concept groups), `classifications` (leaf hits ⧺ folded
vintage groups), and `codes` (#352 — value-label hits annotated with their owning
variables/classifications). **Docs (#354) join as a NEW arm of the `SearchGroup` union +
new result models — existing groups are never reshaped.** The SPA must tolerate an
unknown `group` value (skip it) so a new group can ship before the SPA renders it (the
same payload-skew tolerance the `?period` additive fields rely on). Each result carries
its navigable `fqid`; results within a group are pre-sorted by FTS rank.

- **One reg_meta call per group**: register/variable/classification via the FTS
  `field="description"` path; **codes (#352) via the `field="value", type="value"`
  path** (`value_code_fts` label match + code-shape exact/prefix on `value_code.code`,
  ranked bm25 + rarity-downweight, owner-annotated — see reg_meta DESIGN.md → FTS5
  configuration). Each group gets its own `total_count` + per-group `limit`; codes don't
  fold into concept groups (`fold_groups=False`).
- **Input gates** (`query_input.validate_text_query` / `_validated_limit` /
  `_has_searchable_token`): a query is length-capped (422 over 200 chars) and
  NUL-rejected (422); `limit` is clamped to \[1, 50\] (not 422'd). A blank / whitespace
  / punctuation-only query returns ALL groups EMPTY (200, not 422) — it never reaches
  reg_meta (whose LIKE label-fold would otherwise turn `%%` into a match-everything).
  FTS-operator neutralization + prefix-matching + diacritic folding all live in reg_meta
  (`_fts_match_query`); the webapp passes the raw query through. The query reaches FTS
  only as a bound parameter (no SQLi surface), so the gates guard cost/abuse, not
  injection.
- **Golden-boost seam** (`_apply_golden_boost`): a no-op identity hook where #311's
  curated golden/starred boost will reorder within a group. Wired now so the ordering
  contract and call sites already exist.
- **ETag/caching is automatic**: `/api/search` is a GET, so the `ETagMiddleware` stamps
  a body-derived ETag (the query is part of the URL → part of the CF edge cache key, and
  part of the body → part of the ETag). No per-route caching code.
- **Connection seam** (`conn.py`): the per-request read-only open (`catalog_conn`, the
  threadpool-safe pattern from #168) is shared with the catalog routes — extracted to
  `conn.py` so search doesn't import the catalog route module just for the connection.

The shared `?q=` input gate (`query_input.validate_text_query`: length cap + NUL reject,
both → 422) is reused by the docs endpoints below; per-group `?limit` is clamped, not
422'd.

## Docs library endpoints (`routes/docs.py`)

`GET /api/docs/*` (#354) exposes the prebuilt `reg_meta_docs.db` FTS index — already
baked into the deployed container (the Dockerfile asserts it) but previously unopened by
the webapp. It reuses reg_meta's read-only query layer (`doc_search` / `doc_get` /
`doc_registers`); no new query logic beyond plumbing + the response policy.

- **Endpoints**: `GET /api/docs/search?q=&register=&limit=&offset=` (register-scoped
  optional), `GET /api/docs/doc/{identifier}` (by variable name or filename), and
  `GET /api/docs/for-variable?q=&register=` (the "mentioned in documentation"
  variable-leaf hook).
- **Policy — excerpts, never full text**: the detail endpoint returns metadata + a
  `source` pointer + a BOUNDED `excerpt` (first `_EXCERPT_CHARS` of the cleaned body),
  and search returns the FTS `snippet`. The full converted body is NEVER served
  (marker+Gemini conversion quality + republication exposure). `source` is the SCB
  source-document identifier; `source_url` is a seam (None today — the data carries an
  identifier, not a URL; resolving it is future enrichment / a steward concern).
- **Coverage distinction encoded in the response**: coverage is LISA-only today.
  `ingested` is False when the docs index is absent entirely; the variable hook's
  `register_ingested` is False when *that register* has no ingested docs — so a UI reads
  "no docs ingested", never "this variable is undocumented". The variable hook's results
  are flagged `fuzzy` (a name/provider_key text match, not an authoritative variable→doc
  link).
- **Optional DB / graceful degradation**: the docs DB is OPTIONAL. The boot seam
  (`app._resolve_docs_db_path`) resolves + validates it once; on absence OR
  schema-incompat it sets `app.state.docs_db_path = None` (never crashes — a broken docs
  index must not take down the catalog API). Endpoints then return `ingested=False`
  (search / for-variable) or 404 "not ingested" (doc get). When present, the per-request
  open is `conn.docs_conn` (same threadpool-safe model as `catalog_conn`,
  `check_schema=False`).
- **Not folded into `/api/search`**: the `SearchGroup` union reserves a `docs` arm (#350
  contract), but it remains unused — the docs index is a *separate optional DB* and its
  `ingested` degradation doesn't map onto a group's `total_count`/`results` shape, so
  folding it into the omnibox endpoint would couple `/api/search` to a second DB open on
  every search request. Instead (#394), `SearchView.svelte` fires a SECOND, independent
  `asyncResource` directly at `/api/docs/search` and renders the results as a 5th
  sibling group — failure-isolated, silently omitted when the docs index is absent or
  empty, and never able to blank the four main groups. The reserved `docs` `SearchGroup`
  arm stays unused; the separation is the right call given the optional-DB / degradation
  rationale above.
- **ETag/caching**: GET reads, so the `ETagMiddleware` covers them (query in the URL →
  edge cache key, in the body → ETag) — no per-route caching code.

## Coverage aggregates (#351)

The catalog listing payloads carry an **additive** `coverage` object so a browse row
shows its study-window span without resolving every state:

- **Register-children** (`/api/catalog/{provider}/{register}` binding nodes):
  per-variable `coverage` — `coverage_from` (min `valid_from`), `coverage_to` (max
  finite `valid_to`; None when `open_ended`), `open_ended`, `state_count` (>1 in a
  window = a break worth surfacing).
- **Provider-children** (`/api/catalog/{provider}` register nodes): per-register
  `coverage` — `variable_count` (slugged variables) + the span over all their states.

**Query-time, not materialized — measured first** (the #351 design decision). The
aggregates are one GROUP BY over `variable_state` per listing, in reg_meta
(`Catalog.register_variable_coverage` / `provider_register_coverage`). Measured on the
real v0.11.0 DB: the worst register (scb/ulf, 7.3k variables) computes per-variable
coverage in \~9 ms (\~60 ms end-to-end serializing all 7.3k binding nodes); the heaviest
provider (scb, 238 registers) \~34 ms end-to-end. Both sit behind the ETag/edge cache,
so build-time materialized columns (which would ride the batched Lane R schema bump) are
NOT needed. The covering index `idx_variable_state_coverage` on
`variable_state(variable_id, valid_from, valid_to)` (#371, the 5.4.0 schema cut) lets
the grouped MIN/MAX span scan be satisfied index-only (no table b-tree lookup; EXPLAIN
QUERY PLAN reports `USING COVERING INDEX`).

- **Additive / payload-skew (#317)**: `coverage` is optional and the SPA doesn't read it
  yet — it must tolerate its presence AND absence. It's None on a node that wasn't
  enriched (e.g. a register's own node — coverage is populated only in the two LISTING
  payloads).
- **Open-ended sentinel**: `coverage_to` is None + `open_ended` True when the latest
  window is the `9999-12-31` DDL sentinel ("ongoing"); a stateless variable is
  `state_count == 0` with both bounds None (distinct from open-ended). The sentinel
  constant (`reg_meta.catalog.OPEN_ENDED_VALID_TO`) is now single-sourced in reg_meta.
- **Cadence DEFERRED**: #351 also lists a per-register "cadence", but reg_meta has no
  cadence attribute and no clean derivation (a modal period-grain is fuzzy for
  mixed-grain registers), and no UI consumes it yet. The load-bearing study-window
  signal is span + counts; cadence is a follow-up (a defined source or a build-time
  field) — not shipped here.

## ETag / Cache-Control (`etag.py` + `middleware.py`)

Every read endpoint (`/api/context`, the `/api/catalog` root + catch-all, the 7
sub-endpoints) carries `ETag: "<reg_meta_version>-<steward_id>-<sha256(body)[:16]>"` and
`Cache-Control: public, max-age=86400, must-revalidate`; a matching `If-None-Match`
yields a **304** with no body. The pure logic lives in `etag.py` (`compute_etag` +
`etag_matches`); an ASGI middleware (`ETagMiddleware`) wires it DRY onto every GET read
response.

- **`reg_meta_version`** is the INSTALLED `reg_meta.__version__` (the v1.x Model A
  package release), NOT the DB `schema_version` manifest. `steward_id` is
  `app.state.steward.id`.
- **The body-hash** makes `If-None-Match` per-URL coherent — the `?period` / `?variant`
  query is part of the URL, so it's already part of the cache key (different periods are
  different ETags).
- **Middleware skips WRITE endpoints** via a method gate: only `GET` reads are stamped,
  so the POST endpoints pass through with no ETag. It also skips non-200 responses — an
  error body isn't a cacheable representation, and handing the client a validator for a
  transient error would be wrong.
- We unit-test only the ETag / Cache-Control LOGIC + the 304 behavior. The **edge** side
  (Cloudflare edge caching / DDoS shielding / edge rate-limits) is a deploy/maintainer
  concern and not backend code. Remaining: edge config — see `REFACTOR_SPEC.md`.

## Steward layering and the in-memory catalog index (`stewards.py` + `catalog_index.py`)

A steward is `stewards/<id>/steward.toml` (identity/branding, required) plus an optional
`steward.project_data.json` (the catalog filter). The **`global`** steward ships only
`steward.toml` — the *absence* of the project file means full-universe mode (no filter,
reg_meta's whole catalog). The loader (`stewards.load_steward`) detects that absence via
`has_catalog_filter`.

**Why reuse `project_data.json` as the catalog schema?** A steward catalog is
structurally identical to a researcher's project (same `reg_schema` validator) — many
`sources`, no `panels` — so the FQIDs on its columns *are* the catalog, with no separate
catalog schema to maintain. The webapp can validate both a project and a catalog with
the same structural + semantic validators, so consistency comes for free.

The in-memory **`CatalogIndex`** is built once at boot (`load_catalog_index`, with the
boot connection) and held on `app.state` for the process lifetime. It is the filter that
scopes a steward deployment to a subset of reg_meta's universe. It is an internal frozen
`@dataclass` (never a response body — only response models are Pydantic; webapp
internals are dataclasses), carrying two maps derived from the steward project's
`sources[]` (building needs the same live `Catalog` the boot validation ran against —
see column resolution below):

- `bindings_by_variant` — `register_variant` coordinate → frozenset of admitted
  `(binding FQID, resolved delivery column)` pairs. **Admission is column-based** (#206,
  decided 2026-06-11): a steward is given a concrete dataset, so its catalog is a
  statement of *holdings*, and holdings are physical delivery columns, not concepts —
  bare-FQID admission cannot express "this steward has SSYK, but only at the 1-digit
  level". The FQID side is the bare 3-segment binding FQID (no `@version` pin to
  normalize away — that grammar is retired); the column side is the **resolved**
  `delivery_column_name` of the steward binding's states (its `representation` when
  pinned; every column its states deliver otherwise — a sequential rename inside the
  steward's period contributes one pair per column), never the raw `representation`
  string. Resolving both sides at their own validation time means a steward catalog
  authored as `representation: None` back when the concept had one column still compares
  equal to a researcher who must now pin (reg_meta grew a sibling column). `None` is
  **not** a wildcard — it resolves to the unique column it denoted (pre-v1, no compat
  layers).
- `period_range_by_register` — register FQID → best-effort `(lo, hi)` period span for UI
  hinting **only**, NOT a validity gate (the semantic validator's per-binding
  `period_outside_state_validity` is the gate; mixed period grammars don't sort cleanly
  as strings).

The `global` deployment (`has_catalog_filter=False`) has **no** index (`None`); the
catalog endpoints pass through to reg_meta's full universe.

**Steward-load drift downgrade.** Loading a steward catalog runs the same
`validate_semantic` (below) in **steward-caller** mode. A reg_meta-drift resolution
failure (`fqid_unresolved` / `value_set_missing` / `period_outside_state_validity` /
`binding_representation_unknown`) is downgraded error → warning so the deployment
**boots through** reg_meta evolving out from under a steward's committed catalog: the
affected bindings are DROPPED from the index (unauthorable until the steward updates)
and the warnings ride on `/api/context` so the SPA can show a "catalog drift" banner.
Because the downgrade keeps `result.ok` True, the loader keys on the **warnings list**,
not `.ok`. A *structural* break in the committed catalog (malformed JSON, an
unexpected/typo'd field that survives structural but fails model construction) is
**not** drift — it's a misconfigured deployment, so it fails fast
(`StewardCatalogError`). A residual *semantic* error that survives the drift downgrades
(e.g. a still-ambiguous `binding_value_set_version_ambiguous`, which stays an error
because it's an author-time choice, not drift) also fails the boot — don't admit a
broken binding to the index and never surface it.

Adding a steward is a monorepo PR (drop a directory, register the hostname, rebuild).
`REG_WEBAPP_STEWARD` selects which steward a process serves; `REG_WEBAPP_STEWARDS_DIR`
overrides the on-disk root for a packaged wheel/Docker image (the `stewards/` sibling
doesn't exist there). Remaining: the SPA catalog-authoring mode, a
`reg-meta-build steward-diff` CLI, and per-steward `extensions` — see
`REFACTOR_SPEC.md`.

## Pydantic boundary

reg_webapp defines its **own** webapp-local Pydantic response models (`models.py`) for
the reg_meta dataclass surface — reg_meta stays plain dataclasses on the library surface
(import lightness), so the webapp wraps them 1:1. This is the **only** place a 1:1
Pydantic wrapper remains. For `project_data`-related responses (`/api/project/*`) the
webapp uses **`reg_schema` Pydantic models directly** — no wrapper layer, eliminating
that drift surface. (The cross-package bundle boundary — validated `Source` → dataclass
`LoadedSpec`, which is where the Pydantic side hands off to the dataclass bundle runtime
— lives in `reg_monabundle/DESIGN.md`.)

The catalog response models are 1:1 wrappers of reg_meta's frozen `Catalog` dataclasses.
Each node model carries a `kind` `Literal` discriminator (`provider` / `register` /
`binding` / `classification` / `classification-root` / `root` / `variants-ref`); the
catch-all returns a Pydantic discriminated union (`Field(discriminator="kind")`) so
`openapi-typescript` emits a clean tagged union. FQID fields serialize as plain `str`
(`str(fqid)`), never nested models, so the codegen'd TS sees flat string fields. The
binding **leaf** embeds the variable's FULL longitudinal record from one
`Catalog.resolve` call (states, value sets, and the variable-grain `same_as` /
`replaced_by` / `related_to` / `lineage` edges). `lineage_warnings` are **omitted** —
`ResolvedVariable` doesn't carry them; they arrive via the `/lineage_warnings` endpoint.

One gotcha: a `register` field on a `pydantic.BaseModel` shadows `BaseModel.register` (a
method) and warns. The edge-ref models name the Python attribute `register_name` and
`Field(alias="register")` it, so the wire/JSON key (and OpenAPI schema property) stays
`register` while the warning is gone — the alias is also the canonical init param the
mappers construct with.

## OpenAPI snapshot + TS codegen (the drift gate)

`openapi.json` is committed and is the canonical contract. `gen_openapi.py` dumps
`create_app().openapi()` with `sort_keys=True` + a trailing newline so the snapshot is
byte-stable across machines. `app.openapi()` builds without the lifespan (no DB needed),
so the dumper runs offline. The SPA codegens `src/lib/api-types.ts` from the snapshot
via `openapi-typescript`. Two checks keep these in lockstep: `test_openapi_snapshot.py`
(in the always-run `test` job) asserts the committed `openapi.json` equals a fresh
render of the app, and the `reg-webapp-frontend` CI job regenerates `api-types.ts` from
the committed snapshot and fails on any diff — so app, snapshot, TS types, and the
committed tree must agree.

## Frontend toolchain

Svelte 5 + Vite + TypeScript, bun-managed. **Biome** (`>=2.3.0`) is the single
formatter/linter — no prettier/eslint. Biome's experimental Svelte support formats/lints
the JS/CSS/HTML parts of `.svelte` but does **not** yet parse Svelte control-flow
(`{#if}` / `{#each}`), so:

- `.svelte` formatting is imperfect (an accepted tradeoff).
- `noUnusedVariables` / `noUnusedImports` are disabled for `.svelte` in `biome.json` —
  Biome can't see template-bound usage of `<script>` declarations and false-fires.
  **`svelte-check`** (the `check` script) is the authoritative type/template gate and
  does see template usage.
- The codegen'd `src/lib/api-types.ts` is excluded from Biome entirely (codegen output,
  never hand-formatted).

## SPA routing + production fallback

The SPA (`frontend/`) browses the catalog read-only with **path-based routing**: clean
URLs mirror the API (`/catalog`, `/catalog/scb/lisa`, `/catalog/scb/lisa/kon`,
`/catalog/class/<slug>`). The router is hand-rolled — no routing-library dep — in
`src/lib/router.svelte.ts` (a `.svelte.ts` module so its reactive `$state` route
compiles): it reads `window.location.pathname`, navigates via `history.pushState`,
handles `popstate`, and intercepts internal `<a>` clicks (the `link` action) so
navigation doesn't full-reload.

- **Dev** serving Just Works: the Vite dev server's default `appType: 'spa'` rewrites
  unknown paths to `index.html`, and `vite.config.ts` proxies `/api` to the backend on
  `:8000`. Deep-linking to `/catalog/...` in `bun run dev` works.
- **Production** SPA fallback is a **deploy/maintainer task**, NOT backend code. The
  backend is a pure JSON API — `create_app` mounts no `StaticFiles` and serves no
  `index.html` (keeping `/api`, `/openapi.json`, `/docs` un-shadowed). The SPA is served
  by the edge (Cloudflare), which must rewrite a cold-load deep link to any non-`/api`
  path → `index.html` (a `_redirects` / 404-rewrite rule). This mirrors the "edge config
  is a maintainer task" pattern (ETag section above); see the comment atop
  `router.svelte.ts`.

The fetch wrapper (`src/lib/api.ts`) types every response off
`components["schemas"][...]` from the codegen'd `api-types.ts`, so the SPA and the
backend contract can't drift. The catch-all returns the `kind`-discriminated
`CatalogNode` union; components narrow on `kind` via `src/lib/catalog.ts` helpers
(unit-tested).

## Deployment (`global` on Fly.io, Cloudflare edge in front)

§6.5's origin-platform decision (2026-06-11): the container runs on **Fly.io**, with a
Cloudflare zone in front. The deciding factor was the edge-cache contract: the origin
ETag/`Cache-Control` machinery (above) and the #220 FQID round-trip gate assume a
classic URL-addressed origin behind Cloudflare's zone cache. Cloudflare's own Containers
product routes all traffic through a Worker via a Durable Object binding — zone Cache
Rules never see those responses — so the shipped ETag design would need re-implementing
in Worker code against a per-colo-only cache. Fly is also \~5x cheaper for this shape
and officially documents the Cloudflare-in-front topology
(`fly.io/docs/networking/understanding-cloudflare`). Lock-in is nil: the artifact is the
plain Docker image; only `fly.toml` and the CI deploy job are Fly-specific.

- **App**: `reg-webapp-global` — a single always-on `shared-cpu-1x`/1GB machine in `arn`
  (Stockholm, where the users are). Always-on is deliberate: Fly's ephemeral-rootfs I/O
  is throttled (\~8 MiB/s), so a cold boot re-reads the SQLite pair slowly — keep the OS
  page cache warm rather than scale to zero (\~$6/mo). Config: `reg_webapp/fly.toml`;
  `--ha=false` keeps the machine count at one.
- **Read-only SQLite on the ephemeral rootfs is the right model** — the DB pair is baked
  into the image and replaced with it. No volume, no LiteFS, nothing persists.
- **Deploys**: one workflow (`container-build.yml`) owns both deploy surfaces, scoped by
  a `changes` paths-filter job. Image-affecting main pushes (Dockerfile COPY surfaces +
  bake inputs — NOT baked deps reg_schema/reg_monabundle, which need a manual
  `workflow_dispatch` — decided 2026-06-11: that is the rule, not a gap) build, push to
  `registry.fly.io` (SHA-tagged), and `flyctl deploy --image`. The bake build-arg is the
  RESOLVED newest `reg_meta/v*` tag (never `latest` — a literal `latest` makes the bake
  layer's buildx cache key insensitive to data-only releases and can even resurrect a
  stale cached layer after a pinned dispatch). Nothing deploys without green CI: a
  `wait-ci` job polls this commit's ci.yml run and both deploy jobs require its success
  — an image that builds but fails lint/ty/pytest never ships. Both deploy jobs carry a
  HEAD-of-main guard (GHA concurrency serializes by build-completion order, not commit
  order — without the guard an older commit's slow build could overwrite a newer deploy;
  it also makes non-main dispatches deploy-inert). Two gates guard a bad image: the
  entrypoint smoke gate (container exits non-zero before ever serving) and fly.toml's
  `/api/context` HTTP check (flyctl reports failure if it never passes). Rollback:
  `flyctl releases --image` lists history; `flyctl deploy --image <old>` restores in
  seconds.
- **Build/registry economics (#290)**: the reg_meta DB bake lives in its own Dockerfile
  stage (`regmeta-db`) whose cache key covers only the workspace skeleton, the reg_meta
  source tree, and `REG_META_TAG` — app-code edits reuse the cached DB layer instead of
  re-downloading the release pair. PR builds neither `load` the image into the runner's
  docker (nothing runs it; all gates execute during the build) nor write GHA buildx
  cache (PR-scoped cache is unreadable from main and would only evict useful entries
  from the repo's 10 GB pool); PRs still read main's cache. Every pushed tag is an
  immutable rollback handle: `workflow_dispatch` rebuilds on an existing HEAD get a
  `-<run_id>` suffix instead of overwriting `:sha`. A post-deploy prune step keeps the
  newest 10 tags and deletes older manifests via the registry v2 DELETE (supported by
  Fly — verified live 2026-06-11; buildx pushes OCI indexes, so age is read from the
  image config's `.created`, and a digest shared with any kept tag is never deleted).
- **Cloudflare zone**: `catalog.swecov.se`, orange-cloud A/AAAA → the Fly app's shared
  IPv4 + dedicated IPv6, plus a `_fly-ownership` TXT (proves ownership behind the proxy)
  and a grey-cloud `_acme-challenge` CNAME (DNS-01 cert issuance — the reliable path
  behind a proxy; never proxy a hostname pointing at `*.fly.dev`: Fly's edge has no cert
  for the custom SNI → 525). SSL mode Full (strict). No dedicated IPv4 — the free shared
  IPv4 works behind the proxy.
- **Edge worker** (`reg_webapp/edge/`, Workers free plan): static-assets worker on
  `catalog.swecov.se/*` serving the SPA `dist/` with `single-page-application` deep-link
  fallback; backend paths (`/api/*`, `/openapi.json`, `/docs`) are `run_worker_first` +
  `fetch(request)` passthrough to the zone origin (Fly), so the origin
  ETag/`Cache-Control` contract governs API caching as a classic proxied origin.
  `run_worker_first` is required: SPA mode otherwise serves `index.html` to browser
  navigations without invoking the worker, shadowing `/api` deep-opens. The glob list
  and the worker's `ORIGIN_PATHS` regexes are a LOCKSTEP pair (comments in both files);
  the backend disables `/redoc` (`create_app` passes `redoc_url=None`) so its surface is
  exactly the forwarded set. Cloudflare downgrades the origin's strong ETag to weak
  (`W/`) when compression applies — weak comparison is correct for GET revalidation, not
  a bug.
- **Edge cache generations (#318)**: the worker stamps a per-deploy `DEPLOY_VERSION`
  (wrangler var; CI passes the commit SHA, `-<run_id>`-suffixed on dispatch so same-SHA
  data-only rebuilds still count) onto every origin-bound URL as an `__edge_v` query
  param. The zone cache key is the full URL, so each deploy orphans all prior `/api/*`
  cache entries — fresh payloads immediately after deploy, while the 24h TTL still
  bounds origin traffic *within* a generation. This is the free-plan substitute for
  `cf.cacheKey` (Enterprise-only) and needs no purge credentials. Origin-side the param
  is inert: FastAPI ignores undeclared query params and the ETag is content-derived.
  Consequence: `edge-deploy` runs on **image-affecting** pushes too, not just edge paths
  — an origin deploy that changes API payloads without touching the SPA/contract must
  still ship a new cache generation. The motivating incident (#303 rollout) had the edge
  serving 11h-old pre-deploy catalog JSON against a freshly deployed SPA; the #317
  defensive-rendering rule (SPA tolerates one cache generation of payload skew on
  additive fields) stays in force regardless, for clients holding *browser*-cached
  payloads (`max-age=86400` applies there too, unversioned). Deploys: the `edge-deploy`
  job in `container-build.yml` rebuilds the SPA (bun pinned to the Dockerfile's version
  — bump together) and runs `wrangler deploy` on main pushes touching the SPA, the edge
  worker, the committed `openapi.json`, or the image surface (cache generation, above)
  (`CLOUDFLARE_API_TOKEN` repo secret, "Edit Cloudflare Workers" template scoped to the
  account + swecov.se). The job `needs:` the origin deploy — on a contract-changing push
  the SPA never goes live before the origin serves the new endpoints (deploy-skew guard;
  skew 404s are NOT negatively cached: the Cache Rule's Edge TTL is "bypass if no
  cache-control", and the origin only stamps 200s). After each edge deploy a probe
  asserts a catalog read returns `CF-Cache-Status: HIT` with a young `Age` (a stale
  `Age` means cache-key versioning broke) and an edge 304 — the #220 gate as a standing
  regression check against silent Cache Rule / zone drift. Manual fallback: build the
  SPA, then `wrangler deploy` with a FRESH `--var DEPLOY_VERSION:...` (exact command in
  `wrangler.jsonc`'s header — the config's literal `"dev"` default must not ship).
- **Zone rules (dashboard, free plan)**: a Cache Rule making `/api/*` on the hostname
  cache-eligible (Cloudflare never caches extensionless API paths by default, even with
  `Cache-Control: public` — without the rule every read is `cf-cache-status: DYNAMIC`),
  and the free plan's one WAF rate-limiting rule (path-only match — free-tier rate
  limiting can't match hostname; 100 req/10s/IP → block, burst-verified to 429).
- **#220 gate: PASSED (2026-06-11)** — 20 slash-bearing FQID paths (3-segment bindings,
  `/states` suffixes, `/variants`) round-trip the edge cache byte-identical to origin,
  MISS→HIT per URL, ETag→body mapping consistent, and conditional GETs answer 304 from
  the edge (`CF-Cache-Status: HIT`, no origin traffic). The path-based FQID surface
  stands; no query-string fallback needed before publishing the OpenAPI.
- **Known quirk**: pre-existing zone bot protection 403s non-browser User-Agents (e.g.
  Python's default `urllib` UA) on every path including `/api/*`; the SPA is unaffected,
  but programmatic API consumers must send a real User-Agent header.

## Frontend unit tests (Vitest)

`bun run test` runs **Vitest** (`vitest run`) — Vite-native, so it reuses
`vite.config.ts` and compiles `.svelte` / `.svelte.ts`. The env is `jsdom`
(`router.svelte.ts` reads `window` at module load; `api.ts` mocks `fetch`). Tests live
next to source as `*.test.ts` and cover the fetch-wrapper error path, the
`kind`-narrowing helpers, and route parsing. The `reg-webapp-frontend` CI job runs
`bun run test` alongside `svelte-check` + the codegen drift check. (Use `bun run test`,
not `bun test` — the latter is Bun's own runner, which doesn't compile Svelte.)

## Why CI uses a fixture DB, not a real asset

CI has no published `reg_meta/v*` DB release asset to pull, so `reg-meta update` in CI
would fail or fetch a stale incompatible DB. Instead the backend tests build fixture DBs
in a tmp dir and point the app at them via the highest-precedence `REG_META_DB`
override:

- **`/api/context`** reads only `import_manifest`, so its fixture (`compatible_db` /
  `mismatched_db`) is just that one table.
- **`/api/catalog`** resolves/lists against the full reg_meta schema, so the
  `catalog_db` fixture builds a **slugged** DB via `reg_meta_build`'s `_slugged_db`
  helper (a `scb/lisa/kon` binding with a state + value set, a second `scb/rams`
  register, a `same_as` edge, and a `class/sun2020` classification), then stamps an
  `import_manifest` so the boot compat check passes. The backend `conftest.py` mirrors
  `reg_meta/tests/conftest.py`'s sys.path injection to import that bare-name helper.

The real DB at the default path is the **local** boot smoke the maintainer/ orchestrator
runs.

## Project-write surface (`routes/project.py` + `routes/bundle.py`)

Three POST endpoints: `/api/project/validate`, `/api/project/order`, `/api/bundle`. All
read the body as a **raw JSON dict** (not a typed param): `/validate` must accept
malformed specs to diagnose them, and `/bundle` must preserve steward-namespaced blocks
(`swecov` / `reg_mockdata`) that a typed `extra="ignore"` body would silently drop. The
raw body is documented in OpenAPI as an open object (`additionalProperties: true`) so
the SPA codegen sees a body to send.

- **`/validate` status discipline.** A spec that FAILS validation is a *successful
  validation response* — **HTTP 200 with `ok=false` + the issues**. 4xx is reserved for
  a malformed REQUEST (non-JSON, duplicate JSON keys, a too-deeply-nested or non-object
  body, an oversized body). It runs the §6.8.0 three-layer composition (structural →
  namespaced-block → semantic) and returns the **concatenated** issue list; the DB-free
  layers run first, so a structurally-rejected body costs no DB hit. It also runs the
  build-side cross-block referential checks (orphan `binding_options` keys /
  suppress_k-on-non-categorical), closing that half of the old `/validate`↔`/bundle`
  divergence — the one residual gap is `/bundle`'s step-4 capability gates (e.g. a
  build-required `display_name`), which `/validate` defaults from reg_meta, so a
  `/validate`-clean spec can still 422 at `/bundle`.
- **`/order`** renders the steward's default order-export CSV (a `text/csv` download)
  and is the one documented exception to the "every route declares a `response_model`"
  lint (it returns raw bytes). Unlike `/validate`, it structurally **gates** first: you
  cannot render a provider order from an invalid spec → 422.
- **`/bundle`** embeds the raw dict into a single-file `.py` MONA bundle (a pure
  function of input → content-hash cacheable, no ETag). It is DB-free. A build-gate
  raise (bad input) → 422; a malformed body → 400.

**Connection model = per-request open ON ONE THREAD** (the locked cross-thread guard).
`/validate` and `/order` are `async` only to read the body off the wire; the blocking
work (structural parse + per-binding sqlite resolution) is offloaded via
`run_in_threadpool`, and the reg_meta connection opens on **that** worker thread inside
a `with`-block — NEVER a generator `Depends` (which can run on a different AnyIO thread
→ `sqlite3.ProgrammingError`). `/bundle` is DB-free.

The order CSV cell values are passed through a **spreadsheet formula-injection** guard
(`_csv_safe`): a researcher-controlled `display_name` like `=HYPERLINK(...)` would
otherwise execute as a formula when the data provider opens the manifest. A leading
formula-trigger char (`=+-@\t\r`) is prefixed with a single quote.

## Semantic validation (`semantic.py`)

The §6.8.3 reg_meta-backed validation layer. It lives in the webapp — NOT `reg_schema` —
because `reg_schema` is reg_meta-free by design (MONA-amalgamatable); semantic rules
need the live DB, so they belong where the DB is. `reg_schema` lists these codes as
defined-but-not-emitted on its own surface; this is their home. The webapp invokes it
(with `reg_schema`'s structural validator and the owning packages' block validators) —
`reg_schema` itself never imports the owning packages. It emits the same frozen
`reg_schema.ValidationIssue` shape the other layers do, so composition is plain tuple
concatenation. It takes a `Catalog` and never opens a connection (the caller owns the
connection's lifetime).

Rules, walking each source's `register_variant` + every binding:

- The `register_variant` coordinate resolves to a known variant; the binding `variable`
  (3-segment FQID) resolves to a known variable (following `same_as` links —
  `Catalog.resolve` does that). Unresolved → `fqid_unresolved` (error).
- The binding resolves to a covering `variable_state` at the source's variant AND
  period. None → `period_outside_state_validity` (error). A range / `_default` period
  crossing a state transition (sequential, non-overlapping states) →
  `binding_state_drifts_within_period` (info). A **#307 list period** (interrupted
  series; structurally sorted + disjoint, wire form comma-joined —
  `2005..2010,2015..2020`) resolves **per segment**: `period_outside_state_validity` and
  `range_period_partially_covered` fire per uncovered/under-covered segment (naming it),
  and the PER-INSTANT probes — co-existence/ambiguity, the co-delivered-value-set
  backstop, the pinned representation's presence — also run per segment (the
  whole-series union would false-positive on windows overlapping only BETWEEN segments).
  Only the series-level properties — the resolved columns for steward admission and the
  sequential-drift info — use the `state_id`-deduped union of every segment's states.
  `Catalog.resolve_at` never sees the list form; since #340 the catalog `?period=` query
  accepts the comma wire by doing the same per-segment resolve + union in the route (see
  The `?period` query above).
- The binding's `value_set` (a `class/<slug>` FQID) resolves to a known classification →
  else `value_set_missing` (error).

**Representation, not `@version`.** A FQID names one concept, but a concept may carry
several **co-existing delivery columns** at the same instant — parallel representations
(SSYK 3/4/5-digit, age brackets). When ≥2 distinct delivery columns co-exist
(overlapping validity windows) and the binding sets no `representation`, the extract
would pull more than one column → `binding_value_set_version_ambiguous` (error); the
author must pick one via `Binding.representation` (the delivery column name; the SPA
offers a chooser). This is exactly the job the retired `@version` pin used to do, now
keyed on the delivery column. A `representation` reg_meta no longer delivers as a column
→ `binding_representation_unknown` (error). Crucially, the co-existence test keys on
**overlapping** windows: distinct columns in *non*-overlapping windows are a sequential
rename (drift), NOT ambiguity, and must not demand a `representation`. A separate
defensive backstop (`binding_value_set_version_ambiguous` on ≥2 distinct `value_set_id`s
on **one** column) should be unreachable against a clean catalog — the reg_meta build
enforces one value set per `(variable, variant, period, delivery_column)`.

**Caller context — researcher vs steward.** The `caller` flag drives the level mapping,
NOT a different rule set. The **researcher** path (`POST /api/project/validate`) keeps
unresolved-FQID-class codes as blocking **errors** (fix before extract). The
**steward-catalog load** path (boot) **downgrades** `fqid_unresolved`,
`value_set_missing`, `period_outside_state_validity`, and
`binding_representation_unknown` from error → warning, so the deployment boots through
reg_meta drift (those bindings drop from the index; see the steward section).
`binding_value_set_version_ambiguous` deliberately stays strict on both paths — it's an
author-time choice, not drift.

**Onboarding.** Stewards declare a subset of what reg_meta knows; data without an FQID
can't be authored (no `{display_name + type, no FQID}` escape hatch in v1). New
variables/registers/classifications onboard via slug-TOML PRs against `reg_meta_build`;
once the next reg_meta release lands, the steward adds them to their catalog.

**Steward catalog filtering — `fqid_outside_steward_catalog` /
`representation_outside_steward_catalog`.** When a researcher's project references a
binding outside the loaded steward catalog, the column-based admission check (#206)
emits one of two **warnings** (not errors): `fqid_outside_steward_catalog` when the
steward holds *no* column of the concept, and the distinct
`representation_outside_steward_catalog` when the steward holds the concept but not the
column the binding **resolves** to — its message enumerates what the steward *does* hold
("available from this steward as 'Ssyk1' only" is the actionable form of "not
available"). Warnings, because this is also the deliberate "what would my project look
like under steward X?" feature — load a spec against another steward's deployment and
the warnings enumerate exactly which columns would be unavailable; the SPA offers a
one-click "drop out-of-scope columns" remediation. The check is wired into
`/api/project/validate`: `routes/project.py` threads `app.state.catalog_index` into
`validate_semantic` via `run_in_threadpool`; the check runs **after** the per-binding
period resolution because the researcher side's resolved columns are what
`CatalogIndex.admits(fqid, column)` compares (when those are indeterminate — unresolved
period, unknown pinned representation, ambiguous multi-column binding — the binding
already carries its own error and only the FQID-level arm runs). The `global` deployment
(index `None`) never emits either code. Admission keying stays variant-agnostic and on
the literal binding FQID: a curated same_as sibling (e.g. `kon→syss`,
`same_definition_different_column`) names a *different* physical column, so warning on
it is correct under holdings semantics, not a keying artifact.

## Cost protection (`limits.py`)

Two stdlib-only ASGI middlewares (no `slowapi` dep) gate ONLY the write methods (POST);
read GETs flow through untouched (they have the cheaper edge-cache + ETag axis). These
are **origin-side** guards — Cloudflare fronts production with the same budgets at the
edge (remaining — see `REFACTOR_SPEC.md`); these catch direct origin hits that bypass
the edge.

- **`BodySizeLimitMiddleware`** — a **streaming** byte-count guard that 413s a body
  exceeding `MAX_BODY_BYTES` (1 MB). It counts bytes as they arrive rather than trusting
  `Content-Length` (absent on chunked transfers, and spoofable), so an oversized
  chunked/under-declared body is still caught even if the handler never reads it. 1 MB
  matches the bundle-output budget and is far above any plausible `project_data.json`.
- **`RateLimitMiddleware`** — an in-memory per-IP token bucket (`request.client.host`,
  \~`RATE_LIMIT_PER_MINUTE` req/min/IP → 429). **IP-only** by design: a session token
  would bucket per-browser (helpful behind NAT) but adds a fingerprinting surface for
  anonymous public data — layer it in only if a steward needs it. A missing client host
  buckets under one shared key (fail closed). Buckets are per-process (lost on restart,
  not shared across replicas) — sufficient as the origin backstop behind the edge
  limiter; a shared store (Redis) is a scale-out concern, not v1.

## Browser storage + project-file persistence (the SPA store)

Project files live in the **browser** during a session and as JSON in the user's git
repo for durability. There is **no server-side storage** — git is the durable store,
email/git-sharing handles collaboration (server-side projects are a possible v2
feature). The authoring store (`project_store.svelte.ts`) is a module-singleton Svelte 5
rune store holding one draft per session.

- **Autosave to IndexedDB** (`indexeddb_persistence.ts`) over the raw IndexedDB API (no
  `idb` dep — keeps the frontend dep surface lean) via a debounced (\~500ms) `$effect`.
  **Graceful degradation is mandatory**: in private mode / disabled storage / quota
  failures, `save` resolves and `load` resolves `null` so the app keeps working
  in-memory — autosave NEVER rejects or crashes the effect.
- **Store-schema stamping + gate.** Each persisted draft is stamped with the store's own
  `storeSchemaVersion` (distinct from the project's `schema_version`); `load` restores
  only on a match, else discards the stale-schema draft. This is the store's record
  shape, bumped only when the persisted shape changes.
- **Project-file version gate.** Model A files carry `schema_version` MAJOR **2**
  (reg_schema `2.0.0`) and `reg_meta_version` of the form `reg_meta/v1.x.y`. The SPA
  **hard-rejects** a file whose `schema_version` major is **1** OR whose
  `reg_meta_version` is `reg_meta/v0.x` (pre-Model-A) with a blocking open-error — no
  migration, pre-v1 policy. (`schema_version` major 1 is the *rejected* pre-Model-A
  value, not Model A.) Any other version is a neutral no-op: the backend stays the
  canonical validator.
- **Unsaved-changes warning.** A `dirty` flag derives from the draft diverging from the
  last DOWNLOAD baseline (`lastDownloaded`); a `beforeunload` listener prompts on a
  tab/window close with a dirty draft. The store drives the write endpoints (validate /
  order / bundle download) through `lib/api.ts`; it is NOT a structural validator (the
  backend is canonical).

Note: v1 is **one draft per SPA session** (a single IndexedDB key), not a multi-project
list — opening a file replaces the current draft.

## API surface

The committed `backend/openapi.json` is the canonical contract; this table is the
orientation map. All endpoints are under `/api/`; read GETs are edge-cacheable, write
POSTs are not. Catalog browse paths use FQID segments directly.

  | Method | Path                                          | Purpose                                                                                                                                                                                                                                                                    |
  | ------ | --------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | GET    | `/api/context`                                | Deployment identity, branding, build info, catalog-drift warnings.                                                                                                                                                                                                         |
  | GET    | `/api/catalog`                                | Top-level: every provider the steward exposes + the `class` root.                                                                                                                                                                                                          |
  | GET    | `/api/search`                                 | Global FTS search → typed result groups (`registers` / `variables` (folded) / `classifications` / `codes` (#352)); extensible (docs join as a new group). `?q=` required, `?limit=` per-group cap.                                                                         |
  | GET    | `/api/docs/search`                            | Docs FTS search (excerpts + source pointer), optional `?register=`; `ingested=false` when no docs index.                                                                                                                                                                   |
  | GET    | `/api/docs/doc/{identifier}`                  | One doc by variable/filename — metadata + source pointer + bounded excerpt (never full body).                                                                                                                                                                              |
  | GET    | `/api/docs/for-variable`                      | "Mentioned in documentation" hook: fuzzy name/`provider_key` matches + `register_ingested` coverage flag.                                                                                                                                                                  |
  | GET    | `/api/catalog/{fqid}`                         | Single endpoint for every hierarchy node (`kind`-discriminated). On a binding leaf, embeds the variable's full longitudinal record. Optional `?period` / `?variant` / `?value_set_version` narrow a binding leaf to a `{binding, states}` subset (uniform with `/states`). |
  | GET    | `/api/catalog/{provider}/{register}/variants` | The register's variant browser.                                                                                                                                                                                                                                            |
  | GET    | `/api/catalog/{fqid}/states`                  | Full state history for a binding.                                                                                                                                                                                                                                          |
  | GET    | `/api/catalog/{fqid}/predecessors`            | Inbound `variable_replaced_by` edges.                                                                                                                                                                                                                                      |
  | GET    | `/api/catalog/{fqid}/successors`              | Outbound `variable_replaced_by` edges.                                                                                                                                                                                                                                     |
  | GET    | `/api/catalog/{fqid}/related`                 | `variable_related_to` edges (sibling-grain variables).                                                                                                                                                                                                                     |
  | GET    | `/api/catalog/{fqid}/lineage`                 | Materialized `variable_state_lineage` edges (consumer ← source).                                                                                                                                                                                                           |
  | GET    | `/api/catalog/{fqid}/lineage_warnings`        | Linker-emitted lineage coverage warnings.                                                                                                                                                                                                                                  |
  | POST   | `/api/project/validate`                       | Three-layer validation; 200 + `ok` + issues.                                                                                                                                                                                                                               |
  | POST   | `/api/project/order`                          | Default order-export CSV download.                                                                                                                                                                                                                                         |
  | POST   | `/api/bundle`                                 | Build the MONA `.py` bundle from a spec.                                                                                                                                                                                                                                   |

**Order-export CSV columns** (the v1 default; fixed order is the contract):
`provider,register,variant,variable,representation,period,display_name` — one row per
spec binding. `representation` is its OWN column (not folded into `display_name`): a
custom display name would otherwise hide which delivery column the binding pinned, so
the data provider couldn't tell representations apart. `period` serializes via the
catalog `?period` wire form (range → `"<from>..<to>"`, snapshot → `"_default"`).
Pluggable per-steward `order_template`s are remaining — see `REFACTOR_SPEC.md`.

Remaining (not yet routes): `POST /api/kit` (kit-build — see `REFACTOR_SPEC.md`). Global
FTS search shipped as `GET /api/search` (#350); the docs library shipped as
`/api/docs/*` (#354).

## §16 input-validation gates (security boundary)

Two chokepoints reject hostile input **before** any DB lookup, each pinned by a
parametrized test asserting 422 **and zero SQL executed** (a SQLite trace hook counts
statements == 0):

- **`?period` canonicalization** (`period_param.py`) — the raw query is parsed into a
  typed `Period` against an allow-list of the canonical period forms before any reg_meta
  lookup. SQLi probes / traversal / NUL / URL-encoded slashes aren't period tokens, so
  they 422 and never touch SQL.
- **FQID route-segment validation** (`catalog_fqid.py`) — each `{fqid:path}` segment
  must match the slug grammar (or the leading `class` literal). The grammar excludes
  `.`, `..`, `%`, `\`, and any non-structural `/`, so canonical FQIDs cannot encode path
  traversal; Starlette URL-decodes first, so `%2e%2e` / `%2f` / `%00` fail the
  per-segment check. **`@version` is a 422, not a pin:** `scb/lisa/naringsgren@sni2007`
  is now an explicit *negative* case (the pin is retired), alongside
  `scb/lisa/naringsgren@bad/slug` and `…@@x`.

**Provenance confinement (route introspection).** No handler references the provenance
DB path — the route surface never exposes provenance, so there is no path-confinement to
enforce at the handler level. This is a property of the endpoint set, re-checked when
routes are added.

## Forward-looking open UX notes

These are unresolved UX questions, not built behavior — recorded so they aren't
re-discovered. The underlying data-layer lineage rationale lives in `reg_meta` /
`reg_meta_build` (the `variable_state_lineage` interval-overlap edges); these are purely
the authoring-UI presentation.

- **LISA composite-source presentation.** \~64% of LISA's variable slugs are sourced
  from RTB/RAMS/FastPak/IoT and carry inbound lineage edges. How the catalog UI surfaces
  that origin when a user authors a LISA variable list — hover tooltip, inline note,
  "see also" panel — is undecided. The data is present; the question is purely UX.
- **Per-steward repo autonomy.** v1 hosts every steward's config in this monorepo.
  Stewards versioning their own catalogs in their own repos (if IFAU/SWECOV ever run
  their own deployments) would reintroduce external-repo build wiring v1 sheds — not
  needed until a steward asks.
- **Realign-patch lifecycle** (gated behind the unbuilt merged-mode realign flow).
  Whether the realign-review UI writes an accepted patch back into git automatically or
  just produces a new `project_data.json` the user replaces manually.
