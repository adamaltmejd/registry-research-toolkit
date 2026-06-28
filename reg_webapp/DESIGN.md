# reg_webapp — design

FastAPI backend + Svelte SPA. The backend serves the reg_meta catalog read-only and the
project-authoring write surface (validate / order); the SPA is the researcher's
authoring client. This file records the package-local design rationale. Cross-cutting
topology (package tree, dependency graph, perf budgets, version policy, testing-strategy
overview) lives in the root `ARCHITECTURE.md`; remaining/unbuilt work lives in
`REFACTOR_SPEC.md`. The API contract itself is the committed `backend/openapi.json` (the
reference); `models.py` + the route handlers are the response-shape reference.

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
  **current/terminal** classifications only (via
  `reg_meta.queries.list_classifications`, no new Catalog method). A classification
  whose `superseded_by` is set — i.e. a successor edition exists — is dropped from the
  children list; superseded editions are reached via the leaf's edition-chain panel or a
  direct `class/<slug>` URL.

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

The suffixed surface has one family declared above the catch-all: seven **binding-suffix
routes** (`/states`, `/predecessors`, `/successors`, `/lineage`, `/lineage_warnings`,
`/dimensions`, `/graph`), each mapping 1:1 to a `Catalog` accessor and returning a thin
`{binding, <list>}` envelope so the SPA codegen sees one response type per endpoint.
(`/graph` returns a `RelationshipGraph` — no `{binding, …}` wrapper — so its shape
differs from the others, but the declaration position and slug-reservation rules are
identical.) The `/related` route and `variable_related_to` edge surface were retired in
#800. Plus one **register sub-resource** `/{provider}/{register}/variants` (a FIXED
3-seg shape with a literal `variants` tail — explicit `{provider}`/`{register}`
segments, NOT an `{fqid:path}` suffix). The binding-suffix routes are binding-only: a
non-binding FQID raises reg_meta's `not_a_binding_fqid` (EXIT_USAGE) → **422** (a usage
error, not a 500); an absent binding → 404. A register node's children include a
`variants` reference (`VariantsRef`) so the variant browser has a stable slot in the
discriminated union without the variant being an FQID.

Plus two **concept-group subject routes**, both declared above the catch-all:

- `GET /catalog/group/{provider}/{register}/{key}` (#617/#616) — a FIXED 4-seg shape
  with a literal `group` prefix and explicit `{provider}`/`{register}`/`{key}` segments,
  NOT an `{fqid:path}` suffix. Returns a `ConceptGroupNode` (`kind: "concept-group"`)
  with all group members, facets, and per-member coverage via `Catalog.concept_group`;
  404 on an unknown key or register. `?member=<slug>` is an optional focus hint echoed
  only when it names a real member — a bad hint is silently ignored, keeping the group
  page first-class.

- `GET /catalog/group/class/{key}` (#756) — a FIXED 3-seg shape (literal `group`,
  literal `class`, `{key}`), the **classification-umbrella sibling** of the
  register-scoped route above. Returns a `ClassificationGroupNode`
  (`kind: "classification-group"`) resolved via `Catalog.list_classification_groups()`
  filtered by key; 404 on an unknown key. Has **no** provider/register, **no**
  per-member coverage, and **no** `?member=` focus hint — classification umbrella groups
  are catalog-global, not scoped to a single register. This route is declared
  **immediately above** the register-group route so the literal `class` segment matches
  before `{provider}` is tried. A collision with the register-group route is
  unconstructable: `class` is not a valid provider slug (`Fqid.register_fqid` rejects
  it), so no real register-group URL can share this shape. Classification-root browse
  rows link to this route (via `classGroupHref`) and render through a dedicated
  `ClassificationGroupView` component.

The `group` literal **is** reserved in the **provider slot** of the FQID grammar
(`RESERVED_GROUP_SLUG`, see reg_meta/DESIGN.md → FQID grammar): because the
register-group route puts `group` at a non-leading position, a provider named `group`
would mint a binding-suffix URL `/catalog/group/<register>/<variable>/states` (5
segments) that the earlier-declared 5-segment group route would capture
(provider=`<register>`, register=`<variable>`, key=`states`) instead of the binding's
`/states`. Reserving `group` in the provider slot makes that collision unconstructable.

**Classification succession is embedded, not a sub-resource (#571/#578).** The
classification leaf node carries the **full edition chain** inline as `edition_chain`
(reg_meta's `ClassificationEdition`, embedded directly from
`Catalog.classification_chain`), so the browse panel renders the entire succession
timeline synchronously — no per-neighbor fetch. The server-side walk resolves a
`classification_same_as` alias to its canonical edition, then walks the QUERIED
edition's own path — forward to the terminal via the deterministic-first successor and
backward to the root via the deterministic-first predecessor — ordering it oldest→newest
BY TRAVERSAL (terminal/current last; the `effective_year` is display-only, so an
undated/NULL edge no longer inverts the order), and marks each edition
`is_current`/`is_self`. Anchoring on the queried path means a merge sibling on a
DIFFERENT inbound branch is never included (#588). Every edition is a live
`classification` row — the build validator guarantees succession editions are live (it
fails on any `classification_replaced_by` edge whose endpoint has no live row), so
`fqid` is None only on a malformed/unresolvable slug (rendered as plain text, not a
link). The earlier immediate-neighbor routes (`/classification_predecessors`,
`/classification_successors`) were retired — the embedded full chain subsumes them.
(reg_meta's `Catalog.classification_successors`/`classification_predecessors` accessors
remain as public API and back the chain walk.)

The classification leaf also embeds two further payloads inline for synchronous SPA
render (#609): `codes` (reg_meta's `ClassificationCode`, embedded directly from
`Catalog.classification_codes` — the resolved edition's value-set codes and labels, with
`is_valid` = canonical/observed/unknown; omitted when empty) and `dimensions`
(reg_meta's `ConceptGroupSummary`, embedded directly from
`Catalog.classification_dimensions` — the curated umbrella group(s) the edition belongs
to, reading `concept_group_classification`; omitted when empty). The SPA renders these
as a code/label panel (shared `CodeList` viewer — the same component used for the
variable value set, with a size-dependent filter: the search box appears only when the
set reaches the `CODE_FILTER_THRESHOLD`, hidden for small sets; #638) and a granularity
cross-reference panel respectively.

**Variable succession is embedded too (#582).** The binding leaf node carries the **full
variable succession chain** inline as `succession_chain` (reg_meta's `VariableEdition`,
embedded directly from `Catalog.variable_chain`) — the variable-grain dual of the
classification `edition_chain`. The server-side walk same_as-canonicalizes the queried
binding, then walks the QUERIED binding's own path over `variable_replaced_by` (forward
to the terminal via the deterministic-first successor, backward to the root via the
deterministic-first predecessor), ordering it oldest→newest BY TRAVERSAL
(terminal/current last; `effective_year` is display-only, robust to undated edges), and
marks each edition `is_current`/`is_self`; a merge sibling on a different inbound branch
is not included (#588). Each edition also carries the transition `reason` (the edge's
`beskrivning`) — unlike the classification grain, whose succession table has no reason
column. UNLIKE classifications, a chain edition may be a **dead/renamed predecessor**
with no live `variable` row — the #355/#411 renamed-slug model: variable succession
tolerates dead predecessors by design, and there is NO `variable_replaced_by` validator
forbidding it (the classification grain DOES have such a validator). A dead edition
still carries a syntactically-valid binding `fqid` so a citation 301-redirects to the
current edition, but its `name` is None (no live row); `fqid` is None only on a
malformed/unresolvable triple. On the corpus today all 12 edges are live, but the model
permits a dead predecessor. Also unlike classifications, embedding the chain does
**NOT** retire the immediate-neighbor routes: the `/predecessors` / `/successors`
sub-resources (and reg_meta's
`Catalog.predecessors`/`successors`/`ResolvedVariable.replaced_by`) stay — they back the
#411 permalink-redirect rails and are existing API surface. Only the binding node's
embedded `replaced_by` field is superseded by `succession_chain`.

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

**301 redirect for renamed/dead slugs (#355 PART 2; register grain added in #412;
`?period` and sub-endpoints added in #411; classification grain added in #571).** When a
request for a dead/renamed slug yields a genuine `fqid_not_found` 404, the route calls
`Catalog.resolve_terminal_successor` before surfacing the 404. That method dispatches on
FQID kind — binding FQIDs walk `variable_replaced_by`, register FQIDs walk
`register_replaced_by`, classification FQIDs walk `classification_replaced_by` — so this
single branch handles all grains with no kind-branching in the route. If the FQID has a
successor chain, the handler returns an HTTP 301 to the canonical `/api/catalog/<path>`
of the terminal successor (each path segment percent-encoded via `urllib.parse.quote`).
A truly-unknown slug — no successor edge, or a PROVIDER FQID — re-raises the original
404 unchanged.

The redirect covers all entry points into a dead binding slug (#411): the no-period
catch-all node path, the `?period` branch (query string preserved, so `?period=2019` /
`?variant` ride to the terminal), and all seven binding-suffix sub-endpoints (`/states`,
`/predecessors`, `/successors`, `/lineage`, `/lineage_warnings`, `/dimensions`,
`/graph`), which redirect to the **same suffix** on the terminal (e.g. a dead slug
`/states` → terminal slug `/states`). The shared `_redirect_or_4xx` helper implements
this policy for the `?period` branch and all sub-endpoints; the no-period node path has
a sibling implementation at the `HTTPException` layer (keep the two in sync on any
301→308 switch). Only a genuine `fqid_not_found` ever redirects — a usage 422 (e.g. an
inverted `?period` range) and a build-invariant 500 are never turned into redirects. The
301 is permanent and cache-eligible; the terminal resolution guarantees the redirect
target stays stable under double renames (see
`reg_meta/DESIGN.md → resolve_terminal_successor`).

A dead/renamed **classification** slug still redirects via the catch-all node path: a
`fqid_not_found` 404 on a classification FQID walks `classification_replaced_by` through
`resolve_terminal_successor` (added in #571) to 301 to the terminal edition, same as the
binding/register grains. (`classification_chain` itself tolerates dead slugs internally
— the embedded chain renders even an old/retired edition's full timeline — but a
citation of a slug with no live row AND no successor edge still 404s.)

**Concept groups (#303).** The register and classification-root responses carry a
`groups` list (reg_meta's `ConceptGroupSummary`, embedded directly — see
reg_meta/DESIGN.md → Concept groups) ALONGSIDE the complete flat `children` list:
grouped members appear in both, so the contract stays additive and group-unaware
consumers keep working. The SPA folds client-side (`catalog.ts::foldGroupedRows`):
grouped leaves hide under one expandable `ConceptGroupRow` (a month×rank value matrix
for two facet axes, chips for faceted members — months/vintages in single-axis variable
groups, curated labels in axis-less classification umbrellas — and a plain member list
for edge groups), ungrouped leaves render as before, and the type-to-filter matches a
group on its label/key OR any member's name/FQID (`groupMatchesFilter`) so member
searches still surface the folded group. The CatalogPicker's variable list folds the
same way (#322): `ConceptGroupRow` takes an optional `onpick` that renders members as
pick buttons instead of catalogHref links, and the picker's `rankFilter` ranks a group
row on `groupFilterKeys` (the shared match set behind `groupMatchesFilter`); a picked
member rides the same derive-on-pick path as a leaf row. `foldGroupedRows` tolerates a
stale pre-`groups` edge-cached payload (#317) by degrading to the flat list.

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

`GET /api/search?q=&limit=&type=` (#350) is the discovery surface consumed by the global
header omnibox (`SearchOmnibox.svelte`, shipped in this PR). It returns **typed result
groups** over the shipped FTS5 indexes, reusing reg_meta's concept-group-folded `search`
(`reg_meta.queries.search`, #322) — the webapp does NOT reimplement folding or FTS.
`?type=` (#393) scopes the search to ONE group: `all` (the default, or omitted)
preserves the fixed-order four-group response; any single type (`register` / `variable`
/ `classification` / `value`) runs AND emits only that one group. An unknown value 422s
at the boundary (the valid set mirrors reg_meta's `SEARCH_TYPES`). For a FILTERED
steward, the register and variable surfaces are further scoped to the steward's held
FQIDs — see § Steward layering → Browse and search scoping (#859) above; classification
and code surfaces are catalog-global and unaffected.

The SPA surface: a global `<SearchOmnibox>` in the app header routes to a shareable
`/search?q=` results page (`SearchView.svelte`) that renders the four typed groups with
navigation to catalog nodes. The router gained `search` and `doc` routes (query lives in
`?q=`, keyed on pathname so the page re-runs on every query change) and a
`router.replace()` method (mirrors the `?period` URL-as-single-source-of-truth pattern:
the omnibox syncs back to the URL, and the URL drives the view). `api.ts` gained
`search(q, {limit?, type?})` typed off the codegen'd contract. `SearchView` renders an
"All · Registers · Variables · Classifications · Codes" scope toggle backed by `?type=`
(URL state, like `?q=`/`?period`; `all` is omitted from the canonical URL); the omnibox
preserves an active scope when re-querying. The **docs group** ships as a 5th sibling
group in `SearchView.svelte` (#394): it calls the DEDICATED `/api/docs/search` endpoint
via a SECOND, independent `asyncResource` — failure-isolated from the four main groups
(a docs failure, an absent index returning `ingested:false`, or an empty result silently
omits the docs section and never blanks the main groups). The `/doc/<filename>` route
(router `Route` union arm `{name:"doc",identifier}`) renders `DocView.svelte`: title,
register/variable/tags, a `source_url` link to the SCB source PDF (resolved from the
curated map at doc-DB build, #372; None when uncurated) with `source_title` as label,
and a bounded `excerpt`; 404 distinguishes "not ingested" vs "not found";
`snippet`/`excerpt` are rendered as TEXT, never `{@html}`, and the full converted body
is never fetched.

**The response contract is the point — designed to extend.** The body is
`{kind, query, groups: SearchGroup[]}`; each `SearchGroup` is a discriminated arm
(`group` literal) carrying its own `total_count` + typed `results`. Today: `registers`,
`variables` (leaf hits ⧺ folded concept groups), `classifications` (leaf hits ⧺ folded
classification-succession rows (`type: "classification_succession"`, #571 — a query that
hits ≥2 editions of the same chain collapses to one
`ClassificationSuccessionSearchResult` keyed on the terminal edition, carrying the full
`editions` chain and `matched_count`) ⧺ folded umbrella concept-group rows
(`type: "group"`, #516 — e.g. `group:sun`)), and `codes` (#352 — value-label hits
annotated with their owning variables/classifications). **Docs (#354) join as a NEW arm
of the `SearchGroup` union + new result models — existing groups are never reshaped.**
The SPA must tolerate an unknown `group` value (skip it) so a new group can ship before
the SPA renders it (the same payload-skew tolerance the `?period` additive fields rely
on). Each result carries its navigable `fqid` and a `rank: float` (the FTS rank the
CLI's doc-merge interleaves by, #701); results within a group are pre-sorted by FTS rank
before grouping, so the SPA may ignore `rank` — it is present on the wire as the shared
sort key.

- **One reg_meta call per group**: register/variable/classification via the FTS
  `field="description"` path; **codes (#352) via the `field="value", type="value"`
  path** (`value_code_fts` label match + code-shape exact/prefix on `value_code.code`,
  ranked bm25 + rarity-downweight, owner-annotated — see reg_meta DESIGN.md → FTS5
  configuration). Each group gets its own `total_count` + per-group `limit`; codes don't
  fold into concept groups (`fold_groups=False`). The codes page is then re-ranked
  (#393) so classification-backed (curated) codes lead, then by `classification_count`,
  then `variable_count` — but only WITHIN the FTS-top-N page reg_meta already annotated,
  so it can't pull a curated code that ranked below the FTS cutoff into view. Each
  `CodeSearchResult` carries `code_system` (the primary owning classification's
  `short_name`, else null); the SPA renders the codes group in per-code-system
  subsections, register-local/bespoke (null) codes last.
- **Input gates** (`query_input.validate_text_query` / `_validated_limit` /
  `_has_searchable_token`): a query is length-capped (422 over 200 chars) and
  NUL-rejected (422); `limit` is clamped to \[1, 50\] (not 422'd). A blank / whitespace
  / punctuation-only query returns ALL groups EMPTY (200, not 422) — it never reaches
  reg_meta (whose LIKE label-fold would otherwise turn `%%` into a match-everything).
  FTS-operator neutralization + prefix-matching + diacritic folding all live in reg_meta
  (`_fts_match_query`); the webapp passes the raw query through. The query reaches FTS
  only as a bound parameter (no SQLi surface), so the gates guard cost/abuse, not
  injection.
- **Golden-boost** (`golden.apply_golden_boost`, #393 item 4 / #311): a curated-pin
  INJECTION (no longer the old no-op seam). For an exact (normalized: diacritic-fold +
  casefold + strip — so `sysselsattning` matches the `sysselsättning` pin, consistent
  with FTS unicode61 folding) query, a steward pin
  (`reg_webapp/backend/src/reg_webapp/search_golden.toml`, packaged inside reg_webapp so
  it ships with the runtime image) prepends a canonical result to the TOP of its group
  even when FTS would not surface it — e.g. `sysselsättning` → `scb/lisa` (RAMS is stale
  → BAS; steer to LISA) and `diagnos` → `sos/par` (Patientregistret), both registers
  that don't rank for those terms today. It operates on reg_meta's typed search models
  (the `SearchResult` union, #701) so the route AND the eval runner
  (`scripts/run_search_eval.py`) apply the SAME function — that's what makes the eval
  measure the route's TRUE behavior. Pins dedup by `fqid` (a pin already an FTS hit
  injects nothing), and a group's `total_count` adds the count of net-new injected
  results. `register` + `classification` pins are implemented (resolve cheaply by slug);
  a `variable`/`value` pin is a config error at LOAD (fail fast). The TOML is parsed +
  validated once at import; a typo'd fqid raises at apply (never silently drops). Eval
  gaps the pins close are flipped to `expect = "hit"` in `search_eval.toml` (SUN remains
  the lone gap — a concept-group modeling issue, not a golden-boost one).
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
  source-document identifier; `source_url` is the resolved SCB PDF link (populated at
  doc-DB build from the curated `doc_sources.toml` map, #372), None when the source is
  uncurated; `source_title` is the human-readable publication title (also None when
  uncurated). Coverage is LISA-only today.
- **Coverage distinction encoded in the response**: coverage is LISA-only today.
  `ingested` is False when the docs index is absent entirely; the variable hook's
  `register_ingested` is False when *that register* has no ingested docs. The flag
  distinction is real and preserved in the response — a caller can tell "no docs DB"
  from "this register has no docs" — but the SPA omits the panel entirely in all empty
  cases rather than rendering per-state copy (see `DocMentionsPanel` below). The
  variable hook's results are flagged `fuzzy` (a name/provider_key text match, not an
  authoritative variable→doc link).
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
  empty, and never able to blank the four main groups. The SPA shows the docs group ONLY
  under the unscoped (`all`) view (#393): the scope toggle has no Docs option, and a
  non-`all` `?type=` short-circuits the docs fetch so a scoped search shows just its one
  group. The reserved `docs` `SearchGroup` arm stays unused; the separation is the right
  call given the optional-DB / degradation rationale above. The `/api/docs/for-variable`
  leaf hook has its own SPA consumer (#402): `BindingLeafView.svelte` renders a
  `DocMentionsPanel` sibling of the lineage panels, firing a SEPARATE independent
  `asyncResource` at `/api/docs/for-variable` — a distinct failure domain (a docs error,
  timeout, or absent index never blanks the leaf). The panel omits the entire section
  when the response is empty in any sense (`ingested:false`, `register_ingested:false`,
  or zero results), mirroring the omit-when-empty behaviour of `HistoryGraph` and
  `LineageDetails` (#612); loading and error states still render inline, so an in-flight
  or errored fetch never reads as a confirmed absence. When results are present, fuzzy
  hits are labelled as such; each hit links to the `/doc/<filename>` viewer and renders
  the FTS snippet via a safe inline-emphasis subset (`**…**` → `<mark>` for matched-term
  highlight, `*…*`/`_…_` → `<em>`) through auto-escaped Svelte interpolation — never
  `{@html}`, still excerpt-only.
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

## Catalog stats (`routes/stats.py`, #675)

`GET /api/stats` returns the headline catalog-size counts
(`{providers, registers, variables}`) the landing page renders. It is a **TOP-LEVEL**
route (a sibling of `/api/context`), deliberately NOT under `/api/catalog`: that prefix
is the `{fqid:path}` catch-all, so a `/api/catalog/stats` would be swallowed by the
catch-all (or need a reserved-slug carve-out + above-the-catch-all declaration).

The `global` deployment (no steward filter) uses reg_meta's `Catalog.catalog_sizes()`,
opened through the SAME per-request `catalog_conn` seam (`conn.py`) the catalog routes
use. Those are full-universe, browse-addressable counts: slugged providers, slugged
registers, and slugged variables under slugged registers. A FILTERED steward uses the
boot-time in-memory `CatalogIndex` instead, so the landing-page stats reflect only that
steward's catalog. The index is column-based for admission, so stats de-dupe variables
by binding FQID rather than resolved delivery column; registers come from valid steward
sources plus any kept binding's parent register. Drift-dropped bindings do not inflate
the variable count.

ETag + Cache-Control ride the generic `ETagMiddleware` (a GET read). `/api/stats` uses
the short `public, max-age=60, must-revalidate` tier: for a filtered deployment the body
depends on `steward.project_data.json`, so a same-id steward catalog redeploy must get a
prompt revalidation opportunity instead of letting the browser serve a stale count for
24h.

## ETag / Cache-Control (`etag.py` + `middleware.py`)

Every read endpoint (`/api/context`, `/api/stats`, the `/api/catalog` root + catch-all,
the 7 binding-suffix sub-endpoints) carries
`ETag: "<reg_meta_version>-<steward_id>-<sha256(body)[:16]>"` and a per-route
`Cache-Control` (`cache_control_for`) in three tiers: `/api/context` revalidates always
(see below); fold- or steward-dependent reads (`/api/catalog/*`, `/api/search`, and
`/api/stats`) keep `public, max-age=60, must-revalidate`; rebuild-stable doc-library
reads (`/api/docs/*`) keep `public, max-age=86400, must-revalidate`. A matching
`If-None-Match` yields a **304** with no body. The pure logic lives in `etag.py`
(`compute_etag` + `etag_matches` + `cache_control_for`); an ASGI middleware
(`ETagMiddleware`) wires it DRY onto every GET read response.

- **`reg_meta_version`** is the INSTALLED `reg_meta.__version__` (the v1.x Model A
  package release), NOT the DB `schema_version` manifest. `steward_id` is
  `app.state.steward.id`.
- **The body-hash** makes `If-None-Match` per-URL coherent — the `?period` / `?variant`
  query is part of the URL, so it's already part of the cache key (different periods are
  different ETags).
- **`/api/context` revalidates always** (`Cache-Control: no-cache`, in
  `REVALIDATE_ALWAYS_PATHS`): the SPA vintage footer reads it to assert a specific
  deploy version/date, so a sub-24h-stale copy would *visibly lie* right after a deploy.
  The ETag keeps revalidation cheap — a 304 when nothing changed, a fresh 200 the moment
  a deploy bumps the version. Catalog and search endpoints use `max-age=60` because both
  embed the #322 concept-group folds (which change without a rebuild/deploy) and a
  sub-minute-stale fold set would surface the wrong grouping for a returning user whose
  browser holds the unversioned copy; `/api/stats` shares that short tier because a
  filtered steward's counts depend on the steward catalog index, which can change on a
  same-id redeploy. The body-hash ETag keeps revalidation a cheap 304 when nothing
  changed, and `public` keeps the CF edge cacheable (the #220 probe survives). Only
  `/api/docs/*` keeps `max-age=86400` — doc-library content is rebuild-stable and a
  sub-day-stale list is acceptable there; the ETag still guarantees correctness on
  revalidation. The edge worker (`reg_webapp/edge/`) defers to this origin's
  `Cache-Control` contract (it only stamps the `__edge_v` cache-generation param,
  orthogonal to caching policy), so the per-route policy needs no edge change.
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

**Browse and search scoping (#859).** The `CatalogIndex` now also scopes the **catalog
browse** (`/api/catalog/*`) and **search** (`/api/search`) discovery surfaces for a
filtered steward — previously it gated only validate/authoring/stats/context.

*Browse — column-grain faithful (#206).* The catalog root shows only held providers; a
provider node shows only held registers; a register node shows only held bindings
(filtered by `admitted_variable_fqids`) with concept-group members narrowed to held
(representation members via column-grain `admits`; whole-variable members via bare-FQID
membership in `admitted_variable_fqids`; a group with no surviving member is dropped). A
held binding leaf narrows its embedded `states` to held delivery columns
(`held_columns`), and the `?period` / `/states` resolve_at subset is narrowed the same
way. The `/variants` sub-resource filters to variant coordinates with ≥1 held binding
(`held_variant_coords_for_register`). All seven binding-suffix sub-endpoints (`/states`,
`/predecessors`, `/successors`, `/dimensions`, `/graph`, `/lineage`,
`/lineage_warnings`) apply the ONE pre-resolve admission gate (`_require_admitted`) that
covers binding, register, and provider grains uniformly:

- a LIVE entity the steward does not hold → **404** ("not in this steward's catalog");
- an UNADMITTED but dead/renamed slug whose terminal successor IS held → **301** to that
  terminal (query string and sub-endpoint suffix preserved, mirroring the global
  dead-slug redirect — a live unheld entity NEVER redirects, because succession edges
  exist between live entities and a blind terminal walk would mis-redirect to an unheld
  successor);
- a dead slug whose terminal successor is UNHELD or has no successor → **404**.

The `/graph` sub-endpoint gates the subject binding but does NOT narrow the graph's node
set (same_as/group neighborhood) to held — that traversal-narrowing is deferred as a
follow-up.

*Classification pass-through (decision 2).* Classifications and codes are
catalog-global. A steward `project_data` holds only variable bindings, so there is no
holdings basis to scope reference data. Classification routes (`class/…`) and the codes
arm of search pass through unfiltered for all steward deployments.

*Search.* `/api/search` passes `admitted_variable_fqids | held_register_fqids` as the
`fqids` allow-list to `reg_meta.queries.search`. This restricts register and variable
rows query-time (so `total_count` and paging are exact, including concept-group folding
— a group surfaces only if ≥1 member is held, and its member list is narrowed to held
members). Classification and value/code surfaces are unaffected. The golden-boost
injection (`golden.apply_golden_boost`) is re-filtered for the same set after boost so a
curated pin the steward does not hold is dropped. The `global` deployment (no index) is
byte-for-byte unchanged.

*Performance.* The derived projections (`admitted_variable_fqids`,
`held_register_fqids`, `held_provider_slugs`, `_admitted_pairs`,
`_held_columns_by_fqid`, `_variant_coords_by_register`) are `functools.cached_property`:
each is computed from `bindings_by_variant` on first access and memoized for the process
lifetime. `cached_property` coexists with `@dataclass(frozen=True)` because the value is
written into `__dict__` (no `__slots__`), bypassing the frozen `__setattr__`; the
generated `__hash__` / `__eq__` read declared fields only.

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
doesn't exist there). A real filtered steward catalog now ships:
`stewards/swecov/steward.project_data.json` (column-based admission against the flavored
reg_meta DB; see `stewards/swecov/README.md` for provenance and coverage). Remaining:
deploy wiring for the swecov hostname, the SPA catalog-authoring mode, a
`reg-meta-build steward-diff` CLI, and per-steward `extensions` — see
`REFACTOR_SPEC.md`.

## Pydantic boundary

reg_webapp defines its **own** webapp-local Pydantic response models (`models.py`) for
the catalog surface. As of #681 PR2 (2026-06-22), the webapp's `kind`-discriminated node
models (`ProviderNode`, `RegisterNode`, `BindingNode`, `ClassificationNode`, the
`*Response` composites, `ConceptGroupNode`, and the sub-endpoint envelopes) **embed
reg_meta's frozen Pydantic leaf models directly** as field types — no per-leaf 1:1
wrappers or mapper functions. The 16 per-leaf wrappers shipped before PR2 are deleted.
The node models are **not** 1:1 wrappers: they carry the `kind` discriminator plus
server-computed enrichment (`succession_chain`, per-member `coverage`, `via_same_as`)
that has no counterpart in reg_meta. For `project_data`-related responses
(`/api/project/*`) the webapp uses **`reg_schema` Pydantic models directly** — no
wrapper layer, eliminating that drift surface. The **only** remaining 1:1 Pydantic
wrapper is `ValidationResult`/`ValidationIssue` (reg_schema is a frozen dataclass
consumed cross-runtime by the SPA, so the webapp wraps it 1:1 there).

Each node model carries a `kind` `Literal` discriminator (`provider` / `register` /
`binding` / `classification` / `classification-root` / `root` / `variants-ref` /
`concept-group`). The catch-all (`GET /api/catalog/{fqid:path}`) returns a Pydantic
discriminated union (`Field(discriminator="kind")`) over the five kinds it owns:
`provider` / `register` / `binding` / `classification` / `classification-root`.
`concept-group` is **not** a catch-all arm — it is the sole response type of the
fixed-shape route `GET /api/catalog/group/{provider}/{register}/{key}` (declared above
the catch-all; see § Routing above). The discriminated union drives `openapi-typescript`
to emit a clean tagged union for the catch-all; `ConceptGroupNode` is a standalone
schema used only by the group route. FQID fields serialize as plain `str` (`str(fqid)`),
never nested models, so the codegen'd TS sees flat string fields. The binding **leaf**
embeds the variable's FULL longitudinal record from one `Catalog.resolve` call (states,
value sets, and the variable-grain `same_as` / `lineage` edges), plus the full variable
`succession_chain` (#582, below). `lineage_warnings` are **omitted** —
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

**UI behavior layer: Bits UI** (`bits-ui@2.18.1`, the Svelte-5-runes major) is the
sanctioned headless-primitives library for a11y-critical widgets — comboboxes, menus,
dialogs. It provides behavior + ARIA with no bundled styles; the app's scoped CSS and
design-token set supply all visual styling. Stop hand-rolling widgets that Bits UI
covers — #632 (a hand-extracted slider that pre-dated this decision) is the canonical
motivating example. See the § UI primitives section below for the adoption rationale
(#689).

## UI primitives — Bits UI + scoped CSS (bake-off #689)

A two-arm spike ahead of the #664/#665/#666 redesign wave evaluated what the frontend
should use as its UI foundation. The question was purely about styling strategy — **both
arms sat on Bits UI** for accessible behavior (shadcn-svelte = Bits UI + Tailwind v4),
so the a11y win was not a differentiator.

**Arm A** — Bits UI + Svelte scoped CSS + a CSS-custom-property design-token set. **Arm
B** — Bits UI via shadcn-svelte + Tailwind v4.

Both rebuilt `SearchOmnibox.svelte` as an accessible `Combobox`. The verdict: **adopt
Arm A.**

### Evidence

**Footprint.** Arm A: 1 new runtime dep (`bits-ui@2.18.1`) + a small token block in
`App.svelte`. Arm B: 8 deps (`tailwindcss`, `@tailwindcss/vite`, `bits-ui`, `clsx`,
`tailwind-merge`, `tailwind-variants`, `@lucide/svelte`, `tw-animate-css`) + \~1061 LOC
of vendored `ui/**` components to own + a second, parallel token system (shadcn's oklch
palette, disconnected from the app's existing `--border`/`--accent`/`--surface` tokens).

**Bundle.** JS was roughly a wash (\~+25–32 kB gzip each arm, dominated by the shared
Bits UI runtime). The distinguishing cost was CSS: Arm A near-flat (+0.3 kB gzip); Arm
B's Tailwind utility layer more than doubled CSS (+7.3 kB gzip).

**Tooling friction (Arm B only).** shadcn-svelte's CLI assumes SvelteKit — its `init`
cannot run non-interactively on this plain-Vite SPA (needed a manual `$lib` alias +
manual theme transcription + undeclared peer deps). Biome rejected Tailwind v4
directives and the vendored files violated repo lint conventions; the vendor directory
had to be excluded from lint entirely. No such friction on Arm A.

**The telling detail.** Even in Arm B, the integration-correct choice was to style the
omnibox with the existing app tokens, not Tailwind's palette — shadcn's oklch set is a
disconnected parallel system that doesn't match the app's design language. The Tailwind
arm didn't actually want Tailwind for the part that mattered.

**Conclusion.** For a \~35-component bespoke app with its own design language, Bits UI
captures the entire accessibility goal. Tailwind/shadcn's added cost — deps, vendored
ownership, dual token systems, CSS growth, persistent tooling and lint friction — is
disproportionate. This is independent of the #679 MONA-austerity loosening; the frontend
was never MONA-constrained — the adoption gap was purely historical.

### What shipped

- `bits-ui@2.18.1` added to `frontend/package.json` as the single new runtime dep.
- `App.svelte` `:global(:root)` extended with a purposeful design-token set —
  `--space-1`/`2`/`3`/`4`, `--text-sm`/`--text-base`, `--radius`, `--focus-ring`,
  `--surface-hover`, `--surface-selected` — alongside the pre-existing palette
  (`--border`, `--muted`, `--accent`, `--accent-bg`, `--surface`). These are the
  geometry, type, and interaction-state tokens the bare palette lacked; the existing
  color palette stays the source of truth. (Historical: this #689 stub was wholesale
  superseded by #802's two-layer role system — see § Token architecture below — so
  `--muted`/`--text-base` are no longer current token names; the alias bridge that
  carried them onto the new roles was deleted in #827.)
- `SearchOmnibox.svelte` was rebuilt by #689 as a **Bits UI `Combobox`** with live
  `/api/search` suggestions. **Superseded by #808**: the suggestion dropdown was removed
  — it duplicated the `/search` results page, and the omnibox auto-routes every query to
  `/search` anyway, so the popup only ever flashed. `SearchOmnibox` is now a plain
  routing input and the `/search` results page is the single search surface. The
  `/search?q=` routing + URL↔box sync it established are preserved: Enter routes to
  `/search` from other pages, and typing live-refines in place while already on
  `/search` (no debounced navigation off-route).
- `CatalogPicker.svelte` filtered lists (variant / provider / register / leaf variables)
  migrated to **Bits UI `Command`**: single tab-stop, arrow-key nav, `role="listbox"` /
  `role="option"` ARIA. `rankFilter` stays the single source of truth (`Command`'s own
  scorer is disabled via `shouldFilter={false}`).

### CatalogPicker / Command friction (relevant to #664/#666)

`Command` fits the **homogeneous** filtered lists (variant / provider / register) with
zero friction. The **folded `ConceptGroupRow`** (a nested expandable widget) cannot be a
flat `role="option"` — it is rendered as `role="presentation"` inside the listbox, which
**splits the keyboard model**: arrow-nav covers the leaf options; Tab reaches the group
expanders. The picker's most important list (740 grouped variables) therefore gets only
partial keyboard nav from the primitive. Worth knowing going into the #664 subject-page
and #666 history-graph work.

### Rollout

Incremental — no big-bang rewrite. New redesign UI (#664/#665/#666) builds on Bits UI +
the token set; high-risk existing hand-rolled widgets migrate opportunistically as the
redesign is the natural migration window.

## Visual language (design system)

#689 chose the **foundation** (Bits UI behavior + Svelte scoped CSS +
CSS-custom-property tokens, no Tailwind). This section defines the **language** that
foundation expresses — so every migrated page and every new redesign-wave component
converges on one look instead of re-deciding type, color, and density per `<style>`
block. It is the source of truth the token file and the primitive set implement.

**Committed direction (decided 2026-06-26).** A *modern data-tool / dashboard* aesthetic
— the Linear/Observable lineage: an app shell, dense legible tables, keyboard-first
interaction, subtle surfaces, one confident accent. *Confidently branded* (a clear point
of view in service of the researcher's task, not maximalist decoration) and
*light-first, dark-ready* (tokens authored as semantic roles so dark mode is a later
additive theme, never a retrofit). The audience is academic researchers and data
stewards working dense Swedish register metadata; the design optimizes for clarity and
information density over editorial flourish.

This supersedes the MVP look: `system-ui` everywhere, a single `#2563eb` accent on flat
white, a 56rem centered ribbon, and 35 components each hand-styling cards/panels/tables
in private `<style>` blocks off a \~6-color token stub.

### Token architecture

Two layers, semantic roles only consumed by components:

- **Primitive ramps** — raw scales: `--gray-1..12` (graphite neutrals), `--rost-1..12`
  (brand), and the categorical/semantic hues. Components **never** reference these
  directly.
- **Semantic roles** — what components use: `--bg`, `--surface`, `--surface-raised`,
  `--surface-sunken`, `--text`, `--text-muted`, `--text-faint`, `--border`,
  `--border-strong`, `--accent`, `--accent-fg`, `--accent-bg`, `--accent-ink`, the
  categorical-ink family `--cat-reg-ink` / `--cat-var-ink` / `--cat-code-ink` /
  `--cat-class-ink` / `--cat-group-ink` (AA-cleared text stops — see § Color), plus the
  status roles below. Dark mode is a single `[data-theme="dark"]` block that remaps
  these roles to dark primitive stops — no component CSS changes.

The tokens move out of `App.svelte`'s `:global(:root)` (the #689 spike stub) into a
global `frontend/src/tokens.css` imported once in `main.ts`. The stub's
geometry/interaction tokens (`--space-*`, `--radius`, `--focus-ring`,
`--surface-hover`/`-selected`) fold into the role set; the bare palette
(`--border`/`--accent`/`--surface`) is replaced, not kept in parallel.

`main.ts` is the SPA entrypoint, but the `*.browser.test.ts` suite renders components
directly via `vitest-browser-svelte` and does **not** evaluate `main.ts` — so
`tokens.css` must **also** be imported from the Vitest browser setup (the `setupFiles`
path in `vite.config.ts`'s `browser` project). Otherwise token-dependent component
styling runs without the design-system variables/fonts and either breaks or silently
skips visual regressions.

### Typography

Self-hosted (woff2 in the bundle — no third-party CDN dependency in a research tool;
both faces are OFL):

- **Schibsted Grotesk** — UI + display. A Scandinavian media-house grotesque:
  domain-authentic, characterful, and distinct from the `Inter`/`Roboto`/`Space Grotesk`
  defaults the frontend-design guidance and #689 both warn against.
- **IBM Plex Mono** — every code, FQID, slug, year, and value-set code. The catalog is
  full of machine identifiers; they get a mono face consistently (the MVP already
  half-did this).

Type scale as roles (`--text-display`, `--text-h1..h3`, `--text-body`, `--text-sm`,
`--text-micro`) with a tracked, uppercase **micro-label** style for table headers and
section eyebrows — the device that gives the dashboard look its hierarchy without heavy
headings.

### Color

Graphite ink on a warm off-white (`--bg` \~`#fafaf8`, surfaces toward white). Brand
accent is **Rost `#B8552A`** (a Falu-red-adjacent warm rust).

**The accent-vs-status rule (load-bearing — a warm brand forces it):** the brand accent
paints **only interactive chrome** — links, primary buttons, selection, focus ring,
active nav. It is **never** a status color. Every validation/status state is (a)
chromatically *cooler* than the brand so it can't be mistaken for it, and (b) paired
with a **glyph**, never hue alone:

- error → cherry red `--err` `#C42B2B` (✕)
- warning → cool ochre `--warn` `#7A5C00` (▲) — deliberately yellower/cooler than rost
- info → slate `--info` `#3A6B8C` (i)
- success → green `--ok` `#1E7A3C`

These are the **text/glyph foreground** values: each clears WCAG AA (≥4.5:1) on the
off-white/white surfaces, since status appears as small labels and glyphs. The lighter
fill/badge tints (the soft backgrounds behind a status row) are separate ramp stops, not
these foregrounds — never use a fill tint as text color.

A separate small **categorical** palette tags result/node *type* (`REG` teal, `VAR`
indigo, `CODE` gold, classification, group) in search and listings. It is its own
sub-system — never reuse the brand accent or the status colors for type identity, or
"this is a variable" and "this is selected/an error" collide. The raw `--cat-*` hues are
the **fill and border** values (the 10 % tint backgrounds). For TEXT, each hue has a
corresponding `--cat-*-ink` stop — `color-mix(in srgb, var(--cat-*) 85%, black)` — that
darkens it enough to clear WCAG AA on those tint backgrounds (teal and gold fall just
under 4.5:1 as plain text on their own tint). This mirrors `--accent-ink` for the brand.
Components always use the ink stop for any categorical label text; fill/border use the
raw hue.

### Geometry, elevation, motion

- `--space-*` rhythm (0.25rem base), `--radius` (`--radius-sm` \~5px controls, \~8px
  panels).
- A small **elevation** scale: flat sunken surfaces, a hairline `--border`, and a single
  soft shadow token for raised panels/cards/popovers. No heavy drop shadows.
- `--focus-ring` keeps the #689 accessible-outline intent, retinted to the brand.
- Motion budget is small and functional (popover/disclosure transitions, \~120–180ms),
  not decorative — consistent with the data-tool restraint.

### Surface & layout language

- **App shell** replaces the centered 56rem ribbon: a persistent left **rail** (brand,
  primary nav, contextual facets — keeps all 8 providers reachable), a **topbar**
  (breadcrumb + a command bar promoting the existing `SearchOmnibox`), and a wide
  content canvas. The command-bar shortcut is platform-adaptive — `Meta/Ctrl+K`,
  displayed as `⌘K` on macOS and `Ctrl+K` elsewhere (never bind/label Mac-only). This
  fixes the "cramped *and* empty" failure mode of the narrow column on dense pages.
- **Panels** are the unit of grouping: a header (micro-label title + optional
  meta/badge) over a body. **DataTable** is the workhorse — uppercase micro-label
  headers, right-aligned mono numerics, zebra-free hairline rows, hover +
  keyboard-selected states.
- Density is tuned for scanning long lists (registers, variables, value-set codes,
  search results), not for marketing whitespace.

### Shared primitives (Bits UI behavior + scoped CSS)

The recurring visual units live in `frontend/src/lib/ui/` (shipped in #804, exported via
`index.ts`): `Panel`, `DataTable`, `Breadcrumbs`, `Tag`, `Button`, `KeyValue`,
`Skeleton`, and `EmptyState`. `AppShell` shipped in #803 (rail + topbar command bar —
see § Surface & layout language). Behavior comes from Bits UI (`Command`, `Dialog`,
`Combobox`, etc.); styling is scoped CSS reading **only** semantic tokens. The #689
`CatalogPicker` `Command` keyboard-split caveat (folded `ConceptGroupRow` →
`role="presentation"`) still stands and is relevant where these primitives meet grouped
lists.

Load-bearing decisions downstream children (#806–#809) must not re-litigate:

- **`DataTable` ARIA roles — explicit and unconditional.** Every table element carries
  its ARIA role explicitly (`table`/`grid`, `rowgroup`, `row`, `columnheader`,
  `cell`/`gridcell`) regardless of the selectable variant. This is required because the
  responsive stacked form switches `display` to `block`, which strips native table roles
  in Firefox/Safari — explicit roles keep the semantics intact across that change.
- **`DataTable` selection — ARIA grid, not roving tabindex.** The selectable variant
  sets `role="grid"` on the table; each selectable row carries `aria-selected` and
  `tabindex=0` (its own tab stop). This is deliberately **not** a single-tab-stop
  roving-tabindex grid — list keyboard navigation belongs to Bits UI `Command`
  elsewhere. API: `getRowId` + `selectedId` + `onselect`; omit them for a plain static
  table (`role="table"`).
- **`DataTable` responsive stacking (≤48 rem).** At narrow widths each `<tr>` becomes a
  bordered card; cells stack vertically. The first column is the primary title (no
  micro-label prefix); non-primary cells show their column label as a CSS `::before`
  prefix via `data-label`. Empty cells suppress the prefix via `td:empty::before`. The
  visual-header row is visually hidden (clip, not `display:none`) so `columnheader`
  roles stay in the accessibility tree. Consumers drop per-call
  `overflow-wrap`/`hyphens` — the primitive owns graceful long-word breaking (excluded
  for mono/numeric cells). The primary column gets a `min-width: 12rem` floor on the
  `<th>` (under `table-layout:auto` this sizes the whole column track); an explicit
  `Column.width` override wins via `th.first:not([style*="width"])`.
- **`Tag` `tone` spans three disjoint sub-systems.** Chrome tones (`neutral`/`accent`),
  categorical TYPE tones (`reg`/`var`/`code`/`class`/`group`), and status tones
  (`error`/`warn`/`info`/`ok`). Categorical TYPE tags use the raw `--cat-*` hue as the
  fill/border and `--cat-*-ink` (the 85 % dark mix) as the label text, so every type
  label clears AA without per-component overrides. Status tones **require** a leading
  `glyph` snippet (the accent-vs-status rule: hue alone is never sufficient); the glyph
  is `aria-hidden`, so status meaning must also appear in the label text.
- **Focus-ring convention.** Every interactive primitive applies
  `:focus-visible { box-shadow: var(--focus-ring) }` in its own scoped CSS — no global
  stylesheet owns this.
- **`.ui-btn` global hook.** `Button` delegates element rendering to Bits UI, so its
  variant/size styles are `:global(.ui-btn …)` — scoped through the `ui-btn` namespace
  this component owns, not a generic `.btn` that stray usage could inherit.
- **`.micro-label` global utility.** The tracked-uppercase eyebrow (font-size,
  letter-spacing, text-transform, font-weight 600, muted color) is the design system's
  first cross-component global utility class, defined in `lib/ui/utilities.css` and
  imported in `main.ts` after `tokens.css`. It composes the `--micro-label-*` tokens —
  tokens remain the source of truth; the class de-duplicates the composed rule that was
  re-typed across seven components. A cross-component eyebrow can't be owned by one
  component, so it gets a shared global stylesheet (`lib/ui/utilities.css`). The one
  consumer that keeps an inline copy is `DataTable`'s `td:not(.first)::before`
  stacked-card column-label: a CSS-generated pseudo-element can't take a class, and
  plain CSS has no mixin — that copy is kept in sync by comment.
- **`.visually-hidden` global utility.** The canonical sr-only recipe (modern
  `clip-path: inset(50%)`, not the legacy `clip` property) is the second cross-component
  utility in `lib/ui/utilities.css`. It removes content from the visual layout while
  keeping it in the accessibility tree — unlike `display:none`, which severs both. Used
  by `ConceptGroupNavigator`'s filter-pill checkboxes. `DataTable`'s stacked `<thead>`
  is sr-only only under `@media (max-width: 48rem)`, so it cannot apply the
  (unconditional) class and keeps a media-scoped inline copy held identical to the
  utility — the sr-only analog of the `td::before` micro-label exception.

### Migration discipline

No big-bang. Land the foundation first (tokens → type → shell), then migrate
page-by-page, ordered by traffic and by what the redesign wave is already rewriting:
browse/landing → subject page (with #664) → search → project editor → history/graph
(with #666/#678). Hard rules: components consume **semantic roles only** (so dark mode
and re-tints are free); each migrated page keeps its `*.browser.test.ts` green and ships
a `dev.sh shot` before/after as visual proof (the merge-gate UI requirement). The
foundation should land **before** #664/#666 bake in new screens, or we pay to migrate
them twice.

## Site-wide catalog vintage footer (#355 decision 2)

`App.svelte` renders a `<footer class="vintage">` on every route showing the reg_meta
version, schema version, and DB build date sourced from `/api/context`
(`context.webapp.reg_meta_version`, `context.reg_meta.schema_version`,
`context.reg_meta.import_date`). The footer is guarded on `context` (same as the header
`.build` chip) so it is absent until `/api/context` resolves. `import_date` is a UTC
timestamp string (`"2026-06-12T08:30:00Z"`); the footer displays only the leading
`YYYY-MM-DD` (split on `"T"`). The intent is citation stability: a reader quoting any
catalog node can see which reg_meta build it reflects without navigating away.

`AppShell`'s rail carries a `YearWindowSlider` dual-thumb year slider (#614/#611) as the
"Study window" control — a global control reachable on every route and inside the mobile
drawer. It sets the active project window (1960 floor → the catalog vintage year from
`context.reg_meta.import_date`; current year as the pre-context fallback), with bounds
threaded down from `App.svelte`. It writes through `windowStore`
(`src/lib/window.svelte.ts`) — see the store description below.

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

## Unified catalog subject page (`SubjectView`) (#611/#638)

The catalog's three *leaf* kinds — a **variable** (`scb/lisa/kon`), a **classification**
(`class/sun2020`), and a **concept group** (`group/scb/lisa/agi-astsni`) — render
through one shell, `SubjectView.svelte`, so they share a single article wrapper, one
title/fqid header, and one **canonical section order**:

1. **description**
2. **picker** — slice axis × time axis
3. **value set / codes**
4. **relationships**
5. **docs**

`SubjectView` is a thin *presentational* shell: it owns no data, no headings beyond the
title, and no restyling. Each section arrives as a Svelte `Snippet` from the leaf view
and the shell `{@render}`s the five in the fixed order. Every slot is **optional** — a
kind that has nothing for a section simply doesn't pass that snippet and the slot
renders nothing (no empty wrapper, no "none found" wall). The variable leaf suppresses
the under-header fqid line (`showFqid={false}` — its breadcrumb already ends in the
slug, making the line redundant); the classification leaf keeps the default
(`showFqid=true`). A concept group has no single fqid (its key lives in the
description's Technical details), so it's omitted regardless.

**Dispatch.** `CatalogNodeView.svelte` resolves a node by FQID (a no-query browse fetch,
so the response is always a `kind`-tagged node) and switches on `kind`. The
**list/browse** nodes — `provider`, `register`, `classification-root` — are NOT
subjects: they render their child lists inline (with #303/#516 concept-group folding),
not through `SubjectView`. The three **leaf** kinds each delegate to a per-kind view
that fills the shell: `binding` → `BindingLeafView`, `classification` →
`ClassificationLeafView`, and the `concept-group` (served by the fixed
`/catalog/group/{provider}/{register}/{key}` route, see Catalog router structure above)
→ `ConceptGroupView`. The classification-umbrella group (served by the fixed
`/catalog/group/class/{key}` route) is a fourth non-catch-all kind
(`classification-group`) → `ClassificationGroupView`, its sibling.

Per-kind mapping into the five sections:

  | Section       | Variable (`BindingLeafView`)                                                                             | Classification (`ClassificationLeafView`)               | Concept group (`ConceptGroupView`)                                                                             |
  | ------------- | -------------------------------------------------------------------------------------------------------- | ------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
  | description   | definition / description / unit `<dl>` + `via_same_as` note + Technical details (sensitive / identifier) | short name `<dl>`                                       | shared definition/description (when members agree — #678/#900) above Technical details (key / facets / source) |
  | picker        | `PeriodPicker` (time) + variant-resolution gate + add-to-project                                         | — (editions switch via `HistoryGraph` succession edges) | column picker (`RepresentationPicker`) + `PeriodPicker` (availability lens) (#678)                             |
  | value / codes | states (`StatesView`, each distinct value set via `CodeList`)                                            | `ClassificationCodesPanel` (`CodeList`)                 | —                                                                                                              |
  | relationships | `HistoryGraph` (over `/graph` payload) + `LineageDetails` (provenance/warnings)                          | `HistoryGraph` (edition points, succession edges)       | — (members live in the picker)                                                                                 |
  | docs          | `DocMentionsPanel`                                                                                       | —                                                       | —                                                                                                              |

**#670 — member identity and fetch ownership.** For a grouped variable,
`BindingLeafView` renders a member-distinguishing qualifier (facet labels, e.g. "AGI ·
2007 SNI edition", falling back to the slug for edge-group split siblings) and a "member
of ⟨group⟩" context link directly under the header — both additive and gated on a
resolved `/graph` fetch. The fetch itself is owned by `BindingLeafView`, which derives
the qualifier (`qualifierFromFocus`) and group link (`groupLinkFromFocus`) from the
graph focus node's `facets` / `group_label` — no separate `/dimensions` request. The
same `/graph` fetch feeds `HistoryGraph`; failure domain is unchanged — a graph error
omits both the header qualifier and the graph panel without affecting the rest of the
leaf.

### The picker — slice axis × time axis

The picker section carries up to two orthogonal controls. The **slice axis** differs per
kind:

- **Variable** — the slice is the `register_variant` (population). When ≥2 register
  variants co-exist for the chosen period, `buildAddPlan` (`catalog.ts`) returns
  `choose-variant` and the picker renders a **proactive population selector** that
  **gates the "Add to project" button** (#638 PR2b — the *variant-resolution gate*): the
  user resolves the population *before* committing, not in a post-click modal. A pure
  time-sequential succession (one variant retiring into the next) is NOT a choice — it
  auto-splits into one source per segment — so the selector is invisible for an
  unambiguous variable. (The *representation* / delivery-column choice stays a
  post-click chooser; it is per-segment and a succession split can carry several.)
- **Classification** — editions (`sun1996` → `sun2000` → …) are the slice, but there is
  **deliberately no picker**. Editions switch via the succession edges in `HistoryGraph`
  (rendered under relationships): each edition is its own `class/<slug>` URL, so
  navigating the graph *is* the edition switch. Adding a picker would duplicate that.
- **Concept group** — the **column / representation picker** (`RepresentationPicker`,
  #678): one compact band per member variable, each listing that variable's delivery
  columns as selectable rows. A single-column variable collapses to one row; a
  multi-column variable gets a thin subheading (its distinguishing identity + a
  per-variable "select all") over its column rows. Each band identity links to the
  member's leaf page. When the group spans more than one distinct concept name,
  `clusterBands` (#901) groups the bands under `<h3>` name-cluster headings — each name
  renders once and each band leads with its within-cluster distinguisher (facet,
  delivery column, or member slug) rather than the repeated name. One shared
  cross-variable selection basket and a single "Add to project" footer span all bands.
  Each picker row is marked with the **kind** of dimension that distinguishes it from
  its siblings — a `facet` (a #819 `GroupAxis` value, per member), `variant`
  (population), or `coding` (value-set version label) — and a **per-dimension filter
  strip** lets the user narrow a large multi-axis group to one axis value (#908,
  `pickerFilterDimensions` / `pickerRowPasses` / `PickerDimension` in `catalog.ts`). A
  dimension surfaces as a filter only when it discriminates (≥2 distinct values across
  all visible rows); single-value dimensions are invisible. Filtering is a client-side
  presentation lens: a hidden-but-selected row still commits, and the footer signals
  this. The filter logic mirrors the #819 `ConceptGroupNavigator`: OR within a
  dimension, AND across.

The **time axis** is the shared `PeriodPicker` (see the Project-window store section),
but it does a different job per kind:

- On the **variable** it drives resolution: a local change writes `?period` (precedence
  `?period` > project window > full history), which **refetches** the `resolve_at`
  subset and narrows the visible states.
- On the **concept group** it is a **client-side availability lens only** (#638 PR2a):
  `getConceptGroup` takes no period, so `?period` drives **no refetch** — it only greys
  members whose coverage doesn't span the active window (an open-ended member end is
  first projected to the catalog vintage, mirroring `PeriodWindowSlider`). The member
  links carry the active `?period` into the leaf for continuity.
- The **classification** leaf has no time axis (a classification edition is
  period-less).

The group's availability span is `memberCoverageUnion` over its members' coverages, and
a member's coverage line uses `formatWindow` — including the one-sided `until <year>`
form when the start is unknown (#658).

### Shared section components

- **`CodeList`** (#638 PR3) — the single value-set / code viewer. A variable's value set
  and a classification's code list are the same shape (a code → label set, and a value
  set often *is* a classification), so they render identically: `StatesView` uses it for
  each distinct value set in the multi-state view (and for the single value set in the
  detail mode), and `ClassificationCodesPanel` for the edition's codes. It owns a
  **size-dependent filter** (a search box appears only at ≥ `CODE_FILTER_THRESHOLD`
  codes — pointless for a handful) and a height-constrained scroll for the long LISA
  sets; the classification side's per-code `is_valid` surfaces an "observed" tag (a
  variable member omits it, so no tag shows — same component, fewer columns of signal).
- **`TechnicalDetails`** (#638 PR4) — the shared "Technical details" `<details>`
  disclosure that demotes **backend/structural** fields below the user-facing ones: the
  variable's sensitive / identifier flags, a state's type / length / delivery column
  (`StatesView`), and a group's key / facets / source. One component keeps the summary +
  styling consistent across the three call sites; callers omit it entirely when there's
  nothing to demote.

### Why the unified `HistoryGraph` can cover both leaf kinds (#678)

`HistoryGraph` (#678) renders **both** variable and classification leaves over the same
`RelationshipGraph` wire shape — the kind-branching lives in the backend
(`Catalog.graph_for_binding` vs `graph_for_classification`) and in the pure projection
(`history_graph.ts`), not in the renderer. Variable nodes lay out as horizontal
representation-run cells along the time axis; classification nodes lay out as
version-ordered edition points. Succession edges draw as directed arcs between node
markers. The remaining per-kind differences that the old separate panels had to
special-case are either absent from the graph contract (the classification chain carries
no per-transition reason; dead predecessors are resolved server-side before the graph is
returned) or kept in the non-graph residue: `LineageDetails` carries the variable-only
surfaces (`variable_state_lineage` provenance edges + `/lineage_warnings`) and is simply
not mounted on the classification leaf, which has neither.

### Rejected alternatives + the viz-dependency trigger (#667 spike)

The model is **entity nodes with column/representation slices** — *not* top-level column
nodes, *not* variable-only nodes that hide the columns. A top-level column-node graph
would explode dense monthly families (e.g. `agi1lonfink`'s 12 delivery columns) and the
group pages into unreadable node clouds; a variable-only graph would lose the
same-column-versus-different-variable distinction that motivated the view in the first
place. Keeping one node per variable with its representation-run cells in-node preserves
both: the family reads as one entity over time, and the columns stay visible as slices.
Classifications reuse the same owned primitive but *not* timeline semantics — editions
are standards/versions (a 2024 study may still code against SUN 2000), so they render as
a version-ordered edition graph, never validity intervals.

The primitive is deliberately **hand-built SVG + scoped CSS, not a graph library** (#667
spike conclusion). Lanes/columns, node markers, edge arcs, labels, the keyboard/ARIA
wrapper, and responsive overflow are each small enough to own locally against the design
tokens, and both views are deterministic layouts (a time axis or an edition ordering),
so a force-directed/auto-layout engine buys nothing. **Revisit a dedicated viz
dependency only if** production requirements add pan/zoom, collision-avoidance,
large-graph virtualization, or interactive graph editing — work that materially exceeds
this custom primitive. Short of one of those four triggers, a library is net complexity,
not net simplicity.

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
  bake inputs — NOT baked deps reg_schema, which need a manual `workflow_dispatch` —
  decided 2026-06-11: that is the rule, not a gap) build, push to `registry.fly.io`
  (SHA-tagged), and `flyctl deploy --image`. The bake build-arg is the RESOLVED newest
  `reg_meta/v*` tag (never `latest` — a literal `latest` makes the bake layer's buildx
  cache key insensitive to data-only releases and can even resurrect a stale cached
  layer after a pinned dispatch). Nothing deploys without green CI: a `wait-ci` job
  polls this commit's ci.yml run and both deploy jobs require its success — an image
  that builds but fails lint/ty/pytest never ships. Both deploy jobs carry a
  HEAD-of-main guard (GHA concurrency serializes by build-completion order, not commit
  order — without the guard an older commit's slow build could overwrite a newer deploy;
  it also makes non-main dispatches deploy-inert). Two gates guard a bad image: the
  entrypoint smoke gate (container exits non-zero before ever serving) and fly.toml's
  `/api/context` HTTP check (flyctl reports failure if it never passes). Rollback:
  `flyctl releases --image` lists history; `flyctl deploy --image <old>` restores in
  seconds.
- **Pending-schema-bump guard (#448)**: when `main`'s `SCHEMA_VERSION` /
  `DOC_SCHEMA_VERSION` is AHEAD of the latest released `reg_meta/v*` asset (same major,
  higher minor), the bake's `reg-meta update` would refuse the behind-schema asset (exit 10)
  and turn `build-image` red — pausing **all** deploys for a state that is expected (the
  owed reg_meta release ships the matching asset). A standalone `schema-guard` job
  compares the code constants against the released tag's (`git show <tag>:…`) via the
  pure `scripts/schema_pending_bump.py` helper, which returns a three-way verdict
  (`break` / `pending` / `compatible`). On a detected code-ahead `pending` bump (with
  both assets present) it publishes a `pending_bump=true` job output that defers the
  bake + deploy with a GREEN `build-image` and a `::notice::`. The guard is its **own**
  job (not a step inside `build-image`) so **every** deploy path can consult it —
  `build-image`, `deploy`, AND `edge-deploy` all gate on
  `needs.schema-guard.result == 'success'` (and on `pending_bump`); it runs whenever the
  image OR edge filter matches (or on dispatch), so an edge-only push still gets a
  verdict even though `build-image` is skipped. Once the owed release ships, the
  **build** self-clears on the next image-affecting main push (the bake now passes), and
  the **deploy** is self-clearing on release too: publishing the owed `reg_meta/v*`
  release auto-dispatches `container-build.yml` (via `publish_reg_meta.yml`'s
  `deploy-image` job, after the PyPI publish succeeds), which re-resolves the
  now-current asset and deploys — no manual `workflow_dispatch` needed. During a
  pending-bump window a later **edge-only** main push now correctly waits too:
  `schema-guard` ran (the edge filter matched), so `edge-deploy` sees
  `pending_bump == true` and holds its SPA/cache-gen ship alongside the origin, rather
  than going live against the still-pre-bump origin. The guard green-neutralizes
  **only** the safe code-ahead case; on a genuine **major break** — or a `pending`
  release that is ALSO **missing** a `.zst` asset (a #343 invariant violation, verified
  via `gh release view`) — `schema-guard` **fails red (exits non-zero)** rather than
  emitting `pending_bump=false`. Because all three deploy jobs gate on
  `needs.schema-guard.result == 'success'`, a failed guard cleanly blocks build-image +
  deploy + edge-deploy — closing the edge-only hole where a skipped bake left nothing to
  fail (pre-fix the break surfaced only as the bake's exit 10 on image pushes, so an
  edge-only push shipped a new SPA/cache generation against a still-stuck origin) and
  giving a clearer red than a bake exit-10. The guard is also bypassed for an explicit
  `workflow_dispatch` `reg_meta_tag` pin — a deliberate pin of a specific (possibly
  older) release has no owed release coming, so it must fail loud in the bake if
  incompatible, not green-no-op (and dispatch always runs build-image, so there is no
  edge-only leak there). The comparison rule is unit-tested because CI can't reach the
  code-ahead branch on a normal commit (main's schema usually equals the latest
  release); its source of truth is `_check_schema_compat` in
  `reg_meta/src/reg_meta/db.py`. Trade-off: during the bump window the Dockerfile bake
  isn't exercised (a build-only PR goes green-skipped), re-exercised once the release
  lands.
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
  cache entries — fresh payloads immediately after deploy, while the per-route TTL still
  bounds origin traffic *within* a generation (60s for catalog + search, 24h for
  doc-library). This is the free-plan substitute for `cf.cacheKey` (Enterprise-only) and
  needs no purge credentials. Origin-side the param is inert: FastAPI ignores undeclared
  query params and the ETag is content-derived. Consequence: `edge-deploy` runs on
  **image-affecting** pushes too, not just edge paths — an origin deploy that changes
  API payloads without touching the SPA/contract must still ship a new cache generation.
  The motivating incident (#303 rollout) had the edge serving 11h-old pre-deploy catalog
  JSON against a freshly deployed SPA; the #317 defensive-rendering rule (SPA tolerates
  one cache generation of payload skew on additive fields) stays in force regardless,
  for clients holding *browser*-cached payloads (catalog + search browser TTL is 60s;
  doc-library is 86400s — both unversioned). Deploys: the `edge-deploy` job in
  `container-build.yml` rebuilds the SPA (bun pinned to the Dockerfile's version — bump
  together) and runs `wrangler deploy` on main pushes touching the SPA, the edge worker,
  the committed `openapi.json`, or the image surface (cache generation, above)
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

## Project-write surface (`routes/project.py`)

Two POST endpoints: `/api/project/validate`, `/api/project/order`. Both read the body as
a **raw JSON dict** (not a typed param): `/validate` must accept malformed specs to
diagnose them, and the raw dict preserves steward-namespaced blocks (`swecov` /
`reg_monabundle`) that a typed `extra="ignore"` body would silently drop. The raw body
is documented in OpenAPI as an open object (`additionalProperties: true`) so the SPA
codegen sees a body to send.

- **`/validate` status discipline.** A spec that FAILS validation is a *successful
  validation response* — **HTTP 200 with `ok=false` + the issues**. 4xx is reserved for
  a malformed REQUEST (non-JSON, duplicate JSON keys, a too-deeply-nested or non-object
  body, an oversized body). It runs the §6.8.0 two-layer composition (structural →
  semantic) and returns the **concatenated** issue list; the DB-free structural layer
  runs first, so a structurally-rejected body costs no DB hit. It also runs the
  cross-block referential checks (orphan `binding_options` keys /
  suppress_k-on-non-categorical).
- **`/order`** renders the steward's default order-export CSV (a `text/csv` download)
  and is the one documented exception to the "every route declares a `response_model`"
  lint (it returns raw bytes). Unlike `/validate`, it structurally **gates** first: you
  cannot render a provider order from an invalid spec → 422.

**Connection model = per-request open ON ONE THREAD** (the locked cross-thread guard).
`/validate` and `/order` are `async` only to read the body off the wire; the blocking
work (structural parse + per-binding sqlite resolution) is offloaded via
`run_in_threadpool`, and the reg_meta connection opens on **that** worker thread inside
a `with`-block — NEVER a generator `Depends` (which can run on a different AnyIO thread
→ `sqlite3.ProgrammingError`).

The order CSV cell values are passed through a **spreadsheet formula-injection** guard
(`_csv_safe`): a researcher-controlled `display_name` like `=HYPERLINK(...)` would
otherwise execute as a formula when the data provider opens the manifest. A leading
formula-trigger char (`=+-@\t\r`) is prefixed with a single quote.

## Semantic validation (`semantic.py`)

The §6.8.3 reg_meta-backed validation layer. It lives in the webapp — NOT `reg_schema` —
because `reg_schema` is reg_meta-free by design (the schema has many consumers,
including future exporters and the MONA rebuild, that do not carry the DB); semantic
rules need the live DB, so they belong where the DB is. `reg_schema` lists these codes
as defined-but-not-emitted on its own surface; this is their home. The webapp invokes it
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
  `binding_state_drifts_within_period` (info); the same `(info)` code also fires when a
  pinned `representation` column under-covers the requested range vs a sibling column
  that delivers the shortfall — a leading/trailing gap or **internal** gap inside an
  explicit range, `_default`'s full-history bounds, or a segment of a list period
  (#342/#465, gap-based). A **#307 list period** (interrupted series; structurally
  sorted + disjoint, wire form comma-joined — `2005..2010,2015..2020`) resolves **per
  segment**: `period_outside_state_validity` and `range_period_partially_covered` fire
  per uncovered/under-covered segment (naming it), and the PER-INSTANT probes —
  co-existence/ambiguity, the co-delivered-value-set backstop, the pinned
  representation's presence — also run per segment (the whole-series union would
  false-positive on windows overlapping only BETWEEN segments). Only the series-level
  properties — the resolved columns for steward admission and the sequential-drift info
  — use the compound-key-deduped union of every segment's states. `Catalog.resolve_at`
  never sees the list form; since #340 the catalog `?period=` query accepts the comma
  wire by doing the same per-segment resolve + union in the route (see The `?period`
  query above).
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
the literal binding FQID: a curated same_as sibling (e.g. `kon→syss`) names a
*different* physical column, so warning on it is correct under holdings semantics, not a
keying artifact.

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
  is far above any plausible `project_data.json`.
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
  order download) through `lib/api.ts`; it is NOT a structural validator (the backend is
  canonical).

Note: v1 is **one draft per SPA session** (a single IndexedDB key), not a multi-project
list — opening a file replaces the current draft.

## Project-window store (`window.svelte.ts`) (#614/#611)

`windowStore` (`src/lib/window.svelte.ts`) is a module-singleton Svelte 5 rune store — a
peer of `router.svelte.ts` and `project_store.svelte.ts` — that is the **single
read/write path** for the active study window (`{from, to}` int years, or `null` = full
history). The rail `YearWindowSlider` and each page's period picker go through it rather
than touching the project store or `localStorage` directly. The per-page `PeriodPicker`
now defaults to `PeriodWindowSlider` (#615/#671): it seeds the local dual-thumb slider
with coverage-aware precedence — explicit year `?period` > intersection(coverage,
window) when a window is set > the coverage span when none is > the bare window when
there is no coverage > full bounds when neither — so a variable's true data coverage
shows up front instead of the full 1960–vintage track reading as available (M11). The
out-of-coverage track renders immediately as a greyed **non-selectable** band (no drag
needed), and the thumbs are hard-clamped to coverage via `DualThumbTrack`'s opt-in
`selectableMin/Max` (the rail `YearWindowSlider` passes neither and is unchanged). A
local change writes `?period` only (never the global window). The user-deviation hint
(`?period`/drag ≠ window) fires only on an explicit `?period` or a live thumb drag — not
on the untouched coverage-clamped default seed (`userChosen`); the data narrowing the
window is not a user deviation. This supersedes the #615/#639 "grey only after drag"
posture for this slider: #639's anti-alarm intent is preserved (the up-front band is a
passive "no data here", not a selection warning), while an explicit out-of-coverage
`?period` still renders honestly with its not-delivered gap. Open-ended coverage
additionally surfaces a **"coverage through \<vintage\>"** note (M21). The per-page
picker shares the rail slider's vintage ceiling (#631): `App.svelte` threads the ceiling
(`context.reg_meta.import_date`'s year) down through `CatalogNodeView` →
`BindingLeafView` → `PeriodPicker`, and also through `ConceptGroupView` → `PeriodPicker`
(#638). On the concept-group subject page the picker is a **client-side availability
lens** over the union of member coverage spans — it greys members not delivered in the
active window but drives no refetch (`getConceptGroup` takes no period parameter). The
vintage is the ceiling an OPEN-ENDED coverage (`coverage.to === null`, "still
delivered") projects to — the catalog only knows delivery up to its vintage — so the
coverage band ends at the vintage and a selection past it reads as "not delivered after
`<vintage>`". It is NOT a floor on the slider bounds: a FINITE coverage keeps its real
end (never extended to the vintage), and a window/selection past the vintage still
widens the bounds (the thumb renders the real value) without extending coverage.
Wall-clock is the pre-context fallback only.

Precedence — two backing stores, one source of truth:

- **Draft active**: the window IS `draft.window`. Reading returns the draft's value;
  setting calls `projectStore.updateField("window", …)`, which marks the draft dirty and
  rides the store's existing debounced autosave. The window is durable because it lives
  in `project_data.json`. A draft with no `window` field reads as `null` (its own
  absence) — the active project's state is always authoritative.
- **No draft**: falls back to `localStorage` (key `reg_webapp:project_window`) so the
  slider works and survives a reload when browsing without a project. Writes to
  localStorage are best-effort; a failure (private mode, quota) degrades to an
  in-memory-only session without crashing.

The draft always wins while it exists; localStorage is purely the no-project fallback
and is never mirrored while a draft is active. When a fresh draft is created FROM
BROWSING — `projectStore.newProject` with no draft already open — the browse-time
fallback (`windowStore.fallback`) is seeded into `draft.window` as the clean baseline,
so a window set while browsing without a project carries over into the project created
from that browse state rather than silently reverting to full history. `newProject`
invoked while a draft is already active (the "New" button inside a project) does NOT
seed: the fallback is the stale no-draft value (active-draft window writes/clears don't
update it), so the new project starts windowless (full history) unless the user sets
one. An opened project (`openFromFile` / restore) keeps its own `window` unchanged. The
rail slider also exposes an explicit ✕ clear control that writes `null` back to the
store, making full history reachable at any time after the first interaction.
Catalog-derived slider bounds (from the API rather than the fixed 1960 floor) are a
possible follow-up.

## API surface

The committed `backend/openapi.json` is the canonical contract; this table is the
orientation map. All endpoints are under `/api/`; read GETs are edge-cacheable, write
POSTs are not. Catalog browse paths use FQID segments directly.

  | Method | Path                                             | Purpose                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
  | ------ | ------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | GET    | `/api/context`                                   | Deployment identity, branding, build info, catalog-drift warnings.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
  | GET    | `/api/catalog`                                   | Top-level: every provider the steward exposes + the `class` root.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
  | GET    | `/api/search`                                    | Global FTS search → typed result groups (`registers` / `variables` (folded) / `classifications` / `codes` (#352)); extensible (docs join as a new group). `?q=` required, `?limit=` per-group cap, `?type=` scopes to one group (`all` default; #393).                                                                                                                                                                                                                                                                                                                                                                        |
  | GET    | `/api/docs/search`                               | Docs FTS search (excerpts + source pointer), optional `?register=`; `ingested=false` when no docs index.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
  | GET    | `/api/docs/doc/{identifier}`                     | One doc by variable/filename — metadata + source pointer + bounded excerpt (never full body).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
  | GET    | `/api/docs/for-variable`                         | "Mentioned in documentation" hook: fuzzy name/`provider_key` matches + `register_ingested` coverage flag.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
  | GET    | `/api/catalog/{fqid}`                            | Single endpoint for every hierarchy node (`kind`-discriminated). On a binding leaf, embeds the variable's full longitudinal record + its full variable `succession_chain` (#582); on a classification leaf, embeds the full succession `edition_chain` (#571). Optional `?period` / `?variant` / `?value_set_version` narrow a binding leaf to a `{binding, states}` subset (uniform with `/states`). A dead/renamed binding, register, or classification slug with a successor 301-redirects to its terminal successor (kind-dispatched — #355 PART 2, #412, #571); `?period` branch and sub-endpoints also redirect (#411). |
  | GET    | `/api/catalog/{provider}/{register}/variants`    | The register's variant browser.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
  | GET    | `/api/catalog/group/{provider}/{register}/{key}` | The concept group as a browsable subject (all members; `?member=` focus). (#617/#616)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
  | GET    | `/api/catalog/group/class/{key}`                 | The classification umbrella group as a browsable subject (all members). (#756)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
  | GET    | `/api/catalog/{fqid}/states`                     | Full state history for a binding. Dead/renamed binding 301s to `/states` on its terminal successor (#411).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
  | GET    | `/api/catalog/{fqid}/predecessors`               | Inbound `variable_replaced_by` edges. Dead/renamed binding 301s to `/predecessors` on its terminal successor (#411).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
  | GET    | `/api/catalog/{fqid}/successors`                 | Outbound `variable_replaced_by` edges. Dead/renamed binding 301s to `/successors` on its terminal successor (#411).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
  | GET    | `/api/catalog/{fqid}/lineage`                    | Materialized `variable_state_lineage` edges (consumer ← source). Dead/renamed binding 301s to `/lineage` on its terminal successor (#411).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
  | GET    | `/api/catalog/{fqid}/lineage_warnings`           | Linker-emitted lineage coverage warnings. Dead/renamed binding 301s to `/lineage_warnings` on its terminal successor (#411).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
  | GET    | `/api/catalog/{fqid}/dimensions`                 | Concept-group dimension memberships containing this variable (the variant facet groups: level/population/rank/…). Dead/renamed binding 301s to `/dimensions` on its terminal successor (#411).                                                                                                                                                                                                                                                                                                                                                                                                                                |
  | POST   | `/api/project/validate`                          | Three-layer validation; 200 + `ok` + issues.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
  | POST   | `/api/project/order`                             | Default order-export CSV download.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |

**Order-export CSV columns** (the v1 default; fixed order is the contract):
`provider,register,variant,variable,representation,period,display_name` — one row per
spec binding. `representation` is its OWN column (not folded into `display_name`): a
custom display name would otherwise hide which delivery column the binding pinned, so
the data provider couldn't tell representations apart. `period` serializes via the
catalog `?period` wire form (range → `"<from>..<to>"`, snapshot → `"_default"`).
Pluggable per-steward `order_template`s are remaining — see `REFACTOR_SPEC.md`.

Global FTS search shipped as `GET /api/search` (#350); the docs library shipped as
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
