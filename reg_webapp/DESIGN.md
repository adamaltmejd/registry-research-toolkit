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
db_path = reg_meta.db.db_path_from_args(None)  # REG_META_DB > XDG > platform
conn = reg_meta.db.open_db(db_path)  # mode=ro + _check_schema_compat
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

The `/variants` payload includes the variant's compact display metadata plus nested
`versions` with register-version description/measurement prose and population/object
type rows. The SPA renders that prose on its OWN route (`VariantBrowser` at
`/catalog/<provider>/<register>/variants`, Y-79), not on the register page, which shows
only a compact summary (`VariantsSummary`: one row per variant family with its name,
concrete slugs and year span) over a link to it. `_default`-only and empty variant lists
still suppress that whole summary section.

Plus two **concept-group subject routes**, both declared above the catch-all:

- `GET /catalog/group/{provider}/{register}/{key}` (#617/#616) — a FIXED 4-seg shape
  with a literal `group` prefix and explicit `{provider}`/`{register}`/`{key}` segments,
  NOT an `{fqid:path}` suffix. Returns a `ConceptGroupNode` (`kind: "concept-group"`)
  with all group members, facets, and per-member coverage via `Catalog.concept_group`;
  404 on an unknown key or register. `?member=<slug>` is an optional focus hint echoed
  only when it names a real member — a bad hint is silently ignored, keeping the group
  page first-class.

- `GET /catalog/group/class/{key}` (#756/#1116) — a FIXED 3-seg shape (literal `group`,
  literal `class`, `{key}`), the **classification-subject sibling** of the
  register-scoped route above. Returns either a curated umbrella
  `ClassificationGroupNode` (`kind: "classification-group"`) resolved via
  `Catalog.classification_group(key)`, or a derived one-dimensional
  `ClassificationFamilyNode` (`kind: "classification-family"`) resolved via
  `Catalog.classification_family(key)`; 404 when neither exists. Has **no**
  provider/register, **no** per-member coverage, and **no** `?member=` focus hint —
  classification subjects are catalog-global, not scoped to a single register. This
  route is declared **immediately above** the register-group route so the literal
  `class` segment matches before `{provider}` is tried. A collision with the
  register-group route is unconstructable: `class` is not a valid provider slug
  (`Fqid.register_fqid` rejects it), so no real register-group URL can share this shape.
  Classification-root browse rows link to this route (via `classGroupHref`) and render
  through a dedicated `ClassificationGroupView` component.

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

At the current head, the classification leaf also embeds further payloads inline for
synchronous SPA render: `codes` (reg_meta's `ClassificationCode`, embedded directly from
`Catalog.classification_codes` — the resolved edition's canonical value-set codes and
labels; omitted when empty), `dimensions` (#609; reg_meta's `ConceptGroupSummary`,
embedded directly from `Catalog.classification_dimensions` — the curated umbrella
group(s) the edition belongs to, reading `concept_group_classification`; omitted when
empty), and `family` (#1116; a `ClassificationFamilyNode` for derived one-dimensional
succession families such as ICD/SSYK/LKF/SNI; null when absent). The SPA uses
`dimensions` / `family` to keep grouped classification FQIDs shareable while rendering
the canonical group/family surface with the requested edition tab active. The active tab
renders the resolved edition as a code/label panel (shared `CodeList` viewer — the same
component used for the variable value set, with a size-dependent filter: the search box
appears only when the set reaches the `CODE_FILTER_THRESHOLD`, hidden for small sets;
large sets collapse into derived level/prefix groups or a bounded flat preview; #638 /
#1120), and non-active tabs do not fetch their value sets until selected. The v1 payload
correction under `CodeList` removes the synchronous full-code embedding while retaining
bounded metadata and relationship data here.

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
the list form (keeps the list grammar out of the separately-released reg_meta). This
browse read is its own, but it resolves the way the shared `order.resolve_binding` pass
does and for the same reason: `resolve_at`'s monthly-family fallback is decided per
query, so one segment's window must not suppress another segment's fallback (see Current
semantic validation). `?variant` narrows to one variant; `?value_set_version` narrows to
one vintage (a read-only browse filter matched against `value_set_version_label` by
`resolve_at`, **not** a path pin). The period query is **ignored** on non-binding kinds
(the register / provider / classification node resolves normally). An absent `?period`
still returns the FULL embedded leaf.

**The binding leaf carries coding IDENTITY, not code membership (Y-46).** Both binding
reads — the no-period leaf and its `?period` subset — resolve with reg_meta's
`with_codes=False, with_code_summary=True` (see reg_meta/DESIGN.md → Narrow reads).
Every historical state is still there, with its window, variant, delivery column,
`value_set_id`, `value_set_version_label` and stored conformance verdict; what is gone
is each state's `value_set` members and its conformance `nonconforming_codes`, replaced
by a `value_set_summary` (`code_count` + the dense-integer `integer_range`) computed
once per distinct coding per request. The reason is arithmetic: `scb/rtb/forsamling`'s
290 states share a handful of codings, and embedding those codings per state made the
initial response 24 MB. The payload is now independent of code cardinality, and the only
thing a consumer loses is what it could not render on arrival anyway.

`GET /api/value-sets/{value_set_id}/codes` is the read that gives them back — the ONE
bounded route this added, declared next to the catalog routes (it is not a `{fqid:path}`
suffix, so ordering is not at stake). It answers ONE coding, `?offset`/`?limit`-windowed
(default 200, max 1000, clamped not 422'd — the shared `clamp_limit`), in stable
code/label order, with `total` counted over the WHOLE set. `?q=` filters BEFORE the
window using `query_input.matches_filter` — a character-for-character port of the SPA's
`foldText`/`matchesFilter` (NFD-decompose, drop combining marks, lowercase, plain
substring, `%`/`_` literal), because SQLite's LIKE folds neither non-ASCII case nor
diacritics and an "N of M" that disagreed with the in-browser filters would be a lie.
`?state=` switches the read to that state's stored `classification_conformance_code`
mismatch list, and is refused with a 404 unless the state actually carries the requested
value set — a state id can never read a coding it does not belong to. Not steward-gated,
and its ids are enumerable by design: value-set members and these stored mismatch lists
are catalog-global reference data, on the same footing as the classification codes
already served (→ Classification pass-through (decision 2)). Holding a binding is not
what authorizes reading a coding, so the pass-through rests on that policy and not on an
id being hard to guess. The SPA's `ValueSetCodes` panel owns the paging, the filter and
the loading / error+retry / empty states, and is mounted only where a code table is
actually shown — a closed disclosure issues no request. "Shown" means the state HAS a
coding, not that the coding has members: a `value_set_id` with `code_count` 0 shows its
size on the row and says "This value set has no codes" in place — no disclosure, since
there is nothing to open, and no read, since the leaf already counted it. An empty
coding and no coding at all are different facts, and a reader who cannot tell them apart
is left guessing whether the page failed. Inside the panel the shared `CodeList` renders
each page verbatim: a server page is a WINDOW, so the viewer's own filter and its
large-list grouping are suppressed there — grouping a partial page would group the wrong
thing, and a set that drills down on a classification page reads as a flat bounded list
here. Typing is debounced into one read, because this filter scans the whole set
server-side. `Catalog.resolve`, `/states` and the complete exports keep their
full-membership semantics unchanged.

One consequence in the SPA worth naming: `distinctValueSets` used to synthesize a
cross-state conformance rollup whose mismatch count was the size of the deduped union of
its states' mismatch lists. Those lists are per-state on-demand relations now, so no
single read could produce that number. The entry therefore carries `conformances` — one
STORED verdict per distinct list, each with the state its list is read by and the
variants/period it was recorded over, rendered as its own notice inside the entry.
Nothing a state warned about is dropped by the grouping: a classification edition
spanning two codings keeps both lists, side by side and separately openable. What IS
collapsed is repetition — one entry per (coding, declared classification) pair, because
the build gate derives both the verdict and its mismatch list from exactly those two
(`reg_meta_build/classifications.py` matches the state's value-set members against the
declared edition's valid codes), so an era of yearly states over one coding shares one
list and reports it once. The window on that notice covers the states that CARRY the
verdict, which is often narrower than the coding's own usage window, so it is labelled
"recorded for" rather than left to be read as the era. Every count shown describes the
one list its disclosure opens.

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
searches still surface the folded group. `ConceptGroupRow` takes an optional `onpick`
that renders members as pick buttons instead of catalogHref links (#322) — a browse/pick
mode switch, currently dormant since the #991 cart model retired the only `onpick`
consumer (`CatalogPicker.svelte`; picking now happens from the catalog subject page's
own picker, see § The picker — slice axis × time axis, which does not use
`ConceptGroupRow`'s pick mode). `foldGroupedRows` tolerates a stale pre-`groups`
edge-cached payload (#317) by degrading to the flat list.

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

`GET /api/search?q=&limit=&type=&cursor=` (#350) is the discovery surface consumed by
the global header omnibox (`SearchOmnibox.svelte`, shipped in this PR). It returns
**typed result groups** over the shipped FTS5 indexes, reusing reg_meta's
concept-group-folded `search` (`reg_meta.queries.search`, #322) — the webapp does NOT
reimplement folding or FTS. `?type=` (#393) scopes the search to ONE group: `all` (the
default, or omitted) preserves the fixed-order four-group response; any single type
(`register` / `variable` / `classification` / `value`) runs AND emits only that one
group. An unknown value 422s at the boundary (the valid set mirrors reg_meta's
`SEARCH_TYPES`). For a FILTERED steward, the register and variable surfaces are further
scoped to the steward's held FQIDs — see § Steward layering → Browse and search scoping
(#859) above; classification and code surfaces are catalog-global and unaffected.

The SPA surface: a global `<SearchOmnibox>` in the app header routes to a shareable
`/search?q=` results page (`SearchView.svelte`) that renders an optional compact
cross-group `Top results` group above the four typed groups when multiple candidates
compete, with navigation to catalog nodes. The router gained `search` and `doc` routes
(query lives in `?q=`, keyed on pathname so the page re-runs on every query change) and
a `router.replace()` method (mirrors the `?period` URL-as-single-source-of-truth
pattern: the omnibox syncs back to the URL, and the URL drives the view). `api.ts`
gained `search(q, {limit?, type?, cursor?})` typed off the codegen'd contract. Off
`/search`, typing in the omnibox stays local until Enter/form submit and shows an Enter
hint while focused; on `/search`, typing live-refines with replaceState. `SearchView`
renders an "All · Registers · Variables · Classifications · Codes" scope toggle backed
by `?type=` (URL state, like `?q=`/`?period`; `all` is omitted from the canonical URL),
a Close control that `replace()`s back to the route that entered search (or `/catalog`
for a cold deep-link), and variable rows whose heading carries delivery-column pills
while register, definition, and `operational_definition` live in the muted detail line.
When several search hits address the same variable, `SearchView` folds them into one row
and merges the delivery-column pills so a column-code search shows one variable with the
matched columns inline; the "Variables" group heading itself stays plain text. The
omnibox preserves an active scope when re-querying. Global search does **not** render
documentation results; documentation is reached from item pages via `DocMentionsPanel`
and then the `/doc/<filename>` route (router `Route` union arm
`{name:"doc",identifier}`), which renders `DocView.svelte`: title,
register/variable/tags, a `source_url` link to the SCB source PDF (resolved from the
curated map at doc-DB build, #372; None when uncurated) with `source_title` as label,
and a bounded `excerpt`; 404 distinguishes "not ingested" vs "not found";
`snippet`/`excerpt` are rendered as TEXT, never `{@html}`, and the full converted body
is never fetched.

Each rendered group starts with at most 3 results. A `Load more` control requests that
group's cursor and appends the next bounded page; query or scope changes discard the
continuation state. The heading uses `N+ results` while `has_more` is true, rather than
claiming an exact total. The control is keyboard-native, announces its busy state, and
keeps continuation errors local to the group.

**The response contract is the point — designed to extend.** The body is
`{kind, query, groups: SearchGroup[]}`; each `SearchGroup` is a discriminated arm
(`group` literal) carrying bounded typed `results`, `has_more`, and `next_cursor`.
`top_results` is presentation-only and never paginates. Today: `top_results` (#393 items
6/7 — optional, all-scope-only, and emitted only when multiple candidates compete; built
from the already-prepared typed rows, exact identifier/name/code matches first, then
type priors register → variable/group → classification → code; a code row earns *prefix*
authority from its code identifier only, its label and owning code system counting as
identity on an exact match alone, so a topical term that merely starts an incidental
value label cannot displace the register or variable carrying that term in its purpose
or definition), `registers`, `variables` (leaf hits ⧺ folded concept groups),
`classifications` (leaf hits ⧺ folded classification-succession rows
(`type: "classification_succession"`, #571 — a query that hits ≥2 editions of the same
chain collapses to one `ClassificationSuccessionSearchResult` keyed on the terminal
edition, carrying the full `editions` chain and `matched_count`) ⧺ folded umbrella
concept-group rows (`type: "group"`, #516 — e.g. `group:sun`)), and `codes` (#352 —
value-label hits annotated with their owning variables/classifications). The reserved
docs arm remains unused by global search; docs stay on the separate `/api/docs/*`
endpoints and item-page hooks. The SPA must tolerate an unknown `group` value (skip it)
so a new group can ship before the SPA renders it (the same payload-skew tolerance the
`?period` additive fields rely on). Each result carries its navigable `fqid` and a
`rank: float` (the FTS rank the CLI's doc-merge interleaves by, #701); results within a
group are pre-sorted by FTS rank before grouping, so the SPA may ignore `rank` — it is
present on the wire as the shared sort key.

- **One reg_meta call per group**: register/variable/classification via the FTS
  `field="description"` path; **codes (#352) via the `field="value", type="value"`
  path** (`value_code_fts` label match + code-shape exact/prefix on `value_code.code`,
  ranked bm25 + rarity-downweight, owner-annotated — see reg_meta DESIGN.md → FTS5
  configuration). Each group gets its own bounded page and context-bound cursor; codes
  don't fold into concept groups (`fold_groups=False`). The codes page is then re-ranked
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
- **Bounded-origin budget (#1135):** every reg_meta SQL arm receives a finite prefix
  bound and expensive folding, owner annotation, golden construction, steward backfill,
  and top-results construction operate only on bounded candidates. The default is 3 per
  group (maximum 50). The route emits per-phase `Server-Timing` entries for controlled
  profiling. Representative broad all-scope cache-miss p95 is budgeted at 500 ms and
  browser-cold search LCP below 2.5 s; edge-cache hits are not accepted as cold-origin
  evidence. Invalid/context-stale cursors map to an actionable HTTP 422 at the route
  boundary. There is no in-process response cache.
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
  injects nothing). The route passes every matching pin to reg_meta's cursor-bound
  `exclude_fqids`, so the origin universe omits it on every page and a deep natural FTS
  hit cannot duplicate the injected pin. When a net-new pin displaces an origin row,
  continuation advances only past the origin prefix actually shown, so the displaced row
  appears on the next page. If configured pins outnumber the requested page limit, a
  signed opaque wrapper carries the next pin position plus the unchanged reg_meta origin
  cursor; its context binds the normalized query, group, ordered pin identities, and the
  origin cursor retains the catalog/steward binding. Pins therefore span pages in config
  order without being duplicated or lost; only the current page's pin slice is resolved,
  keeping golden construction bounded by the requested limit. `register` +
  `classification` pins are implemented (resolve cheaply by slug); a `variable`/`value`
  pin is a config error at LOAD (fail fast). The TOML is parsed + validated once at
  import; a typo'd fqid raises at apply (never silently drops). Eval gaps the pins close
  are flipped to `expect = "hit"` in `search_eval.toml` (SUN remains the lone gap — a
  concept-group modeling issue, not a golden-boost one).
- **ETag/cache-header wiring is automatic; cache effectiveness is not assumed**:
  `/api/search` is a GET, so the `ETagMiddleware` stamps a body-derived ETag (the query
  is part of the URL → part of the CF edge cache key, and part of the body → part of the
  ETag). No per-route caching code. The pending v1 performance probe must separately
  prove edge MISS→HIT and no-origin warm behavior for this query route.
- **Connection seam** (`conn.py`): the per-request read-only open (`catalog_conn`, the
  threadpool-safe pattern from #168) is shared with the catalog routes — extracted to
  `conn.py` so search doesn't import the catalog route module just for the connection.

The shared `?q=` input gate (`query_input.validate_text_query`: length cap + NUL reject,
both → 422) is reused by the docs endpoints below; per-group `?limit` is clamped, not
422'd.

## Docs library endpoints (`routes/docs.py`)

`GET /api/docs/*` (#354/#742) exposes the prebuilt `reg_meta_docs.db` surfaces — already
baked into the deployed container (the Dockerfile asserts it) but previously unopened by
the webapp. It reuses reg_meta's read-only query layer (`doc_search` / `doc_get` /
`doc_registers` / `related_documents_for_register` / `related_document_content`); no new
query logic beyond plumbing + the response policy.

- **Endpoints**: `GET /api/docs/search?q=&register=&limit=&offset=` (register-scoped
  optional), `GET /api/docs/doc/{identifier}` (by variable name or filename),
  `GET /api/docs/for-variable?q=&register=` (the "mentioned in documentation"
  variable-leaf hook), `GET /api/docs/related/{register}` (metadata for rehosted
  register-version PDFs), and `GET /api/docs/file/{register}/{filename}` (the PDF bytes
  for an exact register-local filename).
- **Policy — FTS excerpts, never full converted text**: the detail endpoint returns
  metadata + a `source` pointer + a BOUNDED `excerpt` (first `_EXCERPT_CHARS` of the
  cleaned body), and search returns the FTS `snippet`. The full converted body is NEVER
  served (marker+Gemini conversion quality + republication exposure). `source` is the
  SCB source-document identifier; `source_url` is the resolved SCB PDF link (populated
  at doc-DB build from the curated `doc_sources.toml` map, #372), None when the source
  is uncurated; `source_title` is the human-readable publication title (also None when
  uncurated). Coverage is LISA-only today. The related-document PDF route is the
  separate #739/#742 licensed rehost surface: it serves curated PDF bytes verbatim with
  `Content-Type: application/pdf`, an inline filename disposition, and JSON metadata
  carrying `source_url`, `license`, `fetched`, `sha256`, and `byte_size`.
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
  (search / for-variable / related metadata) or 404 "not ingested" (doc get / PDF file).
  When present, the per-request open is `conn.docs_conn` (same threadpool-safe model as
  `catalog_conn`, `check_schema=False`).
- **Not folded into `/api/search` or global SearchView**: the `SearchGroup` union
  reserves a `docs` arm (#350 contract), but it remains unused — the docs index is a
  *separate optional DB* and its `ingested` degradation doesn't map onto a group's
  `total_count`/`results` shape, so folding it into the omnibox endpoint would couple
  `/api/search` to a second DB open on every search request. `SearchView.svelte` no
  longer calls `/api/docs/search` or renders a docs section; the docs search endpoint
  remains available for doc-specific callers, while the SPA entrypoint is through item
  pages. The `/api/docs/for-variable` leaf hook has its own SPA consumer (#402):
  `BindingLeafView.svelte` renders a `DocMentionsPanel` sibling of the lineage panels,
  firing a SEPARATE independent `asyncResource` at `/api/docs/for-variable` — a distinct
  failure domain (a docs error, timeout, or absent index never blanks the leaf). The
  panel omits the entire section when the response is empty in any sense
  (`ingested:false`, `register_ingested:false`, or zero results), mirroring the
  omit-when-empty behaviour of the picker graph mode and `LineageDetails` (#612);
  loading and error states still render inline, so an in-flight or errored fetch never
  reads as a confirmed absence. When results are present, fuzzy hits are labelled as
  such; each hit links to the `/doc/<filename>` viewer and renders the FTS snippet via a
  safe inline-emphasis subset (`**…**` → `<mark>` for matched-term highlight,
  `*…*`/`_…_` → `<em>`) through auto-escaped Svelte interpolation — never `{@html}`,
  still excerpt-only.
- **Source documents on register pages (#742/#967)**: `CatalogNodeView.svelte` renders
  `RelatedDocumentsPanel` on register pages only, using the bare register slug. These
  are rehosted register/register-version source PDFs with provenance, not variable-level
  evidence, so `BindingLeafView.svelte` must not inherit them onto every variable page.
  It renders after the register's `VariantsSummary` so source PDFs stay at the end of
  the register page. The panel is another independent docs failure domain: loading/error
  render inline; absent docs DB, no curated rows, or an empty register result omits the
  whole section. Each row links the title to `/api/docs/file/{register}/{filename}` and
  shows `Källa: SCB · {license}` plus a source URL link.
- **Parsed documentation on variable pages (#402/#967)**: `BindingLeafView.svelte`
  renders `DocMentionsPanel` in the `SubjectView` docs slot. These are fuzzy
  variable-aware parsed-doc matches from `/api/docs/for-variable`, not authoritative
  source-document links. Each row links to the parsed `/doc/{filename}` view and exposes
  the full source PDF link when the docs index provides `source_url`.
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

For a filtered steward, the same payload fields are recomputed over the steward's held
delivery columns (`Catalog.register_column_coverage` + `CatalogIndex.held_columns`) so a
partial-column hold does not inherit the whole-variable coverage span/count. Named held
columns without a per-column state row get `coverage = None`; held unnamed columns use
`Catalog.register_unnamed_column_coverage` because `register_column_coverage` has no
NULL delivery-column key.

Register-scoped concept-group subject pages also use `register_column_coverage` for
representation members, but a missing per-column key is a known curated member with no
`variable_state` row, not absent enrichment. Those members serialize the existing
zero-state `VariableCoverage` shape (`state_count == 0`, null bounds, not open-ended)
instead of `coverage = None`, so downstream period lenses can distinguish "never
delivered" from "unknown".

**Query-time, not materialized — measured first** (the #351 design decision). The
aggregates are one GROUP BY over `variable_state` per listing, in reg_meta
(`Catalog.register_variable_coverage`, `register_column_coverage`,
`register_unnamed_column_coverage`, and `provider_register_coverage`). Measured on the
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

### Per-variable deliveries (Y-82)

A register-child also carries `deliveries`: the `(variant, delivery column)` pairs that
deliver the variable, each with that pair's own `VariableCoverage` window. It answers
the two questions the coverage span can't — *which variant delivers this?* and *what
column name does a researcher know it by?* (LISA's `forvink-ers` is `ForvErs` on paper).
One more GROUP BY in the same query-time family
(`Catalog.register_variable_deliveries`), grouped by
`(variable, register_variant, delivery_column_name)` over the same `variable_state` rows
and behind the same ETag/edge cache — no new endpoint, no build-time materialization.
Selecting `delivery_column_name` puts it outside `idx_variable_state_coverage` (as it
does its `register_column_coverage` sibling), so it reaches the row rather than being
satisfied index-only; both joins are LEFT to pin the join order, because with inner
joins SQLite drives from `variable_state` and scans the WHOLE table instead of searching
`idx_variable_state_variable` per register.

- `column` is None for a state SCB named no delivery column for. The variant still
  delivers the variable, so the row is KEPT — unlike `register_column_coverage`, whose
  per-column keys can't express a NULL key. Variants with a NULL slug are excluded
  (symmetric with `Catalog.list_variants`: an unslugged variant isn't addressable).
- **Alias-backed columns (Y-93)**: `variable_state` names only the state's own column,
  so a #319 monthly family (`LonFinkJan` … `LonFinkDec`) or a #945 co-delivered spelling
  was absent from the listing and the filter found nothing under the name the researcher
  knows. Two more reads — one aggregate over `variable_alias_window`, one listing of
  `variable_alias`, each with the same LEFT-JOIN pinning and its own per-register
  `SEARCH` — add them with the coverage each column is delivered over: a windowed column
  over ITS windows, which replace the base state's claim where the two name the same
  column (as `Catalog._expand_state_windows` expands it for the binding leaf); a column
  with no window over the variant's states. Both are joined to `variable`, never to each
  other: joining the window table to the alias table costs the join-order pinning and
  the plan degrades to a catalog-wide `SCAN v`. This is the `get_datacolumns` view of
  "delivered under" — every column of the alias history is a name to be found by — and
  NOT the resolver's: a browse row names columns rather than promising a resolution,
  which is why it keeps a window no state contains where `_expand_state_windows` drops
  it (what a steward can actually deliver is the boot gate's question, and that one
  reads the resolver — see reg_meta/DESIGN.md → Consistency gate against the catalog
  DB). Columns are identified case-insensitively (`py_lower`, the rule the build
  validates `variable_alias ⊇ state columns` with), so an alias that only re-spells a
  listed column is that one delivery.
- **Steward semantics**: for a filtered steward the deliveries are narrowed to
  `CatalogIndex.held_columns(fqid)` — the SAME held-column set the coverage recompute
  uses — so a partial-column hold names only the columns that steward actually holds.
  The index's grain is variant-blind, so the filter is on the column, not on the
  variant.
- **Additive**: `deliveries` defaults to `[]` and is populated only in the register
  listing payload; the SPA must tolerate its absence (a register node's own payload, or
  a child that predates the field).
- **The SPA's variant lens** (`CatalogNodeView`): the chips narrow the CHILDREN and the
  concept groups' members before `foldGroupedRows` (`narrowGroupsToMembers`), never the
  folded rows. A group row stands in for its members, so its count and its filter keys
  must answer for the delivered ones only; a lens that leaves a group with one member
  drops the group, and that member renders as its own leaf row. (The lens ends at the
  row: `groupHref` carries no variant, so the subject page it links to is unnarrowed.)
  The delivery-column cell and the filter's column keys read the SELECTED variants'
  deliveries too, so a narrowed list never shows — or matches on — a column that variant
  does not deliver. The chip set is read off the deliveries rather than the register's
  declared variants, so no chip can narrow the list to nothing.
- A chip is NAMED from `GET {register}/variants`, which `CatalogNodeView` fetches once
  and hands down to `VariantsSummary`: one page, one request, one spelling of a variant.
  The strip holds a skeleton until that list lands rather than painting slugs and
  swapping to names, which would re-flow it under the pointer; a FAILED load still
  renders the chips under their slugs, because losing the lens costs more than a
  machine-readable label. Chip order is by slug.

### Adding columns from the register list (Y-83)

The register list is an AUTHORING surface: every delivery column it names carries a
tick, and one action adds them all. The consumer is the researcher who knows LISA by its
columns — ticking `ForvErs`, `ForvInk`, `Kon` and `Alder` in the list beats opening four
variable pages to add one column each.

- **The tick grain is the column NAME the list shows** (`catalog.ts`
  `deliveryColumnRows`); the COMMIT grain is the variable's own picker row. A tick names
  a column, an Add maps that name onto the rows the variable's own page builds
  (`variablePickerRows` → `rowCoversColumn`), and `rowAddSegments` fans each of those
  out to ONE staged add per *(variable, concrete `register_variant`, column)* — the same
  per-concrete-segment fan-out (#376) the variable page performs. So the file a tick
  authors is the file that page authors, and the two grades of row the page holds (see
  the two bullets below) never leave it. The chip lens rides along, and it is CAPTURED
  WHEN THE COLUMN IS TICKED: a tick stores the concrete variants the list showed that
  column under, and an Add stages the intersection of those with the variants on screen
  when it is pressed, building the rows from the states of exactly those — the part a
  `?variant` modifier plays on the variable's own page (`narrowStatesByModifier`). The
  capture is the load-bearing half. The lens is live and a tick is not, so reading the
  lens at Add time instead would let a lens lifted in between widen the tick to a
  variant the researcher never saw, and a lens moved to another variant swap the tick
  silently onto that one. Bounded by both pages, a tick can never author a variant the
  researcher filtered away, in either direction: one the lens has moved off reads as
  UNTICKED and adds nothing until the lens that made it comes back — or until it is
  ticked again, which re-captures under what is on screen now. The capture is PER COLUMN
  and stays that way through the commit: a batch is grouped into one *scope* per
  distinct variant set (`stagedTicks`), and each scope builds its variable's rows on its
  own. Two columns of one variable ticked under different lenses must not pool their
  variants, because rows are matched to columns by NAME (`rowCoversColumn`) and both
  names usually exist in both variants — a pooled set would stage each column under the
  other's variants and quietly author four adds where the researcher made two.
- **The staging stack is shared, not copied.** `staged_picker.ts` owns the whole staged
  add → resolve → commit sequence (`stagedAddCandidates` → `applyStagedPicks`,
  committing through `projectStore.applyStagedDiff`), and `StagedAddStatus.svelte` is
  the one confirmation/refusal row. The binding leaf, the concept group and the register
  list are hosts: they own their own selection and scope and nothing else. It was two
  copies of the same forty lines before this ticket — the leaf-helper duplication
  CLAUDE.md names.
- **The study window IS the period here.** The list carries no Period control (that
  belongs to a subject page), so an add is clipped to the rail's window, and without one
  an open-ended column has no finite period to commit — the batch is refused whole
  before the store is touched, exactly as on a leaf. The nudge
  (`ADD_WINDOW_REQUIRED_MESSAGE`) names the one control this page has. Every refusal the
  page shows is a verdict on ONE Add under the window it was made under, so all of them
  retire the moment the window moves — one `addRefusal` slot rather than a flag per
  gate, so a verdict can never outlive the batch it refused. The refusal only: the ticks
  survive, so "add again" is one press. An Add is bound to the window it was pressed
  under for its whole round trip — the rail is not disabled while the era reads and the
  per-add resolves are out, so a window moved mid-Add ABANDONS the batch (`batchGuard`,
  asked again after every await, `applyStagedPicks`'s own included) rather than
  committing it under years the researcher has already left, beside a list redrawn for
  the years they chose. Abandoned, not refused: nothing is authored, nothing is claimed,
  and the ticks survive for an Add under the window now on screen.
- **A column the window has moved off is not tickable.** The list names every column the
  register ever delivered, so a window later (or earlier) than a column's whole history
  leaves it nothing to commit. Its tick is disabled and the row carries the reason. A
  subject page's picker only DIMS such a row and still lets it be picked, and
  `rowAddSegments` deliberately FALLS BACK to a row's whole span when the window clips
  it to nothing so that pick still adds something — that page has a Period control to
  say what. Here the window is the only period there is, so inheriting that fallback
  would author years the researcher never asked for: the page refuses the row before
  staging it, and the bar's count never promises a column an Add cannot commit.
- **The list's windows are display grade; an add re-reads the states.** `deliveries`
  carries a MIN/MAX coverage per (variant, column), which cannot express an
  interruption: a column delivered 1990–1999 and again 2010–2020 reads there as
  1990–2020. Printing that year range is what the list wants and costs no fetch per
  listed variable — committing it would claim years the column was never delivered in.
  So an Add re-reads each ticked VARIABLE's own states (`catalog.ts`
  `variablePickerRows`, one GET per ticked variable per scope — one scope unless the
  lens moved between its ticks — on top of the per-add resolve) and stages rows over the
  exact eras, which commit as the #307 comma-union exactly as the variable page's do. A
  variable whose states can't be read refuses the whole batch rather than falling back
  to the aggregate. The two grades can DISAGREE — a window inside an interruption passes
  a tick that only ever saw the aggregate — so the Add applies the window gate again to
  the rows that come back, and the confirmation counts the columns that actually
  committed. A batch left with nothing authors nothing and names the window it found
  empty.
- **A sequential RENAME is listed twice and committed once.** The list names every
  column a variable was delivered under, so `CDISP` and `CDISP5` are two tickable rows
  (that is what Y-82 shows, and the name is what a researcher hunts for). The variable's
  own page folds that chain into ONE `representation: null` row over the union (#902) —
  pinning either name would break the other's era — and an Add commits THAT row: both
  names map onto it (`rowCoversColumn`) and stage it once, whether one of them is ticked
  or both. Two consequences worth knowing. A tick of the retired name commits the fold's
  window-clipped union, which reaches into the surviving name's era — that is the
  representation the variable has, and per-period resolution reads the right column per
  year; afterwards BOTH names read as in the project, which is the same fact stated on
  the list. And the confirmation counts the ticked COLUMNS, not the rows: ticking both
  names of one chain says "+2 columns" over the one binding they share, because two
  columns is what the researcher ticked.
- **The action bar is sticky (Y-97).** Horizontal overflow for a wide table lives on
  `DataTable`'s own scroll wrapper (`overflow-x: auto`, `max-inline-size: 100%`), not on
  App's `.routed`, which stays `overflow: visible` — so the page scrolls on the viewport,
  the same scrollport every other sticky element on the site already assumes. `position:
  sticky; bottom: 0` on the bar now pins to that viewport, so scrolling any distance
  through a long list (LISA's ~740 variables) still leaves the count and the Add in reach
  at the bottom edge.

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
by binding FQID rather than resolved delivery column; registers come from the
inventory's period spans plus any admitted mapping's parent register. Drift-dropped
mappings do not inflate the variable count.

ETag + Cache-Control ride the generic `ETagMiddleware` (a GET read). `/api/stats` uses
the short `public, max-age=60, must-revalidate` tier: for a filtered deployment the body
depends on `inventory.toml`, so a same-id steward inventory redeploy must get a prompt
revalidation opportunity instead of letting the browser serve a stale count for 24h.

## ETag / Cache-Control (`etag.py` + `middleware.py`)

Every read endpoint (`/api/context`, `/api/stats`, the `/api/catalog` root + catch-all,
the 7 binding-suffix sub-endpoints) carries
`ETag: "<reg_meta_version>-<steward_id>-<sha256(body)[:16]>"` and a per-route
`Cache-Control` (`cache_control_for`) in three tiers: `/api/context` revalidates always
(see below); fold- or steward-dependent reads (`/api/catalog/*`, `/api/search`, and
`/api/stats`) keep `public, max-age=60, must-revalidate`; rebuild-stable doc-library
reads (`/api/docs/*`) keep `public, max-age=86400, must-revalidate`. A matching
`If-None-Match` yields a **304** with no body, but the current body-derived middleware
still executes the route and serializes the response first: it saves transfer, not
origin computation or latency. The pure logic lives in `etag.py` (`compute_etag` +
`etag_matches` + `cache_control_for`); an ASGI middleware (`ETagMiddleware`) wires it
DRY onto every GET read response.

**V1 early-revalidation correction (decision 2026-07-14; not implemented at this
head).** App code, catalog DB, delivery inventory, steward configuration, and docs DB
are immutable for a process lifetime; changing any of them replaces the process. At
startup, derive one content-backed generation token from those inputs. For known pure
GET reads, derive the validator from that token plus steward and the canonical request
identity, and satisfy a matching `If-None-Match` before route execution, DB work, or
body serialization. Keep the current body-derived path as the conservative fallback for
an unknown or mutable GET. This makes a 304 cheap without weakening exact representation
identity.

Browser and shared-cache freshness are separate concerns. Keep a short browser window
where prompt redeploy visibility matters, but let the Cloudflare cache retain immutable,
deploy-generation-keyed catalog/search responses for substantially longer without
synchronous origin revalidation at every browser expiry. A warm search must be served
without route execution. This complements rather than masks the bounded cold-query work:
arbitrary first-time queries still have to meet the origin budget. #1135's bounded SQL
path meets it, so no second in-process response cache is warranted.

- **`reg_meta_version`** is the INSTALLED `reg_meta.__version__` (the v1.x Model A
  package release), NOT the DB `schema_version` manifest. `steward_id` is
  `app.state.steward.id`.
- **The body-hash** makes `If-None-Match` per-URL coherent — the `?period` / `?variant`
  query is part of the URL, so it's already part of the cache key (different periods are
  different ETags).
- **`/api/context` revalidates always** (`Cache-Control: no-cache`, in
  `REVALIDATE_ALWAYS_PATHS`): the SPA vintage footer reads it to assert a specific
  deploy version/date, so a sub-24h-stale copy would *visibly lie* right after a deploy.
  The current ETag keeps an unchanged body off the wire; the early-validator correction
  above makes that path computationally cheap too. A deploy bump produces a fresh 200.
  Catalog and search endpoints use `max-age=60` because both embed the #322
  concept-group folds, which can change at the same browser URL on redeploy. A long
  browser-fresh copy would surface the old grouping to a returning user even though the
  edge generation changed; `/api/stats` shares that short browser tier because filtered
  counts can also change on a same-id redeploy. The body-hash ETag avoids retransmitting
  unchanged bodies, and `public` keeps the CF edge cacheable (the #220 probe survives);
  early validation is what removes repeated route work. Only `/api/docs/*` keeps
  `max-age=86400` — doc-library content is rebuild-stable and a sub-day-stale list is
  acceptable there; the ETag still guarantees correctness on revalidation. The edge
  worker (`reg_webapp/edge/`) defers to this origin's `Cache-Control` contract (it only
  stamps the `__edge_v` cache-generation param, orthogonal to caching policy), so the
  per-route policy needs no edge change.
- **Middleware skips WRITE endpoints** via a method gate: only `GET` reads are stamped,
  so the POST endpoints pass through with no ETag. It also skips non-200 responses — an
  error body isn't a cacheable representation, and handing the client a validator for a
  transient error would be wrong.
- We unit-test only the ETag / Cache-Control LOGIC + the 304 behavior. The **edge** side
  (Cloudflare edge caching / DDoS shielding / edge rate-limits) is a deploy/maintainer
  concern and not backend code. Remaining: edge config — see `REFACTOR_SPEC.md`.

## Production performance baseline (2026-07-14)

The table is the pre-correction production baseline that motivated #1135 and #1136.
Chrome 150 traces used browser-cold isolated contexts, 1× CPU, and no network
throttling. No CrUX field data was available, and DevTools emitted neither TBT nor Speed
Index; do not substitute Lighthouse values for the missing trace metrics.

  | Journey            | TTFB   | FCP    | LCP      | CLS   | Interpretation                                               |
  | ------------------ | ------ | ------ | -------- | ----- | ------------------------------------------------------------ |
  | Home `/`           | 319 ms | 548 ms | 546 ms   | 0.008 | Good; no homepage optimization lane                          |
  | Search `?q=person` | 66 ms  | 152 ms | 2,918 ms | 0.044 | Backend/cache wait dominates LCP                             |
  | ICD-11-SE detail   | 78 ms  | 184 ms | 264 ms   | 0.303 | LCP is the shell footer; classification content shifts later |

Search INP was 26 ms (0.1 ms input delay, 2 ms processing, 24 ms presentation), so the
interaction and subsequent DOM work are not the search bottleneck. The API took 2,759 ms
cold and 1,330 ms repeat/revalidated for only 6.9 KB compressed / 28.4 KB decoded; 97.7%
of LCP was render delay awaiting results. #1135 shipped bounded SQL candidates, stable
cursors, and bounded folding/backfill instead of full-result/count work. On its final
head, 25 distinct direct-origin requests measured 276.1 ms p95 (280.4 ms maximum), and a
browser-cold trace measured 380 ms LCP. The v1 regression budgets remain 500 ms
cache-miss p95 and browser-cold LCP below 2.5 s; warm edge hits must not execute the
origin.

ICD-11-SE transferred 542.5 KB / decoded 3.28 MB in 398 ms. The client sensibly grouped
17,159 `X` codes into only 675 DOM nodes, so DOM virtualization is not the missing fix;
the complete code corpus should not cross the initial detail boundary. One asynchronous
replacement contributed 0.295 CLS at roughly 709 ms, and repeat CLS remained 0.118.
#1136 made the canvas a flex column, let the routed region fill the viewport remainder,
added bounded catalog-loading skeletons, and reserved the multi-edition graph slot only
while it is pending or renderable. Exact-head ICD-11-SE traces then measured CLS 0.0616
cold and 0.0158 repeat (LCP 108 ms and 80 ms). Cold and repeat CLS < 0.1 remains the
regression budget. The separate 3.28 MB payload correction under `CodeList` is still
pending.

Content-hashed JS (\~103 KB), CSS (\~17 KB), and initially used fonts (\~130 KB)
currently revalidate, costing roughly 24–46 ms per main asset on repeat visits. Hashed
`/assets/*` responses must get a long-lived `immutable` policy through Workers Assets'
`frontend/public/_headers` support; `index.html` and SPA fallback documents remain
revalidatable. This is P2: render-blocking CSS cost only 26–39 ms and DevTools estimated
zero FCP/LCP savings from removing it.

## Steward layering and the in-memory catalog index (`stewards.py` + `catalog_index.py`)

A steward ships `stewards/<id>/steward.toml` (identity/branding) plus `inventory.toml` —
its **delivery inventory**, the single source of truth for what the deployment holds
(`reg_meta.inventory`, `REFACTOR_SPEC.md` §12). Each table has one explicit finite
edition and literal physical columns, and each column has zero or more mappings to
`(register_variant, variable FQID, canonical representation)`. Unmapped columns remain
in the physical coverage denominator without becoming admitted or orderable; several
mappings let a combined table serve several variants. That one inventory derives
edition-aware admission, browse unions and normalized order output — there is no second
holdings model. The **`global`** steward ships only `steward.toml`: the *absence* of an
inventory means full-universe mode (no filter, reg_meta's whole catalog), and a stray
one there is a boot failure, not a mode switch (`stewards.load_delivery_inventory`).

The in-memory **`CatalogIndex`** is built once at boot (`build_catalog_index`, from the
loaded inventory) and held on `app.state` for the process lifetime. It is the filter
that scopes a steward deployment to a subset of reg_meta's universe. It is an internal
frozen `@dataclass` (never a response body — only response models are Pydantic; webapp
internals are dataclasses), carrying three maps derived from the inventory's
`tables → columns → mappings`:

- `bindings_by_variant` — `register_variant` coordinate → frozenset of admitted
  `(binding FQID, resolved delivery column)` pairs. **Admission is column-based** (#206,
  decided 2026-06-11): a steward is given a concrete dataset, so its catalog is a
  statement of *holdings*, and holdings are physical delivery columns, not concepts —
  bare-FQID admission cannot express "this steward has SSYK, but only at the 1-digit
  level". The FQID side is the bare 3-segment binding FQID (no `@version` pin to
  normalize away — that grammar is retired); the column side is the **resolved**
  `delivery_column_name`. A mapping's `representation` IS that canonical token — never
  the physical `column.name`, which is the steward's own literal delivery spelling — so
  an explicit representation is admitted verbatim and boot performs **zero** catalog
  resolution (every SWECOV mapping is explicit). A `representation` of `None` states
  "the concept's *single* representation" (§12) and is **not** a wildcard: it is
  resolved against the catalog over the table's edition bounds, so a mapping authored
  before reg_meta grew a sibling column still compares equal to a researcher who must
  now pin. One edition can span a **rename**, so that resolution can answer with several
  columns — each admitted only over its own share of the edition (next bullet).
- `periods_by_coordinate` — the whole §12 coordinate
  `(register_variant, binding FQID, resolved delivery column)` → the ascending,
  non-overlapping union of the intervals every table stating it holds it *over*: the
  table's whole **edition bounds** for an explicit `representation` (the steward's own
  claim, trusted verbatim), and those bounds **clipped to the resolved column's state
  windows** for a `representation = None` mapping — `order.py`'s availability clip, run
  against an edition instead of a requested period. The clip is what keeps admission
  exact where the edition is coarser than the delivery: a table spanning a rename holds
  the old spelling before it and the new one after, never either across the whole run
  (and clipping per column, not per edition segment, is the only form that also splits a
  single *continuous* edition at the rename), while a table whose edition starts before
  the concept does is admitted from its first delivered day, not the edition's. A
  mapping states not only *what* the steward holds but *when*, and that "when" is per
  coordinate: one variant of a register can run 1990–2010 and its successor 2011–.
  Abutting intervals collapse (a column in a yearly table since 1990 is one interval,
  not thirty), disjoint ones do **not** — the committed SWECOV inventory has 1614
  coordinates with a real hole (the biennial innovation survey delivers 2002, 2004, 2006
  …), and flattening those to an outer span is precisely the loss this map exists to
  prevent. Retained for the order lane; the semantic validator does **not** gate on it —
  period coverage against the steward's physical deliveries is the order materializer's
  job (REFACTOR_SPEC.md §12).
- `period_range_by_register` — register FQID → the outer inclusive ISO `(lo, hi)` of its
  coordinates' intervals. Edition-aware by construction (an inventory edition is always
  one explicit finite period, never `_default`), but gap-free by construction too: it is
  the coarse **projection** of `periods_by_coordinate`, a best-effort span for UI
  hinting **only**, NOT a validity gate (the semantic validator's per-binding
  `period_outside_state_validity` is the gate).

The `global` deployment (no inventory) has **no** index (`None`); the catalog endpoints
pass through to reg_meta's full universe.

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

The `/graph` sub-endpoint gates the subject binding, then narrows variable graph nodes,
their state lists, same-as metadata, and edges to held FQIDs/columns. Classification
graphs remain catalog-global.

*Classification pass-through (decision 2).* Classifications and codes are
catalog-global. A steward inventory maps only variable columns, so there is no holdings
basis to scope reference data. Classification routes (`class/…`), the bounded value-set
code read (`/api/value-sets/{id}/codes`, including its `?state=` mismatch list) and the
codes arm of search pass through unfiltered for all steward deployments.

*Search.* `/api/search` passes `admitted_variable_fqids | held_register_fqids` as the
`fqids` allow-list to `reg_meta.queries.search`. This restricts register and variable
rows query-time at FQID grain. The webapp then refines variable groups at
delivery-column grain; for filtered stewards it fetches the full FQID-grain variable
result set once, drops all-unheld representation groups, and applies the display limit
afterward so the shown page backfills correctly. Classification and value/code surfaces
are unaffected. The golden-boost injection (`golden.apply_golden_boost`) is re-filtered
for the same set after boost so a curated pin the steward does not hold is dropped. The
`global` deployment (no index) is byte-for-byte unchanged.

*Performance.* The derived projections (`admitted_variable_fqids`,
`held_register_fqids`, `held_provider_slugs`, `_admitted_pairs`,
`_held_columns_by_fqid`, `_held_columns_by_variant`, `_variant_coords_by_register`) are
`functools.cached_property`: each is computed from `bindings_by_variant` on first access
and memoized for the process lifetime. `cached_property` coexists with
`@dataclass(frozen=True)` because the value is written into `__dict__` (no `__slots__`),
bypassing the frozen `__setattr__`; the generated `__hash__` / `__eq__` read declared
fields only.

**Boot-availability vs. drift.** A *structural* break in the committed inventory
(malformed TOML, an unknown key, a `_default` edition, a §12 one-to-one resolution
conflict) is a misconfigured deployment, so `load_inventory` fails fast before the DB is
even opened. reg_meta **drift** is different and must NOT crash startup: a
`representation = None` mapping whose FQID no longer resolves (`fqid_unresolved`), or
which reg_meta delivers no state for over the table's edition
(`period_outside_state_validity`), is DROPPED from the index — unauthorable until the
steward regenerates — and recorded in `drift_warnings`, which ride on `/api/context` so
the SPA can show a "catalog drift" banner. An **explicit** representation is trusted
verbatim by the index build; what checks it against the flavored DB is §12's
inventory↔DB consistency gate, which runs on the same boot connection right after the
index is built (see "The deployment's inventory" under the order adapter). The two
divide by PERIOD: the gate is period-agnostic, so a coordinate the catalog does not name
at all fails startup there — including the `fqid_unresolved` misses the index just
recorded — and the `period_outside_state_validity` arm is the drift that actually
reaches a booted deployment.

Filtered browse responses narrow concept-group members to held bindings/columns, then
recompute group tags and inherited binding tags from those surviving members. A steward
catalog must not surface a thematic tag that exists only on an excluded sibling.

Pre-v1, adding a proving steward is a monorepo PR (drop a directory, register the
hostname, rebuild). `REG_WEBAPP_STEWARD` selects which steward a process serves;
`REG_WEBAPP_STEWARDS_DIR` overrides the on-disk root for a packaged wheel/Docker image
(the `stewards/` sibling doesn't exist there). SWECOV is the first proving steward and
stays in-repo while testing the model, but that is not the release distribution shape:
before v1, extract SWECOV into its own steward repo/system and keep that system copyable
for later steward deployments. A real filtered steward inventory now ships:
`stewards/swecov/inventory.toml` (column-based admission derived from the delivery
topology; see `stewards/swecov/README.md` for provenance and coverage), and its
`data.swecov.se` deployment is wired. Remaining v1 work is extraction to the
steward-owned system. The SPA catalog-authoring mode and a `reg-meta-build steward-diff`
CLI are deferred post-v1; see `REFACTOR_SPEC.md`. V1 deliberately has no generic
per-steward extension surface.

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
value sets, tag memberships, and the variable-grain `same_as` / `lineage` edges), plus
the full variable `succession_chain` (#582, below). Register nodes likewise expose their
tag memberships for compact thematic chips. `lineage_warnings` are **omitted** —
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

**UI behavior layer: Bits UI** (`bits-ui@2.19.1`, the Svelte-5-runes major) is the
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
  scorer is disabled via `shouldFilter={false}`). **Superseded by the #991 cart model
  (#992/#993)**: `CatalogPicker.svelte` was deleted — `/project` is a read-only cart
  now, and authoring moved to the catalog subject page's own picker (see § The picker —
  slice axis × time axis). The friction note below is retained as a still-relevant
  caveat about `Command` + `ConceptGroupRow`, not as documentation of a live component.

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

The design language — palette, type, layout, elevation, shapes, component styling, and
the do's and don'ts — lives in **`frontend/DESIGN.md`**, a
[DESIGN.md-format](https://github.com/google-labs-code/design.md) file whose YAML front
matter is the normative token set and whose prose is the rationale.
`frontend/src/tokens.css` implements those tokens as CSS custom properties of the same
names, and the frontend test suite fails when they disagree. On any conflict about how
something looks, `frontend/DESIGN.md` wins over this file; this file keeps the
engineering decisions (the two-layer token architecture below, primitives' ARIA and API
contracts, tests).

Two layers, semantic roles only consumed by components: **primitive ramps**
(`--gray-1..12` and the fixed status/categorical/data hues — never referenced by a
component) and **semantic roles** (`--bg`, `--surface*`, `--text*`, `--border*`,
`--accent*`, the status, `--cat-*`, `--facet-axis-*` and `--viz-edge-*` roles, plus
geometry, elevation, focus and motion). Dark mode and the planned per-provider themes
are role remaps under `[data-theme]` / `[data-provider]` — no component CSS changes. The
tokens live in `frontend/src/tokens.css`, imported once in `main.ts`.

**Enforced deterministically** by `frontend/src/style_tokens.test.ts` (part of
`bun run test`, so it runs in the `reg-webapp-frontend` CI job and the yard `frontend`
gate with no extra wiring): every `<style>` block in every `.svelte` under `src/` must
be free of raw color literals (hex, `rgb()`, `hsl()`, `oklch()`, …) and raw
`font-family` stacks — a literal that renders identically today still escapes the
`[data-theme="dark"]` remap. `mask-image` declarations are exempt by rule (a mask is
alpha geometry, not palette). This exists because biome does not lint `<style>` blocks
inside `.svelte`, and because a screenshot cannot see a token bypass. It is a source
check only: it says nothing about hierarchy, copy, layout, or whether the right token
was chosen.

`main.ts` is the SPA entrypoint, but the `*.browser.test.ts` suite renders components
directly via `vitest-browser-svelte` and does **not** evaluate `main.ts` — so
`tokens.css` must **also** be imported from the Vitest browser setup (the `setupFiles`
path in `vite.config.ts`'s `browser` project). Otherwise token-dependent component
styling runs without the design-system variables/fonts and either breaks or silently
skips visual regressions.

### Shared primitives (Bits UI behavior + scoped CSS)

The recurring visual units live in `frontend/src/lib/ui/` (shipped in #804, exported via
`index.ts`): `Panel`, `DataTable`, `Breadcrumbs`, `Tag`, `Button`, `KeyValue`,
`Skeleton`, and `EmptyState`. `AppShell` shipped in #803 (rail + topbar command bar —
see § Surface & layout language). Behavior comes from Bits UI (`Command`, `Dialog`,
`Combobox`, etc.); styling is scoped CSS reading **only** semantic tokens. The #689
`Command` + folded-`ConceptGroupRow` keyboard-split caveat (`role="presentation"`,
originally observed in the now-deleted `CatalogPicker` — see § CatalogPicker / Command
friction) still stands wherever a future `Command`-hosted list meets grouped rows.

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
- **`DataTable` row navigation — link delegation, not selection.** Browse tables whose
  primary cell is a link opt into `rowNavigation`; rows stay in plain `role="table"`
  semantics with no row `tabindex` or `aria-selected`, and the anchor remains the only
  keyboard-focusable accessible link. Plain row/card clicks delegate to the first
  contained `a[href]`; clicks on nested controls, modified/non-primary clicks, and
  text-selection drags are left alone so browser/link semantics stay intact. Row hover
  (`--surface-hover` + pointer cursor) is the interactivity signal, so link hover
  underlines are suppressed inside this variant.
- **`DataTable` framed surface.** Use `framed` when the table itself is the whole
  grouped surface and its column headers should be the single top row. Do not wrap that
  case in `Panel` — a `Panel` title plus table headers creates duplicate heading rows.
  The framed variant keeps its header row visible at narrow widths and suppresses
  repeated per-card column labels. It also keeps narrow rows flat inside the table
  shell, not bordered/rounded cards; two-column framed tables collapse to one visual
  column with the second cell as secondary text under the primary cell. The surface
  still has exactly one visible heading row and one table layer.
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
- **`Tag` declares no font-family.** `frontend/DESIGN.md`'s front matter binds the tag
  primitive to the mono face, so the base `.tag` rule sets none and a tag takes its
  context's. A tag LABEL is usually English copy ("In project", "3 errors"), so the two
  contexts that are mono because they list identifiers — the register list's
  delivery-column cell and `SourceEditor`'s coordinate heading — set `--font-ui` on
  their OWN `:global(.tag)` usage. Putting the UI face on the base instead would re-face
  every tag in the app to fix those two, so the missing declaration is the decision, not
  an oversight. That the contract says mono while most call sites are copy is unresolved
  and filed as a follow-up.
- **Focus-ring convention.** Every interactive primitive applies
  `:focus-visible { box-shadow: var(--focus-ring) }` in its own scoped CSS — no global
  stylesheet owns this.
- **`.ui-btn` global hook.** `Button` delegates element rendering to Bits UI, so its
  variant/size styles are `:global(.ui-btn …)` — scoped through the `ui-btn` namespace
  this component owns, not a generic `.btn` that stray usage could inherit.
- **`.micro-label` global utility.** The label level (font-size, font-weight, muted
  color — sentence case at normal tracking, per `frontend/DESIGN.md`) is the design
  system's first cross-component global utility class, defined in `lib/ui/utilities.css`
  and imported in `main.ts` after `tokens.css`. It composes the `--micro-label-*` tokens
  — tokens remain the source of truth; the class de-duplicates the composed rule that
  was re-typed across seven components. A cross-component label can't be owned by one
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
- **`.cbox` global utility.** The app's checkbox face, in `lib/ui/utilities.css`. Every
  tick in the app is a real native `<input type="checkbox">` — the role, the keyboard
  control and the `:checked`/`:indeterminate` states are the platform's — and this class
  strips the OS chrome (`appearance: none`) and repaints the box in roles: `--border` on
  `--surface`, an `--accent` fill with an `--accent-fg` check when checked, the app's
  `--focus-ring` on `:focus-visible`, the app's dim when disabled. Without it a tick
  renders in the browser's own blue, a hue no theme can remap. It was
  `RepresentationPicker`'s scoped CSS until the register list grew ticks of its own
  (Y-83): a second consumer makes it cross-component, so it moved here rather than being
  re-typed — the same reason `.micro-label` lives here.

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
`/catalog/scb/lisa/variants`, `/catalog/class/<slug>`). The register sub-resource keeps
the API's own shape: a 3-seg path with a literal `variants` tail parses to the
`variants` route (the token is reserved in the variable slot at build time, so no
variable FQID can shadow it), never to a catalog node. The router is hand-rolled — no
routing-library dep — in `src/lib/router.svelte.ts` (a `.svelte.ts` module so its
reactive `$state` route compiles): it reads `window.location.pathname`, navigates via
`history.pushState`, handles `popstate`, and intercepts internal `<a>` clicks (the
`link` action) so navigation doesn't full-reload.

`route` is re-parsed — and so re-assigned — only when the **pathname** moves. A
query-only navigation (Apply on a catalog leaf's period card) parses to the same route,
and handing consumers a fresh object would invalidate every route-derived prop: the
query-independent catalog node refetches and the article remounts around the card the
researcher is typing in, dropping keyboard focus to the document body mid-Apply (Y-65).
What such a navigation does move is `search`, which the resolution fetches read. A
*genuine* refetch still swaps in the loading branch and takes its subtree with it — the
invariant removes the spurious refetch, not the teardown.

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
(`class/sun2020`), and a **concept group** (`group/scb/lisa/naringsgren`) — render
through one shell, `SubjectView.svelte`, so they share a single article wrapper, one
title/fqid header, and one **canonical section order**:

1. **description**
2. **picker** — slice axis × time axis
3. **value set / codes**
4. **relationships**
5. **docs**
6. **technical**

`SubjectView` is a thin *presentational* shell: it owns no data, no headings beyond the
title, and no restyling. Each section arrives as a Svelte `Snippet` from the leaf view
and the shell `{@render}`s the six in the fixed order. Every slot is **optional** — a
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
that fills the shell: `binding` → `BindingLeafView`; ungrouped `classification` →
`ClassificationLeafView`; grouped or family `classification` → `ClassificationGroupView`
with the requested edition tab active; and the `concept-group` (served by the fixed
`/catalog/group/{provider}/{register}/{key}` route, see Catalog router structure above)
→ `ConceptGroupView`. The classification subject route (served by the fixed
`/catalog/group/class/{key}` route) returns either `classification-group` or
`classification-family`, and both render through `ClassificationGroupView`.

Per-kind mapping into the six sections:

  | Section       | Variable (`BindingLeafView`)                                                                            | Classification (`ClassificationLeafView`)                        | Concept group (`ConceptGroupView`)                                                                                                            |
  | ------------- | ------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
  | description   | definition / description / unit `<dl>` + `via_same_as` note                                             | short name `<dl>`                                                | aggregated thematic tags, then shared definition/description (when members agree — #678/#900) above Technical details (key / facets / source) |
  | picker        | `PeriodPicker` (time) + `RepresentationPicker` (list or graph/time-band) + add-to-project               | `ClassificationEditionGraph` compact edition DAG (#906)          | `PeriodPicker` (availability lens) + `RepresentationPicker` (list or graph/time-band)                                                         |
  | value / codes | codings (`ValueSetView` (#905), each distinct value set via `CodeList`)                                 | `ClassificationCodesPanel` (`CodeList`)                          | —                                                                                                                                             |
  | relationships | `LineageDetails` (provenance/warnings); succession/group graph context lives in the picker              | derived classification links; edition succession lives in picker | — (members live in the picker)                                                                                                                |
  | docs          | `DocMentionsPanel`                                                                                      | —                                                                | —                                                                                                                                             |
  | technical     | one bottom `TechnicalDetails` disclosure (sensitive / identifier, plus single-state data type / column) | —                                                                | —                                                                                                                                             |

**#670 — member identity and fetch ownership.** For a grouped variable,
`BindingLeafView` renders a member-distinguishing qualifier (facet labels, e.g. "AGI ·
2007 SNI edition", falling back to the slug for edge-group split siblings) and a "member
of ⟨group⟩" context link directly under the header — both additive and gated on a
resolved `/graph` fetch. The fetch itself is owned by `BindingLeafView`, which derives
the qualifier (`qualifierFromFocus`) and group link (`groupLinkFromFocus`) from the
graph focus node's `facets` / `group_label` — no separate `/dimensions` request. The
same `/graph` fetch feeds `RepresentationPicker`'s graph/time-band mode; failure domain
is unchanged — a graph error omits the header qualifier and falls the picker back to the
compact list without affecting the rest of the leaf.

### The picker — slice axis × time axis

The picker section carries up to two orthogonal controls. The **slice axis** differs per
kind:

- **Variable** — the slice is the `register_variant` (variant/population). The
  subject-page picker lists concrete variant + delivery-column rows over the full state
  history and stages row adds/removes until the footer applies one project diff (#995).
  A pure time-sequential succession (one variant retiring into the next) is NOT a choice
  — it auto-splits into one source per segment — so the selector is invisible for an
  unambiguous variable.

- **Classification** — editions (`sun1996` → `sun2000` → …) are the slice. The
  `ClassificationEditionGraph` (#906) renders the `/graph` succession chain as a compact
  non-timeline DAG in the picker section: topology gives the horizontal order, branches
  open rows only within the affected rank, and edition cards navigate to non-current
  catalog leaves. It is read-only for now, not an add-to-project control.

- **Concept group** — the **column / representation picker** (`RepresentationPicker`,
  #678): one compact band per member variable, each listing that variable's delivery
  columns as selectable rows. A single-column variable collapses to one row; a
  multi-column variable gets a thin subheading (its distinguishing identity + a
  per-variable "select all") over its column rows. Each band identity links to the
  member's leaf page. When the group spans more than one distinct concept name,
  `clusterBands` (#901) groups the bands under `<h3>` name-cluster headings — each name
  renders once and each band leads with its within-cluster distinguisher (facet,
  delivery column, or member slug) rather than the repeated name. One shared staged diff
  footer spans all bands; it is always rendered from first paint (so its presence never
  shifts the layout), with its Apply/Reset controls disabled — or hidden where nothing
  is actionable — until something is staged, and it labels the action by diff shape
  ("Add to project", "Remove from project", or "Apply changes"). Each picker row is
  marked with the **kind** of dimension that distinguishes it from its siblings — a
  `facet` (a #819 `GroupAxis` value, per member), `variant` (variant/population), or
  `coding` (value-set version label) — and a **per-dimension filter strip** lets the
  user narrow a large multi-axis group to one axis value (#908, `pickerFilterDimensions`
  / `pickerRowPasses` / `PickerDimension` in `catalog.ts`). A dimension surfaces as a
  filter only when it discriminates (≥2 distinct values across all visible rows);
  single-value dimensions are invisible. Filtering is a client-side presentation lens: a
  hidden-but-selected row still commits, and the footer signals this. The filter logic
  mirrors the #819 `ConceptGroupNavigator`: OR within a dimension, AND across. When the
  group's graph is edge-bearing, small enough to draw cleanly, and maps every selectable
  graph cell one-to-one to the visible picker rows, the same picker may switch to graph
  / time-band mode instead of the list. The #908 dimension filter strip stays above
  either render mode; active filters narrow graph cells through the same filtered row
  model as the compact list. Leaf graph context with no selectable delivery-column row
  still renders in graph mode as unavailable context cells, so no-column bindings keep
  their succession/group context.

  Two **succession-collapse** folds ship in #902, both client-side and purely
  presentational:

  - **Intra-variable sequential-rename collapse** (`pickerRepresentations` /
    `coexistingColumns` in `catalog.ts`): one variable+variant whose delivery columns
    span NON-overlapping eras (`DINF` → `DINF83` → `DINF84` → `DINF86`) collapses into
    ONE picker row led by the latest-era column, spanning the union of all contributing
    windows. Earlier column names appear as a quiet inline hint ("was DINF, DINF83, …")
    via `renamedColumns` on `PickerRepresentation`. Genuinely parallel
    (overlapping-window) columns stay separate rows. The coexist-vs-rename test is the
    shared `coexistingColumns` leaf — also used by the binding-leaf editor's
    `representationsFromStates` chooser — so the two surfaces can never drift on the
    distinction. The #904 graph/time-band mode renders those eras as selectable cells
    when the graph gate chooses the graph renderer.

  - **Inter-variable succession fold** (`successionFold` derived in
    `ConceptGroupView.svelte`): the group graph's `succession` edges (#761 contract) are
    read to detect predecessor→successor chains where BOTH endpoints are group members.
    The superseded predecessor is dropped as a co-equal selectable band; the chain head
    (latest edition) leads and carries its predecessors as a "supersedes N edition(s)"
    disclosure (a closed `<details>` — oldest-first leaf links with the `effective_year`
    qualifier). When a predecessor has era-specific rows, those rows remain selectable
    inside the disclosure so old study windows can add the covering variable without
    making it a top-level peer. Partial chains — an edge endpoint outside the group —
    stay normal bands. The fold is purely client-side (`bands` derived reads
    `successionFold`) and requires no API change.

  The **graph/time-band render mode** (#904) consumes the same `RelationshipGraph`
  payload and the same picker rows. It is strictly additive: it is enabled only when the
  drawable graph projection stays under conservative node/edge/cell limits, every
  visible picker band has graph coverage, every visible picker row is represented, and
  every selectable graph cell maps to exactly one member column. The #908 filter strip
  remains above both list and graph modes; active filters narrow the graph to the
  visible member projection rather than hiding the filters or leaking filtered-out
  lanes. Otherwise the compact list is the authoritative picker. A variable leaf still
  passes its graph so the picker graph renders no-column, same_as-only, and
  sibling/predecessor context as unavailable cells when there is no safe selectable
  picker row.

The **time axis** is the shared `PeriodPicker` (see the Project-window store section): a
slider-only year-window control seeded from the project window and subject coverage. It
no longer exposes range, list, or free-text authoring modes. Richer server-supported
`?period` wires (terms, comma lists, `_default`) remain valid deep-link state; when one
is active, the picker displays that value read-only with a Clear affordance and does not
silently rewrite it unless the user moves the slider to a year window. The control does
a different job per kind:

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

- **`ValueSetView`** (#905, replacing the retired `StatesView`) — the pure value-set /
  coding viewer for a variable's `variable_state` rows. It is **presentation-only**: no
  fetch, no navigation, no resolution state. The `RepresentationPicker` now owns the
  `?variant` / `?value_set_version` narrowing (the `onpickVariant` /
  `onpickValueSetVersion` callbacks and the `inScope` / out-of-scope greying that lived
  in `StatesView` are retired). `ValueSetView` receives the already-resolved `states`
  list from `BindingLeafView` and renders in one of three modes:

  - **Single-state detail** (`states.length === 1`): variant, validity, non-empty
    value-set version, operational definition, and a height-constrained code table.
    Default variants, wholly unknown validity windows, empty version labels, and
    codeless value-set filler are omitted; state structural rows live in the binding
    leaf's bottom `TechnicalDetails`.
  - **Multi-state / distinct-value-set view** (`states.length > 1`, #668): dedups at two
    levels — classification editions by `classification_slug`, others by `value_set_id`
    — so a column with 415 states collapses to \~21 LKF editions + a few plain lists. A
    `FilterInput` narrows the list; per-row "Isolate" focuses one; "All value sets"
    resets. Period-greying (which value sets are in-scope for the current `?period`) is
    driven by a `scopeStates` prop from `BindingLeafView`, not by internal resolution
    logic (#744).
  - **Empty** (`states.length === 0`): a clean "no state delivered for this period"
    message (a valid resolved period outside every validity window — not an error).

  **`?codes=<variant>::<column>` deep-link** (#905/#1058): the picker's "codings vary"
  nudge became a deep link to `?codes=<variant>::<column>#states-heading`.
  `BindingLeafView` reads `?codes` into value-set focus state (pure view state — no
  refetch) and passes it to `ValueSetView`, which seeds the local isolation onto the
  distinct value set `valueSetKeyForColumn` resolves for that row identity. A folded
  graph cell passes the cell's era column, not the row's latest column. A stale or
  unknown column degrades silently to the default union view.

  The component is kept standalone (not folded back into the leaf) so the graph/picker
  surface can place the same coding display next to the selected representations without
  duplicating value-set rendering.

- **`CodeList`** (#638 PR3) — the single value-set / code viewer. A variable's value set
  and a classification's code list are the same shape (a code → label set, and a value
  set often *is* a classification), so they render identically: `ValueSetView` (#905,
  superseding the retired `StatesView`) uses it for each distinct value set in the
  multi-state view (and for the single value set in the detail mode), and
  `ClassificationCodesPanel` for the edition's codes. It owns a **size-dependent
  filter** (a search box appears only at ≥ `CODE_FILTER_THRESHOLD` codes — pointless for
  a handful), a height-constrained scroll, and the **large-list collapse** (#1120):
  levelled classification codes become drillable groups, prefix-shaped sets group by
  visible code prefix, and genuinely flat sets show a bounded preview with an explicit
  expand control. Classification conformance warnings render on the variable value-set
  surface, not inside the shared code list.

  **V1 payload correction (decision 2026-07-14; not implemented at this head).** A
  classification or value-set detail response does not embed its complete code corpus.
  Codes use a dedicated paginated, searchable endpoint with stable cursors; the detail
  payload carries summary metadata, authoritative level buckets, optional
  presentation-only prefix buckets where the classification explicitly supports them,
  and an initial bounded page only. Expanding a bucket or filtering fetches the matching
  page instead of downloading the corpus before rendering its grouping. Genuinely flat
  sets remain flat: do not promote `CodeList`'s current prefix heuristics to semantic
  hierarchy. The separate full export reuses reg_meta's existing complete-code export
  rather than adding a second exporter. The shared `CodeList` remains the renderer for
  pages from either owner surface.

- **`TechnicalDetails`** (#638 PR4) — the shared "Technical details" `<details>`
  disclosure that demotes **backend/structural** fields below the user-facing ones. The
  binding leaf owns a single bottom disclosure for the variable's sensitive / identifier
  flags and, when exactly one state is in view, that state's type / length / delivery
  column. Concept groups still use the component for key / facets / source. One
  component keeps the summary + styling consistent across call sites; callers omit it
  entirely when there's nothing to demote. `LineageDetails` follows the same
  omit-when-empty rule: with no provenance, warnings, loading state, or error, it
  renders nothing.

### Picker graph ownership (#904, #1057)

`RepresentationPicker` owns the rendered variable/concept-group graph context. It uses
the `RelationshipGraph` payload as a graph/time-band picker when selectable cells can be
projected onto the picker row model without dropping visible rows or leaking non-member
columns. Variable nodes lay out as horizontal representation-run cells along the shared
time axis; each selectable cell maps back to a picker row by variant + delivery column,
including #902's folded rename rows when that mapping is one-to-one. Curated
representation-grain succession (#888) rides the same graph edge set with source/target
column metadata and an optional variant scope, so a variant-local rename is visible only
when that variant's cells are in the current projection. In group mode the rendered
graph is the current visible member projection, so #908 filters can hide whole members
and still keep graph mode when the remaining projection is drawable. Cells that carry
leaf graph context but have no selectable picker row (for example no-column bindings,
same_as-only context, or sibling/predecessor context on a leaf) render as unavailable
context, not as checkboxes. The #908 dimension filter strip is shared by list and graph
modes. `LineageDetails` carries the variable-only non-graph residue
(`variable_state_lineage` provenance edges and `/lineage_warnings`).

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

- **Apps**: `reg-webapp-global` serves `catalog.swecov.se`; `reg-webapp-swecov` serves
  `data.swecov.se`. Each is a single always-on `shared-cpu-1x`/1GB machine in `arn`
  (Stockholm, where the users are). Always-on is deliberate: Fly's ephemeral-rootfs I/O
  is throttled (\~8 MiB/s), so a cold boot re-reads the SQLite pair slowly — keep the OS
  page cache warm rather than scale to zero (\~$6/mo per app). Config:
  `reg_webapp/fly.toml` / `reg_webapp/fly.swecov.toml`; `--ha=false` keeps each machine
  count at one.
- **Read-only SQLite on the ephemeral rootfs is the right model** — the DB pair is baked
  into the image and replaced with it. No volume, no LiteFS, nothing persists.
- **Deploys**: one workflow (`container-build.yml`) owns both origin apps plus the edge
  workers, scoped by a `changes` paths-filter job. Image-affecting main pushes
  (Dockerfile COPY surfaces + bake inputs — NOT baked deps reg_schema, which need a
  manual `workflow_dispatch` — decided 2026-06-11: that is the rule, not a gap) build,
  push to `registry.fly.io` (SHA-tagged), and `flyctl deploy --image` each affected
  origin. The bake build-arg is the RESOLVED newest `reg_meta/v*` tag (never `latest` —
  a literal `latest` makes the bake layer's buildx cache key insensitive to data-only
  releases and can even resurrect a stale cached layer after a pinned dispatch). The
  global Fly app uses `FLY_API_TOKEN`; SWECOV uses the separate app-scoped
  `FLY_API_TOKEN_SWECOV`. The SWECOV image also requires a matching
  `reg_meta_swecov.db.zst` asset on the resolved `reg_meta/v*` release. CI synthesizes
  the BuildKit JSON manifest (`tag`, `url`, `sha256`) from that release asset, hashes
  tag+digest into `SWECOV_REG_META_DB_REV`, and the Docker bake re-checks tag +
  downloaded digest before replacing `reg_meta.db`. The SWECOV metadata is
  non-confidential for the current testing steward, so the flavored DB is a public
  release asset rather than a secret-backed URL. The digest check keeps Docker's
  secret-insensitive cache from reusing a stale flavor layer and stops a public-docs DB
  from being paired with a mismatched flavored main DB. Nothing deploys without green
  CI: a `wait-ci` job polls this commit's ci.yml run and the origin/edge deploy jobs
  require its success — an image that builds but fails lint/ty/pytest never ships. Each
  deploy job carries a HEAD-of-main guard (GHA concurrency serializes by
  build-completion order, not commit order — without the guard an older commit's slow
  build could overwrite a newer deploy; it also makes non-main dispatches deploy-inert).
  Two gates guard a bad image: the entrypoint smoke gate (container exits non-zero
  before ever serving, and SWECOV sets `REG_WEBAPP_FAIL_ON_STEWARD_DRIFT=1` so any
  steward catalog drift warning fails the deploy) and fly.toml's `/api/context` HTTP
  check (flyctl reports failure if it never passes). Rollback: `flyctl releases --image`
  lists history; `flyctl deploy --image <old>` restores in seconds.
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
- **Cloudflare zone**: `catalog.swecov.se` (global catalog) and `data.swecov.se` (SWECOV
  flavor), orange-cloud A/AAAA → each hostname's matching Fly app shared IPv4 +
  dedicated IPv6, plus `_fly-ownership` TXT records (prove ownership behind the proxy)
  and grey-cloud `_acme-challenge` CNAMEs (DNS-01 cert issuance — the reliable path
  behind a proxy; never proxy a hostname pointing at `*.fly.dev`: Fly's edge has no cert
  for the custom SNI → 525). SSL mode Full (strict). No dedicated IPv4 — the free shared
  IPv4 works behind the proxy.
- **Edge workers** (`reg_webapp/edge/`, Workers free plan): static-assets workers on
  `catalog.swecov.se/*` and `data.swecov.se/*` serving the SPA `dist/` with
  `single-page-application` deep-link fallback. They use the same Worker source and SPA
  assets, but separate Worker names/configs so each hostname gets an independent
  `DEPLOY_VERSION` cache generation after its own Fly origin deploys. Backend paths
  (`/api/*`, `/openapi.json`, `/docs`) are `run_worker_first` + `fetch(request)`
  passthrough to the incoming hostname's zone origin (Fly), so the origin
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
- **V1 performance-probe extension (pending)** — add a representative `/api/search`
  MISS→HIT + `Age`/`CF-Cache-Status` assertion and prove its warm conditional request
  performs no origin route work. Probe static responses separately: emitted hashed
  `/assets/*` files must be long-lived `immutable`, while `index.html` and SPA fallback
  documents must revalidate. The original #220 path gate remains unchanged.
- **V1 programmatic boundary (decision 2026-07-14)**: the local agent/CLI reads the
  versioned DB and public delivery inventory directly, so it does not depend on the
  deployed API or impersonate a browser to evade zone bot protection. V1 deployment
  supports the SPA. If remote programmatic API access becomes a product surface later,
  admit a truthful toolkit User-Agent on the API paths under endpoint-specific rate
  limits and probes; do not document a fake browser header as the contract.

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
a **raw JSON dict** (not a typed param) because `/validate` must accept malformed specs
to diagnose them. Unknown top-level keys remain verbatim until the structural layer
reports each as `unexpected_field`; they are never normalized or dropped by typed model
construction first. OpenAPI documents the canonical closed `ProjectData` schema, while
the SPA keeps a raw diagnostic transport type so malformed uploads can reach this
boundary unchanged.

- **`/validate` status discipline.** A spec that FAILS validation is a *successful
  validation response* — **HTTP 200 with `ok=false` + the issues**. 4xx is reserved for
  a malformed REQUEST (non-JSON, duplicate JSON keys, a too-deeply-nested or non-object
  body, an oversized body). It runs the §6.8.0 composition (supported version →
  structural → semantic) and returns the **concatenated** issue list; the DB-free layers
  run first, so a rejected body costs no DB hit. The supported-version decision is
  reg_meta's shared `order.schema_version_issue` — the same one `/order` and
  `reg-meta order` gate on, so a project written for another schema contract gets one
  answer from every consumer — and it returns **alone**: the layers under it read the
  document as the current contract, which is the claim it just rejected.
- **`/order`** materializes the JSON order manifest and serves it as an `order.json`
  download (see below). Unlike `/validate`, it **gates** first: you cannot materialize
  an order from an invalid spec → 422.

**The order manifest (shipped, §12 lane 4).** `/order` is a THIN adapter over
`reg_meta.order.materialize_order(project, inventory, conn)` — the contract, the
pipeline, and every fail-closed finding live in `reg_meta/DESIGN.md` → "Order
materializer and manifest (`order.py`)", not here. The adapter owns exactly three
things:

- **The deployment's inventory**, read once at boot by
  `stewards.load_delivery_inventory` from `stewards/<id>/inventory.toml` and parked on
  `app.state.inventory`. The `global` deployment — the one with no steward configured —
  takes §12's global-deployment fallback (`inventory=None`, handed straight to the
  materializer) **unconditionally**; every other shape fails startup (fail fast): a
  NAMED steward with no inventory, the `global` deployment WITH one, a malformed
  inventory, or one declaring a different steward than the directory it sits in. A named
  steward booting into the fallback would reject every one of its own projects as a
  confusing `steward_mismatch` (the fallback demands `steward == "global"`) from a
  server that reported itself healthy — the deployment error would be deferred to, and
  paid by, each researcher in turn. Conversely, loading a stray inventory under `global`
  would silently swap the full universe it exists to serve for whatever that file lists;
  §12 keeps the fallback until a physical global inventory is introduced deliberately. A
  named steward's inventory is then checked AGAINST THE BOOT CONNECTION
  (`stewards.check_delivery_inventory` over `reg_meta.inventory_check`): §12's standing
  inventory ↔ DB consistency gate, which fails startup when any mapping's
  `(register_variant, variable FQID, representation)` does not resolve against the DB
  this deployment serves — a mapping that pins no representation still has its binding
  checked at its declared variant. The flavored DB and the committed inventory are cut
  separately and pre-v1 slug churn is legal, so they CAN drift — and unlike the catalog
  INDEX the same inventory builds there is no drift downgrade here: a mapping the index
  drops for its table's edition merely narrows the browse, while a coordinate the
  catalog does not name at all is a holdings claim about something that does not exist.
  The rules and the report shape live in `reg_meta/DESIGN.md` → "Consistency gate
  against the catalog DB (`inventory_check.py`)".
- **The download**: the 200 body is `OrderManifest.to_json()` VERBATIM (the handler
  returns a raw `Response`, which FastAPI passes through without re-serializing), so the
  SPA download and `reg-meta order` hand the steward byte-identical files — §12's
  equal-product-surfaces rule, pinned by a cross-adapter test. `response_model=` still
  publishes the reg_meta `OrderManifest` as the typed contract for the OpenAPI snapshot
  and the SPA codegen, so this is NOT a `response_model` carve-out.
- **The "not an order" status**: 422, never a partial 200 — for an invalid spec
  (`order.project_from_raw`, the gate both adapters share) and for a fail-closed blocked
  order alike. The body is the typed `OrderBlockedModel`: a `detail` line
  (`order.blocked_message`, the same text the CLI envelopes, kept because every 4xx here
  carries one) **and `findings` — reg_meta's own `OrderFinding` models**, each with its
  stable `code` and its `source` / `variable` / `period` coordinates. Fail-closed is a
  CONTRACT, so it ships as data at the boundary this API validates JSON contracts at: a
  flattened string would force every client to parse prose. `findings` is empty only for
  a spec the gate rejected before the materializer saw it. The SPA renders each finding
  through the SAME per-finding path as a validation issue (`ValidationPanel`;
  `validation.orderFindingPointer` resolves the materializer's by-VALUE coordinates to
  the by-POSITION pointer `findingLocation` already locates a card by).

**Still to come (decision 2026-07-11).** The SPA will expose one common study window as
the project-authoring default. When a source has any overlap, adding it will immediately
persist the full available intersection, including every disjoint segment; this will be
the default action, not a suggestion the user must accept. With no overlap, the picker
will block the add and explain the incompatibility rather than inventing a period. If a
later common-window edit leaves an existing source disjoint, its explicit period will
remain and the project will become blocking. The picker and project page will highlight
every divergence. The common window will never become hidden inheritance, and an
explicit apply-to-all action will rewrite only sources with an overlap.

Shared `reg_meta` project code still has to absorb the semantic pass (`semantic.py`
below); `REFACTOR_SPEC.md` §12 tracks that.

**Connection model = per-request open ON ONE THREAD** (the locked cross-thread guard).
`/validate` and `/order` are `async` only to read the body off the wire; the blocking
work (structural parse + per-binding sqlite resolution) is offloaded via
`run_in_threadpool`, and the reg_meta connection opens on **that** worker thread inside
a `with`-block — NEVER a generator `Depends` (which can run on a different AnyIO thread
→ `sqlite3.ProgrammingError`).

## Current semantic validation (`semantic.py`)

The current §6.8.3 reg_meta-backed validation layer lives in the webapp, not
`reg_schema`: `reg_schema` stays reg_meta-free and cannot resolve against a live DB.
`reg_schema` lists these codes as defined-but-not-emitted on its own surface. The webapp
currently invokes `semantic.py` with the structural and owning-package block validators;
it emits the same frozen `reg_schema.ValidationIssue` shape, takes a `Catalog`, and
leaves connection ownership to its caller.

This location is provisional. Order materialization has already moved to shared
`reg_meta` project code (`order.py`, above), which is what lets the FastAPI SPA adapter
and the local CLI execute one implementation; the availability/representation-slicing
decisions have followed it there (`order.resolve_binding`, consumed below), and the rest
of semantic validation follows on the same path. `reg_schema` remains independent; the
dependency direction is `reg_meta -> reg_schema`, never the reverse.

Rules, walking each source's `register_variant` + every binding:

- The `register_variant` coordinate resolves to a known variant; the binding `variable`
  (3-segment FQID) resolves to a known variable (following `same_as` links —
  `Catalog.variable_identity` does that). Unresolved → `fqid_unresolved` (error).
- **Availability is not decided here.** The source period is expanded
  (`order.requested_intervals`) and each binding resolved by the SHARED reg_meta pass
  the order materializer runs (`order.resolve_binding` — steps 1+2 in
  `reg_meta/DESIGN.md` → Order materializer); this layer only translates those facts
  into issues, so the two never disagree about what is available. §12 is intersection
  semantics: a binding is requested wherever it IS available inside the source window,
  so narrower availability — a leading/trailing shortfall, an **internal** gap, or a
  pinned `representation` that covers only part of the request — is an informational
  clip (`range_period_partially_covered`, naming the period actually ordered), never an
  error by itself. A clip is not a clean bill of health: the same binding can still
  block on representation/value-set ambiguity here, and on the steward's coverage gate
  at order time. A **#307 list period** (interrupted series; structurally sorted +
  disjoint, wire form comma-joined — `2005..2010,2015..2020`) is one request with holes,
  not a series of independent ones: it reports ONE clip for the whole request, and its
  holes are genuinely absent from the question — a request that skips a year is NOT
  equivalent to the range enclosing it, since a column co-existing only inside a hole is
  not ambiguity and a column delivered only inside a hole is not availability.
  Availability empty across the whole request still blocks. The pass's blocking findings
  map onto this surface's codes — `variable_unresolved` → `fqid_unresolved`,
  `binding_unavailable` → `period_outside_state_validity`, `representation_unknown` →
  `binding_representation_unknown`, `representation_ambiguous` →
  `binding_value_set_version_ambiguous`, and `representation_unresolved` under its own
  name. The kept states still carry the request instants they are available for, so the
  per-instant probes (the co-delivered-value-set backstop) keep segment precision
  without re-deriving the clip, and `binding_state_drifts_within_period` (info) reports
  a request spanning a sequential state transition. Steps 3+4 of the materializer (the
  steward's physical topology and its coverage gate) do NOT run here: a clean validation
  is a resolvable project, never a proof of physical order readiness.
- Resolved variable metadata can emit non-blocking hints. `deprecated_traversal` (info)
  fires when the binding resolves to a variable marked deprecated; the binding remains
  valid. `variable_replaced` (info) fires when a `variable_replaced_by` edge is
  effective at or before the requested period and carries `successor_fqid` when the
  successor resolves to a binding FQID.
- The binding's `value_set` (a `class/<slug>` FQID) resolves to a known classification →
  else `value_set_missing` (error).

**This layer reads identity and state metadata, never code membership.** So it takes the
narrow reg_meta reads (see `reg_meta/DESIGN.md` → Catalog API surface):
`Catalog.variable_identity` for the FQID and its replacement hints, and (inside the
shared pass) `resolve_at(..., with_codes=False)` for the states. The full `resolve`
would hydrate every historical state's code list to answer a question about one period —
on a geography variable whose yearly states share one large code list that is most of
the request. Diagnostics are unchanged: aliases, expanded monthly windows,
representation identity (`state_id`, `delivery_column_name`, `valid_from`) and code-set
identity (`value_set_id`) all come from the same code path.

**Representation, not `@version`.** A FQID names one concept, but a concept may carry
several **co-existing delivery columns** at the same instant — parallel representations
(SSYK 3/4/5-digit, age brackets). When ≥2 distinct delivery columns co-exist
(overlapping windows inside the REQUESTED instants — the shared pass's test, so a
sibling that overlaps only in a hole of a list period is not co-existence) and the
binding sets no `representation`, the extract would pull more than one column →
`binding_value_set_version_ambiguous` (error); the author must pick one via
`Binding.representation` (the delivery column name; the SPA offers a chooser). This is
exactly the job the retired `@version` pin used to do, now keyed on the delivery column.
A `representation` reg_meta no longer delivers as a column →
`binding_representation_unknown` (error). Crucially, the co-existence test keys on
**overlapping** windows: distinct columns in *non*-overlapping windows are a sequential
rename (drift), NOT ambiguity, and must not demand a `representation`. A separate
defensive backstop (`binding_value_set_version_ambiguous` on ≥2 distinct `value_set_id`s
on **one** column) should be unreachable against a clean catalog — the reg_meta build
enforces one value set per `(variable, variant, period, delivery_column)`.

**Onboarding.** Stewards declare a subset of what reg_meta knows; data without an FQID
can't be authored (no `{display_name + type, no FQID}` escape hatch in v1). New
variables/registers/classifications onboard via slug-TOML PRs against `reg_meta_build`;
once the next reg_meta release lands, the steward adds them to their inventory.

**Steward catalog filtering — `fqid_outside_steward_catalog` /
`representation_outside_steward_catalog`.** When a researcher's project references a
binding outside the loaded steward catalog, the column-based admission check (#206)
emits one of two **warnings** (not errors): `fqid_outside_steward_catalog` when the
steward holds *no* column of the concept, and the distinct
`representation_outside_steward_catalog` when the steward holds the concept but not the
column the binding **resolves** to — its message enumerates what the steward *does* hold
("available there as 'Ssyk1' only" is the actionable form of "not available"). These are
warnings during editing so an uploaded project can be inspected, but `/order` does not
consume them: the materializer runs its own fail-closed inventory/resolution gate and
blocks these conditions with its own findings, so a steward-catalog warning never
silently becomes an order. There is no cross- steward preview, retarget, or one-click
mutation feature: the active deployment is the validation target, and the user edits and
re-uploads the JSON if they intend to change it. The current check is wired into
`/api/project/validate`: `routes/project.py` threads `app.state.catalog_index` into
`validate_semantic` via `run_in_threadpool`; it runs **after** the per-binding period
resolution because the researcher side's resolved columns are what
`CatalogIndex.held_columns_for_variant(fqid, variant)` compares (when those are
indeterminate — unresolved period, unknown pinned representation, ambiguous multi-column
binding — the binding already carries its own error and only the FQID-level arm runs).
The `global` deployment (index `None`) never emits either code, and an unresolved
`register_variant` skips the probe entirely (it already earned `fqid_unresolved`;
holdings are keyed *by* variant, so there is nothing truthful left to say). Admission
keys on the **source's variant coordinate** — a mapping states a whole
`(register_variant, variable, representation)` coordinate, so holding `kon` under
`individer-15plus` admits nothing under `individer-16plus`, and the cross-variant union
would let an order through for a column the steward cannot deliver. It keys on the
literal binding FQID: a curated same_as sibling (e.g. `kon→syss`) names a *different*
physical column, so warning on it is correct under holdings semantics, not a keying
artifact. The variant-blind `admits` / `held_columns` probes remain the **discovery**
grain, backing the browse and search listings, which carry their own variant axis
(`held_variant_coords_for_register`).

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

## Browse-only authoring: the data-order cart model (#991, #992/#993)

The project editor was rebuilt around one rule: **the catalog is the only authoring
surface**. `/project` is a **cart** — it shows what has been picked and authors nothing
of its own but a source's **period** (Y-81, below). `ProjectEditor` / `SourceEditor` /
`BindingEditor` display each source's variant coordinate, period, and bindings
(variable, pinned representation) and offer only delete-per-row, that period, the
project's own name, and the New/Open/Download project_data.json/Download order.json
actions — validation runs automatically on every edit, with no separate Validate action.
A wrong variable, variant or representation gets fixed by picking again from the catalog
subject page, not by editing the cart row. `CatalogPicker.svelte`,
`PeriodEditor.svelte`, and `FieldIssues.svelte` — the general in-cart editing UI — were
deleted along with the store methods that only existed to serve them (`addSource`,
`addBinding`, `updateSource`, `updateBinding`, `applyPickedBinding`,
`bindingDerivation`).

A cart row holds **coordinates, not words** — a `register_variant`, a variable FQID, and
a `representation` only where a pick had to choose between co-existing columns — so the
words a researcher recognizes are READ from the catalog (Y-80). A source card is HEADED
by its register in the catalog's own spelling ("LISA", "MiDAS" — never the slug
uppercased), with the variant that names the population on its own line under the
heading: the title is COMPOSED in markup, never strung into one `A · B · C` line, which
frontend/DESIGN.md rules out. A `_default` variant names no population, so the register
heads the card alone. The owning PROVIDER is not part of that title — it is an attribute
of the register rather than something the researcher picked, and a second unlabelled
line under the heading could not be told from the variant — so it heads the card's
`KeyValue` metadata rows, labelled, and only where the deployment serves more than one
provider: a fact `App.svelte` reads ONCE and threads down like `steward`. The removal
dialog and the delete button's accessible name say the same thing as SENTENCES ("Remove
the LISA source (Individer, 16 år och äldre) and its 2 columns?") rather than reciting
the heading block. The coordinate itself stays on the card, in small mono. A column row
leads with the delivery column it orders: an explicit `display_name` first (reg_schema
makes it the binding's OUTPUT column name, so it wins even over a pinned
`representation`), then the pinned `representation`, otherwise the name resolved from
the catalog at THAT source's `(variant, period)` — where a column was renamed inside the
period the current name leads and the superseded ones trail it in mono. Every such read
goes through `catalog_names.svelte.ts`, a module-singleton cache keyed per read (the
catalog root; one per provider, which names every register it owns; one per register's
variant list; one per `(fqid, period, variant)`), so a hundred-column cart issues each
request once and a re-render issues none; the shell's own facet rail reads the root
through it too. A read in flight or a failed one — the root among them, since it is what
says whether a register name needs its provider — leaves the row showing the coordinate
it already holds: a machine coordinate is honest where an invented name is not. Nothing
read is ever written back into the draft.

This retires the \~400-line client-side **re-derivation engine** (`bindingDerivations` /
`rederiveGen` / `rederiveSource` / `applyResolution` / `applyDerivedResult` and the
per-binding `BindingDerivation` provenance markers) that used to re-resolve every
binding whenever its source's period or variant changed, with a clobber-vs-keep
heuristic to decide whether a re-derived value should overwrite an author's edit. Under
the cart model every field is **written once, at pick time**: the subject-page picker
stages concrete `(period, variant, representation)` rows with final `type` /
`representation`, then `applyStagedDiff` commits the diff in one mutation. Nothing
re-derives afterwards, so there is no provenance state to track and no clobber decision
to get wrong. A source's bindings can go stale relative to its period after the fact
(e.g. the author widens the period); that drift is the **server validator's job** to
surface (`range_period_partially_covered` for a widening past availability,
`period_outside_state_validity` when nothing is left,
`binding_state_drifts_within_period` across a transition — see § Semantic validation) —
the auto-validate flow that surfaces this on every edit is the sibling #994 (shipped —
see § "Browser storage + project-file persistence" below). `ValidationPanel` carries a
"Fix in catalog" link on each finding that resolves a catalog coordinate, so the
remediation path is always back to the catalog, never a cart-side patch.

The one field a pick does NOT write is `display_name`, which it leaves **absent**. The
field is optional — an absent one resolves to the reg_meta default from `variable_alias`
— and stamping the delivery column onto it made two disjoint-era bindings of one
physical column (`forvink-ers-aktiv` 1990..2021 and `forvink-ers` 2022..2023, both
`ForvErs`) collide under reg_schema's per-source `display_name_collision`, though the
two never coexist. The rule is deliberately left as it is rather than made period-aware:
the structural layer cannot see periods by design, and the rule still earns its place on
hand-authored specs that set explicit names.

Opened project files are held **verbatim** in the store so serialize/validate see the
same malformed structure the backend diagnoses. The SPA's read side uses one
`project_data.ts` safe-source-slot seam instead: non-array `sources` renders as empty,
while malformed `sources[]` slots normalize to `null` for rendering/validation-display
and remain counted so `/sources/{i}` anchors line up with backend issue paths. Store
mutators that inspect source fields use the same accessors over the raw slots, so an
untouched malformed slot is preserved until the user deletes it or replaces the source
array through an explicit structural edit.

The commit primitive is `applyStagedDiff({adds, removes, periodChange})` — the
browse-and-stage flow accumulates a user's picks/removes as a diff and commits it in
**one atomic synchronous store mutation**, so autosave and the stable-id mirror fire/
rebuild once per commit rather than once per pick (this is the write path the #995
multi-select consumer commits through). Find-or-create for a staged add keys on
`register_variant` **alone** — not `(register_variant, period)` — so two picks against
the same variant land in the same source regardless of period; a disjoint window is
folded into the existing source's period via `mergePeriods` (`period.ts`) rather than
minting a second source: when both periods are pure year grammar it coalesces into the
sorted, disjoint #307 list form (adjacency-merging touching/overlapping intervals),
otherwise (either side is token grammar) it REPLACES with the incoming period, since
mixed-grain union has no defined sort. `applyStagedDiff` is now the SOLE catalog→project
mutation path (the store's earlier single-pick `addFromCatalog` handoff was dead since
#992/#993 and was deleted in #1104). `updateField` (the project's own `name` /
`window`), `removeSource`/`removeBinding` and `applySourcePeriodEdit` are the only other
mutators the cart UI calls — a source's own generated `name` is not editable anywhere.

Bindings, variant and representation are written once at PICK time; a source's
**period** is the one field the cart edits (Y-81). It is a field of the SOURCE rather
than of any single pick — a study window of 2005..2020 with one register reaching back
to 1990 is an ordinary order — and the `/project` source card is the only surface that
shows a source WHOLE: its full coordinate, its stored period and every column it
carries, including columns no one catalog page lists. That is what a source-wide rewrite
has to be looked at against, so that is where it is made. The card authors it in the
catalog's own period vocabulary — two exact-year fields and one Apply, the same entry
`PeriodPicker` carries beside its slider — and writes back through the same wire shaping
a pick uses (a bare year when the bounds meet, `{from, to}` otherwise). A period the
year fields cannot express (a token like `HT2018`, or the #307 comma list two disjoint
picks merge into) is shown as it stands and never silently collapsed into a span. Where
the period differs from the project's `window` the card MARKS it ("Differs from study
window 2005–2020"): the window is an authoring seed, not an inheritance (reg_schema puts
`period` on `Source` alone), so divergence is shown rather than warned about — whether
the window is actually left uncovered is a validation finding and has one.

The rewrite goes through `applySourcePeriodEdit`, keyed by source **name** as well as
coordinate: a draft may carry several differently named sources on one register variant
(reg_schema makes names unique, not variants), and an edit moves only the one it names.
It is its own period-only `applyStagedDiff`, never unioned with staged adds, and it
carries the edited source's complete value plus the draft's `replacementGeneration`.
Both are re-checked through one store-internal predicate immediately before the write: a
source that moved, or a project that was replaced, under an open edit refuses the write
instead of overwriting it, and the card says so. Ordinary browsing still stages nothing
— changing years, filtering rows or following a `?period` link leaves the draft alone,
and `RepresentationPicker` keeps deriving `periodChanges = []` because a partial
leaf/group cannot infer a source-wide rewrite from the columns it happens to show.
Coverage and type drift after the edit stay the server validator's job, as above. (Y-81
retired the catalog-side Y-15 correction that used to do this from a "Project sources on
this page" box on every leaf and group page: the catalog surface had to re-state a
source the cart already shows whole, and nobody found it there.)

## Browser storage + project-file persistence (the SPA store)

Project files live in the **browser** during a session and as JSON in the user's git
repo for durability. There is **no server-side storage** — git is the durable store,
email/git-sharing handles collaboration (server-side projects are a possible v2
feature). The authoring store (`project_store.svelte.ts`) is a module-singleton Svelte 5
rune store holding one draft per session.

- **An APPLICATION-owned lifecycle.** `initDraftLifecycle()` wires the restore, the
  autosave and the automatic validation, and is called **once**, at the app's reactive
  root (`App.svelte`) — never by a route. The draft is authored from the catalog and
  only read at `/project`, so a route-owned lifecycle both loses a catalog-authored
  draft (nothing restores or autosaves until `/project` is visited) and, with a second
  caller, saves and validates each edit twice. The restore is asynchronous, so it is
  exposed as `projectStore.restored` and the catalog's Add path **awaits** it before it
  creates or mutates a draft: without that gate, a cold entry at `/catalog` reads the
  still-empty store, mints a second project, and later overwrites the saved one. Only a
  restore onto an empty store applies, so a late restore never overwrites a deliberate
  new/open — and, symmetrically, a pick queued behind the gate stays bound to the
  project it was staged against: it is discarded (never applied) if a deliberate
  new/open replaced that project, or the authoring page was left, while it waited. The
  generation is captured at the PRESS, not when the store call starts — a page that
  reads anything asynchronously first (the register list re-reads each ticked variable's
  delivery eras) would otherwise leave a window in which a new/open lands unseen and the
  pick commits into the replacement after all.
- **Autosave to IndexedDB** (`indexeddb_persistence.ts`) over the raw IndexedDB API (no
  `idb` dep — keeps the frontend dep surface lean) via a debounced (\~500ms) `$effect`.
  **Graceful degradation is mandatory**: in private mode / disabled storage / quota
  failures, `save` resolves and `load` resolves `null` so the app keeps working
  in-memory — autosave NEVER rejects or crashes the effect. A restore that fails anyway
  settles the gate as "no restore" rather than rejecting it, so a broken IndexedDB
  degrades to in-memory authoring instead of wedging the next pick.
- **Store-schema stamping + gate.** Each persisted draft is stamped with the store's own
  `storeSchemaVersion` (distinct from the project's `schema_version`); `load` restores
  only on a match, else discards the stale-schema draft. This is the store's record
  shape, bumped only when the persisted shape changes.
- **Project-file version gate.** Model A files carry the reg_schema MAJOR — **3** since
  `Source.period` became finite-only (reg_schema `3.0.0`); the major **2** files written
  before it are Model A too, but the BACKEND reads `3.0.0` EXACTLY
  (`order.schema_version_issue`), so it answers them `unsupported_schema_version` like
  any other foreign contract. Plus the deployment's `reg_meta_version` release tag. The
  SPA **hard-rejects** a file whose `schema_version` major is **1** (pre-Model-A) with a
  blocking open-error — no migration, pre-v1 policy. The `reg_meta` package may still be
  `reg_meta/v0.x` while the schema is Model A, so `reg_meta_version` major is not a
  pre-Model-A signal. (`schema_version` major 1 is the *rejected* pre-Model-A value, not
  Model A.) Any other version — including the seeded major **3** — is a neutral no-op:
  the backend stays the canonical validator.
- **Unsaved-changes warning.** A `dirty` flag derives from the draft diverging from the
  last DOWNLOAD baseline (`lastDownloaded`); a `beforeunload` listener prompts on a
  tab/window close with a dirty draft. The store drives the write endpoints (validate /
  order download) through `lib/api.ts`; it is NOT a structural validator (the backend is
  canonical).
- **Deliberate replacement of a dirty draft.** `beforeunload` covers leaving the tab; it
  does NOT run for the in-app New and Open, which replace the draft *and* the single
  IndexedDB recovery copy behind it. Both therefore go through one policy
  (`requestNewProject` / `requestOpenProject`, which hold the incoming project as DATA —
  never a callback): over a dirty draft they raise a single confirmation (a Bits UI
  `AlertDialog`, the app's only modal) offering cancel / download-then-replace / replace
  anyway, and the replacement runs only on a confirm. Nothing about the draft moves
  while that answer is pending, so a cancel leaves the draft and its autosave untouched.
  Open PARSES first (`parseProjectText` — JSON, top-level object, version gate) and asks
  only once the file is one that could be loaded: a cancelled file picker, a
  parse/version rejection, or a refused replacement all leave the current draft exactly
  as it was. A restored autosave stays dirty (a recovery copy is not a downloaded one),
  so it gets the same confirmation — the flag is never cleared to skip the policy. The
  pending project is dropped when `/project` unmounts, so an unanswered question can
  never outlive the page that asks it. Reading a picked file's bytes is the one
  asynchronous step, and `/project` carries a generation counter that a New, a newer
  Open and its own teardown all bump: a read that loses that race is dropped BEFORE the
  ingress runs, since the ingress is what raises the open-error — otherwise a stale file
  could still replace a newer decision, or put a stale banner over it.

Note: v1 is **one draft per SPA session** (a single IndexedDB key), not a multi-project
list — a new or opened project replaces the current draft, deliberately (above).

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
one. An opened project (an open / restore) keeps its own `window` unchanged. The rail
slider also exposes an explicit ✕ clear control that writes `null` back to the store,
making full history reachable at any time after the first interaction. Filtered steward
deployments seed the rail and per-page picker bounds from
`/api/context.steward.catalog_period_span` (#1037), a best-effort year span derived from
the steward index and clamped to the catalog vintage. The global deployment and
unparseable steward periods fall back to the fixed 1960 → catalog-vintage bounds.

## API surface

The committed `backend/openapi.json` is the canonical contract; this table is the
orientation map. All endpoints are under `/api/`; read GETs are edge-cacheable, write
POSTs are not. Catalog browse paths use FQID segments directly.

  | Method | Path                                             | Purpose                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
  | ------ | ------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | GET    | `/api/context`                                   | Deployment identity, branding, build info, catalog-drift warnings.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
  | GET    | `/api/catalog`                                   | Top-level: every provider the steward exposes + the `class` root.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
  | GET    | `/api/search`                                    | Global FTS search → bounded typed groups (`top_results`, `registers`, folded `variables`, `classifications`, `classification_codes`, `register_value_sets`); extensible, with unknown groups skipped by the SPA. Each emitted group carries `has_more` and an opaque `next_cursor`. `?q=` is required; `?limit=` caps each group; `?type=` scopes the response (`all` default); `?cursor=` continues the requested context-bound page. Documentation is not rendered in global search.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
  | GET    | `/api/docs/search`                               | Docs FTS search (excerpts + source pointer), optional `?register=`; `ingested=false` when no docs index.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
  | GET    | `/api/docs/doc/{identifier}`                     | One doc by variable/filename — metadata + source pointer + bounded excerpt (never full body).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
  | GET    | `/api/docs/for-variable`                         | Parsed-document hook: fuzzy name/`provider_key` matches + `register_ingested` coverage flag.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
  | GET    | `/api/catalog/{fqid}`                            | Single endpoint for every hierarchy node (`kind`-discriminated). On a binding leaf, embeds the variable's full longitudinal record (every state's identity, window and coding REFERENCE + a cardinality-independent `value_set_summary` — members come from `/api/value-sets/{id}/codes`, Y-46) + its full variable `succession_chain` (#582). At the current head a classification leaf embeds its succession chain, complete value-set `codes`, curated `dimensions`, and optional derived `family` (#1116); the v1 `CodeList` payload correction removes the complete codes in favor of bounded metadata/buckets plus the dedicated code-page/export paths. Optional `?period` / `?variant` / `?value_set_version` narrow a binding leaf to a `{binding, states}` subset (uniform with `/states`). A dead/renamed binding, register, or classification slug with a successor 301-redirects to its terminal successor (kind-dispatched — #355 PART 2, #412, #571); `?period` branch and sub-endpoints also redirect (#411). |
  | GET    | `/api/value-sets/{value_set_id}/codes`           | One coding's `(code, label)` members, one bounded page at a time (`?offset`/`?limit`, default 200/max 1000, clamped). `?q=` filters the COMPLETE set before the window (browser-identical case/diacritic folding); `total` is the filtered total. `?state=` reads that state's stored classification-mismatch list instead, 404ing unless the state carries the value set. (Y-46)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
  | GET    | `/api/catalog/{provider}/{register}/variants`    | The register's variant browser.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
  | GET    | `/api/catalog/group/{provider}/{register}/{key}` | The concept group as a browsable subject (all members; `?member=` focus). (#617/#616)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
  | GET    | `/api/catalog/group/class/{key}`                 | The classification subject route: curated umbrella group (`ClassificationGroupNode`) or derived one-dimensional succession family (`ClassificationFamilyNode`). (#756/#1116)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
  | GET    | `/api/catalog/{fqid}/states`                     | Full state history for a binding. Dead/renamed binding 301s to `/states` on its terminal successor (#411).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
  | GET    | `/api/catalog/{fqid}/predecessors`               | Inbound `variable_replaced_by` edges. Dead/renamed binding 301s to `/predecessors` on its terminal successor (#411).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
  | GET    | `/api/catalog/{fqid}/successors`                 | Outbound `variable_replaced_by` edges. Dead/renamed binding 301s to `/successors` on its terminal successor (#411).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
  | GET    | `/api/catalog/{fqid}/lineage`                    | Materialized `variable_state_lineage` edges (consumer ← source). Dead/renamed binding 301s to `/lineage` on its terminal successor (#411).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
  | GET    | `/api/catalog/{fqid}/lineage_warnings`           | Linker-emitted lineage coverage warnings. Dead/renamed binding 301s to `/lineage_warnings` on its terminal successor (#411).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
  | GET    | `/api/catalog/{fqid}/dimensions`                 | Concept-group dimension memberships containing this variable (the variant facet groups: level/population/rank/…). Dead/renamed binding 301s to `/dimensions` on its terminal successor (#411).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
  | POST   | `/api/project/validate`                          | Three-layer validation; 200 + `ok` + issues.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
  | POST   | `/api/project/order`                             | The materialized JSON order manifest, downloaded as `order.json`; 422 (`OrderBlockedModel`: `detail` + typed `findings`) when the result is not an order.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |

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
- **Per-steward repo autonomy.** SWECOV lives in this monorepo only as the proving
  steward for pre-v1 testing. Before release, extract it to its own steward repo/system
  and keep that shape reusable for later stewards; do not let the in-repo SWECOV
  directory become the permanent distribution contract.
- **Realign-patch lifecycle** (gated behind the unbuilt merged-mode realign flow).
  Whether the realign-review UI writes an accepted patch back into git automatically or
  just produces a new `project_data.json` the user replaces manually.
