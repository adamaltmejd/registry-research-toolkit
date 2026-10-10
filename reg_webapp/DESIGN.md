# reg_webapp — design

Svelte SPA on the Rust server (`reg-meta serve`), which answers every `/api` route: the
catalog reads (context, search, docs, the catalog pages) and the project operations
(validate / order). The SPA is the researcher's authoring client. There is no Python web
server: `RUST_RUNTIME_SPEC.md` package F deleted the FastAPI app. This file records the
package-local design rationale. Cross-cutting topology (package tree, dependency graph,
perf budgets, version policy, testing-strategy overview) lives in the root
`ARCHITECTURE.md`; remaining/unbuilt work lives in `REFACTOR_SPEC.md`. The API contract
is `conformance/api/operations.toml` with its snapshot `crates/reg-meta/openapi.json`.

**Compiled holdings.** `ARCHITECTURE.md` and `crates/DESIGN.md` own the compiled
contract. The webapp admits one immutable catalog or steward artifact and delegates
scope-sensitive reads to the shared SQL reader. Steward configuration supplies branding;
it does not supply holdings. The optional SPA scope switch is deferred. HTTP browse
clients can request reference or holdings scope explicitly.

## Why no auth — cost protection instead

The data is public-ish registry metadata; there is **no server-side user-private state**
(project files live in the browser, never on the server). "Auth" here is really cost
protection, on two axes: read GETs are edge-cacheable + ETag-revalidated (cheap), and
the actual-work POSTs carry an origin-side body-size cap + per-client rate limit (Cost
protection, below). Real auth is a v2+ concern, layered on only if a steward ever needs
private data.

## Layout

```text
reg_webapp/
  .claude/skills/run-reg-webapp/  # dev.sh, the browser flows, fixture_db.py (the
                                  # synthetic catalog + docs DB pair, dev.sh --fixture-db)
  frontend/               # Svelte 5 + Vite + TS SPA (bun-managed)
    src/lib/api-types-rust.ts  # codegen'd from crates/reg-meta/openapi.json
  stewards/               # per-steward branding, read by `reg-meta serve --stewards`
    global/steward.json    # identity only; no catalog → full universe
  DESIGN.md
```

`stewards/` is deployment data, not server source: `reg-meta serve --stewards DIR` reads
`DIR/<catalog>/steward.json` and nothing else.

## Boot seam (artifact admission)

The Rust server admits its catalog at startup and refuses to serve otherwise
(`Catalog::open` in `crates/reg-catalog`): the schema gate (`schema_incompatible`), the
artifact identity (`catalog_unpublishable`), and the selected `--catalog` against the
artifact's own name (`catalog_mismatch`; a steward artifact never serves as `global`).
The `api` corpus pins each refusal (`conformance/cases/api/admission-*`). The branding
comes from the admitted catalog's name, so a deployment cannot pair one steward's
artifact with another's branding. The webapp ships no DDL and owns no `SCHEMA_VERSION`.

## Catalog pages (the Rust server)

The SPA's catalog pages read the Rust server (`reg-meta serve`): `show`
(`GET /api/catalog` for the root, `GET /api/catalog/{ref}` for every other node) and its
facets `states`, `warnings`, `values`, `graph` and `lineage` (`GET /api/<facet>/{ref}`).
Every response is `{data, meta}`; an error is `{error: {code, message, fields}, meta}`.
The contract is `conformance/api/operations.toml`, the SPA's types are generated from
`crates/reg-meta/openapi.json` into `frontend/src/lib/api-types-rust.ts`, and the
"Catalog surface" section of `frontend/src/lib/api.ts` is the one place the SPA calls
them. Locally the Vite dev proxy sends these paths to the Rust server. Ref and period
grammar, scope, cursors and their located errors live in `crates/reg-catalog`; the SPA
renders the error message rather than re-validating.

- **Route paths mirror `show`'s refs.** The page at `/catalog/<ref>` loads
  `show(<ref>)`. The group pages `/catalog/group/<provider>/<register>/<key>` and
  `/catalog/group/class/<key>` use the group refs `group/<provider>/<register>/<key>`
  and `group/class/<key>`, so the SPA path and the API path are the same string. `show`
  answers a `kind`-tagged node: `root`, `provider`, `register`, `variable`,
  `classification_root`, `classification`, `concept_group`, `classification_group` or
  `classification_family`.
- **A retired ref resolves to its terminal successor, without a redirect.** `show`
  answers the successor's node, and its `data.fqid` is the canonical FQID; the page uses
  that FQID for every facet call. A split (more than one terminal) is `ambiguous_ref`
  (409), shown in the page's error banner.
- **The variable page fetches its facets separately.** `CatalogNodeView` loads the
  variable's full state history (`states`) before it renders `BindingLeafView`. The leaf
  then fetches the `?period` subset (`states` narrowed by `period`, `variant` and
  `value_set_version`), its data warnings (`warnings`) and its relationship graph
  (`graph`, which carries the succession chain); `LineageDetails` fetches `lineage`
  (edges and warnings, one failure domain). Each facet is its own `asyncResource` with
  the loading and error affordances of its neighbours, so one failed facet never blanks
  the page.
- **Register pages embed their variants.** A register's `show` carries `children` (each
  with `coverage` and `deliveries`), `groups`, `tags` and `variants`. The variant
  browser (`/catalog/<provider>/<register>/variants`) and the variant chips read that
  embedded list; there is no separate variants call.
- **Classification pages.** A classification's edition timeline is its `family`'s
  `editions` when it belongs to a derived family (#1116), else its `graph`. Its codes
  come from `values`; `dimensions` names the curated umbrella groups it belongs to.

**States carry coding identity, not code membership (Y-46).** A state carries its
window, variant, delivery column, `value_set_id`, `value_set_version_label` and
conformance verdict, plus a `value_set_summary` (`code_count` and the dense-integer
`integer_range`), never its members. Embedding codings per state made the
`scb/rtb/forsamling` page 24 MB. Members come from `values`: a classification ref pages
its codes; a variable ref with `state=<state_id>` pages that state's value set, and
`classification` with `partition` (`source_extensions` by default, `canonical`,
`nonstandard`, `sentinels`) or `column` with `alias_window_from` selects one part.
Paging is by cursor; `q` filters the whole set before paging and `total` counts the
matches. The SPA's code panel owns paging, the filter and its loading, error and empty
states, and is mounted only where a code table is shown, so a closed disclosure issues
no request. A state whose coding has `code_count` 0 says "This value set has no codes"
in place, without a read: an empty coding and no coding are different facts. Inside the
panel `CodeList` renders each page as given (a page is a window, so the viewer's own
grouping is off), and typing is debounced into one read.

Storage IDs, `state_id` included, are decimal strings on the wire. The browser never
converts them to JavaScript numbers; comparisons use integer-safe ordering.

`distinctValueSets` groups source domains by `value_set_id`, preserving first-seen
order. A shared declared book does not make distinct source domains identical. Every
book remains linked, including a declared book with nonstandard codes. Conformance
notices retain each exact state, column window and book; counts describe the precise
partition each disclosure opens. In-period notices follow the selected window and
variant; whole-history usage spans and explicit out-of-period domains remain available.
Known coverage and unknown bounds remain distinct. An exclusively year-independent
selection hides the leaf's annual availability control while preserving the project's
study window; these deliveries do not acquire invented observation years.

**Data warnings** are catalog evidence, separate from project validation errors. The
`warnings` facet takes a register or variable ref and filters by period, variant and
literal representation; `unassigned_only` keeps a register's limitations that name no
variable, so a variable page does not download every other variable's warnings.
Unassigned register limitations stay visible but do not acquire an inferred owner. The
catalog shows warnings before column selection, with their delivery scope. The project
reads warnings for its selected columns and source periods, showing register limitations
separately. A failed warning request stays visible rather than implying the data has no
limitations. Warnings use the existing status tags and panels; inside project source
cards the same content renders inline. They are not serialized into `project_data.json`.

**Concept groups (#303).** Register and classification-root nodes carry a `groups` list
beside the complete flat `children`: grouped members appear in both. The SPA folds
client-side (`catalog.ts::foldGroupedRows`): grouped leaves hide under one
`ConceptGroupRow`, a link to the group's subject page carrying the label and the
distinct-member count; the members, their facets and picking live on that page.
Ungrouped leaves render as before, and the type-to-filter matches a group on its
label/key or any member's name/FQID (`groupMatchesFilter`), so member searches still
surface the folded group.

## Global catalog search (the Rust server, 3a.11)

`GET /api/search` is answered by the Rust server (`reg-meta serve`, the `search`
operation in `conformance/api/operations.toml`): one ranked list per call (decision 17),
as `{data: {items, next_cursor}, meta}`. With `type` the list is one arm's hits
(`register`, `variable`, `classification`, `classification_code`, `register_value`), its
curated pins first; without it, every arm's hits ranked together, grouped variable
members hidden. Ranking, curated pins (`reg_meta_build/curation/search_pins.toml`, read
from the catalog's `search_pin` table), concept-group folding, input limits and cursors
live in `crates/reg-catalog`. Locally, the Vite dev proxy sends `/api/search` to the
Rust server. Maintainers measure relevance with the repository's
`scripts/run_search_eval.py` (`scripts/search_eval.toml`, #393 item 10) against a
running `reg-meta serve`.

The SPA surface: a global `<SearchOmnibox>` in the app header routes to a shareable
`/search?q=` results page (`SearchView.svelte`) with navigation to catalog nodes. The
router has `search` and `doc` routes (query lives in `?q=`, keyed on pathname so the
page re-runs on every query change) and a `router.replace()` method (mirrors the
`?period` URL-as-single-source-of-truth pattern: the omnibox syncs back to the URL, and
the URL drives the view). `api.ts` has `search(q, {limit?, type?, cursor?})` typed off
the generated `api-types-rust.ts`. Off `/search`, typing in the omnibox stays local
until Enter/form submit and shows an Enter hint while focused; on `/search`, typing
live-refines with replaceState. `SearchView` renders an "All · Registers · Variables ·
Classifications · Codes" scope toggle backed by `?type=` (URL state, like
`?q=`/`?period`; `all` is omitted from the canonical URL; `value` is the SPA's name for
both code arms), a Close control that `replace()`s back to the route that entered search
(or `/catalog` for a cold deep-link), and variable rows whose heading carries
delivery-column pills while register, definition, and `operational_definition` live in
the muted detail line. The omnibox preserves an active scope when re-querying. Global
search does **not** render documentation results; documentation is reached from item
pages via `DocMentionsPanel` and then the `/doc/<filename>` route (router `Route` union
arm `{name:"doc",identifier}`), which renders `DocView.svelte`: title,
register/variable/tags, a `source_url` link to the SCB source PDF (resolved from the
curated map at doc-DB build, #372; None when uncurated) with `source_title` as label,
and a bounded `excerpt`; 404 distinguishes "not ingested" vs "not found";
`snippet`/`excerpt` are rendered as TEXT, never `{@html}`, and the full converted body
is never fetched.

Under `All`, `SearchView` makes one untyped call (5 hits) for a `Top results` strip,
shown only when it ranks more than one hit, and one call per arm (3 hits each) for the
sections below it, in the fixed order registers → variables → classifications →
classification codes → register-local value sets. A scoped `?type=` asks only its arm
(`value` asks both code arms). Any failed call fails the search as a whole. Each section
has a `Load more` control that requests that section's cursor at the same page size and
appends the next page; query or scope changes discard the continuation state. A
section's heading uses `N+ results` while it has a `next_cursor`, rather than claiming
an exact total. The control is keyboard-native, announces its busy state, and keeps
continuation errors local to the section. Classification codes render in per-code-system
subsections, register-local value sets in their own section; a code's owning variables
and classifications are its navigable targets.

## Docs library (the Rust server's `docs` operations)

`/api/docs/*` (#354/#742) is served by the Rust server since package 3b.6 (the vite dev
proxy sends `/api/docs` there). The operations (`conformance/api/operations.toml`) read
the prebuilt `reg_meta_docs.db` beside the catalog, each answering `{data, meta}`:

- `docs_search` (`GET /api/docs/search?q=&register=&limit=&cursor=`): with `q`, the
  entries matching every word, best first, with an FTS `snippet`; without `q`, every
  entry by filename. `register` is a register ref (FQID or bare name); the page carries
  `total` and `register_ingested` (whether that register has docs). It absorbs the old
  `/api/docs/for-variable` hook (`q` = the variable's name). The index holds
  `fold_search` text (decision 16, folded by the docs build), and snippets come from the
  stored body, so they keep case and diacritics.
- `docs_get` (`GET /api/docs/doc/{identifier}`): metadata, source pointer, a BOUNDED
  `excerpt` and the full markdown `body` for agents. The SPA renders only the excerpt
  (marker+Gemini conversion quality + republication exposure).
- `docs_related` (`GET /api/docs/related/{register ref}`) and its download
  (`GET /api/docs/file/{register ref}/{filename}`): the curated #739/#742 rehost
  surface, PDF bytes served verbatim with `Content-Type: application/pdf`, an inline
  filename disposition, and JSON metadata carrying `source_url`, `license`, `fetched`,
  `sha256` and `byte_size`.

`source` is the SCB source-document identifier; `source_url` is the resolved SCB PDF
link (populated at doc-DB build from the curated `doc_sources.toml` map, #372), null
when the source is uncurated; `source_title` is the publication title. Coverage is
LISA-only today.

- **Optional DB**: a deployment without a docs database answers `docs_unavailable` (404)
  on every docs operation; an incompatible one refuses server startup. The SPA's
  `getDocsForVariable` and `getRelatedDocuments` map `docs_unavailable` to `null`, so
  both panels treat it like "no docs" and omit their section.
- **Not in global search**: `SearchView.svelte` does not call `/api/docs/search`; the
  SPA reaches docs through item pages.
- **Parsed documentation on variable pages (#402/#967)**: `BindingLeafView.svelte`
  renders `DocMentionsPanel` in the `SubjectView` docs slot, firing a SEPARATE
  `asyncResource` at `docs_search` with the variable's name and its register FQID — a
  distinct failure domain (a docs error, timeout, or absent index never blanks the
  leaf). The panel omits the entire section when there is nothing to show (no docs
  database, `register_ingested:false`, or zero items); loading and error states render
  inline, so an in-flight or errored fetch never reads as a confirmed absence. The hits
  are fuzzy name matches, not authoritative variable→doc links, and the section caption
  says so. Each hit links to the `/doc/<filename>` viewer and renders the snippet via a
  safe inline-emphasis subset (`**…**` → `<mark>` for the matched term, `*…*`/`_…_` →
  `<em>`) through auto-escaped Svelte interpolation — never `{@html}`.
- **Source documents on register pages (#742/#967)**: `CatalogNodeView.svelte` renders
  `RelatedDocumentsPanel` on register pages only, with the register FQID. These are
  rehosted register-version source PDFs with provenance, not variable-level evidence, so
  `BindingLeafView.svelte` must not inherit them. It renders after the register's
  `VariantsSummary`. The panel is another independent docs failure domain: loading and
  error render inline; no docs database or no curated rows omits the section. Each row
  links the title to `/api/docs/file/{register FQID}/{filename}` and shows
  `Källa: SCB · {license}` plus a source URL link.
- **Caching**: the Rust server's cache tier for the docs operations is a day
  (`public, max-age=86400, must-revalidate`): doc-library content is rebuild-stable.

## Coverage aggregates (#351)

The catalog listing nodes carry a `coverage` object so a browse row shows its
study-window span without resolving every state:

- **Register children** (a register's `show`): per-variable `coverage` — `coverage_from`
  (min `valid_from`), `coverage_to` (max finite `valid_to`; null when `open_ended`),
  `open_ended`, `state_count` (>1 in a window = a break worth surfacing).
- **Provider children** (a provider's `show`): per-register `coverage` —
  `variable_count` (slugged variables) + the span over all their states.

In holdings scope, the reader computes coverage over mapped source variants and
canonical representations before aggregation. A partial-column hold cannot inherit the
whole variable's coverage. Unnamed coverage stays separate from named columns.

Members on a register-scoped concept group's own page carry per-column coverage too, but
a missing per-column key is a known curated member with no `variable_state` row, not
absent enrichment. Those members carry the zero-state coverage shape
(`state_count == 0`, null bounds, not open-ended) instead of `coverage = None`, so
downstream period lenses can distinguish "never delivered" from "unknown".

**Query-time, not materialized — measured first** (the #351 design decision). The
aggregates are grouped reads over `variable_state` in the reader. Measured on reg_meta's
Python reader against the real v0.11.0 DB: the worst register (scb/ulf, 7.3k variables)
computes per-variable coverage in \~9 ms (\~60 ms end-to-end serializing all 7.3k
binding nodes); the heaviest provider (scb, 238 registers) \~34 ms end-to-end. Both sit
behind the ETag/edge cache, so build-time materialized columns (which would ride the
batched Lane R schema bump) are NOT needed. The covering index
`idx_variable_state_coverage` on `variable_state(variable_id, valid_from, valid_to)`
(#371, the 5.4.0 schema cut) lets the grouped MIN/MAX span scan be satisfied index-only
(no table b-tree lookup; EXPLAIN QUERY PLAN reports `USING COVERING INDEX`).

- **Additive / payload-skew (#317)**: `coverage` is optional and the SPA doesn't read it
  yet — it must tolerate its presence AND absence. It's None on a node that wasn't
  enriched (e.g. a register's own node — coverage is populated only in the two LISTING
  payloads).
- **Open-ended sentinel**: `coverage_to` is None + `open_ended` True when the latest
  window is the `9999-12-31` DDL sentinel ("ongoing"); a stateless variable is
  `state_count == 0` with both bounds None (distinct from open-ended). The sentinel is
  `9999-12-31`.
- **Cadence DEFERRED**: #351 also lists a per-register "cadence", but reg_meta has no
  cadence attribute and no clean derivation (a modal period-grain is fuzzy for
  mixed-grain registers), and no UI consumes it yet. The load-bearing study-window
  signal is span + counts; cadence is a follow-up (a defined source or a build-time
  field) — not shipped here.

### Per-variable deliveries (Y-82)

A register-child also carries `deliveries`: the `(variant, delivery column)` pairs that
deliver the variable, each with that pair's own `VariableCoverage` window AND — since
Y-104 — its disjoint `windows`. It answers the two questions the coverage span can't —
*which variant delivers this?* and *what column name does a researcher know it by?*
(LISA's `forvink-ers` is `ForvErs` on paper). One more read in the same query-time
family (`Catalog.register_variable_deliveries`), keyed by
`(variable, register_variant, delivery_column_name)` over the same `variable_state` rows
and behind the same ETag/edge cache — no new endpoint, no build-time materialization.
Selecting `delivery_column_name` puts it outside `idx_variable_state_coverage` (as it
does its `register_column_coverage` sibling), so it reaches the row rather than being
satisfied index-only; both joins are LEFT to pin the join order, because with inner
joins SQLite drives from `variable_state` and scans the WHOLE table instead of searching
`idx_variable_state_variable` per register.

- **Windows, not a GROUP BY (Y-104)**: 7,295 of the corpus's 77,843 delivery groups
  (9.4%) do not tile contiguously, so the MIN/MAX span asserted an unbroken delivery for
  one group in eleven. The reads return ONE ROW PER STATE now — the GROUP BY dropped and
  nothing put in its place, since the fuse sorts and an `ORDER BY` no step reads costs a
  temp b-tree over the whole per-register row set and un-pins the join order the
  all-LEFT joins exist to hold (the per-register
  `SEARCH v USING COVERING INDEX idx_variable_slug` degrades to a catalog-wide `SCAN v`)
  — and Python fuses each group's eras into disjoint `windows`, the SPA's
  `deliveryWindows` rule moved to the model so both ends agree by construction.
  `coverage` is unchanged: it is the span over those windows, which is still what a
  browse row prints. The ceiling is row volume — scb/ulf, the corpus's largest register
  by states, is 22,684 rows over 10,785 groups — and gap-and-islands SQL is the upgrade
  if that stops fitting.
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
  it. Compiler validation checks deliverability before publication; see
  reg_meta_build/DESIGN.md for the compiled mapping and state contracts. Columns are
  identified case-insensitively (`py_lower`, the rule the build validates
  `variable_alias ⊇ state columns` with), so an alias that only re-spells a listed
  column is that one delivery.
- **Steward semantics**: the shared reader limits deliveries to mapped variants and
  canonical representations in holdings scope. Discovery unions held mappings across
  variants; each returned delivery retains its own variant and windows. Reference scope
  returns the catalog's delivery evidence.
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
- A chip is NAMED from the register's embedded `variants`, the same list
  `VariantsSummary` renders: one spelling of a variant per page, and no second request.
  Chip order is by slug.

### Adding columns from the register list (Y-83)

The register list is an AUTHORING surface: every delivery column it names carries a
tick, and one action adds them all. The consumer is the researcher who knows LISA by its
columns — ticking `ForvErs`, `ForvInk`, `Kon` and `Alder` in the list beats opening four
variable pages to add one column each.

- **The tick grain is the column NAME the list shows** (`catalog.ts`
  `deliveryColumnRows`); the COMMIT grain is the variable's own picker row. A tick names
  a column, an Add maps that name onto the rows the variable's own page builds
  (`deliveryColumnRows` → `rowCoversColumn`), and `rowAddSegments` fans each of those
  out to ONE staged add per *(variable, concrete `register_variant`, column)* — the same
  per-concrete-segment fan-out (#376) the variable page performs. So the file a tick
  authors is the file that page authors. The chip lens rides along, and it is CAPTURED
  WHEN THE COLUMN IS TICKED: a tick stores the concrete variants the list showed that
  column under, and an Add stages the intersection of those with the variants on screen
  when it is pressed, staging only the rows those variants deliver — the part a
  `?variant` modifier plays on the variable's own page (`narrowStatesByModifier`). The
  capture is the load-bearing half. The lens is live and a tick is not, so reading the
  lens at Add time instead would let a lens lifted in between widen the tick to a
  variant the researcher never saw, and a lens moved to another variant swap the tick
  silently onto that one. Bounded by both pages, a tick can never author a variant the
  researcher filtered away, in either direction: one the lens has moved off reads as
  UNTICKED and adds nothing until the lens that made it comes back — or until it is
  ticked again, which re-captures under what is on screen now. The capture is PER COLUMN
  and stays that way through the commit: ONE sweep (`stagedBatch`) meets each ticked
  name with the rows that deliver it, and a row joins a tick only where that tick's own
  captured variants cover it. Two columns of one variable ticked under different lenses
  must not pool their variants, because rows are matched to columns by NAME
  (`rowCoversColumn`) and both names usually exist in both variants — a pooled set would
  stage each column under the other's variants and quietly author four adds where the
  researcher made two.
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
  under for its whole round trip — the rail is not disabled while the draft restore and
  the per-add resolves are out, so a window moved mid-Add ABANDONS the batch
  (`batchGuard`, asked again after every await, `applyStagedPicks`'s own included)
  rather than committing it under years the researcher has already left, beside a list
  redrawn for the years they chose. Abandoned, not refused: nothing is authored, nothing
  is claimed, and the ticks survive for an Add under the window now on screen.
- **A column the window has moved off is not tickable.** The list names every column the
  register ever delivered, so a window later than a column's last era, earlier than its
  first, or inside one of its gaps leaves it nothing to commit. The gate is that column
  NAME's own eras, never the row's: a #902 rename chain folds into one row spanning the
  whole chain, so a row-grain gate would offer the RETIRED name under a window only its
  successor covers — a tick on a column the register stopped delivering before the
  window opened, under a label printing the very years that say so. Such a tick is
  disabled and the row carries the reason. A column delivered in NO era — the boundless
  delivery `VariableDelivery` documents, an alias spelling on a variant with no states
  of its own — is refused on its own terms ("Not delivered", naming no window, since
  none would lift it): it has no row to stage, and a checkbox that ticks and commits
  nothing is a control that lies.
- **The list refuses before staging; a subject page refuses at Add.** Neither invents a
  period for a column the window clips to nothing (see "Common study window" below). A
  subject page's picker only DIMS such a row and still lets it be ticked, then refuses
  the Add by name (`outside-scope`); here the window is the only period there is, so the
  page refuses the row before staging it, and the bar's count never promises a column an
  Add cannot commit.
- **The list's windows are EXACT (Y-104), so an Add reads nothing extra.** Each delivery
  carries its own DISJOINT `windows` beside the MIN/MAX `coverage` span, so a column
  delivered 1968, then 1995–1996, then 1998– says so on the wire. `deliveryColumnRows`
  builds a synthetic state per (delivery, window) and runs it through
  `pickerRepresentations`, so the rows a tick stages ARE the rows the variable's own
  page builds from its states — same key format, same window fuse, same #902 rename fold
  — and the list's years label prints the eras the way that page does. One consequence
  everywhere: the years beside a name, the window gate on the tick, the "In project"
  marker and the committed #307 comma-union all read the same eras, so a window inside
  an interruption is not tickable rather than tickable-then-dropped, and a register-list
  add is byte-identical to the variable page's without a second read. Before Y-104 the
  wire carried only the span; an Add paid a GET per ticked variable to rebuild these
  rows from the states, and the two grades could disagree.
- **The label and the tick are NAME-grain; the add and its marker are per (variant,
  name); the commit is ROW-grain.** The split is load-bearing: a #902 rename chain folds
  into ONE row spanning the whole chain, so a row-grain gate offers a retired name for
  its successor's years. The label and the tick pool the name's eras across the variants
  that ship it, because one box covers them all. `stagedBatch` and "In project" then ask
  `windowsByVariant` for the row's OWN variant, because a sibling variant still shipping
  a retired name must not lend those years to the variant that renamed it — that pooled
  reading would stage the successor under the retired name, and mark it added.
- **The years are spelled compactly** — `1968, 1995–1996, 1998–` — rather than through
  the `since 1998` / `until 1968` words `formatWindow` gives the variable page: this
  cell is all-mono and rides inside a tick's accessible name, and frontend/DESIGN.md
  keeps prose out of mono. Each era is one unbreakable run, because a `1995–` wrapped to
  the end of a line is this same grammar's "still delivered". An era undated at one end
  takes the bare dash on that side (`–1968`) rather than being left out: an omission
  from a LIST reads as a gap. Past three eras the list folds to its first and last era
  plus a `+N` affordance (`1968 … 2024– +9`), so a long interrupted history cannot push
  the row's name onto its own line (Y-110); the affordance carries the folded eras in
  both its `title` and its accessible name, so they stay reachable by hover or by screen
  reader.
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
  App's `.routed`, which stays `overflow: visible` — so the page scrolls on the
  viewport, the same scrollport every other sticky element on the site already assumes.
  `position: sticky; bottom: 0` on the bar now pins to that viewport, so scrolling any
  distance through a long list (LISA's ~740 variables) still leaves the count and the
  Add in reach at the bottom edge.

## Context and catalog sizes (the Rust server, 3a.10)

`GET /api/context` is answered by the Rust server (`reg-meta serve`, the `context`
operation in `crates/reg-catalog/src/ops/slice_3a.rs`). One call returns the steward
branding, schema version, import date, the steward's period span, the `reg_meta` version
and the headline `sizes` (`{providers, registers, variables}` in the read scope), as
`{data, meta}`; it replaces FastAPI's `/api/context` and `/api/stats`
(`RUST_RUNTIME_SPEC.md` decision 15). App fetches it once and threads `steward` and
`sizes` to Home, which makes no request of its own. The SPA's types for it are generated
from the committed `crates/reg-meta/openapi.json` into
`frontend/src/lib/api-types-rust.ts` (a `cargo test` keeps the snapshot equal to the
server). Locally, the Vite dev proxy sends `/api/context` to the Rust server and the
rest of `/api` here.

## ETag / Cache-Control

The Rust server sets the reads' ETags and `Cache-Control` (`crates/reg-catalog`); a POST
answer (`validate`, `order`, the manifest download) carries no validator.

**V1 early-revalidation correction (decision 2026-07-14; not implemented at this
head).** App code, compiled catalog DB, steward branding configuration and paired docs
DB are immutable for a process lifetime; changing any of them replaces the process. At
startup, derive one content-backed HTTP generation token from those inputs, including
catalog `generation_id`. Loose delivery inventory is no longer an HTTP input. Include
read scope in the canonical request identity and caches/cursors. For known pure GET
reads, derive the validator from that token plus steward and the canonical request
identity, and satisfy a matching `If-None-Match` before route execution, DB work, or
body serialization. This makes a 304 cheap without weakening exact representation
identity.

Browser and shared-cache freshness are separate concerns. Keep a short browser window
where prompt redeploy visibility matters, but let the Cloudflare cache retain immutable,
deploy-generation-keyed catalog/search responses for substantially longer without
synchronous origin revalidation at every browser expiry. A warm search must be served
without route execution. This complements rather than masks the bounded cold-query work:
arbitrary first-time queries still have to meet the origin budget. #1135's bounded SQL
path meets it, so no second in-process response cache is warranted.

- **The Rust server's tiers**: `/api/context` revalidates always (`no-cache`): the SPA
  vintage footer reads it to assert a specific deploy version/date, so a stale copy
  would *visibly lie* right after a deploy. Catalog and search reads use `max-age=60`
  because both embed the #322 concept-group folds, which can change at the same browser
  URL on redeploy. `public` keeps the CF edge cacheable (the #220 probe survives).
  `/api/docs/*` keeps `max-age=86400` — doc-library content is rebuild-stable and a
  sub-day-stale list is acceptable there; the ETag still guarantees correctness on
  revalidation. The edge worker (`reg_webapp/edge/`) defers to this origin's
  `Cache-Control` contract (it only stamps the `__edge_v` cache-generation param,
  orthogonal to caching policy), so the per-route policy needs no edge change.
- The **edge** side (Cloudflare edge caching / DDoS shielding / edge rate-limits) is a
  deploy/maintainer concern and not backend code. Remaining: edge config — see
  `REFACTOR_SPEC.md`.

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

## Compiled artifact identity and read scope

Boot opens the admitted reg_meta artifact and checks its manifest: schema compatibility,
publishability, completeness, artifact kind, and generation identity. A catalog artifact
requires the `global` deployment; a steward artifact requires its exact manifest
steward. A mismatch fails with the DB path and `REG_WEBAPP_STEWARD` locator. Only
`stewards/<id>/steward.json` is loaded for branding. Runtime inventory loading,
reconciliation, drift warnings, the in-memory index, and the runtime release gate are
removed; builder publication validation owns those invariants.

The Rust server's reads (the catalog pages' `show` and facets, `context`, `search`)
accept `?scope=holdings|reference`. Catalog artifacts default to reference and reject
holdings; steward artifacts default to holdings and allow reference. Scope is applied
inside the shared reader before hydration, grouping, ranking, counts, or pagination. The
adapter consumes scoped pages directly. Finite curated pins are admitted through
`Catalog.exists` before ranking and pagination. Cursors bind both scope and the full
artifact generation. Project endpoints reject any `scope` query parameter with a located
422; browse scope cannot override the selected artifact's orderability.

Provider and register discovery requires a mapped binding. Variable discovery unions
mappings across source variants; states and deliveries retain their actual mapped
variant and canonical representation. Concept-group membership and inherited tags are
scoped by that same representation rule. Register and variable warnings are admitted
with their subjects. A live unheld browse subject returns 404.

Classifications, codes, value-set contents, same-as links, succession relationships,
graph edges, and lineage remain reference evidence. The browse subject is scoped; a
reference neighbor does not grant holdings or orderability. Hydrated graph states use
the scoped reader; unheld neighboring variables remain thin with no selectable states.

The Rust server's `context` reports branding, schema version, import date and the
`reg_meta` version; its `meta` carries the full generation and the effective scope. Its
optional `period_span` is computed from compiled physical `holding_period` MIN/MAX
bounds of known tables with an explicit mapping, then capped at the catalog import year.
It is a coarse UI slider bound, never a coverage or validity check. Catalog artifacts
and holdings without dated periods return null.

The Docker image copies the branding root to `/opt/reg_webapp/stewards/` and passes it
as `--stewards`. SWECOV is the proving steward; extracting its branding and delivery
pipeline into its own system remains separate from this runtime cut. No generic
per-steward extension surface is introduced.

## Pydantic boundary

The webapp defines no Pydantic models of its own: every `/api` response is a Rust server
document, typed for the SPA from `crates/reg-meta/openapi.json`.

## OpenAPI snapshot + TS codegen (the drift gate)

`crates/reg-meta/openapi.json` is committed and is the canonical contract. The SPA
codegens `src/lib/api-types-rust.ts` from it via `openapi-typescript`. Two checks keep
these in lockstep: `cargo test -p reg-meta --test openapi_snapshot` asserts the
committed snapshot equals the server's rendered document, and the frontend gate step
(`scripts/gate.py frontend`, run by the `reg-webapp-frontend` CI job) regenerates the
types from the snapshot and fails on any diff — so server, snapshot, TS types, and the
committed tree must agree. `scripts/gate.py regen` refreshes both.

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
- The codegen'd `src/lib/api-types-rust.ts` is excluded from Biome entirely (codegen
  output, never hand-formatted).

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

**Enforced deterministically** by `frontend/scripts/lint_tokens.ts` (`lint:tokens`, part
of `bun run lint`, so it runs in the `reg-webapp-frontend` CI job with no extra wiring):
every `<style>` block in every `.svelte` under `src/` must be free of raw color literals
(hex, `rgb()`, `hsl()`, `oklch()`, …) and raw `font-family` stacks — a literal that
renders identically today still escapes the `[data-theme="dark"]` remap. `mask-image`
declarations are exempt by rule (a mask is alpha geometry, not palette). This exists
because biome does not lint `<style>` blocks inside `.svelte`, and because a screenshot
cannot see a token bypass. It is a source check only: it says nothing about hierarchy,
copy, layout, or whether the right token was chosen.

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
  its ARIA role explicitly (`table`, `rowgroup`, `row`, `columnheader`, `cell`). This is
  required because the responsive stacked form switches `display` to `block`, which
  strips native table roles in Firefox/Safari — explicit roles keep the semantics intact
  across that change. `getRowId` only keys the rows; there is no selectable variant.
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
- **`Tag` is copy-faced.** (Y-105, superseding Y-83's "declares no font-family") The
  base `.tag` rule sets `font-family: var(--font-ui)` — a tag label is a word of UI text
  — and the `mono` prop opts a tag into the mono face for the exception, an identifier
  shown as a tag.
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
  by `FilterChip`'s checkbox. `DataTable`'s stacked `<thead>` is sr-only only under
  `@media (max-width: 48rem)`, so it cannot apply the (unconditional) class and keeps a
  media-scoped inline copy held identical to the utility — the sr-only analog of the
  `td::before` micro-label exception.
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
(`context.reg_meta_version`, `context.schema_version`, `context.import_date`). The
footer is guarded on `context` (same as the header `.build` chip) so it is absent until
`/api/context` resolves. `import_date` is a UTC timestamp string
(`"2026-06-12T08:30:00Z"`); the footer displays only the leading `YYYY-MM-DD` (split on
`"T"`). The intent is citation stability: a reader quoting any catalog node can see
which reg_meta build it reflects without navigating away.

`AppShell`'s rail carries a `YearWindowSlider` dual-thumb year slider (#614/#611) as the
"Study window" control — a global control reachable on every route and inside the mobile
drawer. It sets the active project window (1960 floor → the catalog vintage year from
`context.import_date`; current year as the pre-context fallback), with bounds threaded
down from `App.svelte`. It writes through `windowStore` (`src/lib/window.svelte.ts`) —
see the store description below.

## SPA routing + production fallback

The SPA (`frontend/`) browses the catalog read-only with **path-based routing**: clean
URLs mirror the API (`/catalog`, `/catalog/scb/lisa`, `/catalog/scb/lisa/kon`,
`/catalog/scb/lisa/variants`, `/catalog/class/<slug>`). The variants page is the one
SPA-only path, over the register's `show`: a 3-seg path with a literal `variants` tail
parses to the `variants` route (the token is reserved in the variable slot at build
time, so no variable FQID can shadow it), never to a catalog node. The router is
hand-rolled — no routing-library dep — in `src/lib/router.svelte.ts` (a `.svelte.ts`
module so its reactive `$state` route compiles): it reads `window.location.pathname`,
navigates via `history.pushState`, handles `popstate`, and intercepts internal `<a>`
clicks (the `link` action) so navigation doesn't full-reload.

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
- **Production** SPA fallback is edge config, NOT server code. The origin
  (`reg-meta serve`) is a pure JSON API and serves no `index.html`, keeping `/api`,
  `/openapi.json` and `/mcp` un-shadowed. The SPA is served by the edge worker
  (Cloudflare static assets, `not_found_handling: single-page-application`), which
  answers a cold-load deep link to any non-origin path with `index.html` (Deployment →
  Edge workers below); see the comment atop `router.svelte.ts`.

The fetch wrapper (`src/lib/api.ts`) types every response off
`components["schemas"][...]` from the codegen'd `api-types-rust.ts`, so the SPA and the
server contract can't drift. The catch-all returns the `kind`-discriminated
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

  | Section       | Variable (`BindingLeafView`)                                                                                                                             | Classification (`ClassificationLeafView`)                        | Concept group (`ConceptGroupView`)                                                                                                            |
  | ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
  | description   | definition / description / unit `<dl>`                                                                                                                   | short name `<dl>`                                                | aggregated thematic tags, then shared definition/description (when members agree — #678/#900) above Technical details (key / facets / source) |
  | picker        | `PeriodPicker` (time) + `RepresentationPicker` (list or graph/time-band) + add-to-project                                                                | `ClassificationEditionGraph` compact edition DAG (#906)          | `PeriodPicker` (availability lens) + `RepresentationPicker` (list or graph/time-band)                                                         |
  | value / codes | codings (`ValueSetView` (#905), each distinct value set via `CodeList`)                                                                                  | `ClassificationCodesPanel` (`CodeList`)                          | —                                                                                                                                             |
  | relationships | `LineageDetails` (provenance/warnings); succession/group graph context lives in the picker                                                               | derived classification links; edition succession lives in picker | — (members live in the picker)                                                                                                                |
  | docs          | `DocMentionsPanel`                                                                                                                                       | —                                                                | —                                                                                                                                             |
  | technical     | one bottom `TechnicalDetails` disclosure (sensitive / identifier, single-state data type / column, and corrected delivery intervals with class/evidence) | —                                                                | —                                                                                                                                             |

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
  is OR within a dimension, AND across. When the group's graph is edge-bearing, small
  enough to draw cleanly, and maps every selectable graph cell one-to-one to the visible
  picker rows, the same picker may switch to graph / time-band mode instead of the list.
  The #908 dimension filter strip stays above either render mode; active filters narrow
  graph cells through the same filtered row model as the compact list. Leaf graph
  context with no selectable delivery-column row still renders in graph mode as
  unavailable context cells, so no-column bindings keep their succession/group context.

  Two **succession-collapse** folds ship in #902, both client-side and purely
  presentational:

  - **Intra-variable sequential-rename collapse** (`pickerRepresentations` /
    `coexistingColumns` in `catalog.ts`): one variable+variant whose delivery columns
    span NON-overlapping eras (`DINF` → `DINF83` → `DINF84` → `DINF86`) collapses into
    ONE picker row led by the latest-era column, spanning the union of all contributing
    windows. Earlier column names appear as a quiet inline hint ("was DINF, DINF83, …")
    via `renamedColumns` on `PickerRepresentation`. Genuinely parallel
    (overlapping-window) columns stay separate rows. The coexist-vs-rename test is the
    shared `coexistingColumns` leaf — also `resolveBindingAt`'s gate for an ambiguous
    add — so the two surfaces can never drift on the distinction. The #904
    graph/time-band mode renders those eras as selectable cells when the graph gate
    chooses the graph renderer.

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

The group's availability span is `ConceptGroupView`'s own `unionCoverage` over its
members' coverages, and a member's coverage line uses `formatWindow` — including the
one-sided `until <year>` form when the start is unknown (#658).

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
  `ClassificationCodesPanel` for the edition's codes, both through `ValueSetCodes`,
  which reads the codes a server page at a time and owns the **size-dependent filter**
  (a search box appears only at ≥ `CODE_FILTER_THRESHOLD` codes — pointless for a
  handful). `CodeList` renders the loaded pages verbatim in a height-constrained scroll;
  a levelled code indents by its depth below the shallowest level loaded, so a
  classification that starts at level 2 renders flat. Classification conformance
  warnings render on the variable value-set surface, not inside the shared code list.

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
  column. Errata-backed states add their exact interval, correction class and supporting
  evidence here. When correction provenance carries scoped attribution records, the
  disclosure names the catalog interval as provider-documented and renders each exact
  source-edition/evidence association separately. Correction-only overlap records show
  the corrected interval and exact associations without claiming provider attribution;
  resolution-only gaps are listed separately as inferred and unattributed. The
  disclosure remains collapsed and ordinary states add no row. Concept groups still use
  the component for key / facets / source. One component keeps the summary + styling
  consistent across call sites; callers omit it entirely when there's nothing to demote.
  `LineageDetails` follows the same omit-when-empty rule: with no provenance, warnings,
  loading state, or error, it renders nothing.

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
(`variable_state_lineage` provenance edges and lineage warnings, both from the `lineage`
facet).

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

- **The image runs the Rust server alone** (RUST_RUNTIME_SPEC.md package 3a.9, decision
  15): `reg-meta serve` with the baked DB pair and the steward branding, no uvicorn and
  no Python at runtime. The Dockerfile has three stages: the `regmeta-db` bake (Debian
  slim with `curl` and `zstd`: download the release assets, verify each against the
  SHA-256 digest GitHub records for it, unpack), a pinned Rust stage building
  `reg-meta`, and a Debian slim runtime with `curl` for the smoke gate. The image serves
  the API and `/mcp` only; the edge workers serve the SPA, which `container-build.yml`'s
  edge jobs build themselves. Since 3e.4 the Rust server answers every route the SPA
  calls; package F deleted the FastAPI app.
- **Hosted MCP** (decision 12): `/mcp` on `catalog.swecov.se`. The global worker
  forwards `/mcp` (`ROUTE_MCP` in `wrangler.jsonc` only; the SWECOV worker does not,
  since a steward catalog is never served over hosted MCP), and `fly.toml` passes
  `REG_META_PUBLIC_HOST`, which rmcp's allowed hosts admit beside the loopback names.
  `/mcp` and the project POSTs share one rate limit per client address (60 a minute;
  Cost protection, below). Behind Fly the peer is Fly's proxy, so the worker proves a
  request came through the edge with a shared secret (`EDGE_TOKEN` on the worker,
  `REG_META_EDGE_TOKEN` on the Fly app, sent as `x-edge-token`); only such a request is
  keyed on its `CF-Connecting-IP`. Any other request, a direct-origin hit included, is
  keyed on its peer, so a forged `CF-Connecting-IP` buys nothing. Without the secret
  every edge `/mcp` client shares the proxy's bucket. After each global edge deploy,
  `container-build.yml` sends `initialize`, `tools/list` and one `search` to the public
  `/mcp`.
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
  (Dockerfile COPY surfaces + bake inputs) build, push to `registry.fly.io`
  (SHA-tagged), and `flyctl deploy --image` each affected origin. The bake build-args
  are the RESOLVED newest `reg_meta/v*` tag (never `latest` — a literal `latest` would
  make the bake layer's buildx cache key insensitive to data-only releases and could
  even resurrect a stale cached layer after a pinned dispatch) and the SHA-256 digests
  GitHub records for its catalog and docs assets, which the `schema-guard` job resolves
  with the tag; the bake has no defaults and refuses an unverified download. The global
  Fly app uses `FLY_API_TOKEN`; SWECOV uses the separate app-scoped
  `FLY_API_TOKEN_SWECOV`. The SWECOV image also requires a matching
  `reg_meta_swecov.db.zst` asset on the resolved `reg_meta/v*` release: the bake fetches
  that asset and the shared docs asset into `/opt/reg_meta`, and the build fails when
  the release lacks it (its digest resolves empty; the global image is unaffected). The
  SWECOV metadata is non-confidential for the current testing steward, so the flavored
  DB is a public release asset. The bake does no schema admission: the server admits the
  pair at boot (`--catalog` rejects a DB of another catalog), and the schema guard below
  keeps a behind-schema asset from being built. Nothing deploys without green CI: a
  `wait-ci` job polls this commit's ci.yml run and the origin/edge deploy jobs require
  its success — an image that builds but fails lint/ty/pytest never ships. Each deploy
  job carries a HEAD-of-main guard (GHA concurrency serializes by build-completion
  order, not commit order — without the guard an older commit's slow build could
  overwrite a newer deploy; it also makes non-main dispatches deploy-inert). Two gates
  guard a bad image: the entrypoint smoke gate (it probes `context`, `search`, docs
  search and `/mcp` with `curl`, each carrying `__edge_v`, and the container exits
  non-zero before ever serving when artifact admission or a probe fails) and fly.toml's
  `/api/context` HTTP check (flyctl reports failure if it never passes). Rollback:
  `flyctl releases --image` lists history; `flyctl deploy --image <old>` restores in
  seconds.
- **Pending-schema-bump guard (#448)**: when the Rust server's schema gates (`SCHEMA` in
  `crates/reg-catalog/src/lib.rs`, `DOC_SCHEMA` in `docs.rs`) are AHEAD of the latest
  released `reg_meta/v*` asset (same major, higher minor), the image would refuse to
  boot on the behind-schema asset and fail its deploy — for a state that is expected
  (the owed reg_meta release ships the matching asset). A standalone `schema-guard` job
  compares the gates against the builder's `SCHEMA_VERSION` / `DOC_SCHEMA_VERSION` at
  the released tag (`git show <tag>:reg_meta_build/…`) via the pure
  `scripts/schema_pending_bump.py` helper, which returns a three-way verdict (`break` /
  `pending` / `compatible`). On a detected code-ahead `pending` bump (with both assets
  present) it publishes a `pending_bump=true` job output that defers the bake + deploy
  with a GREEN `build-image` and a `::notice::`. The guard is its **own** job (not a
  step inside `build-image`) so **every** deploy path can consult it — `build-image`,
  `deploy`, AND `edge-deploy` all gate on `needs.schema-guard.result == 'success'` (and
  on `pending_bump`); it runs whenever the image OR edge filter matches (or on
  dispatch), so an edge-only push still gets a verdict even though `build-image` is
  skipped. Once the owed release ships, the **build** self-clears on the next
  image-affecting main push (the bake now passes), and the **deploy** is self-clearing
  on release too: publishing the owed `reg_meta/v*` release auto-dispatches
  `container-build.yml` (via `publish_reg_meta.yml`'s `deploy-image` job, after the
  release's CI passes), which re-resolves the now-current asset and deploys — no manual
  `workflow_dispatch` needed. During a pending-bump window a later **edge-only** main
  push now correctly waits too: `schema-guard` ran (the edge filter matched), so
  `edge-deploy` sees `pending_bump == true` and holds its SPA/cache-gen ship alongside
  the origin, rather than going live against the still-pre-bump origin. The guard
  green-neutralizes **only** the safe code-ahead case; on a genuine **major break** — or
  a `pending` release that is ALSO **missing** a `.zst` asset (a #343 invariant
  violation: its digest resolves empty) — `schema-guard` **fails red (exits non-zero)**
  rather than emitting `pending_bump=false`. Because all three deploy jobs gate on
  `needs.schema-guard.result == 'success'`, a failed guard cleanly blocks build-image +
  deploy + edge-deploy — closing the edge-only hole where a skipped bake left nothing to
  fail (pre-fix the break surfaced only in the bake on image pushes, so an edge-only
  push shipped a new SPA/cache generation against a still-stuck origin). An explicit
  `workflow_dispatch` `reg_meta_tag` pin is never neutralized: a deliberate pin of a
  specific (possibly older) release has no owed release coming, so anything but
  `compatible` fails the guard red — the bake reads no schema, and the image would only
  refuse at boot on Fly. The comparison rule is unit-tested because CI can't reach the
  code-ahead branch on a normal commit (main's schema usually equals the latest
  release); its source of truth is `reg-catalog`'s schema gates. Trade-off: during the
  bump window the Dockerfile bake isn't exercised (a build-only PR goes green-skipped),
  re-exercised once the release lands.
- **Build/registry economics (#290)**: the reg_meta DB bake lives in its own Dockerfile
  stage (`regmeta-db`) that copies nothing from the build context, so its cache key is
  the base image and its build args (tag, flavor, digests) — app-code edits reuse the
  cached DB layer instead of re-downloading the release pair, and an asset re-uploaded
  under an existing tag changes its digest and rebakes. PR builds neither `load` the
  image into the runner's docker (nothing runs it; all gates execute during the build)
  nor write GHA buildx cache (PR-scoped cache is unreadable from main and would only
  evict useful entries from the repo's 10 GB pool); PRs still read main's cache. Every
  pushed tag is an immutable rollback handle: `workflow_dispatch` rebuilds on an
  existing HEAD get a `-<run_id>` suffix instead of overwriting `:sha`. A post-deploy
  prune step keeps the newest 10 tags and deletes older manifests via the registry v2
  DELETE (supported by Fly — verified live 2026-06-11; buildx pushes OCI indexes, so age
  is read from the image config's `.created`, and a digest shared with any kept tag is
  never deleted).
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
  `DEPLOY_VERSION` cache generation after its own Fly origin deploys. Origin paths
  (`/api/*`, `/openapi.json`, and `/mcp` on the global worker) are `run_worker_first` +
  `fetch(request)` passthrough to the incoming hostname's zone origin (Fly), so the
  origin ETag/`Cache-Control` contract governs API caching as a classic proxied origin.
  `run_worker_first` is required: SPA mode otherwise serves `index.html` to browser
  navigations without invoking the worker, shadowing `/api` deep-opens. The glob list
  and the worker's `ORIGIN_PATHS` regexes are a LOCKSTEP pair (comments in both files);
  `reg-meta serve` answers exactly the forwarded set. Cloudflare downgrades the origin's
  strong ETag to weak (`W/`) when compression applies — weak comparison is correct for
  GET revalidation, not a bug.
- **Edge cache generations (#318)**: the worker stamps a per-deploy `DEPLOY_VERSION`
  (wrangler var; CI passes the commit SHA, `-<run_id>`-suffixed on dispatch so same-SHA
  data-only rebuilds still count) onto every origin-bound URL as an `__edge_v` query
  param. The zone cache key is the full URL, so each deploy orphans all prior `/api/*`
  cache entries — fresh payloads immediately after deploy, while the per-route TTL still
  bounds origin traffic *within* a generation (60s for catalog + search, 24h for
  doc-library). This is the free-plan substitute for `cf.cacheKey` (Enterprise-only) and
  needs no purge credentials. Origin-side the param is inert: `reg-meta serve` drops it
  before validating the query (it rejects every other unknown parameter) and the ETag is
  content-derived. Consequence: `edge-deploy` runs on **image-affecting** pushes too,
  not just edge paths — an origin deploy that changes API payloads without touching the
  SPA/contract must still ship a new cache generation. The motivating incident (#303
  rollout) had the edge serving 11h-old pre-deploy catalog JSON against a freshly
  deployed SPA; the #317 defensive-rendering rule (SPA tolerates one cache generation of
  payload skew on additive fields) stays in force regardless, for clients holding
  *browser*-cached payloads (catalog + search browser TTL is 60s; doc-library is 86400s
  — both unversioned). Deploys: the `edge-deploy` job in `container-build.yml` rebuilds
  the SPA (bun pinned to ci.yml's frontend job — bump together) and runs
  `wrangler deploy` on main pushes touching the SPA, the edge worker, the committed
  `openapi.json`, or the image surface (cache generation, above) (`CLOUDFLARE_API_TOKEN`
  repo secret, "Edit Cloudflare Workers" template scoped to the account + swecov.se).
  The job `needs:` the origin deploy — on a contract-changing push the SPA never goes
  live before the origin serves the new endpoints (deploy-skew guard; skew 404s are NOT
  negatively cached: the Cache Rule's Edge TTL is "bypass if no cache-control", and the
  origin only stamps 200s). After each edge deploy a probe asserts a catalog read
  returns `CF-Cache-Status: HIT` with a young `Age` (a stale `Age` means cache-key
  versioning broke) and an edge 304 — the #220 gate as a standing regression check
  against silent Cache Rule / zone drift. It reads `/api/search`, the cacheable Rust
  route (`/api/context` is `no-cache`). Manual fallback: build the SPA, then
  `wrangler deploy` with a FRESH `--var DEPLOY_VERSION:...` (exact command in
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
  selected compiled SQLite generation directly, so it does not depend on the deployed
  API or impersonate a browser to evade zone bot protection. V1 deployment supports the
  SPA. If remote programmatic API access becomes a product surface later, admit a
  truthful toolkit User-Agent on the API paths under endpoint-specific rate limits and
  probes; do not document a fake browser header as the contract.

## Frontend unit tests (Vitest)

`bun run test` runs **Vitest** (`vitest run`) — Vite-native, so it reuses
`vite.config.ts` and compiles `.svelte` / `.svelte.ts`. The env is `jsdom`
(`router.svelte.ts` reads `window` at module load; `api.ts` mocks `fetch`). Tests live
next to source as `*.test.ts` and cover the fetch-wrapper error path, the
`kind`-narrowing helpers, and route parsing. The `reg-webapp-frontend` CI job runs
`bun run test` alongside `svelte-check` + the codegen drift check. (Use `bun run test`,
not `bun test` — the latter is Bun's own runner, which doesn't compile Svelte.)

## Why CI uses a fixture DB, not a real asset

Backend tests build artifacts in temporary directories and point the app at them with
`REG_META_DB`; they do not fetch released DB assets. Compiled stewardship cases load
readable logical JSON, inventory TOML, policies, and census CSV through the real builder
and compiler. HTTP requests and expected status/body projections live in the owning JSON
corpora. Boot cases vary manifest admission and deployment identity; catalog,
validation, scope, and cache cases assert their public boundaries. No runtime index
fixtures or release-marked inventory reconciliation suite remains.

Older catalog-only fixtures continue to serve unrelated route tests. Real pinned
artifacts are checked separately for order parity, latency, and rendered evidence;
synthetic success is not a real-corpus or deployment claim.

## Project operations (the Rust server, 3e.4)

The project editor POSTs the WHOLE serialized draft, a raw object, to two operations of
the `order` MCP tool (`crates/reg-catalog/src/ops/slice_3e.rs`). The body is parsed
strictly (`ops/body.rs`: one UTF-8 JSON object, no byte-order mark, no duplicate key at
any depth, `serde_json`'s depth limit); anything else is `malformed_request` (400)
before the operation runs. Unknown keys survive the parse, so `validate` reports each as
`unexpected_field`. Neither operation takes a `scope`: a project is read in the
artifact's own identity, and a `scope` parameter is `invalid_parameter`.

- **`POST /api/project/validate`** answers `{data: {ok, issues}, meta}`. A project that
  FAILS validation is a successful answer — **200 with `ok: false`** and every issue in
  emission order (supported version → structural → semantic). A non-2xx is a refused
  REQUEST (`malformed_request`, `payload_too_large`, `rate_limited`), which the SPA
  banners apart from the issue list.
- **`POST /api/project/order`** answers `{data: manifest, meta}`;
  **`POST /api/project/order/manifest`** serves the same manifest as the exact
  `order.json` bytes, `attachment; filename="order.json"`. The SPA's Download order.json
  uses the download. Anything that is NOT an order is a 422 on both routes, never a
  partial manifest: `project_invalid` for a document `validate` rejects structurally
  (its issues in `fields.issues`), `order_blocked` when any finding leaves part of the
  request undeliverable (every finding in `fields.findings`, each with its stable `code`
  and its `source` / `variable` / `period` coordinates). Fail-closed is a contract, so
  the findings ship as data: `orderFindingsFromError` narrows them at the HTTP boundary,
  and the SPA renders each through the SAME per-finding path as a validation issue
  (`ValidationPanel`; `validation.orderFindingPointer` resolves the materializer's
  by-VALUE coordinates to the by-POSITION pointer `findingLocation` already locates a
  card by). The finding shape is declared in `api.ts` (the error catalog types `fields`
  as an open object) and pinned by `conformance/cases/api/order-errors`.

The order pipeline and its findings are `ops/order.rs` (see `crates/DESIGN.md` → Order
manifest): steward holdings on a steward artifact, canonical columns on the global one.

## Semantic validation

The semantic layer, its rules and its issue codes are `ops/validate.rs`, ported from
`reg_meta`'s frozen `semantic.py` (`crates/DESIGN.md` → Project semantic validation),
whose CLI adapter `reg-meta validate` G1 compares it with.

## Cost protection (`crates/reg-meta/src/limit.rs`)

Every POST (the project operations and the manifest download) and `/mcp` sit behind the
same two origin-side guards; GET reads are not limited (they have the cheaper edge-cache

+ ETag axis). Cloudflare fronts production with its own edge budgets; these catch direct
  origin hits that bypass the edge.

- **Rate limit** — one in-memory token bucket per client address, shared by `/mcp` and
  the POSTs: 60 tokens, refilled one a second, then `rate_limited` (429,
  `Retry-After: 1`). `serve --write-limit N` changes the 60, for verification runs (the
  conformance suite, release admission, G1) that replay many projects from one address;
  a deployment keeps the default. The client is the edge's `CF-Connecting-IP` when the
  request carries the edge token (`REG_META_EDGE_TOKEN`), otherwise the peer, and an
  IPv6 address counts as its /64. **Address-only** by design: a session token would
  bucket per browser (helpful behind NAT) but adds a fingerprinting surface for
  anonymous public data. Buckets are per process (lost on restart, not shared across
  replicas), which suffices as the origin backstop behind the edge. The SPA's debounced
  validation and its downloads stay far inside the budget (the `flows` run replays every
  scenario from one address under it). A deployment whose worker sends no edge token
  keys every client on Fly's proxy, so all of them share one bucket.
- **Body cap** — `payload_too_large` (413) over 1 MiB, counted as the body streams in
  (axum's `DefaultBodyLimit`), never trusting `Content-Length`. 1 MiB is far above any
  plausible `project_data.json`.

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
leads with the delivery column it orders: an explicit `display_name` first (reg-core
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
`binding_state_drifts_within_period` across a transition — see `crates/DESIGN.md` →
Project semantic validation) — the auto-validate flow that surfaces this on every edit
is the sibling #994 (shipped — see § "Browser storage + project-file persistence"
below). `ValidationPanel` carries a "Fix in catalog" link on each finding that resolves
a catalog coordinate, so the remediation path is always back to the catalog, never a
cart-side patch.

The one field a pick does NOT write is `display_name`, which it leaves **absent**. The
field is optional — an absent one resolves to the reg_meta default from `variable_alias`
— and stamping the delivery column onto it made two disjoint-era bindings of one
physical column (`forvink-ers-aktiv` 1990..2021 and `forvink-ers` 2022..2023, both
`ForvErs`) collide under reg-core's per-source `display_name_collision`, though the two
never coexist. The rule is deliberately left as it is rather than made period-aware: the
structural layer cannot see periods by design, and the rule still earns its place on
hand-authored specs that set explicit names.

Opened project files are held **verbatim** in the store so serialize/validate see the
same malformed structure the backend diagnoses. The SPA's read side uses one
`project_data.ts` safe-source-slot seam instead: non-array `sources` renders as empty,
while malformed `sources[]` slots normalize to `null` for rendering/validation-display
and remain counted so `/sources/{i}` anchors line up with backend issue paths. Store
mutators that inspect source fields use the same accessors over the raw slots, so an
untouched malformed slot is preserved until the user deletes it or replaces the source
array through an explicit structural edit. The draft is typed `RawDraft`
(`Record<string, unknown>`); the values the SPA writes into it are typed with the
`Project*` schemas, which `reg-core`'s `openapi` feature publishes in `openapi.json` (no
hand-written project types). The accepted model is never written back, so string panel
members, unknown keys and invalid enums survive. One deliberate gap: an add whose column
type did not resolve writes `type: ""` (`DraftBinding`) for the server to report.

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
**period** is the one field the cart edits (Y-81), one From/To row per segment for a
#307 list period (Y-101 — a single range is the one-row form of this). It is a field of
the SOURCE rather than of any single pick — a study window of 2005..2020 with one
register reaching back to 1990 is an ordinary order — and the `/project` source card is
the only surface that shows a source WHOLE: its full coordinate, its stored period and
every column it carries, including columns no one catalog page lists. That is what a
source-wide rewrite has to be looked at against, so that is where it is made. The card
authors it in the catalog's own period vocabulary — exact-year fields and one Apply, the
same entry `PeriodPicker` carries beside its slider, plus "Add years" and a per-row
Remove (hidden at one row) — and writes back through the same wire shaping a pick uses.
Applying sorts and merges the rows into the disjoint ascending wire `mergePeriods` would
produce, refusing a within-row disorder or a genuine cross-row overlap in the same
status line rather than silently collapsing one; a token period (`HT2018`) is a
different vocabulary the rows do not author, and is shown as it stands. Where the years
the period covers differ from the project's `window` (narrower, wider, or holed;
`periodWindowRelation`) the card MARKS it ("Differs from study window 2005–2020"): the
window is an authoring seed, not an inheritance (reg-core puts `period` on `Source`
alone), so divergence is shown rather than warned about. The one divergence that is more
than shown is a period with no years inside the window at all, which blocks the order
(see "Common study window").

The rewrite goes through `applySourcePeriodEdit`, keyed by source **name** as well as
coordinate: a draft may carry several differently named sources on one register variant
(reg-core makes names unique, not variants), and an edit moves only the one it names. It
is its own period-only `applyStagedDiff`, never unioned with staged adds, and it carries
the edited source's complete value plus the draft's `replacementGeneration`. Both are
re-checked through one store-internal predicate immediately before the write: a source
that moved, or a project that was replaced, under an open edit refuses the write instead
of overwriting it, and the card says so. Ordinary browsing still stages nothing —
changing years, filtering rows or following a `?period` link leaves the draft alone, and
`RepresentationPicker` has no period-change path at all: a partial leaf/group cannot
infer a source-wide rewrite from the columns it happens to show, so that rewrite happens
on the `/project` card only. Coverage and type drift after the edit stay the server
validator's job, as above. (Y-81 retired the catalog-side Y-15 correction that used to
do this from a "Project sources on this page" box on every leaf and group page: the
catalog surface had to re-state a source the cart already shows whole, and nobody found
it there.)

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
- **reg-core in the browser.** The server's structural door, `reg_core::project::check`,
  runs in the SPA as WebAssembly (`crates/reg-core-wasm`, built by `bun run gen:wasm`;
  `lib/reg_core.ts` is its only importer). `main.ts` loads it before mounting; if it
  cannot load, `#app` shows a static alert instead of an app that cannot check a draft.
  Every draft replacement (each edit, New, Open, the restore) runs `check_project`
  synchronously, outside the validation debounce and the in-flight gate. A rejected
  draft gets those issues as its validation at once and is never POSTed, so a stale
  green answer for an earlier draft is discarded and cannot reopen the order download;
  an accepted draft is POSTed to `/validate` (debounced) for the semantic layer, which
  needs the catalog. A new draft takes `project_schema_version()`.
- **Project-file versions.** Any JSON object opens (the shape is the only ingress
  check): the server reads exactly reg-core's `SCHEMA_VERSION` and answers any other as
  the single `unsupported_schema_version` issue, which the browser now shows on open. No
  migration, pre-v1 policy.
- **Unsaved-changes warning.** A `dirty` flag derives from the draft diverging from the
  last DOWNLOAD baseline (`lastDownloaded`); a `beforeunload` listener prompts on a
  tab/window close with a dirty draft. The store drives the write endpoints (validate /
  order download) through `lib/api.ts`.
- **Deliberate replacement of a dirty draft.** `beforeunload` covers leaving the tab; it
  does NOT run for the in-app New and Open, which replace the draft *and* the single
  IndexedDB recovery copy behind it. Both therefore go through one policy
  (`requestNewProject` / `requestOpenProject`, which hold the incoming project as DATA —
  never a callback): over a dirty draft they raise a single confirmation (a Bits UI
  `AlertDialog`, the app's only modal) offering cancel / download-then-replace / replace
  anyway, and the replacement runs only on a confirm. Nothing about the draft moves
  while that answer is pending, so a cancel leaves the draft and its autosave untouched.
  Open PARSES first (`parseProjectText` — JSON, top-level object) and asks only once the
  file is one that could be loaded: a cancelled file picker, a parse/shape rejection, or
  a refused replacement all leave the current draft exactly as it was. A restored
  autosave stays dirty (a recovery copy is not a downloaded one), so it gets the same
  confirmation — the flag is never cleared to skip the policy. The pending project is
  dropped when `/project` unmounts, so an unanswered question can never outlive the page
  that asks it. Reading a picked file's bytes is the one asynchronous step, and
  `/project` carries a generation counter that a New, a newer Open and its own teardown
  all bump: a read that loses that race is dropped BEFORE the ingress runs, since the
  ingress is what raises the open-error — otherwise a stale file could still replace a
  newer decision, or put a stale banner over it.

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
(`context.import_date`'s year) down through `CatalogNodeView` → `BindingLeafView` →
`PeriodPicker`, and also through `ConceptGroupView` → `PeriodPicker` (#638). On the
concept-group subject page the picker is a **client-side availability lens** over the
union of member coverage spans — it greys members not delivered in the active window but
drives no refetch (`getConceptGroup` takes no period parameter). The vintage is the
ceiling an OPEN-ENDED coverage (`coverage.to === null`, "still delivered") projects to —
the catalog only knows delivery up to its vintage — so the coverage band ends at the
vintage and a selection past it reads as "not delivered after `<vintage>`". It is NOT a
floor on the slider bounds: a FINITE coverage keeps its real end (never extended to the
vintage), and a window/selection past the vintage still widens the bounds (the thumb
renders the real value) without extending coverage. Wall-clock is the pre-context
fallback only.

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
deployments seed the rail and per-page picker bounds from `/api/context`'s `period_span`
(#1037), a best-effort year span derived from admitted physical holding-period bounds
and capped at the catalog import year. Catalog artifacts and holdings without dated
periods fall back to the fixed 1960 → catalog-vintage bounds.

## Common study window (decision 2026-07-11)

The project's common study window is the authoring default for every dated source. It is
never schema inheritance: each `Source` keeps its own explicit `period` in
`project_data.json`, and nothing derives a period from the window at read time.

- **Where it lives: the existing `window` field, no schema change.**
  `ProjectData.window` (reg-core `StudyWindow`, optional since #611) already persists
  the window with the draft, so it survives download, open and the autosave.
- **The rules are SPA authoring rules; nothing past the SPA enforces them (decision
  2026-10-07).** The window is an authoring default and every source carries its own
  explicit period, so the period is the whole order request: neither `reg-meta order`
  nor the HTTP `/api/project/order` endpoint reads `window`, and neither refuses a
  project whose source is disjoint from it. The SPA's block is a gate on its own
  download control, not a contract on the file. Enforcing it everywhere would make the
  window a constraint on the order — a reg-core structural rule and a `schema_version`
  change — which is deliberately not taken.
- **An add persists the full available intersection.** A catalog Add clips each picked
  column's delivery windows to the add window (the rail's window, or a subject page's
  own `?period`) and commits every surviving era (`windowsAddPeriod`), so an interrupted
  delivery keeps its gap as the #307 list form. This is the Add itself, not a
  suggestion.
- **No overlap blocks the add.** A column with no years inside the add window has no
  period to commit, and none is invented from its own span (the earlier dimmed-row
  fallback is gone). The register list does not offer the tick; a subject page refuses
  the whole Apply before the store is touched (`applyStagedPicks` → `outside-scope`) and
  names the columns and the window in the picker's status row. A subject page's own
  `?period` overrides the add window, so the study window is checked as well: an add the
  page period resolves wholly outside it would author a source that blocks the order,
  and is refused the same way (`outside-study-window`), naming the study window.
- **A window edit never rewrites a source period.** The rail writes `window` and nothing
  else. A source the new window leaves with no years inside it keeps its period and
  blocks the order: `projectStore.canDownloadOrder` closes and `downloadOrder` refuses,
  the card shows an error row, and the validation panel lists each such source
  (`windowDisjointFindings`) with the same locate and catalog links a finding gets. The
  rail's project chip says "Order blocked" (the panel's own wording) wherever validation
  alone would read "Draft valid" or "Warnings". The draft itself stays valid and
  downloadable.
- **Every divergence is marked.** On `/project` the card marks a differing period and,
  at error tone, a disjoint one; the head of the sources list names the window and
  counts both kinds. In the catalog, a committed column's "In project" tag says when its
  source's years differ from the window, or that it is outside it (`committedMarker`,
  shared by the subject-page picker and the register list); the subject page's own
  `?period` deviation keeps the period picker's existing hint.
- **"Apply window overlap" is the one rewrite.** It reads each dated source's register
  (the register list's `VariableDelivery.windows`), plans each source's overlap from its
  own columns at its own variant (`planWindowOverlap`), and asks before writing: the
  dialog lists every period it replaces and every source it leaves alone. Only sources
  whose columns reach the window are rewritten, in one period-only `applyStagedDiff`; a
  source with no overlap keeps its period, and keeps blocking if it is disjoint. So does
  a source one of whose columns has no delivery at its variant in the read: narrowing it
  to the columns that were found would silently drop the missing column's years. A draft
  that moves while the reads are out drops the plan rather than writing it onto a
  project nobody looked at. Once a plan has left nothing to change (a press that changed
  nothing, or a rewrite that just landed), the action stays frozen (`aria-disabled`, so
  focus stays put) until a source or the window moves.

## API surface

The Rust server (`reg-meta serve`; contract `conformance/api/operations.toml`, snapshot
`crates/reg-meta/openapi.json`) answers every `/api` route. This table is the
orientation map. Read GETs are edge-cacheable, write POSTs are not.

  | Method | Path                                 | Server | Purpose                                                                                                                   |
  | ------ | ------------------------------------ | ------ | ------------------------------------------------------------------------------------------------------------------------- |
  | GET    | `/api/context`                       | Rust   | `context`: branding, build info, period span, catalog sizes.                                                              |
  | GET    | `/api/search`                        | Rust   | `search`: one ranked list per call (`?type=` keeps one arm), `{items, next_cursor}`.                                      |
  | GET    | `/api/catalog`, `/api/catalog/{ref}` | Rust   | `show`: the root, or any node by ref (`kind`-tagged); a retired ref answers its terminal successor.                       |
  | GET    | `/api/states/{ref}`                  | Rust   | A variable's states, cursor-paged; `period`, `variant`, `value_set_version` narrow.                                       |
  | GET    | `/api/warnings/{ref}`                | Rust   | A register's or variable's data warnings; `period`, `variant`, `representation`, `unassigned_only` filter.                |
  | GET    | `/api/values/{ref}`                  | Rust   | A classification's codes, or a variable state's value set (`state=`), cursor-paged; `q` filters, `total`.                 |
  | GET    | `/api/graph/{ref}`                   | Rust   | The relationship graph of a variable, classification or group, succession included.                                       |
  | GET    | `/api/lineage/{ref}`                 | Rust   | A variable's lineage edges, lineage warnings and source registers.                                                        |
  | GET    | `/api/docs/search`                   | Rust   | `docs_search`: docs matching `q` (or every doc), optional `?register=`, `{items, next_cursor, total, register_ingested}`. |
  | GET    | `/api/docs/doc/{identifier}`         | Rust   | `docs_get`: one doc by variable/filename — metadata, source pointer, excerpt, body.                                       |
  | GET    | `/api/docs/related/{ref}`            | Rust   | `docs_related`: metadata of the register's rehosted source PDFs.                                                          |
  | GET    | `/api/docs/file/{ref}/{filename}`    | Rust   | One rehosted source PDF's bytes.                                                                                          |
  | POST   | `/api/project/validate`              | Rust   | `validate`: 200 + `ok` + issues, an invalid project included.                                                             |
  | POST   | `/api/project/order`                 | Rust   | `order`: the manifest; 422 `project_invalid` or `order_blocked` (with every finding) when not an order.                   |
  | POST   | `/api/project/order/manifest`        | Rust   | The same manifest as the exact `order.json` bytes, an attachment (the SPA's download).                                    |

Global FTS search shipped as `GET /api/search` (#350) and moved to the Rust server in
3a.11; the docs library shipped as `/api/docs/*` (#354) and moved to the Rust server in
3b.6 (with `/api/docs/related/{ref}` and its file download).

## Input-validation gates (security boundary)

Hostile input on the catalog and docs reads (a malformed ref, a traversal-shaped path, a
bad period, variant or query) is refused by the Rust server with a located error; the
grammars and refusals are pinned in `conformance/cases/api`. A project body is parsed
strictly (`ops/body.rs`, Project operations above), and a `scope` parameter on a project
operation is refused as `invalid_parameter`.

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
