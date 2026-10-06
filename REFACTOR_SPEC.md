# Registry Research Toolkit — Remaining Work

Forward plan for the post-A5 work of the Model A refactor. The Model A schema, FQID
grammar, IR/adapter build, `reg_schema` v2, and the `reg_webapp` backend + SPA all
**shipped**; their design rationale now lives in [`ARCHITECTURE.md`](ARCHITECTURE.md)
and the package `DESIGN.md` files. Per-PR landing history is in git (the
`MIGRATION_PLAN.md` tracker was retired when A5 shipped).

This document is what survives of the original refactor spec: only the **unbuilt**
pieces. It is scoped and self-shrinking — each section moves into the owning `DESIGN.md`
as it ships, and the file is deleted when the last item lands (target: v1.0).

## Status

**Shipped (A0–A5):** two-level `variable`/`variable_state` catalog, 3-segment
`provider/register/slug` FQID grammar, slug curation + grow-only immutability machinery,
edge/lineage tables, build-time triage, SCB + SOS adapters and the first combined build,
`reg_schema` Pydantic v2 + the `project_data.json` v2 Source schema, and the
`reg_webapp` FastAPI backend + Svelte SPA (catalog browse, project authoring, IndexedDB
autosave, validate / order / bundle endpoints).

**Archived (§8/§9/§10a):** the `reg_monabundle` MONA bundle + `mock_data_wizard`
mock-data subsystem (kit-build, the realign-then-extract MONA workflow,
`mock_data_wizard` → `reg_mockdata` rename) have been removed from `main` and archived
to branch `archive/mona-subsystem` (tag `mona-subsystem-pre-rebuild`), pending a
from-scratch rebuild tracked in #707 (archived under #699).

**Remaining (this document):** composite panel keys, the open items of the shipped
compiled-holdings cut (§12), the remaining real steward coverage, measured web
performance hardening, the `reg_meta_build` restructuring, and the v1 slug freeze.
(Webapp deployment — step 6.5 — shipped 2026-06-11; the webapp-authoring hard-cut — step
7 — shipped 2026-06-11.)

## Sequence

A dependency narrative, not a checklist. Numbers continue the original spec's post-A5
step numbering.

  | Step    | Work                                                                      | Gates on | Issues           |
  | ------- | ------------------------------------------------------------------------- | -------- | ---------------- |
  | 6.5     | Containerize + Cloudflare + `global` deploy                               | A5       | #278, #220, #224 |
  | 7       | Webapp-authoring hard-cut; delete `mock_data_wizard/web/`                 | 6.5      | —                |
  | 7.5     | `global` dogfood (2 weeks)                                                | 7        | #200, #266       |
  | 8/9/10a | MONA bundle + mock-data subsystem — **archived** (see below)              | —        | #707             |
  | 10b     | Composite `entity_key` / `time_key` support (gates on MONA rebuild, #707) | —        | —                |
  | 11      | Steward catalogs (ifau, swecov)                                           | 7.5      | #206             |
  | 12      | Delivery inventory + shared normalized order manifest                     | 11       | —                |
  | P       | Remaining cache, classification-payload, and static-asset hardening       | 6.5      | —                |
  | R       | `reg_meta_build` restructuring; no catalog release until parity           | —        | —                |
  | —       | v1 slug freeze + arm immutability                                         | all      | #209, #196, #197 |

## 6.5 — Deployment: containerize, Cloudflare, `global` up

**Shipped 2026-06-11.** `global` is live at `catalog.swecov.se`: Fly.io origin
(`reg-webapp-global`, image bakes the reg_meta/v0.9.0 release DB pair) behind the
Cloudflare zone (SPA via the edge worker, `/api/*` passthrough with edge caching and a
WAF rate limit) — no authoring UI cutover yet (that is step 7). The **#220 edge-cache
gate passed**: slash-bearing FQID paths round-trip the edge cache byte-identical with
per-URL entries and edge-served 304s, so the path-based FQID surface stands in the
OpenAPI (no query-string fallback). The #224 deployment-side provenance assertion and
the per-deploy smoke gate ship in the image; #278's resolvable `reg_meta/v*` release cut
2026-06-10. As-built topology, rationale, and operational notes: `reg_webapp/DESIGN.md`
→ "Deployment".

## 7 — Webapp authoring hard-cut

**Shipped 2026-06-11.** `mock_data_wizard/web/` (the superseded Svelte SPA), the
wheel-shipped `static/` bundle, the frozen `mock-data-wizard ui` stub + its stub tests,
and the `frontend` CI job are all deleted — `reg_webapp` is the only authoring surface
and the package's bun usage is gone. No parallel run, no shim. Testers re-author
affected projects.

**7.5 — `global` dogfood (2 weeks).** Testers exercise the loop that exists at this
point — author → order — against `global`. Authoring-UX ride-alongs #200 (stable editor
list keys) and #266 (rank/default for parallel-delivery choosers) should land before or
early in this window so dogfood feedback isn't polluted by known glitches. `global` is
the staging environment; no separate staging tier.

## 8/9/10a — MONA bundle + mock-data subsystem (archived)

`reg_monabundle` (MONA bundle build + runtime + PII scanner) and `mock_data_wizard`
(local mock-data generation, the planned `reg_mockdata` rename) have been removed from
`main` and preserved in branch `archive/mona-subsystem` (tag
`mona-subsystem-pre-rebuild`), pending a from-scratch rebuild. The archived subsystem
covered: kit-build (`POST /api/kit` + `codes.json` + stats v1 — §8), the
`mock_data_wizard` → `reg_mockdata` rename and reg_meta-dep removal (§9), and the
realign-then-extract MONA workflow + standalone runner build (§10a). Archived under
#699; the from-scratch rebuild is tracked in #707.

The `reg_webapp` `/api/bundle` and `/api/kit` endpoints are removed along with the
packages they depended on. The surviving authoring surface is `/api/project/validate`,
`/api/project/order`, and the SPA's order-CSV download. The typed `reg_monabundle` block
field has been **removed** from `reg_schema`'s `ProjectData` (#702): it was a vestige of
the deleted bundle consumer, the sole reason that field was modeled. #1134 removed the
remaining generic namespaced-root mechanism: archived project files receive no migration
or compatibility path, and unknown root keys are structural errors.

Step 10b (composite `entity_key` / `time_key` runtime support) gates on the MONA rebuild
rather than on §10a as originally planned.

## 10b — Composite `entity_key` / `time_key`

The panel schema already accepts composite `entity_key` (firm × workplace, household ×
person) and composite `time_key` (year × quarter). Runtime support is deferred until the
MONA rebuild (§8/9/10a — tracking issue #707) provides the extract and generate
surfaces. The schema-level composites are additive; single-key panels keep working
unchanged.

## 11 — Steward catalogs

SWECOV branding is tracked in `reg_webapp/stewards/swecov/steward.toml`; accepted
physical inventories and the policies selected by accepted private candidates remain
private builder inputs. The tracked `source_policy.toml`, `inventory_overlay.toml` and
`holdings_policy.toml` are generator defaults, not proof of an accepted candidate's
policy bytes. The tracked `reg_meta_build/input_data/swecov/build_catalog.py inventory`
command generates the physical inventory; no concrete inventory is shipped as a loose
runtime file. Schema-9 runtime holdings, scoped admission and ordering read the selected
compiled steward artifact. The builder compiles one validated global base with steward
metadata and holdings; no loose inventory, runtime reconciliation or drift gate is
shipped. The compiled series was released as `reg_meta/v0.41.0` (with
`reg_meta_build/v0.31.0`) and deployed to `catalog.swecov.se` and `data.swecov.se` on
2026-10-06.

Deployment configuration targets `data.swecov.se` through the separate
`reg-webapp-swecov` Fly app, using app-scoped `FLY_API_TOKEN_SWECOV`. Its reader
requires a publishable artifact whose manifest steward matches the deployment. A global
artifact cannot substitute for that steward artifact. `reg_meta_swecov.db.zst` remains a
public GitHub release asset on the same `reg_meta/v*` tag as the global catalog and
public docs asset. The release skill produces and uploads it; `integration.yml` verifies
and admits it, and `container-build.yml` supplies its `tag`, `url` and `sha256` through
the BuildKit manifest. The bake refuses tag or digest mismatches. Selected sibling docs
preserve the same release identity; local acceptance does not verify live deployment or
publication.

IFAU authoring remains deferred. Before v1, extract SWECOV branding and its delivery
pipeline into its own steward system and make that system copyable for future stewards.
The SPA catalog-authoring mode (distinct from project authoring) and a `reg-meta-build`
steward-diff CLI remain deferred post-v1. Holdings are authored or generated builder
inputs, not another `ProjectData` catalog filter. Both product surfaces emit the
normalized JSON order manifest documented in the owning reader design.

## 12 — Steward delivery inventory + normalized order manifest

**Compiled holdings shipped (schema 9, accepted, released as `reg_meta/v0.41.0` and
deployed 2026-10-06).** One self-identifying SQLite generation carries catalog, holdings
and order facts; there is no runtime inventory. Owning contracts live in
`ARCHITECTURE.md`, `reg_meta/DESIGN.md` and `reg_meta_build/DESIGN.md`. The ratified
decision text below remains only because product comments cite it as `REFACTOR_SPEC.md`
§12; the open items are listed at the end of this section. One common manifest, no
per-steward export templates. A `project_data.json` source is a logical selection; a
steward's physical delivery topology is separate data. Each inventory table has an
opaque identifier (an exact filename or schema-qualified SQL table), one explicit
physical edition, and its literal, case-preserving physical columns. Edition uses the
existing finite period grammar — year, month, day, quarter, semester, or a finite
multi-period range/list. Explicit year-independent scope uses `"_default"` without date
intervals; retained unknown scope carries authored evidence and is nonorderable. Neither
scope is inferred as an unbounded "all periods" sentinel. A table without an edition
encoded in its name still requires an explicit curated edition; filename inference must
fail for review on zero or ambiguous period tokens rather than guess.

Each physical column has zero or more semantic mappings. A mapping names
`register_variant`, variable FQID and required literal `representation`. The compiler
stores their catalog IDs and the representative spelling of the resolver-emitted
delivery column the literal folds onto under shared `lower()`; semantic applicability is
resolved only at query time. Zero mappings keep an unresolved physical column in the
coverage denominator without admitting or ordering it. Several mappings let one physical
table/column serve multiple register variants (the existing combined Utrikeshandel table
does); several tables may map the same logical coordinate over **disjoint** editions
(the ordinary annual series).

**One-to-one resolution invariant (ratified 2026-09-01).** Every admitted
`(register_variant, variable, representation, period)` cell resolves to exactly **one**
physical `(table, column)` — the extraction tool never chooses between sources.
Inventory validation (and therefore the compiled steward artifact's build) **errors**
whenever two mappings could serve the same cell: same variant + variable, same canonical
representation and overlapping editions, whether across tables or across columns of one
table. The inventory describes **current holdings only**: a superseded delivery (a
cumulative re-delivery replacing an earlier snapshot, e.g. the dated `FHM_NVR_Covid*`
series) is discarded at curation, and this validation error is the supersession worklist
— the generator must fail for review on each conflict rather than auto-picking a
survivor (a filename date is not proof of supersession). Zero-column cells are not
errors; they are simply not admitted. A **special-purpose sub-extract** — data already
held, re-delivered at another level of detail for a narrow purpose (the `RTB_SaBo_*`
extracts, the `SWECOV_SOS_*_comorb` comorbidity tables) — is likewise excluded from the
order surface at curation, under its own exclusion reason class distinct from
supersession (ratified 2026-09-01).

**Disjoint-partition arm (ratified 2026-09-01).** Some registers arrive as several
tables partitioned by sub-population **within one edition** — survey strata
(`ITftg_Mikro`/`ITftg_Stora`), reporter streams (`Arb_`/`Soc_AGIIndivid`),
administrative splits (`NDR_adults` over/under 70), per-municipality deliveries (SÄBO) —
and are deliberately unified as **one** user-facing variant: nothing semantic differs
across the shards, and no researcher should have to know delivery trivia to get the
whole register. Partitions are therefore an inventory/order-layer concept, never a
catalog concept. An inventory table may carry an optional `partition` label (a short
slug). The one-to-one invariant then holds per `(cell × partition)`: two tables mapping
the same cell over overlapping editions conflict **unless** they carry distinct
partition labels. Labels are explicit curated facts, never inferred — so a true
redelivery cannot hide behind partitions without a reviewable curation line saying so.
The materializer matches every partition of a cell, and **extraction preserves delivery
topology: what goes in as two tables comes out as two files** — one output file per
contributing inventory table, the filename gaining a partition token
(`agi_individuppgifter-agi_arb_2021-03.csv`). Never a union: shard identity (reporter
stream, municipality) may not exist as a column, so concatenation would destroy
information; an identity-redundant split merely costs the researcher one concatenation
they can always do themselves. Under the invariant, "one file per (ordered variant,
edition segment, partition)" and "one file per table" coincide; a combined table backing
several ordered variants still emits per ordered variant.

Accepted inventory TOML, policy and raw census are builder inputs. Four physical
relations plus manifest identity are the sole runtime holdings truth. Derive exact
edition-aware admission, coverage, browse unions and order materialization from those
facts and query-time resolution. No compiled semantic segments, findings relation,
excluded rows or second runtime inventory model. Replace `steward.project_data.json` in
the same pre-v1 cutover rather than adding a dual-source compatibility path. **Format
(ratified 2026-08-31): the inventory is authored as TOML**, following the repo's
generated-`auto.toml`-plus-curated-overrides pattern — humans touch it (explicitly
curated editions, comments carrying curation rationale), so a comment-capable format is
required. Inventory version stays 1 because the required representation rule changes no
accepted bytes. The compiled relational contract is fixed in the owning designs.
Inventory ↔ reg_meta DB consistency (every mapping's
`(register_variant, variable FQID, representation)` resolves against the flavored DB) is
a **standing build/CI gate**, not a one-off check.

**The v1 order manifest is a versioned JSON contract (ratified 2026-08-31, replacing the
earlier nine-column CSV decision).** It is machine-written by the materializer and
machine-read by the steward-side extract system (MONA for SWECOV; other stewards on
their own private runtimes), never hand-edited, and validated by a Pydantic contract at
both boundaries. It must be **self-contained for offline steward-side extraction** — no
network, no catalog lookup: order metadata plus provenance (steward, catalog/DB
versions, project identity/hash) plus resolved entries, each carrying the logical
coordinate (`provider,register,variant,variable`), the requested period, and the
physical coordinate (`edition,table,column`). Serialization is deterministic (sorted
keys, stable entry order). Extraction output is one UTF-8 CSV per (variant, edition
segment, partition), named in **slug spelling** derived from the manifest entry (e.g.
`lisa_individer-15plus_2019.csv`; a partitioned table adds its partition token; a
multi-period range segment renders `lo..hi` and extracts whole as one file — v1 has no
row filter, so a range is never split per year). The naming convention is pinned in the
order contract, not improvised by the extractor; steward display casing is not carried
in the manifest (decided 2026-08-31, A-28 — re-add only if a steward-side consumer
concretely needs display-cased filenames). A human-readable table rendering of the
manifest may exist as a derived view for the executing data manager; the JSON is the
contract.

Rules:

- every researcher project declares an explicit requested period. Finite sources use the
  shared period grammar; the whole `"_default"` selects year-independent data only at an
  explicit concrete variant with matching year-independent states/holdings. It cannot be
  an all-years sentinel or accompany the variant-less `_default` coordinate; missing or
  incompatible independent holdings block the order, never gain dates;
- the SPA's common study window is an authoring default, not hidden schema inheritance.
  When a source has any overlap, adding it persists the full available intersection by
  default, preserving every disjoint segment. With no overlap, block the add and explain
  the incompatibility in the picker rather than inventing a period. If a later common-
  window edit leaves an existing source disjoint, retain its explicit period and mark
  the project blocking. Highlight every divergence in both the picker and project page.
  A common-window edit never silently rewrites existing source periods; an explicit
  "apply overlap to all" action performs that rewrite where overlap exists;
- **intersection semantics (ratified 2026-08-31):** a source period expresses "these
  columns, wherever each is available inside this window." Before slicing, clip each
  binding to that column's documented availability window; a clip is **reported
  informationally per binding** ("DispInk09: available from 2019, ordered 2019–2020"),
  never silent, and never an error. This is what resolves the variable-by-period matrix
  (dogfood 2026-08-30 P0.4) without schema change: one source per variant, no
  cross-product over-order;
- for each selected binding, after availability clipping, resolve any representation
  changes into deterministic logical slices. A steward table matches a slice only when
  its mapping exactly matches `(register_variant, variable, representation)` and its
  physical edition overlaps that slice; overlap elsewhere in the overall request does
  not match. The edition contributes only its overlap with that slice to coverage. The
  union of those slice-clipped contributions must cover the full availability-clipped
  requested period, including every segment of a disjoint request. Any uncovered
  subperiod **within a column's availability window** blocks the entire order with the
  exact gaps; overlap alone never permits a partial manifest;
- after that coverage gate passes, emit every table matching at least one slice by
  default — every partition included; v1 has no table chooser and no separate population
  field;
- a matching multi-period table is ordered whole, even when its matched slice covers
  only a subset of the table's edition;
- steward entries carry the literal physical `table` and physical `column`. The
  canonical `representation` is a join discriminator, not an output substitute, and
  `display_name` is not a delivery coordinate;
- the confirmed global-deployment fallback is the same entry shape with blank `table`,
  the resolved canonical column in `column`, and `edition = requested_period` until a
  physical global inventory exists. It obeys the same full-coverage gate using canonical
  resolution; if representation changes across the request, fan out deterministically,
  and block unresolved or ambiguous bindings and any gaps within the
  availability-clipped request; availability clipping itself remains informational;
- `steward` names the active deployment/artifact. `ProjectData.steward` is provenance
  and must match before ordering;
- uploading a project to a steward deployment always validates it against that
  deployment's compiled holdings. A provenance mismatch blocks ordering. Steward
  retargeting is deliberately not an application feature: a user who intends to change
  provenance edits the JSON and uploads it again;
- an empty project remains a structurally valid editable draft but is not order-ready
  and cannot produce a header-only manifest;
- missing, unresolved, or ambiguous logical-to-physical mappings are blocking order
  errors, never best-effort labels;
- preserve project source/binding order and sort fan-out rows deterministically by
  table, canonical edition, then physical column;
- the materializer and semantic resolution are shared `reg_meta` domain code, with thin
  FastAPI and CLI/plugin adapters. FastAPI serves the SPA; the local agent/CLI loads the
  selected compiled SQLite generation directly. Both adapters must emit byte-identical
  results. This deliberately adds `reg_meta → reg_schema` rather than creating another
  package.

**Deferred (2026-08-31): per-binding period override.** No schema change now —
intersection semantics above cover every observed case. If a researcher ever
deliberately wants *less* than the availability intersection, the shape is a
binding-level period override narrowing below the variant-level source period
independent of availability (e.g. source LISA 2000–2020 but `DispInk09` only 2000–2002
even though it exists later too). File it when someone actually asks; the availability
clip leaves the seam (an override is just a further clip).

`simplify:` v1 records no row filter and includes the whole matching table. Add
table-specific period predicates when steward delivery/extraction consumes the manifest.
SWECOV's one-large-SQL-table-per-SoS-register delivery is the known upgrade trigger; it
will need period-column `WHERE` clauses later.

### Status and remaining items

The compiled-holdings cut shipped through plans 00 to 05b of the series and passed
independent acceptance on 2026-10-06 against the pinned schema-9 public and SWECOV
steward artifacts. It was released the same day as `reg_meta/v0.41.0` and
`reg_meta_build/v0.31.0`, after the builder defect fix (#1160) refilled
`classification_fts` and added `ANALYZE` to builds, and both deployments serve schema 9.
The closed project root (#1134) and the 2026-07-14 interface decisions now live in
`reg_schema/DESIGN.md`, `reg_meta/DESIGN.md` and `reg_webapp/DESIGN.md`.

Still open under this section:

- **Package test sweep:** see "Remaining test coverage" below.
- **Reader defects found by the sweep:** CLI alias fallback returns unheld reference
  metadata under holdings scope; concept-group `matched_count` counts hit rows, not
  distinct members; `--no-fold` page-1 order depends on `limit`; the three-hint cap
  drops the "Table view truncated" hint; the first-run docs download ignores the
  selected catalog. Each needs a regression case in the owning reader corpus.
- **Shared semantic pass:** the project semantic layer still lives in the webapp
  (`reg_webapp/DESIGN.md` → "Current semantic validation"); the rules above place it in
  shared `reg_meta` code.
- **Common study window:** the SPA authoring default in the rules above is not built
  (`reg_webapp/DESIGN.md`, "Still to come").
- **Citation retarget:** every `rg '§12'` hit across tracked files, including the
  codegen'd frontend types, cites this section as decision text. Move the per-binding
  period-override deferral and the row-filter `simplify:` into `reg_meta/DESIGN.md` →
  "Order materializer and manifest", retarget those citations to the owning
  `reg_meta/DESIGN.md` sections, then delete this section.

Diagnostic and MONA/PII guards remain intact.

## P — Measured web performance hardening

The 2026-07-14 production trace establishes the v1 baseline and rationale in
`reg_webapp/DESIGN.md` → "Production performance baseline". This lane gates v1 quality
but is file-disjoint enough to run alongside the steward inventory/order work. Do not
turn it into generic frontend optimization: the home page and interactions are already
fast, and DevTools estimated zero FCP/LCP savings from removing render-blocking CSS.

The first two corrections shipped 2026-07-14. #1135 replaced full-result/count work with
bounded, stable-cursor search and measured a 276.1 ms direct-origin p95 plus 380 ms
browser-cold LCP for `person`. #1136 stabilized the routed shell and classification
loading geometry; exact-head ICD-11-SE traces measured CLS 0.0616 cold and 0.0158
repeat. The budgets remain regression gates, but their implementation history lives in
git and their lasting contracts live in `reg_webapp/DESIGN.md`.

**P1 — early conditional reads and shared-cache policy.** Edge caching is a second
lever, not a substitute for bounded origin work. Derive a content-backed generation
validator at boot so matching conditional reads can complete before route execution, DB
work, or serialization. Give deploy-generation-keyed responses long shared-cache
freshness independently of the short browser freshness policy, and do not synchronously
revalidate popular searches at every short browser expiry. A warm-query gate must prove
the edge does not execute the origin by checking MISS→HIT, `Age`, `CF-Cache-Status`, and
conditional-response behavior. Do not add an in-process response cache unless bounded
SQL later misses the cold budget.

**P1 — classification payload partition.** The current leaf fetched 542.5 KB compressed
/ 3.28 MB decoded before displaying ICD-11-SE. Return classification metadata, edition
relationships, authoritative level buckets (and presentation-only prefix buckets where
the classification explicitly supports them), and only a bounded initial code page.
Fetch codes by expanded bucket, prefix, cursor, or filter query; genuinely flat sets
stay flat rather than promoting the current client heuristics to domain hierarchy. Reuse
reg_meta's existing complete-code export as the separate streamed full export. Initial
detail cost must be bounded by page/bucket limits, not total classification cardinality.
Reuse the same code-page contract for variable value sets instead of building a
classification-only viewer.

**P2 — immutable static assets.** The Workers Assets response currently makes
content-hashed JavaScript, CSS, and fonts revalidate (`max-age=0, must-revalidate`),
costing roughly 24–46 ms per main asset on repeat loads. Stamp hashed `/assets/*`
responses with a long-lived `immutable` policy through Workers Assets' existing
`frontend/public/_headers` capability, while keeping `index.html` and SPA fallback
documents revalidatable. Add an edge response-header test. This follows the query and
CLS work and does not justify CSS extraction, font churn, or preconnect work.

Lane order: the bounded search contract landed in #1138, so classification payload
partitioning can proceed. #1139 landed the CLS correction first; the code-page loading
path must preserve that stable geometry. Early-validator/shared-cache work and
static-asset headers remain parallel-safe.

Completion: the early validator/shared-cache proof, bounded classification payload, and
immutable asset policy ship; the search load harness joins the existing performance
gate; controlled traces keep #1138's search budgets and #1139's CLS < 0.1 as regression
evidence. Move the lasting cache/payload rationale into `reg_webapp/DESIGN.md`, then
delete this section.

## v1 slug freeze (#209)

The grow-only slug-immutability gate is **per-provider**, not global. There is no
`UNFROZEN` sentinel file; freeze state lives in
`reg_meta_build/curation/slug_state.toml` for global registers (and
`reg_meta_build/fqid_slugs/<steward>/freeze.toml` for steward overlays) as a flat TOML
map `<zone> = "<state>"` (absent file or unlisted zone ⇒ `churning`). The three states
advance one-way: `churning` → `curating` → `frozen`. All 8 global providers are now at
`curating` (#759): `slug_state.toml` is committed and their per-register `*.auto.toml`
slugs are pinned. Steward dirs (e.g. `swecov/`) remain churning. The remaining advance
is the per-provider `frozen` seal (#472).

At the v1 release: curation (#471) and the churning→curating advance (#759) have
shipped. What remains is to seal each provider — (1) verify no identity-churn issues are
open for it (the #418 pre-seal re-verify), and (2) set its zone to `frozen` in
`slug_state.toml`, which arms the rename-refusal gate. Classification slugs moved to
`curation/classifications/<short>.toml` (Y-228) and left the slug snapshot and its
freeze zones; their rename guard is a follow-up to #472. There is no single global step
to arm the gate — the seal is per-provider and per-zone. See #470 (machinery), #471
(curation), #472 (seal).

**Preconditions — the hard identity-churn blockers are resolved.** #196 (curated
column-merge primitive + auto case-fold + panel-key re-curation) and #197 (the FRIDA
`borgnr` cross-var_id attribution decision) both churned variable identity — merges
collapse sibling variables and re-mint slugs, exactly what the grow-only gate locks —
and **both closed COMPLETED 2026-06-10**, so neither gates the freeze any longer. The
remaining identity-churn risk to clear *per provider before sealing it* is any open
issue that still splits or re-mints that provider's slugs (e.g. #677 if its RTB "Ålder"
per-column-split path is taken) plus the slug-anchored-overlay staleness debt (#660 —
delivery_enrichment backfills already rotted on churn; regenerate before the seal) and
the missing-canonical-column class (#400/#428 — mint these into the baseline rather than
as a post-freeze grow-only wave). None are hard blockers; they are the curation backlog
that makes the sealed baseline clean.

**Auto-derivation improvement — shipped, derived *from* the curation, not before it.**
The curation fan-out ran first — agents turned the worklist into final canonical FQIDs
(#471, \~11,802 SCB name-derived slugs); those results were then mined for the
systematic rules the auto-slugger could absorb (#732: one safe lever shipped, broader
levers deferred with evidence). The reconcile then pinned each curated result to its
final FQID (#759). The curated final FQID is **authoritative**: a generator change only
decides override-vs-auto (the reconcile pins each result regardless), so it shrinks the
committed-override surface and improves defaults for future deliveries without altering
any outcome. Non-levers confirmed: there is **no Swedish→English glossary** to expand
(the "glossary" is a DB-column rename), and `v<digit>` slugs are mostly real SCB column
codes.

The reserved HTTP-suffix slug rejection (`states`/`predecessors`/…/`variants`) shipped
in #228 — it is already enforced at curation time and does not need to precede the
freeze.

## Remaining test coverage

Carried from the testing strategy; the shipped categories are in
[`ARCHITECTURE.md`](ARCHITECTURE.md). Still to build:

- **Kit reproducibility** — same spec + codes + stats → identical kit zip. Deferred to
  the MONA rebuild (#707; was gated on `/api/kit`).
- **Performance gates** — wire the 200-column fixture into a load-test harness measuring
  project validation/materialization p95; add release-DB broad-search cases that enforce
  the cold-query budget without relying on edge hits; bound classification detail
  payloads independently of corpus cardinality; retain controlled cold/repeat CLS trace
  evidence; and probe immutable hashed assets plus the search edge MISS→HIT contract.
  See `ARCHITECTURE.md` → Repo-wide invariants.
- **Per-package test sweep** — root `conformance/` now owns the relocated corpora and
  synthetic/real artifact checks. The `reg_webapp` backend, `reg_meta` and
  `reg_meta_build` suites were swept under the `AGENTS.md` policy (plans 06a–06c, #1153
  to #1162). Left: sweep `reg_schema`, and drain the remaining allowlist entries
  (private boundaries: `reg_meta_build/tests/_shared_fixtures.py`,
  `reg_meta_build/tests/test_source_scope.py`, `reg_schema/tests/test_structural.py`,
  `scripts/tests/test_gh_issue.py`, `scripts/tests/test_prototype_scb_inputs.py`; size:
  `reg_schema/tests/test_structural.py`). Accepted-private-input census is shipped via
  opt-in `--holdings-input`. Historical-order comparisons, latency and cold-boot
  measurements remain separate maintainer checks. At the schema-9 acceptance, local
  TestClient measurements were within every holdings budget and cold boot showed no
  regression against `b45f916d` in the same environment; nothing was measured on Fly.
  See `ARCHITECTURE.md` → Testing strategy.

## Open / deferred decisions

- **MONA rebuild** — the archived §8/§9/§10a work (kit-build, mock-data generation,
  realign-then-extract workflow) is deferred to a from-scratch rebuild (#707). The
  realign-patch-lifecycle and `same_as`-at-generate-time questions are also gated on
  this rebuild.
- **Chronological period `kind` field** — a future `kind` (`year_month`,
  `academic_term`, `quarter`) on the `{"period": …}` object form so the generator can
  impose chronological ordering. Schema is forward-compatible; not designed now.
  (Distinct from #207/#219.)
- **Per-steward repo autonomy** — SWECOV stays in this monorepo only as the proving
  steward for pre-v1 testing. The v1 release target is an extracted SWECOV steward
  repo/system whose build/deploy shape can be copied for later stewards, rather than
  treating `reg_webapp/stewards/swecov/` as the permanent distribution model.
- **Variable slug source on rename** — the auto-rule mints a new slug for later editions
  when SCB renames a column before a curator adds `same_as`. The behaviour is fine
  (rename = new variable by default); the curator review cadence is undecided. Overlaps
  #209.
- **LISA composite-source presentation** — the lineage data + endpoints ship; the UX
  treatment (tooltip vs "see also" panel) is a webapp authoring-UI decision.
- **Sub-annual-coding providers (#271)** — SHIPPED ahead of the original post-v1 trigger
  (2026-06-11, deferral revised): the co-delivery resolver is interval-native end-to-end
  — provider-blind engine in `reg_meta_build/resolution.py`, SCB conventions in the
  adapter (see `reg_meta_build/DESIGN.md` → Interval-native co-delivery resolution). The
  term-split bolt-on (Option A) remains permanently rejected. Remaining #271 follow-up:
  the monthly-column-family merge (the design's consumers section) and per-variant month
  claim windows when a genuinely month-stamped provider lands.
- **Materializer-owned value tables (#212)** — retiring the A4.3b content-shared interim
  is post-v1 work whose real deadline is the third provider adapter (FK/Skatteverket);
  nothing in this plan builds on who writes the value tables.

## Tracking issues

Open issues seeded from or feeding this plan: #707 (from-scratch MONA bundle + mock-data
rebuild epic), #1134 (closed project root), #206 (steward admission keying — decided
column-based 2026-06-11 and implemented), #209 (v1 slug freeze), #196 + #197
(identity-churning curation — pre-freeze), and #200 + #266 (authoring-UX ride-alongs for
the 7.5 dogfood). Deferred beyond v1 but recorded so pointers resolve: #212
(materializer-owned value tables) and #271 (interval-native resolver). Resolved since
this spec was seeded: #1135 (bounded search, PR #1138), #1136 (catalog layout stability,
PR #1139), #699 (MONA bundle and mock-data archive, closed when PR #700 removed the
subsystem), #220 + #224 + #278 (the 6.5 deployment set, closed when 6.5 shipped
2026-06-11), #210 (SOS classification path, closed via PRs #273/#274), #211 (LOVA/LVM
deldatamängd→variant curation, shipped early via PR #359 2026-06-12 instead of batching
with step 11; merge-quality follow-up in #362), #208 (closed with the
classification-slug surface, not the keyspace question), #217 (kit-build — archived to
#699), #240 (MSSQL integration test — archived to #699), #227 (wire
`fqid_outside_steward_catalog`), and #228 (reserved suffix slugs).
