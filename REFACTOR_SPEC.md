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

**Remaining (this document):** composite panel keys, the remaining real steward
coverage, measured web performance hardening, the `reg_meta_build` restructuring, and
the v1 slug freeze. (Webapp deployment — step 6.5 — shipped 2026-06-11; the
webapp-authoring hard-cut — step 7 — shipped 2026-06-11.)

**Shipped (step 12):** the steward delivery inventory and normalized order manifest
shipped as compiled holdings (schema 9, released as `reg_meta/v0.41.0` and deployed
2026-10-06), and its open items followed: the reader defects (#1166), the per-package
test sweep (#1169), the shared semantic pass in `reg_meta` (#1167) and the SPA common
study window (#1168). Its decisions live in `reg_meta/DESIGN.md` → "Holdings resolution
invariants" and "Order materializer and manifest", `reg_meta_build/DESIGN.md` → "Steward
extension" and `reg_webapp/DESIGN.md` → "Common study window".

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
  | 12      | Delivery inventory + shared normalized order manifest — **shipped**       | 11       | —                |
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
and admits it, and the SWECOV image bakes it through
`reg-meta update --catalog swecov --tag <tag>` (#1164); boot-time steward admission
refuses a mismatched artifact. Selected sibling docs preserve the same release identity.
See `reg_webapp/DESIGN.md` → Deployment.

IFAU authoring remains deferred. Before v1, extract SWECOV branding and its delivery
pipeline into its own steward system and make that system copyable for future stewards.
The SPA catalog-authoring mode (distinct from project authoring) and a `reg-meta-build`
steward-diff CLI remain deferred post-v1. Holdings are authored or generated builder
inputs, not another `ProjectData` catalog filter. Both product surfaces emit the
normalized JSON order manifest documented in the owning reader design.

## P — Measured web performance hardening

The 2026-07-14 production trace establishes the v1 baseline and rationale in
`reg_webapp/DESIGN.md` → "Production performance baseline". This lane gates v1 quality.
Do not turn it into generic frontend optimization: the home page and interactions are
already fast, and DevTools estimated zero FCP/LCP savings from removing render-blocking
CSS.

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
[`ARCHITECTURE.md`](ARCHITECTURE.md). The per-package test sweep is finished: root
`conformance/` owns the relocated corpora and synthetic/real artifact checks, every
package suite follows the `AGENTS.md` policy (plans 06a–06c and #1169), and the test
lints carry no allowlists. Accepted-private-input census ships via opt-in
`--holdings-input`. Still to build:

- **Kit reproducibility** — same spec + codes + stats → identical kit zip. Deferred to
  the MONA rebuild (#707; was gated on `/api/kit`).
- **Performance gates** — wire the 200-column fixture into a load-test harness measuring
  project validation/materialization p95; add release-DB broad-search cases that enforce
  the cold-query budget without relying on edge hits; bound classification detail
  payloads independently of corpus cardinality; retain controlled cold/repeat CLS trace
  evidence; and probe immutable hashed assets plus the search edge MISS→HIT contract.
  Historical-order comparisons, latency and cold-boot measurements remain separate
  maintainer checks. At the schema-9 acceptance, local TestClient measurements were
  within every holdings budget and cold boot showed no regression against `b45f916d` in
  the same environment; nothing was measured on Fly. See `ARCHITECTURE.md` → Repo-wide
  invariants and Testing strategy.

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
rebuild epic), #206 (steward admission keying — decided column-based 2026-06-11 and
implemented), #209 (v1 slug freeze), #196 + #197 (identity-churning curation —
pre-freeze), and #200 + #266 (authoring-UX ride-alongs for the 7.5 dogfood). Deferred
beyond v1 but recorded so pointers resolve: #212 (materializer-owned value tables) and
#271 (interval-native resolver). Resolved since this spec was seeded: #1134 (closed
project root), #1135 (bounded search, PR #1138), #1136 (catalog layout stability, PR
#1139), #699 (MONA bundle and mock-data archive, closed when PR #700 removed the
subsystem), #220 + #224 + #278 (the 6.5 deployment set, closed when 6.5 shipped
2026-06-11), #210 (SOS classification path, closed via PRs #273/#274), #211 (LOVA/LVM
deldatamängd→variant curation, shipped early via PR #359 2026-06-12 instead of batching
with step 11; merge-quality follow-up in #362), #208 (closed with the
classification-slug surface, not the keyspace question), #217 (kit-build — archived to
#699), #240 (MSSQL integration test — archived to #699), #227 (wire
`fqid_outside_steward_catalog`), and #228 (reserved suffix slugs).
