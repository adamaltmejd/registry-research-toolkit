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
from-scratch rebuild. Tracking issue #699.

**Remaining (this document):** composite panel keys, the real steward catalogs, and the
v1 slug freeze. (Webapp deployment — step 6.5 — shipped 2026-06-11; the webapp-authoring
hard-cut — step 7 — shipped 2026-06-11.)

## Sequence

A dependency narrative, not a checklist. Numbers continue the original spec's post-A5
step numbering.

  | Step    | Work                                                                      | Gates on | Issues           |
  | ------- | ------------------------------------------------------------------------- | -------- | ---------------- |
  | 6.5     | Containerize + Cloudflare + `global` deploy                               | A5       | #278, #220, #224 |
  | 7       | Webapp-authoring hard-cut; delete `mock_data_wizard/web/`                 | 6.5      | —                |
  | 7.5     | `global` dogfood (2 weeks)                                                | 7        | #200, #266       |
  | 8/9/10a | MONA bundle + mock-data subsystem — **archived** (see below)              | —        | #699             |
  | 10b     | Composite `entity_key` / `time_key` support (gates on MONA rebuild, #699) | —        | —                |
  | 11      | Steward catalogs (ifau, swecov)                                           | 7.5      | #206             |
  | 12      | Per-steward order templates + `extensions` toggles                        | 11       | —                |
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
realign-then-extract MONA workflow + standalone runner build (§10a). Tracking issue
#699.

The `reg_webapp` `/api/bundle` and `/api/kit` endpoints are removed along with the
packages they depended on. The surviving authoring surface is `/api/project/validate`,
`/api/project/order`, and the SPA's order-CSV download. The typed `reg_monabundle` block
field has been **removed** from `reg_schema`'s `ProjectData` (#702): it was a vestige of
the deleted bundle consumer, the sole reason that field was modeled. `reg_monabundle`
now rides through the generic steward-namespaced-block mechanism (`extra="ignore"` +
`structural.py`'s "namespaced block must be an object" check) exactly like `swecov`,
requiring no modeled field.

Step 10b (composite `entity_key` / `time_key` runtime support) gates on the MONA rebuild
rather than on §10a as originally planned.

## 10b — Composite `entity_key` / `time_key`

The panel schema already accepts composite `entity_key` (firm × workplace, household ×
person) and composite `time_key` (year × quarter). Runtime support is deferred until the
MONA rebuild (§8/9/10a — tracking issue #699) provides the extract and generate
surfaces. The schema-level composites are additive; single-key panels keep working
unchanged.

## 11 — Steward catalogs

Only `stewards/global/` exists. Author the two real steward catalogs: each steward's
`steward.project_data.json` is built against the `global` deployment and committed to
`reg_webapp/stewards/<id>/`. The Docker image rebuild picks them up; new hostnames are
wired at Cloudflare. Order export exists in CSV form (default template) for all three.
The SOS classification data path this step depended on shipped (#210, closed via PRs
#273/#274). Remaining sub-concern: steward-catalog admission keying (#206) — resolvable
at step-11 kickoff, when it becomes observable whether IFAU/SWECOV actually restrict at
representation level, but it must close before the first non-global catalog is committed
and its hostname goes live. The LOVA/LVM curation (#211) that was originally batched
into this step shipped early (PR #359, 2026-06-12) — no longer a step-11 concern.

The SPA catalog-authoring mode (distinct from project authoring) and a `reg-meta-build`
steward-diff CLI are **deferred post-v1**: steward catalogs are plain `ProjectData`
files authorable via the existing project editor (or by hand), and steward-vs-reg_meta
drift already surfaces on `/api/context`.

## 12 — Per-steward order templates + `extensions`

The default order CSV (7 columns:
`provider,register,variant,variable,representation,period,display_name`) ships. Layer on
pluggable per-steward `order_template.j2` (IFAU spreadsheets, SWECOV PDFs) — needs a
concrete protocol (input contract, template language, output MIME type) — and
per-steward `extensions` feature flags (e.g. SWECOV's `swecov.filters` namespaced
block), concrete shape deferred until SWECOV onboarding.

## v1 slug freeze (#209)

The grow-only slug-immutability gate is **per-provider**, not global. There is no
`UNFROZEN` sentinel file; freeze state lives in
`reg_meta_build/fqid_slugs/<slug-dir>/freeze.toml` as a flat TOML map
`<zone> = "<state>"` (absent file or unlisted zone ⇒ `churning`). The three states
advance one-way: `churning` → `curating` → `frozen`. The repo ships all-churning — no
`freeze.toml` is committed — so slugs regenerate freely until each provider is
deliberately advanced.

At the v1 release: curate the SCB name-fallback auto-slugs (\~325 pairs — the long-pole
human task; safe to chip at in parallel with steps 8–12), then advance each provider by (1)
force-committing its generated file
(`git add -f reg_meta_build/fqid_slugs/<provider>.auto.toml`) and (2) setting its zone
to `curating` then `frozen` in `freeze.toml`. There is no single global delete to arm
the gate — immutability is per-provider and per-zone. See #470 (machinery), #471
(curation), #472 (seal).

**Preconditions — resolve before committing auto-TOMLs or advancing to `curating`:**
#196 (curated column-merge primitive + auto case-fold + panel-key re-curation) and #197
(the FRIDA `borgnr` cross-var_id attribution decision) both churn variable identity —
merges collapse sibling variables and re-mint slugs — which is exactly what the
grow-only gate locks. Land them in a pre-freeze curation pass (natural slot: around step
7.5); arming the gate while either is unresolved either bakes fragmented identities into
v1 or forces post-freeze immutability exceptions.

The reserved HTTP-suffix slug rejection (`states`/`predecessors`/…/`variants`) shipped
in #228 — it is already enforced at curation time and does not need to precede the
freeze.

## Remaining test coverage

Carried from the testing strategy; the shipped categories are in
[`ARCHITECTURE.md`](ARCHITECTURE.md). Still to build:

- **Kit reproducibility** — same spec + codes + stats → identical kit zip. Deferred to
  the MONA rebuild (#699; was gated on `/api/kit`).
- **Performance gate** — wire the 200-column fixture into a load-test harness measuring
  the p95 budgets (see ARCHITECTURE.md → Repo-wide invariants) and failing CI on
  regression.

## Open / deferred decisions

- **MONA rebuild** — the archived §8/§9/§10a work (kit-build, mock-data generation,
  realign-then-extract workflow) is deferred to a from-scratch rebuild (#699). The
  realign-patch-lifecycle and `same_as`-at-generate-time questions are also gated on
  this rebuild.
- **Chronological period `kind` field** — a future `kind` (`year_month`,
  `academic_term`, `quarter`) on the `{"period": …}` object form so the generator can
  impose chronological ordering. Schema is forward-compatible; not designed now.
  (Distinct from #207/#219.)
- **Per-steward repo autonomy** — v1 hosts every steward config in this monorepo;
  stewards versioning their own catalogs in their own repos would reintroduce
  external-repo build wiring. Not until a steward asks.
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

Open issues seeded from or feeding this plan: #699 (MONA bundle + mock-data archive,
tracking the rebuild), #206 (steward admission keying — decided column-based 2026-06-11
and implemented), #209 (v1 slug freeze), #196 + #197 (identity-churning curation —
pre-freeze), and #200 + #266 (authoring-UX ride-alongs for the 7.5 dogfood). Deferred
beyond v1 but recorded so pointers resolve: #212 (materializer-owned value tables) and
#271 (interval-native resolver). Resolved since this spec was seeded: #220 + #224 + #278
(the 6.5 deployment set, closed when 6.5 shipped 2026-06-11), #210 (SOS classification
path, closed via PRs #273/#274), #211 (LOVA/LVM deldatamängd→variant curation, shipped
early via PR #359 2026-06-12 instead of batching with step 11; merge-quality follow-up
in #362), #208 (closed with the classification-slug surface, not the keyspace question),
#217 (kit-build — archived to #699), #240 (MSSQL integration test — archived to #699),
#227 (wire `fqid_outside_steward_catalog`), and #228 (reserved suffix slugs).
