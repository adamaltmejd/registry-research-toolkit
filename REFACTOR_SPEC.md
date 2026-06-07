# Registry Research Toolkit — Remaining Work

Forward plan for the post-A5 work of the Model A refactor. The Model A
schema, FQID grammar, IR/adapter build, `reg_schema` v2, and the
`reg_webapp` backend + SPA all **shipped**; their design rationale now
lives in [`ARCHITECTURE.md`](ARCHITECTURE.md) and the package
`DESIGN.md` files. Per-PR landing history is in git (the
`MIGRATION_PLAN.md` tracker was retired when A5 shipped).

This document is what survives of the original refactor spec: only the
**unbuilt** pieces. It is scoped and self-shrinking — each section moves
into the owning `DESIGN.md` as it ships, and the file is deleted when the
last item lands (target: v1.0).

## Status

**Shipped (A0–A5):** two-level `variable`/`variable_state` catalog,
3-segment `provider/register/slug` FQID grammar, slug curation +
grow-only immutability machinery, edge/lineage tables, build-time
triage, SCB + SOS adapters and the first combined build, `reg_schema`
Pydantic v2 + the `project_data.json` v2 Source schema, and the
`reg_webapp` FastAPI backend + Svelte SPA (catalog browse, project
authoring, IndexedDB autosave, validate / order / bundle endpoints).

**Remaining (this document):** webapp deployment, the realign-then-extract
MONA workflow, kit-build (`/api/kit` + `codes.json` + stats v1),
the `mock_data_wizard` → `reg_mockdata` split, composite panel keys,
the real steward catalogs, and the v1 slug freeze.

The single biggest structural gap: **`reg_mockdata` does not exist
yet.** Its code still lives in `mock_data_wizard/`, reg_meta-coupled and
on the legacy `mock_data_stats.json` contract. `classify.py`/`editor.py`/`server.py`
are already deleted, but the package is not renamed and `web/` still
exists.

## Sequence

A dependency narrative, not a checklist. Numbers continue the original
spec's post-A5 step numbering.

| Step | Work | Gates on | Issues |
|------|------|----------|--------|
| 6.5  | Containerize + Cloudflare + `global` deploy | A5 | #220, #224 |
| 7    | Webapp-authoring hard-cut; delete `mock_data_wizard/web/` | 6.5 | — |
| 7.5  | `global` dogfood (2 weeks) | 7 | — |
| 8    | Kit-build (`/api/kit` + `codes.json` + stats v1) | 7.5 | #217 |
| 9    | `mock_data_wizard` → `reg_mockdata` rename; drop reg_meta dep | 8 | — |
| 10a  | Bundle merged-mode (realign-then-extract) + `reg_monabundle.types` | 9 | — |
| 10b  | Composite `entity_key` / `time_key` support | 10a | — |
| 11   | Steward catalogs (ifau, swecov) | 8 | #206, #210 |
| 12   | Per-steward order templates + `extensions` toggles | 11 | — |
| —    | v1 slug freeze + arm immutability | all | #209 |

## 6.5 — Deployment: containerize, Cloudflare, `global` up

- `reg_webapp` Dockerfile runs `reg-meta update` at image-build time to
  bake the matching reg_meta release's DB into an image layer.
- Cloudflare in front: edge caching with the §9.4 ETag scheme (origin
  ETag/`Cache-Control`/rate-limit machinery already ships — see
  `reg_webapp/DESIGN.md`), per-IP rate limits, DDoS shielding.
- **Edge-cache validation gate (#220):** run a small load through
  Cloudflare to confirm slash-bearing FQID paths round-trip cleanly
  through the edge cache before publishing the OpenAPI. The 3-segment
  binding FQID is edge-cache-friendly; if the edge mangles it, fall back
  to a query-string form *before* the OpenAPI is committed.
- **Provenance-DB confinement (#224):** the deployment image build
  excludes `reg_meta.provenance.db*` (the maintainer-only debug sibling)
  from the catalog volume mount. The bundle-side and route-side
  confinement assertions already ship; this is the third, deployment-side
  assertion.
- `global` deployment goes live serving `/api/catalog`, `/api/context`,
  and the SPA — no authoring UI cutover yet.

## 7 — Webapp authoring hard-cut

Hard cut from any residual local-authoring path to webapp authoring.
`mock_data_wizard.editor`/`server`/`classify` are already deleted;
remaining here is deleting **`mock_data_wizard/web/`** (the superseded
Svelte SPA) and the frozen `mock-data-wizard ui` stub. No parallel run,
no shim (per the compatibility policy). Testers re-author affected
projects.

**7.5 — `global` dogfood (2 weeks).** Testers exercise the full author →
bundle → realign → re-author loop against `global` before kit-build
piles on. Paired with the 200-column load-test fixture for realign-UX
stress. `global` is the staging environment; no separate staging tier.

## 8 — Kit-build (`POST /api/kit`)

`/api/kit` does not exist yet (#217). Kit-build is just file packaging —
no Python-package logic — so `reg_webapp` does **not** depend on
`reg_mockdata`. The webapp emits a downloadable **generation kit**:

- `project_data.json` — the spec with FQID references.
- `project_data.codes.json` — dereferenced codes (see below).
- `project_data.stats.json` — the extract output (uploaded earlier).
- A README and a ready-to-run command.

The user runs `reg-mockdata generate` locally against the kit;
`reg_mockdata` consumes JSON only — no reg_meta dep, fully offline.

### `project_data.codes.json`

Codes live in this sibling file, never in `project_data.json`. Two
keyspaces, split by what determines the list:

- **`classifications`** — keyed by classification FQID (`class/sun2020`).
  The canonical code list, dereferenced from reg_meta at kit-build.
  Period-invariant; shared across every binding that references it via
  `value_set`.
- **`sources`** — keyed by `source.name`, then binding FQID. The codes
  for an ad-hoc-coded binding (no `value_set`) within one source. Nested
  by source because a single binding can project different value sets
  across deliveries (current SCB: 7,423 of 23,864 ad-hoc-coded bindings
  have >1 distinct projected value set across periods). A flat
  binding-FQID key would collide or force a lossy union.

```json
{
  "classifications": { "class/sun2020": [ /* full code list */ ] },
  "sources": {
    "lisa_2010": { "scb/lisa/utbgrp": [{"code": "010", "label": "Förgymnasial utbildning"}] },
    "lisa_2020": { "scb/lisa/utbgrp": [/* extended list */] }
  }
}
```

A categorical binding's `value_set` field selects the path:
`value_set: "class/<…>"` reads `codes.classifications[value_set]`;
absent `value_set` reads `codes.sources[<source.name>][<binding FQID>]`.

> Co-delivered parallel representations of one variable are distinguished
> by the binding-level `representation` field (the retired `@version`
> FQID pin no longer exists — see `reg_meta/DESIGN.md`). Whether the
> `sources` keyspace should key on `(binding FQID, representation)` to
> avoid a same-FQID co-delivery collision is the open question in #206 /
> #208.

After kit-build the trio is **freestanding from reg_meta**: a project
committed to git regenerates the same mock data years later, regardless
of how reg_meta evolves steward-side. Kit-build derives the codes file
fresh each run (orphaned entries from a prior kit are silently dropped —
no explicit GC) and errors loudly when a referenced FQID no longer
resolves. **Codes during authoring:** before kit-build the SPA stores
ad-hoc inline codes in IndexedDB and offers a companion
`project_data.codes.json` download (ad-hoc entries only); kit-build later
populates the `classifications` block.

### `project_data.stats.json` schema (v1)

Produced by the extract phase, consumed by kit-build and
`reg_mockdata`. PII is already removed by aggregation; `null_count` is
suppression-aware. The schema grows as `reg_mockdata` learns more
patterns; v1 fixes the binding-FQID keyspace, the period encoding, and
the sections below.

```json
{
  "schema_version": "2.0.0",
  "project": "swecov-education",
  "generated_at": "2026-03-04T10:30:00Z",
  "reg_meta_version": "reg_meta/v1.0.0",
  "sources": {
    "lisa_2018": {
      "row_count": 8492768,
      "bindings": {
        "scb/lisa/kon": {
          "display_name": "Kon", "nullable": false, "null_count": 0,
          "null_rate": 0.0, "n_distinct": 2,
          "stats": {"frequencies": {"1": 4231000, "2": 4261768}, "suppressed_below_k": 5}
        },
        "scb/lisa/ink": {
          "display_name": "Ink", "nullable": true, "null_count": 152340,
          "null_rate": 0.0179, "n_distinct": 982341,
          "stats": {"min": 0, "max": 5234100, "mean": 425000, "sd": 312000,
                    "quantiles": {"p01": 0, "p05": 50000, "p25": 180000,
                                  "p50": 380000, "p75": 580000, "p95": 1500000,
                                  "p99": 2500000}}
        }
      }
    }
  },
  "shared_columns": [
    {"binding": "scb/lisa/kon", "sources": ["lisa_2018", "lisa_2019"], "max_n_distinct": 2}
  ],
  "panels": [
    {"panel_id": "lisa", "entity_key": "LopNr_PersonNr",
     "members": [{"source": "lisa_2018", "time_key": 2018}],
     "by_period": [{"period": 2018, "source": "lisa_2018", "n_rows": 8492768, "n_entity_ids": 8392104}]},
    {"panel_id": "quarterly", "entity_key": "LopNr_PersonNr",
     "members": [{"source": "sv_quarterly", "time_key": ["AR", "KVARTAL"]}],
     "by_period": [{"period": [2018, 1], "source": "sv_quarterly", "n_rows": 100000, "n_entity_ids": 50000}]}
  ]
}
```

Root keys: `schema_version` (`"2.0.0"`, bumped in lockstep with
`project_data.json` so consumers reject a v0.x stats file against a Model
A spec), `project`, `generated_at`, `reg_meta_version` (drift detection
only, not enforced), `sources`, `shared_columns`, `panels`.

Per-binding type-specific `stats` shapes (keyed off the spec's declared
`type`):

- **`id`** — `{}` (`n_distinct` is the pool-size signal).
- **`categorical`** — `{frequencies, suppressed_below_k}`; codes below
  the threshold are folded/dropped (consumers treat missing as "small
  unknown").
- **`numeric`** — `{min, max, mean, sd, quantiles}`, all deterministically
  perturbed; `min ≤ max`, quantiles monotonic.
- **`date`/`datetime`** — `{min, max}` in the column's format.
- **`opaque`** — `{min_length, max_length, mean_length}`.

`shared_columns[]` sizes cross-source shared pools (`max_n_distinct`).
`panels[]` echoes each spec panel with per-`(member, period)`
`PeriodStat` rows; `period` is `int | string | (int | string)[]` (tuple
for composite time_keys). **Disclosure invariants:** the bundle never
emits a `null_count` in `(0, suppress_k)`; sub-threshold frequencies are
suppressed; consumers treat absent fields as "small unknown ≥ 1".
**Forward-compat:** consumers tolerate unknown keys; new `stats` shapes
are minor bumps, renames/removals are major bumps.

## 9 — `mock_data_wizard` → `reg_mockdata`

Rename the package and delete its `reg_meta` dependency so generation
consumes JSON only. The surviving surface is `reg-mockdata generate` +
`reg-mockdata compare`.

- **`reg-mockdata generate`** — local CSV generation from the kit; reads
  `project_data.json` + `project_data.codes.json` +
  `project_data.stats.json`. Today's `generate` still requires reg_meta
  enrichment (`enrich.py`) and reads the legacy `mock_data_stats.json` —
  both must go; replace the stats parser with the v1 binding-FQID schema
  above.
- **`reg-mockdata compare`** — rewired to read `project_data.json`
  instead of the legacy `manifest.json` (schema `"3"`).
- **Population spine** — birth-invariant attributes (Kön, Födelseår,
  Födelselän, Födelseland) generated once per individual and reused
  across files. Today spine eligibility keys on hardcoded reg_meta
  `var_id`s (`SPINE_VAR_IDS`), so without reg_meta the spine is empty.
  Replace with a hardcoded set of **variable-slug stems** — the trailing
  segment of a binding FQID:

  ```python
  # reg_mockdata.spine
  SPINE_VARIABLE_SLUGS = {"kon", "fodelse-ar", "fodelse-lan", "fodelse-land"}
  ```

  This works across providers automatically (`scb/lisa/kon`,
  `sos/par/kon` are the same variable for spine purposes). The curated
  `same_as` graph is *not* consulted at generate time (the kit is
  reg_meta-free), but `reg_webapp` can use it before kit-build to verify
  the project's "Kön" columns share a canonical variable. The
  cross-provider mismatch case (SCB's `kon` vs another provider's `sex`)
  and a future `reg_mockdata` namespaced override (`spine_groups`) are
  deferred until a concrete project needs them.

`mock_data_wizard`'s `update` subcommand is deleted (users run
`reg-meta update`); the standalone `scan` CLI is replaced by `python -m
reg_monabundle.scan`. The CVID picker is already obsolete under Model A —
`Catalog.resolve_at(fqid, period, …)` returns the `list[VariableState]`
directly, no heuristic scoring.

## 10a — MONA workflow: realign-then-extract

The shipped bundle still runs the legacy two-MODE `discover`/`extract`
model emitting `mock_data_discovery.json`/`mock_data_stats.json`. This
section replaces it. There is no `reg_monabundle.types` module yet and
no realign phase; both land here.

### Single invocation, two phases

`reg_webapp` builds one `.py` per upload via `reg_monabundle.build`,
embedding `project_data.json` as a string literal. The bundle has one
default invocation plus two flag variants:

```text
python project_bundle.py            # realign-then-extract; happy path → stats.json
python project_bundle.py --check    # realign phase only; never extract
python project_bundle.py --force    # skip realign; extract regardless
```

1. **Realign phase** — pulls `INFORMATION_SCHEMA.COLUMNS` + `COUNT(*)`
   only (seconds, no row data). Verifies every spec column's
   `display_name` exists and that the declared `type` is compatible with
   the observed `sql_type` (via `reg_monabundle.types.is_compatible`).
2. **Extract phase** — the aggregation queries (potentially hours).
   Entered only if realign finds zero diffs.

If realign finds diffs the bundle writes `project_data.realign.json`,
exits non-zero, and never extracts — making "I forgot to realign" a
structural impossibility. Happy case is **one** MONA round-trip;
misalignment costs a second after reconciliation. The first historical
round-trip (discover) goes away: the spec is authored from the order,
not derived from the data.

**Build-time pre-resolution.** Before embedding the JSON,
`reg_monabundle.build` resolves every absent `display_name` from
reg_meta (alias resolution) and writes it back, so the embedded JSON
always carries `display_name` on every binding — the bundle never needs
reg_meta on MONA. (Today the runtime instead *requires* hand-written
`display_name` and rejects bindings without it; this pre-resolution is
the on-ramp to making it optional in authored specs.)

`project_data.realign.json` (written only on diffs):

```json
{
  "schema_version": "2.0.0",
  "project": "swecov-education",
  "sources": {
    "lisa_2018": {
      "row_count": 8492768,
      "missing_in_data": [
        {"binding": "scb/lisa/lopnr", "display_name": "LopNr"}
      ],
      "extra_in_data": ["P1105_LopNr_PersonNr", "UnexpectedCol"],
      "type_mismatches": [
        {"binding": "scb/lisa/birthdate", "display_name": "BirthDate",
         "spec_type": "date", "sql_type": "INTEGER"}
      ]
    }
  }
}
```

`missing_in_data` lists columns by binding FQID + the `display_name` the
bundle queried for; `extra_in_data` lists SQL columns found but not
queried; `type_mismatches` lists incompatible declared-vs-observed types.

### Type compatibility lives in `reg_monabundle`

The SQL↔spec-type machinery is owned by `reg_monabundle`'s pure-python
lightweight side (amalgamated into the bundle, imported by `reg_webapp`):

- `is_compatible(spec_type, sql_type) -> bool` — what the extract code
  can ingest (`numeric` ↔ `VARCHAR`/`INTEGER`/`DECIMAL`/`DOUBLE`; `date`
  ↔ `VARCHAR`/`DATE`/`DATETIME` but not `INTEGER`). Drives realign
  mismatch detection.
- `suggest_spec_type(sql_type) -> SpecType` — the inverse, used by the
  realign-review UI to pre-fill "accept SQL type into spec". Always
  returns *some* type; the user can override.

Living in `reg_monabundle` keeps the durable artifacts durable: if the
package learns a new cast, the realign check learns it the same release.

### Reconciling the patch (client-side)

The spec is authoritative. The webapp loads the patch into the in-browser
project state and walks the user through one screen. Four actions:

- **Pair as rename** — link a `missing_in_data` to an `extra_in_data`;
  update the binding's `display_name` to the SQL header. The binding FQID
  (`variable`) is never modified — reg_meta identity is stable.
- **Remove from spec** — drop a truly-absent binding.
- **Add to spec** — a real new delivered column; prompt for a binding
  FQID (chosen against reg_meta via catalog search) and a `type`, store
  the SQL string as `display_name`.
- **Resolve type mismatch** — accept the SQL type into the spec or remove
  the binding. There is no "keep spec type, cast anyway" reconciliation;
  `--force` is the only way to extract past a diff.

The realign-review UI is client-side only — no server endpoint applies
the patch. After reconciliation the in-browser spec updates and the next
bundle download embeds the corrected version.

**`--force` extract semantics.** With realign skipped, extract proceeds
column-by-column: absent `display_name` → warn + skip; present but
incompatible → try the cast, warn + skip on failure. `--force` is the
only path that produces a *partial* stats file; the result should be
treated as provisional.

## 10b — Composite `entity_key` / `time_key`

The panel schema already accepts composite `entity_key` (firm × workplace,
household × person) and composite `time_key` (year × quarter); the
runtime rejects them until now. Additive across three sites:

- **`reg_monabundle.runtime.extract`** — `COUNT(DISTINCT entity_key)` →
  `COUNT(DISTINCT (col_1, col_2, …))`; `GROUP BY <time_key>` → `GROUP BY
  (…)`; tuple periods in `by_period`.
- **stats schema** — `n_entity_ids` becomes a distinct-*tuple* count;
  `period` becomes `int | string | (int | string)[]`.
- **`reg_mockdata` generate** — the shared id pool is keyed by tuple; the
  deterministic shuffle generates shuffled tuples.

Single-key panels keep working unchanged (the schema polymorphism makes
scalar inputs valid). Composite is additive.

## 11 — Steward catalogs

Only `stewards/global/` exists. Author the two real steward catalogs:
each steward's `steward.project_data.json` is built against the `global`
deployment and committed to `reg_webapp/stewards/<id>/`. The Docker image
rebuild picks them up; new hostnames are wired at Cloudflare. Order
export exists in CSV form (default template) for all three. Open
sub-concerns: steward-catalog admission keying (#206) and the SOS
classification data path that some steward catalogs depend on (#210).

Also remaining within the steward surface: the SPA catalog-authoring mode
(distinct from project authoring), a `reg-meta-build` steward-diff CLI,
and wiring `fqid_outside_steward_catalog` into `/validate` (the
membership index ships but is never consulted by the validate path —
tracked separately).

## 12 — Per-steward order templates + `extensions`

The default order CSV (7 columns:
`provider,register,variant,variable,representation,period,display_name`)
ships. Layer on pluggable per-steward `order_template.j2` (IFAU
spreadsheets, SWECOV PDFs) — needs a concrete protocol (input contract,
template language, output MIME type) — and per-steward `extensions`
feature flags (e.g. SWECOV's `swecov.filters` namespaced block), concrete
shape deferred until SWECOV onboarding.

## v1 slug freeze (#209)

The grow-only slug-immutability gate is intentionally lifted pre-v1 by
the on-disk `reg_meta_build/fqid_slugs/UNFROZEN` sentinel; slugs
regenerate from source each build and aren't yet frozen. At the v1
release tag: curate the SCB name-fallback auto-slugs, commit/freeze the
generated `<provider>.auto.toml` files, delete `UNFROZEN`, and arm the
immutability gate. Also enforce the reserved HTTP-suffix slug rejection
(`states`/`predecessors`/…/`variants`) **before** the freeze locks slugs
for good (tracked separately).

## Remaining test coverage

Carried from the §16 testing strategy; the shipped categories are in
[`ARCHITECTURE.md`](ARCHITECTURE.md). Still to build:

- **Kit reproducibility** — same spec + codes + stats → identical kit
  zip (deterministic ordering, no embedded timestamps). Ships with
  `/api/kit` (#217).
- **Per-deploy smoke tests** — golden `/api/context` + shallow
  `/api/catalog` walk on every container start; a failure halts the
  deploy (ships with 6.5).
- **Performance gate** — wire the 200-column fixture into a load-test
  harness measuring the §12 p95 budgets and failing CI on regression.

## Open / deferred decisions

- **Realign patch lifecycle** — should the realign-review UI write the
  accepted patch back into git (download-and-replace) or just produce a
  new `project_data.json` the user replaces manually? Gated behind 10a.
- **Chronological period `kind` field** — a future `kind` (`year_month`,
  `academic_term`, `quarter`) on the `{"period": …}` object form so the
  generator can impose chronological ordering. Schema is
  forward-compatible; not designed now. (Distinct from #207/#219.)
- **`same_as` at generate time** — should `reg-mockdata generate`
  normalize spine grouping via a kit-shipped `same_as` snapshot? A v1.x
  question.
- **Per-steward repo autonomy** — v1 hosts every steward config in this
  monorepo; stewards versioning their own catalogs in their own repos
  would reintroduce external-repo build wiring. Not until a steward asks.
- **Variable slug source on rename** — the auto-rule mints a new slug for
  later editions when SCB renames a column before a curator adds
  `same_as`. The behaviour is fine (rename = new variable by default);
  the curator review cadence is undecided. Overlaps #209.
- **LISA composite-source presentation** — the lineage data + endpoints
  ship; the UX treatment (tooltip vs "see also" panel) is a webapp
  authoring-UI decision.

## Tracking issues

Open issues seeded from or feeding this plan: #206 (steward admission
keying), #209 (v1 slug freeze), #210 (SOS classification path), #217
(kit-build), #220 (Cloudflare edge-cache gate), and #224 (provenance-DB
deployment confinement). Plus the A0–A5 loose ends: issues #227 (wire
`fqid_outside_steward_catalog`) and #228 (reserved suffix slugs).
