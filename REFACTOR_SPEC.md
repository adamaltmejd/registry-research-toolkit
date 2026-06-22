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

**Remaining (this document):** the realign-then-extract MONA workflow, kit-build
(`/api/kit` + `codes.json` + stats v1), the `mock_data_wizard` → `reg_mockdata` split,
composite panel keys, the real steward catalogs, and the v1 slug freeze. (Webapp
deployment — step 6.5 — shipped 2026-06-11; the webapp-authoring hard-cut — step 7 —
shipped 2026-06-11.)

The single biggest structural gap: **`reg_mockdata` does not exist yet.** Its code still
lives in `mock_data_wizard/`, reg_meta-coupled and on the legacy `mock_data_stats.json`
contract. `editor.py`/`server.py`/`web/` are already deleted and `classify.py` moved to
`reg_monabundle/runtime/` (it backs the bundle's runtime classification), but the
package is not renamed.

## Sequence

A dependency narrative, not a checklist. Numbers continue the original spec's post-A5
step numbering.

  | Step | Work                                                               | Gates on | Issues           |
  | ---- | ------------------------------------------------------------------ | -------- | ---------------- |
  | 6.5  | Containerize + Cloudflare + `global` deploy                        | A5       | #278, #220, #224 |
  | 7    | Webapp-authoring hard-cut; delete `mock_data_wizard/web/`          | 6.5      | —                |
  | 7.5  | `global` dogfood (2 weeks)                                         | 7        | #200, #266       |
  | 8    | Kit-build (`/api/kit` + `codes.json` + stats v1)                   | 7.5      | #217             |
  | 9    | `mock_data_wizard` → `reg_mockdata` rename; drop reg_meta dep      | 8        | —                |
  | 10a  | Bundle merged-mode (realign-then-extract) + `reg_monabundle.types` | 9        | #240             |
  | 10b  | Composite `entity_key` / `time_key` support                        | 10a      | —                |
  | 11   | Steward catalogs (ifau, swecov)                                    | 8        | #206             |
  | 12   | Per-steward order templates + `extensions` toggles                 | 11       | —                |
  | —    | v1 slug freeze + arm immutability                                  | all      | #209, #196, #197 |

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
and the package's bun usage is gone. No parallel run, no shim. The package's stale
narrative docs (`## Editor API`, `## Web UI` in `mock_data_wizard/DESIGN.md`) are
rewritten at step 9 with the rename (see below). Testers re-author affected projects.

**7.5 — `global` dogfood (2 weeks).** Testers exercise the loop that exists at this
point — author → order → bundle → (legacy) extract → re-author — against `global` before
kit-build piles on. Paired with the 200-column load-test fixture for re-author stress.
Realign does not exist yet (it ships in 10a); the realign-review UX gets its own focused
dogfood window inside 10a instead. Authoring-UX ride-alongs #200 (stable editor list
keys) and #266 (rank/default for parallel-delivery choosers) should land before or early
in this window so dogfood feedback isn't polluted by known glitches. `global` is the
staging environment; no separate staging tier.

## 8 — Kit-build (`POST /api/kit`)

`POST /api/kit` **shipped** (A5.2c, #217 — see `reg_webapp/DESIGN.md` → Kit-build
surface). Kit-build is just file packaging — no Python-package logic — so `reg_webapp`
does **not** depend on `reg_mockdata`. The webapp emits a downloadable **generation
kit** (`application/zip`):

- `project_data.json` — the spec with FQID references, every binding's `display_name`
  materialized.
- `project_data.codes.json` — dereferenced codes (see below).
- `project_data.stats.json` — the extract output (researcher-supplied; the kit endpoint
  does NOT emit it — the README tells the researcher to drop their MONA-returned stats
  file beside the kit before running).
- A README and a ready-to-run command.

The user runs `reg-mockdata generate` locally against the kit; `reg_mockdata` consumes
JSON only — no reg_meta dep, fully offline.

Validation gates on errors before assembling (structural → block → semantic →
cross-block referential, plus the kit-only `panel_inheritance_unresolvable` check).
**Panel-key materialization** (writing inherited `entity_key`/`time_key` into the kit's
`project_data.json` the way `display_name` is) is deferred until `reg_mockdata`'s panel
contract settles (step 10b) — the check guarantees inheritance is resolvable, which is
what unblocks #217's kit half. **Still open (not kit-coupled, may land separately —
#217):** the `deprecated_traversal` (needs a reg_meta `deprecated` flag) and
`variable_replaced` (needs a structured-successor-hint slot on the issue) semantic
codes.

### `project_data.codes.json`

Codes live in this sibling file, never in `project_data.json`. Two keyspaces, split by
what determines the list:

- **`classifications`** — keyed by classification FQID (`class/sun2020`). The canonical
  code list, dereferenced from reg_meta at kit-build. Period-invariant; shared across
  every binding that references it via `value_set`.
- **`sources`** — keyed by `source.name`, then binding FQID. The codes for an
  ad-hoc-coded binding (no `value_set`) within one source. Nested by source because a
  single binding can project different value sets across deliveries (current SCB: 7,423
  of 23,864 ad-hoc-coded bindings have >1 distinct projected value set across periods).
  A flat binding-FQID key would collide or force a lossy union.

```json
{
  "classifications": { "class/sun2020": [ /* full code list */ ] },
  "sources": {
    "lisa_2010": { "scb/lisa/utbgrp": [{"code": "010", "label": "Förgymnasial utbildning"}] },
    "lisa_2020": { "scb/lisa/utbgrp": [/* extended list */] }
  }
}
```

A categorical binding's `value_set` field selects the path: `value_set: "class/<…>"`
reads `codes.classifications[value_set]`; absent `value_set` reads
`codes.sources[<source.name>][<binding FQID>]`.

> Co-delivered parallel representations of one variable are distinguished by the
> binding-level `representation` field (the retired `@version` FQID pin no longer exists
> — see `reg_meta/DESIGN.md`). **Decided 2026-06-11 (#206/#217): the keyspaces are
> column-based** — keyed on `(binding FQID, resolved delivery column)`, where the column
> is the binding's **resolved** `delivery_column_name` (its `representation` when
> pinned; the unique column otherwise), never the raw `representation` string. The
> exposure is real: the build deliberately keeps coexisting cross-column parallel
> deliveries (PR #265, \~1,372 cross-column pairs kept, \~1,078 of them
> identical-coding). The steward-admission facet is **implemented** (#206:
> `CatalogIndex` stores resolved-column pairs, `admits(fqid, column)`, and the distinct
> `representation_outside_steward_catalog` finding — see `reg_webapp/DESIGN.md` →
> Steward layering); the kit-contract facet (codes.json `sources` + stats `bindings`
> keyspaces) applies the same keying when #217 implements them. (#208, once named here,
> closed with a different deliverable — the classification-slug surface — and no longer
> tracks this.)

After kit-build the trio is **freestanding from reg_meta**: a project committed to git
regenerates the same mock data years later, regardless of how reg_meta evolves
steward-side. Kit-build derives the codes file fresh each run (orphaned entries from a
prior kit are silently dropped — no explicit GC) and errors loudly when a referenced
FQID no longer resolves. **Codes during authoring — DECIDED (#217, 2026-06-15):** the
`sources` codes are **dereferenced from reg_meta at kit-build**, not SPA-authored. The
A5.2c kit-build dereferences both keyspaces (`classifications` + `sources`) from
reg_meta; the SPA-authored-IndexedDB affordance (ad-hoc inline codes + a companion
`project_data.codes.json` download) was explicitly out of #217's scope and is **not
built** — if it is ever wanted, it is a separate frontend effort. IndexedDB persists
only the full draft today.

### `project_data.stats.json` schema (v1)

Produced by the extract phase, consumed by kit-build and `reg_mockdata`. PII is already
removed by aggregation; `null_count` is suppression-aware. The schema grows as
`reg_mockdata` learns more patterns; v1 fixes the binding-FQID keyspace, the period
encoding, and the sections below.

```json
{
  "schema_version": "3.0.0",
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

Root keys: `schema_version` (`"3.0.0"` — deliberately distinct from both the legacy
bundle's `contract_version: "2.0.0"` and any v0.x stats file, so a parser pointed at the
wrong generation fails loudly instead of silently mis-parsing), `project`,
`generated_at`, `reg_meta_version` (drift detection only, not enforced), `sources`,
`shared_columns`, `panels`.

Per-binding type-specific `stats` shapes (keyed off the spec's declared `type`):

- **`id`** — `{}` (`n_distinct` is the pool-size signal).
- **`categorical`** — `{frequencies, suppressed_below_k}`; codes below the threshold are
  folded/dropped (consumers treat missing as "small unknown").
- **`numeric`** — `{min, max, mean, sd, quantiles}`, all deterministically perturbed;
  `min ≤ max`, quantiles monotonic.
- **`date`/`datetime`** — `{min, max}` in the column's format.
- **`opaque`** — `{min_length, max_length, mean_length}`.

`shared_columns[]` sizes cross-source shared pools (`max_n_distinct`). `panels[]` echoes
each spec panel with per-`(member, period)` `PeriodStat` rows; `period` is
`int | string | (int | string)[]` (tuple for composite time_keys). **Disclosure
invariants:** the bundle never emits a `null_count` in `(0, suppress_k)`; sub-threshold
frequencies are suppressed; consumers treat absent fields as "small unknown ≥ 1".
**Forward-compat:** consumers tolerate unknown keys; new `stats` shapes are minor bumps,
renames/removals are major bumps.

**Producer/consumer window:** the contract is fixed here (step 8) but its real producer
is the extract rewrite in 10a — steps 8–9 are fixture-driven. One set of golden v1
fixtures is the shared source of truth: step 8's kit tests, step 9's `reg_mockdata`
parser, and 10a's emitter all test against the same files, and the emitter lands once,
inside 10a (the legacy two-MODE emitter is never restructured).

## 9 — `mock_data_wizard` → `reg_mockdata`

Rename the package and delete its `reg_meta` dependency so generation consumes JSON
only. The surviving surface is `reg-mockdata generate` + `reg-mockdata compare`.

- **`reg-mockdata generate`** — local CSV generation from the kit; reads
  `project_data.json` + `project_data.codes.json` + `project_data.stats.json`. Today's
  `generate` still requires reg_meta enrichment (`enrich.py`) and reads the legacy
  `mock_data_stats.json` — both must go; replace the stats parser with the v1
  binding-FQID schema above.

- **`reg-mockdata compare`** — rewired to read `project_data.json` instead of the legacy
  `manifest.json` (schema `"3"`).

- **Population spine** — birth-invariant attributes (Kön, Födelseår, Födelselän,
  Födelseland) generated once per individual and reused across files. Today spine
  eligibility keys on hardcoded reg_meta `var_id`s (`SPINE_VAR_IDS`), so without
  reg_meta the spine is empty. Replace with a hardcoded set of **variable-slug stems** —
  the trailing segment of a binding FQID:

  ```python
  # reg_mockdata.spine
  SPINE_VARIABLE_SLUGS = {"kon", "fodelse-ar", "fodelse-lan", "fodelse-land"}
  ```

  This works across providers automatically (`scb/lisa/kon`, `sos/par/kon` are the same
  variable for spine purposes). The curated `same_as` graph is *not* consulted at
  generate time (the kit is reg_meta-free), but `reg_webapp` can use it before kit-build
  to verify the project's "Kön" columns share a canonical variable. The cross-provider
  mismatch case (SCB's `kon` vs another provider's `sex`) and a future `reg_mockdata`
  namespaced override (`spine_groups`) are deferred until a concrete project needs them.

- **DESIGN.md rewrite** — `mock_data_wizard/DESIGN.md` is pervasively stale: it still
  documents the deleted editor/server modules, the deleted `web/` SPA (`## Editor API`,
  `## Web UI`), and the deleted `mock_data_config.json` config model (`column_options`,
  `set_column_options`, `snapshot_version`). Rewrite it against the surviving
  generate/compare/spine/stats surface and move it to `reg_mockdata/DESIGN.md`; do not
  salvage the dead sections.

`mock_data_wizard`'s `update` subcommand is deleted (users run `reg-meta update`); the
standalone `scan` CLI is replaced by `python -m reg_monabundle.scan`. The CVID picker is
already obsolete under Model A — `Catalog.resolve_at(fqid, period, …)` returns the
`list[VariableState]` directly, no heuristic scoring.

## 10a — MONA workflow: realign-then-extract

The shipped bundle still runs the legacy two-MODE `discover`/`extract` model emitting
`mock_data_discovery.json`/`mock_data_stats.json`. This section replaces it. There is no
`reg_monabundle.types` module yet and no realign phase; both land here.

### Build the runner standalone (decision)

**Decided 2026-06-22 (#680; epic #679).** Earlier §10a planning grew the *amalgamated*
runtime — e.g. adding a `reg_monabundle.types` module that slices into the bundle.
Pivot: §10a builds the MONA runner as an **isolated standalone runner that imports no
toolkit code**. It targets MONA's WinPython 3.13.7 runtime and may use the preinstalled
duckdb/pyodbc/numpy **at module top level** (no lazy-import dance) — the air-gap
austerity binds this runner alone, not a class of "library surfaces" (see
ARCHITECTURE.md → Repo-wide invariants). Today's amalgamator and its tests still exist
and are still current truth; this records *how* the runner is built once §10a lands, not
a change already made.

**Additional constraint from #682 (2026-06-22):** the workspace floor was raised to
`>=3.14` (ruff `target-version = py314`) deliberately above the 3.13.7 MONA ceiling. The
PEP 758 reformat that followed introduced `except A, B:` syntax (3.14-only) into the
current amalgamated slices `reg_monabundle/runtime/classify.py` and `summarize.py`.
§10a's standalone runner must therefore either target 3.14+ or re-parenthesize those
`except` clauses if the runner must execute on MONA's 3.13.7.

Consequences:

- **Delete the AST amalgamator** (`build/__init__.py`'s `ast.parse`/`ast.unparse`
  slicing) **and the amalgamation-specific tests** — bundle determinism and the
  no-Pydantic / no-`reg_meta`-in-bundle *source* scans — which exist only to make
  amalgamation safe and have nothing to test once the runner is standalone. Two of the
  \~950 LOC of amalgamation tests are **not** amalgamation artifacts and survive,
  re-targeted: the **1 MB size-budget cap** is MONA's *upload* limit (re-asserted on the
  emitted standalone `.py`; see reg_monabundle/DESIGN.md "Bundle-size budget"), and the
  **lightweight-surface import-boundary test** guards `reg_webapp`'s runtime-free import
  of `reg_monabundle.build` (no duckdb/pyodbc in the webapp container) — kept or
  replaced, re-targeted onto the standalone artifact / boundary.
- **Keep the PII scanner a runtime export gate.** The standalone runner carries
  `scan.write_export` and runs it on every payload immediately before writing (the same
  in-memory-scan → temp-file → atomic-rename it does today) — **not** a static artifact
  scan: a misclassified personnummer-like column leaks as a frequency-table key only at
  runtime, which a scan of the `.py` source cannot catch.
- **Re-home the provenance-DB-confinement guarantee** (plus the no-`reg_meta` /
  no-Pydantic *source* guarantees) as a **static output-artifact gate** over the emitted
  `.py` — an AST import walk + literal scan — replacing the invariant currently
  maintained through the amalgamation pipeline.
- **Replace the duplicated `COLUMN_TYPES` / `CONTRACT_VERSION` constants** between the
  bundle runner and the local generator (\~33 LOC, hand-copied across modules to avoid
  importing the runtime tier) **with the versioned stats-v1 data contract + golden
  fixtures** (§8): producer and consumer agree on JSON shape, not on duplicated
  constants.

The realign-then-extract *workflow* below is unaffected — it is the MONA workflow being
built; the pivot is only about how the runner is assembled (standalone, not
amalgamated).

**Precondition (#240):** the MSSQL integration test
(`reg_monabundle/tests/test_integration_mssql.py`) has never executed — CI deselects
integration tests wholesale. Before rewriting the extract surface it covers, run it once
on a Docker+pyodbc host
(`uv run python -m pytest reg_monabundle/ --run-integration -k mssql`), fix what
surfaces, and record the green run on #240; it is the only place the bundle's T-SQL ever
meets a real SQL Server. Decide CI wiring at the same time (a documented manual gate for
bundle-SQL-touching PRs is an acceptable terminal answer).

10a closes with a short realign-UX dogfood window: the 200-column fixture against a
deliberately misaligned source, exercising the realign-review screen end-to-end (the
loop step 7.5 could not cover).

### Single invocation, two phases

`reg_webapp` builds one `.py` per upload via `reg_monabundle.build`, embedding
`project_data.json` as a string literal. The bundle has one default invocation plus two
flag variants:

```text
python project_bundle.py            # realign-then-extract; happy path → stats.json
python project_bundle.py --check    # realign phase only; never extract
python project_bundle.py --force    # skip realign; extract regardless
```

1. **Realign phase** — pulls `INFORMATION_SCHEMA.COLUMNS` + `COUNT(*)` only (seconds, no
   row data). Verifies every spec column's `display_name` exists and that the declared
   `type` is compatible with the observed `sql_type` (via
   `reg_monabundle.types.is_compatible`).
2. **Extract phase** — the aggregation queries (potentially hours). Entered only if
   realign finds zero diffs.

If realign finds diffs the bundle writes `project_data.realign.json`, exits non-zero,
and never extracts — making "I forgot to realign" a structural impossibility. Happy case
is **one** MONA round-trip; misalignment costs a second after reconciliation. The first
historical round-trip (discover) goes away: the spec is authored from the order, not
derived from the data.

**Build-time pre-resolution.** Before embedding the JSON, `reg_monabundle.build`
resolves every absent `display_name` from reg_meta (alias resolution) and writes it
back, so the embedded JSON always carries `display_name` on every binding — the bundle
never needs reg_meta on MONA. (Today the runtime instead *requires* hand-written
`display_name` and rejects bindings without it; this pre-resolution is the on-ramp to
making it optional in authored specs.)

`project_data.realign.json` (written only on diffs; its `schema_version` tracks
`project_data.json` — currently 2.0.0; no legacy file shares this name, so there is no
collision):

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

`missing_in_data` lists columns by binding FQID + the `display_name` the bundle queried
for; `extra_in_data` lists SQL columns found but not queried; `type_mismatches` lists
incompatible declared-vs-observed types.

### Type compatibility lives in `reg_monabundle`

The SQL↔spec-type machinery is owned by `reg_monabundle`'s importable, **non-runtime
lightweight side**, which `reg_webapp` imports for the realign-review UI. The future
standalone runner imports no toolkit code, so it will carry its **own copy** of this
logic — the same way `kit.py` hand-copies `COLUMN_TYPES` today to honor the runtime
import boundary. A golden/drift test pins the runner's copy against the canonical
`reg_monabundle.types`, so a newly-learned cast can't land in one without the other —
otherwise the webapp could call a spec/source pair compatible while the uploaded runner
reports a mismatch (or skips under `--force`):

- `is_compatible(spec_type, sql_type) -> bool` — what the extract code can ingest
  (`numeric` ↔ `VARCHAR`/`INTEGER`/`DECIMAL`/`DOUBLE`; `date` ↔
  `VARCHAR`/`DATE`/`DATETIME` but not `INTEGER`). Drives realign mismatch detection.
- `suggest_spec_type(sql_type) -> SpecType` — the inverse, used by the realign-review UI
  to pre-fill "accept SQL type into spec". Always returns *some* type; the user can
  override.

Living in `reg_monabundle` keeps the durable artifacts durable: if the package learns a
new cast, the realign check learns it the same release.

### Reconciling the patch (client-side)

The spec is authoritative. The webapp loads the patch into the in-browser project state
and walks the user through one screen. Four actions:

- **Pair as rename** — link a `missing_in_data` to an `extra_in_data`; update the
  binding's `display_name` to the SQL header. The binding FQID (`variable`) is never
  modified — reg_meta identity is stable.
- **Remove from spec** — drop a truly-absent binding.
- **Add to spec** — a real new delivered column; prompt for a binding FQID (chosen
  against reg_meta via catalog search) and a `type`, store the SQL string as
  `display_name`.
- **Resolve type mismatch** — accept the SQL type into the spec or remove the binding.
  There is no "keep spec type, cast anyway" reconciliation; `--force` is the only way to
  extract past a diff.

The realign-review UI is client-side only — no server endpoint applies the patch. After
reconciliation the in-browser spec updates and the next bundle download embeds the
corrected version.

**`--force` extract semantics.** With realign skipped, extract proceeds
column-by-column: absent `display_name` → warn + skip; present but incompatible → try
the cast, warn + skip on failure. `--force` is the only path that produces a *partial*
stats file; the result should be treated as provisional.

## 10b — Composite `entity_key` / `time_key`

The panel schema already accepts composite `entity_key` (firm × workplace, household ×
person) and composite `time_key` (year × quarter); the runtime rejects them until now.
Additive across three sites:

- **`reg_monabundle.runtime.extract`** — `COUNT(DISTINCT entity_key)` →
  `COUNT(DISTINCT (col_1, col_2, …))`; `GROUP BY <time_key>` → `GROUP BY   (…)`; tuple
  periods in `by_period`.
- **stats schema** — `n_entity_ids` becomes a distinct-*tuple* count; `period` becomes
  `int | string | (int | string)[]`.
- **`reg_mockdata` generate** — the shared id pool is keyed by tuple; the deterministic
  shuffle generates shuffled tuples.

Single-key panels keep working unchanged (the schema polymorphism makes scalar inputs
valid). Composite is additive.

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

- **Kit reproducibility** — same spec + codes + stats → identical kit zip (deterministic
  ordering, no embedded timestamps). Ships with `/api/kit` (#217).
- **Performance gate** — wire the 200-column fixture into a load-test harness measuring
  the p95 budgets (see ARCHITECTURE.md → Repo-wide invariants) and failing CI on
  regression.

## Open / deferred decisions

- **Realign patch lifecycle** — should the realign-review UI write the accepted patch
  back into git (download-and-replace) or just produce a new `project_data.json` the
  user replaces manually? Gated behind 10a.
- **Chronological period `kind` field** — a future `kind` (`year_month`,
  `academic_term`, `quarter`) on the `{"period": …}` object form so the generator can
  impose chronological ordering. Schema is forward-compatible; not designed now.
  (Distinct from #207/#219.)
- **`same_as` at generate time** — should `reg-mockdata generate` normalize spine
  grouping via a kit-shipped `same_as` snapshot? A v1.x question.
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

Open issues seeded from or feeding this plan: #206 (steward admission keying — decided
column-based 2026-06-11 and implemented; the kit-contract facet lands with #217's
implementation), #209 (v1 slug freeze), #217 (kit-build), #240 (MSSQL integration test —
pre-10a gate), #196 + #197 (identity-churning curation — pre-freeze), and #200 + #266
(authoring-UX ride-alongs for the 7.5 dogfood). Deferred beyond v1 but recorded so
pointers resolve: #212 (materializer-owned value tables) and #271 (interval-native
resolver). Resolved since this spec was seeded: #220 + #224 + #278 (the 6.5 deployment
set, closed when 6.5 shipped 2026-06-11), #210 (SOS classification path, closed via PRs
#273/#274), #211 (LOVA/LVM deldatamängd→variant curation, shipped early via PR #359
2026-06-12 instead of batching with step 11; merge-quality follow-up in #362), #208
(closed with the classification-slug surface, not the keyspace question), #227 (wire
`fqid_outside_steward_catalog`), and #228 (reserved suffix slugs).
