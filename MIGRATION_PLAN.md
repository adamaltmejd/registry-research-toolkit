# Model A Migration — Implementation Tracker

Status: **A0 complete; A1-ready** (Model A design locked 2026-05-22; reg_monabundle carve-out shipped through PR #125).

The PR-sized chunking for the Model A architectural rework. Each
sub-step has a checkbox; tick when the PR merges. Use this file as
the canonical live tracker for "where are we" — REFACTOR_SPEC.md §15
captures the same plan at design-doc level but doesn't move per PR.

## How to use

- **Each PR description references its A* identifier** (e.g. `Stage A2.1: variable_state table + coalescing`).
- **When a PR merges, flip the checkbox** in the matching row below. Optionally append the PR number and commit hash.
- **The "Rework map" table at the bottom** lists which v0.x shipped chunks get superseded by which Model A stages — useful when reading old PRs or working out what regressed.
- **Per-session continuity** lives in this file plus PR descriptions for in-flight stages. This `MIGRATION_PLAN.md` is the version-controlled source of truth; cross-PR design state lives here and in REFACTOR_SPEC §15. No external/private notes are load-bearing.

Estimated total effort: ~95–125 person-days across 26 PRs in 5 stages. Realistic calendar time for a single maintainer: 8–12 weeks. Stages can overlap where dependencies allow.

---

## Stage A0 — Finish reg_monabundle carve-out (v0.x grammar)

Continues in-flight work from REFACTOR_SPEC v0.x §15 step 5. No Model A changes. Lands first to avoid interleaving test failures.

### [x] A0.1 — Step 5 phase 2b: `mdw/scan.py` → `reg_monabundle/scan.py` (PR #122, c0fa0cb)

- Move `mdw/scan.py` to `reg_monabundle/scan.py`
- Update `mock_data_wizard`'s imports
- Bundle amalgamator includes `reg_monabundle.scan` ahead of mdw modules
- Tests for PII scanner stay in `reg_monabundle/tests/test_scan.py`

**Estimate**: 1-2 days. Mechanical move.

### [x] A0.2 — Step 5 phase 2c: runtime modules + LoadedSpec (PR #123, 21543f1)

- Move `mdw/{classify,sql_emit,sources,summarize,spec,extract}.py` to `reg_monabundle/runtime/`
- `LoadedSpec` dataclass lands in `reg_monabundle/runtime/spec.py`
- mdw's CLI imports from `reg_monabundle.runtime`
- Bundle amalgamator updates package prefix tuples
- CI gate enforces lightweight/runtime split: importing `reg_monabundle.build` in a duckdb-less env must not transitively pull `reg_monabundle.runtime.*`

**Estimate**: 3-5 days. Larger module move with the LoadedSpec design decision to resolve.

### [x] A0.3 — Step 5 phase 3: 1 MB bundle-size budget gate (PR #124 + #125 follow-ups, cbd4d84 + 9afbc6d)

- 200-column load-test fixture (per §12 — matches the committed bundle-size gate fixture)
- CI test byte-counts the bundle output, fails on > 1 MB
- Surface bundle composition stats in CI logs (per-module byte breakdown) for visibility

**Estimate**: 1-2 days.

**Gate before Stage A1**: A0.3 must merge. The 1 MB budget proves the amalgamation strategy is sound before Model A's complexity lands on top.

---

## Stage A1 — Universal renames + IR scaffolding

Three independent PRs; can land in any order, but all must complete before A2.

### [ ] A1.1 — Universal English column rename

- Rewrite DDL in `reg_meta_build/src/reg_meta_build/db.py`: every Swedish column name renamed per §5.11 vocabulary glossary. Column **values** unchanged (provider-native strings preserved).
- Sweep `reg_meta/`: ~30 query callsites (Catalog methods, search, info). Mostly `SELECT registernamn` → `SELECT name`.
- Sweep test fixtures: ~50 CSV fixtures, ~30 SQLite fixture files
- Update `reg_meta_build/fqid_slugs/scb.toml` documentation if any field references shifted
- `populate_slugs` populates `slug` columns under their new names (no schema diff)
- Tests rewritten in parallel; CI runs `ruff format --check`, `pytest`, type checks

**Estimate**: 5-7 days. Mechanical but pervasive.

### [ ] A1.2 — Lift sensitivity flags from unika_summary

- Add `is_sensitive`, `is_identifier` BOOLEAN columns to `variable` table (`kanslig_variabel` and `kanslig_variabel_ibland` both fold into `is_sensitive` — the 22 sometimes-sensitive rows aren't worth a separate column)
- Build pipeline populates from `unika_summary.{kanslig_variabel, kanslig_variabel_ibland, identitetsvariabel}`
- `unika_summary.{version_forsta, version_sista}` reserved (will become `variable_state.valid_from/valid_to` in A2, mapped to ISO 8601)
- After this lands, the `unika_summary` table itself is unused outside the validity columns; left in place for A2 to consume then drop

**Estimate**: 2-3 days.

### [ ] A1.3 — IR module + adapter scaffolding

- Create `reg_meta_build/ir/__init__.py` exporting the Pydantic IR dataclasses (§4.4)
- Create `reg_meta_build/sources/` directory; add `IRAdapter` Protocol
- Existing SCB ingest in `db.py` does NOT move yet — just compiles alongside the new module
- Provenance DB sibling artifact: empty placeholder, `build_manifest` table only
- `.prev` rotation logic on rebuild (renames before write, no auto-cleanup)

**Estimate**: 3-4 days. Definition-heavy; minimal logic.

---

## Stage A2 — Model A schema (load-bearing gate)

Seven PRs. The largest and most intricate stage. Sequencing matters; gates are explicit.

### [ ] A2.1 — `variable_state` table + coalescing

- Add `variable_state` table to DDL (sibling to `variable_instance`); `valid_from`/`valid_to` are `TEXT NOT NULL` full `YYYY-MM-DD` dates always; open-ended states use the sentinel `valid_to = '9999-12-31'` (never NULL) — see §5.1
- Build pipeline: after `_import_registerinformation` and `_import_vardemangder`, run a coalescer that groups `variable_instance` rows by `(register_id, regvar_id, var_id, data_type, data_length, value_set_id, value_set_version_label, grain)` — the first six are A1.1-renamed DDL columns (was `datatyp` / `datalangd` / `vardemangdsversion`); `grain` is the transient pre-triage carrier for SCB's `vardemangdsniva` (kept in the IR group key so multi-grain rows stay distinct for A2.2 triage, then dropped — the final `variable_state` schema doesn't carry grain). Derives `valid_from`/`valid_to` from the union of `unika_summary.version_forsta..version_sista` for each cvid in the group, expanding to full dates (year `2018` → `2018-01-01..2018-12-31`); writes one `variable_state` row per coalesced group
- **Drop `unika_summary`** after the coalescer has consumed `version_forsta` / `version_sista` (and after A1.2 already lifted the sensitivity flags) — the table is now unused
- Resolver still uses `variable_instance`; no behavior change yet
- Tests: verify coalescing rates match the empirical predictions (5× shrink, 65% single-state)

**Estimate**: 4-6 days. Coalescer is non-trivial.

**Gate to A2.2**: A2.1 must merge. Triage operates on coalesced output.

### [ ] A2.2 — Build-time triage

- Implement `triage_same_year_collisions` pass per §5.7
- Kolumnnamn-primary discriminator (using `variable_alias.kolumnnamn` set intersection)
- Auto-derive sibling slugs (kolumnnamn → niva-pattern → datalangd → BLAKE2b fallback)
- Auto-emit `variable_related_to` edges per `relation_kind`
- Add `variable_related_to` table to DDL
- TOML override mechanism in `scb.toml` for ~200-300 manual cases
- Tests: run against full SCB DB; verify only the curated manual overrides are needed

**Estimate**: 7-10 days. Heuristic refinement + curation backlog.

**Gate to A2.4**: A2.2 must merge. Lineage join (A2.4) operates on triaged variables.

### [ ] A2.3 — Auto-derive `variable_replaced_by` from `timeseries_event`

- Add `variable_replaced_by` table to DDL (per §5.5)
- Build pipeline: after `_import_timeseries`, materialize succession edges from `timeseries_event` rows with `handelse IN ('Ersatt av', 'Ersätter')`
- Inverse-direction collapse (`Ersätter` is the inverse of `Ersatt av`)
- Add `register_replaced_by` and `variant_replaced_by` parallel tables for register/variant-level rows
- TOML curation slot in `scb.toml` and `sos.toml` for cross-provider edges (empty for now; populated in A4 if needed)

**Estimate**: 3-4 days.

Can run in parallel with A2.2.

### [ ] A2.4 — `variable_state_lineage` interval-overlap join

- Add `variable_state_lineage` and `variable_state_lineage_warning` tables to DDL
- Source-variant pinning is **TOML-only**, not a SQL table:
  - Heuristic defaults in `lineage_defaults` TOML block (per source register)
  - Per-variable overrides in `lineage."<consumer_register>.<variable_slug>"` TOML blocks
- Implement new `link_variable_state_lineage` per §5.6 pseudocode
- Drop old `link_consumer_side_bindings` (its inputs go away with `variable_instance`)
- Tests: 5 worked LISA-RTB examples from the agent report verify the algorithm

**Estimate**: 4-5 days.

**Gate to A2.6**: A2.4 *and* A2.5 must merge. A2.6 flips the FQID grammar, which requires both the new lineage tables (A2.4) and the new catalog API (A2.5) in place.

### [ ] A2.5 — Catalog API shift

- `Catalog.resolve(fqid)` **flips semantics in place** — now returns longitudinal `ResolvedVariable` (per §5.10). The v0.x per-cvid behavior is **deleted**, not aliased — pre-v1 policy allows the break.
- Implement `Catalog.resolve_at(fqid, period, *, value_set_version=None)` returning single `VariableState` (`period` polymorphic per §6.2; not year-only)
- Implement `Catalog.states(fqid)`, `.predecessors(fqid)`, `.successors(fqid)`, `.related(fqid)`, `.lineage(fqid)`, `.lineage_warnings(fqid)` — all list-returning per §5.10
- Post-A2.5 public method roster: `resolve` (new semantics), `resolve_at`, `states`, `predecessors`, `successors`, `related`, `lineage`, `lineage_warnings`
- Tests: round-trip a binding's full state history via `resolve(fqid).states` and via `[resolve_at(fqid, period=y) for y in years]`; they must agree on the unambiguous case

**Estimate**: 5-6 days.

Can run in parallel with A2.3/A2.4 once A2.1 lands.

### [ ] A2.6 — Drop period from FQID grammar

- Update FQID parser, emitter: 4-seg bindings, 3-seg variants, 2-seg classifications (`class/<slug>` only, version in slug)
- Drop ~1,264 `register_version` slug entries from `scb.toml`
- Add `_default` slug to relevant variants (LSS, BU, SOL — synthesized to real rows in A4.3, but the slug exists from now)
- Drop the `register_version` table entirely. Per-edition prose (mätinformation, descriptions) goes to reg-meta-docs at variant level with chronological Markdown sections (build pipeline writes these). Per-edition build artifacts (approval dates, etc.) go to provenance DB, joined to `variable_state` by `state_id` in the sibling provenance artifact — no SCB-specific column on `variable_state` (universal-schema rule, §5.1).
- Update Webapp catalog endpoints: 4-seg binding leaves, new sub-endpoints (§9.5)
- Old API endpoints (v0.x `register_version` leaves) removed; clients break per pre-v1 policy
- UNFROZEN sentinel is active; the slug TOML rewrite is a regular commit

**Estimate**: 7-10 days. Touches grammar, table rename, API, slug TOML.

**Gate to A2.7**: A2.6 must merge.

### [ ] A2.7 — Cleanup

- Drop `variable_instance` table from DDL (kept alive A2.5–A2.6 only for build-pipeline dual-write)
- Drop `via_source_id` column (no remaining consumer once `variable_instance` is gone)
- Bump `reg_meta` to v1.0.0; tag the release
- UNFROZEN snapshot is regenerated (still UNFROZEN — curation polish continues; UNFROZEN deletes at v1 publication, not at v1.0.0 internal tag)

**Estimate**: 2-3 days.

---

## Stage A3 — Consumer migration

Four PRs. Starts after A2 completes. Internal ordering: A3.1 first; A3.2/A3.3/A3.4 in parallel.

### [ ] A3.1 — `reg_schema` v2.0.0

- Migrate dataclasses to Pydantic v2 models
- `Source.register_version` → `Source.register_variant` + `Source.period` (always required; polymorphic int/string/range/snapshot-sentinel)
- `Source.columns` → `Source.bindings`
- Binding `name` → `variable` (4-seg)
- Panel `entity_key` / `time_key` inheritance from `variant.panel_template` when omitted
- TimePoint gains range form `{"range": {"from", "to"}}`
- New issue codes: `invalid_period`, `period_outside_state_validity`, `binding_state_drifts_within_period`, `binding_state_ambiguous`, `variable_replaced`, `panel_inheritance_unresolvable` (the last is semantic-layer; raised by kit/bundle-build when a member's variant has no `panel_template` and no explicit keys — §6.4 + §6.8.3)
- Rename: `fqid_register_version_mismatch` → `fqid_register_variant_mismatch`
- Rewrite test corpus (`minimal`, `with_panel`, `composite_entity_key`, `with_namespaced_block`, `invalid_root_array`)
- Bump pinned `reg_meta_version` in steward catalogs to `reg_meta/v1.0.0`
- JSON schema codegen produces SPA TypeScript types

**Estimate**: 5-7 days (slightly larger than initial 4-6 due to broader rename surface).

### [ ] A3.2 — `mock_data_wizard/spec.py` adoption

- `_build_source` reads new `register_variant` + `period` fields; `columns` → `bindings`; `Column.name` → `Binding.variable`
- `lookup_options` keys remain FQID-based (4-seg now)
- Fixture sweep for all `project_data.json` files under `mock_data_wizard/`
- Companion `project_data.codes.json` fixtures restructured to the new shape (`classifications` + `sources.<name>.<binding_fqid>` blocks; §6.6) — every test that asserts against the old flat FQID-keyed codes file follows
- Tests follow

**Estimate**: 3-4 days.

### [ ] A3.3 — `reg_monabundle/validate.py` 4-seg update

- One-line edit in `_is_binding_fqid` (5 → 4 segments)
- Error message text update
- Tests follow

**Estimate**: 1 day.

### [ ] A3.4 — Bundle amalgamator update

- Add `reg_monabundle/build/spec_loader.py` with `source_to_loadedspec(pydantic_source) -> LoadedSpec` — lives in `build/`, not `runtime/`, so the bundle never imports Pydantic (the §9.6 boundary). Called by the bundle builder before embedding JSON.
- `reg_monabundle/runtime/spec.py` `LoadedSpec` fields updated to Model A shape: `register_variant` (3-seg FQID), `period` (polymorphic), `bindings` with `variable` (4-seg). PR #123 shipped LoadedSpec under v0.x grammar; this is the breaking shape evolution.
- Amalgamator's `_AMALGAMATED_PACKAGE_PREFIXES` excludes `reg_schema` (Pydantic stays out of bundle)
- Bundle's `LoadedSpec` parsing reads `register_variant` + `period` from embedded JSON
- `LoadedSpec` deserialization is plain `@dataclass` machinery — no re-validation on MONA (§6.8.1, §9.6)

**Estimate**: 3-4 days.

---

## Stage A4 — Adapter refactor + SOS

Five PRs. Can run in parallel with A3 after A2.6 lands.

### [ ] A4.1 — SCB adapter refactor

- Move SCB ingest from `db.py`'s `_import_*` functions to `reg_meta_build/sources/scb.py` `SCBAdapter`
- Adapter emits IR; materializer in `db.py` becomes provider-blind
- Test SCB rebuild produces byte-identical universal DB output
- Provenance DB populated with SCB-specific debug data (raw CSV checksums, import warnings)

**Estimate**: 5-7 days. Refactor only; no new functionality.

### [ ] A4.2 — Deterministic IDs + provenance DB

- SCB universal IDs verified to match source IDs verbatim (already the case from coalescer; this PR enforces and documents)
- SOS ID mint scheme implemented (BLAKE2b, top-bit-namespaced)
- Provenance DB populated by adapters
- `.prev` rotation verified in CI

**Estimate**: 2-3 days.

### [ ] A4.3 — SOS adapter

- `reg_meta_build/sources/sos.py` `SOSAdapter` implementing `IRAdapter`
- Consumes the 13 SOS workbooks via existing parser at `reg_meta_build/src/reg_meta_build/sources/sos.py`
- Variant synthesis for LSS/BU/SOL (`_default` real variant row)
- Kodlista state-era parsing per §5.7
- MFR IVF_klinik entity-registry heuristic (collapse to 1 state with per-code validity, not 15 states)
- Outputs ~2,300 IR rows

**Estimate**: 7-10 days. Most complex SOS-specific logic.

### [ ] A4.4 — Slug TOML + panel_template curation (SCB + SOS)

- Create `reg_meta_build/fqid_slugs/sos.toml`
- Register slugs: 3-letter SOS abbreviations (`par`, `mfr`, `dors`, etc.)
- Variant slugs from deldatamangd names (lowercase, kebab-case)
- Variable slugs auto-derived from SOS variable names; TOML overrides where needed
- **Per-variant `panel_entity_key` / `panel_time_key` / `panel_time_grain` curation** for both SCB (~150 variants × 5 average = ~750 variant rows) and SOS (~50 variants). `seed-slugs --propose-panel` auto-suggests from:
  - SCB: `Tabelldefinitioner.sql` PK declarations + `Identifierare.csv` entity-type signals
  - SOS: workbook `is_join_variable` annotations + variable definitions
- Curator hand-reviews suggestions; sets explicit values where the auto-suggester is wrong or missing data
- TOML grow-only rules apply (UNFROZEN sentinel active during this work)

**Estimate**: 4-5 days (initial estimate of 2-3 days didn't account for the panel_template curation pass — ~800 variant rows to review).

### [ ] A4.5 — First combined SCB+SOS build

- CI pipeline produces `reg_meta.db` containing both providers
- Verify cross-provider FTS doesn't bleed
- Verify no ID collisions (BLAKE2b top-bit namespace held)
- Verify no spurious cross-provider same_as edges
- Document the resulting DB shape (sample queries, row counts)

**Estimate**: 2-3 days.

---

## Stage A5 — Webapp + SPA

Four PRs. Lands after A2 + A3. A4 not required (SCB-only deployment can ship first).

### [ ] A5.1 — `reg_webapp` Pydantic models

- FastAPI endpoints use `reg_schema` Pydantic models directly for project_data responses
- `reg_meta` library types still wrapped 1:1 for catalog responses (the only remaining wrapper layer)
- OpenAPI codegen updated

**Estimate**: 3-4 days.

### [ ] A5.2 — New API endpoints

- Implement `?period=...` query on canonical catalog endpoint (polymorphic per §9.5 — accepts int year, period-token, range object, or `_default` snapshot sentinel; not year-only)
- Implement `/states`, `/predecessors`, `/successors`, `/related`, `/lineage` sub-endpoints (`/lineage` is a first-class v1 endpoint per §9.5, not deferred)
- Suffixed routes registered BEFORE the `/api/catalog/{fqid:path}` catch-all in the FastAPI router (router ordering matters; see §9.5 URL routing notes); CI introspection test enforces the order
- ETag scheme verified to include the `?period` query in the cache key
- Cloudflare edge-cache validation gate: small load test through Cloudflare confirms slash-bearing FQID paths still work cleanly with the new shapes

**Estimate**: 4-5 days.

### [ ] A5.3 — SPA TypeScript regen

- OpenAPI codegen against new Pydantic models
- SPA components updated for 4-seg FQIDs
- New sub-endpoint integrations (states picker, replaced-by remediation, related-to siblings picker)
- 409 (`variable_state_ambiguous`) handling

**Estimate**: 5-7 days.

### [ ] A5.4 — SPA IndexedDB hard-reject for v0.x project files

- On Open-from-file: check `schema_version` and `reg_meta_version`; reject v0.x with blocking error
- IndexedDB schema version stored alongside each project; reject mismatched on load
- Clear migration message: "this project predates Model A. Re-author or load a v1.0+ file."

**Estimate**: 2 days.

---

## Gates summary

```text
A0.3 ──→ {A1.1, A1.2, A1.3} ──→ A2.1 ──┬──→ A2.2 ──→ A2.4 ──┐
                                       ├──→ A2.3            │
                                       └──→ A2.5 ───────────┴──→ A2.6 ──→ A2.7
                                                                            │
                                                                            ├──→ A3.1 ──→ {A3.2, A3.3, A3.4}
                                                                            │
                                                                            ├──→ A4.1 ──→ A4.2 ──→ A4.3 ──→ A4.4 ──→ A4.5
                                                                            │
                                                                            └──→ A5.1 ──→ {A5.2, A5.3, A5.4}
                                                                                  (after A3.1 lands; A4 not required)
```

Reading notes: braces `{...}` group steps that can run in parallel
once their shared predecessor lands. After A2.1, three branches open
in parallel: A2.2→A2.4 (coalesced states → triage → lineage join),
A2.3 (independent — reads `timeseries_event`, no dependency on
coalescer/triage), and A2.5 (catalog API on the new tables — needs
only A2.1's `variable_state` to exist). A2.6 needs **both** A2.4
(new lineage tables) and A2.5 (new catalog API); A2.3 is not on
A2.6's critical path — it produces `variable_replaced_by` for
consumer use, not for A2.4.

## Effort estimate

| Stage | PRs | Cumulative effort (single maintainer) |
|---|---|---|
| A0 | 3 | ~8 days |
| A1 | 3 | ~10-14 days (parallelizable) |
| A2 | 7 | ~32-44 days |
| A3 | 4 | ~11-15 days (parallelizable after A3.1) |
| A4 | 5 | ~19-26 days |
| A5 | 4 | ~14-18 days |
| **Total** | **26 PRs (incl. parallelism)** | **~95-125 days** |

With parallelism across stages where dependencies allow, calendar time is closer to **8-12 weeks** for a single maintainer focused on this work.

## Risk register

1. **A2.2 build-time triage backlog larger than estimated.** If 200-300 manual TOML curations turns out to be 600-900, A2.2 lengthens. Mitigation: empirical sample from current SCB DB shows 99% auto-handle rate; risk is bounded.
2. **A2.4 source-variant heuristic doesn't fit some real consumer-source pairs.** Mitigation: warning + TOML override mechanism captures the cases the heuristic misses; ~50 manual overrides expected.
3. **A2.6 FQID grammar change breaks downstream tools we haven't catalogued.** Mitigation: search for `register_version` and 5-seg FQID patterns across the monorepo before A2.6. The grep needs to be slug-grammar-aware to avoid swamping the signal with paths/URLs/JSON pointers:

    ```bash
    # 5-seg slugs per §5.2 (allow _default and v0.11 _YYYY period slugs)
    rg "[a-z][a-z0-9-]*(/(_default|_[0-9]+|[a-z][a-z0-9-]*)){4}" --type py --type md --type toml

    # Or scope to known FQID call sites:
    rg "Catalog\.(resolve|resolve_at|states|predecessors|successors|related|replaced|lineage|lineage_warnings)\("
    ```

    Combine both passes — the first finds string-literal FQIDs in fixtures/configs; the second finds programmatic callsites that may construct FQIDs at runtime.
4. **A4.3 SOS adapter discovers workbook-shape variations we haven't covered.** Mitigation: process all 13 workbooks against the adapter before merging; failures surface as `IRWarning` rows. New workbook variations would extend the adapter, not gate it.
5. **A5 webapp work lags A3 by several weeks.** Mitigation: webapp can adopt incrementally; some endpoints can land before others. The hard reject on v0.x SPA files is the only end-of-stage gate.

## What lands as soon as A5.4 ships

- v1.0 reg_meta with SCB + SOS (if A4 ran in parallel) or SCB-only (if A4 deferred)
- v1.0 reg_schema (Pydantic, Model A Source shape)
- v1.0 reg_monabundle (4-seg FQID, LoadedSpec mediation)
- v1.0 reg_webapp (Pydantic, new API surface)
- v1.0 SPA (no v0.x file support; Model A only)
- v1.0 `global` deployment ready to host

UNFROZEN sentinel deletion happens at v1 *public release* (not at v1.0.0 internal tag) — give the v1.0.0 build a curation polish window first.

---

## v0.x → Model A rework map

| v0.x PRs | Shipped under v0.x | What survives Model A | What gets redone | Model A stage |
|---|---|---|---|---|
| #78–#82, #85–#87, #89, #104, #112 | reg_meta v0.11 FQID rebuild — 5-seg binding FQID, same_as edges, slug TOMLs (~1,264 entries), §5.8 cross-edition traversal | Provider / register / variant / variable / classification slug curation; same_as table shape; consumer-side binding concept; FTS layer | 5-seg FQID grammar → 4-seg (period slot dropped); ~1,264 `register_version` slug entries deleted; `same_as.*_period` columns dropped; `register_version` table dropped entirely | A2.6, A2.7 |
| #103, #105, #108 | `reg_meta_build` package carve-out (mechanical split: scaffold, db.py, doc_db.py, CLI split, CI workflow) | Package boundary intact; CLI binaries (`reg-meta`, `reg-meta-build`); test helpers | Column names → English (data values unchanged); SCB-Swedish column renames | A1.1 (rename only) |
| #110, #111, #115 | `reg_schema` v0.x — `ValidationIssue` / `ValidationResult` contract; frozen dataclasses (ProjectData, Source, Column, …); `validate_structural` entrypoint | `ValidationResult` JSON contract concept; 22+ stable issue codes (most unaffected); validate_structural API surface | Dataclasses → Pydantic v2; `Source.register_version` → `register_variant + period`; `columns` → `bindings`; `Column.name` → `variable`; 1 issue code renamed + 5 new codes; bump to `schema_version: "2.0.0"` | A3.1 |
| #116 | `mock_data_wizard` adopts `project_data.json` (config rename, fixture corpus rewrite for v0.11 5-seg shape) | Config-rename machinery; fixture-rewrite tooling; v0.x test fixtures' overall structure | Source schema break propagated through mdw; fixture corpus rewritten again to Model A shape; `_build_source` reads new fields | A3.2 |
| #113 | Shared validator test corpus — `reg_schema/test_corpus/` with 4 well-formed + 1 negative cases; harness with drift protection | Corpus discovery + harness machinery | All 5 cases rewritten: source becomes 3-seg FQID + `period`; bindings replace columns; namespaced-block keys 4-seg | A3.1 (paired with reg_schema migration) |
| #120, #121, #122, #123, #124, #125 | `reg_monabundle` carve-out — phases 1, 2a, 2b, 2c, 3 + follow-ups (scaffold, validator relocation, bundle builder, PII scanner, runtime modules + LoadedSpec, 1 MB bundle-size gate) | Survives entirely; this is the Stage A0 work | (none) — A0 complete | A0 ✅ |

Conventions: stage IDs reference the sections above. PR links (`[#NNN]`) resolve against the project's GitHub repo.
