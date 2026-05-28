# Model A Migration — Implementation Tracker

Status: **A0 complete; A1-ready** (Model A design locked 2026-05-22; reg_monabundle carve-out shipped through PR #125).

**Scoped, self-deleting tracker** — this file exists under the
governance exception in `AGENTS.md` for multi-PR refactors spanning
weeks. It is **not a permanent implementation tracker**: it gets
deleted when stage A5.4 ships (Model A migration complete; v1.0
tagged). After deletion, history of which step landed when lives in
git like everything else; the design-level narrative survives in
`REFACTOR_SPEC.md` §15 as a record of the sequencing.

The PR-sized chunking for the Model A architectural rework. Each
sub-step has a checkbox; tick when the PR merges. Use this file as
the live cross-PR coordination doc for the duration of the refactor —
REFACTOR_SPEC.md §15 captures the same plan at design-doc level but
doesn't move per PR.

## How to use

- **Each PR description references its A* identifier** (e.g. `Stage A2.1: variable_state table + coalescing`).
- **When a PR merges, flip the checkbox** in the matching row below. Optionally append the PR number and commit hash.
- **The "Rework map" table at the bottom** lists which v0.x shipped chunks get superseded by which Model A stages — useful when reading old PRs or working out what regressed.
- **Per-session continuity** lives in this file plus PR descriptions for in-flight stages. This `MIGRATION_PLAN.md` is the version-controlled source of truth; cross-PR design state lives here and in REFACTOR_SPEC §15. No external/private notes are load-bearing.

Estimated total effort: ~98–136 person-days across 27 PRs in 5 stages (the respec three-level restructure added stage A2.1.5). Realistic calendar time for a single maintainer: 8–12 weeks. Stages can overlap where dependencies allow.

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

### [x] A1.1 — Universal English column rename (PR #129, 12955f9)

- DDL in `reg_meta_build/src/reg_meta_build/db.py` rewritten: ~21 column renames + 7 drops per §5.11 vocabulary glossary. Column **values** unchanged (provider-native strings preserved).
- Swept ~30 query callsites in `reg_meta/src/reg_meta/{queries,catalog,cli,fqid}.py`. `SELECT registernamn` → `SELECT r.name`, etc. Public dict-key contract preserved where intentional (`r.name AS register_name` in JOINs).
- Cross-package sweep also covered `mock_data_wizard/`, `reg_monabundle/runtime/classify.py` (MONA-bundled runtime — renamed internal Python identifiers `_reg_meta_datatyp_kind` → `_reg_meta_data_type_kind`, `RegMetaSignal.datatyp_kind` → `data_type_kind`), and `scripts/parse_lisa_docs.py`.
- Operational-definition fold: `VariabelOperationell_definition` is no longer a dedicated column; folded into `variable.description` with `\n\n` separator when distinct + non-empty. Substring guard (`op not in desc`) ensures rebuild idempotency.
- `SCHEMA_VERSION` bumped 3.3.0 → **4.0.0** (major break; pre-A1.1 DBs rejected up-front).
- `_cmd_get_values` SQL discriminator updated to query renamed `value_set_version_label` column.
- New `TestOperationalDefinitionFold` test class — 5 tests locking the fold contract.
- Test sweep: ~128 assertion updates across 8 test files. SCB CSV headers (wire format) stay Swedish.

### [x] A1.2 — Lift sensitivity flags from unika_summary (PR #128, e2d1abb)

- Added `is_sensitive`, `is_identifier` BOOLEAN columns (DEFAULT 0) to `variable` table. `kanslig_variabel` and `kanslig_variabel_ibland` both fold into `is_sensitive` (the 22 sometimes-sensitive rows aren't worth a separate column).
- `_populate_sensitivity_flags(conn)` runs after `_import_registerinformation` + `_import_unika`. Joins through `variable_instance × variable_alias × variable` and disambiguates by the full `unika_summary` PK `(register_id, regvar_id, kolumnnamn, variabelnamn)` — without the `variabelnamn` join, the same `kolumnnamn` reused across distinct variables under one variant would fan flags onto wrong siblings.
- `unika_summary.{version_forsta, version_sista}` reserved (will become `variable_state.valid_from/valid_to` in A2, mapped to ISO 8601). `unika_summary` table itself stays in place for A2.1 to consume then drop.
- `SCHEMA_VERSION` rides on A1.1's 4.0.0 major bump — no further bump (additive columns).

### [x] A1.3 — IR module + adapter scaffolding (PR #127, 8c92942)

- New `reg_meta_build/ir/__init__.py` with 13 Pydantic v2 `BaseModel` classes per §4.4 (IRRegister, IRVariant, IRVariable, IRVariableState, IRValueCode, IRValueSet, IRClassification, IRLineageEdge, IRReplacedByEdge, IRRelatedToEdge, IRWarning, IRDeliveryProvenance). All inherit from `_IRBase` with `model_config = ConfigDict(extra="forbid")` so adapter typos fail loudly instead of dropping into defaults.
- `IRAdapter` Protocol + `IRObject` union added to `reg_meta_build/sources/__init__.py` (existing `sos.py` parser untouched).
- `IRValueSet.member_hash` typed as `bytes` (raw 32-byte SHA-256 digest) to match the universal `value_set.member_hash` BLOB column verbatim — no hex encode/decode at the IR↔materializer boundary.
- Provenance DB scaffolding: `PROVENANCE_DDL` for `build_manifest` table, `create_empty_provenance_db(path)` helper, `rotate_db_to_prev(db_path)` single-generation `.prev` rotation. `build_db` wires both — universal DB rotates + atomic-replaces; provenance DB scaffolding wrapped in `try/except` so a post-swap IOError logs a warning instead of flipping the build to "failed". `build_manifest` stays empty until A4.x populates it.
- `pydantic>=2.13.4` added to `reg_meta_build` runtime deps (carve-out matches `reg_schema`'s; the IR is build-time-only, never imported by `reg_meta` runtime / MONA bundle).
- 46 contract tests in `test_ir_scaffolding.py`: import surface, BaseModel conformance, round-trip per IR class, `extra="forbid"` rejection per IR class, `IRAdapter` Protocol shape, `IRObject` union drift guard, rotation semantics, provenance DB schema.

Existing SCB ingest in `db.py` did NOT move — A4.x rewrites the SCB adapter onto the IR contract.

---

## Stage A2 — Model A schema (load-bearing gate)

Eight PRs. The largest and most intricate stage. Sequencing matters; gates are explicit.

**Three-level restructure (respec).** The object model splits today's
conflated `variable` into `variable_concept` (register-scoped, "define
once") → `variable` (variant-scoped, **1:1 with the binding FQID**,
holds the variable slug) → `variable_state` (eras), and demotes
`same_as` to concept grain (deleting the var_id `(N choose 2)`
auto-derive). See REFACTOR_SPEC §5.1, §5.5. This re-derives three A2
steps:

- **A2.1.5 (new)** introduces the three tables + stored variable slug
  + resolver flip — landing *before* triage/lineage because both now
  operate on the three-level shape. It supersedes the in-flight PR #133
  (which stored the slug on `variable_state` with an "all eras agree"
  assertion — the wrong place; the slug belongs on the single
  `variable` row).
- **A2.2** reworks so triage siblings are variant-scoped `variable`
  rows under a *shared* concept (DECISION POINT 1 in the respec PR),
  with the variable slug as their identity.
- **A2.6** keeps the FQID-grammar flip but drops the three-level table
  work (now in A2.1.5); it gains the `same_as` concept-demotion + the
  var_id auto-derive deletion + the `variable_same_as` →
  `variable_concept_same_as` rename.

**Sequencing recommendation (respec).** Land the slug-storage +
resolver-flip as the distinct earlier stage **A2.1.5**, not folded into
A2.6. Rationale: triage (A2.2) needs sibling `variable` rows to exist;
lineage (A2.4) now traverses concept-grain `same_as` and the
concept→variable hierarchy; the resolver flip (A2.5) reads the stored
variable slug. All three depend on the three-level tables, so the
tables must precede them. Folding them into A2.6 would force A2.2/A2.4/
A2.5 to dual-target two schemas. A2.6 then stays focused on the grammar
flip it was always about.

### [x] A2.1 — `variable_state` table + coalescing (PR #130)

- `variable_state` table added to DDL (sibling to `variable_instance`); `valid_from`/`valid_to` are `TEXT NOT NULL` full `YYYY-MM-DD`, enforced by CHECK constraints (`length() = 10`, `valid_to >= valid_from`). Open-ended states get the sentinel `valid_to = '9999-12-31'`; the unknown-lower-bound fallback uses `'0001-01-01'` for the rare yearless cvid case
- `_coalesce_variable_states` (`reg_meta_build/src/reg_meta_build/db.py`) groups `variable_instance` rows by the 8-tuple `(register_id, regvar_id, var_id, data_type, data_length, value_set_id, value_set_version_label, grain)`. `grain` lives only in the in-memory group key — never lands on `variable_state`, per the universal-schema contract. Reads `unika_summary` for VersionForsta/VersionSista (primary), falls back to `register_version.registerversionnamn` year extraction when unika has no matching `(register, regvar, kolumnnamn, variabelnamn)` row. `delivery_column_name` denormalizes the latest alias for the era (highest `regver_id`, lexically smallest on ties)
- **`unika_summary` dropped** at end of build via `DROP TABLE` + `VACUUM`. Both consumers (`_populate_sensitivity_flags`, `_coalesce_variable_states`) have run; the table is now unused in the shipped DB
- Resolver still uses `variable_instance`; no query-layer behavior change (that's A2.5)
- `SCHEMA_VERSION` bumped `4.0.0` → `4.1.0` (additive new table, drop of a build-only table — minor bump per the §5.11 compat rule)
- Tests: 10 new tests under `TestBuildDb` cover row presence per `(register, var)` triple, full-ISO column shape, year expansion (2022 → 2022-01-01..2022-12-31), unika min/max range, delivery_column_name tie-break, `register_version` fallback path, value_set_version_label preservation, grain split, FK to `variable`, and manifest `coalesce_stats` consistency. Empirical coalescing-rate validation (5× shrink, 65% single-state) lands when running against the full SCB corpus

**Gate to A2.1.5**: ✅ Met. The coalesced `variable_state` rows are the input the three-level restructure re-parents.

### [ ] A2.1.5 — Three-level variable tables + stored variable slug + resolver flip (respec; supersedes PR #133)

The three-level restructure's table + slug + resolver work, landed
early so triage (A2.2), lineage (A2.4), and the catalog API (A2.5)
operate on the new shape. Per REFACTOR_SPEC §5.1, §5.3, §5.10.

- **Rename `variable` → `variable_concept`** (register-scoped, PK `(register_id, var_id)`). The A1.2 sensitivity flags (`is_sensitive`, `is_identifier`) and the shared-metadata columns (`name`, `definition`, `description`, `measurement_unit`, `source_register_id`, `source_register_text`) ride along unchanged — same grain, pure rename. Add the `concept_slug` column (register-unique; §5.3).
- **Add the variant-scoped `variable` table** (synthetic `variable_id` PK, `variant_id`, `slug`, FK `(register_id, var_id)` → `variable_concept`). One row per variant deliverable, **1:1 with the binding FQID**. The `slug` is the variable slug (FQID leaf); in the common case it equals the concept's `concept_slug`. **DECISION POINT 2 (respec): synthetic `variable_id`** over a composite key — single-column FK for `variable_state`, lets triage add sibling rows without minting a new var_id.
- **Re-parent `variable_state`** from `(register_id, var_id)` to FK `variable_id` → `variable`. **Remove the `slug` column** that PR #133 added to `variable_state` (it moves to the single parent `variable` row; the "all eras agree" denormalization assertion is deleted).
- **Stored slug population** (`populate_concept_slugs` + variable-slug copy): concept slug auto-derives from the latest kolumnnamn into `<provider>.auto.toml` (register-unique; collision → `slug_concept_collision`, DECISION POINT 4); each variant's `variable.slug` defaults to the concept slug. Lift the `slug_variable_override_unsupported` gate for the `[variable_concept]` (concept-slug) and `[variable."<reg>.<regvar>.<var>"]` (sibling-slug) paths. `[variable.<...>]` TOML key in `scb.toml` is renamed to `[variable_concept.<...>]`.
- **Resolver flip** (`reg_meta/catalog.py`): `_resolve_binding_direct` reads the stored `variable.slug` via an exact match (no `derive_variable_slug`-at-resolve, no fold ambiguity) and joins `variable_state` through `variable_id`; `ResolvedVariable` inlines the concept's shared metadata + sibling variants. `_variable_source_slug` (`fqid_slugs.py`) reads the stored concept slug instead of deriving-and-requiring-unique.
- `SCHEMA_VERSION` bumped on the 4.x line (additive tables + a column move; rebases above whichever of #131/#132/#133's bumps land first).
- Tests: stored-slug round-trip via `Catalog.resolve`; concept↔variable FK integrity; `variable_state` re-parent; concept-slug register-uniqueness + collision failure; sibling-slug override; `[variable_concept]` TOML parse; resolver reads stored slug (no derive).

**Estimate**: 5-7 days. Table restructure + slug population + resolver flip (absorbs most of PR #133's mechanism, corrected onto the `variable` table).

**Gate to A2.2 / A2.4 / A2.5**: A2.1.5 must merge. Triage mints sibling `variable` rows; lineage traverses the concept hierarchy; the resolver reads the stored variable slug — all need the three tables in place.

### [ ] A2.2 — Build-time triage (reworked onto three-level model)

- Implement `triage_same_year_collisions` pass per §5.7
- Kolumnnamn-primary discriminator (using `variable_alias.kolumnnamn` set intersection)
- **Siblings are variant-scoped `variable` rows under a *shared* `variable_concept`** (DECISION POINT 1 in the respec PR) — a split adds N `variable` rows (each a synthetic `variable_id`, all FK-pointing at the one concept's `(register_id, var_id)`), **not** N new concepts and **not** N minted var_ids. Each sibling's `variable.slug` is the variable slug.
- Auto-derive sibling **variable** slugs from the shared concept slug + suffix (kolumnnamn → niva-pattern → datalangd → BLAKE2b fallback)
- Auto-emit `variable_related_to` edges (variable grain) among siblings per `relation_kind`
- Add `variable_related_to` table to DDL (variable-grain endpoints, 4-part slug tuples)
- TOML override mechanism in `scb.toml` for ~200-300 manual cases — sibling-slug overrides use the `[variable."<reg>.<regvar>.<var>"]` key (§5.3)
- Tests: run against full SCB DB; verify siblings share a concept, only the curated manual overrides are needed

**Estimate**: 7-10 days. Heuristic refinement + curation backlog.

**Gate to A2.4**: A2.2 must merge. Lineage join (A2.4) operates on triaged variables.

### [ ] A2.3 — Auto-derive `variable_replaced_by` from `timeseries_event`

- Add `variable_replaced_by` table to DDL (per §5.5)
- Build pipeline: after `_import_timeseries`, materialize succession edges from `timeseries_event` rows with `handelse IN ('Ersatt av', 'Ersätter')`
- Inverse-direction collapse (`Ersätter` is the inverse of `Ersatt av`)
- Add `register_replaced_by` and `variant_replaced_by` parallel tables for register/variant-level rows
- TOML curation slot in `scb.toml` and `sos.toml` for cross-provider edges (empty for now; populated in A4 if needed)

**Estimate**: 3-4 days.

Can run in parallel with A2.2. **Respec note (survives, in-flight PR #131):** `variable_replaced_by` stays **variable grain** — endpoints are 4-part `(provider, register, variant, variable_slug)` tuples (§5.5), so PR #131's design carries forward. The only wording tweak is that the variable slot now resolves to exactly one stored `variable` row (A2.1.5's stored slug), not a derive-at-resolve fold. If A2.3 lands before A2.1.5, it rebases onto the three-level tables (the edge tables themselves don't change).

### [ ] A2.4 — `variable_state_lineage` interval-overlap join

- Add `variable_state_lineage` and `variable_state_lineage_warning` tables to DDL
- Source-variant pinning is **TOML-only**, not a SQL table:
  - Heuristic defaults in `lineage_defaults` TOML block (per source register)
  - Per-variable overrides in `lineage."<consumer_register>.<variable_slug>"` TOML blocks
- **Respec: source link + matching are concept-grain.** Read the source register from `variable_concept.source_register_id` (shared metadata, A2.1.5). Source-side matching traverses **concept-level `variable_concept_same_as`** (A2.6's renamed table) and the concept→variable hierarchy — `concept_set_via_same_as` replaces the per-variant `slug_set_via_same_as` fold (§5.6). One concept edge covers every variant the source delivers.
- Implement new `link_variable_state_lineage` per §5.6 pseudocode
- Drop old `link_consumer_side_bindings` (its inputs go away with `variable_instance`)
- Tests: 5 worked LISA-RTB examples from the agent report verify the algorithm

**Estimate**: 4-5 days.

**Gate to A2.6**: A2.4 *and* A2.5 must merge. A2.6 flips the FQID grammar, which requires both the new lineage tables (A2.4) and the new catalog API (A2.5) in place. (The `concept_set_via_same_as` traversal in A2.4 reads `variable_concept_same_as`; if A2.4 lands before A2.6's rename, it reads the pre-rename table and rebases the table name in A2.6.)

### [ ] A2.5 — Catalog API shift

- `Catalog.resolve(fqid)` **flips semantics in place** — now returns longitudinal `ResolvedVariable` (per §5.10). The v0.x per-cvid behavior is **deleted**, not aliased — pre-v1 policy allows the break. **Respec:** A2.1.5 already flipped the *binding-resolution read* (stored variable slug, exact match, `variable_state` joined via `variable_id`, concept metadata inlined); A2.5 builds the *longitudinal aggregate* (`ResolvedVariable` with full state list + sibling variants + all edges) on top of that read path.
- Implement `Catalog.resolve_at(fqid, period, *, value_set_version=None) -> list[VariableState]` (`period` polymorphic per §6.2; not year-only). Always returns a list: length 1 for unambiguous point queries, length N for range periods crossing state transitions and the rare LKF-shape multi-vintage case. Empty list when no state covers the period (no exception). `value_set_version` narrows multi-vintage results to a single state.
- Implement `Catalog.states(fqid)`, `.predecessors(fqid)`, `.successors(fqid)`, `.related(fqid)`, `.lineage(fqid)`, `.lineage_warnings(fqid)` — all list-returning per §5.10. `same_as` on `ResolvedVariable` returns concept-grain `ConceptRef`s (3-part, no FQID); the others return variable-grain `VariableRef` / `RelatedRef` (4-part binding FQIDs).
- Post-A2.5 public method roster: `resolve` (new semantics), `resolve_at`, `states`, `predecessors`, `successors`, `related`, `lineage`, `lineage_warnings`
- Tests: round-trip a binding's full state history via `resolve(fqid).states` and via `[s for y in years for s in resolve_at(fqid, period=y)]`; they must agree on the unambiguous case. Add a multi-vintage fixture asserting `len(resolve_at(...)) == 2` and that `value_set_version="..."` narrows to length 1. Add a sibling-variant assertion: `resolve(binding).siblings` enumerates the concept's other variant deliverables.

**Estimate**: 5-6 days.

Can run in parallel with A2.3/A2.4 once A2.1.5 lands (it needs the three-level read path, not just A2.1's `variable_state`).

### [ ] A2.6 — Drop period from FQID grammar + demote `same_as` to concept grain

The FQID-grammar flip. (The three-level **table** work moved to A2.1.5
per the respec; A2.6 keeps the grammar/period changes and the `same_as`
concept-demotion.)

- Update FQID parser, emitter: 4-seg bindings, 3-seg variants, 2-seg classifications (`class/<slug>` only, version in slug). Grammar is unchanged by the three-level split — there is **no concept FQID kind** (§5.2); the `concept_slug` stays an internal column.
- Drop ~1,264 `register_version` slug entries from `scb.toml`
- Add `_default` slug to relevant variants (LSS, BU, SOL — synthesized to real rows in A4.3, but the slug exists from now)
- Drop the `register_version` table entirely. Per-edition prose (mätinformation, descriptions) goes to reg-meta-docs at variant level with chronological Markdown sections (build pipeline writes these). Per-edition build artifacts (approval dates, etc.) go to provenance DB, joined to `variable_state` by `state_id` in the sibling provenance artifact — no SCB-specific column on `variable_state` (universal-schema rule, §5.1).
- **Demote `same_as` to concept grain (respec).** Rename `variable_same_as` → **`variable_concept_same_as`**; rebuild the DDL with concept endpoints `(a_provider, a_register, a_concept, b_provider, b_register, b_concept)` — **drop the `a_variant` / `b_variant` slots** *and* the `a_period` / `b_period` columns. Rewrite `_resolve_binding_via_same_as` in `reg_meta/catalog.py`: it no longer carries variant/period through the BFS (the start node is the binding's concept; the traversal is over concept-slug triples; on a hit it descends concept→variable to find the equivalent variant deliverable). Remove the empty-string-sentinel variant/period fallback path entirely (current query: `AND (a_variant = '' OR a_variant = ?)` / `AND (a_period = '' OR a_period = ?)`). `_var_same_as_source_keys` shrinks to a 3-tuple `(provider, register, concept_slug)`.
- **Delete the var_id `(N choose 2)` auto-derive (respec).** The `auto:var_id_match` edge-emission mechanism (build-time, in `fqid_slugs.py` / materializer) is removed entirely — within-register var_id reuse is now the concept→variable hierarchy (A2.1.5), not edges. This collapses the SCB `same_as` row count from tens of thousands to the low-hundreds curated cross-register set. `classification_same_as` (cross-classification equivalence) keeps its existing shape — it was never variant/period-bearing in the same way — but drop any `*_period` columns it carries for consistency.
- **TOML same_as target tuples drop to 3-part** `{provider, register, variable}` where `variable` resolves against the target register's register-unique concept slug (§5.3, §5.5). The `same_as` field moves from the (renamed) `[variable_concept."<reg>.<var>"]` key. Dedup step for existing TOML same_as edges that carry variant/period: collapse to a single concept-grain edge; emit a build-time warning on contradiction (same (a-concept, b-concept) pair with different intended targets across variants/periods — should not occur in curated TOML but verify).
- Update Webapp catalog endpoints: 4-seg binding leaves, new sub-endpoints (§9.5). Binding-leaf response embeds concept metadata + sibling variants (§9.5).
- Old API endpoints (v0.x `register_version` leaves) removed; clients break per pre-v1 policy
- UNFROZEN sentinel is active; the slug TOML rewrite is a regular commit

**Estimate**: 7-10 days. Touches grammar, `same_as` table rename + grain demotion, API, slug TOML.

**Gate to A2.7**: A2.6 must merge.

### [ ] A2.7 — Cleanup

- Drop `variable_instance` table from DDL (kept alive A2.1.5–A2.6 only for build-pipeline dual-write while the new `variable`/`variable_state` read path stabilizes). **Respec reconciliation:** the three-level tables already exist (A2.1.5: `variable_concept`, `variable`, re-parented `variable_state`); A2.7 is purely the removal of the now-orphaned legacy `variable_instance` + `via_source_id`. By A2.7 every consumer reads `variable` / `variable_state`; `variable_instance` has no readers.
- Drop `via_source_id` column (no remaining consumer once `variable_instance` is gone; lineage is `variable_state_lineage` from A2.4)
- **Bump `SCHEMA_VERSION` to `"5.0.0"`** in `reg_meta/src/reg_meta/db.py` (currently on the 4.x line). Manifest writers in `reg_meta_build/src/reg_meta_build/db.py` pick up the constant automatically. `_check_schema_compat` rejects v4.x DBs with a clear error; testers regenerate via `reg-meta-build build-db`. Major bump lands at A2.7 (not during A2.1.5–A2.6) so a half-migrated dev DB doesn't fall into a mid-stage compatibility cliff — the intervening stages stay on the 4.x line. A1.1 already burned the 3.x → 4.0.0 break; A2.1 added `variable_state` on a minor; A2.1.5's three-level tables + A2.6's `same_as` rename also ride the 4.x line as additive/rename steps; A2.7's `variable_instance` drop is the next breaking change.
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
- Rewrite test corpus (`minimal`, `with_panel`, `composite_entity_key`, `with_namespaced_block`, `invalid_root_array`) **and `load_test_200col`** — the 200-column bundle-size-gate fixture (`reg_schema/test_corpus/load_test_200col/{input.json, expected_ValidationResult.json, build.py}`) is on v0.11 5-seg grammar with `register_version` + `columns` and will fail the A0.3 1 MB bundle-size CI gate the moment `reg_schema` flips to Model A. Rewrite it in lockstep; `build.py` regenerates the fixture from a Model A template
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
- `reg_monabundle/runtime/spec.py` `LoadedSpec` fields updated to Model A shape. PR #123 shipped LoadedSpec under v0.x grammar (`_build_source` currently reads `data["register_version"]`); this is the breaking shape evolution. **Field map** (drives both the `spec_loader.py` converter and `_build_source` rewrite):
  - `Source.register_version: str` (4-seg) → `Source.register_variant: str` (3-seg) + `Source.period` (polymorphic per §6.2: int / period-token / range / `_default` snapshot)
  - `Source.columns: tuple[Column, ...]` → `Source.bindings: tuple[Binding, ...]`
  - `Column.name: str` (5-seg) → `Binding.variable: str` (4-seg)
  - `Column.value_set: str` → `Binding.value_set` keyed by 2-seg classification FQID (`class/<slug>`)
- Amalgamator's `_AMALGAMATED_PACKAGE_PREFIXES` excludes `reg_schema` (Pydantic stays out of bundle)
- Bundle's `LoadedSpec` parsing reads `register_variant` + `period` from embedded JSON
- `LoadedSpec` deserialization is plain `@dataclass` machinery — no re-validation on MONA (§6.8.1, §9.6)

**Estimate**: 3-4 days.

---

## Stage A4 — Adapter refactor + SOS

Five PRs. Can run in parallel with A3 after **A2.7** lands. (A4.1
moves SCB ingest out of `db.py`'s `_import_*` functions; doing this
before A2.7 drops `variable_instance` would force the adapter to
dual-write both schemas. A2.7's cleanup is therefore the start gate,
matching the gates diagram below.)

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
- **Property test: namespace invariant** (§16) — 10k random `mint(...)` inputs all land in `[2^62, 2^63)` (bit 62 set, bit 63 clear); SCB ID band `[0, 2^32)` is provably disjoint
- **Provenance DB confinement test** (§16) — bundle amalgamator's import allow-list rejects modules that open `reg_meta.provenance.db`; FastAPI route introspection asserts no handler references the provenance path; deployment image excludes `reg_meta.provenance.db*` from the catalog volume mount

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

Four PRs. **Starts after A3.1 lands** (only the Pydantic models from
A3.1 are load-bearing for A5.1; A3.2/A3.3/A3.4 are consumer-side and
don't block webapp work). A4 not required — SCB-only deployment can
ship first. Matches the gates diagram annotation below.

### [ ] A5.1 — `reg_webapp` Pydantic models

- FastAPI endpoints use `reg_schema` Pydantic models directly for project_data responses
- `reg_meta` library types still wrapped 1:1 for catalog responses (the only remaining wrapper layer)
- OpenAPI codegen updated

**Estimate**: 3-4 days.

### [ ] A5.2 — New API endpoints

- Implement `?period=...` query on canonical catalog endpoint. **Wire format is a single query-string value per §9.5**, not `deepObject`: int year (`?period=2020`), period-token (`?period=HT2020` / `?period=2020-Q3` / `?period=2020-08`), range (`?period=<from>..<to>`, e.g. `?period=2018..2020`), snapshot sentinel (`?period=_default`). Server canonicalizes and returns 422 on malformed tokens; OpenAPI parameter is a plain string. Polymorphic across all `Source.period` shapes — not year-only.
- Implement `/states`, `/predecessors`, `/successors`, `/related`, `/lineage`, `/lineage_warnings` sub-endpoints (`/lineage` and `/lineage_warnings` are first-class v1 endpoints per §9.5, not deferred — `/lineage_warnings` surfaces the `variable_state_lineage_warning` rows emitted by A2.4)
- Suffixed routes registered BEFORE the `/api/catalog/{fqid:path}` catch-all in the FastAPI router (router ordering matters; see §9.5 URL routing notes); CI introspection test enforces the order for all six suffixes
- ETag scheme verified to include the `?period` query in the cache key
- Cloudflare edge-cache validation gate: small load test through Cloudflare confirms slash-bearing FQID paths still work cleanly with the new shapes
- **Server-side input-validation gates** (§16): (a) `?period=` parser is an allow-list — malformed values (SQL-injection probes, path-traversal probes, embedded NULs) return 422 with zero SQL executed (verified via SQLite trace hook); (b) per-segment FQID grammar check rejects `.`, `..`, `%`-encoded variants, and any non-slug-grammar input with 422 and no DB hit. Parametrized tests cover both.

**Estimate**: 4-5 days.

### [ ] A5.3 — SPA TypeScript regen

- OpenAPI codegen against new Pydantic models
- SPA components updated for 4-seg FQIDs
- New sub-endpoint integrations (states picker, replaced-by remediation, related-to siblings picker)
- Multi-vintage `{states: [...]}` rendering on the canonical `?period=` response: when the list has length > 1 (LKF-shape true multi-vintage), SPA shows an edition picker keyed by `value_set_version_label`; on selection, refetches with `&value_set_version=<label>` to narrow to a single state. No 409 path — the uniform list contract makes multi-vintage just length-N rather than an error.

**Estimate**: 5-7 days.

### [ ] A5.4 — SPA IndexedDB hard-reject for v0.x project files

- On Open-from-file: check `schema_version` and `reg_meta_version`; reject v0.x with blocking error
- IndexedDB schema version stored alongside each project; reject mismatched on load
- Clear migration message: "this project predates Model A. Re-author or load a v1.0+ file."
- **Delete `MIGRATION_PLAN.md`** as part of this PR (or the v1.0.0 release PR that follows it). The tracker exists under the AGENTS.md governance exception for self-deleting refactor coordination docs; A5.4 is its completion gate. Per-step landing history survives in git; the design-level narrative survives in REFACTOR_SPEC.md §15.

**Estimate**: 2 days.

---

## Gates summary

```text
A0.3 ──→ {A1.1, A1.2, A1.3} ──→ A2.1 ──→ A2.1.5 ──┬──→ A2.2 ──→ A2.4 ──┐
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
once their shared predecessor lands. **A2.1.5 (respec) is now the
hinge after A2.1** — it lands the three-level tables (`variable_concept`,
`variable`, re-parented `variable_state`) + stored variable slug +
resolver flip, which A2.2/A2.4/A2.5 all depend on. After A2.1.5, three
branches open in parallel: A2.2→A2.4 (triage siblings as `variable`
rows under a shared concept → lineage join over concept-grain
`same_as`), A2.3 (independent — reads `timeseries_event`, variable-grain
edges), and A2.5 (longitudinal catalog API on the three-level read
path). A2.6 needs **both** A2.4 (new lineage tables) and A2.5 (new
catalog API), and itself carries the FQID-grammar flip + the `same_as`
concept-grain demotion (rename + var_id-auto-derive deletion). A2.3 is
not on A2.6's critical path — it produces `variable_replaced_by` for
consumer use, not for A2.4.

In-flight PRs #131 (A2.3), #132 (A2.2), #133 (early stored-slug) all
predate the three-level respec. **#133 is superseded by A2.1.5** (its
slug-on-`variable_state` + all-eras-agree assertion is the wrong
placement). **#132 reworks** onto A2.2's three-level shape. **#131
survives** (variable-grain edges) modulo a rebase onto A2.1.5. See the
respec PR body for the close-or-rework recommendation.

## Effort estimate

| Stage | PRs | Cumulative effort (single maintainer) |
|---|---|---|
| A0 | 3 | ~5-9 days |
| A1 | 3 | ~10-14 days (parallelizable) |
| A2 | 8 | ~37-51 days (was 7 PRs / 32-44; +A2.1.5 three-level restructure, +5-7) |
| A3 | 4 | ~12-16 days (parallelizable after A3.1) |
| A4 | 5 | ~20-28 days |
| A5 | 4 | ~14-18 days |
| **Total** | **27 PRs (incl. parallelism)** | **~98-136 days** |

Numbers are **person-day sums of the sub-step ranges**, not calendar days — A1's three PRs are independent, so calendar time at that stage is `max(5-7, 2-3, 3-4) = 5-7 days`, while the table reports the 10-14 sum. The 8-12-week calendar estimate below is the parallelism-discounted figure for a single maintainer; mixing the two would double-count the savings.

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

    # SQL column references for the same_as variant/period drop + table rename +
    # register_version table drop + via_source_id removal — the string-literal FQID
    # grep above misses raw SQL. Also catch the variant slot drop and the rename.
    rg "(a_period|b_period|a_variant|b_variant|variable_same_as|register_version_id|via_source_id|register_version)" --type py --type sql
    ```

    Combine all three passes — the first finds string-literal FQIDs in fixtures/configs; the second finds programmatic callsites that may construct FQIDs at runtime; the third finds hardcoded column names in SQL queries that the FQID greps would skip.
4. **A4.3 SOS adapter discovers workbook-shape variations we haven't covered.** Mitigation: process all 13 workbooks against the adapter before merging; failures surface as `IRWarning` rows. New workbook variations would extend the adapter, not gate it.
5. **A5 webapp work lags A3 by several weeks.** Mitigation: webapp can adopt incrementally; some endpoints can land before others. The hard reject on v0.x SPA files is the only end-of-stage gate.
6. **(respec) Concept-slug register-uniqueness collides for some register where two var_ids fold to the same kolumnnamn-derived slug.** Mitigation: the build fails fast with `slug_concept_collision` listing both `(register_id, var_id)` pairs; the curator disambiguates one in `<provider>.toml` (DECISION POINT 4). This is bounded — empirically the kolumnnamn-derived slug is unique within a register in the overwhelming majority of cases (the same signal that drives §5.7 triage), and the failure is a build-time refusal, not a silent mis-link.

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
| #78–#82, #85–#87, #89, #104, #112 | reg_meta v0.11 FQID rebuild — 5-seg binding FQID, same_as edges, slug TOMLs (~1,264 entries), §5.8 cross-edition traversal | Provider / register / variant / variable / classification slug curation; consumer-side binding concept; FTS layer | 5-seg FQID grammar → 4-seg (period slot dropped); ~1,264 `register_version` slug entries deleted; `register_version` table dropped entirely. **(respec)** `variable_same_as` → `variable_concept_same_as`: variant slot **and** `*_period` columns dropped, demoted to concept grain, var_id `(N choose 2)` auto-derive deleted (within-register reuse becomes the concept→variable hierarchy) | A2.1.5, A2.6, A2.7 |
| #103, #105, #108 | `reg_meta_build` package carve-out (mechanical split: scaffold, db.py, doc_db.py, CLI split, CI workflow) | Package boundary intact; CLI binaries (`reg-meta`, `reg-meta-build`); test helpers | Column names → English (data values unchanged); SCB-Swedish column renames | A1.1 (rename only) |
| #110, #111, #115 | `reg_schema` v0.x — `ValidationIssue` / `ValidationResult` contract; frozen dataclasses (ProjectData, Source, Column, …); `validate_structural` entrypoint | `ValidationResult` JSON contract concept; 22+ stable issue codes (most unaffected); validate_structural API surface | Dataclasses → Pydantic v2; `Source.register_version` → `register_variant + period`; `columns` → `bindings`; `Column.name` → `variable`; 1 issue code renamed + 6 new codes (per A3.1: `invalid_period`, `period_outside_state_validity`, `binding_state_drifts_within_period`, `binding_state_ambiguous`, `variable_replaced`, `panel_inheritance_unresolvable`); bump to `schema_version: "2.0.0"` | A3.1 |
| #116 | `mock_data_wizard` adopts `project_data.json` (config rename, fixture corpus rewrite for v0.11 5-seg shape) | Config-rename machinery; fixture-rewrite tooling; v0.x test fixtures' overall structure | Source schema break propagated through mdw; fixture corpus rewritten again to Model A shape; `_build_source` reads new fields | A3.2 |
| #113 | Shared validator test corpus — `reg_schema/test_corpus/` with 4 well-formed + 1 negative cases; harness with drift protection | Corpus discovery + harness machinery | All 5 cases rewritten: source becomes 3-seg FQID + `period`; bindings replace columns; namespaced-block keys 4-seg | A3.1 (paired with reg_schema migration) |
| #120, #121, #122, #123, #124, #125 | `reg_monabundle` carve-out — phases 1, 2a, 2b, 2c, 3 + follow-ups (scaffold, validator relocation, bundle builder, PII scanner, runtime modules + LoadedSpec, 1 MB bundle-size gate) | Survives entirely; this is the Stage A0 work | (none) — A0 complete | A0 ✅ |

In-flight Model A PRs affected by the three-level respec:

| In-flight PR | Built (pre-respec) | Disposition under three-level respec | Stage |
|---|---|---|---|
| #130 (merged) | A2.1 `variable_state` coalescer; `valid_from`/`valid_to`; `unika_summary` drop | **Survives.** A2.1.5 re-parents `variable_state` from `(register_id, var_id)` to the new `variable.variable_id`; the coalescer output is the input it re-parents | A2.1 → re-parented by A2.1.5 |
| #131 (open) | A2.3 `variable_replaced_by` + register/variant parallel tables, `auto:timeseries_event` | **Survives** (variable-grain edges; 4-part endpoints). Wording tweak only: the variable slot resolves to one stored `variable` row. Rebase onto A2.1.5 if it lands after | A2.3 |
| #132 (open) | A2.2 build-time triage; siblings via **minted var_id** + `variable_state.slug`; `variable_related_to` | **Reworks** onto A2.2's three-level shape: siblings become `variable` rows under a *shared* concept (no new var_id; synthetic `variable_id` + stored variable slug). `variable_related_to` shape unchanged | A2.2 |
| #133 (open) | early stored slug on **`variable_state.slug`** + "all eras agree" assertion + resolver flip | **Superseded** by A2.1.5 — the slug moves to the new `variable` table (one deliverable, one slug), so the era-denormalization + all-eras-agree assertion are deleted. Close or rework into A2.1.5; the slug-population + resolver-flip mechanism is reusable, just retargeted | A2.1.5 |

Conventions: stage IDs reference the sections above. PR links (`[#NNN]`) resolve against the project's GitHub repo.
