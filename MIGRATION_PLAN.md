# Model A Migration — Implementation Tracker

Status: **A0–A1 complete; A2.1 shipped (PR #130); A2.2–A2.7 open** (Model A design locked 2026-05-22; **two-level variable-model respec in flight** — variable + state, FQID→variable; see REFACTOR_SPEC §5.0.1/§5.1).

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

Estimated total effort: ~97–135 person-days across 27 PRs in 5 stages (the two-level respec re-derives A2's contents and adds stage A2.1.5 — the table restructure pulled earlier as a hard prerequisite for triage/lineage, per the maintainer red-line). Realistic calendar time for a single maintainer: 8–12 weeks. Stages can overlap where dependencies allow.

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
- `_populate_sensitivity_flags(conn)` runs after `_import_registerinformation` + `_import_unika`. Joins through `variable_instance × variable_alias × variable` and disambiguates by the full `unika_summary` PK `(register_id, register_variant_id, kolumnnamn, variabelnamn)` — without the `variabelnamn` join, the same `kolumnnamn` reused across distinct variables under one variant would fan flags onto wrong siblings.
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

**Two-level redefinition (post-#135, standalone PR).** A1.3 shipped `IRVariable` with a variant-scoped `variant_id` (the pre-§5.0.1 model). PR #135's two-level respec redefined the §4.4 contract, so the (inert, no consumer until A4.x) IR code is brought in line here — landed **independently** of the A2.1.5 DB restructure (#136), since the IR types don't touch the DB schema:

- `IRVariable` is now register-scoped: drops `variant_id`, gains `provider_key` (the NON-unique join hint). The variant coordinate moves to `IRVariableState.register_variant_id`.
- `IRVariant.variant_id` → `register_variant_id` (matches the `register_variant` table PK, per the A2.1.5 naming decision).
- `IRVariableState.data_length` typed `str | None` (TEXT — SCB `datalangd` may carry precision/scale "8,2"; nullable since an adapter may omit it), per the #135 review.
- Spec §4.4 + the `test_ir_scaffolding.py` contract fixtures updated to match.

---

## Stage A2 — Model A schema (load-bearing gate)

Eight PRs. The largest and most intricate stage. Sequencing matters; gates are explicit.

**Two-level restructure (respec).** The object model splits today's
conflated `variable` into exactly two levels — `variable` (the
addressable variable, the FQID target, holds the register-unique
variable slug + shared metadata) and `variable_state` (per-delivery
shape, child of the variable, each state carrying both a **variant
coordinate and a period range**). The variant (SCB `registervariant` /
SOS `deldatamängd`) is a *delivery coordinate*, not an identity level,
so it leaves the binding FQID (4→3 seg `provider/register/slug`);
the period was already out. `same_as` / `replaced_by` / `related_to`
all become **variable grain**, and the within-register var_id
`(N choose 2)` `variable_same_as` auto-derive is **deleted** (identity
is the variable itself). See REFACTOR_SPEC §5.0.1, §5.1, §5.2, §5.5.

**Sequencing — the restructure is a hard prerequisite, not a late
fold (maintainer red-line).** An earlier draft of this plan said the
table restructure "folds into A2.6" and that A2.2/A2.4 "stay on the
A2.1 shape until then." That hid a hard dependency:

- A2.2's deliverable is "splits mint **distinct `variable`
  rows**" — but the A2.1 schema has no `variable` table and
  keys `variable`/`variable_state` on `(register_id, var_id)`, which
  structurally **cannot hold two variables per `var_id`**. Triage
  literally has nowhere to write a sibling variable until the table
  exists.
- A2.4's `variable_set_via_same_as` BFS reads a table that pre-restructure
  is *variant-grained* (`a_variant`/`b_variant`) and *full of the
  `(N choose 2)` auto-derived edges* — so the variable-grain traversal
  isn't a rename, it needs the demoted table.

So the restructure is pulled **earlier into its own stage, A2.1.5**
(promote the register-scoped `variable` to the addressable variable — add
a synthetic `variable_id` PK + register-unique `slug`; re-parent
`variable_state` onto `variable_id` with a `register_variant_id` coordinate;
populate the stored variable slug; demote `provider_key` to a non-unique
join hint).
A2.1.5 **gates A2.2 and A2.4**. It **supersedes the stored-slug idea in
PR #133** (the slug lives on `variable`, register-unique, not
denormalized onto `variable_state`). A2.6 is then left with only what
genuinely belongs to the grammar flip: drop the variant from the
binding FQID (4→3 seg), the resolver flip, the `variable_same_as` grain
demotion, and the var_id-auto-derive deletion.
(This partly revives the early-restructure stage an earlier draft folded
away — deliberately.)

### [x] A2.1 — `variable_state` table + coalescing (PR #130)

- `variable_state` table added to DDL (sibling to `variable_instance`); `valid_from`/`valid_to` are `TEXT NOT NULL` full `YYYY-MM-DD`, enforced by CHECK constraints (`length() = 10`, `valid_to >= valid_from`). Open-ended states get the sentinel `valid_to = '9999-12-31'`; the unknown-lower-bound fallback uses `'0001-01-01'` for the rare yearless cvid case
- `_coalesce_variable_states` (`reg_meta_build/src/reg_meta_build/db.py`) groups `variable_instance` rows by the 8-tuple `(register_id, register_variant_id, var_id, data_type, data_length, value_set_id, value_set_version_label, grain)`. `grain` lives only in the in-memory group key — never lands on `variable_state`, per the universal-schema contract. Reads `unika_summary` for VersionForsta/VersionSista (primary), falls back to `register_version.registerversionnamn` year extraction when unika has no matching `(register_id, register_variant_id, kolumnnamn, variabelnamn)` row. `delivery_column_name` denormalizes the latest alias for the era (highest `regver_id`, lexically smallest on ties)
- **`unika_summary` dropped** at end of build via `DROP TABLE` + `VACUUM`. Both consumers (`_populate_sensitivity_flags`, `_coalesce_variable_states`) have run; the table is now unused in the shipped DB
- Resolver still uses `variable_instance`; no query-layer behavior change (that's A2.5)
- `SCHEMA_VERSION` bumped `4.0.0` → `4.1.0` (additive new table, drop of a build-only table — minor bump per the §5.11 compat rule)
- Tests: 10 new tests under `TestBuildDb` cover row presence per `(register, var)` triple, full-ISO column shape, year expansion (2022 → 2022-01-01..2022-12-31), unika min/max range, delivery_column_name tie-break, `register_version` fallback path, value_set_version_label preservation, grain split, FK to `variable`, and manifest `coalesce_stats` consistency. Empirical coalescing-rate validation (5× shrink, 65% single-state) lands when running against the full SCB corpus

**Gate to A2.1.5**: ✅ Met. The coalesced `variable_state` rows are the input the two-level restructure re-parents.

### [ ] A2.1.5 — Two-level table restructure + stored variable slug (respec; supersedes PR #133)

The structural prerequisite for triage (A2.2) and lineage (A2.4) — it
creates the `variable` table they write to and re-parents
`variable_state`. Lands **before** A2.2/A2.4, on the 4.x line (additive
tables + a column move + an FK re-parent; regenerate-not-migrate, no
data migration). Per REFACTOR_SPEC §5.1, §5.3.

**Status:** the restructure (promote `variable`, re-parent `variable_state`,
and the `var_id`→`provider_key` CAST rework) landed in **#136**; the IR
redefinition in **#137**. The remaining A2.1.5 work landed on the
`a2.1.5-variable-slug` branch: stored-variable-slug population, the
resolver/emitter flip, and the `variable_same_as` grain demotion (dropped
`*_variant`/`*_period` columns + the lockstep `_resolve_binding_via_same_as`
rewrite — the resolver now inherits the query's variant/period since edges no
longer narrow). Notes: there was no `(N choose 2)` `var_id` auto-derive to
delete (it was never implemented), and `classification_same_as` already had no
`*_period` columns. **Remaining before this checkbox closes:** curate the
register-unique slug collisions a real `build-db` surfaced (register-scoped
uniqueness is stricter than the old variant grain — e.g. `tidomspct` on
1.881/1.887) via `[variable]` overrides in `scb.toml`, and commit the generated
`scb.auto.toml`.

- **Promote `variable` in place** (it keeps its name — it is already the register-scoped `variable` table on `main`; this is not a rename). Add the synthetic `variable_id AUTOINCREMENT` PK (DECISION POINT 1) and a register-unique `slug`. The A1.2 sensitivity flags (`is_sensitive`, `is_identifier`) and the shared-metadata columns (`name`, `definition`, `description`, `measurement_unit`, `source_register_id`, `source_register_text`) ride along unchanged — same grain, pure column add.
- **Natural key = `(register_id, slug)`** UNIQUE (the FQID leaf; §5.1 DDL). Add the `slug` column (register-unique). **`provider_key` (the old `var_id` / SOS name) is a NON-unique join hint** — a plain index, *not* UNIQUE — because A2.2's triage splits put several variables under one source key (maintainer red-line on DP1). The build's source-row → variable join refines `provider_key` by the triage discriminator when a split exists (§5.7), 1:1 otherwise.
- **Re-parent `variable_state`** from FK `(register_id, var_id)` to FK `variable_id` → `variable`, and **add an explicit `register_variant_id` coordinate** (FK → `register_variant`). `value_set_version_label` becomes `NOT NULL DEFAULT ''` (the coalescer coalesces NULL → ''). **The §5.1 state-uniqueness index `(variable_id, register_variant_id, valid_from, value_set_version_label)` is NOT added here** — `_coalesce_variable_states` emits one *pre-triage* row per shape group, so a same-year variable with multiple grains / codings (groups differing on `data_type` / `value_set_id` / `grain`, all carrying `value_set_version_label = ''`) produces rows that share that 4-tuple and would collide. The invariant only holds *after* A2.2 folds them (→ `value_set_version_label`-discriminated states) or splits them (→ distinct `variable_id`s), so the unique index lands in **A2.2**. The `''` default is set here so the index bites once added.
- **Stored-variable-slug population** (`populate_variable_slugs`) — **landed** (build-side population commit + query-side resolver-flip commit on `a2.1.5-variable-slug`). Variable slug auto-derives from the latest kolumnnamn into `<provider>.auto.toml` (register-unique; collision → `slug_variable_collision`, DP1; underivable → `slug_variable_underivable`, lists all offenders). Cross-era tiebreak (§5.3): the slug derives from the variable's latest `variable_state` era — highest `valid_to`, `delivery_column_name` lexically smallest on ties. (The §5.3 prose says "highest `regver_id`", but `regver_id` is **not** on the re-parented `variable_state`, so `valid_to` is the era-order key — same intent, the only era column available post-reparent.) There are no `[variable]` keys in `scb.toml` today, and `[variable]` keys already parse as the register-scoped 2-part `<reg>.<var>` form (via `_parse_variant_id`), so there was no 3-part→2-part collapse to perform; the `slug_variable_override_unsupported` gate is lifted and curated `[variable]` overrides now write the column. The resolver + `get_schema`/`get_varinfo` read the stored `variable.slug` (exact match) instead of deriving at query time; `SCHEMA_VERSION` bumped 4.2.0 → 4.3.0 (a slug-NULL 4.2.0 DB resolves nothing under the flip). **(This is the part of PR #133's stored-slug idea that survives — moved onto `variable`, register-unique, not onto `variable_state`.)**
- **Demote the `variable_same_as` table to variable grain** (the table keeps its name): drop the `a_variant`/`b_variant` and `a_period`/`b_period` columns, and **delete the `auto:var_id_match` `(N choose 2)` auto-derive** — within-register `var_id` reuse is now the variable itself (the promoted register-scoped row), so those edges are redundant the moment this stage lands. Materialize the remaining curated cross-register/cross-provider edges at variable grain. **Rewrite the `_resolve_binding_via_same_as` query in `reg_meta/catalog.py` to the variable-grain column shape in the same stage**, in lockstep with the column drop: the live resolver `SELECT`s `b_variant`/`b_period` and filters `AND (a_variant = '' OR a_variant = ?)` / `AND (a_period = '' OR a_period = ?)`, so dropping those columns *without* the query rewrite would raise `no such column` on the first binding that hits the `same_as` fallback during A2.1.5–A2.5 — the table shape and the code that reads it have to change together. The rewrite drops the variant/period select + filter and looks up variable-grain `(a_provider, a_register, a_variable)` triples; it still derives that triple from the interim **5-seg** binding (ignoring the variant/period segments) — the FQID-grammar flip to 3-seg is A2.6 and only simplifies how the start node is parsed. *(Table + its reader changing together, decoupled from the grammar flip, is what breaks the A2.4↔A2.6 cycle: A2.4's build-time linker traverses the variable-grain `same_as` table with no dependency on A2.6.)*
- Resolver still reads `variable_instance` for **direct** binding resolution at this stage (the longitudinal `resolve` flip is A2.5; the FQID-grammar flip is A2.6) — its resolution *logic* is unchanged. Two **mechanical** query-layer reworks ride along: (1) the `var_id` → `provider_key` (TEXT) rename means every `variable` join/read CASTs across the resolver + `queries.py` (`variable_instance.var_id` INTEGER ↔ `variable.provider_key` TEXT; the CLI still exposes an integer `var_id` via `CAST(provider_key AS INTEGER)`, and `variable_fts.var_id` likewise becomes `provider_key`); (2) the `same_as` fallback rewrite must track the column drop in lockstep. `variable_state` is dual-written from the coalescer onto the new shape.
- `SCHEMA_VERSION` rides the 4.x line (promotion + re-parent + column move + `same_as` grain demotion; no consumer-visible break until A2.7).
- Tests: `variable` row presence per `(register, var)`; `(register_id, slug)` uniqueness; `provider_key` **non**-uniqueness (two variables can share it once A2.2 lands — assert the index allows it); `variable_state` re-parent FK integrity; `''` default on `value_set_version_label`; cross-variant slug tiebreak. (The state-uniqueness index and its rejection test move to A2.2 — see above.)

**Estimate**: 4-6 days. Table restructure + slug population; no triage/grammar logic yet.

**Gate to A2.2 and A2.4**: A2.1.5 must merge. A2.2 mints sibling `variable` rows (needs the table); A2.4 traverses the variable hierarchy. Both are structurally impossible on the A2.1 `(register_id, var_id)` schema.

### [ ] A2.2 — Build-time triage (reworked onto two-level model)

Requires **A2.1.5** (the `variable` table siblings are written
to). In-flight PR #132 reworks onto this shape.

- Implement `triage_same_year_collisions` pass per §5.7
- Kolumnnamn-primary discriminator (using `variable_alias.kolumnnamn` set intersection)
- **Triage *folds* or *splits*** (§5.7): same-concept representations (shared column stem + a vintage/grain/coding axis — `FtgSni69`/`FtgSni92`, `Ssyk3`/`Ssyk5`, `BCIV`/`BCIVRED`) **fold** into one variable with overlapping `value_set_version_label`-discriminated states (no edge); genuinely-different concepts (disjoint stems — e.g. rooms vs area under a generic `Imputerat` `var_id`) **split** into N distinct variables with their own register-unique slugs + `variable_related_to` edges. `Variabelnamn` is a generic family label (useless as the signal); the column stem decides. (Same split path handles SOS name-reuse collisions, §5.1, DECISION POINT 4; empirically ~56% fold / ~44% split, §5.7.)
- Auto-derive slugs: a **folded** variable gets one slug from the shared stem (representation suffix stripped); a **split** mints distinct per-column slugs (kolumnnamn → datalangd → BLAKE2b fallback). The vintage/grain/coding token that would have suffixed a sibling slug now populates `value_set_version_label` on the folded states instead.
- **Persist the source-row discriminator maps** (§5.7): for each *split* `var_id`, record which kolumnnamn maps to which sibling `variable_id` (a later delivery row resolves to the right sibling by its own column); for each *fold*, record which representation token (grain/vintage/coding) maps to which `value_set_version_label` (a delivery row resolves to the right *state* of the one variable). Emit `triage_unresolved_split` when a split row's column matches no recorded sibling (route to a new auto-slugged sibling, additive).
- Auto-emit `variable_related_to` edges (variable grain, 3-part endpoints) between the sibling variables per `relation_kind`
- Add `variable_related_to` table to DDL (variable-grain endpoints)
- **Add the `variable_state` state-uniqueness index** `UNIQUE(variable_id, register_variant_id, valid_from, value_set_version_label)` (deferred from A2.1.5, §5.1). Now valid: triage has folded same-year multi-shape groups into `value_set_version_label`-discriminated states or split them into distinct `variable_id`s, so the 4-tuple is unique. Add the rejection test here.
- TOML override mechanism in `scb.toml` for ~200-300 manual cases — sibling-variable-slug overrides use the `[variable."<reg>.<var>"]` key (§5.3)
- Tests: run against full SCB DB; verify *folds* keep one variable with discriminated overlapping states (one `variable_id`, distinct `value_set_version_label` per state) and *splits* create distinct variables (distinct `variable_id`/slug, shared `provider_key`); confirm the heuristic auto-resolves the clear cases and only the fuzzy-boundary cases need TOML overrides

**Estimate**: 7-10 days. Heuristic refinement + curation backlog.

**Gate to A2.4**: A2.2 must merge. Lineage join (A2.4) operates on triaged variables.

### [ ] A2.3 — Auto-derive `variable_replaced_by` from `timeseries_event`

- Add `variable_replaced_by` table to DDL (per §5.5) at **variable grain** — 3-part `(provider, register, variable)` endpoints
- Build pipeline: after `_import_timeseries`, materialize succession edges from `timeseries_event` rows with `handelse IN ('Ersatt av', 'Ersätter')`, mapping each variable to its variable
- Inverse-direction collapse (`Ersätter` is the inverse of `Ersatt av`)
- Add `register_replaced_by` and `variant_replaced_by` parallel tables for register/variant-level rows
- TOML curation slot in `scb.toml` and `sos.toml` for cross-provider edges (empty for now; populated in A4 if needed)

**Estimate**: 3-4 days.

Can run in parallel with A2.2. **Respec note (in-flight PR #131):** #131 built `variable_replaced_by` with variant-bearing 4-part endpoints (matching the then-current 4-seg FQID). The two-level model makes it **variable grain** — drop the `*_variant` columns; the variable slot becomes the target's variable slug. The auto-derive logic is otherwise unchanged (succession is a register-level fact about the variable). PR #131 mostly survives; this is the only schema adjustment.

### [ ] A2.4 — `variable_state_lineage` interval-overlap join (variable-grain matching)

Requires **A2.1.5** (it reads `variable.source_register_id` and
descends the variable hierarchy).

- Add `variable_state_lineage` and `variable_state_lineage_warning` tables to DDL
- **Source link + matching are variable grain (respec).** The source register comes from `variable.source_register_id` (shared metadata, from A2.1.5); source-side matching traverses variable-grain `same_as` — `variable_set_via_same_as` replaces the per-variant `slug_set_via_same_as` fold (§5.6). One variable edge covers every variant the source delivers.
- **`same_as` is already variable-grain by A2.4 — no A2.6 dependency (cycle resolved).** The `variable_same_as` *table* is demoted to variable grain back in **A2.1.5** (variant/period columns dropped, `(N choose 2)` var_id auto-derive deleted; the table keeps its name). So `variable_set_via_same_as` reads a clean variable-grain table here, with **no dependency on A2.6** — A2.1.5 already rewrote the resolver-side `_resolve_binding_via_same_as` query to match the demoted columns (in lockstep with the drop), and A2.4's *build-time* linker never calls the resolver anyway. This is what breaks the earlier A2.4↔A2.6 ordering cycle: A2.6 gates on A2.4 (it needs the lineage tables), and A2.4 no longer gates on A2.6.
- Source-variant pinning is **TOML-only**, not a SQL table:
  - Heuristic defaults in `lineage_defaults` TOML block (per source register)
  - Per-variable overrides in `lineage."<consumer_register>.<slug>"` TOML blocks
- Implement new `link_variable_state_lineage` per §5.6 pseudocode
- Drop old `link_consumer_side_bindings` (its inputs go away with `variable_instance`)
- Tests: 5 worked LISA-RTB examples from the agent report verify the algorithm

**Estimate**: 4-5 days.

**Gate to A2.6**: A2.4 *and* A2.5 must merge. A2.6 flips the FQID grammar (drop the variant from the binding) + resolver flip + the `same_as` start-node parse update (the `same_as` **table** demotion **and** its `_resolve_binding_via_same_as` reader rewrite already landed in A2.1.5), which requires both the new lineage tables (A2.4) and the new catalog API (A2.5) in place. No reverse dependency — A2.4 reads the already-demoted table, so the gate is acyclic.

### [ ] A2.5 — Catalog API shift

- `Catalog.resolve(fqid)` **flips semantics in place** — now returns longitudinal `ResolvedVariable` (per §5.10): the variable's shared metadata + its `variable_state` rows (each tagged with its variant) + variable-grain edges. The v0.x per-cvid behavior is **deleted**, not aliased — pre-v1 policy allows the break. (A2.5 reads the A2.1.5 tables — `variable` + re-parented `variable_state`; the *binding-FQID* resolution still parses the **v0.11 5-seg** grammar until A2.6 (the Model-A 4-seg form was specced in #126 but never implemented, so the interim is the shipped 5-seg). So A2.5 builds the longitudinal aggregate on the new tables, A2.6 flips the FQID parser + the binding read path to 3-seg + exact variable-slug match, dropping the period **and** variant segments.)
- Implement `Catalog.resolve_at(fqid, period, *, variant=None, value_set_version=None) -> list[VariableState]` (`period` polymorphic per §6.2; not year-only). Always returns a list: length 1 for an unambiguous single-state-in-one-variant-and-one-version point query, length N across variants / range periods / folded classification-version (multi-vintage) states (common, not rare). Empty list when no state covers the period (no exception). `variant` narrows to one variant (the Source's `register_variant`); `value_set_version` narrows multi-vintage results to a single state.
- Implement `Catalog.states(fqid)`, `.predecessors(fqid)`, `.successors(fqid)`, `.related(fqid)`, `.lineage(fqid)`, `.lineage_warnings(fqid)` — all list-returning per §5.10. `same_as` / `replaced_by` / `related_to` accessors return variable-grain refs (3-part binding FQIDs).
- Post-A2.5 public method roster: `resolve` (new semantics), `resolve_at`, `states`, `predecessors`, `successors`, `related`, `lineage`, `lineage_warnings`
- Tests: round-trip a binding's full state history via `resolve(fqid).states` and via `[s for y in years for s in resolve_at(fqid, period=y, variant=v)]`; they must agree on the unambiguous case. Assert each state carries its variant coordinate. Add a multi-vintage fixture asserting `len(resolve_at(...)) == 2` and that `value_set_version="..."` narrows to length 1.

**Estimate**: 5-6 days.

**Gate: A2.5 requires A2.2 + A2.3 + A2.4** (not just A2.1.5). Its edge accessors read tables those stages create — `related` ← `variable_related_to` (A2.2), `predecessors`/`successors` ← `variable_replaced_by` (A2.3), `lineage`/`lineage_warnings` ← `variable_state_lineage` (A2.4) — so A2.5 is the join point after the A2.2→A2.4 chain and A2.3, not a parallel branch (a query against a not-yet-created edge table raises `no such table`). `resolve`/`resolve_at`/`states` need only the A2.1.5 tables, but the accessor roster ships as one PR.

### [ ] A2.6 — Drop period & variant from FQID grammar (resolver flip)

The grammar flip. The two-level **table** restructure already landed in
A2.1.5 (the `variable` table, the re-parented `variable_state`,
the stored variable slug, the `same_as` table grain-demotion, **and the
lockstep `_resolve_binding_via_same_as` column-shape rewrite it forced**);
A2.6 flips the FQID grammar to match — the 3-segment binding FQID names
the variable directly — plus the resolver flip. No table rename,
re-parent, or column drop here (all done in A2.1.5); the only `same_as`
touch left is feeding the query its start node from the new 3-seg parse.

- **Update FQID parser, emitter:** 3-seg bindings (`provider/register/slug`), 2-seg classifications (`class/<slug>`). **The variant FQID kind is removed** — variants become a navigational register sub-resource (§5.2, §9.5). Both the period (already out) and the variant leave the binding.
- **Resolver flip.** `_resolve_binding_direct` in `reg_meta/catalog.py` now parses the 3-seg binding and reads the A2.1.5 stored `variable.slug` via an **exact match** (no `derive_variable_slug`-at-resolve, no fold ambiguity), joining `variable_state` through `variable_id`, filtered by `register_variant_id` (from the Source's `register_variant`) + period. The v0.11 5-seg parse path used by A2.5 is deleted.
- Drop ~1,264 `register_version` slug entries from `scb.toml`
- Add `_default` slug to relevant variants (LSS, BU, SOL — synthesized to real rows in A4.3) — as a variant coordinate, not an FQID segment
- Drop the `register_version` table entirely. Per-edition prose → reg-meta-docs at variant level; per-edition build artifacts → provenance DB, joined to `variable_state` by `state_id` — no SCB-specific column on `variable_state` (universal-schema rule, §5.1).
- **`same_as` start node from the 3-seg parse.** The heavy lifting already happened in A2.1.5: the `variable_same_as` **table** was demoted to variable grain (dropped `a_variant`/`b_variant` + `a_period`/`b_period`, deleted the `auto:var_id_match` `(N choose 2)` auto-derive; name kept) **and** `_resolve_binding_via_same_as` in `reg_meta/catalog.py` was rewritten to the variable-grain column shape in lockstep (variant/period select + filter removed — `AND (a_variant = '' OR a_variant = ?)` / `AND (a_period = '' OR a_period = ?)` gone; `_var_same_as_source_keys` already a 3-tuple `(provider, register, slug)`). A2.6 only changes where that query's start-node triple comes from — the new 3-seg binding parse, instead of stripping the variant/period off the interim 5-seg form. No further `same_as` query or column change. (The row-count collapse to the low-hundreds curated cross-register set, and `classification_same_as` losing its `*_period` columns, both also happened at A2.1.5.)
- **TOML same_as target tuples drop to 3-part** `{provider, register, variable}` where `variable` resolves against the target register's register-unique variable slug; the `same_as` field lives on the A2.1.5 `[variable."<reg>.<var>"]` key. Dedup step for existing TOML same_as edges carrying variant/period: collapse to a single variable-grain edge; emit a build-time warning on contradiction (should not occur in curated TOML but verify). An endpoint naming a split `var_id` is resolved by the §5.5 discriminator, else dropped with a warning.
- Update Webapp catalog endpoints: 3-seg binding leaves, variant sub-resource (`/{provider}/{register}/variants`), new sub-endpoints (§9.5). Binding-leaf response embeds variable metadata + states tagged with their variant.
- Old API endpoints (v0.x `register_version` leaves) removed; clients break per pre-v1 policy
- UNFROZEN sentinel is active; the slug TOML rewrite is a regular commit

**Estimate**: 7-10 days. Grammar flip + resolver flip + `same_as` grain demotion + slug TOML purge. (The table restructure that an earlier draft bundled here is now A2.1.5.)

**Gate to A2.7**: A2.6 must merge.

### [ ] A2.7 — Cleanup

- Drop `variable_instance` table from DDL (kept alive A2.5–A2.6 only for build-pipeline dual-write while the new `variable` / re-parented `variable_state` read path stabilizes)
- Drop `via_source_id` column (no remaining consumer once `variable_instance` is gone; lineage is `variable_state_lineage` from A2.4)
- **Bump `SCHEMA_VERSION` to `"5.0.0"`** in `reg_meta/src/reg_meta/db.py` (currently `"4.1.0"` after A2.1's minor bump). Manifest writers in `reg_meta_build/src/reg_meta_build/db.py` pick up the constant automatically. `_check_schema_compat` rejects v4.x DBs with a clear error; testers regenerate via `reg-meta-build build-db`. Major bump lands at A2.7 (not during A2.1.5–A2.6) so a half-migrated dev DB doesn't fall into a mid-stage compatibility cliff — the intervening stages stay on the 4.x line. A1.1 already burned the 3.x → 4.0.0 break; A2.1 added `variable_state` on a minor (4.1.0); A2.1.5's two-level promotion + re-parent and A2.6's `same_as` demotion also ride the 4.x line (additive tables + promotion + a column move + row deletions); A2.7's `variable_instance` drop is the next breaking change.
- **Stated outright (lower-severity item):** `SCHEMA_VERSION` deliberately **stays `4.x` across A2.1.5 → A2.6**, even though those stages promote a table (add a synthetic PK + slug to `variable`), re-parent an FK (`variable_state` → `variable_id`), drop columns (`same_as.*_variant`/`*_period`), and delete rows (the var_id auto-derive). So `_check_schema_compat` is effectively a **no-op mid-stage** — a dev DB built at A2.3 and queried at A2.5 won't be rejected by the version gate even though its shape differs. This is **acceptable under regenerate-not-migrate + UNFROZEN**: pre-v1 there are no external DB consumers to protect, the build is the only writer, and testers `reg-meta-build build-db` from source whenever they pull a new stage (the README/CI instruct this). The single 4.x→5.0.0 break at A2.7 is the one consumers ever see. If a mid-stage dev hits a stale-DB crash, the fix is "rebuild," not a finer-grained version gate — adding per-stage minors would be churn with no payoff pre-v1.
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
- Binding `name` → `variable` (**3-seg** variable FQID; `provider/register` prefix must equal the source's `register_variant` prefix — the variant is not repeated on the binding)
- Panel `entity_key` / `time_key` inheritance from `variant.panel_template` when omitted
- TimePoint gains range form `{"range": {"from", "to"}}`
- New issue codes: `invalid_period`, `period_outside_state_validity`, `binding_state_drifts_within_period`, `binding_value_set_version_ambiguous`, `binding_value_set_version_mismatch`, `variable_replaced`, `panel_inheritance_unresolvable` (the last is semantic-layer; raised by kit/bundle-build when a member's variant has no `panel_template` and no explicit keys — §6.4 + §6.8.3)
- Rename: `fqid_register_version_mismatch` → `fqid_register_variant_mismatch`
- Rewrite test corpus (`minimal`, `with_panel`, `composite_entity_key`, `with_namespaced_block`, `invalid_root_array`) **and `load_test_200col`** — the 200-column bundle-size-gate fixture (`reg_schema/test_corpus/load_test_200col/{input.json, expected_ValidationResult.json, build.py}`) is on v0.11 5-seg grammar with `register_version` + `columns` and will fail the A0.3 1 MB bundle-size CI gate the moment `reg_schema` flips to Model A. Rewrite it in lockstep; `build.py` regenerates the fixture from a Model A template
- Bump pinned `reg_meta_version` in steward catalogs to `reg_meta/v1.0.0`
- JSON schema codegen produces SPA TypeScript types

**Estimate**: 5-7 days (slightly larger than initial 4-6 due to broader rename surface).

### [ ] A3.2 — `mock_data_wizard/spec.py` adoption

- `_build_source` reads new `register_variant` + `period` fields; `columns` → `bindings`; `Column.name` → `Binding.variable`
- `lookup_options` keys remain FQID-based (3-seg variable FQID now)
- Fixture sweep for all `project_data.json` files under `mock_data_wizard/`
- Companion `project_data.codes.json` fixtures restructured to the new shape (`classifications` + `sources.<name>.<binding_fqid>` blocks; §6.6) — every test that asserts against the old flat FQID-keyed codes file follows
- Tests follow

**Estimate**: 3-4 days.

### [ ] A3.3 — `reg_monabundle/validate.py` 3-seg update

- One-line edit in `_is_binding_fqid` (5 → 3 segments)
- Error message text update
- Tests follow

**Estimate**: 1 day.

### [ ] A3.4 — Bundle amalgamator update

- Add `reg_monabundle/build/spec_loader.py` with `source_to_loadedspec(pydantic_source) -> LoadedSpec` — lives in `build/`, not `runtime/`, so the bundle never imports Pydantic (the §9.6 boundary). Called by the bundle builder before embedding JSON.
- `reg_monabundle/runtime/spec.py` `LoadedSpec` fields updated to Model A shape. PR #123 shipped LoadedSpec under v0.x grammar (`_build_source` currently reads `data["register_version"]`); this is the breaking shape evolution. **Field map** (drives both the `spec_loader.py` converter and `_build_source` rewrite):
  - `Source.register_version: str` (4-seg) → `Source.register_variant: str` (3-part coordinate) + `Source.period` (polymorphic per §6.2: int / period-token / range / `_default` snapshot)
  - `Source.columns: tuple[Column, ...]` → `Source.bindings: tuple[Binding, ...]`
  - `Column.name: str` (5-seg) → `Binding.variable: str` (**3-seg** variable FQID)
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
- **Variable merge (respec, DECISION POINT 4):** `SosVariable` identity is `(deldatamangd, name)`, but a SOS variable is `(register, variable_name)` — the adapter **merges same-named variables across deldatamängder into one `IRVariable`** (the register-level kodlistor are shared; §5.0.1), emitting one `IRVariableState` per `(deldatamängd, period)` with the deldatamängd as the `register_variant_id` coordinate. It **splits** into distinct variables only on a genuine meaning conflict (incompatible data types, or disjoint code-list shapes for the same name — BU `FOD_DATUMN`, PAR `ATC`), via the §5.7 triage path.
- Variant synthesis for LSS/BU/SOL (`_default` real variant row, referenced as the state's `register_variant_id`)
- Kodlista state-era parsing per §5.7 (period-scoped `tidsperiod` ranges → state validity)
- MFR IVF_klinik entity-registry heuristic (collapse to 1 state with per-code validity, not 15 states)
- Outputs ~2,300 IR rows

**Estimate**: 7-10 days. Most complex SOS-specific logic.

### [ ] A4.4 — Slug TOML + panel_template curation (SCB + SOS)

- Create `reg_meta_build/fqid_slugs/sos.toml`
- Register slugs: 3-letter SOS abbreviations (`par`, `mfr`, `dors`, etc.)
- Variant slugs from deldatamangd names (lowercase, kebab-case) — browsing coordinates, not FQID segments (§5.2)
- Variable slugs auto-derived from the merged SOS variable names (one variable per `(register, variable_name)`, §5.1); TOML overrides where needed
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

- Implement `?period=...` query (plus optional `?variant=...`) on canonical catalog endpoint. **Wire format is a single query-string value per §9.5**, not `deepObject`: int year (`?period=2020`), period-token (`?period=HT2020` / `?period=2020-Q3` / `?period=2020-08`), range (`?period=<from>..<to>`, e.g. `?period=2018..2020`), snapshot sentinel (`?period=_default`); `?variant=<slug>` narrows to one variant. Server canonicalizes and returns 422 on malformed tokens; OpenAPI parameters are plain strings. Polymorphic across all `Source.period` shapes — not year-only.
- Implement `/states`, `/predecessors`, `/successors`, `/related`, `/lineage`, `/lineage_warnings` sub-endpoints + the `/{provider}/{register}/variants` register sub-resource (the variant browser, §9.5 — variants are not FQIDs under the two-level grammar). `/lineage` and `/lineage_warnings` are first-class v1 endpoints, not deferred — `/lineage_warnings` surfaces the `variable_state_lineage_warning` rows emitted by A2.4
- Suffixed + sub-resource routes registered BEFORE the `/api/catalog/{fqid:path}` catch-all in the FastAPI router (router ordering matters; see §9.5 URL routing notes); CI introspection test enforces the order for all seven
- ETag scheme verified to include the `?period` (and `?variant`) query in the cache key
- Cloudflare edge-cache validation gate: small load test through Cloudflare confirms slash-bearing FQID paths still work cleanly with the new shapes
- **Server-side input-validation gates** (§16): (a) `?period=` / `?variant=` parsers are an allow-list — malformed values (SQL-injection probes, path-traversal probes, embedded NULs) return 422 with zero SQL executed (verified via SQLite trace hook); (b) per-segment FQID grammar check rejects `.`, `..`, `%`-encoded variants, and any non-slug-grammar input with 422 and no DB hit. Parametrized tests cover both.

**Estimate**: 4-5 days.

### [ ] A5.3 — SPA TypeScript regen

- OpenAPI codegen against new Pydantic models
- SPA components updated for 3-seg binding FQIDs + the variant browser (variant axis presented as a register sub-resource, not an FQID path)
- New sub-endpoint integrations (states picker keyed by variant × period, replaced-by remediation, related-to siblings picker)
- Multi-vintage `{states: [...]}` rendering on the canonical `?period=` response: when the list has length > 1 (cross-variant, or LKF-shape true multi-vintage), SPA shows a variant / edition picker; for the multi-vintage case keyed by `value_set_version_label`, on selection it refetches with `&value_set_version=<label>` (or `&variant=<slug>`) to narrow to a single state. No 409 path — the uniform list contract makes multi-vintage just length-N rather than an error.

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
                                                  └──→ A2.3 ───────────┴──→ A2.5 ──→ A2.6 ──→ A2.7
                                                                                               │
                                                                                               ├──→ A3.1 ──→ {A3.2, A3.3, A3.4}
                                                                                               │
                                                                                               ├──→ A4.1 ──→ A4.2 ──→ A4.3 ──→ A4.4 ──→ A4.5
                                                                                               │
                                                                                               └──→ A5.1 ──→ {A5.2, A5.3, A5.4}
                                                                                                     (after A3.1 lands; A4 not required)
```

Reading notes: braces `{...}` group steps that can run in parallel
once their shared predecessor lands. **A2.1.5 (respec) is the hinge
after A2.1** — it lands the two-level tables (`variable`,
re-parented `variable_state` with `register_variant_id`) + stored variable slug,
which A2.2/A2.3/A2.4/A2.5 all build on. The maintainer red-line
established that A2.2 and A2.4 have a **hard structural dependency** on
these tables (you can't mint a sibling variable or descend the variable
hierarchy on the A2.1 `(register_id, var_id)` schema), so the
restructure can no longer be deferred into A2.6 — it is its own gating
stage. This grows A2 to **8 PRs** (no longer 7). After A2.1.5, two branches
open in parallel: A2.2→A2.4 (triage into distinct variables → lineage
join over variable-grain `same_as`) and A2.3 (reads `timeseries_event`,
emits the variable-grain `variable_replaced_by` edges). **A2.5 — the
longitudinal catalog API — is the join point after them, not a parallel
branch:** its accessors read every edge table (`related` ←
`variable_related_to` from A2.2; `predecessors`/`successors` ←
`variable_replaced_by` from A2.3; `lineage`/`lineage_warnings` ←
`variable_state_lineage` from A2.4), so shipping it before any of those
would hit `no such table`. A2.5 therefore gates on A2.2 + A2.3 + A2.4.
A2.6 follows A2.5 (and thus all three transitively) and carries only the
FQID-grammar flip + resolver flip + the resolver-side `same_as`
start-node update — the table work is already done in A2.1.5. So A2.3
**is** on the path to A2.6 (A2.5's `predecessors`/`successors` read its
table); an earlier draft wrongly called it independent.

In-flight PRs #131 (A2.3), #132 (A2.2), #133 (early stored-slug), #134
(three-level respec) all predate the two-level respec. **#134 is
superseded by this respec** (the variant level it kept does not carry
identity — §5.0.1; recommend close). **#133 is superseded by A2.1.5**
(its slug-on-`variable_state` + all-eras-agree assertion is the wrong
placement — the slug lives on `variable`, register-unique; the
salvageable `.auto.toml` / grow-only plumbing folds into A2.1.5).
**#132 reworks** onto A2.2 (siblings become distinct variables) and now
**depends on A2.1.5** for the table to write them to. **#131 survives**
modulo the variable-grain adjustment (drop its `*_variant` columns),
rebasing onto A2.1.5's tables. See the respec PR body for the
close-or-rework recommendation.

## Effort estimate

| Stage | PRs | Cumulative effort (single maintainer) |
|---|---|---|
| A0 | 3 | ~5-9 days |
| A1 | 3 | ~10-14 days (parallelizable) |
| A2 | 8 | ~36-50 days (+A2.1.5 two-level table restructure, 4-6 days; A2.6 stays 7-10 now that the table work moved out — maintainer red-line pulled the restructure into its own gating stage) |
| A3 | 4 | ~12-16 days (parallelizable after A3.1) |
| A4 | 5 | ~20-28 days |
| A5 | 4 | ~14-18 days |
| **Total** | **27 PRs (incl. parallelism)** | **~97-135 days** |

Numbers are **person-day sums of the sub-step ranges**, not calendar days — A1's three PRs are independent, so calendar time at that stage is `max(5-7, 2-3, 3-4) = 5-7 days`, while the table reports the 10-14 sum. The 8-12-week calendar estimate below is the parallelism-discounted figure for a single maintainer; mixing the two would double-count the savings.

With parallelism across stages where dependencies allow, calendar time is closer to **8-12 weeks** for a single maintainer focused on this work.

## Risk register

1. **A2.2 build-time triage backlog larger than estimated.** If 200-300 manual TOML curations turns out to be 600-900, A2.2 lengthens. Mitigation: empirical sample from current SCB DB shows 99% auto-handle rate; risk is bounded.
2. **A2.4 source-variant heuristic doesn't fit some real consumer-source pairs.** Mitigation: warning + TOML override mechanism captures the cases the heuristic misses; ~50 manual overrides expected.
3. **A2.6 FQID grammar change breaks downstream tools we haven't catalogued.** Mitigation: search for `register_version` and multi-seg FQID patterns across the monorepo before A2.6. The two-level respec makes the binding **3-seg**, so the grep must catch the shipped v0.11 5-seg shape (anything with ≥3 slug segments after the provider; the Model-A 4-seg form was specced but never implemented). The grep is slug-grammar-aware to avoid swamping the signal with paths/URLs/JSON pointers:

    ```bash
    # ≥3-seg slug paths per §5.2 (allow _default and v0.11 _YYYY period slugs) —
    # catches the shipped v0.11 5-seg binding FQIDs that flip to 3-seg.
    rg "[a-z][a-z0-9-]*(/(_default|_[0-9]+|[a-z][a-z0-9-]*)){3,}" --type py --type md --type toml

    # Or scope to known FQID call sites:
    rg "Catalog\.(resolve|resolve_at|states|predecessors|successors|related|replaced|lineage|lineage_warnings)\("

    # SQL column references for the same_as variant + period drop + grain demotion,
    # register_version table drop, and via_source_id removal — the string-literal FQID
    # grep above misses raw SQL. Also catch the variant-slot drop.
    rg "(a_period|b_period|a_variant|b_variant|variable_same_as|register_version_id|via_source_id|register_version)" --type py --type sql
    ```

    Combine all three passes — the first finds string-literal FQIDs in fixtures/configs; the second finds programmatic callsites that may construct FQIDs at runtime; the third finds hardcoded column names in SQL queries that the FQID greps would skip.
4. **A4.3 SOS adapter discovers workbook-shape variations we haven't covered.** Mitigation: process all 13 workbooks against the adapter before merging; failures surface as `IRWarning` rows. New workbook variations would extend the adapter, not gate it.
5. **A5 webapp work lags A3 by several weeks.** Mitigation: webapp can adopt incrementally; some endpoints can land before others. The hard reject on v0.x SPA files is the only end-of-stage gate.

## What lands as soon as A5.4 ships

- v1.0 reg_meta with SCB + SOS (if A4 ran in parallel) or SCB-only (if A4 deferred)
- v1.0 reg_schema (Pydantic, Model A Source shape)
- v1.0 reg_monabundle (3-seg variable FQID, LoadedSpec mediation)
- v1.0 reg_webapp (Pydantic, new API surface)
- v1.0 SPA (no v0.x file support; Model A only)
- v1.0 `global` deployment ready to host

UNFROZEN sentinel deletion happens at v1 *public release* (not at v1.0.0 internal tag) — give the v1.0.0 build a curation polish window first.

---

## v0.x → Model A rework map

| v0.x PRs | Shipped under v0.x | What survives Model A | What gets redone | Model A stage |
|---|---|---|---|---|
| #78–#82, #85–#87, #89, #104, #112 | reg_meta v0.11 FQID rebuild — 5-seg binding FQID, same_as edges, slug TOMLs (~1,264 entries), §5.8 cross-edition traversal | Provider / register / classification slug curation; variant slugs (now a browsing coordinate, not an FQID segment); variable slug curation (was variable slug); consumer-side binding variable; FTS layer | 5-seg FQID grammar → **3-seg** (period **and** variant slots dropped — variant is a delivery coordinate, §5.0.1); `variable` promoted (synthetic PK + register-unique slug) + `variable_state` re-parented with a `register_variant_id`; ~1,264 `register_version` slug entries deleted; `variable_same_as` demoted to variable grain (`*_variant` + `*_period` columns dropped; the var_id `(N choose 2)` auto-derive deleted); `register_version` table dropped entirely | A2.6, A2.7 |
| #103, #105, #108 | `reg_meta_build` package carve-out (mechanical split: scaffold, db.py, doc_db.py, CLI split, CI workflow) | Package boundary intact; CLI binaries (`reg-meta`, `reg-meta-build`); test helpers | Column names → English (data values unchanged); SCB-Swedish column renames | A1.1 (rename only) |
| #110, #111, #115 | `reg_schema` v0.x — `ValidationIssue` / `ValidationResult` contract; frozen dataclasses (ProjectData, Source, Column, …); `validate_structural` entrypoint | `ValidationResult` JSON contract concept; 22+ stable issue codes (most unaffected); validate_structural API surface | Dataclasses → Pydantic v2; `Source.register_version` → `register_variant + period`; `columns` → `bindings`; `Column.name` → `variable`; 1 issue code renamed + 7 new codes (per A3.1: `invalid_period`, `period_outside_state_validity`, `binding_state_drifts_within_period`, `binding_value_set_version_ambiguous`, `binding_value_set_version_mismatch`, `variable_replaced`, `panel_inheritance_unresolvable`); bump to `schema_version: "2.0.0"` | A3.1 |
| #116 | `mock_data_wizard` adopts `project_data.json` (config rename, fixture corpus rewrite for v0.11 5-seg shape) | Config-rename machinery; fixture-rewrite tooling; v0.x test fixtures' overall structure | Source schema break propagated through mdw; fixture corpus rewritten again to Model A shape; `_build_source` reads new fields | A3.2 |
| #113 | Shared validator test corpus — `reg_schema/test_corpus/` with 4 well-formed + 1 negative cases; harness with drift protection | Corpus discovery + harness machinery | All 5 cases rewritten: source becomes 3-part `register_variant` coordinate + `period`; bindings replace columns; binding + namespaced-block keys 3-seg | A3.1 (paired with reg_schema migration) |
| #120, #121, #122, #123, #124, #125 | `reg_monabundle` carve-out — phases 1, 2a, 2b, 2c, 3 + follow-ups (scaffold, validator relocation, bundle builder, PII scanner, runtime modules + LoadedSpec, 1 MB bundle-size gate) | Survives entirely; this is the Stage A0 work | (none) — A0 complete | A0 ✅ |

Conventions: stage IDs reference the sections above. PR links (`[#NNN]`) resolve against the project's GitHub repo.
