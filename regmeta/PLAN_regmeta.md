# PLAN: regmeta

Status: Complete
Created: 2026-03-01
Last updated: 2026-03-22
Owner: Research engineering
Linked spec: `SPEC_regmeta.md`
Domain model: `STRUCTURE.md`

## 1. Goal

Deliver `regmeta` v1: query commands (`search`, `get`, `resolve`) backed by a pre-built SQLite database. Database is built once from CSV exports via `regmeta maintain build-db`. Query commands are read-only against the main DB.

## 2. Prior Art And Code Lineage

An earlier CSV-based CLI prototype (`catalog.py`, `service.py`, old `cli.py`) read CSV files at query time with auto-detected delimiters. This was replaced on 2026-03-12 in favor of the SQLite architecture described in the SPEC. The old code was deleted.

A `schema_prototype.py` analysis tool was used during discovery to profile the SCB data, prove out normalization logic, and document anomalies (alias, context, cross-register VarId). Its findings are captured in `STRUCTURE.md` and `docs/discovery/`. The normalization logic was extracted and generalized into `db.py`. The prototype and its tests were deleted on 2026-03-13 — the findings persist in documentation, not code.

## 3. Key Decisions

### 2026-03-12

1. **SQLite backend.** No CSV parsing at query time.
2. **`maintain` namespace.** Setup/maintenance commands under `regmeta maintain`, shown only in `regmeta maintain --help`.
3. **Full import.** `build-db` imports all registers and rows. No slicing or filtering.
4. **Alias-aware design.** `variable_alias` table preserves all known column names per CVID. Column names live only in `variable_alias`, not on `variable_instance`.
5. **Batch resolve by default.** Resolve takes a list of column names, returns results for each.
6. **Rebuild, not migrate.** `build-db` replaces the main DB. No schema migration.
7. **Performance from day one.** Stream large files (no in-memory bulk collections), use indexed lookups. Not an afterthought.
8. **Agent-first design.** Primary consumers are LLM agents and other tools. JSON is the primary output. Structured errors for programmatic branching. Core functions importable as Python library. See SPEC §8.

### 2026-03-14: CLI Redesign

9. **Fuzzy register lookup everywhere.** All commands accepting a register accept name or ID. Resolution: exact ID → exact name → substring match. Shared helper `_resolve_register_ids`.
10. **`get schema` restructured.** Accepts `regvar_id` or `--register`, with `--years` filter. Output organized by variant → version → columns. Primary command for `mock_data_wizard`.
11. **`get varinfo` replaces `get variable`.** Accepts variable name or var_id with optional `--register` filter. Returns full variable deep dive with instance history.
12. **`resolve` simplified to exact lookup.** No FTS fallback, no confidence scoring, no ambiguity classification. `--register` acts as a hard filter, not a hint. Removed: `--file-name`, `--register-hint`, `--top-k`, `--min-confidence`.
13. **`search` gains `--register` filter.** FTS results can be scoped to a specific register.
14. **Contract version bumped to 2.0.0.**

### 2026-03-20: Temporal validity

15. **`value_item_validity` table.** SCB provided `VardemangderValidDates.csv` mapping ItemId to validity date ranges. Items absent from the file have no temporal restriction (always valid). Schema version bumped to 1.1.0.
16. **`get values --valid-at` filter.** ISO date input, validated at CLI boundary.

### 2026-03-22: Storage optimization (schema 2.0.0)

17. **INTEGER IDs.** All ID columns (`register_id`, `regvar_id`, `regver_id`, `var_id`, `cvid`, `code_id`, `item_id`) stored as `INTEGER` instead of `TEXT`. Reduces storage and enables faster comparison.
18. **Content-synced FTS5.** Both `register_fts` and `variable_fts` use `content=` and `content_rowid=` to avoid storing text twice. Requires explicit `rowid` in FTS INSERT statements since the content tables use `INTEGER PRIMARY KEY` (rowid alias).
19. **Value code deduplication.** `value_code` table deduplicates (vardekod, vardebenamning) pairs across all CVIDs. `cvid_value_code` is a `WITHOUT ROWID` junction with PK(cvid, code_id).
20. **Sparse validity tracking.** `value_item` stores (cvid, code_id, item_id) only for items that have validity date records in `VardemangderValidDates.csv`. `WITHOUT ROWID` with PK(cvid, code_id, item_id) — PK order supports temporal query lookups. Codes with no `value_item` entry are treated as always valid.
21. **`code_variable_map` summary table.** Pre-aggregated (code_id, register_id, var_id) for `search --value` queries. Replaces a 662MB secondary index on the 51.7M-row junction with a 90MB `WITHOUT ROWID` table of 3.8M rows.

## 4. V1 Command Set

Top-level: `search`, `get`, `resolve`
Get subcommands: `register`, `schema`, `varinfo`, `values`, `datacolumns`, `coded-variables`
Maintain: `build-db`, `info`

## 5. Testing And Cleanup Policy

Tests are written **after each phase is complete**, not during implementation. Each phase gate includes:

1. **Write tests for the completed phase.** Tests lock in the behavior of implemented features so they don't drift or regress. Test against realistic fixtures that exercise the actual edge cases (alias anomalies, context anomalies, cross-register ambiguity, cp1252 encoding, error paths).
2. **Review and clean up prior phases.** Look at code and tests from earlier phases. Delete dead code, simplify over-abstractions, remove tests that no longer test the right thing, consolidate duplicated logic.
3. **No premature tests.** Don't write tests for code that hasn't stabilized. Tests for in-flux code cost more to maintain than they save.

## 6. Work Plan

### Phase 1: Schema And Build-Db

Objective: `regmeta maintain build-db --csv-dir SCB-data/` produces a normalized SQLite database.

The schema follows `STRUCTURE.md` and is specified in `SPEC_regmeta.md` §3.3. Column mappings from CSV headers to DB columns are implemented in `db.py` (`_import_registerinformation` and the per-file import functions).

#### Tasks

- [x] Define DDL (CREATE TABLE + FTS5 + indexes) as embedded SQL. → `db.py:DDL`
- [x] CSV reader for SCB format: pipe-delimited, cp1252, encoding validation. → `db.py:_open_scb_csv`, `_decode_cp1252`
- [x] Normalization from Registerinformation.csv → core tables (register, variant, version, population, object_type, variable, instance, alias, context). → `db.py:_import_registerinformation`
- [x] Enrichment from UnikaRegisterOchVariabler, Identifierare, Timeseries. → `db.py:_import_unika`, `_import_identifierare`, `_import_timeseries`
- [x] Value-item import from Vardemangder.csv (102M rows — batch inserts, WAL mode, progress to stderr). → `db.py:_import_vardemangder`
- [x] Value-item validity dates from VardemangderValidDates.csv. → `db.py:_import_vardemangder_valid_dates`
- [x] FTS5 index population. → `db.py:_populate_fts`
- [x] Import manifest (checksums, row counts, schema version). → `db.py:build_db`
- [x] CLI wiring: `maintain build-db --csv-dir <PATH> [--db <PATH>]`. → `cli.py:_cmd_maintain_build_db`
- [x] CLI wiring: `maintain info [--db <PATH>]`. → `cli.py:_cmd_maintain_info`
- [x] Atomic replace: build to temp file, rename on success. → `db.py:build_db`

#### Exit Criteria

- [x] Full database builds from real `SCB-data/`.
- [x] Row counts match source files.

#### Smoke Test Results (2026-03-14)

- Build completed in ~17 min (996K backbone rows, 102M value-item rows).
- Three bugs found and fixed:
  - cp1252 bytes 0x8F/0x90/0x9D (DOS cp850 remnants) rejected the Vardemangder import. Fixed: mapped to Å/É/Ø.
  - `maintain info` took 33s due to `COUNT(*)` on 96M rows. Fixed: switched to `MAX(rowid)`.
  - Duplicate `utc_now()` function in both `cli.py` and `db.py`. Fixed: single definition in `db.py`.

#### Phase Gate: Tests And Cleanup

- [x] Tests for build-db pipeline: CSV parsing (cp1252, cp850 fixup, header validation), normalization (deduplication, hierarchy integrity, alias/context anomalies), enrichment joins, FTS population, manifest correctness, atomic replace, error paths. → `test_build_db.py` (22 tests)
- [x] Review and clean up Phase 1 code.

### Phase 2: Query Commands

Objective: Expose `search`, `get`, and `resolve` against the built database.

#### Tasks

- [x] DB connection layer: open, read-only mode, error if missing DB. → `db.py:open_db`
- [x] Shared register lookup helper (ID → name → substring). → `queries.py:resolve_register_ids`
- [x] `search` — FTS across registers and variables, type filter, register filter, pagination. → `cli.py:_cmd_search`
- [x] `get register <name_or_id>` — register with variants, fuzzy matched. → `cli.py:_cmd_get_register`
- [x] `get schema` — variant → version → columns, with `--register` and `--years` filters. → `cli.py:_cmd_get_schema`
- [x] `get varinfo <name_or_id>` — variable deep dive with instance history. → `cli.py:_cmd_get_varinfo`
- [x] `get values <cvid>` — value-set members, with `--valid-at` temporal filter. → `cli.py:_cmd_get_values`
- [x] `get datacolumns <variable>` — all column aliases across registers/versions. → `cli.py:_cmd_get_datacolumns`
- [x] `get coded-variables` — variables with value sets, ranked by usage. → `cli.py:_cmd_get_coded_variables`
- [x] `resolve` — exact alias lookup, `--register` as hard filter, batch input. → `cli.py:_cmd_resolve`
- [x] JSON envelope. → `cli.py:_success_envelope`
- [x] Table formatter. → `cli.py:_write_table`, `_write_table_from_payload`
- [x] Common flags: `--db`, `--format`, `--output`. → `cli.py:_build_parser`

#### Removed (2026-03-14 redesign)

- `get variable <register_id> <var_id>` — replaced by `get varinfo`.
- Resolve FTS fallback, confidence scoring, ambiguity classification, `--register-hint`, `--top-k`, `--min-confidence`, `--file-name`.

#### Exit Criteria

- [x] All queries work against real DB and return deterministic results.

#### Smoke Test Results (2026-03-14)

- All commands verified against production DB (238 registers, 42K variables, 515K instances, 96M value items).
- Resolve dedup bug found and fixed: same `(register_id, var_id)` appeared multiple times per CVID. Added `GROUP BY`.
- `resolve --columns Kon --register LISA` → 1 match (correct). `resolve --columns Kon` → 97 matches (correct).
- `get schema --register LISA --years 2015` → 6 variants, 23ms.
- `get varinfo "Arbetssökande i november" --register LISA` → 26 instances across 1998-2023.

#### Phase Gate: Tests And Cleanup

- [x] Tests for all query commands: search, get (all subcommands), resolve, error model, envelope, table format. → `test_commands.py` (82 tests)
- [x] Review and clean up Phase 1 + Phase 2 code.

### Phase 3: Release Readiness

- [x] Table formatter implemented. → `cli.py:_write_table`, `_write_table_from_payload`
- [x] Reference table imports. → `db.py:_import_tabelldefinitioner`, `_import_id_kolumner`
- [x] Known limitations documented. → SPEC §6.4, README.

#### Phase Gate: Tests And Cleanup

- [x] Full regression suite: 82 command tests + 22 build-db tests = 104 total.
- [x] Final code review and cleanup.

### Phase 4: V2 Features — Temporal Diff + Cross-Register Lineage

Objective: Two new query commands (`get diff`, `get lineage`) per `SPEC_regmeta_v2.md`.

#### Tasks

- [x] `get diff` — temporal schema comparison between two years. → `queries.py:get_diff`, `cli.py:_cmd_get_diff`
- [x] `get lineage` — cross-register variable provenance. → `queries.py:get_lineage`, `cli.py:_cmd_get_lineage`
- [x] Variable resolution by var_id, name, or alias for both commands.
- [x] Output formats: table, list, JSON (shared with v1 commands).
- [x] Contract version bumped to 3.0.0.

#### Phase Gate: Tests And Cleanup

- [x] Tests for `get diff` (9 tests) and `get lineage` (9 tests). → `test_commands.py`
- [x] Output format tests (table, list, JSON). → `test_commands.py`
- [x] Full regression suite: 102 tests total.

### Phase 5: Storage Optimization

Objective: Reduce database size from ~13GB to ~1.6GB without losing functionality or query speed.

#### Tasks

- [x] TEXT→INTEGER for all ID columns. → `db.py:DDL`, `_import_registerinformation`, `_import_identifierare`
- [x] Content-synced FTS5 with explicit rowid. → `db.py:_populate_fts`, DDL
- [x] `_try_int()` helper for CLI string→INTEGER coercion. → `queries.py`
- [x] Value code deduplication via `value_code` + `cvid_value_code` WITHOUT ROWID junction. → `db.py:_import_vardemangder`
- [x] Sparse `value_item` WITHOUT ROWID PK(cvid, code_id, item_id). → `db.py:DDL`, `_import_vardemangder`
- [x] `code_variable_map` summary table, drop `idx_cvid_value_code_code`. → `db.py:DDL`, `build_db`, `queries.py:_search_values`
- [x] Drop unused `idx_value_item_item` index.

#### Phase Gate: Tests And Cleanup

- [x] Tests for `value_item` population, `code_variable_map` population, multi-item temporal gaps, empty ItemId handling, search --value via summary table. → `test_build_db.py`, `test_commands.py`
- [x] Full regression suite: 108 tests total.
- [x] SPEC and STRUCTURE docs updated to reflect new schema.

## 7. Risks

| Risk | Mitigation |
|---|---|
| Vardemangder volume (102M rows) | Stream from CSV reader — no in-memory collections for large files. Batch inserts in transactions, WAL mode, progress reporting. Verified: ~17 min build time. |
| cp1252 dirty bytes | DOS cp850 remnants (0x8F, 0x90, 0x9D) mapped to Å/É/Ø during import. ~2700 occurrences in Vardemangder. |
| CVID alias/context anomalies | Handled by alias + context tables by design |
| Scope creep into live API | Hard v1 boundary in spec |
| Value sets not version-specific | Resolved. SCB provided `VardemangderValidDates.csv` (2026-03). Integrated into `build-db` as `value_item_validity` table. `get values --valid-at` filters by date. |
| Database size (13GB raw CSV → SQLite) | Resolved. INTEGER IDs, value code dedup, WITHOUT ROWID tables, content-synced FTS, summary tables. Final DB ~1.6GB (compresses to ~30MB). |
