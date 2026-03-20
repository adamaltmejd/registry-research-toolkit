# SPEC: regmeta (Product PRD)

Status: Draft for sign-off (freeze after approval)
Version: 2.0.0
Created: 2026-03-01
Last updated: 2026-03-14
Owner: Research engineering
Product type: Python CLI package distributed for `uv`/`uvx`

**Maturity: pre-release, zero users.** There is no deployed version and no backwards compatibility obligation. No migration code, shims, or deprecation wrappers. If something needs to change, change it directly and delete what is no longer needed.

## 1. Product Summary

### 1.1 Product Name
`regmeta` (module), CLI executable `regmeta`.

### 1.2 Purpose
Deterministic CLI for searching and resolving SCB registry metadata from a local SQLite database. Designed primarily for programmatic consumption by LLM agent skills and other tools; direct human use is secondary.

### 1.3 V1 Decision
V1 is local-only. No live network calls to SCB services.

1. User exports CSV files from `https://mikrometadata.scb.se/`.
2. User runs `regmeta maintain build-db` once to build the local database.
3. All query commands run against the database.

### 1.4 Integration Model
1. Primary consumers are LLM agent skills, `mock_data_wizard`, and other tools in the research toolkit.
2. Direct human CLI use is supported but not the primary interface.
3. Output contracts are stable, versioned, and machine-parseable.

## 2. Scope

### 2.1 In Scope (v1)
1. Import SCB metadata CSV exports into a normalized SQLite database.
2. Full-text search over registers and variables, filterable by register.
3. Lookup registers by name or ID (fuzzy matched).
4. Schema retrieval by register or variant, filterable by year range.
5. Variable deep dive by name or ID, filterable by register.
6. Resolve column names to variables via exact alias lookup (batch).
7. Value-set retrieval by CVID.
8. Deterministic JSON output with stable exit codes.
9. Optional table output for terminal usage.

### 2.2 Out Of Scope (v1)
1. Row-level microdata processing.
2. Query database for caching/adaptation (deferred to v2).

### 2.3 Deferred To V2
1. Query database for caching and user adaptation.
2. Sensitivity/identifier flag queries.

### 2.4 Explored And Ruled Out
The following were investigated during discovery (see `regmeta/docs/discovery/`) and will not be pursued:

1. **Direct API integration against `mikrometadata.scb.se`.** The service has no stable public API. Transport analysis showed session-bound WebSocket communication with no documented contract. Not viable for automated access.
2. **Browser automation / session replay / UI simulation.** Explored as a fallback for API access. Fragile, unrepeatable, and dependent on undocumented UI state. Manual CSV export is more reliable.
3. **Automatic CSV export/download from provider UI.** Depends on browser automation (see above).

## 3. Architecture

### 3.1 Database

**Main database** (`regmeta.db`): normalized metadata store.
- Written exclusively by `maintain` commands.
- Read-only from the perspective of query commands.
- Rebuilt from source CSVs via `maintain build-db`.
- Schema follows the domain model in `STRUCTURE.md`.

### 3.2 Default Paths
- Default directory: `~/.local/share/regmeta/`
- Override: `--db <PATH>` or `REGMETA_DB` env var.

### 3.3 Main Database Schema

The database normalizes the hierarchy described in `STRUCTURE.md`.

**Core tables** (from `Registerinformation.csv`):

| Table | PK | Role |
|---|---|---|
| `register` | `register_id` | Registry identity and purpose |
| `register_variant` | `regvar_id` | Dataset family within a registry (→ register_id FK) |
| `register_version` | `regver_id` | Time-slice / release of a variant (→ regvar_id FK) |
| `population` | `(regver_id, populationnamn)` | Population scope at a version |
| `object_type` | `(regver_id, objekttypnamn)` | Object type at a version |
| `variable` | `(register_id, var_id)` | Variable meaning and definition |
| `variable_instance` | `cvid` | Concrete occurrence in a version (register_id, regvar_id, regver_id, var_id, datatyp, datalangd) |
| `variable_alias` | `(cvid, kolumnnamn)` | All known column names for an instance |
| `variable_context` | `(cvid, populationnamn, objekttypnamn)` | Population/object-type scope |

Note: column names (`kolumnnamn`) live exclusively in `variable_alias`, not on `variable_instance`.

**Enrichment tables:**

| Table | Source file | Role |
|---|---|---|
| `value_item` | `Vardemangder.csv` | Coded value-set members keyed by CVID |
| `unika_summary` | `UnikaRegisterOchVariabler.csv` | Lifecycle and sensitivity flags |
| `identifier_semantics` | `Identifierare.csv` | Identifier variable definitions |
| `timeseries_event` | `Timeseries.csv` | Structural/semantic change log |

**Reference tables:**

| Table | Source file | Role |
|---|---|---|
| `source_column_type` | `Tabelldefinitioner.sql` | SQL types and constraints per export column |
| `source_join_key` | `ID-kolumner.xlsx` | Join-key semantics between export files |

**Search and metadata:**

| Table | Type |
|---|---|
| `register_fts` | FTS5 over register name, rubrik, syfte |
| `variable_fts` | FTS5 over variable name, definition, beskrivning |
| `import_manifest` | Import date, source checksums, schema version, row counts |

### 3.4 FTS5 Configuration

**`register_fts`:** content-synced with `register` table. Indexes `register_id`, `registernamn`, `registerrubrik`, `registersyfte`. Uses default FTS5 ranking (BM25).

**`variable_fts`:** tokenizer `unicode61`. Indexes `register_id`, `var_id`, `variabelnamn`, `variabeldefinition`, `variabelbeskrivning`. Column names (`kolumnnamn`) are excluded from the FTS index — they contain technical suffixes (e.g. `_LISA`) that pollute search results. Column name matching is handled by `resolve`. Uses default FTS5 ranking (BM25). Stores content internally (not contentless) so column values are available in search results and JOINs.

The `unicode61` tokenizer handles Swedish characters (å, ä, ö) correctly via Unicode case folding and diacritic removal.

### 3.5 Register Lookup

All commands accepting a register argument (`--register` or positional) use a shared resolution strategy:

1. **Exact ID match.** If the input matches a `register_id`, return that register.
2. **Exact name match.** Case-insensitive match against `registernamn`. Returns all matches.
3. **Substring match.** Case-insensitive LIKE match against `registernamn`. Returns all matches.

This allows inputs like `34`, `LISA`, or `utbildning` to all work.

### 3.6 Resolve Strategy

Resolve performs **exact alias lookup only** against `variable_alias.kolumnnamn` (case-insensitive). No FTS fallback, no confidence scoring, no ambiguity classification.

- With `--register`: results are filtered to the specified register(s).
- Without `--register`: all registers with a matching alias are returned.
- Status is `matched` (one or more hits) or `no_match` (zero hits).

Results are deduplicated by `(register_id, var_id)` and sorted by `register_id`, `var_id`.

## 4. User Workflow

1. Export metadata CSV files from `mikrometadata.scb.se`.
2. Run `regmeta maintain build-db --csv-dir <path>` once.
3. Run query commands (`search`, `get`, `resolve`).
4. When metadata is updated upstream, re-export and re-run `maintain build-db`.

## 5. CLI Model

### 5.1 Invocation
`uvx regmeta ...` or `uv run regmeta ...`

### 5.2 Top-Level Help (`regmeta --help`)
```
search      Free-text search across registers and variables
get         Retrieve records by entity type and ID
resolve     Resolve column names to variables (exact alias lookup)
maintain    Setup and maintenance (see: regmeta maintain --help)
```

### 5.3 Maintain Help (`regmeta maintain --help`)
```
build-db    Build database from SCB CSV exports
info        Database stats and import metadata
```
Future: `rebuild-index`, `check-integrity`, `compact`.

### 5.4 Common Query Flags
- `--db <PATH>` — database directory (default: `~/.local/share/regmeta/`, env: `REGMETA_DB`)
- `--format {json,table}` — default `json`
- `--output <PATH>` — write to file instead of stdout

### 5.5 Maintain Flags

`build-db`:
- `--csv-dir <PATH>` — directory containing SCB CSV exports (required)
- `--db <PATH>` — output directory (same default as query commands)

`info`:
- `--db <PATH>` — database directory

### 5.6 Search Flags
- `--query <TEXT>` (required) — free-text search term
- `--type {register,variable,all}` — default `all`
- `--register <NAME_OR_ID>` — filter results to matching register(s)
- `--limit <INT>` — default `50`
- `--offset <INT>` — default `0`

### 5.7 Get Subcommands And Flags

`get register <name_or_id>`:
- Accepts register name or ID (fuzzy matched, see §3.5).
- Single match: returns register fields + array of variants.
- Multiple matches: returns `{"registers": [...]}`.

`get schema [<regvar_id>] [--register <name_or_id>] [--years <range>]`:
- Requires either `regvar_id` or `--register`.
- `--years` filters versions by year (e.g. `2015`, `2010-2015`, `2010-`, `-2015`). Year is extracted from `registerversionnamn` via first 4-digit number.
- Returns `{"variants": [{"regvar_id", "versions": [{"regver_id", "version_name", "year", "columns": [...]}]}]}`.
- Each column entry: `cvid`, `var_id`, `variabelnamn`, `datatyp`, `datalangd`, `aliases`.
- This is the primary command for `mock_data_wizard` — provides exact CVIDs and types per version.

`get varinfo <name_or_var_id> [--register <name_or_id>]`:
- Accepts variable name or var_id. Variable name matched case-insensitively against `variabelnamn`.
- `--register` filters to matching register(s).
- Single match: returns variable fields + `register_name` + `instances` array.
- Multiple matches: returns `{"variables": [...]}`.
- Each instance: `cvid`, `regvar_id`, `variant_name`, `regver_id`, `version_name`, `year`, `datatyp`, `datalangd`, `aliases`, `value_set_count`.

`get values <cvid>`:
- Returns coded value-set members for a variable instance.
- Array of `{vardekod, vardebenamning, vardemangdsversion, vardemangdsniva}`.

### 5.8 Resolve Flags
- `--columns <TEXT>` — comma-separated column names
- `--register <NAME_OR_ID>` — filter to matching register(s). Acts as a hard filter, not a hint.
- `--require-match` — fail with exit 17 when any column has no matches

Input: `--columns` or JSON array of strings on stdin.

## 6. CSV Import

### 6.1 Input Format
SCB exports: pipe-delimited (`|`), cp1252 encoding, quote char `"`.

Bytes undefined in cp1252 but present in SCB data as DOS cp850 remnants (0x81, 0x8D, 0x8F, 0x90, 0x9D) are mapped to their cp850 equivalents (ü, ì, Å, É, Ø) during import.

### 6.2 Input Files
- `Registerinformation.csv` — required (backbone; ~1M rows)
- `UnikaRegisterOchVariabler.csv` — enrichment (lifecycle, sensitivity flags)
- `Identifierare.csv` — enrichment (identifier semantics)
- `Timeseries.csv` — enrichment (change log)
- `Vardemangder.csv` — enrichment (value-set members; ~102M rows)
- `Tabelldefinitioner.sql` — reference (SQL types and constraints for export columns)
- `ID-kolumner.xlsx` — reference (join-key documentation between export files)

### 6.3 Rebuild Semantics
`build-db` replaces the main database entirely. Not incremental.

### 6.4 Known Data Limitations

**Value sets are not version-specific.** The Värdemängder export attaches a flat historical union of all code definitions to every CVID, regardless of which year that CVID represents. When a code's meaning changes between years, both definitions appear on all CVIDs with no temporal binding. See `reports/arbsoknov_report.md` for a detailed case study. Temporal code validity can only be determined from external documentation (Bakgrundsfakta PDFs). Pending clarification from SCB on whether this is intentional or whether temporal data exists in MetaPlus but is not exported (see `reports/draft_email_vardemangder_temporal.md`).

## 7. Data Contract

### 7.1 Success Envelope (JSON)
1. `contract_version` (currently `"2.0.0"`)
2. `generated_at` (UTC RFC3339)
3. `request` (command + effective args)
4. `database` (import date, schema version)
5. `data`
6. `run` (`duration_ms`)

### 7.2 Search Result
1. `total_count` — total matches before pagination
2. `results` — array of records, each with:
   - `type`: `register` | `variable`
   - `register_id`, `register_name`, `register_rubrik` (always present)
   - `var_id`, `variable_name`, `variable_definition` (when type=variable)
   - `fts_rank` — relevance score

### 7.3 Get Result
Returns the requested entity as a JSON object. Shape depends on subcommand:
- `get register`: register fields + array of variants (single match), or `{"registers": [...]}` (multiple matches)
- `get schema`: `{"variants": [{"regvar_id", "versions": [{"columns": [...]}]}]}`
- `get varinfo`: variable fields + instances array (single match), or `{"variables": [...]}` (multiple matches)
- `get values`: array of `{vardekod, vardebenamning, vardemangdsversion, vardemangdsniva}`

### 7.4 Resolve Result
1. `columns` — array, one entry per input column:
   - `column_name`
   - `status`: `matched` | `no_match`
   - `matches`: array of `{var_id, variable_name, matched_column, register_id}`

## 8. Integration Design Rules
1. **Stdout is data, stderr is diagnostics.** JSON output and progress/warnings must never mix on the same stream.
2. **JSON is the primary output path.** Table format is a convenience for human inspection only.
3. **Errors are structured.** Callers branch on `error.code` and exit codes, not message text.
4. **Contract stability.** `contract_version` in every response. Breaking changes require a version bump.
5. **Importable as library.** Core query functions are usable as Python imports, not only through CLI subprocess calls.

## 9. Determinism Rules
1. Stable ordering for repeated runs against the same database.
2. Stable JSON key ordering.
3. Deterministic paging (`offset`, `limit`).
4. Resolve results sorted by `register_id`, `var_id`.

## 10. Error Model And Exit Codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 2 | Usage/argument error |
| 10 | Configuration error (missing DB, missing CSVs, bad encoding) |
| 16 | Not found (get with nonexistent ID/name) |
| 17 | No match with `--require-match` |
| 20 | Output write/serialization error |
| 30 | Unexpected internal error |

Error JSON: `error.code`, `error.class`, `error.message`, `error.remediation`.

## 11. Security And Compliance
1. Metadata only — no microdata.
2. No credentials/tokens read or stored.
3. No outbound network requests.

## 12. Non-Functional Requirements
1. Python ≥ 3.11.
2. Third-party dependencies are fine when they solve real problems (e.g. `openpyxl` for xlsx). Prefer well-maintained libraries over reinventing.
3. Query commands: < 500ms against built database.
4. `build-db`: minutes acceptable for full import; progress to stderr.

## 13. Acceptance Criteria (v1)
1. `maintain build-db` produces a complete normalized SQLite database from all SCB exports.
2. `search` returns FTS-backed deterministic results, filterable by register.
3. `get register` returns register info by name or ID with fuzzy matching.
4. `get schema` returns version-level column listings, filterable by year range.
5. `get varinfo` returns variable deep dive with instance history.
6. `get values` returns value-set members by CVID.
7. `resolve` returns exact alias matches in batch, filterable by register.
8. No network calls required.
9. Re-running `build-db` cleanly replaces the database.

## 14. V2 Placeholder
1. Query database for caching and user adaptation.
2. Sensitivity/identifier flag queries.
3. **Pre-built DB distribution.** The regmeta DB is built from publicly available SCB metadata (no PII). Publish pre-built `regmeta.db` as a GitHub release asset (or LFS). Add `regmeta maintain download-db` command that fetches the latest release to `~/.local/share/regmeta/`. Optionally auto-download on first `open_db` if the DB is missing. This removes the requirement for users to have access to raw SCB CSV exports and run `build-db` themselves.

## 15. V3 Placeholder: Semantic Docs Layer
The SCB metadata export is a structural catalog — it records what exists but not what it means in context. Key information is only available in external documentation (e.g. Bakgrundsfakta PDFs): temporal code validity, sub-category composition, code migration history, and domain-specific interpretation guidance. See `reports/arbsoknov_report.md` for a detailed case study.

A future version may add a curated docs layer: parsed markdown files keyed to `register_id` / `(register_id, var_id)`, searchable alongside the metadata DB. This would support questions like "what is the best registry for X?" or "what's the difference between variable X and Y?".

Design constraints:
1. **Separate lifecycle.** Curated docs must not be destroyed by `build-db` rebuilds. Either a separate DB or separate import step (`maintain build-docs`).
2. **Keyed to existing IDs.** Docs join against `register_id` and `(register_id, var_id)` — no new ID schemes.
3. **Pending SCB response.** If SCB confirms that temporal code validity exists in MetaPlus but is not exported, the right fix may be a better export rather than PDF parsing. Defer design until the export question is resolved.
