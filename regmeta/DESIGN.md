# Design: regmeta

Design rationale and constraints. For usage, see `regmeta --help`.
For the domain model, see [STRUCTURE.md](STRUCTURE.md).

## Agent-first design

The primary consumers are LLM agent skills and `mock_data_wizard`.
Human terminal use is supported but secondary. This drives several choices:

- Three output formats: table (default), list, and JSON for machine consumption
- All output follows a stable envelope contract (version, timing, request echo)
- Errors are structured with codes, not just messages
- Exit codes are meaningful (see below)
- Core query functions are importable as a Python library, not just CLI

## SQLite backend

All metadata lives in a single SQLite file (~1.6 GB). Chosen because:

- Zero-dependency deployment (Python stdlib)
- Single-file distribution via GitHub Releases + zstd compression
- FTS5 built in
- Read performance is excellent for this workload

The database is read-only from the perspective of query commands.
`maintain build-db` replaces it entirely (not incremental).

## CSV import and encoding

SCB exports are pipe-delimited, cp1252 encoded. Several bytes in the
exports are actually DOS cp850 remnants undefined in cp1252:

| Byte | cp850 | Mapped to |
|------|-------|-----------|
| 0x81 | ü     | ü         |
| 0x8D | ì     | ì         |
| 0x8F | Å     | Å         |
| 0x90 | É     | É         |
| 0x9D | Ø     | Ø         |

These are mapped during import. The build reads ~1M backbone rows
from `Registerinformation.csv` and ~102M value-item rows from
`Vardemangder.csv`.

## FTS5 configuration

Two content-synced FTS5 indexes:

- **`register_fts`** — indexes register name, rubrik, syfte
- **`variable_fts`** — indexes variable name, definition, beskrivning.
  Uses `unicode61` tokenizer for correct Swedish character handling.
  Column names (`kolumnnamn`) are deliberately excluded — they contain
  technical suffixes (e.g. `_LISA`) that pollute search results.
  Column name matching is handled by `resolve` instead.

## Register lookup strategy

All commands accepting a register argument use a three-step resolution:

1. Exact ID match
2. Case-insensitive exact name match
3. Case-insensitive substring match

This allows `34`, `LISA`, and `utbildning` to all work.

## Resolve: exact match only

`resolve` performs exact alias lookup against `variable_alias.kolumnnamn`.
No FTS fallback, no confidence scoring. Status is `matched` or `no_match`.
This is intentional — resolve is for mapping known column headers, not
discovery.

## Value sets are not version-specific

The Värdemängder export attaches a flat historical union of all code
definitions to every CVID, regardless of year. When a code's meaning
changes between years, both definitions appear. Temporal filtering via
`get values --valid-at <date>` uses supplementary validity date ranges
from `VardemangderValidDates.csv`.

## Storage optimization

IDs stored as INTEGER (not TEXT). Tables with composite integer-only PKs
use WITHOUT ROWID. Value codes are deduplicated into `value_code` +
`cvid_value_code` junction. A pre-aggregated `code_variable_map` replaces
large secondary indexes for value search queries. These brought the
database from ~13 GB to ~1.6 GB.

## Documentation layer

Register documentation (parsed from SCB PDFs) is stored as Obsidian-compatible
markdown files with YAML frontmatter, indexed into a separate FTS5 database
(`regmeta_docs.db`). This has a separate lifecycle from the metadata DB — `build-db`
does not touch it, `maintain build-docs` rebuilds it independently. Docs are
keyed to register and variable names, not database IDs, to allow independent
updates.

See [docs/SCHEMA.md](docs/SCHEMA.md) for the markdown file format.

## Exit codes

| Code | Meaning |
|------|---------|
| 0    | Success |
| 2    | Usage/argument error |
| 10   | Configuration error (missing DB, bad encoding) |
| 16   | Not found |
| 17   | No match with `--require-match` |
| 25   | Network error (`maintain download`) |
| 30   | Unexpected internal error |

## Determinism

- Stable ordering for repeated runs against the same database
- Stable JSON key ordering
- Deterministic paging (offset, limit)

## Security

- Metadata only — no microdata
- No credentials read or stored
- No outbound network requests (except `maintain download`)

## Explored and ruled out

- **Direct API integration** against `mikrometadata.scb.se` — no stable
  public API. Session-bound WebSocket with no documented contract.
- **Browser automation** — fragile, unrepeatable. Manual CSV export is
  more reliable.
- **Query caching / user adaptation database** — deferred. Not needed yet.
