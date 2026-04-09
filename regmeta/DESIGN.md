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

## Versioning and compatibility

Three independent version numbers:

| Version | Location | Purpose |
|---------|----------|---------|
| Package version (`__version__`) | `__init__.py`, `pyproject.toml` | Python package / CLI release |
| Schema version (`SCHEMA_VERSION`) | `db.py` | Database schema compatibility |
| Contract version (`CONTRACT_VERSION`) | `cli.py` | CLI output envelope format |

**Schema version** uses semver. The **major** component gates compatibility:
`open_db` compares the major version in the database's `import_manifest`
against the code's `SCHEMA_VERSION`. A mismatch raises `schema_incompatible`
(exit 10) and directs the user to re-download the database.

When making a **breaking schema change** (renamed/removed tables or columns,
changed semantics), bump the `SCHEMA_VERSION` major version in `db.py`. The
`TestSchemaCompat` tests in `test_build_db.py` verify this guard works.

Minor/patch bumps (new tables, new optional columns) are backwards-compatible
and do not trigger rejection.

**Update command**: `maintain update` is the single command that brings
everything current — it runs `uv tool upgrade regmeta` for the package and
downloads a new database if the release includes one. A background version
checker runs once per week (cached in `~/.local/share/regmeta/.update_check`)
and prints a hint on interactive runs when a newer release exists.

**Auto-download on first use**: query commands (`search`, `get`, `resolve`)
prompt to download the database interactively when none is found, so users
don't need to know about `maintain update` on first install.

### Package version format

Package versions follow `X.Y.Z` with two optional pre-release suffixes:

- `X.Y.Z` — final release
- `X.Y.ZaN` — alpha (e.g. `0.5.0a1`)
- `X.Y.Z.devN` — development build (e.g. `0.5.0.dev3`)

No other suffixes (beta, rc, post, epoch) are used. The update checker
relies on this format for version comparison.

## Exit codes

| Code | Meaning |
|------|---------|
| 0    | Success |
| 2    | Usage/argument error |
| 10   | Configuration error (missing DB, bad encoding) |
| 16   | Not found |
| 17   | No match with `--require-match` |
| 25   | Network error (`maintain update`) |
| 30   | Unexpected internal error |

## Determinism

- Stable ordering for repeated runs against the same database
- Stable JSON key ordering
- Deterministic paging (offset, limit)

## Security

- Metadata only — no microdata
- No credentials read or stored
- No outbound network requests (except `maintain update` and the weekly version check)

## Explored and ruled out

- **Direct API integration** against `mikrometadata.scb.se` — no stable
  public API. Session-bound WebSocket with no documented contract.
- **Browser automation** — fragile, unrepeatable. Manual CSV export is
  more reliable.
- **Query caching / user adaptation database** — deferred. Not needed yet.
