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

## Composite registers and source tracking

Registers like LISA, FRIDA, LINDA, and STATIV are composites — most of
their variables originate in source registers (RTB, RAMS, etc.). The
`variable` table tracks this via `source_register_id` (FK to `register`)
and `source_label` (display abbreviation or raw text).

During `build-db`, the `VariabelRegister_Källa` field is resolved using
deterministic matching only — no fuzzy logic:

1. Extract parenthesized abbreviation (e.g. "Befolkningsregistret (RTB)" → RTB)
2. Match text before ` : ` separator against register names
3. Match entire text against register names

Unresolved sources are stored as raw text in `source_label` for human
review. This is surfaced in `get schema` (source column) and `get lineage`
(consumer/source classification).

## Value sets are not version-specific

The Värdemängder export attaches a flat historical union of all code
definitions to every CVID, regardless of year. When a code's meaning
changes between years, both definitions appear. Temporal filtering via
`get values --valid-at <date>` uses supplementary validity date ranges
from `VardemangderValidDates.csv`.

## Classifications

Named code systems (SUN2000, SSYK2012, SNI2007, LKF, ...) are first-class
entities. Each `classification` row carries metadata (publisher, version,
validity range, supersedes link, canonical URL) and a cached `code_count`.
The `classification_code` junction holds the deduplicated union of value
codes that belong to the classification, with an optional `level` integer
for prefix-hierarchy filtering (length of all-digit codes; NULL for
non-numeric codes like ICD letters).

The FK lives on `variable_instance`, not on `variable`. SCB's data model
already places the classification label (`vardemangdsversion`) per
instance, and many headline variables genuinely span multiple
classifications across their lifetime — e.g. `Utbildningsnivå` (var_id 66)
uses SUN 2000 codes through 2018 and SUN 2020 codes from 2019 onwards;
`SSYK` and `SNI` show the same generational drift. Linking at the instance
level keeps each code system distinct (SUN 2000 codes never bleed into
SUN 2020) and lets variable-level helpers aggregate when needed.

The `classification_id` column is populated at build time from a
maintainer-curated TOML seed at `regmeta/classifications.toml`. Each entry
declares a normalized classification and lists the raw
`vardemangdsversion` strings that map to it — exact match, no fuzzy
inference. Match strings are deterministic and auditable: any maintainer
can enumerate them via
`SELECT DISTINCT vardemangdsversion FROM variable_instance`.

Build-time invariants (violations fail `maintain build-db` loudly, exit 10):

- Every seed `vardemangdsversion` string must match at least one instance.
- Every classification must resolve to at least one tagged instance and
  at least one value code.
- A given `vardemangdsversion` string may belong to at most one
  classification.
- Every `supersedes` reference must resolve to a declared `short_name`.
- Every `valid_codes_file`, when present, must resolve to a CSV under the
  classifications directory with header `vardekod,vardebenamning`.

### Canonical vs observed codes

`classification_code.is_valid` distinguishes published canonical codes
from codes that merely show up in the data. SCB's metadata exports
contain plenty of noise (`*`, `***`, `0000`, `[BLANK]`, stray prefix
levels) that has no place in an authoritative code list, but is also
useful to keep around so a researcher seeing one of those values in a
register can look it up.

A seed entry's optional `valid_codes_file` points at a CSV under
`regmeta/input_data/classifications/` (header
`vardekod,vardebenamning`). At build time:

- Every CSV code is ensured to exist in `value_code` (canonical-but-
  unobserved codes get a fresh row with no `cvid_value_code` linkage).
- Every `classification_code` row in that classification is marked
  `is_valid=1` (canonical) or `is_valid=0` (observed-only).
- `classification.valid_code_count` caches the canonical count; it is
  `NULL` for classifications without a CSV.

Without a CSV, every row carries `is_valid=NULL` (validity unknown).
The CLI exposes this via `get classification --codes --only-valid` and
includes `is_valid` per code in JSON output (omitted when NULL).

Hierarchy is intentionally not encoded as `parent_code_id`. The `level`
column captures the most useful filter ("top-level only"); deeper
parent/child queries fall back to prefix matching on `vardekod`. Code
sets without prefix hierarchy (ICD-10, ATC) keep `level = NULL` and use
their own conventions.

The seed lives in the repo (alongside `DESIGN.md`) and is **not** bundled
in the wheel — same status as `regmeta/docs/`. Users receive the
already-populated classification tables via the prebuilt DB asset.

## Storage optimization

IDs stored as INTEGER (not TEXT). Tables with composite integer-only PKs
use WITHOUT ROWID. Value codes are deduplicated into `value_code` +
`cvid_value_code` junction. A pre-aggregated `code_variable_map` replaces
large secondary indexes for value search queries. These brought the
database from ~13 GB to ~1.6 GB.

## Documentation layer

Register documentation (parsed from SCB PDFs) lives as Obsidian-compatible
markdown files under `regmeta/docs/`, source-of-truth for maintainers, and
is indexed into a separate FTS5 database (`regmeta_docs.db`) with its own
`DOC_SCHEMA_VERSION`. Docs are keyed to register and variable names, not
numeric IDs, so doc updates and main-DB updates are independent.

End users never see the markdown files. The doc DB is distributed as a
GitHub Release asset (`regmeta_docs.db.zst`) parallel to the main DB asset,
installed into the same cache dir (`$XDG_DATA_HOME/regmeta/`), and fetched
by `maintain update` alongside the main DB. Query commands (`search`,
`get`, `resolve`, `docs/*`) refuse to run without the doc DB — on first
use the CLI offers to download both artifacts.

`maintain build-docs` is a maintainer-only command that rebuilds the doc
DB from a repo checkout of `regmeta/docs/` before upload. Runtime never
reads markdown — `repo_docs_dir()` in `doc_db.py` is only consulted by
`build-docs` when run from a repo checkout, and is absent in installed
wheels.

See [docs/SCHEMA.md](docs/SCHEMA.md) for the markdown file format.

## Versioning and compatibility

Four independent version numbers:

| Version | Location | Purpose |
|---------|----------|---------|
| Package version (`__version__`) | `__init__.py`, `pyproject.toml` | Python package / CLI release |
| Main schema version (`SCHEMA_VERSION`) | `db.py` | Main-DB schema compatibility |
| Doc schema version (`DOC_SCHEMA_VERSION`) | `doc_db.py` | Doc-DB schema compatibility |
| Contract version (`CONTRACT_VERSION`) | `cli.py` | CLI output envelope format |

**Schema version** uses semver. `open_db` compares the `import_manifest`'s
`schema_version` to the code's `SCHEMA_VERSION`: the major components must
match and the DB's minor must be `>=` the code's minor. A mismatch raises
`schema_incompatible` (exit 10) and directs the user to re-download the
database. Patch differences are ignored.

Bumping rules:

- **Major bump** on breaking changes (renamed/removed tables or columns,
  changed column semantics).
- **Minor bump** when code starts reading a new column/table added in the
  build. This forces old DBs (that lack it) to be rejected cleanly at
  `open_db` instead of failing later with a SQL error.

Either bump requires rebuilding and re-uploading the DB asset before the
package release goes live — see `.agents/skills/release/SKILL.md`. The
`TestSchemaCompat` tests in `test_build_db.py` verify the guard.

### Release tags and distribution

The monorepo uses **per-package release tags**: `regmeta/v0.5.0`,
`mock-data-wizard/v0.4.0`, etc.  Each tag corresponds to a GitHub release
scoped to that package.

| Channel | Trigger | What it distributes |
|---------|---------|---------------------|
| PyPI | `publish_regmeta.yml` on `regmeta/v*` release | Python package (wheel + sdist) |
| GitHub Release asset | Manual upload to the same release | Pre-built main DB (`regmeta.db.zst`) |
| GitHub Release asset | Manual upload to the same release | Pre-built doc DB (`regmeta_docs.db.zst`) |

Both DB assets are **optional** per release. A package release only needs a
new main DB when `SCHEMA_VERSION` changes, and only needs a new doc DB when
`DOC_SCHEMA_VERSION` changes or `regmeta/docs/` content changes.
`resolve_latest_release()` walks recent releases backwards looking for each
asset independently, so a doc-less or DB-less package release does not
orphan older assets. The publish workflow's smoke step exercises
`maintain update --force` before allowing PyPI publish, so a release that
breaks the walker (e.g. incompatible assets, or no resolvable asset at all)
fails CI instead of shipping.

The wheel contains Python source only. The markdown under `regmeta/docs/`
is maintainer source-of-truth and is **not** bundled — end users receive
the built doc DB via `maintain update`.

Legacy bare `v*` tags (pre-0.6.0) are still recognized during the transition
but new releases must use the `regmeta/v*` prefix.

**Update command**: `maintain update` is the single command that brings
everything current — it runs `uv tool upgrade regmeta` for the package and
walks releases to find the latest main-DB and doc-DB assets. Already-current
assets are skipped (tracked via `.db_source` and `.docs_source` in the cache
dir). A background version checker runs once per week (cached in
`~/.local/share/regmeta/.update_check`) and prints a hint on interactive
runs when a newer release exists.

**Auto-download on first use**: query commands (`search`, `get`, `resolve`,
`docs/*`) prompt to download whichever artifacts are missing when invoked
interactively, so users don't need to know about `maintain update` on first
install. Non-interactive invocations fail with structured errors
(`db_not_found`, `doc_db_not_found`) rather than silently skipping.

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
