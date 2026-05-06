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

## Data providers

regmeta is provider-agnostic at the query layer: one metadata DB, one
docs DB, one CLI. Users searching or resolving variables shouldn't need
to know which agency published a given register.

Provider-specific logic lives under `regmeta/src/regmeta/sources/`:

- `sources/sos.py` — parses Socialstyrelsen register metadata Excel
  deliveries (`.xlsx` per register). Returns `SosRegister` dataclasses.
  Isolated here so the quirks of that format (sheet-name variance,
  "metadatat"-typo section headings, phantom row counts, non-standard
  kodlistor) don't leak into schema or query code.
- SCB CSV import currently lives in `db.py::build_db`. Moving it under
  `sources/scb.py` with the same intermediate-representation pattern is
  tracked as a follow-up — non-blocking for additional providers.

The build-db step consumes each provider's parser and maps into the
unified schema. Providers share `register`, `variable`, `value_code`,
etc.; provider-specific fields live on optional columns or in
enrichment tables when they have no shared analogue. Adding a new
provider means writing a parser in `sources/` and a mapping step in
`build-db`; no query-layer or CLI changes should be needed.

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

## Vardemängder sentinel filtering

`Vardemangder.csv` ships a row for every variable, including those with no
enumerated code list. SCB encodes "no codes" by stuffing a placeholder string
into `Värdekod` so that `Värdekod == Värdemängdsversion` (and typically
`Värdemängdsnivå`). Two disjoint cases occur with this shape, classified by
two allowlists in `db.py`:

`_VARDEMANGDER_SENTINELS` — placeholder strings that mean "no enumerated
code list." Not real value codes; dropped silently.

| Värdekod | Meaning |
|---|---|
| `Tal` | Numeric variable |
| `Beskrivande text` | Free-form text variable |

Importing sentinels would pollute `value_code` with
rows that are never valid lookups, and write the placeholder into
`variable_instance.vardemangds{version,niva}` where downstream consumers
would mistake it for a real classification label. The authoritative type
signal is `variable_instance.datatyp` — the placeholder adds nothing and is
sometimes misleading (e.g. cvid 207 `DatInv` is `datatyp='int'` but tagged
`Beskrivande text`).

`_VARDEMANGDER_REAL_SHAPED` — kods that *happen* to equal their version
label but are real single-code value sets. Kept silently.

| Värdekod | Label |
|---|---|
| `1` | Hade ingen anställning före YH-utbildningen |
| `2` | Övriga civilstånd |

Both classifications are required because the shape alone is ambiguous. An
unguarded skip on `kod == version` would silently drop the real codes; an
unguarded keep would let new SCB placeholders pollute the DB.

The skip rule is tight: `kod == version == niva` AND `kod` ∈ `_VARDEMANGDER_SENTINELS`. Looser variants (e.g. `kod == version` but `niva` diverging)
fall through to the drift detector below and fail the build for human
review, even when the kod is already a known sentinel string. This guards
against a future SCB change to the sentinel shape.

A cvid whose only Vardemängder rows were sentinels gets `NULL` for
`vardemangdsversion`/`vardemangdsniva` on `variable_instance`. Fully-empty
rows (kod, label, item all empty) are dropped silently.

### Drift detection

A `kod == version` row where kod is in neither allowlist is treated as drift
and fails the build with `RegmetaError(code="vardemangder_drift", exit 10)`.
The importer can't tell whether such a row is a new sentinel or a new real
single-code value set, so the build refuses to ship and prompts the
maintainer to add the kod to one of the two allowlists.

The drift trigger only requires `kod == version`, not `kod == version ==
niva`, so a placeholder where SCB drops the niva equality still surfaces.
Currently observed sentinels have all three fields equal, but no upstream
guarantee.

This makes the maintainer's release workflow self-checking: any new SCB
sentinel string causes the rebuild to fail loudly with an actionable
remediation, rather than silently shipping pollution. There is no
interactive escape hatch — drift always fails — because the only correct
response is to update the allowlists, which is a one-line code change.

## Value sets are year-projected at build time

SCB's `Vardemangder.csv` is the historical union — every code that ever
applied to a variable in any register year, with no temporal qualification.
SCB's `VardemangderValidDates.csv` (added after this project flagged the
issue) is the authoritative temporal filter: per `(ItemId, valid_from,
valid_to)`, with NULL bounds meaning "no boundary." A code without a
validity row is always valid throughout the variable's lifetime (per SCB
correspondence).

`build-db` projects every `(cvid, code_id)` pair through validity at build
time so each cvid carries the codes that were actually valid in its
regver year. The projection rule:

- For each pair, collect validity windows of all *tracked* ItemIds (those
  with at least one row in `VardemangderValidDates.csv`).
- If no tracked windows → include the code (always-valid fallback).
- Otherwise → include iff at least one window covers the cvid year.
- Yearless cvids (regver name has no plausible 4-digit year, e.g.
  `Person-År`) include all union pairs as a fallback.
- An untracked ItemId next to a tracked one does NOT relax the constraint:
  the tracked window is authoritative.

Projection is intentionally year-precision, not exact-date. SCB's
metadata is annual; sub-year boundaries (e.g. `valid_from=1995-09-01`) are
administrative artifacts that year overlap absorbs losslessly. The
trade-off — losing sub-year query precision — is paid for by removing
the temporal axis from the schema entirely. There is no `get values
--valid-at` flag and no historical-union opt-in: the union is discarded
by design. Maintainers auditing the raw union should read
`Vardemangder.csv` directly.

The result is content-addressed and deduplicated: identical year-projected
sets across cvids share one `value_set` row (`member_hash` = sha256 of
sorted (vardekod, vardebenamning) pairs); each cvid links to its set via
`variable_instance.value_set_id`. NULL `value_set_id` means the cvid had
no codes (sentinel-only or every union pair excluded by projection).

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
  unobserved codes get a fresh row with no `value_set_member` linkage).
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
use WITHOUT ROWID. Value codes are deduplicated into `value_code` (with
`UNIQUE(vardekod, vardebenamning)`); cvid → code membership is a
content-addressed `value_set` / `value_set_member` pair, where each
distinct year-projected code list is stored once and shared by every
cvid that observes it. SCB's validity windows are applied at build time
(see "Value sets are year-projected at build time"), eliminating the
historical-union junction and the per-item validity tables entirely. A
pre-aggregated `code_variable_map` replaces large secondary indexes for
value search queries. The original 13 GB raw DB shrank to ~1.6 GB through
deduplication and integer keys; year-projection is expected to take it
further still.

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
  changed column semantics that consumers must adapt to).
- **Minor bump** in either of these cases:
  1. Code starts reading a new column/table added in the build. This
     forces old DBs (that lack it) to be rejected cleanly at `open_db`
     instead of failing later with a SQL error.
  2. Build-time content semantics change in a way that should invalidate
     prior DBs even though no schema shape changed — e.g. dropping
     polluting rows from `value_code`, populating columns with NULL where
     they used to carry placeholder strings. Old DBs would silently serve
     pre-cleanup data; the bump forces a rebuild on the next `maintain
     update`.

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
