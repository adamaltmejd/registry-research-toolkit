# Design: reg_meta

Design rationale and constraints for the query layer. For usage, see
`reg-meta --help`. For the domain model, see [STRUCTURE.md](STRUCTURE.md).
For build-pipeline rationale (CSV import, sentinel filtering, year
projection, classification seeding, doc-DB build), see
[../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md).

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
`reg-meta-build build-db` replaces it entirely (not incremental).

## Data providers

reg_meta is provider-agnostic at the query layer: one metadata DB, one
docs DB, one CLI. Users searching or resolving variables shouldn't need
to know which agency published a given register. Provider-specific
parsers live in `reg_meta_build` (see its DESIGN.md § "Source parsers");
the resulting unified schema is what query commands see.

## FTS5 configuration

Two content-synced FTS5 indexes:

- **`register_fts`** — indexes register `name`, `purpose`.
- **`variable_fts`** — indexes variable `name`, `definition`, `description`.
  Uses `unicode61` tokenizer for correct Swedish character handling.
  Delivery column names (`variable_alias.delivery_column_name`) are
  deliberately excluded — they contain technical suffixes (e.g. `_LISA`)
  that pollute search results.
  Column name matching is handled by `resolve` instead.

## Register lookup strategy

All commands accepting a register argument use a three-step resolution:

1. Exact ID match
2. Case-insensitive exact name match
3. Case-insensitive substring match

This allows `34`, `LISA`, and `utbildning` to all work.

## Resolve: exact match only

`resolve` performs exact alias lookup against `variable_alias.delivery_column_name`.
No FTS fallback, no confidence scoring. Status is `matched` or `no_match`.
This is intentional — resolve is for mapping known column headers, not
discovery.

## Composite registers and source tracking

Registers like LISA, FRIDA, LINDA, and STATIV are composites — most of
their variables originate in source registers (RTB, RAMS, etc.). The
`variable` table tracks this via `source_register_id` (FK to `register`)
and `source_label` (display abbreviation or raw text). Unresolved sources
remain as raw text for human review and surface in `get schema` (source
column) and `get lineage` (consumer/source classification). The
resolution rules used during build are documented in
[../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md) §
"Source-register resolution".

## Catalog API surface (§5.10)

`Catalog` (`catalog.py`) is the in-process FQID→entity API the webapp's
`/api/catalog/*` endpoints wrap. `resolve(fqid)` is polymorphic over FQID
kind; the provider/register/variant/version/classification arms each
return their dedicated `Resolved*` row. The **binding** arm is
longitudinal (A2.5): a binding FQID resolves to a `ResolvedVariable` —
the addressable variable's shared metadata + its full `variable_state`
history (each state tagged with its variant coordinate) + the
variable-grain edges. Period-specific resolution lives in `resolve_at`;
cross-variable traversal in the per-edge accessors. All accessors are
list-returning; `resolve_at` returns `[]` (never raises) when no state
covers the period — only the binding FQID not resolving raises
`fqid_not_found`. See REFACTOR_SPEC.md §5.10 for the normative
signatures.

The §5.10 spec says the exact dataclass shapes live here. They are
frozen `@dataclass` (no Pydantic — reg_meta is the no-Pydantic library
surface, see root CLAUDE.md "Stack"); collection fields are tuples for
frozen-dataclass immutability/hashability.

**`Period`** — `int | str | dict`, the polymorphic period `resolve_at`
accepts (mirrors `Source.period`, §6.2): a bare year (`2018`), a period
token (`"HT2020"`/`"2020-Q3"`/`"2020-08"`/`"2018-12-31"`), an explicit
range `{"from", "to"}` (endpoints are int or token), or the `"_default"`
snapshot sentinel (no period filter). Expanded to an inclusive ISO
`(lo, hi)` interval by `_period_bounds` + `fqid.period_token_to_bounds`,
intersected against the full-date `variable_state` validity ranges — so
sub-annual and range queries are precise, not year-granular.

**`ResolvedVariable`** — the longitudinal binding resolution. Fields:
`fqid` (the caller's binding FQID, preserved through a `same_as`
traversal — still the interim 5-seg form until A2.6), `variable_id`,
`register_id`, `provider_key`, the shared metadata (`name`,
`definition`, `description`, `measurement_unit`, `is_sensitive`,
`is_identifier`, `source_register_id`, `source_register_text`), `states`
(tuple of `VariableState`, chronological ascending), the variable-grain
edges `same_as` / `replaced_by` (OUTBOUND successors) / `related_to` /
`lineage`, and `via_same_as` (the traversal path when resolved via a
`same_as` edge, else None).

**`VariableState`** — one `variable_state` row tagged with its variant.
Fields: `state_id`, `variant` (the `register_variant.slug`),
`register_variant_id`, `valid_from` / `valid_to` (inclusive ISO dates),
`data_type`, `data_length`, `delivery_column_name` (denormalized latest
alias), `value_set_version_label` (NOT NULL, `''` = no discriminator),
`value_set_id`, and `value_set` (hydrated `(code, label)` tuple, None
when the state has no value set).

**`VariableRef`** — a variable-grain edge endpoint
(`same_as` / `predecessors` / `successors`). Fields: `fqid` (None — the
edge tables store only the 3-part identity, and the binding FQID grammar
is 5-seg until A2.6, so no addressable FQID can be built yet), the
load-bearing `provider` / `register` / `variable` triple, and (#142, on
succession refs only) `reason` (the `timeseries_event.beskrivning`
transition reason) + `effective_year` (the AktuellVariabel-grain
successor edition year; None on `same_as` refs and on bare-grain
succession with no edition).

**`RelatedRef`** — a `variable_related_to` sibling (§5.7 split). Same
`fqid` (None) + `provider`/`register`/`variable` triple as `VariableRef`,
plus `relation_kind` (the split reason,
e.g. `same_definition_different_column`).

**`LineageEdge`** — one `variable_state_lineage` row (§5.6
consumer-side, state grain): `consumer_state_id`, `source_state_id`,
`valid_from` / `valid_to` (the validity intersection), and `source_fqid`
(the source-side 3-part binding FQID — None for the same reason as the
refs' `fqid`).

**`LineageWarning`** — one `variable_state_lineage_warning` row:
`consumer_state_id`, `warning_kind` (`no_source_state` /
`ambiguous_source_variant`), `message`.

`ResolvedVariableBinding` (the interim per-edition binding row) and the
`editions()` discovery path that returned it were **removed in A2.6** along
with the v0.11 5-seg binding parse. Resolution is now `ResolvedVariable` +
`resolve_at` / `states` (§5.10): the variable's shared metadata plus its
`variable_state` rows, each tagged with its variant. The per-edition cvid is
no longer a catalog return shape, and the variant is a register sub-resource
coordinate (passed to `resolve_at`), not a slash-path FQID segment. The v0.x
per-edition `resolve()` behavior was deleted, not aliased — pre-v1 policy
(no shims).

## Value sets are year-projected

`Vardemangder.csv` is the historical union — every code that ever
applied to a variable in any register year, with no temporal qualification.
`VardemangderValidDates.csv` is the authoritative temporal filter: per
`(ItemId, valid_from, valid_to)`, with NULL bounds meaning "no boundary."
A code without a validity row is always valid throughout the variable's
lifetime (per SCB correspondence).

The DB stores year-projected value sets, not the raw union: each cvid
carries the codes that were actually valid in its regver year.
Projection is intentionally year-precision, not exact-date. SCB's
metadata is annual; sub-year boundaries (e.g. `valid_from=1995-09-01`)
are administrative artifacts that year overlap absorbs losslessly. The
trade-off — losing sub-year query precision — is paid for by removing
the temporal axis from the schema entirely. There is no `get values
--valid-at` flag and no historical-union opt-in: the union is discarded
by design.

The projection rule and its build-time mechanics are documented in
[../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md) § "Year
projection".

The result is content-addressed and deduplicated: identical year-projected
sets across cvids share one `value_set` row (`member_hash` = sha256 of
sorted `(code, label)` pairs); each cvid links to its set via
`variable_instance.value_set_id`. NULL `value_set_id` means the cvid had
no codes (every union pair excluded by projection, or only sentinel rows
in the source — see [../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md)
§ "Vardemängder sentinel filtering").

## Classifications

Named code systems (SUN2000, SSYK2012, SNI2007, LKF, ...) are first-class
entities. Each `classification` row carries metadata (publisher, version,
validity range, supersedes link, canonical URL) and a cached `code_count`.
The `classification_code` junction holds the deduplicated union of value
codes that belong to the classification, with an optional `level` integer
for prefix-hierarchy filtering (length of all-digit codes; NULL for
non-numeric codes like ICD letters).

The FK lives on `variable_instance`, not on `variable`. SCB's data model
already places the classification label (`value_set_version_label`) per
instance, and many headline variables genuinely span multiple
classifications across their lifetime — e.g. `Utbildningsnivå` (var_id 66)
uses SUN 2000 codes through 2018 and SUN 2020 codes from 2019 onwards;
`SSYK` and `SNI` show the same generational drift. Linking at the instance
level keeps each code system distinct (SUN 2000 codes never bleed into
SUN 2020) and lets variable-level helpers aggregate when needed.

The `classification_id` column is populated at build time from a
maintainer-curated TOML seed at `reg_meta_build/classifications.toml`
(exact match against `value_set_version_label`, no fuzzy inference). The seed
schema, build-time invariants, and validation rules live in
[../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md) §
"Classification seed".

### Canonical vs observed codes

`classification_code.is_valid` distinguishes published canonical codes
from codes that merely show up in the data. SCB's metadata exports
contain plenty of noise (`*`, `***`, `0000`, `[BLANK]`, stray prefix
levels) that has no place in an authoritative code list, but is also
useful to keep around so a researcher seeing one of those values in a
register can look it up.

Canonical codes come from per-classification CSVs ingested by the build
(see [../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md) §
"Canonical code CSVs"). A classification with a CSV gets
`classification_code.is_valid` populated as 1 (canonical) or 0
(observed-only) and `classification.valid_code_count` cached for that
canonical count. Without a CSV, every `classification_code` row carries
`is_valid=NULL` (validity unknown).

The CLI exposes this via `get classification --codes --only-valid` and
includes `is_valid` per code in JSON output (omitted when NULL).

Hierarchy is intentionally not encoded as `parent_code_id`. The `level`
column captures the most useful filter ("top-level only"); deeper
parent/child queries fall back to prefix matching on `value_code.code`. Code
sets without prefix hierarchy (ICD-10, ATC) keep `level = NULL` and use
their own conventions.

## Storage optimization

IDs stored as INTEGER (not TEXT). Tables with composite integer-only PKs
use WITHOUT ROWID. Value codes are deduplicated into `value_code` (with
`UNIQUE(code, label)`); cvid → code membership is a
content-addressed `value_set` / `value_set_member` pair, where each
distinct year-projected code list is stored once and shared by every
cvid that observes it. SCB's validity windows are applied at build time
(see "Value sets are year-projected"), eliminating the historical-union
junction and the per-item validity tables entirely. A pre-aggregated
`code_variable_map` replaces large secondary indexes for value search
queries. The original 13 GB raw DB shrank to ~1.6 GB through
deduplication and integer keys; year-projection is expected to take it
further still.

## Documentation layer

Register documentation (parsed from SCB PDFs) is curated as
Obsidian-compatible markdown files under `reg_meta_build/docs/`,
source-of-truth for maintainers, and indexed into a separate FTS5
database (`reg_meta_docs.db`) with its own `DOC_SCHEMA_VERSION`. Docs are
keyed to register and variable names, not numeric IDs, so doc updates
and main-DB updates are independent.

End users never see the markdown files. The doc DB is distributed as a
GitHub Release asset (`reg_meta_docs.db.zst`) parallel to the main DB asset,
installed into the same cache dir (`$XDG_DATA_HOME/reg_meta/`), and fetched
by `reg-meta update` alongside the main DB. Query commands (`search`,
`get`, `resolve`, `docs/*`) refuse to run without the doc DB — on first
use the CLI offers to download both artifacts.

`reg-meta-build build-docs` is a maintainer-only command that rebuilds
the doc DB from a repo checkout of `reg_meta_build/docs/` before upload.
Runtime never reads markdown — `repo_docs_dir()` in
`reg_meta_build.doc_db` is only consulted by `build-docs` when run from a
repo checkout, and is absent in installed wheels of `reg_meta`.

See [../reg_meta_build/docs/SCHEMA.md](../reg_meta_build/docs/SCHEMA.md)
for the markdown file format.

## Versioning and compatibility

Four independent version numbers:

| Version | Location | Purpose |
|---------|----------|---------|
| Package version (`__version__`) | `__init__.py`, `pyproject.toml` | Python package / CLI release |
| Main schema version (`SCHEMA_VERSION`) | `db.py` | Main-DB schema compatibility |
| Doc schema version (`DOC_SCHEMA_VERSION`) | `doc_db.py` | Doc-DB schema compatibility |
| Contract version (`CONTRACT_VERSION`) | `cli_common.py` | CLI output envelope format |

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
     pre-cleanup data; the bump forces a rebuild on the next `reg_meta
     update`.

Either bump requires rebuilding and re-uploading the DB asset before the
package release goes live — see `.claude/skills/release/SKILL.md`. The
`TestSchemaCompat` tests in `reg_meta_build/tests/test_build_db.py`
verify the guard.

### Release tags and distribution

The monorepo uses **per-package release tags**: `reg_meta/v0.5.0`,
`reg_meta_build/v0.1.0`, `mock-data-wizard/v0.4.0`, etc.  Each tag
corresponds to a GitHub release scoped to that package.

| Channel | Trigger | What it distributes |
|---------|---------|---------------------|
| PyPI | `publish_reg_meta.yml` on `reg_meta/v*` release | Python package (wheel + sdist) |
| PyPI | `publish_reg_meta_build.yml` on `reg_meta_build/v*` release | Builder package (wheel + sdist) |
| GitHub Release asset | Manual upload to the `reg_meta/v*` release | Pre-built main DB (`reg_meta.db.zst`) |
| GitHub Release asset | Manual upload to the `reg_meta/v*` release | Pre-built doc DB (`reg_meta_docs.db.zst`) |

Both DB assets are **optional** per release. A package release only needs a
new main DB when `SCHEMA_VERSION` changes, and only needs a new doc DB when
`DOC_SCHEMA_VERSION` changes or `reg_meta_build/docs/` content changes.
`resolve_latest_release()` walks recent releases backwards looking for each
asset independently, so a doc-less or DB-less package release does not
orphan older assets. The publish workflow's smoke step exercises
`reg-meta update --force` before allowing PyPI publish, so a release that
breaks the walker (e.g. incompatible assets, or no resolvable asset at all)
fails CI instead of shipping.

The wheel contains Python source only. The markdown under
`reg_meta_build/docs/` is maintainer source-of-truth and is **not** bundled
— end users receive the built doc DB via `reg-meta update`.

Legacy bare `v*` tags (pre-0.6.0) are still recognized during the transition
but new releases must use the `reg_meta/v*` prefix.

**Update command**: `reg-meta update` is the single command that brings
everything current — it runs `uv tool upgrade reg-meta` for the package and
walks releases to find the latest main-DB and doc-DB assets. Already-current
assets are skipped (tracked via `.db_source` and `.docs_source` in the cache
dir). A background version checker runs once per week (cached in
`~/.local/share/reg_meta/.update_check`) and prints a hint on interactive
runs when a newer release exists.

**Auto-download on first use**: query commands (`search`, `get`, `resolve`,
`docs/*`) prompt to download whichever artifacts are missing when invoked
interactively, so users don't need to know about `reg-meta update` on first
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
| 25   | Network error (`reg-meta update`) |
| 30   | Unexpected internal error |

## Determinism

- Stable ordering for repeated runs against the same database
- Stable JSON key ordering
- Deterministic paging (offset, limit)

## Security

- Metadata only — no microdata
- No credentials read or stored
- No outbound network requests (except `reg-meta update` and the weekly version check)

## Explored and ruled out

- **Direct API integration** against `mikrometadata.scb.se` — no stable
  public API. Session-bound WebSocket with no documented contract.
- **Browser automation** — fragile, unrepeatable. Manual CSV export is
  more reliable.
- **Query caching / user adaptation database** — deferred. Not needed yet.
