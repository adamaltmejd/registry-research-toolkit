# Design: mock_data_wizard

Design rationale and constraints. For usage, see `mock-data-wizard --help`.

## Workflow

The bundle has two modes (`MODE = "discover"` / `"extract"`) and the
end-to-end loop crosses the MONA boundary three times:

1. `mock-data-wizard build-bundle` (local) — amalgamates the runtime
   modules into a single `mdw_runner.py` for MONA upload.
2. **Discover** on MONA. Edit `configure()` in the bundle, leave
   `MODE = "discover"`, upload, and run:
   `python mdw_runner.py` — writes `mock_data_discovery.json`
   (metadata only: column names, SQL types, row counts; no values, no
   distinct counts, no samples).
3. **Author config** (local) — author `mock_data_config.json` from
   `mock_data_discovery.json` via `mock_data_wizard.editor.init_if_missing`
   (or an external UI calling the same API; see *Editor API* below).
   The editor applies the layered classifier (id-name → regmeta
   classification → categorical-name → sql_type → `opaque` default;
   see *Configure classifier priority* below) and persists year +
   register per source. Subsequent edits go through the editor
   mutators; the file may also be hand-edited.
4. **Extract** on MONA. Switch the bundle's `MODE = "extract"`, place
   `mock_data_config.json` next to it, re-run on MONA — writes `mock_data_stats.json`
   (only aggregate statistics; the configured types drive per-column
   SQL with no data-driven classifier pass).
5. `mock-data-wizard generate` (local) — produces mock CSVs from
   `mock_data_stats.json`.

Why three trips. Discover is metadata-only and PII-safe by
construction; running it first means the per-column type assignment
happens locally where regmeta and human review are available. Extract
is the slow part (full-population aggregation) — splitting it out
means each iteration of the type config doesn't pay 20-hour-class
re-runs to fix a misclassified column. The earlier R-script-generation
approach is preserved in git history; the runtime is Python going
forward.

### Configure step (editor API)

There is no built-in TUI for the configure step. The
`mock_data_wizard.editor` module exposes a stateless API
(`init_if_missing`, `get_state`, mutators) that authors and mutates
`mock_data_config.json` in place; a UI lives outside this package. See
the *Editor API* section for the contract.

The CLI keeps the bundle / aggregate / generate surface only:
`mock-data-wizard build-bundle`, `mock-data-wizard generate`,
`mock-data-wizard compare`, `mock-data-wizard scan`,
`mock-data-wizard update`. Bare `mock-data-wizard` prints help.

The generate command exposes `seed`, `sample-pct`, regmeta-enrichment
toggle, register filter (only when regmeta is on), output dir, and
stale-file handling. Pass `--yes` to skip prompts and `--force` to
delete stale files instead of warning-and-keeping them.

## MONA Python runtime (probed 2026-04-25 on project P1105)

The batch client ships with the WinPython-31700 distribution at
`E:\Programs\WinPython-31700\python` (Python 3.13.7, MSC v.1944 64-bit).
This is a curated bundle: 955 packages pre-installed, including every
runtime dep we need. No internet access; no internal PyPI mirror at the
common paths we checked. `python -m pip` works, but `pip` is not on
PATH (the `Scripts\` folder is not exported).

Pre-installed deps relevant to the rework:

| Package  | Version | Used for                                      |
|----------|---------|-----------------------------------------------|
| duckdb   | 1.4.0   | `file_source` aggregation over CSVs           |
| pyodbc   | 5.2.0   | `sql_source` aggregation against MS SQL views |
| numpy    | 2.3.3   | shared with the local `generate` step         |

ODBC: `Driver={ODBC Driver 17 for SQL Server}` per the MONA docs;
`Trusted_Connection=yes` (no passwords carried in code). DSN-based
connections (`pyodbc.connect("DSN=P1105")`) also work — the R-side
probe verified the per-project DSN exists and resolves.

Disk: `C:\Windows\TEMP` has ~54 GB free on the batch client (good for
DuckDB spill). The user's home share `\\micro.intra\mydocs\...` only
has ~250 MB free; we never write outputs there.

Stdout footgun: in batch mode, Python's stdout is buffered to an
in-memory buffer; once full, the script hangs in BatchClient with no
error. Mitigation per MONA's docs: detect hostname starting with
`MBS` and redirect `sys.stdout` to `os.devnull` at the top of any
batch-run script.

The bundle has two flags at the top of its USER CONFIGURATION block —
`DEBUG` (default `False`) and `VERBOSE` (default `False`) — that
switch the diagnostic strategy:

- **`DEBUG=False`** (default, clean runs): no log file is written.
  On MBS hosts we still redirect `sys.stdout` *and* `sys.stderr` to
  `/dev/null`, including `os.dup2` over fd 1/fd 2 so C-extension
  output can't slip through. On non-MBS hosts the console is left
  alone (interactive use). On a successful run, the only artefact on
  disk is `mock_data_stats.json`.
- **`DEBUG=True`**: a single combined log file
  `mdw_log_<HOST>_<TS>.txt` is opened line-buffered and used for
  everything — boot trace, our `logging.FileHandler`, and (via
  `sys.stdout`/`sys.stderr` redirection plus `os.dup2`) any output
  from pyodbc / MSSQL driver / duckdb / numpy. One file, no
  interleaving with /dev/null, full diagnostics.
- **`VERBOSE=True`** (only effective with `DEBUG`): drops the logger
  level from `INFO` to `DEBUG`, which adds the per-column progress
  lines from `process_handle`. Worth turning on for a long
  `sql_source` run to see which column the script is stuck on; noisy
  for short runs.

Why redirect stderr too: the MONA doc example only shows stdout, but
the underlying problem ("the console" is buffered to memory)
plausibly applies to both, and our per-column logging produces real
volume there. Why redirect at the OS fd level: Python's
`sys.stdout`/`sys.stderr` swap only catches Python-side writes; C
extensions can bypass it.

RAM: 150–200 GB on the batch server. DuckDB defaults to ~80% of RAM
for `memory_limit`, which is plenty for any single-source aggregation
we'll run. We don't override it; we just set `temp_directory` to point
at `C:\Windows\TEMP` and `preserve_insertion_order = false`.

## MONA upload (probed 2026-04-27 on MBS16)

The MONA upload UI's officially advertised whitelist (`TXT/RTF/PDF/DTA/
SAS7BDAT/SPSS/QGIS SHAPE/PNG/JPG`, 10 MB cap, "TXT (Not UNICODE)") is
stricter than what's actually enforced. Verified directly:

- **`.py` is accepted** and runs under WinPython on the batch host.
- **Source bytes round-trip verbatim** — UTF-8 sentinels (`Födelseår
  Kön Län`) survive in the file's own bytes after upload; the file
  decodes cleanly as UTF-8 with no BOM. **The shipped bundle can use
  raw UTF-8** — no ASCII-escape pass needed.
- **Non-ASCII filenames** can be created on the home share.
- **cwd at batch start is the user's home share** (`\\micro.intra\
  mydocs\...\InBox`, ~250 MB free) — the script must never depend on
  cwd for output. `mock_data_stats.json` is small enough to live next to the
  script; everything else (DuckDB spill especially) goes to
  `C:\Windows\TEMP`.
- **`locale.getpreferredencoding()` is `cp1252`** — pass `encoding=`
  explicitly on every CSV/text open; do not rely on the default. The
  wizard's bundle generator pins `encoding='latin-1'` on every emitted
  `file_source(...)` call for this reason; `file_source`'s own default
  is `utf-8` (right for general code), so the override has to land at
  the bundle-authoring boundary, not in the constructor. Users with
  UTF-8 files edit the literal in the generated bundle.

Architectural consequence: we ship `mock_data_wizard` to MONA as a
single bundled `.py` file built by an in-repo amalgamator. One file
sidesteps the multi-upload UX, fits the 10 MB cap with two orders of
magnitude to spare, and the "Not UNICODE" line in the upload notice
turns out to be advisory rather than enforced.

## Source model

The bundle's `configure()` function is the single place users declare
what data to aggregate. It returns a list of sources. Two constructors
are available:

- `file_source(path, include, exclude, pattern)` — a directory (or single
  file) of CSV/TXT data.
- `sql_source(dsn, tables, pattern, queries, ...)` — an ODBC-accessible
  database. On MONA this is MS SQL via a per-project DSN. Credentials
  come from the Windows system DSN; the bundle never carries passwords.

Sources dispatch through `iter_source(src)`, which yields streaming
`SourceHandle` instances (one per table). The main loop pulls one
handle at a time, runs the classify/summarize pipeline against it, and
closes it before the next fetch. Lazy-by-design: peak memory stays
near a single table rather than the sum of all tables in the source,
which matters on MONA projects with hundreds of SQL views.

### Discover mode (`MODE = "discover"`)

The bundle's first MONA trip. SQL sources without `tables=`, `pattern=`,
or `all=True` are listed permissively here — the user typically declares
`sql_source(dsn="P1105")` and lets discover enumerate every non-archived
view in the DSN. File sources walk every CSV that matches the default
pattern. For each table/file, the discoverer pulls `COUNT(*)` plus
column metadata from `INFORMATION_SCHEMA.COLUMNS` (SQL) or DuckDB
`DESCRIBE` (files). No row-level data is read.

Output is `mock_data_discovery.json`:

```json
{
  "contract_version": "discover-1.0.0",
  "sources": [
    {
      "source_name": "lisa_2018",
      "source_type": "sql",
      "source_detail": {"dsn": "P1105", "table": "dbo.lisa_2018"},
      "row_count": 8492768,
      "columns": [
        {"name": "P1105_LopNr_PersonNr", "sql_type": "varchar", "nullable": false},
        {"name": "Lan", "sql_type": "char", "nullable": true}
      ]
    }
  ]
}
```

Discover passes through the same `scan.write_export` PII scanner as
`mock_data_stats.json` — column names and `sql_type` strings are unlikely to
contain personnummer, but defense-in-depth is cheap.

### Extract mode (`MODE = "extract"`)

The bundle's second MONA trip. Requires `mock_data_config.json` next to
the bundle. Source filtering is **strict** here — `sql_source` must
declare `tables=`, `pattern=`, or `all=True`; the permissive
unfiltered mode is discover-only. Every column the source yields must
have a type override in `mock_data_config.json`; an unconfigured column
errors out (it would have to fall back to a data-driven classifier
pass, which this mode explicitly does not have).

### Cohort filtering with `where`

Filters are declared **per-table** via `sql_table()`, not at the source
level. Different tables in one source typically have different filter
columns (LISA's `AR`, PAR's `INDATUM`, etc.), so a source-wide `where=`
would silently fail or — worse — mismatch a column the next table
doesn't have.

```python
sql_source(
    dsn = "P1105",
    tables = (
        sql_table("dbo.lisa_2018", where = "AR > 2015"),
        sql_table("dbo.par",       where = "INDATUM > '2015-01-01'"),
        "dbo.fodelse",  # plain string -> no filter
    ),
)
```

For files, `where=` lives on `file_source(...)` itself: each file is its
own table and the predicate runs against the DuckDB-typed columns from
`read_csv_auto`.

Implementation: the iterator wraps the table reference in a derived
table — `(SELECT * FROM [dbo].[lisa_2018] WHERE AR > 2015) AS
__mdw_src` — that downstream emitters just paste into `FROM {table}`.
Cohort filtering is transparent to `count_rows`, `_pre_classify`, and
every typed aggregate query.

The small-population warning fires on the **filtered** row count,
which is the disclosure-relevant denominator. A `where` that narrows
to a handful of individuals is exactly the kind of risk
SMALL_POP_MULT × SUPPRESS_K is meant to flag.

The clause is recorded in `source_detail.where` in `mock_data_stats.json` so the
downstream `generate` step can echo it (e.g., apply the same year
filter to the mock data range).

### Configure classifier priority

`configure` walks each column from `mock_data_discovery.json` and assigns one of
the five types via this chain (first match wins):

1. **`is_known_id(name)`** — `lopnr` / `persnr` patterns. SQL type
   can't tell a BIGINT identifier from a BIGINT measure; the name
   has to.
2. **Regmeta evidence** (only when the source's group has a register
   assigned — auto-detected at init or chosen via the editor) — joining
   `variable_alias` → `variable_instance` for that register:
   - non-null `value_set_id` *or* non-null `classification_id`
     → `categorical` (SCB enumerated codes / shared classification)
   - `datatyp` ∈ {int, decimal, ...} → `numeric`
   - `datatyp` ∈ {date, datetime, ...} → `date`

   Storage type alone (`char` / `varchar` with no value codes and no
   classification) is **not** taken as a categorical signal — text
   storage is often free text. Project-prefix stripping (`P1105_LopNr`
   → `LopNr`) mirrors the same logic used by enrich.
3. **`is_rtb_named_categorical(name, register)`** — narrow exact-name
   (case-insensitive) allowlist scoped to RTB. Covers SCB names
   regmeta is known to be missing under RTB: the record-quality flags
   `AterAnv` / `FelPersonNr` / `LopNrByte` plus the birth-time grouping
   variables `FodelseAr` / `FodelseArMan`. No fuzzy patterns —
   variants fall through.
4. **`sql_type`** — `BIGINT/INTEGER/DOUBLE/DECIMAL/...` → `numeric`;
   `DATE/TIMESTAMP/...` → `date`. For SQL sources the database's
   declared type is authoritative; for CSVs read by DuckDB, `sql_type`
   is DuckDB's own inference (which already does int-vs-double on
   the data — no separate value-peeking pass at discover time).
5. **Fallthrough** — `opaque` (we don't model the value
   distribution; record length stats and emit placeholders). The
   editor surfaces these via `ColumnInfo.current_type == "opaque"`;
   the user overrides via `set_column_type` or by hand-editing
   `mock_data_config.json`.

The chain deliberately gives regmeta authority over names for
categorical detection but not over `is_known_id`: regmeta has no
"this is an identifier" type, and id-naming conventions are stable
across registers. The earlier loose "known categorical name" fallback
(`Kon` / `Kommun` / `Sun2000Inr` / `FodelseLand` / ...) was removed
once regmeta's `value_set` schema made these signals authoritative —
common Swedish stems (`land`, `civil`, `medb`, ...) carry too much
false-positive risk for a name-pattern guesser to be a net win.

### Per-column type config via `mock_data_config.json`

Authored by `mock_data_wizard.editor.init_if_missing` from a
`mock_data_discovery.json` and uploaded next to the bundle. Extract
mode is strict: every column on every source must carry a type entry,
the schema is validated on load, and typos error out instead of
getting silently dropped.

```json
{
  "contract_version": "mdw-config-3.0.0",
  "discover_hash": "<sha256>",
  "column_types": {
    "Population_PersonNr_2018": {
      "FelPersonNr": {"type": "opaque"},
      "BirthDate": {"type": "date", "date_format": "%Y%m%d"},
      "Salary": {"type": "numeric", "numeric_subtype": "integer"}
    },
    "Individ_2018": {
      "Distriktskod": {"type": "opaque"}
    }
  },
  "column_options": {
    "Population_PersonNr_2018": {
      "Salary": {"suppress_k": 20}
    }
  },
  "sources": {
    "Individ_2018": {"year": 2018, "register": "LISA"},
    "Individ_2019": {"year": 2019, "register": "LISA"}
  },
  "manual_columns": [["Population_PersonNr_2018", "FelPersonNr"]]
}
```

- Keys in `column_types`, `column_options`, and `sources` are exact
  `source_name` matches. The 2.0.0 `fnmatchcase` glob form was
  dropped in 3.0.0 — with the editor API in place, globs are
  redundant noise the editor would silently flatten on first edit
  anyway. N exact entries is what the editor produces and the
  bundle parser consumes.
- Each `column_types` entry's `type` is required and must be one of
  `id`, `categorical`, `numeric`, `opaque`, `date`. Inline
  subtype/format hints are optional and only valid for the matching
  type. When *any* inline hint is supplied, the bundle skips the
  per-column sample query for that column entirely — that is the
  perf win the override is for.
- `column_options` is a separate namespace reserved for non-type
  overrides (e.g. `suppress_k` for disclosure-control hardening).
  Validated here; consumed in `summarize`. Each option key is
  checked against `VALID_OPTION_KEYS` and the option's own
  invariants. `suppress_k` in particular is floored at the global
  `SUPPRESS_K` — overrides may only *raise* the disclosure-control
  threshold for a column, never lower it. A typo'd `0` would
  otherwise turn the override into a fail-open path.
- `sources[name]` carries `year` and `register`. `year` is populated
  by the configurer from a 4-digit name regex; an explicit
  `"year": null` suppresses the regex fallback (the user is asserting
  "no year"). Read by `enrich.py` to bias CVID picking toward the
  right register version (see CVID picker tier 1). `register`
  records which register's regmeta evidence drove auto-classification
  for the source; persisted so reopening the editor restores the
  context, and so the file documents itself.
- `discover_hash` is sha256 of the discover payload's
  `(source_name, [(col_name, sql_type), ...])` tuples (sorted on
  both axes for determinism). `row_count`, `nullable`, and
  `source_detail` are deliberately excluded: they shift between
  MONA runs without invalidating type overrides, and including them
  would fire spurious drift warnings. The editor recomputes the
  hash on every read and surfaces a `discover_drift` warning when
  it differs from what's stored.
- `manual_columns` records `[source_name, column_name]` pairs the
  user explicitly overrode. Re-classification operations (e.g.
  changing a group's register) skip these by default. Kept as a
  side namespace rather than a per-column `provenance` field so the
  bundle's strict parser doesn't need to know about it; the bundle
  ignores `manual_columns` entirely.

Strict validation: unknown types, unknown option keys, duplicate JSON
keys, schema-version mismatches, and stray fields all raise. The
configurer file is meant to be hand-edited, so silent drops would
mask user typos.

The auto-classifier runs once when `mock_data_config.json` doesn't
exist. After that, every load reads the JSON as-is — re-running
discover does *not* re-trigger classification. The user (or UI)
re-runs it explicitly: per-group via `editor.set_group_register`
(skips `manual_columns` by default), or whole-project via
`editor.init_if_missing(overwrite=True)`, which wipes the JSON and
re-runs from scratch. The classifier is a starting point, not an
authority that gets to second-guess the user on every reload.

## Editor API

The `mock_data_wizard.editor` module exposes a stateless, autosaving
local API for authoring and mutating `mock_data_config.json`. Pure
functions over `project_dir` plus the regmeta DB; no module-level
session state. UI tooling (browser, TUI, etc.) lives outside this
package and calls this API directly.

### Design principles

1. **Single source of truth.** `mock_data_config.json` is the source
   of truth for configuration. Every API call builds, reads,
   validates, mutates and writes this file.
2. **Autosave and crash safety.** Every mutation writes the new
   configuration atomically (`tmp` file + `os.replace`) and returns a
   fresh snapshot. There is no explicit "save" operation.
3. **Concurrency via revision tokens.** Each `StateSnapshot` carries
   a `snapshot_version` token (SHA-256 of the on-disk config bytes),
   *not* stored in the JSON. Mutating functions require an
   `expected_version` argument; if the on-disk file's current token
   differs (another writer or a manual `vim` edit), the function
   raises `StaleStateError` without writing. Each mutation also holds
   an exclusive `fcntl.flock` on a `.mock_data_config.lock` sidecar
   for the read+write window, so two in-flight mutators serialise
   instead of both passing the version check and clobbering each
   other's writes.
4. **Manual overrides preserved.** A top-level `manual_columns` array
   records `(source, column)` pairs the user explicitly overrode.
   Re-classification operations (e.g. `set_group_register`) skip
   those entries by default. Provenance lives in this side namespace
   so the bundle's strict parser doesn't need to know about it.
5. **Stable discovery hash.** A `discover_hash` field summarises the
   discover payload (sorted sources × sorted columns of
   `(name, sql_type)` tuples). The editor recomputes the hash on
   every read and surfaces a `discover_drift` warning when it differs
   from the stored value.
6. **Grouping rule.** Sources are grouped by their current `register`
   field, not by schema fingerprint:
   - All sources sharing a non-null `register` form one group, with
     `group_id = "reg-<register_id>"`.
   - Each source with `register == null` forms its own singleton
     group, `group_id = "noreg-<source_name>"`. Unassigned sources
     are heterogeneous and shouldn't share a reclassification action.
7. **Dense classification.** Every column on every source carries a
   type entry after `init_if_missing` (defaulting to `opaque`). The
   bundle's extract mode is strict — a sparse config has no use.
8. **Inline subtype/format hints** (`id_subtype`, `numeric_subtype`,
   `date_format`) are an extract-time optimization (skip the
   per-column sample query on MONA) *and* a manual-override hatch
   for cases where sampling is ambiguous (e.g. `01-02-2018` parses
   two ways) or impossible (all-NULL columns). The classifier
   populates them at `init_if_missing` when it can derive them
   locally; the bundle uses them when present and falls back to
   sampling when absent.

### Data models

`StateSnapshot` is the frozen return value of `get_state` and every
mutator:

| Field | Description |
| --- | --- |
| `config` | Parsed `MDWConfig` — the raw configuration data. |
| `groups` | Tuple of `RegisterGroupView` — one per register-group. |
| `discover` | The discovery payload, if loaded, for client use. |
| `warnings` | Tuple of `EditorWarning(code, message, context)`. Codes include `discover_drift`. |
| `snapshot_version` | Opaque token for the current config version. Pass back as `expected_version` on the next mutation. Not stored in the JSON. |

`RegisterGroupView` exposes `group_id`, `register_id`, `register_name`,
`confidence` (`"high"` / `"partial"` / `"none"`), `sources`,
`columns_by_source` (mapping to tuples of `ColumnInfo`),
`schema_variants` (count of distinct column schemas in the group;
`>1` means drift), and `panel_candidate` (a `PanelCandidate` from
`panels.py` or `None`).

`ColumnInfo` exposes `name`, `sql_type`, `current_type`, `hint`
(inline `id_subtype` / `numeric_subtype` / `date_format` projected
into a dict, or `None`), `provenance` (`"manual"` / `"auto"`, derived
from `manual_columns`), `regmeta_signal`, and `regmeta_implied_type`.

### API functions

All functions live in `mock_data_wizard.editor` and take a
`project_dir: Path` plus an optional `db_path: Path | None` to
override the regmeta DB location.

**Reading and initialization.**

- `get_state(project_dir, *, discover_path=None, db_path=None)` —
  reads the config, returns a snapshot. Raises
  `NotInitializedError` when the config is absent. When
  `discover_path` is None, defaults to
  `project_dir / mock_data_discovery.json`; if that's also absent,
  succeeds with empty `discover` and skips drift detection.
- `init_if_missing(project_dir, discover_path, *, db_path=None,
  overwrite=False)` — creates an initial config from a discover
  payload. Idempotent unless `overwrite=True`. Auto-detects each
  source's register, runs the per-group classifier, persists year +
  register, and surfaces unambiguous panel candidates directly into
  `panels`.

**Mutators** (all require `expected_version: str`).

- `set_column_type(project_dir, source_name, column_name, new_type, *,
  expected_version, hint=UNCHANGED, db_path=None)` — sets the type of
  one column. `(source_name, column_name)` must exist in the discover
  payload; unknown pairs raise `ValidationError`. Adds the pair to
  `manual_columns`. `hint` semantics: `UNCHANGED` preserves any
  existing hint that's still valid for `new_type` (silently dropped
  otherwise); `None` clears any hint; a dict sets it (validated
  against `INLINE_HINT_KEYS[new_type]`).
- `set_group_register(project_dir, group_id, register, *,
  expected_version, db_path=None, reclassify_manual=False)` —
  assigns or clears a register for a group, then re-classifies. With
  `reclassify_manual=False` (default), columns in `manual_columns`
  are preserved; with `reclassify_manual=True`, all columns are
  re-classified and the affected entries are removed from
  `manual_columns`. **When a column's type changes during
  reclassification, its `column_options` entry is dropped** —
  options can be type-specific. Note that affected sources'
  `group_id`s change (since they derive from `register`); clients
  re-fetch.
- `set_source_metadata(project_dir, source_name, *, expected_version,
  year=UNCHANGED, db_path=None)` — modifies per-source metadata.
  Currently scoped to `year`; register changes go through
  `set_group_register`.
- `set_column_options(project_dir, source_name, column_name, options,
  *, expected_version, db_path=None)` — sets or clears column
  options. Keys validated against `VALID_OPTION_KEYS`; passing
  `options=None` clears all options for the column.
- `put_panel(project_dir, panel, *, expected_version, db_path=None)`
  — adds or replaces a panel by `panel_id`. The full payload is
  re-validated via `parse_config` so source-collision and
  panel-key invariants surface as `ValidationError`.
- `remove_panel(project_dir, panel_id, *, expected_version,
  db_path=None)` — removes a panel by id. No-op when absent.

**Helpers** (no `expected_version`; pure relative to regmeta).

- `list_registers(*, db_path=None) -> list[Register]` — returns `[]`
  when regmeta is unavailable.
- `resolve_register(name_or_id, *, db_path=None) -> Register | None`
  — returns `None` when regmeta is unavailable, the input doesn't
  match, or the match is ambiguous.
- `detect_year_from_source_name(source_name) -> int | None` — naive
  4-digit-year search, exposed for UI affordances.
- `detect_panel_member_kind(source_name, columns) ->
  PanelMemberSuggestion` — single-source panel-member shape inference.

The editor re-exports `Panel`, `PanelMember`, `Register`, and
`PanelMemberSuggestion` from their defining modules, plus the
constants `VALID_COLUMN_TYPES`, `VALID_OPTION_KEYS`,
`VALID_ID_SUBTYPES`, `VALID_NUMERIC_SUBTYPES`, `INLINE_HINT_KEYS`,
and `GLOBAL_SUPPRESS_K`.

### Errors

- `NotInitializedError` — `get_state` when the config file is absent.
- `ValidationError` — invalid input (unknown type, unknown option
  key, unresolved register, unknown source/column).
- `StaleStateError` — `expected_version` mismatch. Clients
  re-fetch via `get_state` and retry.

### Performance expectations

The API is designed for local operation on projects with up to
hundreds of sources and thousands of columns. Estimated worst-case
timings on typical hardware:

| Operation | Disk I/O | Regmeta DB | Target total |
| --- | --- | --- | --- |
| `get_state` | 1 read | 0 queries (lookups via cached signals) | <50 ms |
| `set_column_type` | 1 read + 1 write | 0 queries | <100 ms |
| `set_group_register` | 1 read + 1 write | batched per-register query | <300 ms |

If classification on large projects proves too slow, a regmeta cache
keyed by `(register_id, db_mtime)` may be introduced.

### File materialisation threshold

`iter_file_source` size-gates how each CSV is exposed to DuckDB. Files
at or below `MDW_MEMORY_THRESHOLD_MB` (default 50 GiB on MONA-class
hosts) become a `CREATE OR REPLACE TABLE` — `read_csv_auto` runs once
and every downstream `aggs` / `quantiles` / `freqs` query hits the
materialised columns. Larger files stay as a `VIEW` so peak memory
stays bounded: each query reparses the CSV but the table never lives
in RAM.

The threshold matters because the per-column query overhead dominates
wall time on small inputs that would otherwise be summarised in a
single pass. The output is identical either way — only the time/memory
trade differs. Override the threshold via the env var; set it to `0`
to force the VIEW path for every file.

The default is sized for the MONA batch server (150–200 GB RAM,
DuckDB at ~80% of that, sources iterate sequentially with
`DROP TABLE` between handles, percentile sorts spill to
`C:\Windows\TEMP` if needed). Lower it on tighter hosts. If we ever
parallelise across sources, the threshold needs to drop in
proportion or scheduling needs to become budget-aware — peak memory
is currently single-source because the loop is single-threaded.

### File discovery quirks

Two files with the same basename in different subdirectories collide
— `include=("name.csv",)` can't select between them, and they'd both
get `source_name = "name.csv"`. Discovery dedupes basenames in the
written suggestion and warns about the collision; processing fails
fast if the user narrows `include` but the matched files still have
duplicate basenames. The fix is to narrow `path=` to a subdirectory
that selects the specific file.

## PII safety

The bundle exports **only** aggregate statistics. This is the core safety
invariant — no individual-level data leaves MONA.

| Column type | What gets exported |
|---|---|
| Numeric | min, max, mean, sd, quantiles (each ±0.5% noise), null_rate¹ |
| Categorical | frequency table (top 200 groups), `_other` bucket k-censored |
| Opaque (free-text string we don't model) | n_distinct, min/max/mean length, null_rate¹ |
| Date | min, max, quantiles (each ±7-day jitter), date_format, null_rate¹ |
| ID-like | n_distinct, id_subtype, null_rate¹ |

¹ When `0 < null_count < SUPPRESS_K`, both `null_count` and `null_rate`
are omitted from the per-column dict (the `nullable: true` flag stays).
An exact small null-count would expose a handful of outliers.

**Categorical-vs-opaque routing** is decided by the classifier, not by
cardinality at summarize time: regmeta value codes / classifications
or the RTB exact-name backstop yield `categorical`; everything else
falls through to `opaque`. The categorical frequency query is capped
at 200 groups (`categorical_freqs_sql`); higher-cardinality columns
that legitimately want frequency tables need a manual override and
will hit that cap.

**`SUPPRESS_K` (default 10).** Frequency-table cells with counts below
`SUPPRESS_K` fold into a single `_other` bucket. The `_other` bucket
itself is k-anonymized: when `0 < other < SUPPRESS_K`, the bucket is
dropped entirely (consumers default its weight to 0). Override
per-column via `mock_data_config.json`'s `column_options[<glob>][<col>]
.suppress_k` — values must be ≥ the global `SUPPRESS_K`, so the
override can only *raise* the threshold, never lower it.

**Date jitter (`DATE_JITTER_DAYS = 7`).** `min`/`max`/quantiles for
date columns are perturbed by a deterministic uniform jitter of ±7
days. Quantiles are estimated from the per-column sample in Python
rather than via SQL — server-side `DATEDIFF` would need a per-dialect,
per-storage-format dance (DATE vs YYYYMMDD-int vs YYYY-MM-DD-string)
that buys nothing because the sample is already on the wire.

### Pre-export PII scanner

`scan.write_export(path, payload)` is the *only* code path that writes
files leaving the bundle's `output_dir`: `mock_data_stats.json` (extract mode)
and `mock_data_discovery.json` (discover mode) both go through it.
`mock_data_config.json` is an *input* and isn't covered by this scanner.
The scanner is defense-in-depth on top of the per-type branches in
`summarize.py`, which already only emit aggregates by construction.
It exists for the case where a misclassified column (e.g.
`FelPersonNr` flickering into `categorical` in tests / dev), would
route raw values into a frequency table.

Patterns applied (compiled at import):

- Swedish personnummer (12-digit YYYYMMDDXXXX and 10-digit
  YYMMDD-XXXX / YYMMDD+XXXX), with date-validity gate AND Luhn check.
- Email address (conservative shape).
- Swedish mobile number (07X / +46-7X prefixes).

Numeric scalars are **not** scanned by default — counts that happen
to be 8–12 digits long would false-positive without telling us
anything useful. Strings only.

Flow:

1. Stamp an in-band `pii_scan: {scanner_version, patterns_applied,
   matches_found: 0}` attestation into the payload.
2. Serialise to `<path>.tmp`.
3. Walk all string-typed values *and* string-typed dict keys; collect
   matches.
4. Clean → `os.replace(tmp, path)` (atomic). Match → `unlink(tmp)`,
   raise `PIIScannerError`. The canonical path is **never** created
   on a match.

A standalone `mock-data-wizard scan <file>` re-runs the same scanner
against an existing JSON file (`--keep` to inspect without deleting).

**Small-population warning:** If a source has fewer than
`SMALL_POP_MULT × SUPPRESS_K` rows (default 200), the bundle emits a
warning. This catches narrowed populations — a `WHERE` clause or
`include` list that collapses the source to a handful of individuals
can leave aggregates effectively identifiable even after cell
suppression. The warning doesn't block; it surfaces the risk.

## Generation strategy

| Type | Method |
|---|---|
| Numeric | `normal(mean, sd)` clamped to `[min, max]` |
| Categorical (with frequencies) | Sample from frequency weights |
| Categorical (with regmeta codes) | Sample from regmeta value set |
| Opaque (high-cardinality string we don't model) | `val_000001` placeholders |
| Date | Uniform between min and max |
| Shared ID | Shared pool of synthetic IDs across files |
| Nulls | Boolean mask at observed `null_rate` |

## Determinism and seeding

All randomness is seeded. Sub-seeds are derived via
`sha256(f"{master_seed}:{file}:{column}")`. Same seed produces identical
output. This makes mock data reproducible for CI and testing.

The extract step has a separate `CLASSIFIER_SEED` (default `0`,
exposed at the top of the bundle) that controls the per-column sample
used by `_pre_classify` to infer column type. The DuckDB branch uses
`USING SAMPLE reservoir(N ROWS) REPEATABLE (seed)` so the sample is
content-addressed and reproducible. The MSSQL branch orders by
`HASHBYTES('SHA1', CAST(col AS NVARCHAR(MAX)))` instead — same data
yields the same row order regardless of physical layout, so
same-shape sibling tables (e.g. `lisa_2015` … `lisa_2019`) classify
the same column the same way. Earlier runs used `LIMIT 1000` /
`TOP 1000` with no order, which produced flicker like `Distriktskod`
classifying as `date` in some LISA years and `opaque` in
others — that flicker is gone.

## Population spine

Birth-invariant attributes (Kön, Födelseår, Födelselän, Födelseland) are
generated once per individual and reused across files. Without this, the
same person could have different sex or birth year in different files.

Spine-eligible variables are a hardcoded set of regmeta `var_id`s. The
authority file (which stats drive generation) is selected by highest
`n_distinct` for the shared ID column — proxy for largest population.

Without regmeta enrichment, the spine is empty and behavior is identical
to pre-spine generation.

## CVID picker

A `var_id` can resolve to multiple `cvid`s under the same register — the
SCB metadata carries one CVID per coding-scheme version (e.g. SUN2000 vs
SUN2020 for variable 784 "Yrkesinriktning") and per register version
(e.g. Kommun in 2019 vs 2020). The picker chooses one.

Tiered scoring per `(cvid)` candidate: `(year_known, -year_distance,
shared_tokens, prefix_hits, overlap, code_count)`. Earlier tiers
dominate; later fields break ties within a tier.

1. **Year match.** When both the source carries a `year` (set in
   `source_detail` from a name regex or `mock_data_config.json`'s `sources`
   block) and the CVID's `register_version.registerversionnamn` parses
   as a year, prefer the CVID whose year is closest. Exact match
   beats "close"; "close" beats CVIDs with no year info. Year ranks
   above name because for register-version drift the wrong year's
   labels are wrong, not merely under-precise. When either side has
   no year, this tier is neutral and the next tiers decide.
2. **Name / classification.** Tokenize the column name and the CVID's
   `(classification.short_name, vardemangdsversion)` strings using a
   camelcase regex with four alternatives: `[A-Z]+(?=[A-Z][a-z])` (run
   before a CamelCase boundary), `[A-Z]+(?![a-z])` (run not followed by
   lowercase, capturing all-caps abbreviations like `SUN`/`SSYK`/`SNI`),
   `[A-Z]?[a-z]+` (a normal word), and `\d+` (digits). Inputs are first
   NFKD-folded and stripped of combining marks so `Kön` and `Kon`
   produce the same tokens — SCB column names typically drop diacritics
   while regmeta labels keep them. Shared-token count is the primary
   signal; **prefix containment** in either direction is the secondary
   fallback (catches Swedish compound splits like `FamSt` ↔
   `FamiljeStallningKod`). Free infix matching is deliberately avoided
   — `btyp` should not match `aktivitetstyp`. Tokens shorter than 2
   chars are dropped.
3. **Code-set overlap (last resort).** When no CVID has any year or
   name signal, fall back to overlap between observed codes and the
   CVID's code set. Requires `overlap / max(|observed|, 1) >=
   MIN_OVERLAP_RATIO` (default `0.5`); below the floor, no entry is
   emitted. This avoids enriching e.g. a 3-digit BTYP column with a
   4-letter FamStF code universe just because it's the only candidate.
   **Tradeoff:** the 50% floor will suppress legitimate enrichment for
   cohort or sample columns that only observe a fraction of the
   universe. The principled fix is wider classification metadata (so
   tier 2 fires); the floor is the safety net while metadata coverage
   is incomplete.

When year or name match exists, the picker accepts the CVID even at
zero code overlap — those are the principled signals, and code drift
is already surfaced separately by the value-code drift warnings.
Callers must therefore treat enrichment's `value_codes` as a
coding-scheme hint, not a validation of the observed code set.

## Panels (`mdw_config.panels`)

Many SCB datasets have **panel structure** — the same person (or firm,
or family) appears across multiple time periods. Mock data preserves
this structure when the user declares panels in `mock_data_config.json`:

```json
{
  "panels": [
    {
      "panel_id": "swecov_inpatient",
      "panel_key": "P1105_LopNr_PersonNr",
      "members": [{"source": "SWECOV_SOS_SV", "time_key": "AR"}]
    },
    {
      "panel_id": "lisa",
      "panel_key": "P1105_LopNr_PersonNr",
      "members": [
        {"source": "lisa_2018.csv", "period": 2018},
        {"source": "lisa_2019.csv", "period": 2019}
      ]
    }
  ]
}
```

A panel is `(panel_id, panel_key, members)`. Each member declares its
source plus exactly one of:

- **`period`**: a literal integer for a one-period-per-file delivery.
  ``n_rows`` and ``n_panel_ids`` come from the source's row count and
  the panel-key column's ``n_distinct``.
- **`time_key`**: a column on the source whose values carry the period
  for each row. Extract runs `GROUP BY time_key` to get per-period
  ``n_rows`` and ``COUNT(DISTINCT panel_key)``.

Mixing the two within one panel is allowed: a long history in one
merged file with a `year` column plus the most recent year as a
separate file, joined under one panel_id, is a valid configuration.

Periods with `n_panel_ids < SUPPRESS_K` are dropped — a tiny panel
cohort is identifying.

`period` is normalised at extract time: integer-valued time_keys (years
in the dominant case, or numeric strings like `"2018"`) become `int`;
anything else (date / quarter strings like `"2019-Q1"`) is preserved as
`str` so sub-annual panels don't crash the run.

Source-collision rule: a source may participate in **at most one panel**.
`parse_config` rejects two panels claiming the same source so a
generator-side `panel_by_source` collision can't silently drop the
second panel's behavior. Multi-key panels (one source, two panel_keys)
are explicitly out of scope.

`mock_data_stats.json` carries a top-level `panels` array that mirrors
the config schema, plus the per-period stats in `by_period`:

```json
{
  "panels": [
    {
      "panel_id": "swecov_inpatient",
      "panel_key": "P1105_LopNr_PersonNr",
      "members": [{"source": "SWECOV_SOS_SV", "time_key": "AR"}],
      "by_period": [
        {"period": 2018, "source": "SWECOV_SOS_SV", "n_rows": 1234567, "n_panel_ids": 980000},
        ...
      ]
    }
  ]
}
```

Generation: panels sharing a `panel_key` share one id pool — in SCB
data nearly every register is keyed on the same person id (e.g.
`P1105_LopNr_PersonNr`), and that's *the* id universe, not many
parallel ones. Each pool is deterministically shuffled per panel_key
and sized to `max(n_panel_ids)` across every period of every panel
using that key. Each `(period, source)` entry takes a *prefix* of
the pool sized to that entry's own `n_panel_ids`. Strict prefix
nesting gives stable cross-period overlap (panel persistence) —
sequential periods share `min(n_panel_ids)` of their ids — and
per-source distinctness matches stats (a smaller member contributing
to the same period draws fewer distinct panel-keys, not the
sibling's larger count). The panel pool also overrides
`shared_pools[panel_key]` so non-panel sources sharing the same column
draw from the same id universe.

For column-members (a `time_key` column inside one source),
`(time_key, panel_key)` are co-generated per row: each row's period is
drawn from `n_rows[t]` weights, and its panel-key value comes from
that period's subset. For file-members (one source IS one period),
panel-key values come straight from the `(panel_id, period, source)`
subset. Mixed panels (some members file, some column) work because
every `by_period` entry carries a `source`, so subsets never bleed
across members.

**Out of scope:** cross-period transition matrices, attrition / re-entry
modelling, and multi-key panels. The "fixed pool with shrinking active
prefix" model is good enough for mock-data fidelity — downstream
panel-regression code sees correct id stability without us having to
build a full demographic model.

## Value code drift warnings

After enrichment, frequency codes from stats are cross-checked against
regmeta value sets. Codes absent from the value set trigger stderr
warnings. This catches column name typos and wrong-year stats exports.

Warnings don't block generation. Unseen regmeta codes (codes in metadata
but absent from stats) are deliberately not warned on — registers
legitimately contain rare codes.

## Manifest

Generation produces a `manifest.json` alongside the mock CSVs. The
manifest includes per-source column lists, register and year hints, and
header hashes. `mock-data-wizard compare` reads this to verify local
files against registry schema without requiring separate input.

## Stale-file handling on regenerate

When `generate` runs into an output directory that already contains
files, it warns about any file that would no longer be produced but
leaves them on disk by default. Pass `--force` to remove stale files.

This matters because `SOURCES` can shrink between runs (e.g., the user
dropped a `sql_source` they no longer need). Silently deleting
previously-generated mock CSVs from that run would surprise downstream
code that still references them. Warn-and-keep is the safer default;
`--force` is the explicit opt-in to clean up.

## Register hint confidence

`register_hint` is set per file by voting on the register that resolves
the most column names. Files where the top register covers fewer than
40% of the file's non-id columns emit `register_hint: null` instead of a
low-confidence winner. Candidates (with `match_count` and
`total_nonid_cols`) are always written to `register_hint_candidates` so
downstream tooling can surface the ambiguity instead of silently
mislabeling the file.

## Web UI

`mock-data-wizard ui <project_dir>` starts a local HTTP server that
exposes the editor API and serves a Svelte SPA from
`src/mock_data_wizard/static/`. Single-project per server: each invocation
binds to one `project_dir` and survives until Ctrl-C.

**Binding + safety.** Default `--host 127.0.0.1`. Non-loopback hosts
(including `0.0.0.0`) are rejected at parse time unless `--unsafe-host`
is also passed. The bind family (`AF_INET` vs `AF_INET6`) is picked
from `getaddrinfo` rather than the host string, so loopback hostnames
that resolve only to IPv6 (`ip6-localhost`, or `localhost` on
IPv6-only setups) bind correctly; literal `::1` always uses IPv6.
There is no auth — local-only binding is the only control. Document
this explicitly in any deploy notes.

POST bodies are capped at 1 MiB (`_MAX_REQUEST_BYTES`); oversized
`Content-Length` headers are rejected with a 413 envelope before any
body bytes are read.

**HTTP surface (v1).**

| Method | Path | Editor call | Errors |
| --- | --- | --- | --- |
| GET  | `/api/state` | `editor.get_state` | 404 not_initialized |
| POST | `/api/init` | `editor.init_if_missing` | 404 not_initialized (no discover file), 400 validation |
| POST | `/api/column-type` | `editor.set_column_type` | 400 validation, 409 stale_state |
| POST | `/api/group-register` | `editor.set_group_register` | 400 validation, 409 stale_state |
| GET  | `/api/registers` | `editor.list_registers` | — |
| GET  | `/`, `/assets/*` | static SPA bundle | 404 not_found |

`POST /api/init` is idempotent: runs `init_if_missing` against
`project_dir/mock_data_discovery.json`. If the config already exists,
returns the current snapshot. The endpoint exists so the SPA can offer
a one-click bootstrap when `GET /api/state` returns `not_initialized` —
without it, a fresh project lands on a hard error and the user has to
drop into Python. Overwrite mode is deliberately not exposed; clobbering
manual edits requires deleting the file.

Error envelope: `{"error": {"code", "message", "context?"}}`. The 409
`stale_state` envelope carries the fresh `StateSnapshot` in
`context.fresh_state` so the SPA can re-apply without an extra GET. If
the post-stale `get_state` itself fails (config deleted mid-flight),
the 409 is still returned without `fresh_state`.

**Concurrency.** `ThreadingHTTPServer` runs one thread per request
because the SPA fires the four GET endpoints in parallel on first load.
Mutations are still serialised by the editor's `_config_lock` (fcntl on
a sidecar file), so the server doesn't need a top-level lock.

**Static asset shipping.** The frontend lives in `mock_data_wizard/web/`
(plain Svelte 5 + Vite + TS, bun-managed). `vite build` writes to
`../src/mock_data_wizard/static/` and that directory is committed —
hatchling auto-bundles non-Python files in the package, so the wheel
ships pure-Python with the SPA inside it. Editable installs serve the
on-disk directory directly, so `bun run dev` (or a fresh `bun run
build`) is live immediately. CI rebuilds the bundle and fails the PR if
`static/` differs from a clean rebuild — that is the drift guard.

**Wire format contract.** `_serialize.state_snapshot_to_dict` converts
the editor's frozen dataclasses to JSON-safe dicts; the frontend's
`web/src/lib/types.ts` mirrors that shape by hand. Drift is caught by a
golden-fixture test (`tests/data/state_snapshot.golden.json`): the
Python side serializes a deterministic synthetic project (regmeta
stubbed) and diffs against the committed JSON; the Bun side parses the
same file via `isStateSnapshot`. Update with
`uv run pytest tests/test_serialize.py::test_golden_fixture_matches --update-golden`,
then update `types.ts` until both tests pass.

**v1 mutators.** `set_column_type` and `set_group_register` only.
Source year, column options, panel CRUD, and `init_if_missing`
overwrite are deferred to v2.

## Deliberate exclusions

- Household structures, time-varying attributes, employer links
- HTTP portal for metadata browsing
- Per-column type info in manifest (misleading for mock data)
