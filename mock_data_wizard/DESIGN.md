# Design: mock_data_wizard

Design rationale and constraints. For usage, see `mock-data-wizard --help`.

## Two-step workflow

1. `extract` runs on MONA: aggregates configured sources to `stats.json`.
2. User exports `stats.json` from MONA.
3. `generate` runs locally: produces mock CSVs from `stats.json`.

The Python package itself ships to MONA and runs as the extract step;
nothing is generated as a templated R script. This is possible because
the MONA batch client has Python pre-installed (see "MONA Python runtime"
below). The earlier R-script-generation approach is preserved in git
history (commits up to and including the streaming-iterator refactor),
but the runtime is Python going forward.

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
  disk is `stats.json`.
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
  cwd for output. `stats.json` is small enough to live next to the
  script; everything else (DuckDB spill especially) goes to
  `C:\Windows\TEMP`.
- **`locale.getpreferredencoding()` is `cp1252`** — pass `encoding=`
  explicitly on every CSV/text open; do not rely on the default.

Architectural consequence: we ship `mock_data_wizard` to MONA as a
single bundled `.py` file built by an in-repo amalgamator. One file
sidesteps the multi-upload UX, fits the 10 MB cap with two orders of
magnitude to spare, and the "Not UNICODE" line in the upload notice
turns out to be advisory rather than enforced.

## Source model

The R script's `SOURCES <- list(...)` block is the single place users
declare what data to aggregate. Two constructors are available:

- `file_source(path, include, exclude, pattern)` — a directory (or single
  file) of CSV/TXT data.
- `sql_source(dsn, tables, pattern, queries, where, select, ...)` — an
  ODBC-accessible database. On MONA this is MS SQL via a per-project DSN.
  Credentials come from the Windows system DSN; the script never carries
  passwords.

Sources dispatch through `source_iter(src)`, which returns a streaming
iterator `list(n_items, fetch_item(i), close())`. Main pulls one item
at a time, fetching as `list(source_name, source_type, source_detail,
dt)`, runs the classify/summarize pipeline, and drops the table before
the next fetch. Lazy-by-design: peak memory stays near a single table
rather than the sum of all tables in the source, which matters on MONA
projects with hundreds of SQL views.

### Discovery mode

If any source has no filtering info (`file_source` with no include/
exclude/pattern, or `sql_source` with no tables/pattern/queries), the
script writes a timestamped file `mdw_sources_<YYYYMMDD_HHMMSS>.R`
alongside itself and exits without writing `stats.json`. The file
contains a `SOURCES <- list(...)` block listing everything discoverable.

Users who know up-front that they want everything can opt out of
discovery with `all = TRUE` on either constructor: `file_source(path, all
= TRUE)` processes every matching file in `path`; `sql_source(dsn, all =
TRUE)` discovers all non-archived views and processes each. The flag
keeps the in-script `SOURCES` block compact compared to a giant
`include = c(...)` list.

On the next run, the extract script automatically loads the latest
`mdw_sources_*.R` file (sourcing it overrides the in-script `SOURCES`)
and processes normally. The user narrows the list by editing the file
directly — no copy-paste back into the extract script. Deleting the
file(s) triggers a fresh discovery on the next run. If the loaded file
is still in a discovery-triggering state (the user ran discovery but
forgot to narrow a source), the script errors with a message pointing
at the file rather than silently overwriting it.

Discovery failures are tolerated: a `sql_source` pointed at a DSN that
doesn't exist on a given project just emits a `[sql] discovery failed`
comment and the other sources continue. `generate-script -p P<num>`
uses this to emit both a `file_source` and a `sql_source` skeleton by
default — whichever doesn't apply drops itself.

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

The clause is recorded in `source_detail.where` in `stats.json` so the
downstream `generate` step can echo it (e.g., apply the same year
filter to the mock data range).

### File discovery quirks

Two files with the same basename in different subdirectories collide
— `include = c("name.csv")` can't select between them, and they'd
both get `source_name = "name.csv"`. Discovery dedupes basenames in
the written suggestion and warns about the collision; processing fails
fast if the user narrows `include` but the matched files still have
duplicate basenames. The fix is to narrow `path =` to a subdirectory
that selects the specific file.

## PII safety

The R script exports **only** aggregate statistics. This is the core safety
invariant — no individual-level data leaves MONA.

| Column type | What gets exported |
|---|---|
| Numeric | min, max, mean, sd, quantiles, null_rate |
| Low-cardinality categorical | frequency table `{value: count}` |
| High-cardinality string | n_distinct, min/max length, null_rate |
| Date | min, max, null_rate |
| ID-like | n_distinct, null_rate |

**Low-cardinality threshold:** `n_distinct <= min(50, n_rows * 0.01)`.

Cells with 5 or fewer individuals are censored in frequency tables.

**Small-population warning:** If a source has fewer than
`SMALL_POP_MULT × SUPPRESS_K` rows (default 100), the R script emits a
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
| High-cardinality string | `val_000001` placeholders |
| Date | Uniform between min and max |
| Shared ID | Shared pool of synthetic IDs across files |
| Nulls | Boolean mask at observed `null_rate` |

## Determinism and seeding

All randomness is seeded. Sub-seeds are derived via
`sha256(f"{master_seed}:{file}:{column}")`. Same seed produces identical
output. This makes mock data reproducible for CI and testing.

## Population spine

Birth-invariant attributes (Kön, Födelseår, Födelselän, Födelseland) are
generated once per individual and reused across files. Without this, the
same person could have different sex or birth year in different files.

Spine-eligible variables are a hardcoded set of regmeta `var_id`s. The
authority file (which stats drive generation) is selected by highest
`n_distinct` for the shared ID column — proxy for largest population.

Without regmeta enrichment, the spine is empty and behavior is identical
to pre-spine generation.

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

## Deliberate exclusions

- Household structures, time-varying attributes, employer links
- Interactive wizard / state machine
- HTTP portal for metadata browsing
- Per-column type info in manifest (misleading for mock data)
