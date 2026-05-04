# Design: mock_data_wizard

Design rationale and constraints. For usage, see `mock-data-wizard --help`.

## Workflow

The bundle has two modes (`MODE = "discover"` / `"extract"`) and the
end-to-end loop crosses the MONA boundary three times:

1. `mock-data-wizard build-bundle` (local) — amalgamates the runtime
   modules into a single `mock_data_wizard_extract.py` for MONA upload.
2. **Discover** on MONA. Edit `configure()` in the bundle, leave
   `MODE = "discover"`, upload, and run:
   `python mock_data_wizard_extract.py` — writes `discover.json`
   (metadata only: column names, SQL types, row counts; no values, no
   distinct counts, no samples).
3. `mock-data-wizard configure discover.json` (local) — reads
   `discover.json`, applies the name-pattern classifier (`lopnr → id`,
   `*Datum* → date`, `*Belopp* → numeric`, …), defaults the rest to
   `high_cardinality`, and writes `mdw_config.json`. The user reviews
   and edits this file by hand before re-uploading.
4. **Extract** on MONA. Switch the bundle's `MODE = "extract"`, place
   `mdw_config.json` next to it, re-run on MONA — writes `stats.json`
   (only aggregate statistics; the configured types drive per-column
   SQL with no data-driven classifier pass).
5. `mock-data-wizard generate` (local) — produces mock CSVs from
   `stats.json`.

Why three trips. Discover is metadata-only and PII-safe by
construction; running it first means the per-column type assignment
happens locally where regmeta and human review are available. Extract
is the slow part (full-population aggregation) — splitting it out
means each iteration of the type config doesn't pay 20-hour-class
re-runs to fix a misclassified column. The earlier R-script-generation
approach is preserved in git history; the runtime is Python going
forward.

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

Output is `discover.json`:

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
`stats.json` — column names and `sql_type` strings are unlikely to
contain personnummer, but defense-in-depth is cheap.

### Extract mode (`MODE = "extract"`)

The bundle's second MONA trip. Requires `mdw_config.json` next to
the bundle. Source filtering is **strict** here — `sql_source` must
declare `tables=`, `pattern=`, or `all=True`; the permissive
unfiltered mode is discover-only. Every column the source yields must
have a type override in `mdw_config.json`; an unconfigured column
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

The clause is recorded in `source_detail.where` in `stats.json` so the
downstream `generate` step can echo it (e.g., apply the same year
filter to the mock data range).

### Per-column type config via `mdw_config.json`

Authored by `mock-data-wizard configure` from a `discover.json` and
uploaded next to the bundle. Extract mode is strict: every column on
every source must carry a type entry, the schema is validated on load,
and typos error out instead of getting silently dropped.

```json
{
  "contract_version": "mdw-config-1.0.0",
  "column_types": {
    "Population_PersonNr_*": {
      "FelPersonNr": {"type": "high_cardinality"},
      "BirthDate": {"type": "date", "date_format": "%Y%m%d"},
      "Salary": {"type": "numeric", "numeric_subtype": "integer"}
    },
    "Individ_*": {
      "Distriktskod": {"type": "high_cardinality"}
    }
  },
  "column_options": {
    "Population_*": {
      "Salary": {"suppress_k": 20}
    }
  },
  "sources": {
    "Individ_2018": {"year": 2018},
    "Individ_2019": {"year": 2019}
  }
}
```

- Table-glob keys use `fnmatchcase` semantics. Multiple globs may
  match a `source_name`; **insertion order matters and last match
  wins**. List broad globs first (`lisa_*`) and specific overrides
  below them (`lisa_2018`). The same precedence applies in both
  `column_types` and `column_options`.
- Each `column_types` entry's `type` is required and must be one of
  `id`, `categorical`, `numeric`, `high_cardinality`, `date`. Inline
  subtype/format hints are optional and only valid for the matching
  type. When *any* inline hint is supplied, the bundle skips the
  per-column sample query for that column entirely — that is the
  perf win the override is for.
- Each output column carries `source_of_type: "override"` so
  downstream consumers can audit that every column went through the
  config — the extract path has no auto-classifier fallback.
- `column_options` is a separate namespace reserved for non-type
  overrides (e.g. `suppress_k` for disclosure-control hardening).
  Validated here; consumed in `summarize`. Each option key is
  checked against `VALID_OPTION_KEYS` and the option's own
  invariants. `suppress_k` in particular is floored at the global
  `SUPPRESS_K` — overrides may only *raise* the disclosure-control
  threshold for a column, never lower it. A typo'd `0` would
  otherwise turn the override into a fail-open path.
- `sources` is a third namespace, keyed by *exact source name* (no
  glob — year is per-source, not per-source-class). Currently only
  `year`; populated by the configurer from a 4-digit name regex and
  editable to fix mis-detections. An explicit `"year": null`
  suppresses the regex fallback (the user is asserting "no year").
  Read by `enrich.py` to bias CVID picking toward the right register
  version (see CVID picker tier 1).

Strict validation: unknown types, unknown option keys, duplicate JSON
keys, schema-version mismatches, and stray fields all raise. The
configurer file is meant to be hand-edited, so silent drops would
mask user typos.

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
| Low-cardinality categorical | frequency table, `_other` bucket k-censored |
| High-cardinality string | n_distinct, min/max/mean length, null_rate¹ |
| Date | min, max, quantiles (each ±7-day jitter), date_format, null_rate¹ |
| ID-like | n_distinct, id_subtype, null_rate¹ |

¹ When `0 < null_count < SUPPRESS_K`, both `null_count` and `null_rate`
are omitted from the per-column dict (the `nullable: true` flag stays).
An exact small null-count would expose a handful of outliers.

**Low-cardinality threshold:** `n_distinct <= min(50, n_rows * 0.01)`.

**`SUPPRESS_K` (default 10).** Frequency-table cells with counts below
`SUPPRESS_K` fold into a single `_other` bucket. The `_other` bucket
itself is k-anonymized: when `0 < other < SUPPRESS_K`, the bucket is
dropped entirely (consumers default its weight to 0). Override
per-column via `mdw_config.json`'s `column_options[<glob>][<col>]
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
files leaving the bundle's `output_dir`: `stats.json` (extract mode)
and `discover.json` (discover mode) both go through it.
`mdw_config.json` is an *input* and isn't covered by this scanner.
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
| High-cardinality string | `val_000001` placeholders |
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
classifying as `date` in some LISA years and `high_cardinality` in
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
   `source_detail` from a name regex or `mdw_config.json`'s `sources`
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
this structure when the user declares panels in `mdw_config.json`:

```json
{
  "panels": [
    {
      "panel_id": "swecov_inpatient",
      "layout": "merged_table",
      "source": "SWECOV_SOS_SV",
      "panel_key": "P1105_LopNr_PersonNr",
      "time_key": "AR"
    },
    {
      "panel_id": "lisa",
      "layout": "separate_files",
      "panel_key": "P1105_LopNr_PersonNr",
      "members": [
        {"source": "lisa_2018.csv", "period": 2018},
        {"source": "lisa_2019.csv", "period": 2019}
      ]
    }
  ]
}
```

Two layouts share one downstream representation. Extract:

- **`merged_table`**: one extra `GROUP BY time_key` query per panel,
  yielding `n_rows` and `COUNT(DISTINCT panel_key)` per period.
- **`separate_files`**: each member is a separate source whose
  panel-key column's `n_distinct` already gives `n_panel_ids` for that
  period. No extra query.

Periods with `n_panel_ids < SUPPRESS_K` are dropped — a tiny panel
cohort is identifying.

`stats.json` carries a top-level `panels` array, decoupled from
`sources`:

```json
{
  "panels": [
    {
      "panel_id": "swecov_inpatient",
      "panel_key": "P1105_LopNr_PersonNr",
      "layout": "merged_table",
      "source": "SWECOV_SOS_SV",
      "time_key": "AR",
      "by_period": [{"period": 2018, "n_rows": 1234567, "n_panel_ids": 980000}, ...]
    }
  ]
}
```

Generation: each panel gets a deterministically-shuffled id pool sized
to `max(n_panel_ids)`, and each period takes a *prefix* of the pool
sized to that period's `n_panel_ids`. Strict prefix nesting gives
stable cross-period overlap (panel persistence) — sequential periods
share `min(n_panel_ids)` of their ids. The panel pool also overrides
`shared_pools[panel_key]` so non-panel sources sharing the same column
draw from the same id universe.

For `merged_table`, `(time_key, panel_key)` are co-generated per row:
each row's period is drawn from `n_rows[t]` weights, and its panel-key
value comes from that period's subset. For `separate_files`, each
source IS one period, so panel-key values come straight from the
period subset.

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

## Deliberate exclusions

- Household structures, time-varying attributes, employer links
- Interactive wizard / state machine
- HTTP portal for metadata browsing
- Per-column type info in manifest (misleading for mock data)
