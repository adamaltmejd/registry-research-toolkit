# Design: mock_data_wizard

Design rationale and constraints. For usage, see `mock-data-wizard --help`.

## Two-step workflow

1. `generate-script` produces an R script to run on MONA
2. User runs the script on MONA, exports `stats.json`
3. `generate` produces mock CSVs locally from `stats.json`

This separation exists because MONA has no internet access and no Python.
The R script runs inside MONA; everything else runs locally.

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
