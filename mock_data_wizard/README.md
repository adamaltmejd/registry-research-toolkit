# mock_data_wizard

Generate mock CSV data from MONA project metadata without exporting
personal data. Designed for LLM agent consumption; terminal use is
secondary.

## Install

```bash
uv tool install mock-data-wizard
```

Requires `regmeta` for metadata enrichment (population spine, value
code validation, compare). Install regmeta first — see
[regmeta/README.md](../regmeta/README.md).

Upgrade with `mock-data-wizard update`. The CLI also checks for a newer
version on startup and points at this command if one is available.

## Quick start

```bash
# Step 1: Generate an R script to run on MONA
mock-data-wizard generate-script -p P1405

# Upload and run on MONA. The first run is a discovery pass — the
# script lists available files and SQL tables into a timestamped file
# `mdw_sources_<YYYYMMDD_HHMMSS>.R` alongside the script, then exits.
# Edit that file to narrow each source to what you want, then re-run
# the extract script — it auto-loads the file and produces stats.json.
# IMPORTANT: verify stats.json contains no PII.

# Step 2: Generate mock CSV files locally
mock-data-wizard generate --stats stats.json --seed 42

# Optional: compare mock data against registry schema
mock-data-wizard compare manifest.json
```

Use `--help` on any command for full flag documentation.

## Data sources

The generated R script declares sources in a `SOURCES <- list(...)` block
at the top. Two constructors are available:

```r
SOURCES <- list(
  file_source(
    path    = "\\\\micro.intra\\projekt\\P1405$\\P1405_Data",
    include = c("lisa_2020.csv", "lisa_2021.csv")   # optional subset
  ),
  sql_source(
    dsn    = "P1405",                 # Windows System DSN (no password here)
    tables = c("dbo.persons", "dbo.events"),
    where  = list(persons = "year >= 2020"),        # optional, per table
    select = list(persons = c("LopNr", "Kon", "FodelseAr"))  # optional column projection
  )
)
```

`generate-script -p P<num>` emits a skeleton with both sources by default
— the file path follows the MONA convention and the SQL DSN is named
after the project number. Add `--no-sql` to skip the SQL skeleton, or
`--sql-dsn <name>` for a non-default DSN.

Discovery mode: when a source has no filtering info (no `include`/
`tables`/`pattern`/`queries`), the script writes a timestamped
`mdw_sources_<YYYYMMDD_HHMMSS>.R` file listing everything discoverable,
and exits without writing `stats.json`. Edit that file to narrow each
source to the items you want, then re-run the extract script — it
auto-loads the file on the next run (no copy-paste back into the
script). Delete the file to re-discover. A source that can't be
reached on this project (e.g., a DSN that doesn't exist) fails
gracefully and is omitted from the suggestion.

Want everything without the discovery dance? Pass `all = TRUE`:

```r
SOURCES <- list(
  file_source(path = "...", all = TRUE),   # every matching CSV/TXT in path
  sql_source(dsn = "...", all = TRUE)      # every non-archived view in the DSN
)
```

## Commands

| Command | Purpose |
|---|---|
| `generate-script` | Create an R script that extracts aggregate statistics on MONA |
| `generate` | Produce mock CSV files from stats.json |
| `compare` | Compare local file columns against registry metadata |

## PII safety

The R script exports **only** aggregate statistics (counts, means,
frequencies). Cells with 5 or fewer individuals are censored. No
individual-level data leaves MONA. See [DESIGN.md](DESIGN.md) for
the full safety specification.

## Files

| Path | Purpose |
|---|---|
| [DESIGN.md](DESIGN.md) | Design rationale, PII safety rules, generation strategy |
| `src/mock_data_wizard/` | Package source |
| `tests/` | Test suite |
