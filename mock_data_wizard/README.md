# mock_data_wizard

Generate mock CSV data from MONA project metadata without exporting
personal data. Designed for LLM agent consumption; terminal use is
secondary.

## Install

```bash
uv tool install mock-data-wizard
```

Requires `reg_meta` for metadata enrichment (population spine, value
code validation, compare). Install reg_meta first — see
[reg_meta/README.md](../reg_meta/README.md).

Upgrade with `mock-data-wizard update`. The CLI also checks for a newer
version on startup and points at this command if one is available.

## Quick start

```bash
# Step 1: Build the MONA extract bundle
mock-data-wizard build-bundle
# Writes mdw_runner.py in the current directory.

# Upload the bundle to MONA. The first run is a discovery pass — the
# bundle lists available files and SQL tables into a timestamped sidecar
# `mdw_sources_<YYYYMMDD_HHMMSS>.py` alongside the script, then exits.
# Edit the sidecar to narrow each source to what you want, then re-run
# `python mdw_runner.py` — it auto-loads the latest sidecar
# and produces mdw_step3_stats.json. (Once a sidecar exists it overrides `configure()`;
# delete the sidecar if you want `configure()` edits to take effect again.)
# IMPORTANT: verify mdw_step3_stats.json contains no PII.

# Step 2: Generate mock CSV files locally
mock-data-wizard generate --stats mdw_step3_stats.json --seed 42

# Optional: compare mock data against registry schema
mock-data-wizard compare manifest.json
```

Use `--help` on any command for full flag documentation.

## Data sources

The bundle exposes a `configure()` function near the top of
`mdw_runner.py`. Edit it to declare what to aggregate.
Two constructors are available:

```python
def configure():
    return [
        file_source(
            path=r"\\micro.intra\projekt\P1405$\P1405_Data",
            include=("lisa_2020.csv", "lisa_2021.csv"),  # optional subset
        ),
        sql_source(
            dsn="P1405",  # Windows System DSN (no password here)
            tables=(
                sql_table("dbo.persons", where="year >= 2020"),
                sql_table("dbo.events", where="INDATUM > '2020-01-01'"),
                "dbo.fodelse",  # plain string -> no filter
            ),
        ),
    ]
```

Discovery mode: when a source has no filtering info (no `include`/
`tables`/`pattern`/`queries`), the bundle writes a timestamped
`mdw_sources_<YYYYMMDD_HHMMSS>.py` sidecar listing everything
discoverable, and exits without writing `mdw_step3_stats.json`. Edit the sidecar
to narrow each source to the items you want, then re-run the bundle —
it auto-loads the sidecar on the next run (no copy-paste back into
`configure()`). Delete the sidecar to re-discover. A source that can't
be reached on this project (e.g., a DSN that doesn't exist) fails
gracefully and is omitted from the suggestion.

Want everything without the discovery dance? Pass `all=True`:

```python
def configure():
    return [
        file_source(path="...", all=True),  # every matching CSV/TXT in path
        sql_source(dsn="...", all=True),    # every non-archived view in the DSN
    ]
```

## Commands

| Command | Purpose |
|---|---|
| `build-bundle` | Build the single-file Python bundle to upload to MONA |
| `generate` | Produce mock CSV files from mdw_step3_stats.json |
| `compare` | Compare local file columns against registry metadata |

## PII safety

The bundle exports **only** aggregate statistics (counts, means,
frequencies). Cells with 5 or fewer individuals are censored. No
individual-level data leaves MONA. See [DESIGN.md](DESIGN.md) for
the full safety specification.

## Files

| Path | Purpose |
|---|---|
| [DESIGN.md](DESIGN.md) | Design rationale, PII safety rules, generation strategy |
| `src/mock_data_wizard/` | Package source |
| `tests/` | Test suite |
