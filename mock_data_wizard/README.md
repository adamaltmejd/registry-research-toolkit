# mock_data_wizard

Generate mock CSV data from MONA project metadata without exporting personal data. Designed for LLM agent consumption; terminal use is secondary.

## Setup

```bash
# Step 1: Generate an R script to run on MONA
uv run mock-data-wizard generate-script -p P1405
# Upload the script to MONA. Run it: Rscript extract_stats_P1405.R
# Download the resulting stats.json. Verify it contains no PII.

# Step 2: Generate mock CSV files from the stats
uv run mock-data-wizard generate --stats stats.json --seed 42
```

## Commands

| Command | Purpose |
|---|---|
| `generate-script` | Create an R script that extracts aggregate statistics on MONA |
| `generate` | Produce mock CSV files from stats.json output |

### generate-script flags
- `--project`, `-p` — SCB project number (e.g. P1405)
- `--project-dir` — Custom data path(s) to scan
- `--output`, `-o` — Output path for the R script

### generate flags
- `--stats` — Path to stats.json (default: `stats.json`)
- `--seed` — Random seed for reproducible output (default: 42)
- `--sample-pct` — Fraction of rows to generate (default: 1.0)
- `--output-dir` — Directory for generated CSV files (default: `mock_data`)
- `--db` — Path to regmeta database directory
- `--register` — Filter regmeta matches to a specific register
- `--no-regmeta` — Skip regmeta enrichment
- `--force` — Overwrite existing output directory (stale files are removed)
- `-y`, `--yes` — Skip confirmation prompt
- `-v`, `--verbose` — Show per-file timing breakdown

## PII Safety

The R script exports **only** aggregate statistics (counts, means, frequencies). Cells with 5 or fewer individuals are censored. No individual-level data leaves MONA.

## Files

- [SPEC_mock_data_wizard.md](SPEC_mock_data_wizard.md) — Product specification
- [PLAN_mock_data_wizard.md](PLAN_mock_data_wizard.md) — Implementation tracker
- `src/mock_data_wizard/` — Package source
- `tests/` — Test suite (40 tests)
