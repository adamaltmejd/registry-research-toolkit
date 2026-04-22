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

# Upload and run on MONA, download stats.json
# IMPORTANT: verify stats.json contains no PII

# Step 2: Generate mock CSV files locally
mock-data-wizard generate --stats stats.json --seed 42

# Optional: compare mock data against registry schema
mock-data-wizard compare manifest.json
```

Use `--help` on any command for full flag documentation.

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
