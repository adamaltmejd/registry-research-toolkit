# regmeta

Search and query SCB registry metadata from a local SQLite database. Designed for programmatic use by LLM agents and tools; terminal use is secondary.

## Setup

```bash
# 1. Export CSV files from mikrometadata.scb.se
# 2. Build the database
uv run regmeta maintain build-db --csv-dir path/to/SCB-data/

# 3. Query
uv run regmeta search --query "kommun" --datacolumn     # find column headers
uv run regmeta search --query "inkomst"                  # search all fields
uv run regmeta search --query "0180" --value             # find value codes
uv run regmeta get register LISA
uv run regmeta get schema --register LISA --years 2020-2023
uv run regmeta get varinfo "Kön" --register LISA
uv run regmeta get values 12345 --valid-at 2020-01-01     # temporally filtered values
uv run regmeta get datacolumns "Kommun"                  # all column aliases
uv run regmeta get coded-variables --min-registers 5     # variables with value sets
uv run regmeta resolve --columns "Kon,FodelseAr" --register LISA
```

## Commands

| Command | Purpose |
|---|---|
| `search` | Search across all fields (default), or narrow with `--datacolumn`, `--varname`, `--description`, `--value` |
| `get register` | Register overview with variants (by name or ID) |
| `get schema` | Column listing per version (by variant ID or `--register`, filterable by `--years`) |
| `get varinfo` | Variable details with instance history (by name or var_id) |
| `get values` | Value-set members (code + label) for a CVID, with optional `--valid-at` date filter |
| `get datacolumns` | All column aliases a variable appears under across registers |
| `get coded-variables` | Variables with value sets, ranked by usage (`--min-codes`, `--min-registers`) |
| `resolve` | Exact alias lookup for column names (batch, via `--columns` or stdin) |
| `maintain build-db` | Build database from SCB CSV exports |
| `maintain info` | Database stats and import metadata |

All commands support `--format json` (default) and `--format table`. Register arguments accept IDs, exact names, or substring matches.

## Known Limitations

1. **Value sets are not version-specific.** Värdemängder attaches a historical union of all code definitions to every CVID regardless of year. Use `get values --valid-at <date>` to filter by temporal validity (requires `VardemangderValidDates.csv` in the build). See SPEC §6.4.
2. **Timeseries data is sparse.** Only 155 events across all 238 registers. Not a reliable source for change history.
3. **Database size.** ~13 GB with full value-set import (~96M rows). Build takes ~17 minutes.
4. **No network calls.** V1 is local-only. User must manually export CSVs from mikrometadata.scb.se.

## Files

- [SPEC_regmeta.md](SPEC_regmeta.md) — Product specification
- [PLAN_regmeta.md](PLAN_regmeta.md) — Implementation tracker
- [STRUCTURE.md](STRUCTURE.md) — Domain model documentation
- `docs/` — Discovery evidence and notes
- `src/regmeta/` — Package source
- `tests/` — Test suite
