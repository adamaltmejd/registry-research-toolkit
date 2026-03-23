# regmeta

Search and query SCB registry metadata from a local SQLite database. Designed for programmatic use by LLM agents and tools; terminal use is secondary.

## Setup

```bash
# Download the pre-built database (~400 MB download, ~1.6 GB on disk)
regmeta maintain download

# Query
regmeta search --query "kommun" --datacolumn     # find column headers
regmeta search --query "inkomst"                  # search all fields
regmeta search --query "0180" --value             # find value codes
regmeta get register LISA
regmeta get schema --register LISA --years 2020-2023
regmeta get varinfo "Kön" --register LISA
regmeta get values 12345 --valid-at 2020-01-01     # temporally filtered values
regmeta get datacolumns "Kommun"                  # all column aliases
regmeta get coded-variables --min-registers 5     # variables with value sets
regmeta resolve --columns "Kon,FodelseAr" --register LISA
```

Alternatively, build from raw SCB CSV exports (requires access to mikrometadata.scb.se):

```bash
regmeta maintain build-db --csv-dir path/to/SCB-data/
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
| `get diff` | Compare schema between two years for a register |
| `get lineage` | Track variable provenance across registers |
| `resolve` | Exact alias lookup for column names (batch, via `--columns` or stdin) |
| `maintain download` | Download pre-built database from GitHub Releases |
| `maintain build-db` | Build database from SCB CSV exports |
| `maintain info` | Database stats and import metadata |

All commands support `--format json` (default) and `--format table`. Register arguments accept IDs, exact names, or substring matches.

## Database

The database is stored at `$XDG_DATA_HOME/regmeta/regmeta.db` (default: `~/.local/share/regmeta/`). Override with `$REGMETA_DB` or `--db`.

## Known Limitations

1. **Value sets are not version-specific.** Värdemängder attaches a historical union of all code definitions to every CVID regardless of year. Use `get values --valid-at <date>` to filter by temporal validity (requires `VardemangderValidDates.csv` in the build). See SPEC §6.4.
2. **Timeseries data is sparse.** Only 155 events across all 238 registers. Not a reliable source for change history.
3. **Database size.** ~1.6 GB on disk. The compressed download is ~400 MB.
4. **Network calls limited to `maintain download`.** All query commands are fully offline.

## Files

- [SPEC_regmeta.md](SPEC_regmeta.md) — Product specification
- [PLAN_regmeta.md](PLAN_regmeta.md) — Implementation tracker
- [STRUCTURE.md](STRUCTURE.md) — Domain model documentation
- `docs/` — Discovery evidence and notes
- `src/regmeta/` — Package source
- `tests/` — Test suite
