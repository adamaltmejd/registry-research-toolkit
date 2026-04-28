# regmeta

Search and query SCB registry metadata from a local SQLite database.
Designed for programmatic use by LLM agents and tools; terminal use
is secondary.

## Install

```bash
uv tool install regmeta
regmeta maintain update      # downloads package + database (~400 MB compressed)
```

Alternatively, build from raw SCB CSV exports (requires access to
mikrometadata.scb.se):

```bash
regmeta maintain build-db --input-dir regmeta/input_data/
```

## Quick start

```bash
regmeta search --query "kommun"                     # search all fields
regmeta search --query "kommun" --field datacolumn  # column headers only
regmeta get register LISA                           # register overview
regmeta get schema --register LISA --years 2020     # columns for a year
regmeta get varinfo "Kön"                           # variable details
regmeta resolve --columns "Kon,FodelseAr"           # map column names
regmeta docs search "disponibel inkomst"            # search documentation
```

Use `--help` on any command or subcommand for full flag documentation.

## Commands

### Query

| Command | Purpose |
|---|---|
| `search` | Free-text search across registers, variables, columns, and value codes |
| `get register` | Register overview with variants |
| `get schema` | Column listing per version, with `--years`, `--columns-like`, `--summary`, `--flat` |
| `get varinfo` | Variable details with instance history |
| `get values` | Value-set members for a CVID, with optional `--valid-at` date filter |
| `get datacolumns` | All column aliases for a variable across registers |
| `get coded-variables` | Variables with value sets, ranked by usage |
| `get diff` | Schema changes between two years for a register |
| `get lineage` | Variable provenance across registers |
| `get availability` | Temporal coverage for a variable or register |
| `get classification` | Normalized code systems (SUN, SSYK, SNI, LKF, ...) with `--list`, `--codes`, `--variables` |
| `resolve` | Exact alias lookup for column names (batch) |

### Documentation

| Command | Purpose |
|---|---|
| `docs search` | Full-text search over curated register documentation |
| `docs get` | Retrieve full documentation for a variable or topic |
| `docs list` | Browse available documentation by type, topic, or register |

### Maintenance

| Command | Purpose |
|---|---|
| `maintain update` | Update package and database to the latest version |
| `maintain build-db` | Build database from SCB CSV exports |
| `maintain build-docs` | Build documentation search index from markdown files |
| `maintain info` | Database stats and import metadata |

## Output formats

All commands support `--format {table,list,json}`. Default is table,
which auto-switches to list for narrow results. Add `-v` for the full
JSON envelope (contract version, timing, database info).

Register arguments accept numeric IDs, exact names, or substring matches.

## Database

Stored at `~/.local/share/regmeta/` by default.
Override with `--db` or `$REGMETA_DB`.

## Files

| Path | Purpose |
|---|---|
| [DESIGN.md](DESIGN.md) | Design rationale and constraints |
| [STRUCTURE.md](STRUCTURE.md) | Domain model (SCB metadata hierarchy) |
| [docs/SCHEMA.md](docs/SCHEMA.md) | Documentation file format |
| `src/regmeta/` | Package source |
| `tests/` | Test suite |
