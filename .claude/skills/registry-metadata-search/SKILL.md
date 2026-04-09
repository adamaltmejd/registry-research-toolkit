---
name: registry-metadata-search
description: Query SCB registry metadata using the regmeta CLI. Use when answering questions about Swedish register data — variable definitions, value codes, register schemas, column names, or how data is structured across registers and years.
---

# regmeta — Registry Metadata Queries

You have access to `regmeta`, a CLI tool for querying SCB (Statistics Sweden) registry metadata. The database contains metadata for **238 registers**, **42,000+ variables**, and **710,000+ value codes**. It does NOT contain microdata — only structural metadata about registers.

## Install

If `regmeta` is not yet installed, run these commands:

```bash
# Install regmeta as a global CLI tool
uv tool install regmeta

# Download the pre-built metadata database (~400 MB download, ~1.6 GB on disk)
regmeta maintain update --yes
```

Verify the install works:

```bash
regmeta search --query "kommun" --datacolumn --limit 1
```

## Output formats

The default output is `table` (compact, auto-switches to `list` when too wide). Use `--format json` when you need structured data for further processing. Format flags are global — place them **before** the subcommand:

```bash
regmeta --format json search --query "kommun"
regmeta --format list get varinfo "Kön"
```

## When to use regmeta

- User asks "what variables are in LISA?" → `get schema`
- User asks "what does column Kon mean?" → `resolve` or `search --datacolumn`
- User asks "what are the valid values for kommun?" → `get values`
- User asks "what changed in LISA between 2015 and 2020?" → `get diff`
- User asks "which registers have a variable called Kön?" → `get lineage` or `search`
- User asks "is variable X available in 2015-2024?" → `get availability` or `search --years`
- User asks "what SCB data is missing from my local files?" → `mock-data-wizard compare`
- User has a CSV with column headers and needs to understand them → `resolve`

## Important: `--type register` vs `--register`

These two flags on `search` do different things:

- **`--type register`** — filters search **results** to only show registers (not variables). Use when searching for a register by name or description.
- **`--register LISA`** — restricts the search **scope** to a specific register. Use when you know the register and want to find variables within it.

```bash
# Find registers related to education
regmeta search --query "utbildning" --type register

# Find variables within LISA that mention kommun
regmeta search --query "kommun" --register LISA
```

## Commands

### search — Find variables, columns, registers, or value codes

```bash
# Broad search across all fields
regmeta search --query "inkomst"

# Search column headers only (what appears in data files)
regmeta search --query "kommun" --datacolumn

# Search canonical variable names
regmeta search --query "Kön" --varname

# Search value codes and labels
regmeta search --query "0180" --value

# Narrow to a specific register
regmeta search --query "kommun" --datacolumn --register LISA

# Filter by year availability
regmeta search --query "inkomst" --years 2015-2024
regmeta search --query "utbildning" --type register --years 2020

# Find registers by name/description
regmeta search --query "Grundskola" --type register
```

Results include `type`, `register_id`, `register_name`, `var_id`, `variable_name`.

### resolve — Map column names to variables (batch, exact match)

The fastest way to identify what columns in a data file mean:

```bash
regmeta resolve --columns "Kon,FodelseAr,Kommun" --register LISA
```

Each column gets status `matched` or `no_match`. Matches include `var_id`, `variable_name`, `matched_column`, and `register_id`.

Can also read a JSON array from stdin:

```bash
echo '["Kon","FodelseAr"]' | regmeta resolve --register LISA
```

### get register — Register overview

```bash
regmeta get register LISA
```

Returns register metadata including variants (each with `regvar_id`, name, description, secrecy level).

### get schema — Column listing for a register

```bash
# All variants and years
regmeta get schema --register LISA

# Specific years
regmeta get schema --register LISA --years 2020-2023

# Specific variant (by regvar_id from get register)
regmeta get schema 153 --years 2022

# Filter columns by name pattern
regmeta get schema --register 340 --columns-like "Merit|Betyg|Prov"

# Condensed overview (one row per variant)
regmeta get schema --register LISA --summary

# Flat output (one row per year/alias — grep-friendly)
regmeta get schema --register LISA --flat
```

Returns variants → versions → columns. Each column has `var_id`, `variabelnamn`, `datatyp`, `aliases` (column header names in data files), and `cvid` (link to value set).

**Note:** For large registers, `get schema` without filters can produce very verbose output. Use `--years`, `--columns-like`, `--summary`, or `--flat` to narrow results. When scripting, prefer `--format json`.

### get varinfo — Variable details and history

```bash
regmeta get varinfo "Kön"
regmeta get varinfo 44              # by var_id
regmeta get varinfo "Kön" --register LISA
```

Returns variable definition, description, and instances — every register version where this variable appears, with `cvid`, data type, aliases, and value set count.

### get values — Value code lookup

Requires a CVID (get it from `get varinfo` or `get schema`):

```bash
regmeta get values 1001
regmeta get values 1001 --valid-at 2020-01-01
```

Returns code/label pairs. Use `--valid-at` for codes valid at a specific date.

### get datacolumns — All aliases for a variable

```bash
regmeta get datacolumns "Kommun"
```

Shows every column header this variable appears under across all registers and versions.

### get coded-variables — Find categorical variables

```bash
regmeta get coded-variables --min-registers 5 --min-codes 10
```

Lists variables that have coded value sets, ranked by usage.

### get diff — Schema changes between years

```bash
regmeta get diff --register LISA --from 2015 --to 2020
regmeta get diff --register LISA --from 2015 --to 2020 --variable Kon
```

Returns added, removed, and changed variables between two versions.

### get lineage — Variable provenance

```bash
regmeta get lineage "Kön"
```

Shows which register is the source (producer) and which registers consume the variable, with year ranges and instance counts.

### get availability — Temporal availability summary

```bash
# When is a variable available?
regmeta get availability "Kön"
regmeta get availability "Kön" --register LISA

# When is a register available?
regmeta get availability LISA
```

Returns min year, max year, year list, gaps, and per-register aliases (for variables) or per-variant year coverage (for registers). Auto-detects whether the target is a variable or register.

### maintain — Setup and maintenance

```bash
# Download pre-built database from GitHub Releases
regmeta maintain update                    # interactive confirmation
regmeta maintain update --yes              # skip confirmation
regmeta maintain update --tag v0.1.0       # specific release
regmeta maintain update --force --yes      # overwrite existing DB

# Build database from raw SCB CSV exports (alternative to download)
regmeta maintain build-db --csv-dir path/to/SCB-data/

# Database stats and import metadata
regmeta maintain info
```

## Typical workflows

### "What's in this register?"

```bash
regmeta get register LISA          # overview + variants
regmeta get schema --register LISA --years 2022  # columns
```

### "What does this column mean?"

```bash
regmeta resolve --columns "Kon,AstKommun" --register LISA
# Then for value codes:
regmeta get varinfo 44 --register LISA  # get CVIDs
regmeta get values 1001                  # get code labels
```

### "What are the valid values for variable X?"

```bash
regmeta get varinfo "Kommun" --register LISA  # find CVID
regmeta get values <cvid> --valid-at 2022-01-01
```

### "How has this register changed over time?"

```bash
regmeta get diff --register LISA --from 2010 --to 2022
```

### "Which registers contain income data?"

```bash
regmeta search --query "inkomst" --varname
```

### "Is this variable available for 2015-2024?"

```bash
regmeta get availability "Kön" --register LISA
# or filter search results by year
regmeta search --query "inkomst" --years 2015-2024
```

### "What SCB data exists but isn't in my local files?"

```bash
# Use mock-data-wizard compare (calls regmeta.compare() internally):
mock-data-wizard compare mock_data/manifest.json
mock-data-wizard compare --files mock_data/*.csv --register LISA
mock-data-wizard compare --columns "Kon,FodelseAr" --register 189
```

## Key concepts

- **register** — A statistical register (e.g. LISA, RTB). Has an integer `register_id`.
- **variant** — A sub-table within a register (e.g. LISA/Individer, LISA/Företag). Has `regvar_id`.
- **version** — A year-specific release of a variant. Named by year (e.g. "2022").
- **variable** — A logical concept (e.g. "Kön"). Has `var_id`. Appears across registers.
- **alias / kolumnnamn** — The column header in the actual data file. May differ across registers/versions.
- **CVID** — Links a variable instance to its value set. Use with `get values`.
- **value set** — The valid coded values for a categorical variable (e.g. 1=Man, 2=Kvinna).

## Notes

- All query commands are offline. The database must be installed first with `regmeta maintain update`.
- Register arguments accept numeric IDs, exact names, or substring matches.
- Variable arguments accept `var_id` (integer) or variable name (string).
- Search uses substring matching for most fields; `--description` uses full-text search.
- Value sets are a historical union — use `--valid-at` for temporal filtering.
- `get schema` output can be very large for big registers. Use `--summary` for an overview, `--flat` for grep-friendly output, or `--years` and `--columns-like` to filter.
