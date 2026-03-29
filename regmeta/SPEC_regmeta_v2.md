# SPEC: regmeta V2 Features

Status: Approved
Version: 3.0.0
Created: 2026-03-20
Updated: 2026-03-23
Owner: Research engineering
Parent spec: `SPEC_regmeta.md` (v1, frozen)

**Maturity: pre-release, zero users.** No backwards compatibility obligation.

## 1. Overview

Six features on top of regmeta v1:

1. **Temporal diff** — "what changed between year X and Y" for a register, with optional variable filter.
2. **Cross-register lineage** — where a variable originates and which registers consume it.
3. **Compare** — library function to compare local file columns against registry metadata. CLI command lives in `mock-data-wizard`.
4. **Availability** — temporal availability summary for a variable or register.
5. **Search year filter** — `search --years` to filter results by version year range.
6. **Schema ergonomics** — `--summary`, `--flat`, `--columns-like` for `get schema`.

All are query-layer features. No new CSV imports, no schema migration. The underlying data already exists.

## 2. Feature 1: Temporal Diff

### 2.1 Purpose

Answer: "What changed in register X between year A and year B?" Users working with longitudinal MONA data need to understand structural changes — added/removed columns, changed types, altered value sets — without manually diffing two `get schema` snapshots.

### 2.2 Command

```bash
regmeta get diff --register <name_or_id> --from <year> --to <year>
                 [--variant <regvar_id>]
                 [--variable <name_or_var_id_or_alias> ...]
                 [--format {table,list,json}] [--output <path>] [--db <path>]
```

**Required:** `--register`, `--from`, `--to`.

`--from` and `--to` are 4-digit years. `--to` must be greater than `--from`.

`--variable` accepts one or more space-separated values (`nargs="+"`). Each value is resolved by var_id, variabelnamn (case-insensitive), or column alias.

### 2.3 Semantics

For each variant in the register (or the single variant specified by `--variant`):

1. Find the version whose year matches `--from` (or closest available year ≤ `--from`). Same for `--to`.
2. If no version exists for either year in a variant, skip that variant.
3. Compare the column sets of the two versions.

**Change detection per variable:**

| Change type | Condition |
|---|---|
| `added` | Present in `to` version but not in `from` version |
| `removed` | Present in `from` version but not in `to` version |
| `changed` | Present in both, but `datatyp`, `datalangd`, or aliases differ |

When `--variable` is specified, only report changes for the named variable(s). The diff still runs across all variables internally, but the output is filtered.

### 2.4 Version Matching

A year matches a version if the version's extracted year (first 4-digit number in `registerversionnamn`) equals the requested year. If no exact match exists, use the latest version with year ≤ requested year. If no version exists at or before the requested year, the variant is skipped for that side of the diff.

### 2.5 Output (JSON)

```json
{
  "register_id": "34",
  "register_name": "LISA",
  "from_year": 2015,
  "to_year": 2020,
  "resolved_variables": [
    {"input": "KON_NY", "variabelnamn": "Kon", "var_id": "44"}
  ],
  "variants": [
    {
      "regvar_id": "...",
      "variant_name": "Individer, 15 år och äldre",
      "from_version": {"regver_id": "...", "version_name": "...", "year": 2015},
      "to_version": {"regver_id": "...", "version_name": "...", "year": 2020},
      "summary": {"added": 3, "removed": 1, "changed": 2, "unchanged": 45},
      "added": [
        {"var_id": "...", "variabelnamn": "NyVariabel", "datatyp": "int", "aliases": ["NyKol"]}
      ],
      "removed": [
        {"var_id": "...", "variabelnamn": "GammalVar", "datatyp": "varchar", "aliases": ["GammalKol"]}
      ],
      "changed": [
        {
          "var_id": "...",
          "variabelnamn": "Kon",
          "changes": [
            {"field": "datatyp", "from": "int", "to": "varchar"},
            {"field": "aliases", "from": ["Kon"], "to": ["Kon", "KON_NY"]}
          ]
        }
      ]
    }
  ],
  "unchanged": ["VarA", "VarB"]
}
```

**`resolved_variables`** (present only when `--variable` used): list of `{input, variabelnamn, var_id}` showing input→canonical mapping for each resolved variable.

**`unchanged`** (present only when `--variable` used and some variables had zero changes): simple list of canonical variabelnamn for variables that were unchanged in ALL variants. Variables that changed in at least one variant are excluded.

Variants with no changes are omitted from the output.

### 2.6 Table Output

When `--variable` is used with alias-to-canonical mappings, a "Resolved variables:" header is printed first. Then the diff table with columns: variant, change (+/-/~), var_id, variabelnamn, detail. Then an "Unchanged: ..." footer if applicable.

```text
Resolved variables:
  KON_NY → Kon (var_id 44)

variant                          change  var_id  variabelnamn  detail
-------------------------------  ------  ------  ------------  -----------------------
Individer, 15 år och äldre      ~       44      Kon           datatyp: int → varchar

Unchanged: VarA, VarB
```

### 2.7 Error Cases

| Condition | Exit code | Behavior |
|---|---|---|
| Register not found | 16 | Standard not-found error |
| No versions in range for any variant | 16 | "No versions found for register X between years A and B" |
| `--from` ≥ `--to` | 2 | Usage error |
| `--variable` not found in register | 16 | "No variables matching ... in register ..." |

## 3. Feature 2: Cross-Register Lineage

### 3.1 Purpose

Answer: "Where does this variable come from, and where else does it appear?" SCB registers are interconnected — LISA aggregates from UREG, RTB, and other source registers. A variable like `Kon` appears in dozens of registers. Users need to understand the provenance chain.

### 3.2 Available Data

The `variable` table already contains two provenance fields from `Registerinformation.csv`:

- **`variabelhamtadfran`** — free-text field indicating where the variable was fetched from (e.g. a source system or register name)
- **`variabelregister_kalla`** — free-text field indicating the source register

These fields are populated by SCB but are free-text, not foreign keys. They may contain register names, abbreviations, system names, or be empty. The lineage feature must handle this gracefully.

### 3.3 Command

```bash
regmeta get lineage <name_or_var_id>
        [--register <name_or_id>]
        [--format {table,list,json}] [--output <path>] [--db <path>]
```

### 3.4 Semantics

1. Find all `(register_id, var_id)` entries matching the variable name or var_id (same resolution as `get varinfo`).
2. For each match, collect:
   - The register it belongs to
   - The provenance fields (`variabelhamtadfran`, `variabelregister_kalla`)
   - Instance count (number of CVIDs / versions)
   - Year range (earliest to latest version year)
3. Attempt to resolve `variabelregister_kalla` values to actual `register_id`s using the same fuzzy register lookup (exact ID → exact name → substring). Mark as `resolved` or `unresolved`.
4. Group results into a lineage view: sources (registers that provide the variable) and consumers (registers that fetch it from elsewhere).

**Classification logic:**

- A variable in register R is a **source** if `variabelregister_kalla` is empty or points to R itself.
- A variable in register R is a **consumer** if `variabelregister_kalla` names a different register.
- If provenance fields are empty for all occurrences, classification is `unknown` — just show cross-register occurrence without directionality.

### 3.5 Output (JSON)

```json
{
  "variable_name": "Kon",
  "occurrences": 97,
  "registers": [
    {
      "register_id": "1",
      "register_name": "RTB",
      "var_id": "44",
      "role": "source",
      "variabelhamtadfran": "",
      "variabelregister_kalla": "",
      "source_register_id": null,
      "instance_count": 12,
      "year_range": [1990, 2023]
    },
    {
      "register_id": "34",
      "register_name": "LISA",
      "var_id": "44",
      "role": "consumer",
      "variabelhamtadfran": "Registret över totalbefolkningen",
      "variabelregister_kalla": "RTB",
      "source_register_id": "1",
      "instance_count": 26,
      "year_range": [1998, 2023]
    }
  ],
  "provenance_coverage": {
    "total": 97,
    "with_source": 85,
    "without_source": 12
  }
}
```

### 3.6 Table Output

A register table with columns: register, var_id, role, instances, years, source. Followed by a "Provenance: X/Y (Z%)" footer.

```text
register    var_id  role      instances  years      source
----------  ------  --------  ---------  ---------  ------
RTB (1)     44      source    12         1990-2023
LISA (34)   44      consumer  26         1998-2023  ← RTB

Provenance: 85/97 (88%)
```

### 3.7 Edge Cases

- **Free-text provenance doesn't match any register**: `source_register_id` is `null`, `role` is `consumer` (we know it has a source, just can't resolve it).
- **Variable name appears with different var_ids across registers**: Each `(register_id, var_id)` is a separate entry. The output groups by register, not by var_id.
- **`--register` filter**: When specified, shows lineage only for that register's variables — but still resolves and displays the source register information.

### 3.8 Error Cases

| Condition | Exit code |
|---|---|
| Variable not found | 16 |
| Register filter matches nothing | 16 |

## 4. Feature 3: Compare

### 4.1 Purpose

Answer: "What SCB data exists but isn't in my local files?" and "What columns do I have locally that aren't in SCB metadata?" Users working with MONA projects need to verify their local data files against the registry schema without manually stitching together multiple commands.

### 4.2 Command

The CLI command has moved to `mock-data-wizard compare`. regmeta exposes the `compare()` library function:

```python
regmeta.compare(
    conn,
    columns_by_file={"file.csv": ["Kon", "FodelseAr"]},
    register_hints={"file.csv": 34},
    year_hints={"file.csv": 2022},
)
```

The caller (mock-data-wizard) is responsible for parsing manifests/CSVs and resolving register names to IDs via `regmeta.resolve_register_ids()`.

### 4.3 Semantics

For each file/column set:

1. Determine register: from `register_hint` in manifest, or `--register` from CLI.
2. Get registry schema for that register (optionally filtered to `year_hint`).
3. Flatten schema aliases into a lookup set.
4. Classify each local column as `matched`, `extra_local`, or identify `missing_from_registry`.

### 4.4 Manifest Input

Reads mock-data-wizard manifest v2 (`schema_version: "2"`). Extracts per-file `columns`, `register_hint`, and `year_hint`. Files without `register_hint` are reported as `register_status: "no_hint"`.

### 4.5 Output (JSON)

```json
{
  "files": [
    {
      "file": "LISA_2022.csv",
      "register_id": 34,
      "register_name": "LISA",
      "register_status": "resolved",
      "year_hint": 2022,
      "matched": [{"column": "Kon", "var_id": 44, "variable_name": "Kön"}],
      "extra_local": ["MyCustomCol"],
      "missing_from_registry": [{"var_id": 99, "variable_name": "NyVar", "aliases": ["NyKol"]}],
      "summary": {"matched": 5, "extra_local": 1, "missing_from_registry": 12}
    }
  ]
}
```

`register_status` values: `resolved`, `no_hint`, `not_found`, `no_schema`.

### 4.6 Error Cases

| Condition | Exit code |
|---|---|
| Manifest file not found | 2 |
| Unsupported schema_version | 2 |
| `--files`/`--columns` without `--register` | 2 |
| Register not found | 16 |

## 5. Feature 4: Availability

### 5.1 Purpose

Answer: "Is variable X available for years 2015-2024?" without chaining `search` → `get varinfo` → inspect instance years.

### 5.2 Command

```bash
regmeta get availability <target> [--register <name_or_id>]
        [--format {table,list,json}] [--output <path>] [--db <path>]
```

Auto-detects whether target is a variable or register (tries variable first).

### 5.3 Output (JSON)

For variables:

```json
{
  "target": "Kön",
  "target_type": "variable",
  "variable_name": "Kön",
  "min_year": 2010,
  "max_year": 2022,
  "years": [2010, 2011, 2012, ...],
  "gaps": [2013],
  "register_count": 5,
  "registers": [
    {
      "register_id": 34,
      "register_name": "LISA",
      "var_id": 44,
      "min_year": 2010,
      "max_year": 2022,
      "years": [2010, 2011, ...],
      "gaps": [],
      "aliases_by_year": {"2010": ["Kon"], "2015": ["KON"]}
    }
  ]
}
```

For registers: similar structure with `variants` instead of `registers`, each with year coverage.

## 6. Feature 5: Search Year Filter

### 6.1 Command

```bash
regmeta search --query <term> --years <range> [other search flags]
```

`--years` accepts the same format as `get schema --years` (e.g. `2015`, `2015-2024`, `2015-`, `-2024`).

### 6.2 Semantics

Post-filters search results: for variable-type results, checks if any `variable_instance` exists in a `register_version` within the year range. For register-type results, checks if the register has any version in range. `total_count` reflects the filtered count.

## 7. Feature 6: Schema Ergonomics

### 7.1 New Flags

- `--columns-like PATTERN` — regex filter (case-insensitive) on column aliases and variable names. Applied at the query level before building results.
- `--summary` — display mode: one row per variant with year range and column count. Mutually exclusive with `--flat`.
- `--flat` — display mode: one row per (regvar_id, year, alias, variable_name, var_id). Mutually exclusive with `--summary`.

`--summary` and `--flat` are display concerns only — JSON output structure is unchanged.

## 8. Output Formats

Three output formats, shared across all commands:

- **`--format table`** (default): Columnar table. Auto-switches to list format if the table is wider than the terminal. Truncates at 100 rows with a note to use `--format json` for full output.
- **`--format list`**: Block/record style (key-value pairs per row).
- **`--format json`**: Machine-readable JSON. Plain data object by default; `--verbose`/`-v` wraps in an envelope with contract version, timing, and database info.

## 9. Repeated Flags

All repeated optional flags error at parse time via `_NoRepeatParser` subclass (not just `--variable`).

## 10. Contract Version

Unchanged at `3.0.0`. Pre-release with zero users — no contract to break.

## 11. Non-Functional

- Same performance target as v1 queries: < 500ms.
- No new dependencies.
- No new CSV imports or schema changes. All features query existing tables.
- Library-importable: `get_diff()`, `get_lineage()`, `get_availability()`, and `compare()` available as Python functions.

## 12. Acceptance Criteria

1. `regmeta get diff --register LISA --from 2015 --to 2020` returns added/removed/changed variables.
2. `regmeta get diff --register LISA --from 2015 --to 2020 --variable Kon KON_NY` resolves inputs and returns changes for matched variables.
3. `regmeta get lineage Kon` returns cross-register occurrence with provenance classification.
4. `regmeta get lineage Kon --register LISA` returns LISA-scoped lineage with source info.
5. `mock-data-wizard compare manifest.json` reads v2 manifest and classifies columns (calls `regmeta.compare()`).
6. `mock-data-wizard compare --columns "Kon" --register LISA` works with explicit input.
7. `regmeta get availability "Kön"` returns year coverage across registers.
8. `regmeta search --query "Kön" --years 2020` filters results by year.
9. `regmeta get schema --register LISA --columns-like "Kon" --flat` filters and flattens.
10. All commands produce valid JSON and table/list output in all three formats.
11. All new query functions are library-importable.
