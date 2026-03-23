# SPEC: Mock Data Wizard V2 Features

Status: Approved
Version: 2.0.0
Created: 2026-03-20
Owner: Research engineering
Parent spec: `SPEC_mock_data_wizard.md` (v1, frozen)

**Maturity: pre-release, zero users.** No backwards compatibility obligation.

## 1. Overview

Two features on top of mock_data_wizard v1:

1. **Population spine** — birth-invariant attributes are generated once per individual and reused across files, ensuring cross-file join consistency.
2. **Value code drift warnings** — enrichment warns when stats contain frequency codes absent from regmeta's value set.

## 2. Feature 1: Population Spine

### 2.1 Purpose

When the same individual (shared ID) appears in multiple files, birth-invariant attributes like Kön and Födelseår must be consistent. Without a spine, each file generates these independently — person 12345 might be male in one file and female in another.

### 2.2 Spine-Eligible Variables

Hardcoded set of regmeta var_ids known to be birth-invariant:

| var_id | Variable |
|---|---|
| 44 | Kön |
| 1378 | Födelseår |
| 256 | Födelselän |
| 257 | Födelseland |

A shared column qualifies for the spine when:
- It appears in `shared_columns` (same column name across files)
- It is NOT an ID column
- Its enriched `var_id` is in the spine-eligible set

### 2.3 Authority File Selection

For each spine column, the authority file is the file with the highest `n_distinct` for the associated shared ID column (proxy for largest population). The authority file's stats and value_codes drive spine value generation.

### 2.4 Generation

1. After shared ID pool creation, identify spine columns from enriched metadata.
2. For each spine column, generate values for the full shared ID pool using the authority file's distribution. Seeded via `_sub_seed(seed, "__spine__", column_name)`.
3. Per-file: ID columns are generated first. Spine columns look up each row's ID in the spine mapping instead of generating fresh values. Null rates are still applied per-file.

### 2.5 Fallback

Without regmeta enrichment (`--no-regmeta`), no columns have `var_id` set, so the spine is empty. Behavior is identical to v1.

### 2.6 Deliberate Exclusions

- Household structures, time-varying attributes, employer links
- User-declared stable columns
- Spine for non-shared columns

## 3. Feature 2: Value Code Drift Warnings

### 3.1 Purpose

Catch mistakes in stats exports (column name typos, wrong register year) by cross-checking observed frequency codes against regmeta's value set.

### 3.2 Semantics

After enrichment, for each categorical column that has both `stats.frequencies` and regmeta `value_codes`:

- Compute `unknown = frequency_keys - value_code_keys` (excluding `_other`)
- If non-empty, emit a warning: `file/column: codes [X, Y] not in regmeta value set`

Warnings are printed to stderr during enrichment. They do not block generation.

### 3.3 Deliberate Exclusions

- **Unseen regmeta codes** (codes in regmeta but absent from stats): too noisy — registers legitimately contain rare codes never seen in a given dataset.
- **Column completeness** (regmeta says register should have column X): doesn't work because researchers typically order only a subset of a register's columns.
