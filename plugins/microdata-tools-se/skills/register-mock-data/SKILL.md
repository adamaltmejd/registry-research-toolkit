---
name: register-mock-data
description: Generate local mock register data from a project_data.json with
  `reg-meta mock`, so analysis code runs on the researcher's machine before the real
  extract exists. Use when the user asks for mock, synthetic or fake data for a register
  project, or a mock run of their pipeline.
---

# Mock register data

`reg-meta mock` writes files shaped like the project's delivery: the same file and
column names, the runner's types and format (`.dta`, Parquet or CSV), drawn from a
simulated population built from SCB's public demography. It reads catalog metadata only,
never a data row; every key is synthetic. A pipeline switches between real and mock data
by its input directory alone.

Three steps, each leaving a file the researcher can review and commit: save the
catalog's answers, write the rules and compile them into `mock_spec.json`, generate.

## 0. The binary and the catalog

- `reg-meta` ships for macOS arm64 and Linux x86_64 on each `reg_meta/v*` GitHub
  release; the download and SHA-256 check are in the repository README's Local server
  section. Use the release that matches `reg_meta_version` in `project_data.json`.
- The answers come from a catalog's HTTP API. Pick it from the plugin's `catalog_url`:
  - a steward's server (for example `https://data.swecov.se/mcp`): its origin,
    `https://data.swecov.se`, with mode `materialize`, which gives the delivery's own
    file and column names;
  - the global catalog (`https://reg-meta.vqv.uk/mcp`) serves MCP only. Download the
    same release's `reg_meta.db.zst`, unpack it into `catalog/` (README, Local server),
    run `reg-meta serve --db catalog --port 8000`, and use `http://127.0.0.1:8000` with
    mode `order`. Files then follow the standard names: the table's public id with `_`
    for `/` and `-`, one file per year (`scb_lsum_individer_sociala_agi_ku_2019`).
- The project must validate first (`register-project` skill).

## 1. Save the answers

```bash
scripts/save-answers.sh <api-origin> project_data.json order|materialize mock/answers
```

The script (curl and jq) posts the project and saves the order answer, every segment
page for each column, and every page of each value set, unchanged. Commit
`mock/answers/`: compile reads only these files, so the mock never depends on the
network again. Save again whenever the project changes; compile refuses answers saved
for another version of it (`answers_stale`).

## 2. Write the rules and compile

The catalog cannot say who a table holds or how many rows a person has. Write that in
`mock/mock_rules.json`, one entry per project source, and give the reason for each
choice so the researcher can judge it:

```json
{
  "version": 1,
  "sources": {
    "lisa": {
      "unit": "person",
      "population": { "age_min": 16 },
      "rows_per_unit": { "mean": 1, "reason": "yearly snapshot of residents 16+" },
      "columns": {
        "LopNr": { "attribute": "person_key" },
        "Kon": { "attribute": "sex", "map": { "male": "1", "female": "2" } },
        "FodelseAr": { "attribute": "birth_year" },
        "Kommun": { "attribute": "municipality" }
      }
    },
    "par": {
      "unit": "person",
      "rows_per_unit": { "mean": 1.8, "reason": "about two outpatient visits a year" },
      "columns": { "LopNr": { "attribute": "person_key" } }
    }
  }
}
```

- `unit`: `person` or `firm`. `population`: `age_min`, `age_max`, and `include_departed`
  for registers that keep the dead and emigrated (cause of death).
- `rows_per_unit.mean`: below 1, the share of eligible units with a row; above 1, the
  mean count (events).
- `columns`, by the column's `name` in `mock_spec.json` (the order's name, not the
  `stata_name` a `.dta` file shows for a long or invalid one): `attribute`
  (`person_key`, `firm_key`, `sex`, `birth_year`, `birth_date`, `age`, `county`,
  `municipality`, `death_date`, `year`), `map` from an attribute's value to the
  delivered code (`male`/`female` for sex), `null_rate`, or `codes` replacing the
  catalog's. Mark every person or firm id that should join across files as `person_key`
  or `firm_key`; any other identifier is random.
- Optional: `null_rate` for the whole spec (default 0.02), `rename` from a compiled file
  name to the delivery's, `tables` for files the catalog does not know (in the spec's
  file shape).

```bash
reg-meta mock compile --project project_data.json --rules mock/mock_rules.json \
  --answers mock/answers --out mock/mock_spec.json
```

A refusal is JSON on stdout, exit 2: every problem at once, each with a `code`, where
(`at`) and what to do. Fix the rules or save the answers again; never edit
`mock_spec.json` by hand. Show the researcher the rules and the compiled spec's files
and columns, and commit both.

## 3. Generate

```bash
reg-meta mock generate --spec mock/mock_spec.json --out <mock-input-dir> \
  [--seed 1] [--persons 10000]
```

The same spec, seed and size give the same bytes. More than 1 000 000 persons needs
`--allow-large`. Keep the output out of Git, beside the real-input directory the
pipeline is configured with. Each file states it is mock data (the `.dta` label, Parquet
metadata) and `mock_manifest.json` lists the files and row counts.

## Limits

- The data are plausible in shape, not in content: values within a column are drawn
  independently, so no relation between columns (income by age, diagnosis by sex)
  survives beyond the population's age, sex, county and deaths. Never read a result off
  a mock run.
- To make the mock closer for a specific test, add a post-step in the project that edits
  the generated files; keep it in the project's own code, not in the spec.
- Geometry, uuid and binary columns, and SQL storage the runner refuses, are refused.
