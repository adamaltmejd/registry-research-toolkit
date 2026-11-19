# Reference: disposable income by sex

The project's instructions (copy as `CLAUDE.md` or `AGENTS.md`). A worked example of
the `register-research-scaffold` skill: LISA Individer 2021 to 2022 from the global
catalog, one question, run on MONA, results through the export gate.

## Boundaries

- The agent works locally on code, catalog metadata and `reg-mock` data. It never runs
  on MONA, never reads the extract, the key file or an unreviewed export, and learns a
  gate refusal only by its category code.
- Row-level data never leaves MONA. Every result leaves as the export gate's
  `out/real/export/` folder, after the researcher has reviewed it.
- Mock and real runs write to `out/mock/` and `out/real/`; `run.R` picks the mode from
  which input folder exists (`config.R`) and never falls back to the other.

## Commands

From the project folder:

```sh
# Mock inputs (register-mock-data skill); a changed project_data.json needs
# reg-mock fetch into mock/answers first
reg-mock compile --project project_data.json --rules mock/mock_rules.json \
  --answers mock/answers --out mock/mock_spec.json
reg-mock generate --spec mock/mock_spec.json --out ../reference-mock --seed 1 --persons 2000
Rscript make_keys.R   # once per mode: the cell-key file, outside the project
Rscript run.R         # a run: inputs, analysis, disclosure, staging, export gate
```

On MONA the researcher uploads the committed project with `export_gate/`, sets
`REAL_DIR` and the real key file in `config.R`, runs `make_keys.R` once and `run.R`
for each batch, reviews `out/real/export/` and exports it through MONA's own export.
`out/real/run_record.json` (commit, key epoch, versions) stays on MONA.

## Pins

Catalog `reg_meta/v0.90.0` (`project_data.json`). R packages: `arrow`, `jsonlite`,
`openssl` (the key file), and on R below 4.5 `digest`.

| File                                 | Version                              | SHA-256                                                            |
| ------------------------------------ | ------------------------------------ | ------------------------------------------------------------------ |
| `export_gate/export_gate.R`          | `EXPORT_GATE_VERSION` 0.6.0          | `51d6f810ef0c93faccb69f748382b6cdce93243af838788f9165e094a103869a` |
| `export_gate/aggregates.schema.json` | `aggregates` 1                       | `8160f4cb2250b5d22e609bf0a58935a221bd61ec407dd0403e2ccdd0537f1f93` |
| `export_gate/aggregates.policy.json` | `aggregates-min5` 1                  | `e3c0835c7963efa39f035216087c08b7558ab5f3887c3ae7bbae2ed0f0098f5c` |
| `export_gate/cell_key.R`             | `CELL_KEY_VERSION` 0.1.0             | `585080051893308794fd43a298a867236e6861d1eb2b1c791b7d5aac660e197e` |

Never edit these files. A newer gate is a deliberate update of this table.

## Questions

One Markdown ticket per question in `questions/`; status in its frontmatter. Closed
tickets stay in Git.
