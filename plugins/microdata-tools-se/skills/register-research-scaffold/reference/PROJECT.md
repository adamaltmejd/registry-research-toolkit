# Reference: disposable income by sex

The project's instructions (copy as `CLAUDE.md` or `AGENTS.md`). This is the scaffold
of the `register-research-scaffold` skill as its stage 1 leaves it: the skill's
example, LISA Individer 2021 to 2022 from the global catalog and one example question
(`q001`), run on MONA, results through the export gate. The example is not the user's
research. It stays until the user asks for a research question to be added (the skill's
stage 2); the first one replaces it.

## Boundaries

- The agent reads only this project folder and the sources the user names. It does not
  open neighbouring folders, other repositories or the user's other projects unless the
  user points to them. The project's own mock folders beside it (`MOCK_DIR` and the mock
  key folder in `config.R`) are in scope for the commands below; the agent never opens
  a key file.
- The agent works locally on code, catalog metadata and `reg-mock` data. It never runs
  on MONA, never reads the extract, the key file or an unreviewed export, and learns a
  gate refusal only by its category code.
- Row-level data never leaves MONA. Every result leaves as the export gate's
  `out/real/export/` folder, after the researcher has reviewed it.
- The agent reads an export only after the researcher has approved those exact files
  for model access. A gate pass or MONA's export approval is not that approval.
- Mock and real runs write to `out/mock/` and `out/real/`. The mode is named on the
  command line; its input folder (`config.R`) must exist, a mock run refuses where the
  real folder exists, and nothing falls back to the other mode.

## Commands

From the project folder:

```sh
# Mock inputs (register-mock-data skill); a changed project_data.json needs
# reg-mock fetch into mock/answers first
reg-mock compile --project project_data.json --rules mock/mock_rules.json \
  --answers mock/answers --out mock/mock_spec.json
# The mock folders sit beside the project and carry its folder name (config.R):
# ../<project>-mock and ../<project>-mock-keys
reg-mock generate --spec mock/mock_spec.json --out "../$(basename "$PWD")-mock" \
  --seed 1 --persons 2000
Rscript make_keys.R mock   # once per mode: the cell-key file, outside the project
Rscript run.R mock         # a run: inputs, analysis, disclosure, staging, gate
```

Before MONA:

- `LISA_FILES` and `LISA_COLUMNS` in `config.R` name the delivery's files, each by the
  delivery year the order answer gives it, and their stored column names, from the
  order answer (a steward's: `materialize_order`). The mocks
  mirror them; a real run refuses a missing or extra file.
- `git rev-parse HEAD > SOURCE_COMMIT` from a clean checkout, just before upload: the
  upload has no `.git`, and a real run reads its commit from this file.

On MONA the researcher uploads the committed project with `export_gate/` and
`SOURCE_COMMIT`, sets `REAL_DIR` and the real key file in `config.R`, runs
`Rscript make_keys.R real` once and `Rscript run.R real` for each batch, reviews
`out/real/export/` and exports it through MONA's own export.
`out/real/run_record.json` (commit, key epoch, versions) stays on MONA.

## Pins

Catalog `reg_meta/v0.91.0` (`project_data.json`). R packages: `arrow`, `jsonlite`,
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
tickets stay in Git. The agent adds a question only when the user asks for one and names
it (the skill's stage 2). It does not propose questions or start one on its own.
