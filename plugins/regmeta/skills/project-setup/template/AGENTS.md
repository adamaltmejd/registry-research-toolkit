# <project name>

A register research project. `RESEARCH.md` says what it studies.

## Boundaries

- No agent runs on the server that holds the row-level data (<MONA, project P1234>).
  The researcher uploads, runs and exports.
- Row-level data never leaves that server. Only what the export gate passed and the
  researcher reviewed.
- Read an export only after the researcher has approved it for model access. Never read
  a batch log or a diagnostics file from a real run.
- Read only this folder and the sources the researcher names.

## Layout

```text
RESEARCH.md        what the project studies
project_data.json  the data: the order, and what is delivered
run.R              the runner; uploaded once; only its settings block changes
rproject.toml, rv/, .Rprofile, air.toml, jarl.toml   local R tooling; never uploaded
src/               everything that runs on the server: flat, text only
tests/             local tests on small fixtures
questions/         one ticket per research question
output/            a run's export/, review/, run_manifest.json (ignored)
results/           approved real exports, by date
.regmeta/          mock/ (rules, committed); mock-data/, keys/, staging/, targets/ (ignored)
```

## Commands

- A mock run: `Rscript run.R` (the `.Rprofile` sets `REGMETA_MOCK`). It ends with the
  export gate and prints a source id.
- Before a commit: `air format .` and `jarl check .`.
- Packages: `rv sync`. A new package goes in `rproject.toml` and in `PACKAGES` in
  `run.R`; say so to the researcher, because `run.R` is then uploaded again.
- A real run happens only on the server: `run.R` as it is, with no arguments.

## A batch

1. Commit, then a mock run from the clean checkout that passes the gate.
2. The researcher uploads every file in `src/`, empties `src\` in
   `<MONA code folder, e.g. //micro.intra/projekt/P1234$/P1234_Gem/project>`, moves the
   uploaded files into it and submits `run.R` to the batch runner.
3. The source id in the log equals the mock run's. The researcher reviews
   `output\review\`, zips `output\export\` and exports it.

## Pins

- Catalog: <global | steward host>, release `reg_meta/v<X.Y.Z>`; `reg-mock` the same.
- Export gate 0.6.0, schema `aggregates`, policy `aggregates-min5`, cell key 0.2.0.
  Their hashes are `PINS` in `run.R`; a run stops on a file that differs.

## Skills (plugin `regmeta`)

- `catalog-search`: what a register, variable or code means.
- `project-data`: change `project_data.json`; the data summary.
- `mock-data`: make or update the mock files.
- `research`: questions, analyses, runs, batches, results.
