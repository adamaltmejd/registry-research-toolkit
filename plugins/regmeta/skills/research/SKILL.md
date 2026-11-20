---
name: research
description: Work on research questions in a register project that `project-setup` has
  set up. Add a question and its analysis, run the pipeline on mock data through the
  export gate, prepare a batch for the server that holds the data (MONA or another),
  and import results a human has approved. Use when the user asks for an analysis, a
  result, a run or a batch in such a project.
---

# Research in a register project

The project folder has `AGENTS.md`, `RESEARCH.md`, `project_data.json`, `run.R` and
`src/`. If it does not, use the `project-setup` skill first. If a question needs data
the project lacks, change the order with the `project-data` skill and the mock files
with `mock-data` before writing the analysis.

## Boundaries

- No agent runs where the row-level data is. The researcher uploads, runs and exports.
- The agent reads code, catalog metadata, mock data and results the researcher has
  approved for model access. Never a batch log, a diagnostics file or an unapproved
  export. A gate pass is not that approval.
- A refused real run is reported to the agent by its category code only (`setup`,
  `inventory`, `structure`, `disclosure`, `backstop`). A real run that fails before the
  gate is reported by the step it stopped in (packages, pins, pipeline and which
  target), never by the log's text. Reproduce either on mock data.
- A mock run shows that the code runs. It is never a result.

## A question

One Markdown ticket per question, `questions/q<NNN>-<slug>.md`, with `id`, `title`,
`status` and `depends_on` frontmatter and the question, method and answer in prose
([contracts.md](references/contracts.md), "Questions"). Ask the researcher about
anything that changes the population or the estimand; state other assumptions in the
ticket. The agent proposes methods, not research questions.

## An analysis

[example/](example/) is a worked question on LISA 2021 and 2022: persons and mean
disposable income by sex and year. Read it as a pattern. Never copy it into a project.
Its `mock/` is a project's `.regmeta/mock/`.

1. **Inputs** (`src/inputs.R`). Add each source to `SOURCES`: its files by year (a file
   that pools years has no year name and maps its own `year` column) and its stored
   column names by the project's names, from the order answer or the data
   summary (`project-data` skill). A file's year is never parsed from its name.
2. **Analysis** (a new file in `src/`). A function from input rows to person-level
   rows, never counts or statistics. Put the data assertions in it (`stopifnot` on
   keys, ranges, uniqueness): they run on mock and real data alike.
3. **Disclosure** (the same file, or `src/disclosure.R`). One function turns the rows
   into the schema's tables, in this order: suppress, perturb counts
   (`perturbed_counts()`), round. Nothing else makes a publishable number
   ([export-gate.md](references/export-gate.md), "The disclosure policy").
4. **Pipeline** (`src/pipeline.R`). Per input, a `format = "file"` target for its files
   that always runs, and one that reads them; then one target per analysis and per
   disclosed result, and each result named in `release_tables()`. Every disclosure step
   takes `cell_key_epoch`, so a new key epoch runs it again.

`src/` is flat and holds only text files that run on the server: no subfolders, no
tests, no notes. `run.R` (but for `PACKAGES`), `src/staging.R` and the four pinned
files (`export_gate.R`, `cell_key.R`, the schema and the policy) are not edited. The export is the schema's
Parquet files and `receipt.json`; tables and figures are made locally from them.

## Packages and data.table

A new package goes in `rproject.toml` (`rv add <package>`) and in `PACKAGES` in `run.R`;
a mock run stops if the lists differ. The server installs it from its own mirror, in
whatever version that holds; the run manifest records the versions that ran.

With data.table:

- Write `dt[flag == TRUE]`, never `dt[flag]`: a single name there is looked up outside
  the table, not as a column. `jarl.toml` turns off the rule that would "fix" it.
- `:=` and `set*()` change a table in place. A step that modifies its input starts with
  `copy()`, or it alters the cached result of the step before it.

## A mock run

```bash
Rscript run.R
```

The project's `.Rprofile` sets `REGMETA_MOCK`, so a local run is a mock run. Without
that file: `REGMETA_MOCK=1 Rscript run.R`.

The run checks the mode, the input folder, the packages and the pinned files, builds the
pipeline (unchanged steps are skipped), writes staging and ends with the export gate. A
pass leaves `output/export/`, CSV copies in `output/review/` and
`output/run_manifest.json`, and prints a source id. A refusal leaves no export; its
report is `output/.diagnostics/export_gate_report.json`, readable on a mock run. Fix the
analysis or the disclosure stage. Never change the policy to make a run pass.

The project's first mock run is also its first check of the tooling. A failure before
the pipeline starts (packages, pins, the mock folder) is a setup fault: fix it there.

Tests in `tests/` run locally only, on small hand-written fixtures, never on mock data
or real data. Add one for a named failure an assertion cannot catch. Do not test that
the gate refuses a bad file: the gate's own cases cover it.

## A batch

A batch is the committed `src/` at one commit.

1. `air format .` and `jarl check .`, then commit. From the clean checkout, run the
   tests and a mock run that ends in a pass.
2. Write the commit and the printed source id in the ticket. The id is a hash of
   `run.R` and every file in `src/`, so the real run prints the same id when the
   uploaded files are the committed ones.
3. Tell the researcher what to do, in a few lines: upload every file in `src/`, submit
   `run.R`, compare the source id, review, export. On MONA:
   [mona.md](references/mona.md). On another server:
   [operator-review.md](references/operator-review.md).

`run.R` is uploaded once. Tell the researcher when a batch needs it again (a changed
data folder, package list, schema, policy or pin).

## Results

The researcher reviews the export where it was made, sends it out, and approves it for
model access. Then:

1. Import only what the researcher sent out from the server, never a local mock run's
   `output/export/`: a receipt does not say which mode ran. Check every file against
   `receipt.json`, and the receipt against the pins in `run.R`
   ([export-gate.md](references/export-gate.md), "Importing an export"). A mismatch is
   a rejected import.
2. Keep the export unchanged in `results/<date>/` and commit it.
3. Write the answer in the ticket, with the commit, the source id and the result folder.
   A suppressed cell stays missing: never zero, never rebuilt from margins.

Make tables, figures and text locally from `results/`. A new sample, grouping or
statistic needs a new batch; a change of presentation does not.

## References

- [contracts.md](references/contracts.md): tickets, source contracts, runs, results and
  disclosure.
- [execution.md](references/execution.md): local and remote work, transfers,
  incremental builds.
- [export-gate.md](references/export-gate.md): the gate, its pins, the disclosure
  policy, cell keys, importing.
- [mona.md](references/mona.md): a project on MONA.
- [operator-review.md](references/operator-review.md): another server, and approval for
  model access.
