# A register project on MONA

[MONA](https://www.scb.se/mona) is Statistics Sweden's microdata platform. No agent runs
on MONA, and row-level data never leaves it: only aggregates checked by the export gate
do, after a human has reviewed them. The agent works on the researcher's machine, from
code, catalog metadata and mock data. Everything on MONA is done by the researcher.

## What MONA gives a project

- A project code `PXXXX` (`P` and four digits) and two network folders its members
  share: `PXXXX_Data`, read-only, where delivered files land, and `PXXXX_Gem`, for the
  project's own files. Data ordered directly from SCB may instead be delivered in SQL
  Server, data source `PXXXX`; the template reads files only.
- A batch runner. It starts one script with no arguments and no terminal, and writes the
  log beside it (`run.R` gives `run.Rout`). The log may show values: it never leaves
  MONA and is never pasted to the agent.
- Upload through a web form: several files at once, text files only, no folders, no
  archives. Every uploaded file lands in one inbox folder.
- No Git and no internet. R packages install from MONA's own mirror.
- Its own export, with size limits per export and per month. So everything that leaves
  is in one folder, `output/export/`, in one format.

## The project folder on MONA

The project folder is a folder in `PXXXX_Gem`, with the same layout as the local one.
Everything the project writes on MONA stays inside it.

```text
<project>\
  run.R                 uploaded once; submitted to the batch runner
  run.Rout              the batch log; stays on MONA
  src\                  every file from the local src/, replaced by each batch
  output\
    export\             only after a gate pass: the schema's files and receipt.json
    review\             the same tables as CSV, to read on MONA; not exported
    run_manifest.json   what ran: file hashes, source id, versions; not exported
    .diagnostics\       the gate's report; not exported
  .regmeta\
    keys\               the cell-key file; never leaves, never opened by the agent
    staging\, targets\  the run's working files
```

## The order of work

1. **The data** (`project-data` skill). A project that waits for data drafts its order
   in `project_data.json` and sends it to whoever delivers: SCB, or a data steward
   following its own instructions. A project whose data is already delivered starts
   from the data summary instead: the researcher runs the generated summary script on
   MONA, reviews the CSV and exports it, and the agent matches it to the catalog.
2. **Mock data** (`mock-data` skill). Files shaped like the delivery, in the project's
   `.regmeta/mock-data/`. The pipeline is written and run on them first.
3. **A batch.** The agent finishes the batch locally: a mock run from a clean commit
   that ends in a gate pass and prints a source id. The researcher uploads every file
   in `src/`, empties the project's `src\` on MONA and moves the uploaded files from the
   inbox into it (and `run.R` beside it, the first time), then submits `run.R`. Emptying
   first matters: every R file in `src\` is loaded, a removed one included.
4. **The run** reads `PXXXX_Data`, creates or extends the cell keys, builds the
   pipeline and ends with the export gate ([export-gate.md](export-gate.md)).
   Any failure, a refused gate included, leaves no `export\`. The researcher tells the
   agent only the refusal's category code (`setup`, `inventory`, `structure`,
   `disclosure`, `backstop`), or the step a run stopped in before the gate, never the
   log.
5. **Review and export.** On a pass the researcher compares the source id at the end of
   `run.Rout` with the one the local mock run printed: equal ids mean the uploaded files
   are the committed ones. They read the tables in `output\review\`, zip `output\export\`
   and send it out through MONA's own export. Nothing else leaves.
6. **Import.** Outside MONA the import checks each file against `receipt.json`
   ([export-gate.md](export-gate.md), "Importing an export"). The agent sees the files
   only after the researcher has approved them for model access.

## A steward's delivery

A data steward holds an extract of register data in MONA and delivers from it. Its
catalog knows its holdings and its own file and column names, and its release can lag
the global catalog: the project's `reg_meta_version`, `reg-mock` and the export gate
follow the steward's release (`catalog-search` skill, `references/data-stewards.md`).

The researcher sends the reviewed `project_data.json` to the steward following the
steward's `order_instructions` (in `data.steward_config.presentation` of
`https://<steward-host>/api/context`). The steward writes the MONA order file and runs
the extract; the files land in the project's `PXXXX_Data`, one per delivered table (a
register delivered per year gives one file per year), with a `run_log.csv` whose row
and column counts can be checked against the order. Manual bindings and sources are not
extracted; they are added by hand. The project holds none of the steward's tools.

## Not yet tried on MONA

The template is verified locally, under `Rscript` and `R CMD BATCH`. A first real batch
is a trial of: how the batch runner starts `run.R` and from which folder; whether the
web upload changes line endings (file hashes ignore carriage returns); creating and
locking the key file on the network share; installing packages from MONA's mirror;
`targets` under the batch runner.
