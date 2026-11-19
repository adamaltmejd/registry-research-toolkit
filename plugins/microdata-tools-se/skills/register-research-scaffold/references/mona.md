# A register project on MONA

[MONA](https://www.scb.se/mona) is Statistics Sweden's microdata platform. No agent runs
on MONA, and row-level data never leaves it: only aggregates checked by the export gate
do, after a human has reviewed them. The agent works on the researcher's machine, from
code, catalog metadata and mock data.

This is the order of work for a project whose data a data steward
delivers into MONA from its holdings. Each step names who does it.

## 1. The catalog (researcher, once)

Add the steward's catalog to the project as its `steward-catalog` MCP server, in the
project's `.mcp.json` (or `.codex/config.toml`), and record in the project instructions
that the agent uses it, not the plugin's global `catalog`: the
`register-metadata-search` skill's `references/data-stewards.md`. Only a steward's
catalog knows its holdings, delivers its own file and column names, and writes MONA
order files. Every `meta.catalog_version` the tools return is that steward's release,
which can lag the global catalog. Use that release for everything below: the project's
`reg_meta_version`, the `reg-mock` binary, `extract.R` and `export_gate.R`.

## 2. The order (agent drafts, researcher approves)

Draft `project_data.json` with the `register-project` skill, against the steward's
catalog. Validate until `ok` is true, and settle every warning with the researcher. A
steward with several databases needs `linkage`: one project per database. Choose
`extract_format` (`dta`, the default, `parquet` or `csv`) for the language the analysis
runs in on MONA. Commit the file.

`order` with `operation: "materialize_order"` returns the MONA order file the steward
would write. Calling it shows the researcher exactly which files and columns the extract
will hold, but it submits nothing.

## 3. Mock inputs (agent)

Generate mock files shaped like the delivery with the `register-mock-data` skill (`reg-mock`):
the same file and column names and types as the extract, in the order's format. Keep them
outside the project in the configured mock input directory. The pipeline switches between
mock and real data by its input directory alone.

## 4. The extract (steward, on MONA)

The researcher sends the reviewed `project_data.json` to the steward following the
steward's `order_instructions` (in `data.steward_config.presentation` of
`https://<steward-host>/api/context`; ask the steward if it has none). The steward's
coordinator writes the order file with `materialize_order` (or
`POST /api/project/materialize`). The steward's data manager uploads the order file to
MONA with `extract.R` from the toolkit's `reg_mona_runner/` at the steward's pinned tag
and the steward's own `extract-config.json`, and runs it. The extract is one file per
delivered table (a register delivered per year gives one file per year), plus
`run_log.csv`, in the order's format. Where it lands in the researcher's MONA project is
the steward's procedure.

The project holds neither `extract.R` nor `extract-config.json`: both belong to the
steward. Manual bindings and sources (`manual` in the project) are not extracted; the
data manager adds them by hand. `extract.R` has not yet been verified on MONA with a real
order. Treat the first extract as a trial and check its `run_log.csv` row and column
counts against the order.

## 5. Analysis (researcher, on MONA)

The researcher uploads the project's committed analysis code, the pinned
`export_gate.R` and the chosen schema and policy to the MONA project folder, then runs
the pipeline by hand. The pipeline reads the extract folder as its real input
directory, writes its aggregates to a fresh staging folder and ends with the export gate
([export-gate.md](export-gate.md)). The same code ran locally on the mock inputs first,
ending with the same gate call.

A project folder on MONA:

```text
<project>\
  <the analysis code, as committed>
  export_gate\
    export_gate.R               pinned, unchanged
    cell_key.R                  pinned, unchanged
    <name>.schema.json          chosen, unchanged
    <name>.policy.json          chosen, unchanged
  staging\                      the pipeline's aggregates; the gate only reads it
  export\                       only on a pass: the schema's files and receipt.json
  .diagnostics\                 the gate's report; never exported
```

## 6. Export (researcher)

On a pass, the researcher reviews `export\`, zips it and sends it out through MONA's own
export. Never include `staging\` or `.diagnostics\`. Outside MONA, the import checks each
file against `receipt.json` ([export-gate.md](export-gate.md), "Importing an export").
The agent sees the files only after a human has approved them for model access
([operator-review.md](operator-review.md)). On a refusal, the researcher tells the agent
only the category code (`setup`, `inventory`, `structure`, `disclosure`, `backstop`).

## The schema and policy

The gate checks the export against a schema (which files and columns may leave) and a
policy (the disclosure thresholds). The toolkit ships its pairs in
`reg_export_gate/schemas/`. The default for a project's results is
`schemas/aggregates.schema.json` with `schemas/aggregates.policy.json` (policy name
`aggregates-min5`): long-form counts, means, quantiles and regression
coefficients by groups. Shape the pipeline's outputs to it, a first batch included, so
the whole flow, gate included, runs end to end on mocks. A file the project does not use
is written with zero rows. `trial` and `mock-stats` are not for a project's outputs.

The gate only checks; the pipeline suppresses, perturbs (`cell_key.R`) and rounds. The
default policy is a motivated, cited set (a floor of 5, cell-key perturbation of counts,
4 significant digits, a regression small-group check, no dominance check), and every
parameter is configurable per project. [export-gate.md](export-gate.md), "The disclosure
policy", lists them and says where a project sets its own. The agent lists the files and
columns the analysis needs and shapes outputs to the schema; it changes a policy only
when the researcher asks, and never to make a failing run pass. A policy or schema
outside the toolkit's shipped files is the researcher's to review before real release.
