# A register project on MONA

[MONA](https://www.scb.se/mona) is Statistics Sweden's microdata platform. No agent runs
on MONA, and row-level data never leaves it: only aggregates checked by the export gate
do, after a human has reviewed them. The agent works on the researcher's machine, from
code, catalog metadata and mock data.

This is the order of work for a project whose data a steward (SWECOV, for example)
delivers into MONA from its holdings. Each step names who does it.

## 1. The catalog (researcher, once)

Point the plugin at the steward's catalog, not the global one: the plugin README,
"Steward catalogs". Only a steward's catalog knows its holdings, delivers its own file
and column names, and writes MONA order files. Every `meta.catalog_version` the tools
return is that steward's release, which can lag the global catalog. Use that release for
everything below: the project's `reg_meta_version`, the `reg-mock` binary, `extract.R`
and `export_gate.R`.

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

The researcher sends the reviewed `project_data.json` to the steward by the steward's own
procedure. The steward's coordinator writes the order file with `materialize_order` (or
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
policy (the disclosure thresholds). The toolkit's `reg_export_gate/schemas/` ships
`trial` (person counts by year and group, and a per-year mean, counts at least 5) and
`mock-stats` (the mock-data statistics scaffold's own outputs); neither declares a
research project's outputs. A real project needs a schema that declares its outputs and
a policy that a human has reviewed against the data holder's disclosure rules. Both are
toolkit files, not project code: the agent may list the files and columns the analysis
needs, but it never writes, edits or loosens a schema or a policy.

Until the researcher has chosen a reviewed pair, a first batch can shape its outputs to
the `trial` schema, so the whole flow, gate included, runs end to end on mocks. Keep real
release disabled: exporting real results under `trial` is the researcher's explicit
decision, not a default.
