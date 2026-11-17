# The export gate

The release stage is the toolkit's export gate, not project code. Its canonical source
is the `reg_export_gate/` folder of the toolkit repository
(<https://github.com/adamaltmejd/registry-research-toolkit/tree/main/reg_export_gate>):
`export_gate.R`, the schemas and policies in `schemas/`, the MONA steps in `README.md`
and the design in `DESIGN.md`. There is one copy of the check, there; a project holds a
pinned copy of its bytes and never a variant.

**Pinned version: `EXPORT_GATE_VERSION <- "0.4.0"`.** Take `export_gate.R` from the
`reg_meta` tag the project's `reg_meta_version` names (0.4.0 is in `reg_meta/v0.78.0` and
later):
`https://raw.githubusercontent.com/adamaltmejd/registry-research-toolkit/reg_meta/vX.Y.Z/reg_export_gate/export_gate.R`.
Check the version line in the copy and record its SHA-256. Take the schema and policy from
wherever the researcher chose the reviewed pair, possibly a later tag, and record their
SHA-256 too. A newer gate version is a
deliberate update of this pin, not something to pick up silently.

The R file imitates the Rust gate (`crates/reg-export-gate`), which is the reference and
can be ahead: Rust 0.5.0 adds check-only columns, `share` and `count_units_required`. A
schema or policy that uses them is refused by R 0.4.0 as `setup` (`schema_invalid`,
`policy_invalid`), so on MONA use only pairs the R version accepts.

## What the project holds

- `export_gate.R`, byte for byte as the toolkit ships it. Never edit, wrap or
  concatenate it into project code: every receipt names its version.
- The chosen schema and its policy, `<name>.schema.json` and `<name>.policy.json`, also
  unchanged. The user chooses them; the agent never writes, edits or loosens a schema or
  a policy, and never adds one to the project. If no reviewed schema declares the
  project's outputs, say so and keep real release disabled: a new schema or policy is a
  toolkit change a human reviews ([mona.md](mona.md), "The schema and policy").
- In the project instructions: the gate version, the schema and policy names and
  versions, and the three files' SHA-256, so a changed copy is visible.

Commit the three files with the project. Upload them to MONA unchanged (the toolkit
README's step 1).

## The pipeline's last step

The pipeline writes its complete output into a fresh staging folder (delete and recreate
it at the start of every run), then calls the gate as its final step, with nothing after
it writing to the output folder:

```r
source("export_gate/export_gate.R")
export_gate(
  staging = "<run>/staging",
  output = "<run>/export",
  schema = "export_gate/<name>.schema.json",
  policy = "export_gate/<name>.policy.json"
)
```

- The output files are Parquet, one per schema file; column types as the schema says (R
  `integer` for counts); a suppressed cell is `NA` (null), never `0`, `NaN` or `Inf`.
- On a pass the gate rewrites each checked table (the decoded data only, no metadata)
  into a temporary `.export-gate-*` folder beside `<run>/export`, adds `receipt.json`
  and renames that folder to `<run>/export`. Staging is not moved or changed. The export
  holds exactly the schema's files and the receipt; the receipt's hashes are of the
  files the gate wrote, not of the staged bytes.
- `<run>/export` must be absent, empty or a prior gate export (a receipt and the files
  it lists). The gate deletes a prior export before checking, so a refused run never
  leaves an old export looking current; any other content, even a file Windows Explorer
  adds (`desktop.ini`, `Thumbs.db`), is refused as `setup` (`output_not_gate_export`).
- On a refusal there is no export, staging is as it was, and the call errors with class
  `export_gate_refusal` and a category code: `setup`, `inventory`, `structure`,
  `disclosure` or `backstop`. The pipeline fails with it.
- The report (rule, plus file and column where the schema declares them; never a value)
  is `<run>/.diagnostics/export_gate_report.json`. A file or column the schema does not
  declare is located by its `position` in the sorted listing or column order, never by
  name. It stays on the data side: never exported, synced or shown to the agent. The
  agent learns a refusal's category code from the user, nothing more.

## A Python pipeline

The gate is R even when the pipeline is not. With the scaffold's Python/DuckDB stack,
`run.py` writes the staging Parquet files and exits; the gate then runs as its own R
step:

```sh
uv run run.py                     # writes <run>/staging; non-zero exit on any failure
Rscript export_gate/run_gate.R    # the gate, only if run.py succeeded
```

`run_gate.R` is a few lines of project code: it `source()`s the pinned `export_gate.R`
and makes the `export_gate()` call above with the paths `run.py` used. It adds no checks
and never edits the gate. Here "the execution file's last step" means the gate is the
last command of the run: `run.py` never writes to `<run>/export`, and nothing runs after
`Rscript`. A refusal makes `Rscript` exit non-zero; the category code is in its console
line and in the report.

The gate checks each column's Arrow type as Parquet stores it. Write each column with a
type the gate accepts for its schema type:

  | Schema type | Arrow types accepted     | Write from DuckDB as  |
  | ----------- | ------------------------ | --------------------- |
  | `integer`   | `int32`, `int64`         | `INTEGER` or `BIGINT` |
  | `double`    | `double` (float64)       | `DOUBLE`              |
  | `string`    | `string`, `large_string` | `VARCHAR`             |
  | `boolean`   | `bool`                   | `BOOLEAN`             |
  | `date`      | `date32[day]`            | `DATE`                |

Cast every output column explicitly in the final `SELECT` (`n::BIGINT`, `mean::DOUBLE`):
`FLOAT` and `DECIMAL` are refused as `structure` (`column_type`). A dictionary column (an
R factor, a pandas `category`) is read as its value type. A staging file over 8 MiB or
100,000 rows is refused as `structure` (`too_large`). A suppressed cell is SQL `NULL`, never `NaN` or `Infinity`:
a `NaN` in any `double` column is refused as `disclosure` (`nan_value`). The gate writes
`integer` columns as `int64`, `double` as `float64` and `date` as `date32`. It also
refuses a value shaped like a personnummer in any string, integer, date or the
whole-number part of a double (`backstop`, `identifier_pattern`). Under a policy that
sets them, a statistic resting on too small a support is `disclosure`
(`statistic_support_too_small`), and a `double` statistic with more significant digits
than allowed is `disclosure` (`statistic_not_rounded`): round it before writing.

## Preflight

The gate needs R 4.2 or newer with `arrow` 9.0.0 or newer and `jsonlite`, plus SHA-256:
base `tools::sha256sum` on R 4.5 or newer, else the `digest` package. Each call installs
whichever is missing or too old into R's default library from the configured repository;
one it cannot install refuses as `setup` (`arrow_missing`, `arrow_too_old`,
`jsonlite_missing`, `sha256_missing`). Step 3 of the gate README says what to do then.
Check that `Rscript` is on the path where `run.py` runs.

## Locally, on mocks

Run the same call last in every local mock run (the Preflight packages, including
SHA-256), so the pipeline's final step is exercised before any handoff. A mock pass is
not disclosure clearance. Keep one rejection case in the small fixture suite: a mock run
the gate refuses, leaving no export.

## Importing an export

Before reading an export, check each file's SHA-256 against `receipt.json` (it hashes
the files the gate wrote), and the receipt's gate version and schema and policy hashes
against the project's pins. A mismatch is a rejected import. The receipt is unsigned: it
shows integrity, not that the gate ran.

## Other servers

A Linux x86_64 server that can run executables uses the `reg-export-gate` binary, an
asset of each `reg_meta/v*` release named by the gate's own version
(`reg-export-gate-<version>-x86_64-unknown-linux-gnu`). The pipeline's last step runs
`reg-export-gate check --project NAME --staging DIR`, with the schema, policy and folders
in the server's project config, outside the project. The reviewer, after inspecting the
checked output, runs `reg-export-gate export --project NAME --receipt <SHA-256 of the
reviewed receipt.json>` to deliver it one way to the agent's side. There is no review
UI; the setup is in `crates/reg-export-gate/DESIGN.md` in the toolkit repository. A server that cannot run it runs the same R file as the
last step, the same way, and the human review of the export stays as described in
[operator-review.md](operator-review.md).
