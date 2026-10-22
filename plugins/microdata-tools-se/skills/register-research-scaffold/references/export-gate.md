# The export gate

The release stage is the toolkit's export gate, not project code. Its canonical source
is the `reg_export_gate/` folder of the toolkit repository
(<https://github.com/adamaltmejd/registry-research-toolkit/tree/main/reg_export_gate>):
`export_gate.R`, the schemas and policies in `schemas/`, the MONA steps in `README.md`
and the design in `DESIGN.md`. There is one copy of the check, there; a project holds a
pinned copy of its bytes and never a variant.

**Pinned version: `EXPORT_GATE_VERSION <- "0.1.0"`.** Copy `export_gate.R` from the
toolkit revision that carries this line, and check the line in the copy. A newer gate
version is a deliberate update of this pin, not something to pick up silently.

## What the project holds

- `export_gate.R`, byte for byte as the toolkit ships it. Never edit, wrap or
  concatenate it into project code: every receipt names its version.
- The chosen schema and its policy, `<name>.schema.json` and `<name>.policy.json`, also
  unchanged. The user chooses them; the agent never writes, edits or loosens a schema or
  a policy, and never adds one to the project. If no shipped schema declares the
  project's outputs, say so and keep real release disabled: a new schema is a toolkit
  change a human reviews.
- In the project instructions: the gate version, the schema and policy names and
  versions, and the three files' SHA-256, so a changed copy is visible.

Commit the three files with the project. MONA accepts only `.R` and `.py` uploads; the
toolkit README's step 1 says how to bring the JSON files in unchanged.

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

- The output files are Parquet, one per schema file, written with `arrow`; column types
  as the schema says (R `integer` for counts); a suppressed cell is `NA`, never `0`.
- On a pass the gate writes `receipt.json` and moves staging to `<run>/export` in one
  rename, so staging and export must be on the same volume. The export holds exactly the
  schema's files and the receipt.
- On a refusal there is no export, staging is left as it was, and the call errors with
  class `export_gate_refusal` and a category code: `setup`, `inventory`, `structure`,
  `disclosure` or `backstop`. The pipeline fails with it.
- The report (file, column and rule; never a value) is
  `<run>/.diagnostics/export_gate_report.json`. It stays on the data side: never
  exported, synced or shown to the agent. The agent learns a refusal's category code
  from the user, nothing more.

## Locally, on mocks

Run the same call last in every local mock run (R with `arrow` and `jsonlite`), so the
pipeline's final step is exercised before any handoff. A mock pass is not disclosure
clearance. Keep one rejection case in the small fixture suite: a mock run the gate
refuses, leaving no export.

## Importing an export

Before reading an export, check each file's SHA-256 against `receipt.json`, and the
receipt's gate version and schema and policy hashes against the project's pins. A
mismatch is a rejected import. The receipt is unsigned: it shows integrity, not that the
gate ran.

## Other servers

The `reg-export-gate` binary (review UI, one-way delivery) is designed but not released.
Until it is, a flexible server runs the same R file as the last step, the same way, and
the human review of the export stays as described in
[operator-review.md](operator-review.md).
