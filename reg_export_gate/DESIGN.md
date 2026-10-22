# Design: the export gate (`reg_export_gate`)

The export gate is the toolkit's disclosure checker and transfer. It is the only path by
which results computed on microdata reach an agent. It fills the checker slot of the
register-research scaffold, which treats the disclosure checker as an external
executable it consumes. The domain invariant holds throughout: PII never leaves the data
side, only aggregate statistics do, and disclosure control is a load-bearing guard.

The MONA check (`export_gate.R`), its schemas and policies and the case corpus are
built. The Rust binary is not (#1803). Trial steps for MONA are in
[README.md](README.md).

## Components

- **`reg-export-gate`.** One static Rust binary, in a crate of the same name in
  `crates/`, built and released next to `reg-meta`. It runs the whole disclosure check,
  stages the checked bytes for human review, serves the review UI and pushes approved
  bytes to the agent VM. An agent calls it; it never reimplements it. The name says what
  it does: the check is disclosure control on aggregates, not only PII detection, so
  "pii" is avoided. Not built yet.
- **Schemas and policy.** Fixed, versioned JSON files supplied by the toolkit, each with
  a policy file beside it, in `schemas/`. Choosing or adding a schema is a deliberate
  user configuration step. An agent never writes or edits a schema or a policy.
- **MONA check file.** `export_gate.R`: one fixed, version-pinned R file of check
  functions, for MONA, where only `.R` and `.py` uploads are accepted (see "MONA mode").

## Trust boundaries

Three parties, which may share one host; on a shared host the same separation holds
between the agent's environment and the gate:

- **Agent VM.** Code, mock data and approved aggregates only. It never sees unreviewed
  output, check reports (beyond the refusal category code), logs or the review UI.
- **Data VM.** Microdata, the pipeline and `reg-export-gate`.
- **Human.** Reviews the checked output in the review UI and presses Export.

The review UI is unreachable from the agent VM, by `curl` or any other client. That is
enforced by the VM network setup, not by instructions to the agent. The review UI is
part of the `reg-export-gate` binary and listens only on the data-side network. After
Export, the binary pushes the approved bytes one way to the agent VM. The gate records
each file's SHA-256 at check time and verifies it on delivery. A changed byte voids the
approval and needs a new check.

The prior art is the education-returns `server.sh` (adamaltmejd/education-returns#18).
It stops three times: before code reaches the data server, before checked output is
transferred, and before results are published to git. It stages output with SHA-256
checksums. The gate takes over the second stop. Carrying code into the data VM (the
first stop) is out of scope for v1.

## Schemas

A schema is a JSON file that declares exactly the files an export may hold. Per file:
its name, its columns, each column's type and its role. JSON is readable from Rust and,
on MONA, from R through jsonlite, which the extract runner already uses.

Thresholds are not in the schema. A separate policy file beside the schema holds the
disclosure rules, such as the smallest count allowed. The schema says what each column
is (a count of persons, a statistic resting on a count); the policy says how strict the
rules on those roles are. The schema stays fixed while an institution's thresholds
change. The toolkit asserts no universal threshold. The schema in force is identified by
name, version and content hash, and every check records them.

The code under check must not also define what the check accepts. In education-returns
each script brought its own schema (`schemas/<script>.R`) and its checks were project R
(`src/export.R`, `EXPORT_COLUMNS`). An agent writing the analysis could widen the gate
in the same commit. So schemas and policy are toolkit files, never project code.

The choice of schema is configuration, kept in two places by mode:

- **MONA.** It lives with the project, since MONA mode has no separate app.
- **Server.** The app has its own config directory outside the project repo, for example
  `~/.config/reg-export-gate/`.

Wherever the choice lives, the schema and policy files are toolkit files: on MONA the
scaffold copies them in with the R file, and on the server they live in the app's config
directory, outside any repo the agent can write. A policy change loosens or tightens
disclosure, so it is a human, reviewed step.

### Schema format (`format_version` 1)

Every object is closed: an unknown key is a setup refusal.

```json
{
  "format": "reg-export-gate/schema",
  "format_version": 1,
  "name": "trial",
  "version": 1,
  "description": "optional",
  "files": [
    {
      "name": "summary_by_year.parquet",
      "description": "optional",
      "columns": [
        { "name": "year", "type": "integer", "role": "dimension" },
        { "name": "n_persons", "type": "integer", "role": "person_count" },
        { "name": "mean_value", "type": "double", "role": "statistic", "support": "n_persons" }
      ]
    }
  ]
}
```

- `name` is a lowercase slug; `version` a positive integer. A file name is a lowercase
  slug ending in `.parquet`, so `receipt.json` can never be a data file; a schema that
  declares it anyway is refused by name (`schema_reserved_name`). Column names are
  lowercase snake case.
- `type` is one of `integer`, `double`, `string`, `boolean`, `date`. Each accepts these
  Arrow types in the Parquet file: `integer` → `int32` or `int64` (width is not a
  disclosure concern, and R writes `int32`), `double` → `double`, `string` → `string` or
  `large_string`, `boolean` → `bool`, `date` → `date32[day]`.
- `role` is one of `dimension` (a key: year, region, group), `count`, `person_count` and
  `statistic`. A count of either kind has type `integer`. A `statistic` names in
  `support` the count or person count in its own file that it rests on; no other role
  has `support`.

`count` and `person_count` are separate roles so a later policy can treat them
differently (a person count is the unit disclosure control protects); the format-1
policy applies one minimum to both.

### Policy format (`format_version` 1)

```json
{
  "format": "reg-export-gate/policy",
  "format_version": 1,
  "name": "trial-min5",
  "version": 1,
  "description": "optional",
  "schema": { "name": "trial", "version": 1 },
  "min_cell_count": 5
}
```

The policy names the schema name and version it was written for; any other pairing is
refused. `min_cell_count` is a positive integer.

The shipped files: `schemas/trial.schema.json` and `schemas/trial.policy.json` (minimum
5), for the first MONA trial.

## The check

The check runs as the pipeline's last step, over the complete candidate output in a
staging directory. A partial run is never checked, so it cannot become a releasable
output. In order:

1. **Inventory.** The staging directory holds exactly the schema's files: none missing,
   none extra, no links, folders or hidden files, and no `receipt.json`.
2. **Structure.** Each file is readable Parquet, and its columns are the schema's
   columns in the schema's order, each of an accepted type.
3. **Disclosure.** Each file passes the policy's rules: every non-null `count` and
   `person_count` is at least `min_cell_count`, and a `statistic` is null (or NaN)
   wherever its `support` is null. Null is the suppression marker, so a zero count is
   refused like any other count below the minimum.
4. **Backstop scan.** String values, and each file's key-value metadata, are scanned for
   personnummer and samordningsnummer shapes (10 or 12 digits, optional `-` or `+`, a
   valid month and day or day + 60, not inside a longer digit run). This is a backstop
   only: a pattern scan cannot establish safety (#20), and a misclassified column can
   pass a count threshold (#17). Organisationsnummer share the shape and also match.

Setup stops at its first finding; each of the four steps collects every finding over all
files, and the check stops at the first step with a finding. Only when every file passes
does the gate write `receipt.json` into the staging directory (step 1 ran before it
existed), and the files and the receipt then move together, atomically, to the output
folder. On any refusal the output folder holds nothing. The output folder is data-side:
its files are checked but not yet approved, and only the human's Export pushes them to
the agent VM. Refusals have stable codes by category: setup, inventory, structure,
disclosure, backstop, and delivery integrity (a hash mismatch; binary only). A report
names the file, column and rule, never a value, as in the MONA extract runner
([../reg_mona_runner/DESIGN.md](../reg_mona_runner/DESIGN.md), "No row values in logs").
Reports, logs and diagnostics stay on the data side and never enter the output folder.

A failed check tells the agent only the refusal category code. Each export is checked
alone in v1; differencing against earlier approved exports comes later. The format-1
policy has a single threshold rule: no secondary suppression, dominance rule or check
across files (a suppressed cell recoverable from a margin in another file passes). Those
are policy work for later versions; until then the human review covers them.

### Refusal codes and rules

The code is the category; the rule locates the finding. Both are stable.

- `setup`: `staging_missing`, `output_parent_missing`, `paths_overlap`,
  `output_not_gate_export`, `output_not_removable`, `jsonlite_missing`, `arrow_missing`,
  `sha256_missing`, `schema_unreadable`, `schema_invalid`, `schema_version`,
  `schema_reserved_name`, `policy_missing`, `policy_unreadable`, `policy_invalid`,
  `policy_version`, `policy_schema_mismatch`, `move_failed`, `internal_warning`,
  `internal_error:<R condition class>`.
- `inventory`: `missing_file`, `extra_file`, `hidden_file`, `not_a_file`,
  `reserved_name`.
- `structure`: `unreadable`, `missing_column`, `extra_column`, `column_order`,
  `column_type`.
- `disclosure`: `min_cell_count`, `unsupported_statistic`.
- `backstop`: `identifier_pattern`, `identifier_pattern_metadata`.

`schema_version` and `policy_version` mean a `format_version` this gate does not read.
`output_not_gate_export` means the output folder holds files without a `receipt.json`:
the gate removes only its own previous export (or an empty folder) and leaves anything
else for the user.

### Receipt

The receipt is exported with the data. It holds only the gate version, the schema's and
policy's name, version and hash, and each output file's SHA-256: no values, counts or
messages. Anyone downstream (the agent, a co-author, a replication) can check the files
against it with any SHA-256 tool. That is integrity, not authenticity: the receipt is
unsigned, so it shows the files match what the gate recorded, not that the gate ran. The
gate's delivery record covers `receipt.json` too, so a changed receipt fails delivery
like a changed data file. Its format is fixed by the toolkit, not by the schema:

```json
{
  "format": "reg-export-gate/receipt",
  "format_version": 1,
  "gate": { "implementation": "R", "version": "0.1.0" },
  "schema": { "name": "trial", "version": 1, "sha256": "…" },
  "policy": { "name": "trial-min5", "version": 1, "sha256": "…" },
  "files": [{ "name": "persons_by_year_group.parquet", "sha256": "…" }]
}
```

Files are listed in schema order; there is no timestamp, so a rerun over the same bytes
writes the same receipt. Hashes are of the file bytes; the schema and policy hashes are
of the JSON files as read.

Reports, logs and diagnostics go in a `.diagnostics/` folder beside the output folder
and are never exported. The MONA check writes one,
`.diagnostics/export_gate_report.json`: the gate version, `result` (`pass` or
`refused`), `code` and `findings` (`file`, `column`, `rule`, each null where it does not
apply).

## Output format

One Parquet file per schema file, readable from Rust, R and Python. Outputs are Parquet
only. On MONA they are written with the `arrow` R package. The `duckdb` R package is
also installed there, but DuckDB is a reader only: no `.duckdb` files are outputs.
Column types come from the schema. A suppressed cell is null, never zero.

## MONA mode

No standalone MONA system and no web UI. No agent runs on MONA and the Rust binary is
not run there; the R file is the only toolkit code on MONA. The toolkit ships one fixed,
version-pinned R file of check functions; the scaffold copies it into the project, and
the pipeline's last step calls it with the chosen schema and its policy. The
output-folder model is the same: the folder holds exactly the schema's files and
`receipt.json`, which appear only if the whole check passes. Logs, error messages and
diagnostics never go in output files; they go in `.diagnostics/` beside the folder. The
user reviews the folder, zips it and sends it out through MONA's export.

The Rust binary and the R file are two implementations of one check, an exception to
"one implementation per contract". One case corpus (schema and candidate folder →
expected pass or refusal code) runs against both, so their agreement is the oracle. Each
R file version will be pinned to a binary release once the binary ships; until then
`EXPORT_GATE_VERSION` in the file is the pin, and it is in every receipt.

### The R file

- **Base R (>= 4.2), jsonlite and arrow**, all on MONA. SHA-256 comes from
  `tools::sha256sum` (R >= 4.5) or else the `digest` package; with neither the check
  refuses `sha256_missing` before reading anything. Both hash the bytes in memory.
- **Two names.** Sourcing defines `EXPORT_GATE_VERSION` and `export_gate()`; the helpers
  live in the function's closure, so the file does not clutter the pipeline's
  environment.
- **`export_gate(staging, output, schema, policy)`** returns `"pass"` invisibly, or
  signals an error of class `export_gate_refusal` with `$code`, so a refused gate fails
  the pipeline. The console shows the code and the findings (rules, files, columns). The
  `.diagnostics/` folder is `dirname(output)/.diagnostics`.
- **Each file is read once.** Its bytes are hashed and parsed from the same raw vector,
  so the receipt hashes the bytes that were checked.
- **The move is one `file.rename()`** of the staging folder to the output folder, so
  staging and output must be on the same volume; the gate never copies. A failed rename
  removes the receipt from staging and refuses `move_failed`. Before any check the gate
  removes its previous export from `output`, so a refused run never leaves an old export
  looking current.
- **No values in output.** Rules, file and column names only. An unexpected R error is
  reported by its condition class, and an R warning refuses (`internal_warning`) rather
  than printing, since R and Arrow messages can quote a value.

## Case corpus

`tests/cases/<case>/case.json`, one behavior per case, run by `tests/testthat/` and
later by the Rust binary's tests. It is data: every expectation is in the JSON.

- `description`: the behavior, and so the regression that breaks it.
- `schema`, `policy`: paths relative to the case folder (the shipped files are
  `../../../schemas/trial.*.json`; a setup case carries its own beside `case.json`), or
  null for "not given".
- `candidate`: the staging folder, by file name. A Parquet file is
  `{"columns": [{"name", "type", "values"}], "metadata": {...}}` with Arrow types
  `int32`, `int64`, `double`, `string`, `bool` and JSON `null` for a null cell; other
  entries are `{"text": "..."}` (raw bytes) or `{"folder": true}`.
- `output_before`: the output folder before the run, in the same form (optional).
- `expect`: `{"result": "pass"}`, or
  `{"result": "refused", "code", "findings": [{"file", "column", "rule"}]}` with
  `"output": "unchanged"` when the output folder is not the gate's to clear.

Every case asserts that no string value of its candidate appears in the report. A
refusal asserts the code, the exact findings, staging left as it was and no export. A
pass asserts staging gone, the output folder holding exactly the schema's files and
`receipt.json`, and the receipt's hashes recomputed from the files as they landed.

## Out of scope

- Level-1 mock data (metadata-only, generated from a draft `project_data.json`): epic
  #707.
- The disclosure policy values of any institution.
- Changes to the MONA extract runner.
- Carrying code into the data VM (the first stop of the education-returns flow).
- Differencing against earlier approved exports.
- Signing the receipt.

## Future direction

The agent may later run code on the data and see a strictly limited subset of error
messages, so it can iterate without a human relaying failures. Not in v1; the v1 agent
sees only the refusal category code. Any such subset needs its own design for what it
may reveal.
