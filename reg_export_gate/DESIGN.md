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
the SHA-256 of each file it writes and verifies it on delivery. A changed byte voids the
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
   `person_count` is at least `min_cell_count`, and a `statistic` is null wherever its
   `support` is null. Null is the suppression marker, so a zero count is refused like
   any other count below the minimum. No `double` column holds NaN, whatever its role:
   NaN is not null, and its payload bits (about 51) survive Parquet and the rewrite, so
   a cell that looks suppressed could carry a value. A suppressed statistic is a true
   null. The rule covers every double rather than only statistics beside a suppressed
   count, because the channel is the same in a dimension or an unsuppressed row, and an
   aggregate has no legitimate NaN to lose. Null-ness comes from Arrow's validity, which
   is what the rewrite writes, not from R's conversion: R reads some valid Arrow values
   as `NA` (a NaN whose low 32-bit word is 1954, which is R's `NA_real_` pattern with
   room for 21 free payload bits; `int32` `INT_MIN`; the `int64` NA sentinel). A column
   whose R `NA`s are not exactly its Arrow nulls is refused (`na_not_null`), so every
   rule sees the same nulls the export holds, and a cell that passes as suppressed is
   written as a true null. It is a disclosure rule, beside `nan_value`, because the harm
   is the same: a value hidden in a cell that looks suppressed.
4. **Backstop scan.** Every value is scanned for personnummer and samordningsnummer
   shapes (10 or 12 digits, optional `-` or `+`, a valid month and day or day + 60, not
   inside a longer digit run): strings as written, integers in full, dates as their day
   number, and doubles by their whole-number part (an identifier stored as a number is
   whole; the digits of a fraction would match by chance). This is a backstop only: a
   pattern scan cannot establish safety (#20), and a misclassified column can pass a
   count threshold (#17). Organisationsnummer share the shape and also match. Key-value
   metadata is not scanned: the gate never exports it (below).

Setup stops at its first finding; each of the four steps collects every finding over all
files, and the check stops at the first step with a finding. On any refusal the output
folder holds nothing. The output folder is data-side: its files are checked but not yet
approved, and only the human's Export pushes them to the agent VM. Refusals have stable
codes by category: setup, inventory, structure, disclosure, backstop, and delivery
integrity (a hash mismatch; binary only). Reports, logs and diagnostics stay on the data
side and never enter the output folder.

A finding names the rule and locates it, never by a value, as in the MONA extract runner
([../reg_mona_runner/DESIGN.md](../reg_mona_runner/DESIGN.md), "No row values in logs").
A file or column the schema declares is named, since the name comes from the fixed
schema. A file or column it does not declare (an extra or hidden file, a folder, an
extra column) is located by its 1-based `position` instead: its name is the author's and
can itself be a value (a wide pivot makes values into column names). A staging position
counts the folder's entries sorted by bytes, not by the locale; a column position counts
the file's columns as written.

### The export is a rewrite

The gate never exports the author's bytes. Only when every file passes does it write
each checked table afresh, from the decoded data: new arrays of one Arrow type per
schema type (`integer` → `int64`, `double` → `double`, `string` → `string`, `boolean` →
`bool`, `date` → `date32[day]`), in the schema's column order, with no schema or field
key-value metadata. It writes these files and `receipt.json` into a new folder beside
the output folder (same volume), then renames that folder to the output folder, so the
files and the receipt appear together. Staging is left as it was and is never exported.

So whatever a Parquet file can carry beside its values never leaves: bytes between
column chunks and the footer that readers skip, file and column metadata (free text, R
attributes), dictionary, page and row-group layout. A file changed or added in staging
after the check is not exported either, since only what the gate read and checked is
written. Links in staging are refused where R can see them (`Sys.readlink()` does not on
Windows); the rewrite is what guarantees that only checked data leaves.

Only checked data leaving is not only aggregates leaving. A double that is not
suppressed carries 64 bits the check does not constrain, and row order and the values of
a string dimension are open channels too. These are inherent to a threshold-only policy
and outside what the gate checks; the human review covers them.

Every write option that affects bytes is pinned (no compression, no dictionary, no
statistics, Parquet 2.4, one row group per 1,048,576 rows), so the same data writes the
same bytes under one arrow release, whatever file it came from. Two entries remain that
arrow always writes and the gate cannot turn off: `ARROW:schema`, the Arrow schema the
gate built, and the `created_by` string naming the arrow version. Neither holds anything
of the author's; the version string means byte identity holds per arrow release, not
across releases.

A failed check tells the agent only the refusal category code. Each export is checked
alone in v1; differencing against earlier approved exports comes later. The format-1
policy has a single threshold rule: no secondary suppression, dominance rule or check
across files (a suppressed cell recoverable from a margin in another file passes). Those
are policy work for later versions; until then the human review covers them.

### Refusal codes and rules

The code is the category; the rule locates the finding. Both are stable.

- `setup`: `staging_missing`, `output_parent_missing`, `paths_overlap`,
  `jsonlite_missing`, `arrow_missing`, `arrow_too_old`, `sha256_missing`,
  `output_not_gate_export`, `output_not_removable`, `schema_unreadable`,
  `schema_invalid`, `schema_version`, `schema_reserved_name`, `policy_missing`,
  `policy_unreadable`, `policy_invalid`, `policy_version`, `policy_schema_mismatch`,
  `export_not_writable`, `move_failed`, `internal_warning`,
  `internal_error:<R condition class>`.
- `inventory`: `missing_file`, `extra_file`, `hidden_file`, `not_a_file`,
  `reserved_name`.
- `structure`: `unreadable`, `missing_column`, `extra_column`, `column_order`,
  `column_type`.
- `disclosure`: `nan_value`, `na_not_null`, `min_cell_count`, `unsupported_statistic`.
- `backstop`: `identifier_pattern`.

`schema_version` and `policy_version` mean a `format_version` this gate does not read.
`paths_overlap` means staging and output are the same folder or one is inside the other;
an existing output is resolved on disk first, so another spelling of staging (case,
Unicode form, a symlink, a Windows short name) cannot pass as a different folder and be
cleared. A Windows junction or a mapped drive versus its UNC path may still pass; then
`clear_output()` can delete only files a gate receipt lists, in a folder holding nothing
else. Before any check the gate removes its previous export from the output folder, so a
refused run never leaves an old export looking current. A previous export is an output
folder holding a `receipt.json` whose `format` is `reg-export-gate/receipt` and, beside
it, only files that receipt lists (no folders); the gate deletes exactly those files and
then the empty folder. An empty folder is removed too. Anything else is
`output_not_gate_export`, with nothing deleted: the gate leaves it for the user.
`export_not_writable` means the gate could not create its new folder beside the output
folder; `move_failed` that it could not rename that folder to the output folder (on
Windows, an Explorer window or another program holding the folder open blocks it).

### Receipt

The receipt is exported with the data. It holds only the gate version, the schema's and
policy's name, version and hash, and the SHA-256 of each file the gate wrote: no values,
counts or messages. Anyone downstream (the agent, a co-author, a replication) can check
the files against it with any SHA-256 tool. That is integrity, not authenticity: the
receipt is unsigned, so it shows the files match what the gate wrote, not that the gate
ran. The author's staging files do not match it: they are not what was exported. The
gate's delivery record covers `receipt.json` too, so a changed receipt fails delivery
like a changed data file. Its format is fixed by the toolkit, not by the schema:

```json
{
  "format": "reg-export-gate/receipt",
  "format_version": 1,
  "gate": { "implementation": "R", "version": "0.2.0" },
  "schema": { "name": "trial", "version": 1, "sha256": "…" },
  "policy": { "name": "trial-min5", "version": 1, "sha256": "…" },
  "files": [{ "name": "persons_by_year_group.parquet", "sha256": "…" }]
}
```

Files are listed in schema order; there is no timestamp, so a rerun over the same data
under the same arrow release writes the same receipt. Hashes are of the file bytes the
gate wrote; the schema and policy hashes are of the JSON files as read.

Reports, logs and diagnostics go in a `.diagnostics/` folder beside the output folder
and are never exported. The MONA check writes one,
`.diagnostics/export_gate_report.json`: the gate version, `result` (`pass` or
`refused`), `code` and `findings` (`file`, `column`, `position`, `rule`, each but `rule`
null where it does not apply).

## Output format

One Parquet file per schema file, readable from Rust, R and Python. Outputs are Parquet
only. On MONA they are written with the `arrow` R package. The `duckdb` R package is
also installed there, but DuckDB is a reader only: no `.duckdb` files are outputs.
Column types come from the schema; the gate writes each as one Arrow type ("The export
is a rewrite"). A suppressed cell is null, never zero or NaN.

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

- **Base R (>= 4.2), jsonlite and arrow (>= 9.0.0).** SHA-256 comes from
  `tools::sha256sum` (R >= 4.5) or else the `digest` package. Both hash the bytes in
  memory.
- **It installs its own packages.** MONA has its own CRAN mirror and library path in R's
  options, and a batch server can lack the packages the user has elsewhere. So
  `export_gate()` first installs whichever of jsonlite, arrow (missing or below 9.0.0)
  and, without `tools::sha256sum`, digest is missing, with plain
  `install.packages(pkg)`: R's default library (`.libPaths()[1]`) and the configured
  repository. It never adds a library path (a project folder is likely NFS and does not
  belong on it), and installs only when the default library is writable and a CRAN
  mirror is set: otherwise `install.packages` would prompt for a personal library (which
  it prepends to `.libPaths()`) or a mirror, pausing the gate on a dialog. Without both,
  the researcher installs the package. Then it checks again, and a package still missing
  or too old refuses `setup` with the rule that names it (`jsonlite_missing`,
  `arrow_missing`, `arrow_too_old`, `sha256_missing`), before reading or deleting
  anything. No new rule: after an attempted install, these rules already mean "could not
  be installed", and a generic rule would lose the package's name, which a finding has
  no other slot for. Install warnings and errors are caught, so a failed install reports
  that rule rather than `internal_warning` or `internal_error`. A namespace already
  loaded below the floor cannot be replaced in the session (on Windows the DLL copy
  fails), so the gate does not try and refuses `arrow_too_old` at once. The researcher
  restarts R, runs `install.packages("arrow")` before anything loads arrow, and reruns
  the pipeline. The install runs in the call, after the path checks and before the prior
  export is cleared, not when the file is sourced: sourcing stays free of side effects,
  and a mistyped path refuses at once without a slow install. The extract runner
  (`reg_mona_runner/swecov_extract.R`) carries the same helper, since each file is
  uploaded to MONA alone.
- **Two names.** Sourcing defines `EXPORT_GATE_VERSION` and `export_gate()`; the helpers
  live in the function's closure, so the file does not clutter the pipeline's
  environment.
- **`export_gate(staging, output, schema, policy)`** returns `"pass"` invisibly, or
  signals an error of class `export_gate_refusal` with `$code`, so a refused gate fails
  the pipeline. The console shows the code and the findings (rules, files, columns). The
  `.diagnostics/` folder is `dirname(output)/.diagnostics`.
- **Each file is read once,** into memory, and parsed from that raw vector, so no file
  handle stays open in staging.
- **The export is written, then renamed.** The checked tables and the receipt go into a
  folder `.export-gate-<random>` (hidden on Unix only) the gate creates beside `output`,
  which one `file.rename()` turns into `output`; so only the output folder's parent must
  be writable, and the new folder is on its volume. On any failure the gate removes its
  own new folder whole. Staging is never moved or changed.
- **No values in output.** Rules, schema file and column names and positions only. An
  unexpected R error is reported by its condition class, and an R warning refuses
  (`internal_warning`) rather than printing, since R and Arrow messages can quote a
  value.

## Case corpus

`tests/cases/<case>/case.json`, one behavior per case, run by `tests/testthat/` and
later by the Rust binary's tests. It is data: every expectation is in the JSON.

- `description`: the behavior, and so the regression that breaks it.
- `schema`, `policy`: paths relative to the case folder (the shipped files are
  `../../../schemas/trial.*.json`; a setup case carries its own beside `case.json`), or
  null for "not given".
- `candidate`: the staging folder, by file name. A Parquet file is
  `{"columns": [{"name", "type", "values", "metadata"}], "metadata": {...}, "inner_bytes": "..."}`
  with Arrow types `int32`, `int64`, `double`, `string`, `bool`, JSON `null` for a null
  cell and `"NaN"` for NaN in a `double` column. In a `double` column, `"0x"` and 16 hex
  digits is the value with exactly those IEEE 754 bits (high byte first), except
  `0x8000000000000000` (-0), which is int64's NA and becomes a null. The optional
  `metadata` are the column's and the file's key-value metadata; `inner_bytes` is text
  spliced in just before the footer, where readers skip it. Other entries are
  `{"text": "..."}` (raw bytes) or `{"folder": true}`.
- `output_before`: the output folder before the run, in the same form (optional).
- `expect`: `{"result": "pass"}`, or
  `{"result": "refused", "code", "findings": [{"file", "column", "position", "rule"}]}`
  (an absent key is null) with `"output": "unchanged"` when the output folder is not the
  gate's to clear.

Every case asserts that the report holds no string value, no metadata or inner bytes,
and no file or column name the schema does not declare, and that staging is left as it
was. A refusal asserts the code, the exact findings and no export. A pass asserts the
output folder holding exactly the schema's files and `receipt.json`, no key-value
metadata and none of the candidate's metadata or inner bytes in the exported files, the
receipt's hashes recomputed from the files as they landed, and a second run writing the
same receipt.

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
