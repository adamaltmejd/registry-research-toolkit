# The export gate

The release stage is the toolkit's export gate, not project code. Its canonical source
is the `reg_export_gate/` folder of the toolkit repository
(<https://github.com/adamaltmejd/registry-research-toolkit/tree/main/reg_export_gate>):
`export_gate.R`, the schemas and policies in `schemas/`, the MONA steps in `README.md`
and the design in `DESIGN.md`. There is one copy of the check, there; a project holds a
pinned copy of its bytes and never a variant.

**Pinned version: `EXPORT_GATE_VERSION <- "0.6.0"`.** Take `export_gate.R` from the
`reg_meta` tag the project's `reg_meta_version` names (0.6.0 is in `reg_meta/v0.87.3` and
later):
`https://raw.githubusercontent.com/adamaltmejd/registry-research-toolkit/reg_meta/vX.Y.Z/reg_export_gate/export_gate.R`.
Check the version line in the copy and record its SHA-256. In `reg_meta/v0.87.3` the file
is `51d6f810ef0c93faccb69f748382b6cdce93243af838788f9165e094a103869a`, the default
`aggregates` schema `8160f4cb2250b5d22e609bf0a58935a221bd61ec407dd0403e2ccdd0537f1f93`
and its policy (internal name `aggregates-min5`)
`e3c0835c7963efa39f035216087c08b7558ab5f3887c3ae7bbae2ed0f0098f5c`. A newer gate
version is a deliberate update of this pin, not something to pick up silently.

The R file imitates the Rust gate (`crates/reg-export-gate`), which is the reference and
can be ahead of it. A schema or policy that uses a field a gate predates is refused as
`setup` (`schema_invalid`, `policy_invalid`), so on MONA use only pairs the R version
accepts: 0.6.0 accepts everything the shipped schemas and policies use.

## What the project holds

- `export_gate.R`, byte for byte as the toolkit ships it. Never edit, wrap or
  concatenate it into project code: every receipt names its version.
- The schema and its policy, by file name in the toolkit's `reg_export_gate/schemas/`.
  Default: `schemas/aggregates.schema.json` (long-form counts, means, quantiles and
  regression coefficients by groups) with `schemas/aggregates.policy.json`, whose
  internal policy name is `aggregates-min5` (there is no `aggregates-min5.policy.json`);
  both unchanged from the toolkit.
  The toolkit's other pairs (`trial`, `mock-stats`) are for trials and local mock
  statistics, not a project's outputs. If the project needs a different policy, see
  "The disclosure policy" below.
- In the project instructions: the gate version, the schema and policy names and
  versions, and the three files' SHA-256, so a changed copy is visible.

Commit the three files with the project. Upload them to MONA unchanged (the toolkit
README's step 1).

## The disclosure policy

The gate only checks. The pipeline does the protecting: it suppresses, rounds and
perturbs before writing, and the gate refuses what still breaks the policy. The toolkit
ships cited defaults, and every disclosure parameter is configurable per project. The
default policy (`schemas/aggregates.policy.json`, named `aggregates-min5`) sets:

- a floor of 5, for every published count and every statistic's true support;
- cell-key perturbation of counts (D 5, V 6.5, js 4), applied by the pipeline; the gate
  is told it was done and cannot verify the noise, so it keeps the floor;
- at most 4 significant digits for every statistic (coefficients and standard errors
  included);
- a regression small-group check: a term describes 0 or at least 5 true persons, and
  leaves 0 or at least 5 beside it;
- no dominance check: a magnitude statistic over firms is the reviewer's to judge.

Each choice and its source is in `reg_export_gate/DESIGN.md`, "The shipped files".

**The disclosure stage.** The pipeline has one disclosure stage, between the analyses and
the staging writer, in this order:

1. **Suppress.** Set a statistic to `NA` where its true support is below the minimum:
   `n_true` below 5 for `statistics.parquet` and each quantile column's `min_support`
   (50, 20, 10, 20, 50) in `quantiles.parquet`. In `coefficients.parquet`, set
   `estimate` and `std_error` to `NA` where `n_true` is below 5, or where `k_true` or
   `n_true - k_true` is 1 to 4.
2. **Perturb.** Every published person count (`n_persons` in all four files) comes from
   `ck_perturb()` in [cell_key.R](../scripts/cell_key.R), from the persons behind the
   cell, never from a computed count. It returns `n_true` (true, check-only) and the
   perturbed `n_persons`, already `NA` below the floor. A share is one `counts.parquet`
   row per outcome category, each perturbed, never a ratio.
3. **Round** every `double` statistic to 4 significant digits (`signif(x, 4)`).

`n_true` and `k_true` stay true and unperturbed: the gate tests them and never exports
them.

Stage exactly the columns the schema declares for each file, nothing more:

- Under the default `aggregates` schema, `counts.parquet` has no `n_true`. Write
  `ck_perturb()`'s `n_persons` and drop its `n_true`, or the gate refuses the file as
  `structure` (`extra_column`). `statistics.parquet`, `quantiles.parquet` and
  `coefficients.parquet` declare `n_true`, so they keep it.
- A project schema that gives a count `"units": "n_true"` checks the true count too. Set
  `n_persons` to `NA` wherever `n_true` is below `min_cell_count` as well: otherwise a
  true 1 perturbed to 5 passes the published floor and is refused as `disclosure`
  (`count_units_too_small`). Such a count also needs `"perturbation": 5` (D): without
  it, the bound is 0 and any perturbed count is refused as
  `count_perturbation_exceeded`.

`cell_key.R` is the toolkit's helper, shipped with this skill. Copy it into the project
unchanged, as `export_gate/cell_key.R`, and record its `CELL_KEY_VERSION` and SHA-256 in
the project instructions beside the gate's. Its record keys are a secret key file that
stays on MONA, outside the project folder that is synced or exported, readable by the
researcher only. On every platform it lives in a folder only the researcher can read;
that folder is the guarantee. The helper also writes the file with mode 0600, a second
layer that a folder's inherited access rights (a default ACL, Windows permissions) can
override. The keys come from a cryptographic generator, the `openssl` R package, which
also checks the p-table's SHA-256: it is the helper's one package requirement on every R
version (install it on MONA from the configured repository; without it the helper
refuses as `cell_key_openssl_missing`). The researcher creates the key file once with
`ck_keys_create(<all person ids in the extract>, <path>)`; this is key epoch 1.
Persons it lacks are refused (`cell_key_missing_ids`); `ck_keys_extend()` adds keys for
them in the same epoch. A new epoch (`new_epoch = TRUE`) replaces every key, so a table
published under two epochs loses its protection. Make one only on the researcher's
explicit request. Every result carries its `key_epoch` (an attribute). The pipeline
writes it to the project's run record on MONA, never to staging or the export. Locally,
on mocks, use a key file made from the mock person ids, outside Git.

**A project sets its own policy** as a `<name>.policy.json` (format
`reg-export-gate/policy`, fields in `reg_export_gate/DESIGN.md`, "Policy format"),
paired with a schema it names, and passes it as `policy =` in the `export_gate()` call.
The policy must name the schema it is written for; a mismatch is refused
(`policy_schema_mismatch`). The receipt records the policy's name, version and SHA-256,
so the importing side sees which policy ran. The researcher decides the parameters,
against the data holder's disclosure rules. The agent drafts or edits a policy only when
the researcher asks, and never changes a parameter to make a failing run pass. A policy
or schema outside the toolkit's shipped files is the researcher's to review before real
release.

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

The gate is R even when the pipeline is not. With a Python/DuckDB stack,
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
not disclosure clearance. Do not add a test that the gate refuses a bad file: the gate's
own case corpus covers it.

## Importing an export

Before reading an export, check each file's SHA-256 against `receipt.json` (it hashes
the files the gate wrote), and the receipt's gate version and schema and policy hashes
against the project's pins. A mismatch is a rejected import. The receipt is unsigned: it
shows integrity, not that the gate ran.

## Other servers

A Linux x86_64 server that can run executables uses the `reg-export-gate` binary, an
asset of each `reg_meta/v*` release named by the gate's own version
(`reg-export-gate-<version>-x86_64-unknown-linux-gnu`; in `reg_meta/v0.87.3`,
`reg-export-gate-0.6.0-x86_64-unknown-linux-gnu`, SHA-256
`d1c4330b58d1bf99244ddf6c2abf1961a021f646dec45602cbec18e20233157f`). Its `export`
step refuses a schema with check-only columns, which the default `aggregates` schema
has, so that schema is delivered through MONA's own export. The pipeline's last step runs
`reg-export-gate check --project NAME --staging DIR`, with the schema, policy and folders
in the server's project config, outside the project. The reviewer, after inspecting the
checked output, runs `reg-export-gate export --project NAME --receipt <SHA-256 of the
reviewed receipt.json>` to deliver it one way to the agent's side. There is no review
UI; the setup is in `crates/reg-export-gate/DESIGN.md` in the toolkit repository. A server that cannot run it runs the same R file as the
last step, the same way, and the human review of the export stays as described in
[operator-review.md](operator-review.md).
