# Export gate on MONA

`export_gate.R` is the last step of an analysis pipeline on MONA. It checks the
pipeline's complete output against a fixed schema and disclosure policy and, only if
everything passes, moves the files and a `receipt.json` into an export folder. If
anything fails, the export folder stays empty and a report says which file, column and
rule failed, never a value. Rationale and formats: [DESIGN.md](DESIGN.md).

These steps are the first MONA trial, with the trial schema in `schemas/`: two files of
person counts (minimum 5, else suppressed) and a per-year mean.

## 1. Files to bring

From this folder of the toolkit:

```text
export_gate.R                  the check (version in its EXPORT_GATE_VERSION line)
schemas/trial.schema.json      which files and columns the export may hold
schemas/trial.policy.json      the disclosure rule: counts >= 5 or null
```

Upload all three unchanged. If MONA's upload accepts only `.R` and `.py`, upload the two
JSON files with `.R` appended (`trial.schema.json.R`) and remove the `.R` on MONA. Do
not paste their contents into a new file: the receipt records each file's SHA-256, and
pasting can change the bytes (line endings).

## 2. Folders on MONA

Put everything in one project folder, so staging and the export are on the same drive
(the gate moves the staging folder, it never copies):

```text
export_gate_trial\
  export_gate.R
  trial.schema.json
  trial.policy.json
  staging\          the pipeline writes its output here; the gate moves it away on a pass
  export\           appears only when the check passes: the files plus receipt.json
  .diagnostics\     the gate's report; stays on MONA, never exported
```

## 3. Preflight (one minute)

In R on MONA:

```r
R.version.string                              # 4.2 or newer
packageVersion("arrow"); packageVersion("jsonlite")
exists("sha256sum", asNamespace("tools")) ||  # TRUE on R >= 4.5
  requireNamespace("digest", quietly = TRUE)  # else digest must be installed
```

If the last line is `FALSE`, the gate cannot hash files and refuses every run with
`setup` (`sha256_missing`): stop the trial there and report it.

## 4. The pipeline's last step

Build the two trial files from any data `d` with one row per person and year and the
columns `year`, `group` (any grouping, such as sex or county) and `value` (any number,
such as income). Then run the gate:

```r
trial <- "\\\\micro.intra\\projekt\\<project>\\export_gate_trial"  # edit
source(file.path(trial, "export_gate.R"))
staging <- file.path(trial, "staging")

# A fresh staging folder: never check a mix of old and new output.
unlink(staging, recursive = TRUE)
dir.create(staging)

d$year <- as.integer(d$year)
d$group <- as.character(d$group)

# Persons per year and group; counts under 5 suppressed (NA, never 0).
persons <- aggregate(
  list(n_persons = rep(1L, nrow(d))),
  d[c("year", "group")],
  sum
)
persons$n_persons[persons$n_persons < 5L] <- NA

# Persons and the mean value per year; the mean is suppressed with its count.
by_year <- split(d$value, d$year)
summary <- data.frame(
  year = as.integer(names(by_year)),
  n_persons = unname(lengths(by_year)),
  mean_value = unname(vapply(by_year, mean, numeric(1), na.rm = TRUE))
)
small <- summary$n_persons < 5L
summary$n_persons[small] <- NA
summary$mean_value[small] <- NA

arrow::write_parquet(persons, file.path(staging, "persons_by_year_group.parquet"))
arrow::write_parquet(summary, file.path(staging, "summary_by_year.parquet"))

export_gate(
  staging,
  output = file.path(trial, "export"),
  schema = file.path(trial, "trial.schema.json"),
  policy = file.path(trial, "trial.policy.json")
)
```

Column types matter: R `integer` is the schema's `integer`, `numeric` its `double`,
`character` its `string`. A factor, or a count computed as `numeric`, is refused as
`structure` (`column_type`): convert with `as.character()` or `as.integer()`.

## 5. Success

The console shows

```text
export gate 0.1.0: pass, 2 files
  moved with receipt.json to \\micro.intra\...\export_gate_trial\export
```

`export\` holds exactly `persons_by_year_group.parquet`, `summary_by_year.parquet` and
`receipt.json`; `staging\` is gone (the next run creates it again). Review the two
files, zip the `export` folder and send it out through MONA's export as usual. Never
include `.diagnostics\` or `staging\`.

Outside MONA, check the files against the receipt: `shasum -a 256 *.parquet` (macOS,
Linux) or `Get-FileHash *.parquet` (Windows) must print the receipt's `sha256` values.

## 6. Refusal

The call stops with an error, for example

```text
export gate 0.1.0: refused (disclosure)
  min_cell_count file=persons_by_year_group.parquet column=n_persons
Error: export gate refused (disclosure); report in ...\.diagnostics\export_gate_report.json
```

There is no `export` folder (a previous export there was removed when the run started),
`staging\` is left as it was, and `.diagnostics\export_gate_report.json` lists the same
findings. Fix the pipeline and run it again from a fresh staging folder.

  | Code         | Meaning and what to do                                                                                                     |
  | ------------ | -------------------------------------------------------------------------------------------------------------------------- |
  | `setup`      | A path, package, schema or policy problem, or the move failed (keep staging and export in one folder); see the rule        |
  | `inventory`  | `staging` does not hold exactly the schema's files: one missing, an extra or hidden file, a folder, a stray `receipt.json` |
  | `structure`  | A file is not Parquet, or a column is missing, extra, out of order or of the wrong type                                    |
  | `disclosure` | A count under the minimum (suppress it as `NA`), or a statistic beside a suppressed count                                  |
  | `backstop`   | A text value or file metadata looks like a personnummer                                                                    |

If `export\` holds files the gate did not write (no `receipt.json`), the gate refuses
`setup` (`output_not_gate_export`) and leaves them: move them away yourself.

## 7. Trial drills

Each drill changes one thing in step 4, reruns it and must refuse with the code shown,
leaving no `export` folder:

1. `persons$n_persons[1] <- 3L` after the suppression line → `disclosure`.
2. `writeLines("note", file.path(staging, "notes.txt"))` → `inventory`.
3. `persons$group[1] <- "19121212-1212"` (Skatteverket's published test number) →
   `backstop`.
4. `summary$n_persons <- as.numeric(summary$n_persons)` → `structure`.

Then run step 4 unchanged once more: it passes, and the new export replaces the old.

## Report back

Note the R, arrow and jsonlite versions, which SHA-256 route the preflight found, how
long the gate took, and anything that refused unexpectedly (the rule and file, not the
data).
