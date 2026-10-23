# Export gate on MONA

`export_gate.R` is the last step of an analysis pipeline on MONA. It checks the
pipeline's complete output against a fixed schema and disclosure policy and, only if
everything passes, writes the checked data afresh, with a `receipt.json`, into an export
folder. It never exports the pipeline's own files: only the values it checked leave. If
anything fails, the export folder stays empty and a report says which rule failed and
where, never a value. Rationale and formats: [DESIGN.md](DESIGN.md).

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

Put everything in one project folder:

```text
export_gate_trial\
  export_gate.R
  trial.schema.json
  trial.policy.json
  staging\          the pipeline writes its output here; the gate only reads it
  export\           appears only when the check passes: the gate's files plus receipt.json
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
# A mean over no values is NaN in R; the gate refuses NaN, so make it NA.
summary$mean_value[is.nan(summary$mean_value)] <- NA

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
  written with receipt.json to \\micro.intra\...\export_gate_trial\export
```

`export\` holds exactly `persons_by_year_group.parquet`, `summary_by_year.parquet` and
`receipt.json`. The two files are the gate's own: the checked values only, written
afresh, with no metadata or anything else the pipeline's files carried. `staging\` is
left as it was. Review the two files in `export\`, zip the `export` folder and send it
out through MONA's export as usual. Never include `.diagnostics\` or `staging\`.

Outside MONA, check the files against the receipt: `shasum -a 256 *.parquet` (macOS,
Linux) or `Get-FileHash *.parquet` (Windows) in the exported folder must print the
receipt's `sha256` values. The files in `staging\` will not match: they are not what was
exported.

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

A finding names a file or column of the schema. A file or column the schema does not
declare is never named, since its name could be data; it is shown by position instead:
`extra_file position=1` is the first entry of `staging\` sorted by name (capitals before
lowercase), `extra_column file=summary_by_year.parquet position=3` the file's third
column.

  | Code         | Meaning and what to do                                                                                                     |
  | ------------ | -------------------------------------------------------------------------------------------------------------------------- |
  | `setup`      | A path, package, schema or policy problem, or writing the export failed; see the rule                                      |
  | `inventory`  | `staging` does not hold exactly the schema's files: one missing, an extra or hidden file, a folder, a stray `receipt.json` |
  | `structure`  | A file is not Parquet, or a column is missing, extra, out of order or of the wrong type                                    |
  | `disclosure` | A count under the minimum (suppress it as `NA`), a statistic beside a suppressed count, or a NaN (use `NA`)                |
  | `backstop`   | A value, text or number, looks like a personnummer                                                                         |

If `export\` holds anything but the gate's last export (its `receipt.json` and the files
that receipt lists), the gate refuses `setup` (`output_not_gate_export`) and deletes
nothing: move the other files away yourself. Windows can add a hidden `desktop.ini` or
`Thumbs.db` to a folder opened in Explorer; that counts too.

`move_failed` (or `output_not_removable`) on Windows usually means a program holds the
folder open: close any Explorer window showing `export\` or the project folder, and any
file open from `export\`, then rerun. A failed run removes the hidden `.export-gate-...`
folder it was writing; if one is ever left beside `export\`, delete it.

## 7. Trial drills

Each drill changes one thing in step 4, reruns it and must refuse with the code shown,
leaving no `export` folder:

1. `persons$n_persons[1] <- 3L` after the suppression line → `disclosure`.
2. `writeLines("note", file.path(staging, "notes.txt"))` → `inventory`.
3. `persons$group[1] <- "19121212-1212"` (Skatteverket's published test number) →
   `backstop`.
4. `summary$n_persons <- as.numeric(summary$n_persons)` → `structure`.
5. `summary$mean_value[1] <- NaN` after the suppression lines → `disclosure`.

Then run step 4 unchanged once more: it passes, and the new export replaces the old.

## Report back

Note the R, arrow and jsonlite versions, which SHA-256 route the preflight found, how
long the gate took, whether two passing runs over the same staging wrote the same
receipt, any `.export-gate-...` folder left beside `export\`, and anything that refused
unexpectedly (the rule and file, not the data).
