# Extract runner

`extract.R` extracts a steward's orders inside MONA. Each order is an order file
(version 4) that the steward's catalog wrote for one sub-project; the script writes the
ordered columns into that sub-project's export folder, in the format the researcher
chose for the project: Parquet, Stata `.dta` or CSV. Run it by hand in
MONA. Use the copy at the `reg_meta` tag the steward pins:
`https://raw.githubusercontent.com/adamaltmejd/registry-research-toolkit/reg_meta/vX.Y.Z/reg_mona_runner/extract.R`.

## Steward configuration

Everything steward-specific is one JSON file the steward repository keeps (SWECOV's is
`mona/extract-config.json` in swecov-steward), read from `extract-config.json` in R's
working directory or from the path in `REG_EXTRACT_CONFIG`:

```json
{
  "steward": "<the steward id, as in the order's provenance.steward>",
  "base": "<the export root>",
  "odbc": { "dsn": "<DSN>", "Driver": "ODBC Driver 17 for SQL Server", "Server": "<server>", "Database": "<database>" }
}
```

`odbc` holds the `DBI::dbConnect(odbc::odbc(), ...)` arguments, all strings; the script
adds `encoding = "UTF-8"` and `bigint = "character"`. A missing or malformed file stops
the run with exit code 2 before any order.

## Folders

All under the export root:

```text
<base>\
  orders\                  orders waiting to run, named <project number>.json
  orders\archive\          orders that ran, as <project number>_<date>.json
  export_data\             one folder per finished order, <project number>_<date>\, and
                           <project number>.partial\ for one that has not finished
```

## Run an order

1. The coordinator writes the order file from the reviewed `project_data.json` with the
   steward server's `POST /api/project/materialize` (or the MCP `materialize_order`
   tool). The runner trusts the file fully: no allowlist, no hash check. The steward
   repository documents its own order workflow (SWECOV: `mona/ORDERS.md`).

2. Upload the order file to MONA together with `extract.R` and `extract-config.json`,
   and save it as `orders\<project number>.json` (for example `orders\1234.json`).

3. In R on MONA, with the working directory holding `extract-config.json`,
   `source("extract.R")`, or run `Rscript extract.R`. Every order in `orders\` runs, one
   after the other.

4. Read the console (or the batch run's `.Rout`). Each order prints a header line,
   then a count block: the rows its files will hold, its rough size in the order's
   format, its three largest files, the columns not in the size, any count that failed
   and how long counting took. Then one line per finished file, with rows, size on
   disk, elapsed time and the time left; a file running longer than five minutes gets
   a progress line every five minutes. Each order ends with `done` or
   `FAILED (exit code N)`. A failure is followed by an error block with the full R
   or database message, which can quote row values: keep it, and screenshots of it,
   on MONA. A date-time finer than the format holds (Parquet: a microsecond; `.dta`: a
   millisecond) is cut, never rounded, and the console names each such column with the
   number of values cut; CSV keeps every digit.

   The count runs one count query per file, which reads the whole table (or view) under
   the order's filter: it is faster than the extract but adds to the run on a large
   view. It is information only: a count that fails is printed and the file is
   extracted anyway. The size is rough: it takes text columns at their declared maximum
   length, ignores Parquet's compression, and leaves out long text columns, which it
   names.

The script first installs any of its R packages (DBI, odbc, jsonlite) that is missing,
and haven (for `.dta`) or arrow (for `.parquet`) when an order needs it, into R's
default library from MONA's mirror, so the first run on a new server takes longer. It
installs only when the default library is writable and a CRAN mirror is configured. If
DBI, odbc or jsonlite cannot be installed, the run stops with exit code 2 before any
order, and the console names the package; if haven or arrow cannot, only the orders that
need it fail, with exit code 2. Install it yourself, or ask MONA support (unwritable
library, no or unreachable mirror).

## Results

A successful order writes `export_data\<nr>_<date>\`:

- one file per delivered table in the order's format, named after the database table,
  such as `sos_sv.dta` (or `.parquet`, `.csv`); a register delivered as one table per
  year, such as LISA, gives one file per year. A table that holds several years is one
  file, holding only the ordered years' rows (`run_log.csv` shows the filter). A `.dta`
  file is Stata 14 format (UTF-8); a CSV is UTF-8 with a header row and an empty field
  for a missing value. A `.dta` column whose name is not a Stata variable name (for
  example one over 32 characters) is written under its alias, the order's `stata_name`
  (a Stata-safe form of its first 25 characters, `_` and 6 hex digits), with the full
  name as the variable label. [DESIGN.md](DESIGN.md), "Types per format", lists what
  each column type becomes;
- `run_log.csv`: each file's row count, the count made before extracting
  (`rows_counted`, empty if that count failed), column count, the period filter applied
  and, for
  a filtered file, how many rows it left out because their period value is missing or
  unreadable (`rows_without_period`), and the file's size on disk (`bytes_written`).

The order then moves to `orders\archive\`.

An order is written into `export_data\<nr>.partial\`, each file under a `.partial` name
until it is complete, and the folder takes its dated name only when the order succeeds.
A failed order stays in `orders\`; the console shows the exit code and the reason, and
`<nr>.partial\` keeps the files that finished; the last line says what the next run
resumes, or what to do by hand. A run that was stopped leaves the same
folder. Fix the cause and run the script again: it resumes, keeping each finished file
whose order entry is unchanged and whose table still has the same number of rows, and
extracts the rest. If `export_data\<nr>_<date>\` already exists (the same order finished
earlier that day), the run refuses before touching anything: move or delete that
folder, then run again.
Run one `extract.R` at a time per order: two runs of the same order would share its
`.partial` folder.

  | Code    | What to do                                                                                                                                                                                                                                                                                                                                  |
  | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | 2       | Check the steward configuration, the export root, the R packages (DBI, odbc, jsonlite, haven, arrow) and the database                                                                                                                                                                                                                       |
  | 3, 4, 6 | The file is not a usable order: write it again with `materialize`                                                                                                                                                                                                                                                                           |
  | 5       | Not an order from this steward's holdings: rebuild the project in its view                                                                                                                                                                                                                                                                  |
  | 7       | A table or column is not in the database, or is stored otherwise than the order expects: report it to the catalog team. Or the format cannot hold a column (its type, or a `.dta` name that is not a Stata name even as an alias, or a full name over 80 characters, which a label cannot hold): the researcher orders it as Parquet or CSV |
  | 8       | The query or the write failed: check disk space, then report it. Or a value does not fit `.dta` exactly (the log names the column): the researcher orders it as Parquet or CSV                                                                                                                                                              |

## Safety

Everything the script writes stays on MONA. The extract files are microdata; only
approved aggregates leave MONA. `run_log.csv` has file names, counts and the order's
own fields. The console can hold row values too: a failure prints the full R or
database message, which can quote a value, between marker lines that say so. Don't copy
or screenshot that block off MONA.

## Tests

The automated suite (testthat, run by hand) covers only the refusals and validation that
fire before the database, and needs no database. Extraction itself is verified by a
manual run on MONA with real code.
