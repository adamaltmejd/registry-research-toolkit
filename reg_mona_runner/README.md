# Extract runner

`extract.R` extracts a steward's orders inside MONA. Each order is an order file
(version 6) that the steward's catalog wrote for one sub-project; the script writes the
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
  "odbc": { "dsn": "<DSN>" },
  "fetch_rows": 1000000
}
```

`odbc` holds the `DBI::dbConnect(odbc::odbc(), ...)` arguments, all strings; the script
adds `encoding = "UTF-8"` and `bigint = "character"`. Name either a DSN, as above, or a
driver and server, as in `{ "Driver": "<driver>", "Server": "<server>", "Database":
"<database>", "Trusted_Connection": "Yes" }`, never both: a DSN takes precedence, and a
`Driver` beside it is ignored. The driver must be installed where the runner runs, which
on MONA can be another machine than the interactive session (`odbc::odbcListDrivers()`
under the same `Rscript` or `R CMD BATCH` shows which). A missing or malformed file stops
the run with exit code 2 before any order.

`fetch_rows`, optional, is how many rows the runner fetches at a time (100,000 when
absent); each fetch is one Parquet row group, split at arrow's own cap (1,048,576 rows
in current versions) and wherever a text column would pass 1 GB. Larger row groups
compress better and read faster. Larger fetches cost memory: the runner holds a whole
fetch in R and, while writing it, its converted copy, so peak memory grows with
`fetch_rows` times the width of the widest file. Size it for the machine the runner
runs on (on MONA, a batch server has far more memory than an interactive session).

## Folders

All under the export root:

```text
<base>\
  orders\                  orders waiting to run, named <project>.json
  orders\archive\          orders that ran, as <project>_<date>.json
  export_data\             one folder per finished order, <project>_<date>\, and
                           <project>.partial\ for one that has not finished
```

`<project>` is the MONA project name, as MONA writes it: `P1234`, never the bare number.

## Run an order

1. The coordinator writes the order file from the reviewed `project_data.json` with the
   steward server's `POST /api/project/materialize` (or the MCP `materialize_order`
   tool). The runner trusts the file fully: no allowlist, no hash check. The steward
   repository documents its own order workflow (SWECOV: `mona/ORDERS.md`).

2. Upload the order file to MONA together with `extract.R` and `extract-config.json`,
   and save it as `orders\<project>.json`, named by the MONA project name as MONA
   writes it (for example `orders\P1234.json`).

3. In R on MONA, with the working directory holding `extract-config.json`,
   `source("extract.R")`, or run `Rscript extract.R`. Every order in `orders\` runs, one
   after the other.

4. Read the console (or the batch run's `.Rout`). Each order prints a header line,
   then a count block: the rows its files will hold, its rough size in the order's
   format, its three largest files, the columns not in the size, any count that failed
   and how long counting took. Then one line per finished file, with rows, size on
   disk, elapsed time and the time left; a file running longer than five minutes gets
   a progress line every five minutes. Each order ends with a `done … exit 0` line, or
   with `FAILED (exit code N): reason` in the failing file's row, an error block with
   the full R or database message, what the next run does, and a `stopped … exit N`
   line. The error block can quote row values: keep it, and screenshots of it, on
   MONA. The run's last line is `finished … exit N`, the exit code a batch job ends
   with (0 when every order finished, else the first failed order's code). A date-time finer than the format holds (Parquet: a microsecond; `.dta`: a
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

A successful order writes `export_data\<project>_<date>\`:

- one file per delivered table in the order's format, named after the database table,
  such as `sos_sv.dta` (or `.parquet`, `.csv`); a register delivered as one table per
  year, such as LISA, gives one file per year. A table that holds several years is one
  file, holding only the ordered years' rows (`run_log.csv` shows the filter). A `.dta`
  file is Stata 14 format (UTF-8); a CSV is UTF-8 with a header row and an empty field
  for a missing value. A `.dta` column whose name is not a Stata variable name (for
  example one over 32 characters) is written under its alias, the order's `stata_name`
  (a Stata-safe form of its first 25 characters, `_` and 6 hex digits), with the full
  name as the variable label. An SCB löpnummer (a pseudonymised person or firm key the
  order marks) is written as a 64-bit integer whatever its storage, so keys join across
  files. [DESIGN.md](DESIGN.md), "Types per format", lists what each column type becomes;
- `run_log.csv`: each file's row count, the count made before extracting
  (`rows_counted`, empty if that count failed), column count, the period filter applied
  and, for
  a filtered file, how many rows it left out because their period value is missing or
  unreadable (`rows_without_period`), and the file's size on disk (`bytes_written`).

The order then moves to `orders\archive\`.

An order is written into `export_data\<project>.partial\`, each file under a `.partial` name
until it is complete, and the folder takes its dated name only when the order succeeds.
A failed order stays in `orders\`; the console shows the exit code and the reason, and
`<project>.partial\` keeps the files that finished; the line after the error block says
what the next run resumes, or what to do by hand. A run that was stopped leaves the same
folder. Fix the cause and run the script again: it resumes, keeping each finished file
whose order entry is unchanged and whose table still has the same number of rows, and
extracts the rest. If `export_data\<project>_<date>\` already exists (the same order finished
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
  | 8       | The query or the write failed: check disk space, then report it. Or a value does not fit `.dta` exactly (the log names the column): the researcher orders it as Parquet or CSV. Or an SCB löpnummer column holds a value that is not a whole number (the log names the column and the count): report it to the catalog team                                                                                                                                                             |

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

## Key check

`key_check.R` checks, before a steward marks its SCB löpnummer columns (its `[[lopnr]]`
entries) for int64 delivery, that every such key converts, and shows what the other
key-like columns (holders' own serials, `…id` columns) look like. It counts only; no
value leaves the database.

1. Edit the settings at the top of the script: `SCB_KEY_NAMES` and `SCB_KEY_PREFIXES`
   as the steward's `[[lopnr]]` entries name them (exact names and name prefixes, both
   case-insensitive, for example `P1234_LopNr_`), `OTHER_KEY_PATTERNS` for the other
   key-like names, and optionally `SCHEMAS`, `TABLE_PATTERN` and `TABLE_TYPES`.
2. Upload it to MONA and run it beside the steward's `extract-config.json` (only its
   `odbc` connection is used; the connection's database is the one checked):
   `Rscript key_check.R`, `R CMD BATCH key_check.R`, or `source("key_check.R")`. It
   installs DBI, odbc and jsonlite when missing, as `extract.R` does.
3. Each table is read once, in one `SELECT` for all its matched columns; on a large
   table that is a full scan. The console prints one line per table (rows, columns,
   time, and `check:` naming the SCB key columns `extract.R` would not write as
   distinct int64 keys). A table whose query fails prints `FAILED (SQLSTATE …)` and the
   driver's message in an error block that stays on MONA; the run goes on with the
   next table. The last line is `finished … exit N`: 0, 8 when any table failed, 2 for
   a setup failure (packages, configuration, connection, the column listing).

It writes `key_check.csv` (in R's working directory) after every table: a few `#`
comment lines, then one row per column with `schema`, `table`, `column`, `group`
(`scb_key` or `other_key`), `sql_type`, `status` (`ok` or `failed: SQLSTATE …`) and:

  | Count           | Storage            | Counts the non-null values that …                                     |
  | --------------- | ------------------ | ------------------------------------------------------------------------ |
  | `rows`, `nulls` | any                | (the table's rows; the column's NULLs)                                   |
  | `not_bigint`    | numbers, text      | `TRY_CAST(… AS bigint)` cannot convert                                   |
  | `not_whole`     | decimal, float     | have a fraction                                                          |
  | `beyond_exact`  | float, real        | are at or beyond 2^53 (real: 2^24), where distinct keys may have merged  |
  | `leading_zeros` | text               | start with `0` and another digit (`012` and `12` would become one key)   |
  | `spaces`        | text               | have a leading space, or a trailing one in `varchar`/`nvarchar`          |
  | `not_digits`    | text               | are empty or hold anything but ASCII digits once spaces are trimmed      |

and `min_length`, `max_length`: the characters of a text value (trailing spaces
excluded) or the digits of a number. A count that does not apply to the storage is
empty.

An SCB key converts under `extract.R` when `not_bigint`, `not_whole`, `beyond_exact`,
`spaces` and `not_digits` are all 0 and the storage is not `real` (a negative key also
counts as `not_digits`, though `extract.R` writes it); `leading_zeros` above 0 means
two keys would merge.

**Safe to export.** The CSV holds names, SQL types and aggregate counts only: any count
from 1 to 4 is written as `<5`, and a length is left out when fewer than 5 values are
non-null. There is no value, and no minimum or maximum of values. The console's error
block, like `extract.R`'s, may quote a value and stays on MONA.
