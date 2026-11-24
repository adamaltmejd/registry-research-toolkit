# Design: the MONA extract runner (`reg_mona_runner`)

`extract.R` turns order files into extract files inside MONA. It reads the MONA order
file (version 4) that `materialize_order` writes, whose contract is
[../crates/DESIGN.md](../crates/DESIGN.md) ("MONA order file") and
`crates/reg-core/src/manifest.rs`. A steward's data manager runs it by hand; no agent
and no network reach MONA, so the script is one self-contained file that gets uploaded
with the orders. Run steps are in [README.md](README.md).

## Shape

- **One file, base R plus DBI, odbc and jsonlite, and a writer per format.** R is >=
  4.2, so the native encoding is UTF-8. jsonlite parses the manifest; a hand-written
  JSON parser would be more code to trust than the dependency. haven writes `.dta` and
  arrow writes `.parquet`: both formats are binary with type metadata, which no
  hand-written writer should reproduce.
- **It installs its own packages.** A batch server can lack them, and MONA's CRAN mirror
  and library path are already in R's options. So the run starts by installing whichever
  of DBI, odbc and jsonlite is missing, with plain `install.packages(pkg)` into R's
  default library (`.libPaths()[1]`); it never adds a library path, and installs only
  when that library is writable and a CRAN mirror is set (otherwise `install.packages`
  would prompt for a personal library or a mirror). One still missing after that ends
  the run with exit code 2 and a console line naming the package, before any order is
  read, since no order ran. The install
  runs when the script runs, which for this script is when it is sourced: sourcing it is
  running it. haven and arrow are installed the same way, but per order and only for the
  format that order needs, so a MONA without arrow still runs `.dta` and `.csv` orders;
  one still missing fails that order with exit code 2. The export gate carries the same
  helper (`reg_export_gate/DESIGN.md`, "The R file"), since each file is uploaded alone.
- **One generic script, one steward file.** The steward id, the export root and the ODBC
  connection arguments are a closed JSON file the steward repository keeps
  (`extract-config.json`, or the path in `REG_EXTRACT_CONFIG`), read after the package
  install, so every steward runs the released `extract.R` unchanged (SWECOV move, R11).
  A missing or malformed file is exit code 2 before any order. The tests write their own
  file; there is no other override.
- **No dry run** (maintainer, 2026-10-21). A rerun of an unfinished order resumes it
  ("Resuming"), so a run is cheap to repeat. An order whose dated export folder
  already exists is refused before anything is touched: the coordinator moves or
  deletes that folder by hand (same-day reruns are rare).
- **Source or Rscript.** Under `source()` in an interactive session the script reports
  its exit code instead of quitting, since quitting would end the data manager's
  session. Under `Rscript` it exits with the code.

## The run

Every `<base>/orders/*.json` runs independently, in name order; the file stem is the
project number. An order writes into `<base>/export_data/<nr>.partial/` and, once
complete, renames it to `<nr>_<YYYY-MM-DD>/`, which must not exist yet:

1. Parse and check the whole manifest before touching the database: version, then mode
   and steward, then shape. The manifest, its provenance and every object the runner
   reads (files, columns, storage, row filters) are closed, so an unknown field there is
   a refusal. What the runner does not read (clips, findings, a column's `role`, `refs`
   and `slot`, the provenance ids) is only required by name: order files come from the
   catalog's writer, not by hand (maintainer, Q8, 2026-11-04). A field the order file
   writes as null is required and may be null (a timeless file's `delivery`, a storage
   `length`); a field it omits when empty is optional. `extract_format` is required: the
   runner has no default format. A column without an expected storage `type` is refused,
   since the storage check below needs one.
2. Plan the files: one per manifest file, named `<name>.<format>` (the catalog names the
   file after the physical table, [../crates/DESIGN.md](../crates/DESIGN.md), "MONA
   order file"), its columns in manifest order (ordered columns first, then held keys).
   A name outside slug spelling is refused, so a manifest cannot name a path; so are two
   files whose names differ only in case (MONA's file system would let one overwrite the
   other), a stem of the run's own logs, and a Windows device name (`nul`, `con`, ...),
   which would swallow the data. So is a file listing a column twice, or naming both a
   partition and a projection. A schema, table or column name is at most 128 UTF-16
   units, SQL Server's `sysname`.
3. Resolve every file before writing any: its table in the schema the order names, from
   `INFORMATION_SCHEMA.TABLES` (exactly one hit), then each column and the period
   column's `DATA_TYPE`, length, precision and scale. Names match byte-exactly
   (`Latin1_General_BIN2`). Each column's storage must equal the order's expected
   storage exactly: type, length, precision and scale, a null being SQL `NULL` and -1
   `max`. A drifted column is refused, naming the file, the column and both storages,
   never a value: the order was made against other holdings, and a drift is not
   permission to coerce. The row-filter column has no expected storage; its type is
   checked against the encoding (below). A column the format cannot hold is refused here
   too, before any row is read: for `.dta` and `.parquet` a SQL type outside the mapping
   below, for `.dta` a name it is written under that is not a Stata variable name (the
   order gives every such column a `stata_name`, so this is a backstop) or a full name
   longer than a variable label holds (80 characters).
4. Count, before writing any file, the rows each file's extract will write: one
   `COUNT_BIG(*)` per file with the file's own filter, through the same `sp_executesql`
   path as the extract ("Pre-count" below). Print the order's totals, its three
   largest files, the columns not in the size, the failed counts and how long the
   count took ("Console" below).
5. Extract each file with one `SELECT`, fetched in chunks of `fetch_rows` rows (the
   steward configuration's; 100,000 when it names none) into the order's one format
   (below) under `<name>.partial`, renamed to its own name once
   complete, printing one line per finished file. `run_log.csv` (file, rows,
   rows_counted, columns, filter, rows_without_period, bytes_written; the file name's
   extension is the format) gets each file's row as it finishes. `bytes_written`, the
   file's size on disk, is there to compare with the size estimate.

On success the folder is renamed and the order moves to
`orders/archive/<nr>_<YYYY-MM-DD>.json`. On failure the order stays in `orders/`, the
console prints the failure (below, "Row values"), and the `.partial` folder keeps its
finished files: only the file that was being written and any `.tmp` copy are removed,
and a `.tmp` copy whose file is missing is renamed back, since it is then the only
whole copy. A refusal before the folder is opened (setup, or an existing dated
folder) touches nothing. The console's last line says what is left: the files the
next run resumes, a complete folder to rename by hand (its rename failed), or a
published export whose order could not be archived. A run that is stopped
(Esc, Ctrl-C, a killed batch job) leaves the same folder plus that file's `.partial`
copy. Either way the folder name says the order did not finish, and the next run
resumes from it (#2630).

## Pre-count

The person running an order on MONA should know its size and likely run time before
the expensive part starts (#2624). The count runs in every order, after the manifest checks and table resolution and
before the first file is written, with no flag and no extra step, since a batch run takes no arguments; it is
information only and never gates, prompts or refuses.

- **What it counts.** For each manifest file, the rows its extract selects: the
  extract's own `WHERE` (one shared builder), so the count and the extract agree.
  `run_log.csv` keeps it as `rows_counted` beside the rows written (`rows`); the two
  differ only if the table changed during the run.
- **What it costs.** One `COUNT_BIG(*)` per file, which is a scan of the table, or of
  the view's query, under the filter. It reads no columns and writes nothing, so it is
  cheaper than the extract, but on a large view it is not free: it can add a
  noticeable share of the extract's time. No figure is measured: agents are never on
  MONA. The console reports how long counting took.
- **Size.** Rows times the column widths written in the order's format ("Types per
  format"): a `.dta` long 4 bytes, a double 8; Parquet's own widths before
  compression; a CSV field's usual text plus its comma; a string at its declared length.
  It is rough: a declared string length is a maximum, Parquet compresses and a CSV
  number's text varies. A string with no declared length (`max`, `text`, `ntext`), and
  a CSV column of a type the table does not list, has no width: the console names
  those columns as not in the size. A file with no column of known width has an
  unknown size, not zero. The console shows the size as an estimate, `~300 GB est.`,
and marks one that leaves such a column or file out with `+`.
- **Time left.** No throughput is known before the first rows arrive, so there is no
  time before the extract starts. During it, the rate is the cells (rows times
  columns) actually fetched per second so far in the order. The work left is each
  unfinished file's counted cells, less what the current file has fetched; a
  finished file drops out whatever its actual rows were, so a count that was off
  does not carry into later estimates. The rate is not shown before extracting has run
  five minutes: the first small files are mostly query overhead. A file whose count
  failed is left out of the time left. With no counted work left (the last file done, or only uncounted
  files left) no time is given, and a file's closing line comes after its
  `rows_without_period` count. A total over no estimate reads unknown, not 0. The rate ignores
  that text columns cost more than numbers, so the estimate settles as the run goes on.
  Rows count as written when fetched: a `.dta` file is written whole at its end (see
  "Memory"), and that write is not in the estimate.
- **Failure.** A count that fails (a driver error, an R error or warning) is listed in
  the count block, with its message in an error block ("Row values"), `rows_counted` is empty for that file, and the extract runs as
  it would have without it. The console lines of the pre-count and progress are
  formatted inside a guard too: a warning or error there (such as R's `OutDec` locale
  option clashing with a separator) drops the line, never the order. A run that passed
  before the pre-count still passes.

## Console

The coordinator follows a batch run by reading its `.Rout` file, where no line can
be overwritten, so the console prints few lines in fixed-width, ASCII-only columns
(#2631; trial on MONA, 2026-11-23). Durations are `h:mm`, an estimate has a `~`, and
a file is named without the order's format, which the header gives.

```text
order 1234   136 files   parquet   -> export_data/1234_2026-11-23
count      0:05   2,916,948,188 rows   ~300 GB+ est. uncompressed
  largest    table_c          1,183,402,551 rows   ~140 GB
             table_a              6,233,616 rows   ~900 MB
             table_b                912,004 rows   ~88 MB+
  not sized  table_d          COLUMN_X (varchar(max))
  failed     none
extract                          rows       size  elapsed     left
    1/136  table_a          6,233,616      80 MB     0:03
   97/136  table_c     310,000,000 of 1,183,402,551  1:05    ~3:10
   97/136  table_c      1,183,402,551      81 GB     1:16    ~1:02
done   136 files   2,916,948,188 rows   171 GB   4:12   exit 0
finished   1 order   0 failed   exit 0
```

- **One line per finished file**: its place among the order's files (kept files of
  a resumed run first), rows written, size on disk, time since extracting began and
  the time left. No line when a file starts.
- **A long file** gets a progress line in the same columns once it has run five
  minutes, then at most every five minutes, so a file of a billion rows does not look
  hung.
- **A failure** takes the file's row, `FAILED (exit code N): reason`, followed by the
  error block, what is left ("Resuming") and a closing line in `done`'s place:
  `stopped   132 of 136 files   <rows>   <size>   2:07   exit 8`. The first failure is
  the one reported: a warning or error raised while the order stops (closing a file,
  clearing a database result) is listed in the error block as `also, while stopping:`
  and cannot replace it. On MONA (2026-11-24) such a warning, `Result already
  cleared`, hid the fetch error that stopped P1105.
- **The last line** of every run is `finished   <n> orders   <m> failed   exit N`, or
  `finished   exit N` when the run stopped before any order: the exit code the batch
  job ends with, printed in an interactive session too, where the script does not
  quit.
- **The count block** keeps each file's count out of the console: `run_log.csv` has
  it (`rows_counted`).

## SQL

The script is ASCII. Non-ASCII identifiers sent as R string bytes fail on MONA, so every
name reaches SQL Server as an `nvarchar` expression of `N'...'` runs and `NCHAR(n)` code
units, which is a value and never SQL text. The `SELECT` is assembled server-side from
`QUOTENAME()`d names and run by `sp_executesql`; the filter bounds are its parameters,
bound from R.

A filter keeps a row whose period value lies in any range (OR), comparing at the
encoding's grain:

  | Encoding   | Column types                  | Compared value                              | Bound       |
  | ---------- | ----------------------------- | ------------------------------------------- | ----------- |
  | `year`     | int, float, decimal, varchar  | `TRY_CAST(col AS bigint)`                   | `YYYY`      |
  | `yyyymm`   | int, float, decimal, varchar  | `TRY_CAST(col AS bigint)`, a calendar month | `YYYYMM`    |
  | `yyyymmdd` | int, float, decimal, varchar  | `TRY_CAST(col AS bigint)`, a calendar day   | `YYYYMMDD`  |
  | `date`     | date, datetime2, varchar, ... | `TRY_CAST(col AS date)`                     | the ISO day |

A bound is the ISO day truncated to the encoding's digits, which is the contract's
widening: a finer bound covers its whole year or month. `date` compares days, so a
datetime keeps every time of a bound day. Blank text reads as NULL (SQL Server would cast
it to 1900-01-01 or 0), and so does a `yyyymm` that is not six digits naming a calendar
month (`201813`) and a `yyyymmdd` that is not eight digits naming a calendar day
(`20170230`). Text is checked as written (`0201801` is no month, `099912` is one), a
number by its range; the digit count comes first, as style 112 would read `180101` as a
day. A fractional number (`20180101.5`, tested exactly in the column's own type with
`col = ROUND(col, 0, 1)`) reads as NULL under any integer encoding. A NULL or unparseable period value
matches no range and is left out of a filtered file; a second query counts those rows
for `run_log.csv` (`rows_without_period`, an aggregate, empty for an unfiltered file,
which keeps every row). The filter and the count read the same value. Another type for
an encoding (a date column read as `year`) is refused before any query rather than
guessed.

## CSV

Header row of the manifest's column names. Numbers in fixed notation to 15 significant
digits (a float id never prints as `1e+15`); `bigint` is read as text and `decimal` and
`numeric` are fetched as text, so an 18-digit key never passes through a double. `char`,
`varchar` and `text` are fetched as `nvarchar`, so they arrive as UTF-8 whatever the
client code page; text that is still not UTF-8 fails the order. `bit` is 0/1, days
`YYYY-MM-DD`, date-times `YYYY-MM-DD HH:MM:SS` and every fractional digit SQL Server
holds, trailing zeros dropped (legacy `datetime` at its displayed milliseconds), NULL an
empty field. A text field is quoted only when it must be: when empty (so it differs from
NULL), or holding a quote, comma, line break or edge whitespace. Lines end in LF. Rows
come in the server's order: there is no `ORDER BY`, which would sort whole registers for
nothing.

## Types per format

The researcher picks one format per project (`extract_format`,
[../crates/reg-core/DESIGN.md](../crates/reg-core/DESIGN.md)); the order carries it and
every file of the order is written in it. The project must state one. CSV keeps every value
as text but its reader guesses the types back, losing a leading-zero code, a date or a
large id. What each SQL type becomes:

  | SQL type                                                   | R value fetched | `.dta` (format 118)                       | `.parquet`             | `.csv`                |
  | ---------------------------------------------------------- | --------------- | ----------------------------------------- | ---------------------- | --------------------- |
  | `tinyint`, `smallint`, `int`                               | integer         | long; above 2147483620 refused            | int32                  | digits                |
  | `bigint`                                                   | text            | double `%20.0g`; at or above 2^53 refused | int64                  | digits                |
  | `decimal(p, s)`, `numeric(p, s)`, p <= 15                  | text            | double `%20.0g`                           | decimal128(p, s)       | the decimal as text   |
  | `decimal(p, s)`, `numeric(p, s)`, p > 15                   | text            | str (exact digits)                        | decimal128(p, s)       | the decimal as text   |
  | `float`, `real`                                            | double          | double; beyond Stata's range refused      | float64, float32       | 15 significant digits |
  | `bit`                                                      | logical         | long 0/1                                  | bool                   | 0/1                   |
  | `date`                                                     | text            | `%td` day                                 | date32                 | `YYYY-MM-DD`          |
  | `datetime`, `datetime2`, `smalldatetime`                   | text            | `%tc`, cut to the millisecond            | timestamp(us, UTC), cut to the microsecond | `YYYY-MM-DD HH:MM:SS[.f]` |
  | `char`, `varchar`, `nchar`, `nvarchar`, `text`, `ntext`    | UTF-8 text      | str or strL; NULL as `""`                 | string                 | text                  |
  | `time`, `datetimeoffset`                                   | -               | refused at resolve (7)                    | refused at resolve (7) | refused at resolve (7) |
  | any other (`money`, binary, ...)                           | as odbc returns | refused at resolve (7)                    | refused at resolve (7) | as text               |

Choices behind the table:

- **Never round, never cut**, with one exception below (a date-time's fraction). A
  value a `.dta` file cannot hold exactly fails the order (exit 8) naming the column,
  never a value; a column whose type or name it cannot hold fails before any row is
  read (exit 7). The refusal names `parquet` and `csv`, which hold everything.
- **`bigint` is a double in `.dta`.** Stata has no 64-bit integer. A double holds every
  integer below 2^53 (about 9 \* 10^15) exactly, which covers every pseudonymised
  `LopNr` and a 12-digit personal or organisation number, and a numeric id merges like
  SCB's own Stata deliveries. The alternative, a string, never refuses but makes every
  id a `destring` away from a merge with other numeric-id files. The value is checked
  after parsing, so one that rounded on parse is still caught (a value at or above 2^53
  parses to at least 2^53).
- **A decimal goes by its declared precision**, a schema fact, so a column's Stata type
  never depends on its rows: up to 15 digits fit a double, a wider decimal stays exact
  as text. Parquet keeps decimal128 at the column's own precision and scale.
- **Dates and date-times are fetched as ISO text** (`CONVERT` styles 23 and 121), so
  every driver returns them alike. The legacy Windows "SQL Server" driver, which a
  DSN may name, returns `datetime2` and `date` as text of its own, and a `CAST` of
  `datetime` to text is `Jan  1 2026  4:33PM`. The runner then reads the text: a
  date-time is a time of no zone, written as UTC. A `.dta` `%tc` value is whole
  milliseconds since 1960, computed exactly. CSV keeps every digit, trailing zeros
  dropped (found on MONA, 2026-11-23: the DSN's driver returned `datetime2` as text,
  which Arrow refused as a UTC timestamp).
- **A time finer than the format holds is cut, and the console says so.** `datetime2`
  holds 100 ns and system timestamps usually fill all seven digits; Parquet's
  timestamp holds a microsecond and Stata's `%tc` a millisecond. Refusing them would
  fail nearly every `datetime2` column, so the fraction is truncated (never rounded,
  so a time never moves into the next second or day), and each column that lost a
  non-zero digit gets one console line after its file: `<column>: fraction cut to us
  in N values` (`ms` for `.dta`). It is the format's limit, not the runner's; an order
  that needs every digit is ordered as CSV (coordinator review, 2026-11-23). Legacy
  `datetime` counts in 1/300 s ticks; it is written at its displayed millisecond
  precision (style 121), as SQL Server shows it.
- **Text NULL is `""` in `.dta`.** Stata has no missing string, and reads an empty
  string as missing. CSV and Parquet keep NULL apart from `""`.
- **Stata limits.** Format 118 (Stata 14 and later) is UTF-8 with 32-character names and
  strL for text over 2045 bytes. A column whose name is not a Stata variable name is
  written under the column's `stata_name`, the order's alias
  ([../crates/DESIGN.md](../crates/DESIGN.md), "MONA order file"), with its full name as
  the variable label. Only a column that is not a Stata variable name has an alias, and
  no two columns of a file are written under one name, or the order is refused (exit 6).
  `%tc` holds milliseconds: a `datetime2`'s finer digits are cut and counted (above).
- **Memory.** CSV and Parquet stream one chunk of `fetch_rows` rows at a time. A chunk
  is written in row slices of at most 1 GB of text per column (Parquet) or per slice
  (CSV), since an arrow string array and an R string hold under 2 GB; a Parquet row
  group is one slice, split again at arrow's own cap (1,048,576 rows in current
  versions). Peak memory is the fetched chunk plus its converted copy, so it grows
  with `fetch_rows` times the width of the file; the steward sizes `fetch_rows` for
  the machine the runner runs on. haven cannot append, so a `.dta` file is collected whole and written once: the
  ceiling is MONA's RAM for the order's widest file (`simplify:` in the script).

The type table is verified by the manual run on MONA (see Tests); a long (`%12.0g`) and
a double (`%20.0g`) differ in the `.dta` display format.

## Resuming

Run one `extract.R` at a time per order: two runs of the same order share its
`.partial` folder, and nothing locks it (a coordinator rule, not code).

An unfinished order's folder, `<nr>.partial/`, holds its finished files, `run_log.csv`
with a row for each, and `order.json`, a copy of the order that ran. The next run of
the same order number reads both after the pre-count and keeps a finished file only if
its whole manifest entry is identical to the new order's (the format too, through the
file name), it is listed in the log, and its source now counts the rows it was written
with. Everything else in the folder is removed before extracting, a `.partial` file
included, and the kept files are not extracted again. The pre-count is the only check
on the source: a resume assumes the source tables did not change between the runs in a
way that keeps their row counts. A count that fails keeps nothing for that file. A
missing or unreadable `order.json` or log keeps nothing. `order.json` is removed when
the order completes.

Resuming works only within one runner version: a log whose columns differ (written by
another version) is unreadable, and the console says so before its files are cleared,
`run_log.csv is from another runner version: N finished file(s) will be extracted
again`. There is no compatibility path before v1 (coordinator review, 2026-11-23).

`run_log.csv` and `order.json` are written to a `.tmp` name and renamed over the old
copy, so a readable checkpoint is always there (where the share refuses to rename over
a file, the old one is removed first, and a run stopped in that moment resumes nothing).
Keep-or-delete is decided from one listing of the folder's regular files by exact
name. When the order completes, the folder must hold exactly the order's files, the
log and `order.json`; then `order.json` is removed and the folder renamed to its dated
name. If that name already exists the order is refused, at the start of the run and
again here, and the `.partial` folder stays resumable. A rename that fails reports
its value rather than a warning, so the fallbacks above run and are not turned into an
exit 8 by the order's warning rule.

The unit is the file: a file of a billion rows that fails at the end is written again
whole.

## Row values

Everything the runner writes stays on MONA: the extract files are microdata, and only
approved aggregates leave MONA. `run_log.csv` holds file names, counts and manifest
fields only.

The console (and a batch run's `.Rout`) can hold row values too. A failure prints its headline (exit
code, the file, what failed, a driver error's SQLSTATE) and then, between marker lines
whose header says the text may contain row values and must not be copied off MONA, the
underlying message in full: the driver's message (SQL Server's conversion errors quote
the offending value) or the R error's, with the runner functions it was raised in. A
failed count prints the same block. Before 2026-11-23 these messages were withheld, and
a failed order showed only an error class, which left the data manager unable to
diagnose a failure on MONA (P1105 trial). Like the extract, the console stays on MONA:
the error block must not be copied or screenshotted off it, which is the data
manager's responsibility (coordinator, 2026-11-23). An R warning during an
order still fails it (R warns when it cannot translate a string, so a value may have
changed), and prints the warning in the same block.

The manifest decides which tables and columns are read: nothing checks it against an
allowlist or the project hash. The data manager's review of the order, and the catalog
that wrote it, are the control on what is extracted.

## Exit codes

Stable; the process exits with the code of the first order, in name order, that failed.

  | Code | Meaning                                                                                                                                         |
  | ---- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
  | 0    | every order extracted (or no orders)                                                                                                            |
  | 2    | setup: a package not installable, no `orders/` folder, no connection, no archive                                                                |
  | 3    | the order is not JSON                                                                                                                           |
  | 4    | `version` is not 4                                                                                                                              |
  | 5    | `provenance.mode` is not `steward_holdings`, or another steward's order                                                                         |
  | 6    | malformed manifest: unknown or missing field, bad value, unsafe file name, a null storage type                                                  |
  | 7    | a table not in its schema, a missing column, storage drift, a wrong period type, a column the format cannot hold (SQL type, `.dta` name, label) |
  | 8    | a query or a file write failed, or a value the format cannot hold exactly                                                                       |

## Tests

Process-level, run by hand
(`Rscript -e 'testthat::test_dir("reg_mona_runner/tests/testthat")'`; neither the gate
nor CI runs it): the script runs under `Rscript` against an export root with no
database, and tests read the exit code, the console and the export folder. The suite has one case per
refusal code that fires before the database (2 to 6) and needs no SQL Server.

Extraction is not in the automated suite: no test requires a SQL Server. It is verified
by a manual run on MONA with real code and real orders, covering the formats, the
filters and file topology, and refusals 7 and 8.
