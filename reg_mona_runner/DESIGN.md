# Design: the MONA extract runner (`reg_mona_runner`)

`swecov_extract.R` turns order manifests into extract files inside MONA. It is the first
reader of the order manifest, whose v3 contract is frozen in
[../crates/DESIGN.md](../crates/DESIGN.md) ("Order manifest"). A SWECOV data manager
runs it by hand; no agent and no network reach MONA, so the script is one self-contained
file that gets uploaded with the orders. Run steps are in [README.md](README.md).

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
  read; there is no per-order `error_log.csv` for it, since no order ran. The install
  runs when the script runs, which for this script is when it is sourced: sourcing it is
  running it. haven and arrow are installed the same way, but per order and only for the
  format that order needs, so a MONA without arrow still runs `.dta` and `.csv` orders;
  one still missing fails that order with exit code 2. The export gate carries the same
  helper (`reg_export_gate/DESIGN.md`, "The R file"), since each file is uploaded alone.
- **One configuration block.** The steward id, the export root and the connection (DSN
  `P1105`) sit at the top of the script, so another steward copies the file and edits
  that block. Two environment variables (`SWECOV_EXTRACT_BASE` and
  `SWECOV_EXTRACT_CONNECTION`) override the root and the connection; they exist for the
  tests and are unset on MONA.
- **No dry run** (maintainer, 2026-10-21). A rerun overwrites the order's export folder
  and archive copy, so a run is cheap to repeat.
- **Source or Rscript.** Under `source()` in an interactive session the script reports
  its exit code instead of quitting, since quitting would end the data manager's
  session. Under `Rscript` it exits with the code.

## The run

Every `<base>/orders/*.json` runs independently, in name order; the file stem is the
project number. An order writes `<base>/export_data/<nr>_<YYYY-MM-DD>/`, cleared first:

1. Parse and check the whole manifest before touching the database: version, then mode
   and steward, then shape. Each object is closed, so an unknown field anywhere is a
   refusal (the v3 freeze relies on this). `extract_format` is required: the runner has
   no default format.
2. Plan the files: one per physical table and table edition, whatever variants bind it,
   named `<physical.file>.<format>` (the catalog names the file after the table,
   [../crates/DESIGN.md](../crates/DESIGN.md), "Extraction keeps delivery topology"). A
   file's columns are its entries' distinct physical columns in manifest order (ordered
   entries first, then held keys). Every entry of a file must carry the same name and
   the same filter. A name outside slug spelling is refused, so a manifest cannot name a
   path; so are two files whose names differ only in case (MONA's file system would let
   one overwrite the other), a file named like the run's own logs, and a Windows device
   name (`nul`, `con`, ...), which would swallow the data. A table or column name is at
   most 128 UTF-16 units, SQL Server's `sysname`.
3. Resolve every file before writing any: its table's schema from
   `INFORMATION_SCHEMA.TABLES` (exactly one hit; the holdings put 17 views in schema
   `P1105` and the rest in `dbo`, with no name in both), then each column and the period
   column's `DATA_TYPE` (and each column's precision and scale). Names match
   byte-exactly (`Latin1_General_BIN2`). A column the format cannot hold is refused
   here, before any row is read: for `.dta` and `.parquet` a SQL type outside the
   mapping below, for `.dta` a name it is written under that is not a Stata variable
   name (the order gives every such column a `stata_name`, so this is a backstop) or a
   full name longer than a variable label holds (80 characters).
4. Extract each file with one `SELECT`, fetched in chunks of 100,000 rows into the
   order's one format (below), then write `run_log.csv` (file, rows, columns, filter;
   the file name's extension is the format).

On success the order moves to `orders/archive/<nr>_<YYYY-MM-DD>.json`. On failure the
order stays in `orders/` and its export folder holds only `error_log.csv` (code,
message): partial files are removed so they cannot pass as an extract.

## SQL

The script is ASCII. Non-ASCII identifiers sent as R string bytes fail on MONA, so every
name reaches SQL Server as an `nvarchar` expression of `N'...'` runs and `NCHAR(n)` code
units, which is a value and never SQL text. The `SELECT` is assembled server-side from
`QUOTENAME()`d names and run by `sp_executesql`; the filter bounds are its parameters,
bound from R.

A filter keeps a row whose period value lies in any range (OR), comparing at the
encoding's grain:

  | Encoding   | Column types                  | Compared value            | Bound       |
  | ---------- | ----------------------------- | ------------------------- | ----------- |
  | `year`     | int, float, decimal, varchar  | `TRY_CAST(col AS bigint)` | `YYYY`      |
  | `yyyymm`   | int, float, decimal, varchar  | `TRY_CAST(col AS bigint)` | `YYYYMM`    |
  | `yyyymmdd` | int, float, decimal, varchar  | `TRY_CAST(col AS bigint)` | `YYYYMMDD`  |
  | `date`     | date, datetime2, varchar, ... | `TRY_CAST(col AS date)`   | the ISO day |

A bound is the ISO day truncated to the encoding's digits, which is the contract's
widening: a finer bound covers its whole year or month. `date` compares days, so a
datetime keeps every time of a bound day. A NULL or unparseable period value matches no
range and is left out of a filtered file; an unfiltered file keeps every row. Another
type for an encoding (a date column read as `year`) is refused before any query rather
than guessed.

## CSV

Header row of the manifest's column names. Numbers in fixed notation to 15 significant
digits (a float id never prints as `1e+15`); `bigint` is read as text and `decimal` and
`numeric` are fetched as text, so an 18-digit key never passes through a double. `char`,
`varchar` and `text` are fetched as `nvarchar`, so they arrive as UTF-8 whatever the
client code page; text that is still not UTF-8 fails the order. `bit` is 0/1, days
`YYYY-MM-DD`, date-times `YYYY-MM-DD HH:MM:SS` (sub-second precision dropped), NULL an
empty field. A text field is quoted only when it must be: when empty (so it differs from
NULL), or holding a quote, comma, line break or edge whitespace. Lines end in LF. Rows
come in the server's order: there is no `ORDER BY`, which would sort whole registers for
nothing.

## Types per format

The researcher picks one format per project (`extract_format`,
[../crates/reg-core/DESIGN.md](../crates/reg-core/DESIGN.md)); the order carries it and
every file of the order is written in it. Stata is the default because most SWECOV
researchers use it; CSV keeps every value as text but its reader guesses the types back,
losing a leading-zero code, a date or a large id. What each SQL type becomes:

  | SQL type                                                   | R value fetched | `.dta` (format 118)                       | `.parquet`             | `.csv`                |
  | ---------------------------------------------------------- | --------------- | ----------------------------------------- | ---------------------- | --------------------- |
  | `tinyint`, `smallint`, `int`                               | integer         | long; above 2147483620 refused            | int32                  | digits                |
  | `bigint`                                                   | text            | double `%20.0g`; at or above 2^53 refused | int64                  | digits                |
  | `decimal(p, s)`, `numeric(p, s)`, p <= 15                  | text            | double `%20.0g`                           | decimal128(p, s)       | the decimal as text   |
  | `decimal(p, s)`, `numeric(p, s)`, p > 15                   | text            | str (exact digits)                        | decimal128(p, s)       | the decimal as text   |
  | `float`, `real`                                            | double          | double; beyond Stata's range refused      | float64, float32       | 15 significant digits |
  | `bit`                                                      | logical         | long 0/1                                  | bool                   | 0/1                   |
  | `date`                                                     | Date            | `%td` day                                 | date32                 | `YYYY-MM-DD`          |
  | `datetime`, `datetime2`, `smalldatetime`                   | POSIXct, UTC    | `%tc` (milliseconds)                      | timestamp(us, UTC)     | `YYYY-MM-DD HH:MM:SS` |
  | `char`, `varchar`, `nchar`, `nvarchar`, `text`, `ntext`    | UTF-8 text      | str or strL; NULL as `""`                 | string                 | text                  |
  | any other (`time`, `datetimeoffset`, `money`, binary, ...) | as odbc returns | refused at resolve (7)                    | refused at resolve (7) | as text               |

Choices behind the table:

- **Never round, never cut.** A value a `.dta` file cannot hold exactly fails the order
  (exit 8) naming the column, never a value; a column whose type or name it cannot hold
  fails before any row is read (exit 7). The refusal names `parquet` and `csv`, which
  hold everything.
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
- **Text NULL is `""` in `.dta`.** Stata has no missing string, and reads an empty
  string as missing. CSV and Parquet keep NULL apart from `""`.
- **Stata limits.** Format 118 (Stata 14 and later) is UTF-8 with 32-character names and
  strL for text over 2045 bytes. A column whose name is not a Stata variable name is
  written under the entry's `physical.stata_name`, the order's alias
  ([../crates/DESIGN.md](../crates/DESIGN.md), "Order manifest"), with its full name as
  the variable label. Every entry of a file names a column under one alias, only a
  column that is not a Stata variable name has one, and no two columns of a file share a
  name, or the order is refused (exit 6). `%tc` holds milliseconds, so a `datetime2`'s
  finer digits are dropped; Parquet keeps microseconds, dropping `datetime2(7)`'s 100 ns
  digit; CSV already writes whole seconds.
- **Memory.** CSV and Parquet stream one chunk at a time (a Parquet row group per
  chunk). haven cannot append, so a `.dta` file is collected whole and written once: the
  ceiling is MONA's RAM for the order's widest file (`simplify:` in the script).

The type table is pinned by one case per format in the SQL Server suite (below); the
`.dta` oracle reads each column's Stata display format back, so a long (`%12.0g`) and a
double (`%20.0g`) differ. A refusal case pins the 2^53 bound.

## No row values in logs

Console output, `run_log.csv` and `error_log.csv` hold file names, counts and manifest
fields only. A driver error is reported by the SQLSTATE in its message prefix: the rest
is withheld, since SQL Server's conversion errors quote the offending value. For the
same reason an unexpected R error is reported by its class alone, and an R warning
during an order (R quotes a string it cannot translate) fails the order instead of
printing.

The manifest decides which tables and columns are read: nothing checks it against an
allowlist or the project hash. The data manager's review of the order, and the catalog
that wrote it, are the control on what is extracted.

## Exit codes

Stable; the process exits with the code of the first order, in name order, that failed.

  | Code | Meaning                                                                                                                                       |
  | ---- | --------------------------------------------------------------------------------------------------------------------------------------------- |
  | 0    | every order extracted (or no orders)                                                                                                          |
  | 2    | setup: a package not installable, no `orders/` folder, no connection, no archive                                                              |
  | 3    | the order is not JSON                                                                                                                         |
  | 4    | `version` is not 2                                                                                                                            |
  | 5    | `provenance.mode` is not `steward_holdings`, or another steward's order                                                                       |
  | 6    | malformed manifest: unknown or missing field, bad value, unsafe file name                                                                     |
  | 7    | a table in no schema or several, a missing column, a wrong period type, a column the format cannot hold (SQL type, or a `.dta` name or label) |
  | 8    | a query or a file write failed, or a value the format cannot hold exactly                                                                     |

## Tests

Process-level, through `scripts/gate.py runner`: the script runs under `Rscript` against
an export root, and tests read the exit code, the extracted files and logs. Locally (no
SQL Server) one case per refusal code that fires before the database (2 to 6). In CI a
SQL Server service container loads `tests/mssql/fixture.sql`, and one run extracts the
conformance order goldens that cover filters and file topology plus the runner's own
manifests (`tests/cases/*/order.json`: every encoding and delivery type, schema `P1105`,
non-ASCII names, compact JSON, and `types-dta`, `types-parquet` and `types-csv` over one
table of every mapped SQL type) and the refusals 7 and 8. Expected CSVs under
`tests/cases/*/expected/` are the fixture rows passed through each order by hand; a
`.dta` or `.parquet` file's oracle is `<file>.json`, its column types and rows as text,
read back with haven or arrow by the test, not the runner.
