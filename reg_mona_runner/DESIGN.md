# Design: the MONA extract runner (`reg_mona_runner`)

`swecov_extract.R` turns order manifests into extract files inside MONA. It is the first
reader of the order manifest, whose v1 contract is frozen in
[../crates/DESIGN.md](../crates/DESIGN.md) ("Order manifest"). A SWECOV data manager
runs it by hand; no agent and no network reach MONA, so the script is one self-contained
file that gets uploaded with the orders. Run steps are in [README.md](README.md).

## Shape

- **One file, base R plus DBI, odbc and jsonlite.** These packages are assumed to be
  preinstalled in MONA's R (>= 4.2, so the native encoding is UTF-8). jsonlite parses
  the manifest; a hand-written JSON parser would be more code to trust than the
  dependency.
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
   refusal (the v1 freeze relies on this).
2. Plan the files: one per variant, physical table and table edition, named
   `<register>_<variant>[_<partition>|_<projection>]_<edition>.csv`. A file's columns
   are its entries' distinct physical columns in manifest order (ordered entries first,
   then held keys). Every entry of a file must carry the same filter. A name outside
   slug spelling is refused, so a manifest cannot name a path, and so are two files
   whose names differ only in case (MONA's file system would let one overwrite the
   other). A table or column name is at most 128 UTF-16 units, SQL Server's `sysname`.
3. Resolve every file before writing any: its table's schema from
   `INFORMATION_SCHEMA.TABLES` (exactly one hit; the holdings put 17 views in schema
   `P1105` and the rest in `dbo`, with no name in both), then each column and the period
   column's `DATA_TYPE`. Names match byte-exactly (`Latin1_General_BIN2`).
4. Extract each file with one `SELECT`, streamed in chunks of 100,000 rows to UTF-8 CSV,
   then write `run_log.csv` (file, rows, columns, filter).

On success the order moves to `orders/archive/<nr>_<YYYY-MM-DD>.json`. On failure the
order stays in `orders/` and its export folder holds only `error_log.csv` (code,
message): partial CSVs are removed so they cannot pass as an extract.

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

  | Code | Meaning                                                                   |
  | ---- | ------------------------------------------------------------------------- |
  | 0    | every order extracted (or no orders)                                      |
  | 2    | setup: no `orders/` folder, a package missing, no connection, no archive  |
  | 3    | the order is not JSON                                                     |
  | 4    | `version` is not 1                                                        |
  | 5    | `provenance.mode` is not `steward_holdings`, or another steward's order   |
  | 6    | malformed manifest: unknown or missing field, bad value, unsafe file name |
  | 7    | a table in no schema or several, a missing column, a wrong period type    |
  | 8    | a query or a file write failed                                            |

## Tests

Process-level, through `scripts/gate.py runner`: the script runs under `Rscript` against
an export root, and tests read the exit code, CSVs and logs. Locally (no SQL Server) one
case per refusal code that fires before the database (2 to 6). In CI a SQL Server
service container loads `tests/mssql/fixture.sql`, and one run extracts the conformance
order goldens that cover filters and file topology plus the runner's own manifests
(`tests/cases/*/order.json`: every encoding and delivery type, schema `P1105`, non-ASCII
names, compact JSON) and the refusals 7 and 8. Expected CSVs under
`tests/cases/*/expected/` are the fixture rows passed through each order by hand.
