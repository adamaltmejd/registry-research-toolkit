# SWECOV extract runner

`swecov_extract.R` extracts SWECOV orders inside MONA. Each order is an `order.json`
that the catalog wrote for one sub-project; the script writes the ordered columns as CSV
files into that sub-project's export folder. Run it by hand in MONA.

## Folders

All under the export root, set at the top of the script:

```text
\\micro.intra\projekt\P1105$\P1105_Gem\subproject_export\
  orders\                  orders waiting to run, named <project number>.json
  orders\archive\          orders that ran, as <project number>_<date>.json
  export_data\             one folder per run, <project number>_<date>\
```

## Run an order

1. Download the order from the catalog (data.swecov.se) after validating the project.
   The file is named `order_<project>_<date>.json`; the coordinator renames it in the
   next step.
2. Upload it to MONA together with `swecov_extract.R`, and save it as
   `orders\<project number>.json` (for example `orders\1234.json`).
3. In R on MONA, `source("swecov_extract.R")`, or run `Rscript swecov_extract.R`. Every
   order in `orders\` runs, one after the other.
4. Read the console: each order ends with `done` or `FAILED (exit code N)`.

The script first installs any of its R packages (DBI, odbc, jsonlite) that is missing,
into R's default library from MONA's mirror, so the first run on a new server takes
longer. It installs only when the default library is writable and a CRAN mirror is
configured. If one cannot be installed, the run stops with exit code 2 before any order,
and the console names the package. Install it yourself, or ask MONA support (unwritable
library, no or unreachable mirror).

## Results

A successful order writes `export_data\<nr>_<date>\`:

- one CSV per output file, named `<register>_<variant>_<period>.csv`, or
  `<register>_<variant>_<part>_<period>.csv` for a partition or projection table (UTF-8,
  header row, empty field for a missing value);
- `run_log.csv`: each file's row count, column count and the period filter applied.

The order then moves to `orders\archive\`.

A failed order stays in `orders\`, and its export folder holds only `error_log.csv` with
the exit code and the reason. Fix the cause and run the script again: a rerun overwrites
that day's export folder.

  | Code    | What to do                                                                   |
  | ------- | ---------------------------------------------------------------------------- |
  | 2       | Check the export root, the R packages (DBI, odbc, jsonlite) and the database |
  | 3, 4, 6 | The file is not a usable order: download it again from the catalog           |
  | 5       | Not an order from SWECOV's holdings: rebuild the project in the SWECOV view  |
  | 7       | A table or column is not in the database: report it to the catalog team      |
  | 8       | The query or the write failed: check disk space, then report it              |

## Safety

The script never prints or logs row values: only file names, counts and the order's own
fields. A database error is reported by its code, not its message. The extract files
contain microdata: they stay in MONA like any other project data.

## Other stewards

The steward-specific settings (steward id, export root, database connection) are in the
configuration block at the top of the script. Copy the script and edit that block.
