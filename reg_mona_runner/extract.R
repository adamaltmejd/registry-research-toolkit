options(echo = FALSE) # R CMD BATCH: keep the script out of the .Rout (non-interactive only)

# extract.R: a steward's order extract runner, run by hand inside MONA.
#
# For every order manifest in <base>/orders/*.json (the order.json the catalog
# writes, renamed to the MONA project name as MONA writes it, e.g. P1234), extract
# the ordered columns from the steward's holdings: one file per manifest file, in
# the order's extract_format (Stata .dta, Parquet or CSV), plus run_log.csv. An
# order is written into <base>/export_data/<project>.partial/, each file under a
# .partial name until it is complete; a success renames the folder to
# <project>_<YYYY-MM-DD>/ and moves the order to
# orders/archive/<project>_<YYYY-MM-DD>.json. A failure, or a run that is stopped,
# leaves the order in orders/ and its finished files in <project>.partial/, and
# the next run resumes from them.
#
# Run: source("extract.R") in R on MONA, `Rscript extract.R`, or
# `R CMD BATCH extract.R` on the batch server, beside extract-config.json.
# The contract it reads is crates/DESIGN.md, "MONA order file"; the rationale and
# the exit codes are reg_mona_runner/DESIGN.md.
#
# Packages: base R (>= 4.2, UTF-8 native), DBI, odbc and jsonlite, plus haven for
# a .dta order and arrow for a .parquet one; any missing is installed into R's
# default library first (haven and arrow when an order needs them). Files under
# export_data/ hold no row values beyond the extract itself. The console does: a
# failure prints the full R or driver message in a marked error block, since the
# console stays on MONA (reg_mona_runner/DESIGN.md, "Row values").
#
# This file is ASCII only: a non-ASCII name reaches SQL Server as NCHAR() code
# points, never as R string bytes (non-ASCII identifiers sent as text fail on MONA).

# ---- Steward configuration ----------------------------------------------------------

# Everything steward-specific is a small JSON file the steward repository keeps:
# `extract-config.json` in R's working directory, or the file REG_EXTRACT_CONFIG
# names. Closed keys: `steward` (the manifest's provenance.steward this deployment
# serves), `base` (the export root holding orders/, orders/archive/ and
# export_data/) and `odbc` (the DBI::dbConnect arguments for odbc::odbc(), all
# strings, such as dsn, Driver, Server and Database), and optionally `fetch_rows`
# (rows per fetch, and so per Parquet row group; FETCH_ROWS when absent). Read in
# main(), once the packages are in place.
config <- NULL

read_config <- function() {
  path <- Sys.getenv("REG_EXTRACT_CONFIG", "extract-config.json")
  if (!file.exists(path)) {
    return(paste0("no steward configuration: ", path))
  }
  cfg <- tryCatch(
    jsonlite::read_json(path, simplifyVector = FALSE),
    error = function(e) NULL
  )
  strings <- function(x) {
    is_object(x) && all(nzchar(names(x))) && all(vapply(x, is_string, TRUE))
  }
  whole <- function(x) {
    is.numeric(x) && length(x) == 1L && !is.na(x) && x >= 1 &&
      x <= .Machine$integer.max && x == round(x)
  }
  if (
    !is_object(cfg) ||
      !all(c("steward", "base", "odbc") %in% names(cfg)) ||
      !all(names(cfg) %in% c("steward", "base", "odbc", "fetch_rows")) ||
      anyDuplicated(names(cfg)) ||
      ("fetch_rows" %in% names(cfg) && !whole(cfg$fetch_rows)) ||
      !is_string(cfg$steward) ||
      !grepl("^[a-z0-9]+(-[a-z0-9]+)*$", cfg$steward) ||
      !is_string(cfg$base) ||
      !nzchar(cfg$base) ||
      !strings(cfg$odbc)
  ) {
    return(paste0(
      "steward configuration ",
      path,
      " is not {\"steward\": slug, \"base\": path, \"odbc\": {name: string}},",
      " with an optional \"fetch_rows\": positive whole number"
    ))
  }
  cfg$fetch_rows <- as.integer(if (is.null(cfg$fetch_rows)) FETCH_ROWS else cfg$fetch_rows)
  cfg$connect <- function() {
    do.call(
      DBI::dbConnect,
      c(
        list(odbc::odbc()),
        cfg$odbc,
        list(encoding = "UTF-8", bigint = "character")
      )
    )
  }
  cfg
}

# ---- Exit codes (stable; reg_mona_runner/DESIGN.md) ---------------------------

EXIT <- c(
  ok = 0L,
  setup = 2L, # steward configuration, export root, packages or database connection
  json = 3L, # the order is not JSON
  version = 4L, # manifest version is not 5
  mode = 5L, # not a steward_holdings order for this steward
  manifest = 6L, # unknown field, missing or malformed field, unsafe file name
  # table or column not found in its schema, a column stored otherwise than the
  # order expects, a period column of the wrong type, or a column the order's
  # format cannot hold (its SQL type or, for .dta, its name)
  resolve = 7L,
  extract = 8L # the query or the file write failed, or a value the format cannot hold
)

# Rows per fetch when the steward configuration names no `fetch_rows`.
FETCH_ROWS <- 100000L
# Text bytes one write may hold, per Parquet column or per CSV slice: half of
# what an arrow string array or an R string holds (2^31 bytes).
SLICE_BYTES <- 1e9
# The suffix of a file or order folder that is not finished yet.
PARTIAL <- ".partial"
ENCODINGS <- c("date", "year", "yyyymm", "yyyymmdd")
DATE_TYPES <- c(
  "date",
  "datetime",
  "datetime2",
  "smalldatetime",
  "datetimeoffset"
)
NUMBER_TYPES <- c(
  "tinyint",
  "smallint",
  "int",
  "bigint",
  "decimal",
  "numeric",
  "float",
  "real"
)
TEXT_TYPES <- c("char", "varchar", "nchar", "nvarchar")
# The SQL types a .dta or .parquet file maps (reg_mona_runner/DESIGN.md, "Types per
# format"); a CSV writes any type as text.
INT_TYPES <- c("tinyint", "smallint", "int")
TIME_TYPES <- c("datetime", "datetime2", "smalldatetime")
STRING_TYPES <- c(TEXT_TYPES, "text", "ntext")
MAPPED_TYPES <- c(
  INT_TYPES,
  "bigint",
  "decimal",
  "numeric",
  "float",
  "real",
  "bit",
  "date",
  TIME_TYPES,
  STRING_TYPES
)
# The largest value a Stata long holds (larger ones are its missing codes), the
# bound below which a double holds every integer exactly, and the largest double
# Stata holds (larger ones are its missing codes).
STATA_LONG_MAX <- 2147483620L
DOUBLE_EXACT <- 2^53
STATA_DOUBLE_MAX <- 8.98846567431157e307
# A decimal of at most this many digits is a Stata double; a wider one is text.
DTA_DOUBLE_DIGITS <- 15L

# `detail` is the underlying R or driver message, which can quote a row value:
# it is printed only inside the failure's error block (run_order).
refuse <- function(code, ..., detail = NULL) {
  stop(structure(
    class = c("runner_refusal", "error", "condition"),
    list(
      message = paste0(...),
      call = NULL,
      code = EXIT[[code]],
      detail = detail
    )
  ))
}

# Flushed after every line: in a batch run R buffers its output, and a line
# printed before a minutes-long step reached the .Rout only after it (MONA,
# 2026-11-24).
say <- function(...) {
  cat(paste0(...), "\n", sep = "")
  # Both: R CMD BATCH writes to a file connection, Rterm and Rgui to a console.
  flush(stdout())
  flush.console()
}

# The underlying R or driver message of a failure, between marker lines. It can
# quote a row value, so it goes to the console only, which stays on MONA, never
# to a file under export_data/.
error_block <- function(detail, trace = NULL, late = NULL) {
  if (!length(detail) && !length(trace) && !length(late)) {
    return(invisible())
  }
  say("  ---- error detail: may contain row values, do not copy off MONA ----")
  for (line in if (length(detail)) unlist(strsplit(detail, "\n", fixed = TRUE))) {
    say("  ", line)
  }
  if (length(trace)) {
    say("  in: ", paste(trace, collapse = " > "))
  }
  for (line in late) {
    say("  also, while stopping: ", line)
  }
  say("  ---- end of error detail ----")
}

PACKAGES <- c("DBI", "odbc", "jsonlite")
FORMATS <- c("dta", "parquet", "csv")
# The writer package each format needs beyond PACKAGES, checked per order, so a
# MONA without arrow still runs .dta and .csv orders.
FORMAT_PACKAGES <- c(dta = "haven", parquet = "arrow", csv = NA)

# Same helper in reg_export_gate/export_gate.R: each script is uploaded to MONA
# alone. Installs each package into R's default library (.libPaths()[1]) from
# the repository R's options name (MONA's own mirror), so neither `lib` nor
# `repos` is set. It installs only when that library is writable and a mirror
# is set: otherwise install.packages() would prompt (personal library, mirror
# menu) or add a library path. A failure is not reported here: the caller
# checks the packages again and refuses, naming the package. Warnings and
# errors are caught: a caller that turns any warning into a failure would
# otherwise report a failed install without the package's name.
install_missing <- function(pkgs) {
  lib <- .libPaths()[1]
  repos <- getOption("repos")
  # install.packages()'s own test: on Windows file.access() ignores ACLs,
  # so R creates a test folder there instead.
  probe <- file.path(lib, paste0("_test_dir_", Sys.getpid()))
  unlink(probe, recursive = TRUE)
  writable <- file.access(lib, 2L) == 0L &&
    isTRUE(tryCatch(
      dir.create(probe, showWarnings = FALSE),
      error = function(e) FALSE
    ))
  unlink(probe, recursive = TRUE)
  if (!writable || !length(repos) || "@CRAN@" %in% repos) {
    return(invisible())
  }
  for (pkg in pkgs) {
    tryCatch(
      suppressWarnings(utils::install.packages(pkg)),
      error = function(e) NULL
    )
  }
}

# ---- Manifest validation --------------------------------------------------------
#
# The v4 order file (crates/reg-core/src/manifest.rs), checked here at the
# runner's own input boundary. The manifest, its provenance and every object the
# runner reads (files, columns, storage, row filters) are closed: an unknown key
# is a refusal, and each field the runner reads is checked. What it does not read
# (clips, findings, a column's role, refs and slot, the provenance ids, a file's
# delivery) is only required by name: the Rust writer is the authority on those.
# A field Rust writes as null is required and may be null; a field Rust omits
# when empty is optional (absent, or null, means none).

is_object <- function(x) is.list(x) && !is.null(names(x))
is_array <- function(x) is.list(x) && is.null(names(x))
is_string <- function(x) is.character(x) && length(x) == 1L && !is.na(x)
is_count <- function(x) {
  is.numeric(x) && length(x) == 1L && !is.na(x) && x >= 0 && x == round(x)
}

# `x` must be a JSON object with every `required` key and no key outside
# `required` and `optional`.
check_object <- function(x, where, required, optional = character()) {
  if (!is_object(x)) {
    refuse("manifest", where, " is not a JSON object")
  }
  unknown <- setdiff(names(x), c(required, optional))
  if (length(unknown)) {
    refuse(
      "manifest",
      where,
      " has unknown field(s): ",
      paste(unknown, collapse = ", ")
    )
  }
  missing <- setdiff(required, names(x))
  if (length(missing)) {
    refuse(
      "manifest",
      where,
      " lacks field(s): ",
      paste(missing, collapse = ", ")
    )
  }
  invisible(x)
}

check_string <- function(x, where) {
  if (!is_string(x) || !nzchar(x)) {
    refuse("manifest", where, " is not a non-empty string")
  }
  x
}

check_count_or_null <- function(x, where) {
  if (!is.null(x) && !is_count(x)) {
    refuse("manifest", where, " is not a non-negative integer or null")
  }
  x
}

check_enum <- function(x, where, values) {
  if (!is_string(x) || !(x %in% values)) {
    refuse(
      "manifest",
      where,
      " is not one of ",
      paste(values, collapse = ", ")
    )
  }
  x
}

# A SQL Server identifier: at most 128 UTF-16 code units (sysname), so
# sql_text's nvarchar(128) never truncates it into another name.
check_name <- function(x, where) {
  check_string(x, where)
  points <- utf8ToInt(enc2utf8(x))
  if (anyNA(points) || length(points) + sum(points > 0xFFFF) > 128L) {
    refuse("manifest", where, " is not a name of at most 128 UTF-16 units")
  }
  x
}

check_iso_day <- function(x, where) {
  check_string(x, where)
  day <- if (grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", x)) {
    as.Date(x, format = "%Y-%m-%d", optional = TRUE)
  }
  if (is.null(day) || is.na(day) || format(day, "%Y-%m-%d") != x) {
    refuse("manifest", where, " is not an ISO date (YYYY-MM-DD)")
  }
  x
}

check_filter <- function(f, where) {
  check_object(f, where, c("column", "encoding", "ranges"))
  check_name(f$column, paste0(where, ".column"))
  check_enum(f$encoding, paste0(where, ".encoding"), ENCODINGS)
  if (!is_array(f$ranges) || !length(f$ranges)) {
    refuse("manifest", where, ".ranges is not a non-empty array")
  }
  for (i in seq_along(f$ranges)) {
    at <- sprintf("%s.ranges[%d]", where, i - 1L)
    r <- f$ranges[[i]]
    check_object(r, at, character(), c("from", "to"))
    if (!is.null(r$from)) {
      check_iso_day(r$from, paste0(at, ".from"))
    }
    if (!is.null(r$to)) {
      check_iso_day(r$to, paste0(at, ".to"))
    }
    # An inverted range would select nothing and write a silently empty file.
    if (!is.null(r$from) && !is.null(r$to) && r$from > r$to) {
      refuse("manifest", at, " ends before it starts")
    }
  }
  f
}

check_column <- function(col, where, format) {
  check_object(
    col,
    where,
    c("name", "storage", "role", "refs"),
    c("stata_name", "slot", "lopnr")
  )
  # Absent is false, so a present one is true (not false, not null), as
  # crates/reg-core/src/manifest.rs reads it.
  if ("lopnr" %in% names(col) && !isTRUE(col$lopnr)) {
    refuse("manifest", where, ".lopnr is not true")
  }
  check_name(col$name, paste0(where, ".name"))
  if (!is.null(col$stata_name)) {
    check_string(col$stata_name, paste0(where, ".stata_name"))
    if (format != "dta") {
      refuse("manifest", where, ".stata_name is set on a .", format, " order")
    }
    # Only a name .dta cannot hold is renamed, so an alias never takes the name
    # of a column written under its own.
    if (stata_name_ok(col$name)) {
      refuse(
        "manifest",
        where,
        ".stata_name renames column ",
        col$name,
        ", which is a Stata variable name"
      )
    }
  }
  s <- col$storage
  check_object(
    s,
    paste0(where, ".storage"),
    c("type", "length", "precision", "scale")
  )
  # The drift check needs a type to compare: an order without one cannot be
  # held against the server's.
  check_string(s$type, paste0(where, ".storage.type"))
  check_count_or_null(s$precision, paste0(where, ".storage.precision"))
  check_count_or_null(s$scale, paste0(where, ".storage.scale"))
  # -1 is SQL Server's max (parse_json reads it as the integer -1L).
  if (!identical(s$length, -1L)) {
    check_count_or_null(s$length, paste0(where, ".storage.length"))
  }
  col
}

# A file stem the runner can write: slug spelling (no path separator, drive or
# parent reference), at most 128 UTF-16 units, and not the stem of a Windows
# device name, which would swallow the data, nor of the run's own logs, case
# ignored (safe_file_name in crates/reg-core/src/manifest.rs).
check_file_name <- function(x, where) {
  check_name(x, where)
  if (!grepl("^[A-Za-z0-9_][A-Za-z0-9_.,-]*$", x)) {
    refuse("manifest", where, " ", x, " is not slug spelling")
  }
  device <- c(
    "con",
    "prn",
    "aux",
    "nul",
    paste0(c("com", "lpt"), rep(1:9, each = 2))
  )
  stem <- tolower(sub("\\..*$", "", x))
  if (stem %in% c(device, "run_log", "error_log")) {
    refuse("manifest", where, " ", x, " is reserved")
  }
  x
}

check_file <- function(f, where, format) {
  check_object(
    f,
    where,
    c("name", "schema", "table", "delivery", "columns"),
    c("partition", "projection", "row_filter")
  )
  check_file_name(f$name, paste0(where, ".name"))
  check_name(f$schema, paste0(where, ".schema"))
  check_name(f$table, paste0(where, ".table"))
  for (k in c("partition", "projection")) {
    if (!is.null(f[[k]])) check_string(f[[k]], paste0(where, ".", k))
  }
  if (!is.null(f$partition) && !is.null(f$projection)) {
    refuse("manifest", where, " has both a partition and a projection")
  }
  if (!is.null(f$row_filter)) {
    check_filter(f$row_filter, paste0(where, ".row_filter"))
  }
  if (!is_array(f$columns) || !length(f$columns)) {
    refuse("manifest", where, ".columns is not a non-empty array")
  }
  for (i in seq_along(f$columns)) {
    check_column(
      f$columns[[i]],
      sprintf("%s.columns[%d]", where, i - 1L),
      format
    )
  }
  columns <- vapply(f$columns, `[[`, "", "name")
  twice <- unique(columns[duplicated(columns)])
  if (length(twice)) {
    refuse(
      "manifest",
      where,
      " lists column ",
      paste(twice, collapse = ", "),
      " twice"
    )
  }
  # Case-sensitive, as Stata's variable names are.
  written <- vapply(f$columns, written_name, "")
  clash <- unique(written[duplicated(written)])
  if (length(clash)) {
    refuse(
      "manifest",
      where,
      " would hold two columns named ",
      paste(clash, collapse = ", ")
    )
  }
  f
}

# The whole manifest, in the order of the stable exit codes: version, then mode
# and steward, then shape.
check_manifest <- function(doc) {
  if (!is_object(doc)) {
    refuse("manifest", "the order is not a JSON object")
  }
  v <- doc$version
  if (!(is.numeric(v) && length(v) == 1L && !is.na(v) && v == 5)) {
    refuse(
      "version",
      "manifest version ",
      format(v),
      " is not supported; this runner reads version 5"
    )
  }
  p <- doc$provenance
  if (!is_object(p) || !identical(p$mode, "steward_holdings")) {
    refuse(
      "mode",
      "provenance.mode is not steward_holdings: only an order from a steward's ",
      "holdings names physical tables"
    )
  }
  if (!identical(p$steward, config$steward)) {
    refuse(
      "mode",
      "provenance.steward is not ",
      config$steward,
      ": the order was made for another steward's holdings"
    )
  }
  check_object(
    doc,
    "the manifest",
    c("version", "extract_format", "provenance", "files", "clips", "findings")
  )
  format <- check_enum(doc$extract_format, "extract_format", FORMATS)
  check_object(
    p,
    "provenance",
    c("mode", "steward", "catalog", "holdings", "base_generation", "project")
  )
  if (!is_array(doc$files) || !length(doc$files)) {
    refuse("manifest", "files is not a non-empty array")
  }
  for (i in seq_along(doc$files)) {
    check_file(doc$files[[i]], sprintf("files[%d]", i - 1L), format)
  }
  doc
}

# ---- Output files -----------------------------------------------------------------

# The name a column is written under: its .dta alias, else its own.
written_name <- function(col) {
  if (is.null(col$stata_name)) col$name else col$stata_name
}

# One output file per manifest file, named by its stem and the format's extension
# (crates/DESIGN.md, "MONA order file"), its columns in manifest order, each
# written under its .dta alias or its own name.
plan_files <- function(manifest_files, format) {
  files <- lapply(manifest_files, function(f) {
    list(
      name = paste0(f$name, ".", format),
      schema = f$schema,
      table = f$table,
      columns = vapply(f$columns, `[[`, "", "name"),
      names = vapply(f$columns, written_name, ""),
      storage = lapply(f$columns, `[[`, "storage"),
      lopnr = vapply(f$columns, function(c) isTRUE(c$lopnr), NA),
      filter = f$row_filter
    )
  })
  # Case-insensitive: MONA's Windows file system would let one file overwrite
  # another that differs only in case.
  file_names <- tolower(vapply(files, `[[`, "", "name"))
  clash <- unique(file_names[duplicated(file_names)])
  if (length(clash)) {
    refuse(
      "manifest",
      "two output files share the name ",
      paste(clash, collapse = ", ")
    )
  }
  files
}

describe_filter <- function(f) {
  if (is.null(f)) {
    return("")
  }
  ranges <- vapply(
    f$ranges,
    function(r) {
      paste0(
        if (is.null(r$from)) "" else r$from,
        "..",
        if (is.null(r$to)) "" else r$to
      )
    },
    ""
  )
  paste0(f$column, " (", f$encoding, "): ", paste(ranges, collapse = " OR "))
}

# ---- SQL --------------------------------------------------------------------------

# `s` as an ASCII-only T-SQL nvarchar expression: printable ASCII runs as N'...'
# literals with quotes doubled, every other UTF-16 code unit as NCHAR(n). The
# expression is a value, so a name never becomes SQL text.
sql_text <- function(s) {
  points <- utf8ToInt(enc2utf8(s))
  if (anyNA(points)) {
    refuse("manifest", "a name is not valid UTF-8")
  }
  units <- unlist(lapply(points, function(p) {
    if (p <= 0xFFFF) {
      return(p)
    }
    p <- p - 0x10000
    c(0xD800 + p %/% 0x400, 0xDC00 + p %% 0x400)
  }))
  plain <- units >= 32 & units <= 126
  runs <- rle(plain)
  ends <- cumsum(runs$lengths)
  parts <- vapply(
    seq_along(ends),
    function(k) {
      u <- units[(ends[k] - runs$lengths[k] + 1L):ends[k]]
      if (runs$values[k]) {
        paste0("N'", gsub("'", "''", intToUtf8(u), fixed = TRUE), "'")
      } else {
        paste0("NCHAR(", u, ")", collapse = " + ")
      }
    },
    ""
  )
  paste0("CAST(", paste(parts, collapse = " + "), " AS nvarchar(128))")
}

# Run a query. A driver error names its SQLSTATE in the headline; the driver's
# full message, which can quote a row value (a conversion error names the value
# it failed on), is the refusal's detail.
query <- function(con, sql, what, params = list()) {
  tryCatch(
    if (length(params)) {
      DBI::dbGetQuery(con, sql, params = params)
    } else {
      DBI::dbGetQuery(con, sql)
    },
    error = function(e) driver_refusal(e, "extract", what, " failed")
  )
}

driver_refusal <- function(e, code, ...) {
  refuse(code, ..., sqlstate(e), detail = conditionMessage(e))
}

# The SQLSTATE from odbc's own state marker ("nanodbc/nanodbc.cpp:N: XXXXX: " or
# "ODBC failed with error XXXXX from "), first match only. A value would have to
# spell that marker to be mistaken for it, and would still yield five characters.
sqlstate <- function(e) {
  msg <- conditionMessage(e)
  markers <- regmatches(
    msg,
    gregexpr(
      "(?:nanodbc/nanodbc\\.cpp:[0-9]+: |ODBC failed with error )[0-9A-Z]{5}(?:: | from )",
      msg,
      perl = TRUE
    )
  )[[1]]
  states <- regmatches(
    markers,
    regexpr("[0-9A-Z]{5}(?=(?:: | from )$)", markers, perl = TRUE)
  )
  # The driver can report a success (00xxx) or warning (01xxx) state before the error.
  state <- states[!startsWith(states, "00") & !startsWith(states, "01")][1]
  if (is.na(state)) "" else paste0(" (SQLSTATE ", state, ")")
}

# The order's table in its named schema: exactly one, both names matched
# byte-exact.
resolve_table <- function(con, schema, table) {
  hits <- query(
    con,
    paste0(
      "SELECT TABLE_NAME AS table_name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ",
      sql_text(schema),
      " COLLATE Latin1_General_BIN2 AND TABLE_NAME = ",
      sql_text(table),
      " COLLATE Latin1_General_BIN2"
    ),
    paste0("looking up table ", schema, ".", table)
  )
  if (nrow(hits) != 1L) {
    refuse("resolve", "table ", schema, ".", table, " does not exist")
  }
  invisible()
}

# Each of `columns` on the resolved table: its DATA_TYPE,
# CHARACTER_MAXIMUM_LENGTH (-1 for max), NUMERIC_PRECISION and NUMERIC_SCALE. A
# missing column is refused.
resolve_columns <- function(con, schema, table, columns) {
  values <- paste0(
    "(",
    seq_along(columns),
    ", ",
    vapply(columns, sql_text, ""),
    ")",
    collapse = ", "
  )
  found <- query(
    con,
    paste0(
      "SELECT v.i AS i, c.DATA_TYPE AS data_type, ",
      "CAST(c.CHARACTER_MAXIMUM_LENGTH AS int) AS max_length, ",
      "CAST(c.NUMERIC_PRECISION AS int) AS precision, ",
      "CAST(c.NUMERIC_SCALE AS int) AS scale FROM (VALUES ",
      values,
      ") AS v(i, n) LEFT JOIN INFORMATION_SCHEMA.COLUMNS AS c ON c.TABLE_SCHEMA = ",
      sql_text(schema),
      " COLLATE Latin1_General_BIN2 AND c.TABLE_NAME = ",
      sql_text(table),
      " COLLATE Latin1_General_BIN2 AND c.COLUMN_NAME = v.n COLLATE Latin1_General_BIN2 ",
      "ORDER BY v.i"
    ),
    paste0("looking up the columns of ", table)
  )
  found <- found[order(found$i), ]
  missing <- columns[is.na(found$data_type)]
  if (length(missing)) {
    refuse(
      "resolve",
      "table ",
      schema,
      ".",
      table,
      " has no column ",
      paste(missing, collapse = ", ")
    )
  }
  list(
    type = tolower(found$data_type),
    max_length = found$max_length,
    precision = found$precision,
    scale = found$scale
  )
}

# Refuse, before any row is read, a column whose storage on the server differs
# from the storage the order expects: type, length, precision and scale exactly,
# a JSON null being SQL NULL and -1 max. A drifted column is not coerced: the
# order was made against other holdings. The refusal names metadata, never a
# value. The row-filter column has no expected storage; period_key checks it.
check_storage <- function(file, types) {
  spell <- function(type, length, precision, scale) {
    shown <- function(x) if (is.null(x) || is.na(x)) "null" else format(x)
    paste0(
      type,
      " (length ",
      shown(length),
      ", precision ",
      shown(precision),
      ", scale ",
      shown(scale),
      ")"
    )
  }
  same <- function(want, got) {
    if (is.null(want)) is.na(got) else !is.na(got) && want == got
  }
  for (j in seq_along(file$columns)) {
    s <- file$storage[[j]]
    ok <- identical(s$type, types$type[j]) &&
      same(s$length, types$max_length[j]) &&
      same(s$precision, types$precision[j]) &&
      same(s$scale, types$scale[j])
    if (!ok) {
      refuse(
        "resolve",
        file$name,
        ": column ",
        file$columns[j],
        " of ",
        file$schema,
        ".",
        file$table,
        " is stored as ",
        spell(
          types$type[j],
          types$max_length[j],
          types$precision[j],
          types$scale[j]
        ),
        " but the order expects ",
        spell(s$type, s$length, s$precision, s$scale)
      )
    }
  }
}

# The SQL types an SCB lopnummer column (the order's `lopnr`) may be stored as,
# which lopnr_digits() reads as int64 (#2648): integers, decimals, a
# double-precision float and text. lopnr_storage in
# crates/reg-readers/src/holdings.rs admits the same, so a steward learns of any
# other at build time. Single precision (real, float(1..24)) is refused: above
# 2^24 it has already merged distinct keys.
LOPNR_TYPES <- c(INT_TYPES, "bigint", "decimal", "numeric", "float", STRING_TYPES)

lopnr_storage_ok <- function(type, precision) {
  type %in% LOPNR_TYPES && !(type == "float" && !is.na(precision) && precision <= 24)
}

# Refuse, before any row is read, an SCB lopnummer column stored otherwise than
# LOPNR_TYPES admits (a date, a real, money): it is no key read exactly.
check_lopnr <- function(file, types) {
  for (j in which(file$lopnr)) {
    if (!lopnr_storage_ok(types$type[j], types$precision[j])) {
      refuse(
        "resolve",
        file$name,
        ": column ",
        file$columns[j],
        " is an SCB lopnummer, written as int64, but is stored as ",
        types$type[j]
      )
    }
  }
}

# The types the file's columns are written in: the SQL types, an SCB lopnummer
# column's as bigint, whatever its storage ("Types per format").
written_types <- function(file, types) {
  n <- length(file$columns)
  written <- lapply(types, `[`, seq_len(n))
  lopnr <- which(file$lopnr)
  written$type[lopnr] <- "bigint"
  written$max_length[lopnr] <- NA
  written$precision[lopnr] <- 19L
  written$scale[lopnr] <- 0L
  written
}

INT64_MAX <- "9223372036854775807"

# An SCB lopnummer column's values as int64 digits, which every writer takes as
# a bigint's: an integer as it is, a decimal or a double only when whole, text
# only when digits (an optional minus; a char or nchar column's trailing padding
# trimmed, nothing else), leading zeros dropped. Any other value, or one beyond int64, refuses the file with its count,
# never the value. `rows` is how many rows the file wrote before this chunk.
lopnr_digits <- function(x, type, column, what, rows) {
  if (!length(x)) {
    return(character())
  }
  if (is.double(x)) {
    # A double at or above 2^53 may already have rounded in storage.
    whole <- is.finite(x) & x == trunc(x) & abs(x) < DOUBLE_EXACT
    text <- rep(NA_character_, length(x))
    text[whole] <- sprintf("%.0f", x[whole])
  } else {
    text <- as.character(x)
    if (type %in% c("char", "nchar")) {
      text <- sub(" +$", "", text)
    }
    if (type %in% c("decimal", "numeric")) {
      text <- sub("\\.0*$", "", text)
    }
    whole <- grepl("^-?[0-9]+$", text)
    text[!whole] <- NA_character_
  }
  negative <- startsWith(text, "-")
  digits <- sub("^-?0*", "", text)
  digits[!is.na(digits) & !nzchar(digits)] <- "0"
  # Equal-length digit strings compare as numbers, in two exact halves.
  above <- function(d, limit) {
    high <- as.numeric(substr(d, 1L, 10L))
    low <- as.numeric(substr(d, 11L, 19L))
    high > as.numeric(substr(limit, 1L, 10L)) |
      (high == as.numeric(substr(limit, 1L, 10L)) & low > as.numeric(substr(limit, 11L, 19L)))
  }
  wide <- !is.na(digits) & nchar(digits) >= 19L
  beyond <- wide
  beyond[wide] <- nchar(digits[wide]) > 19L |
    above(digits[wide], ifelse(negative[wide], "9223372036854775808", INT64_MAX))
  bad <- (!is.na(x) & !whole) | beyond
  if (any(bad)) {
    refuse(
      "extract",
      what,
      ": column ",
      column,
      " is an SCB lopnummer, written as int64, but ",
      sum(bad),
      " of its values in rows ",
      format(rows + 1, scientific = FALSE),
      " to ",
      format(rows + length(x), scientific = FALSE),
      " are not whole numbers within int64"
    )
  }
  ifelse(is.na(digits), NA_character_, paste0(ifelse(negative & digits != "0", "-", ""), digits))
}

# Whether `name` is a variable name a .dta file (format 118) can hold: Stata's
# rule (stricter than ReadStat, which writes any non-ASCII name), which
# is_stata_name in crates/reg-core/src/manifest.rs repeats, so every name a
# dta order writes (a column's own, or its stata_name) passes this backstop. 1
# to 32 characters; a letter (\p{L}) or _ first, then letters, ASCII digits and
# _; not a reserved word, nor str and a digit.
stata_name_ok <- function(name) {
  reserved <- c(
    "_all",
    "_b",
    "byte",
    "_coef",
    "_cons",
    "double",
    "float",
    "if",
    "in",
    "int",
    "long",
    "_n",
    "_N",
    "_pi",
    "_pred",
    "_rc",
    "_skip",
    "strL",
    "using",
    "with"
  )
  grepl("^[\\p{L}_][\\p{L}0-9_]{0,31}$", name, perl = TRUE) &&
    !(name %in% reserved) &&
    !grepl("^str[0-9]", name)
}

# Refuse, before any row is read, a column of `file` the order's format cannot
# hold: a SQL type outside MAPPED_TYPES (.dta and .parquet), or a name it is
# written under (its stata_name, else its own) that is not a Stata variable name
# and a label too long for its full name (.dta). A CSV holds anything as text,
# except a time of day or a date-time with an offset: those are refused in every
# format, since the driver returns them in forms the runner does not read.
check_format <- function(file, types, format) {
  odd <- which(types$type[seq_along(file$columns)] %in% c("time", "datetimeoffset"))
  if (length(odd)) {
    j <- odd[1]
    refuse(
      "resolve",
      "column ",
      file$columns[j],
      " of ",
      file$table,
      " is SQL type ",
      types$type[j],
      ", which this runner does not map to any format"
    )
  }
  if (format == "csv") {
    return(invisible())
  }
  for (j in seq_along(file$columns)) {
    column <- file$columns[j]
    if (!(types$type[j] %in% MAPPED_TYPES)) {
      refuse(
        "resolve",
        "column ",
        column,
        " of ",
        file$table,
        " is SQL type ",
        types$type[j],
        ", which this runner does not map to .",
        format
      )
    }
    if (format == "dta" && !stata_name_ok(file$names[j])) {
      refuse(
        "resolve",
        "column ",
        column,
        " of ",
        file$table,
        " would be written as ",
        file$names[j],
        ", which is not a Stata variable name (1 to 32 letters, digits or _, ",
        "not starting with a digit, not a reserved word)"
      )
    }
    # A renamed column's label is its full name, and Stata shows at most 80
    # characters of a label (haven cuts one past 320 UTF-8 bytes without a word).
    if (
      format == "dta" &&
        file$names[j] != column &&
        nchar(column) > 80L
    ) {
      refuse(
        "resolve",
        "column ",
        column,
        " of ",
        file$table,
        " is longer than a Stata variable label holds (80 characters)"
      )
    }
  }
}

# A selected column as T-SQL text around `@col`. Text the client would fetch in
# its code page (char, varchar, text) is fetched as nvarchar, so it arrives as
# UTF-8 whatever MONA's code page; decimal and numeric are fetched as text, so
# an 18-digit id never passes through a double. Dates and date-times are fetched
# as ISO text in an explicit style (23: YYYY-MM-DD; 121: YYYY-MM-DD HH:MM:SS and
# the type's fractional digits, none for datetime2(0)), so every driver returns
# them alike: the legacy "SQL Server" driver returns datetime2 and date as text
# of its own, and a plain CAST of datetime gives "Jan  1 2026  4:33PM".
select_expr <- function(type, max_length) {
  if (type == "date") {
    return("CONVERT(nvarchar(10), @col, 23) AS @col")
  }
  if (type %in% TIME_TYPES) {
    return("CONVERT(nvarchar(27), @col, 121) AS @col")
  }
  if (type %in% c("char", "varchar", "text")) {
    size <- if (is.na(max_length) || max_length < 0L || max_length > 4000L) {
      "max"
    } else {
      max(max_length, 1L)
    }
    return(paste0("CAST(@col AS nvarchar(", size, ")) AS @col"))
  }
  if (type %in% c("decimal", "numeric")) {
    return("CAST(@col AS nvarchar(64)) AS @col")
  }
  "@col"
}

# The period value a filter compares, as T-SQL text around `@col` (the quoted
# column), and the type its bounds take. year, yyyymm and yyyymmdd read the
# column as an integer (TRY_CAST, so an unparseable value is NULL and excluded),
# and a yyyymmdd that is no calendar day or a yyyymm that is no month is NULL too;
# date reads it as a day, so a datetime keeps every time of its day. Blank text
# is NULL first: SQL Server casts '' to 1900-01-01 or 0, which would pass a range
# or escape the rows_without_period count. The filter and that count read this
# one value.
period_key <- function(encoding, data_type, table, column) {
  text <- data_type %in% TEXT_TYPES
  value <- if (text) "NULLIF(LTRIM(RTRIM(@col)), '''')" else "@col"
  if (encoding == "date" && data_type %in% c(DATE_TYPES, TEXT_TYPES)) {
    return(list(expr = paste0("TRY_CAST(", value, " AS date)"), type = "date"))
  }
  if (encoding != "date" && data_type %in% c(NUMBER_TYPES, TEXT_TYPES)) {
    n <- paste0("TRY_CAST(", value, " AS bigint)")
    # A yyyymm or yyyymmdd checked as its digits: text as written (exactly six
    # or eight digits, so '0201801' is no month and '099912' is one), a number
    # by its range. Style 112 also reads 4 digits as a year and 6 as yymmdd, so
    # the digit count comes before the calendar check.
    digits <- function(width, low, high, day) {
      if (text) {
        paste0(
          "LEN(", value, ") = ", width, " AND ", value,
          " NOT LIKE ''%[^0-9]%'' AND TRY_CONVERT(date, ", value, day, ", 112) IS NOT NULL"
        )
      } else {
        paste0(
          n, " BETWEEN ", low, " AND ", high, " AND TRY_CONVERT(date, CAST(", n,
          " AS varchar(20))", day, ", 112) IS NOT NULL"
        )
      }
    }
    valid <- c(
      # A fractional number (20180101.5) is no period: bigint would truncate it.
      # Compared in the column's own type, so a decimal is tested exactly.
      if (!data_type %in% c(TEXT_TYPES, INT_TYPES, "bigint")) "@col = ROUND(@col, 0, 1)",
      switch(
        encoding,
        year = NULL,
        yyyymm = digits(6L, 100001L, 999912L, " + ''01''"),
        yyyymmdd = digits(8L, 10000101L, 99991231L, "")
      )
    )
    expr <- if (is.null(valid)) {
      n
    } else {
      paste0("CASE WHEN ", paste(valid, collapse = " AND "), " THEN ", n, " END")
    }
    return(list(expr = expr, type = "bigint"))
  }
  refuse(
    "resolve",
    "period column ",
    column,
    " of ",
    table,
    " is ",
    data_type,
    ", which encoding ",
    encoding,
    " cannot read"
  )
}

# An ISO day as the bound of `encoding`: the day for date, else the integer the
# encoding spells (2019, 201903, 20190315). Truncating to the encoding widens a
# finer bound to its whole year or month.
bound <- function(day, encoding) {
  if (encoding == "date") {
    return(day)
  }
  digits <- gsub("-", "", day, fixed = TRUE)
  width <- c(year = 4L, yyyymm = 6L, yyyymmdd = 8L)[[encoding]]
  as.integer(substr(digits, 1L, width))
}

# A name as a T-SQL expression quoting it as an identifier.
sql_quoted <- function(name) paste0("QUOTENAME(", sql_text(name), ")")

# A template's fixed text as N'...' literals around the quoted column at each
# @col; the templates are this script's constants, never manifest text.
sql_splice <- function(template, col) {
  parts <- strsplit(paste0(template, " "), "@col", fixed = TRUE)[[1]]
  parts[length(parts)] <- sub(" $", "", parts[length(parts)])
  paste(paste0("N'", parts, "'"), collapse = paste0(" + ", col, " + "))
}

# The filter's period value as T-SQL text and the type its bounds take. The
# filter column follows the file's columns in `types` (extract_order resolves it
# there).
filter_key <- function(file, types) {
  f <- file$filter
  key <- period_key(
    f$encoding,
    types$type[length(file$columns) + 1L],
    file$table,
    f$column
  )
  list(sql = sql_splice(key$expr, sql_quoted(f$column)), type = key$type)
}

sql_from <- function(file) {
  paste0(
    " + N' FROM ' + ",
    sql_quoted(file$schema),
    " + N'.' + ",
    sql_quoted(file$table)
  )
}

# `select` run by sp_executesql with `declared` parameters bound from R.
sql_batch <- function(select, declared, params) {
  exec <- if (length(params)) {
    paste0(
      "EXEC sp_executesql @sql, N'",
      paste(declared, collapse = ", "),
      "', ",
      paste0(sub(" .*", "", declared), " = ?", collapse = ", "),
      ";"
    )
  } else {
    "EXEC sp_executesql @sql;"
  }
  list(
    sql = paste0(
      "SET NOCOUNT ON; DECLARE @sql nvarchar(max) = CAST(N'' AS nvarchar(max)) + ",
      select,
      "; ",
      exec
    ),
    params = params
  )
}

# The file's row filter as T-SQL text to append to a SELECT (empty for an
# unfiltered file), with the declared parameters and the bounds bound from R. The
# extract and its pre-count share it, so both select the same rows.
filter_where <- function(file, types) {
  where <- ""
  declared <- character()
  params <- list()
  f <- file$filter
  if (!is.null(f)) {
    key <- filter_key(file, types)
    terms <- character()
    for (r in f$ranges) {
      parts <- character()
      for (side in c("from", "to")) {
        if (is.null(r[[side]])) {
          next
        }
        name <- sprintf("@p%d", length(params) + 1L)
        params[[length(params) + 1L]] <- bound(r[[side]], f$encoding)
        declared <- c(declared, paste(name, key$type))
        parts <- c(
          parts,
          paste0(
            key$sql,
            " + N' ",
            if (side == "from") ">=" else "<=",
            " ",
            name,
            "'"
          )
        )
      }
      if (!length(parts)) {
        parts <- paste0(key$sql, " + N' IS NOT NULL'")
      }
      terms <- c(
        terms,
        paste0("N'(' + ", paste(parts, collapse = " + N' AND ' + "), " + N')'")
      )
    }
    where <- paste0(
      " + N' WHERE ' + ",
      paste(terms, collapse = " + N' OR ' + ")
    )
  }
  list(sql = where, declared = declared, params = params)
}

# One batch per file: the SELECT is assembled server-side from QUOTENAME()d
# identifiers and run by sp_executesql with the filter bounds as parameters,
# bound from R.
# Whether a column is fetched as a long value: nvarchar(max) or ntext, and in a
# CSV order xml, image and varbinary(max), which the legacy driver exposes as
# ntext and image. That driver reads such a column only after every other column
# of the row, so a SELECT with one before an ordinary column fails with SQLSTATE
# 07009, Invalid Descriptor Index (P1105 on MONA, 2026-11-24). A max length reads
# as -1 (CHARACTER_MAXIMUM_LENGTH), text, ntext and image as 2^31 - 1.
fetched_long <- function(type, max_length) {
  unbounded <- is.na(max_length) | max_length < 0L
  type %in% c("text", "ntext", "xml", "image") |
    (type %in% STRING_TYPES & (unbounded | max_length > 4000L)) |
    (type == "varbinary" & unbounded)
}

# The extract's SELECT, its long columns last (fetched_long); `fetched` is the
# order's column positions in SELECT order, which extract_file() undoes.
select_batch <- function(file, types) {
  n <- length(file$columns)
  long <- fetched_long(types$type[seq_len(n)], types$max_length[seq_len(n)])
  fetched <- c(which(!long), which(long))
  selected <- vapply(
    seq_along(file$columns),
    function(j) {
      # A marked int is fetched as a bigint, as text: R's integer reads
      # -2147483648 as NA, which would lose that key.
      expr <- if (file$lopnr[j] && types$type[j] %in% INT_TYPES) {
        "CAST(@col AS bigint) AS @col"
      } else {
        select_expr(types$type[j], types$max_length[j])
      }
      sql_splice(expr, sql_quoted(file$columns[j]))
    },
    ""
  )
  where <- filter_where(file, types)
  batch <- sql_batch(
    paste0(
      "N'SELECT ' + ",
      paste(selected[fetched], collapse = " + N', ' + "),
      sql_from(file),
      where$sql
    ),
    where$declared,
    where$params
  )
  batch$fetched <- fetched
  batch
}

# The count of the rows the extract of `file` will write: its own filter, one
# COUNT_BIG, so one scan of the table (or the view's query).
count_batch <- function(file, types) {
  where <- filter_where(file, types)
  sql_batch(
    paste0(
      "N'SELECT CAST(COUNT_BIG(*) AS float) AS n'",
      sql_from(file),
      where$sql
    ),
    where$declared,
    where$params
  )
}

# The count of a filtered file's rows that pass no range for a NULL or
# unreadable period value: an aggregate the run log reports.
null_period_batch <- function(file, types) {
  sql_batch(
    paste0(
      "N'SELECT CAST(COUNT_BIG(*) AS float) AS n'",
      sql_from(file),
      " + N' WHERE ' + ",
      filter_key(file, types)$sql,
      " + N' IS NULL'"
    ),
    character(),
    list()
  )
}

# ---- CSV --------------------------------------------------------------------------

# A text field, quoted only when it must be: an empty string (so it differs
# from NULL, which is an empty field), a quote, comma or line break, or edge
# whitespace.
csv_text <- function(x) {
  x <- enc2utf8(as.character(x))
  quote <- !is.na(x) & (!nzchar(x) | grepl("[\",\r\n]|^\\s|\\s$", x))
  x[quote] <- paste0("\"", gsub("\"", "\"\"", x[quote], fixed = TRUE), "\"")
  x
}

# ---- Dates and times ------------------------------------------------------------

# A date-time as select_expr fetches it: whole seconds, then 0 to 7 fractional
# digits.
TIME_TEXT <- "^([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})(\\.([0-9]{1,7}))?$"

# Counts, per column, the values whose fraction a format cut: add(column, n)
# while writing, counts() for the file's console lines.
cut_counter <- function() {
  state <- new.env()
  state$n <- numeric()
  list(
    add = function(column, n) {
      if (n > 0) {
        state$n[column] <- n + if (is.na(state$n[column])) 0 else state$n[column]
      }
    },
    counts = function() state$n
  )
}

# A fetched date-time column as its whole seconds and its fraction cut to
# `digits` places (NULL stays NA). The fraction is truncated, never rounded, so
# a time never moves into the next second or day; `cut(column, n)` is told how
# many values lost a non-zero digit. That limit is the format's: datetime2 holds
# 100 ns, Parquet's timestamp a microsecond, Stata's %tc a millisecond.
time_parts <- function(x, digits, column, what, cut = function(column, n) NULL) {
  bad <- !is.na(x) & !grepl(TIME_TEXT, x)
  if (any(bad)) {
    refuse("extract", what, ": column ", column, " returned a date-time in an unexpected form")
  }
  fraction <- substr(paste0(sub(TIME_TEXT, "\\3", x), "0000000"), 1L, 7L)
  cut(column, sum(!is.na(x) & substr(fraction, digits + 1L, 7L) != strrep("0", 7L - digits)))
  fraction <- substr(fraction, 1L, digits)
  fraction[is.na(x)] <- NA_character_
  list(seconds = sub(TIME_TEXT, "\\1", x), fraction = fraction)
}

# A fetched date column as Date, or a refusal if a value does not read as one.
parse_date <- function(x, column, what) {
  day <- as.Date(x, format = "%Y-%m-%d", optional = TRUE)
  if (any(is.na(day) & !is.na(x))) {
    refuse("extract", what, ": column ", column, " returned a date in an unexpected form")
  }
  day
}

# Seconds from Stata's epoch (1960-01-01) to R's (1970-01-01).
STATA_EPOCH_SECONDS <- 315619200

# A fetched date-time column as Stata %tc: milliseconds since 1960-01-01 UTC,
# computed in whole numbers so every value is exact (a POSIXct's fractional
# seconds would not be).
parse_time_ms <- function(x, column, what, cut) {
  parts <- time_parts(x, 3L, column, what, cut)
  at <- as.POSIXct(parts$seconds, tz = "UTC", format = "%Y-%m-%d %H:%M:%S")
  if (any(is.na(at) & !is.na(x))) {
    refuse("extract", what, ": column ", column, " returned a date-time in an unexpected form")
  }
  ms <- (as.numeric(at) + STATA_EPOCH_SECONDS) * 1000 + as.numeric(parts$fraction)
  attr(ms, "format.stata") <- "%tc"
  ms
}

# A fetched date-time column as CSV text: whole seconds, then the fraction
# without trailing zeros, if any is left.
csv_time <- function(x, column, what) {
  parts <- time_parts(x, 7L, column, what)
  fraction <- sub("0+$", "", parts$fraction)
  ifelse(nzchar(fraction) & !is.na(x), paste0(parts$seconds, ".", fraction), parts$seconds)
}

# One CSV field per value: numbers in fixed notation to 15 significant digits
# (no 1e+15 for a numeric id), text (dates and times as fetched, csv_time) as
# UTF-8, NULL as an empty field.
csv_field <- function(x) {
  out <- if (is.double(x)) {
    formatC(x, digits = 15L, format = "fg", width = 1L)
  } else if (is.character(x) || is.factor(x)) {
    csv_text(x)
  } else if (is.logical(x)) {
    # bit, as the 0/1 the catalog's value codes use
    as.character(as.integer(x))
  } else {
    as.character(x)
  }
  out[is.na(x)] <- ""
  out
}

# Rows of a data frame as CSV lines, LF-terminated, as UTF-8 bytes.
csv_bytes <- function(df) {
  fields <- lapply(df, csv_field)
  charToRaw(enc2utf8(paste0(
    do.call(paste, c(unname(fields), sep = ",")),
    "\n",
    collapse = ""
  )))
}

csv_header <- function(names) {
  charToRaw(enc2utf8(paste0(paste(csv_text(names), collapse = ","), "\n")))
}

write_csv <- function(df, path) {
  sink <- file(path, open = "wb")
  on.exit(close(sink))
  writeBin(csv_header(names(df)), sink)
  if (nrow(df)) writeBin(csv_bytes(df), sink)
}

# ---- Writers --------------------------------------------------------------------

# A text vector's bytes per value, a missing value as 0.
text_bytes <- function(x) {
  b <- as.numeric(nchar(x, type = "bytes"))
  b[is.na(b)] <- 0
  b
}

# The row ranges of an n-row chunk to write one at a time: each as long as it can
# be while every vector in `bytes` (bytes per row) sums to at most SLICE_BYTES
# within it. One range when the chunk is small enough, as it always is at the
# default fetch size. A single row over the limit is refused.
row_slices <- function(bytes, n, what) {
  if (!n) {
    return(list())
  }
  if (all(vapply(bytes, sum, 0) <= SLICE_BYTES)) {
    return(list(seq_len(n)))
  }
  totals <- lapply(bytes, cumsum)
  slices <- list()
  start <- 1L
  while (start <= n) {
    end <- min(vapply(totals, function(t) {
      before <- if (start > 1L) t[start - 1L] else 0
      findInterval(before + SLICE_BYTES, t)
    }, 0))
    if (end < start) {
      refuse("extract", what, ": one row holds more than 1 GB of text")
    }
    end <- min(end, n)
    slices[[length(slices) + 1L]] <- start:end
    start <- end + 1L
  }
  slices
}

# A writer for one output file: write(chunk) takes each fetched chunk (the first
# may have no rows), finish() completes the file, release() frees what an
# unfinished file holds (its .partial file is then removed). `types` are
# the file's columns' resolve_columns() facts, in column order; `names` are the
# names they are written under, which only a .dta order renames (plan_files).
csv_writer <- function(path, columns, types, names) {
  what <- paste0("writing ", sub("\\.partial$", "", basename(path)))
  times <- which(types$type %in% TIME_TYPES)
  sink <- file(path, open = "wb")
  writeBin(csv_header(columns), sink)
  state <- new.env()
  state$open <- TRUE
  list(
    write = function(chunk) {
      if (!nrow(chunk)) {
        return(invisible())
      }
      for (j in times) {
        chunk[[j]] <- csv_time(chunk[[j]], columns[j], what)
      }
      # One R string holds under 2 GB: the chunk is written in row slices. A
      # number's field is counted as 32 bytes, more than it takes.
      row <- Reduce(`+`, lapply(chunk, function(x) {
        if (is.character(x)) text_bytes(x) + 3 else rep(32, length(x))
      }))
      slices <- row_slices(list(row), nrow(chunk), what)
      for (rows in slices) {
        part <- if (length(slices) == 1L) chunk else chunk[rows, , drop = FALSE]
        writeBin(csv_bytes(part), sink)
      }
    },
    finish = function() {
      close(sink)
      state$open <- FALSE
    },
    release = function() if (state$open) close(sink),
    cut = function() numeric()
  )
}

# The arrow type each SQL type is written as: the SQL type kept (int64 ids,
# decimal128 at the column's own precision and scale, days and UTC instants).
arrow_type <- function(type, precision, scale) {
  switch(
    type,
    tinyint = ,
    smallint = ,
    int = arrow::int32(),
    bigint = arrow::int64(),
    decimal = ,
    numeric = arrow::decimal128(precision, scale),
    float = arrow::float64(),
    real = arrow::float32(),
    bit = arrow::boolean(),
    date = arrow::date32(),
    datetime = ,
    datetime2 = ,
    smalldatetime = arrow::timestamp("us", "UTC"),
    arrow::utf8()
  )
}

# Streams one row group per chunk, so a file is never held whole in memory.
parquet_writer <- function(path, columns, types, names) {
  what <- paste0("writing ", sub("\\.partial$", "", basename(path)))
  cuts <- cut_counter()
  schema <- arrow::schema(stats::setNames(
    lapply(seq_along(columns), function(j) {
      arrow_type(types$type[j], types$precision[j], types$scale[j])
    }),
    columns
  ))
  sink <- arrow::FileOutputStream$create(path)
  writer <- tryCatch(
    arrow::ParquetFileWriter$create(
      schema,
      sink,
      properties = arrow::ParquetWriterProperties$create(columns)
    ),
    error = function(e) {
      # The constructor's error is the one reported, whatever the close does.
      report(sink$close())
      stop(e)
    }
  )
  state <- new.env()
  state$open <- TRUE
  # The sink closes even when the writer's close fails, so the failure path can
  # remove a partial file (an open handle blocks that on Windows).
  close_all <- function() {
    if (state$open) {
      state$open <- FALSE
      tryCatch(writer$Close(), finally = sink$close())
    }
  }
  write_rows <- function(chunk) {
    arrays <- lapply(seq_along(chunk), function(j) {
      x <- chunk[[j]]
      if (types$type[j] %in% TIME_TYPES) {
        # Read as a time of no zone, then marked UTC: arrow refuses to read
        # text without an offset straight into a UTC timestamp.
        parts <- time_parts(x, 6L, columns[j], what, cuts$add)
        text <- ifelse(is.na(x), NA_character_, paste0(parts$seconds, ".", parts$fraction))
        return(arrow::Array$create(text)$cast(arrow::timestamp("us"))$cast(schema[[j]]$type))
      }
      # bigint, decimal and date arrive as text: arrow parses them exactly, or
      # fails.
      arrow::Array$create(x)$cast(schema[[j]]$type)
    })
    writer$WriteTable(
      arrow::Table$create(
        !!!stats::setNames(arrays, columns),
        schema = schema
      ),
      chunk_size = config$fetch_rows
    )
  }
  list(
    write = function(chunk) {
      if (!nrow(chunk)) {
        return(invisible())
      }
      # An arrow string array holds under 2 GB of text: a chunk with more in
      # one text column is written in row slices, a row group each.
      text <- Filter(Negate(is.null), lapply(chunk, function(x) {
        if (is.character(x)) text_bytes(x)
      }))
      slices <- row_slices(text, nrow(chunk), what)
      for (rows in slices) {
        write_rows(if (length(slices) == 1L) chunk else chunk[rows, , drop = FALSE])
      }
    },
    finish = close_all,
    cut = cuts$counts,
    release = close_all
  )
}

# One column as .dta storage (reg_mona_runner/DESIGN.md, "Types per format").
# A value Stata cannot hold exactly is refused, never rounded or cut.
dta_column <- function(x, type, precision, column, what, cut) {
  beyond <- function(limit) {
    refuse(
      "extract",
      what,
      ": column ",
      column,
      " holds a value beyond ",
      limit,
      ", which a .dta file cannot hold exactly: order it as parquet or csv"
    )
  }
  if (type %in% INT_TYPES) {
    if (any(x > STATA_LONG_MAX, na.rm = TRUE)) {
      beyond(paste0("Stata's long range (", STATA_LONG_MAX, ")"))
    }
    return(x)
  }
  if (
    type == "bigint" ||
      (type %in% c("decimal", "numeric") && precision <= DTA_DOUBLE_DIGITS)
  ) {
    # Text in, so the check sees the parsed value: one at or above 2^53 may have
    # rounded on parse.
    x <- as.numeric(x)
    if (any(abs(x) >= DOUBLE_EXACT, na.rm = TRUE)) {
      beyond("2^53")
    }
    # Stata's default %10.0g would show a 12-digit id in exponent notation.
    attr(x, "format.stata") <- "%20.0g"
    return(x)
  }
  if (type == "bit") {
    return(as.integer(x))
  }
  if (type == "date") {
    return(parse_date(x, column, what))
  }
  if (type %in% TIME_TYPES) {
    return(parse_time_ms(x, column, what, cut))
  }
  if (
    type %in% c("float", "real") && any(abs(x) > STATA_DOUBLE_MAX, na.rm = TRUE)
  ) {
    beyond("Stata's double range")
  }
  if (is.character(x)) {
    # Stata has no missing string: NULL is "", as Stata itself reads it (haven
    # also fails mid-write on an NA in a long strL column).
    x[is.na(x)] <- ""
  }
  x
}

# Collects the converted chunks and writes the file once: haven cannot append.
# Each column is written under its `names` entry (its manifest stata_name, or
# its own name); a renamed column's own name is its variable label.
# simplify: the whole file is in memory; a LISA year file is about 8M rows x
# 100 columns, so stream (or split) if a real .dta order exhausts MONA's RAM.
dta_writer <- function(path, columns, types, names) {
  what <- paste0("writing ", sub("\\.partial$", "", basename(path)))
  cuts <- cut_counter()
  state <- new.env()
  state$chunks <- list()
  list(
    write = function(chunk) {
      state$chunks[[length(state$chunks) + 1L]] <- lapply(
        seq_along(chunk),
        function(j) {
          dta_column(
            chunk[[j]],
            types$type[j],
            types$precision[j],
            columns[j],
            what,
            cuts$add
          )
        }
      )
    },
    finish = function() {
      data <- lapply(seq_along(columns), function(j) {
        parts <- lapply(state$chunks, `[[`, j)
        # Drop the chunks' copy as each column is combined: about one copy of
        # the data is held at a time, not two.
        for (k in seq_along(state$chunks)) {
          state$chunks[[k]][j] <- list(NULL)
        }
        x <- do.call(c, parts)
        # c() keeps neither the display format nor a POSIXct's time zone.
        attr(x, "format.stata") <- attr(parts[[1]], "format.stata")
        if (inherits(x, "POSIXct")) {
          attr(x, "tzone") <- "UTC"
        }
        if (names[j] != columns[j]) {
          attr(x, "label") <- columns[j]
        }
        x
      })
      state$chunks <- list()
      haven::write_dta(
        structure(
          stats::setNames(data, names),
          class = "data.frame",
          row.names = .set_row_names(length(data[[1]]))
        ),
        path,
        version = 14
      )
    },
    release = function() {
      state$chunks <- list()
    },
    cut = cuts$counts
  )
}

WRITERS <- list(dta = dta_writer, parquet = parquet_writer, csv = csv_writer)

# `progress(rows)` is called after each fetched chunk with the file's rows so far.
# Returns the rows written and, per column, how many values the format cut.
# The file is written as <name>.partial and takes its own name only once complete,
# so a run that is stopped leaves nothing that passes for a finished file.
extract_file <- function(con, file, types, format, out, progress) {
  batch <- select_batch(file, types)
  n <- length(file$columns)
  path <- file.path(out, paste0(file$name, PARTIAL))
  column_types <- written_types(file, types)
  writer <- WRITERS[[format]](path, file$columns, column_types, file$names)
  on.exit(writer$release(), add = TRUE)
  what <- paste0("extracting ", file$name)
  res <- tryCatch(
    if (length(batch$params)) {
      DBI::dbSendQuery(con, batch$sql, params = batch$params)
    } else {
      DBI::dbSendQuery(con, batch$sql)
    },
    error = function(e) driver_refusal(e, "extract", what, " failed")
  )
  # odbc may already have dropped the result when a fetch failed, and clearing it
  # again warns: a warning here would replace the fetch's own error.
  on.exit(if (DBI::dbIsValid(res)) DBI::dbClearResult(res), add = TRUE, after = FALSE)
  rows <- 0
  repeat {
    chunk <- tryCatch(
      DBI::dbFetch(res, n = config$fetch_rows),
      error = function(e) driver_refusal(e, "extract", what, " failed")
    )
    if (ncol(chunk) != n) {
      refuse("extract", what, " returned an unexpected column count")
    }
    # Back to the order's column order.
    chunk <- chunk[order(batch$fetched)]
    for (j in which(file$lopnr)) {
      chunk[[j]] <- lopnr_digits(chunk[[j]], types$type[j], file$columns[j], what, rows)
    }
    for (j in seq_along(chunk)) {
      x <- chunk[[j]]
      if (is.character(x) && !all(validUTF8(x[!is.na(x)]))) {
        refuse(
          "extract",
          what,
          ": column ",
          file$columns[j],
          " holds text that is not UTF-8"
        )
      }
    }
    writer$write(chunk)
    rows <- rows + nrow(chunk)
    progress(rows)
    if (DBI::dbHasCompleted(res)) break
  }
  writer$finish()
  if (!rename(path, file.path(out, file$name))) {
    refuse("extract", "cannot rename ", basename(path), " to ", file$name)
  }
  list(rows = rows, cut = writer$cut())
}

# ---- Pre-count, size and progress ---------------------------------------------------

# Seconds a file runs before its first progress line, and between its progress
# lines: a long file shows it is moving, a short one prints only its closing line.
PROGRESS_SECONDS <- 300

# Written Parquet bytes per byte of written_width()'s uncompressed estimate: one
# factor, from P1105 on MONA (2026-11-24: 136 files, 33 GB written against 300 GB
# estimated). Per-type factors (integers 0.22, date-times 0.86, strings 0.056;
# decimals not identified) did not bring single files within 2x either, so one
# factor it is: the order total is what it is for. simplify: calibrated on one
# project, where a file's actual size ran 0.4x to 5.7x its estimate (5th to 95th
# percentile); recalibrate if an order's total is off by more than about 2x.
# .dta and CSV are written uncompressed: their factor is 1.
PARQUET_DISK_FACTOR <- 0.11

# Roughly how many bytes one value of a column takes in the written file: the
# written type's width ("Types per format"; .csv: its usual text plus a comma),
# a string at its declared length. NA where no width is known: a string with no
# declared length (max, text, ntext) and, in a CSV, a type the table does not list.
written_width <- function(type, max_length, precision, format) {
  if (type %in% STRING_TYPES) {
    if (type %in% c("text", "ntext") || is.na(max_length) || max_length < 0L) {
      return(NA_real_)
    }
    return(max_length + (format == "csv"))
  }
  wide_decimal <- type %in%
    c("decimal", "numeric") &&
    precision > DTA_DOUBLE_DIGITS
  width <- switch(
    format,
    dta = switch(
      type,
      tinyint = ,
      smallint = ,
      int = ,
      bit = 4,
      decimal = ,
      numeric = if (wide_decimal) precision + 2 else 8,
      bigint = ,
      float = ,
      real = ,
      date = ,
      datetime = ,
      datetime2 = ,
      smalldatetime = 8,
      NA
    ),
    parquet = switch(
      type,
      tinyint = ,
      smallint = ,
      int = ,
      real = ,
      date = 4,
      bigint = ,
      float = ,
      datetime = ,
      datetime2 = ,
      smalldatetime = 8,
      decimal = ,
      numeric = 16,
      bit = 1,
      NA
    ),
    csv = switch(
      type,
      tinyint = 4,
      smallint = 7,
      int = 12,
      bigint = 21,
      decimal = ,
      numeric = precision + 3,
      float = ,
      real = 18,
      bit = 2,
      date = 11,
      # Style 121 text plus the comma: datetime2(7) has seven fractional
      # digits, datetime three, smalldatetime's are zeros and dropped.
      datetime2 = 28,
      datetime = 24,
      smalldatetime = 20,
      NA
    )
  )
  as.numeric(width)
}

# decimal.mark is explicit: under options(OutDec = ","), a Swedish setup's
# choice, format() warns that the marks clash, and a warning fails an order.
files_text <- function(n) paste(n, if (n == 1) "file" else "files")

with_commas <- function(n) {
  format(
    n,
    big.mark = ",",
    decimal.mark = ".",
    scientific = FALSE,
    trim = TRUE
  )
}

# The pre-count's and progress's console lines. A warning or error while one is
# formatted (an unexpected locale, say) drops the line, or returns `fallback`,
# instead of reaching the order's handlers: the report never fails an order.
report <- function(expr, fallback = NULL) {
  tryCatch(
    expr,
    error = function(e) fallback,
    warning = function(w) fallback
  )
}

# say(), its arguments formatted inside report(): TRUE when the line printed.
note <- function(...) {
  report(
    {
      say(...)
      TRUE
    },
    FALSE
  )
}

# Bytes at two significant figures, in SI units (1 GB = 10^9 bytes).
bytes_text <- function(n) {
  units <- c("bytes", "KB", "MB", "GB", "TB")
  # Rounded first, so 999,600 bytes is 1 MB, not 1000 KB.
  n <- signif(n, 2)
  k <- min(max(floor(log10(max(n, 1)) / 3), 0), length(units) - 1)
  paste(
    format(n / 1000^k, decimal.mark = ".", scientific = FALSE, trim = TRUE),
    units[k + 1]
  )
}

# A duration as h:mm.
clock_text <- function(seconds) {
  m <- round(seconds / 60)
  sprintf("%d:%02d", m %/% 60, m %% 60)
}

seconds_since <- function(t) as.numeric(difftime(Sys.time(), t, units = "secs"))

# A column's SQL type as text, with its declared length where it has one.
type_text <- function(type, max_length) {
  if (!type %in% STRING_TYPES || type %in% c("text", "ntext")) {
    return(type)
  }
  paste0(type, "(", if (is.na(max_length) || max_length < 0L) "max" else max_length, ")")
}

# Counts the rows each file's extract will write, one COUNT_BIG per file with the
# file's own filter, before any file is written, and estimates each file's size.
# Information only: a count that fails leaves the file not counted and the
# extract runs as it would have without it. Returns each file's count (NA when
# not counted) and size, its columns of no known width, the failed counts and
# how long counting took; print_count() prints them.
precount <- function(con, files, resolved, format) {
  started <- Sys.time()
  counts <- rep(NA_real_, length(files))
  bytes <- rep(NA_real_, length(files))
  unsized <- character(length(files))
  failed <- list()
  fail <- function(f, message, detail) {
    failed[[length(failed) + 1L]] <<- list(name = f$name, message = message, detail = detail)
    NA_real_
  }
  for (i in seq_along(files)) {
    f <- files[[i]]
    types <- resolved[[i]]
    # The count's own failure, R warnings included, is caught here, so it never
    # reaches the order's handlers and never fails an order.
    counts[i] <- tryCatch(
      {
        batch <- count_batch(f, types)
        n <- as.numeric(
          query(
            con,
            batch$sql,
            paste0("counting ", f$name),
            batch$params
          )$n
        )
        if (length(n) == 1L) n else NA_real_
      },
      runner_refusal = function(e) fail(f, conditionMessage(e), e$detail),
      error = function(e) fail(f, "the count failed", conditionMessage(e)),
      warning = function(w) fail(f, "the count raised an R warning", conditionMessage(w))
    )
    n <- length(f$columns)
    written <- written_types(f, types)
    widths <- vapply(
      seq_len(n),
      function(j) {
        written_width(
          written$type[j],
          written$max_length[j],
          written$precision[j],
          format
        )
      },
      0
    )
    gaps <- which(is.na(widths))
    if (length(gaps)) {
      unsized[i] <- paste0(
        f$columns[gaps],
        " (",
        vapply(gaps, function(j) type_text(types$type[j], types$max_length[j]), ""),
        ")",
        collapse = ", "
      )
    }
    # No column of known width: the size is unknown, not zero.
    bytes[i] <- if (length(gaps) == n) {
      NA_real_
    } else {
      counts[i] * sum(widths, na.rm = TRUE) * if (format == "parquet") PARQUET_DISK_FACTOR else 1
    }
  }
  list(
    counts = counts,
    bytes = bytes,
    unsized = unsized,
    failed = failed,
    seconds = seconds_since(started)
  )
}

# The count block: totals, the three largest files, the columns not in the size
# and the failed counts, each failure's message in an error block. The size is an
# estimate of the files on disk (PARQUET_DISK_FACTOR).
print_count <- function(pre, files, format) {
  names <- short_name(vapply(files, `[[`, "", "name"), format)
  width <- max(nchar(names))
  # An estimate; `+` when columns of no known width are left out of it.
  est_size <- function(b, partial) {
    if (is.na(b)) "size unknown" else paste0("~", bytes_text(b), if (partial) "+")
  }
  counted <- !is.na(pre$counts)
  note(
    sprintf("%-11s", "count"),
    clock_text(pre$seconds),
    "   ",
    if (any(counted)) paste(with_commas(sum(pre$counts, na.rm = TRUE)), "rows") else "rows unknown",
    if (!all(counted)) paste0(" (", sum(!counted), " not counted)"),
    "   ",
    if (all(is.na(pre$bytes))) {
      "size unknown"
    } else {
      paste0(
        est_size(
          sum(pre$bytes, na.rm = TRUE),
          any(counted & (is.na(pre$bytes) | nzchar(pre$unsized)))
        ),
        " est. on disk"
      )
    }
  )
  top <- utils::head(order(-pre$counts, names, na.last = NA, method = "radix"), 3L)
  for (k in seq_along(top)) {
    i <- top[k]
    note(
      sprintf("  %-9s  %-*s  %15s rows   %s", if (k == 1L) "largest" else "", width, names[i], with_commas(pre$counts[i]), est_size(pre$bytes[i], nzchar(pre$unsized[i])))
    )
  }
  for (i in which(nzchar(pre$unsized))) {
    note(sprintf("  %-9s  %-*s  %s", "not sized", width, names[i], pre$unsized[i]))
  }
  if (!length(pre$failed)) {
    note(sprintf("  %-9s  none", "failed"))
  }
  for (f in pre$failed) {
    note(sprintf("  %-9s  %-*s  %s; extracting it anyway", "failed", width, short_name(f$name, format), f$message))
    report(error_block(f$detail))
  }
}

# The extract block's fixed-width lines: `k/n`, the file name as wide as the
# longest, then rows, size, elapsed and time left, right-aligned.
row_layout <- function(names, format) {
  names <- short_name(names, format)
  n <- length(names)
  kn <- 2L * nchar(n) + 1L
  name <- max(nchar(names))
  lead <- function(k, file) {
    sprintf("  %*s  %-*s", kn, paste0(k, "/", n), name, short_name(file, format))
  }
  list(
    head = function() {
      sprintf("%-*s  %15s  %9s  %7s  %7s", 2L + kn + 2L + name, "extract", "rows", "size", "elapsed", "left")
    },
    row = function(k, file, rows, size, elapsed, left) {
      sprintf("%s  %15s  %9s  %7s  %7s", lead(k, file), rows, size, elapsed, left)
    },
    # A file still running: its rows against its count, across rows and size.
    progress = function(k, file, rows, elapsed, left) {
      sprintf("%s  %26s  %7s  %7s", lead(k, file), rows, elapsed, left)
    },
    text = function(k, file, text) paste0(lead(k, file), "  ", text),
    note = function(file, text) sprintf("  %*s  %-*s  %s", kn, "", name, file, text)
  )
}

# The time left at the rate seen so far, over an order's files. The rate is the
# cells (rows x columns) actually fetched per second since extracting began, so a
# wide file counts as more work than a narrow one. The work left is each
# unfinished counted file's counted cells, less what the current file has
# fetched; a finished file drops out whatever its actual rows were. Files not
# counted are left out.
progress_tracker <- function(counts, columns) {
  state <- new.env()
  state$started <- Sys.time()
  state$fetched <- 0 # cells fetched by finished files
  state$finished <- rep(FALSE, length(counts))
  # Seconds left while file i has fetched `rows`; NA with no rate yet or no
  # counted work left.
  left <- function(i, rows) {
    fetched <- state$fetched + rows * columns[i]
    elapsed <- seconds_since(state$started)
    open <- !state$finished & !is.na(counts)
    open[i] <- FALSE
    work <- sum(counts[open] * columns[open])
    if (!state$finished[i] && !is.na(counts[i])) {
      work <- work + max(counts[i] - rows, 0) * columns[i]
    }
    # Before PROGRESS_SECONDS the rate is too noisy to show (the first small
    # files are mostly query overhead).
    if (fetched <= 0 || elapsed < max(PROGRESS_SECONDS, 1) || work <= 0) {
      return(NA_real_)
    }
    work / (fetched / elapsed)
  }
  list(
    elapsed = function() seconds_since(state$started),
    left = left,
    start = function() {
      state$file_started <- Sys.time()
      state$shown <- state$file_started
    },
    # TRUE when file i's next progress line is due.
    due = function() {
      if (seconds_since(state$shown) < PROGRESS_SECONDS) {
        return(FALSE)
      }
      state$shown <- Sys.time()
      TRUE
    },
    # A file kept from an earlier run: no work left, and none fetched in this one.
    skip = function(i) {
      state$finished[i] <- TRUE
    },
    # Marks file i done and returns the seconds left after it.
    # The estimate comes first: left() adds the current file's rows itself.
    finish = function(i, rows) {
      state$finished[i] <- TRUE
      seconds <- report(left(i, rows), NA_real_)
      state$fetched <- state$fetched + rows * columns[i]
      seconds
    }
  )
}

# A file's name in the console's columns: without the order's format, which the
# header names.
short_name <- function(name, format) sub(paste0("\\.", format, "$"), "", name)

left_text <- function(seconds) if (is.na(seconds)) "" else paste0("~", clock_text(seconds))

# ---- One order ----------------------------------------------------------------------

read_bytes <- function(path) readBin(path, "raw", file.size(path))

read_order <- function(path) parse_order(read_bytes(path))

parse_order <- function(bytes) {
  text <- rawToChar(bytes)
  Encoding(text) <- "UTF-8"
  tryCatch(
    jsonlite::parse_json(text, simplifyVector = FALSE),
    error = function(e) refuse("json", "the order is not valid JSON")
  )
}

connection <- function(db) {
  if (is.null(db$con)) {
    if (!is.null(db$failed)) {
      refuse("setup", db$failed, detail = db$failed_detail)
    }
    db$con <- tryCatch(config$connect(), error = function(e) {
      db$failed <- paste0("cannot connect to the database", sqlstate(e))
      db$failed_detail <- conditionMessage(e)
      refuse("setup", db$failed, detail = db$failed_detail)
    })
  }
  db$con
}

extract_order <- function(path, out, db, run) {
  # Read once: the bytes parsed are the bytes saved as the order copy.
  bytes <- read_bytes(path)
  doc <- check_manifest(parse_order(bytes))
  format <- doc$extract_format
  files <- plan_files(doc$files, format)
  say(
    "order ", run$nr, "   ", files_text(length(files)), "   ", format,
    "   -> ", file.path("export_data", basename(run$final))
  )
  run$headed <- TRUE
  run$total <- length(files)
  # Said before the count, which can take minutes: which files stay is decided
  # after it ("resume" below).
  prior <- report(read_prior(out, regular_files(out)))
  if (!is.null(prior) && nrow(prior$log)) {
    say(
      sprintf("%-11s", "resume"), "an earlier run left ", files_text(nrow(prior$log)),
      " finished in ", basename(out), "; counting rows to check them"
    )
  }
  pkg <- FORMAT_PACKAGES[[format]]
  if (!is.na(pkg) && !nzchar(system.file(package = pkg))) {
    install_missing(pkg)
  }
  if (!is.na(pkg) && !requireNamespace(pkg, quietly = TRUE)) {
    refuse(
      "setup",
      "R package ",
      pkg,
      " (for .",
      format,
      ") is missing and could not be installed into R's default library"
    )
  }
  con <- connection(db)
  # Resolve every file before writing any, so a missing table or a drifted
  # column fails fast.
  resolved <- lapply(files, function(f) {
    resolve_table(con, f$schema, f$table)
    # The filter column after the file's, where filter_key finds its type.
    types <- resolve_columns(
      con,
      f$schema,
      f$table,
      c(f$columns, f$filter$column)
    )
    check_storage(f, types)
    check_lopnr(f, types)
    if (!is.null(f$filter)) {
      filter_key(f, types)
    }
    check_format(f, types, format)
    types
  })
  file_names <- vapply(files, `[[`, "", "name")
  pre <- report(precount(con, files, resolved, format))
  if (is.null(pre)) {
    pre <- list(counts = rep(NA_real_, length(files)))
  } else {
    report(print_count(pre, files, format))
  }
  counts <- pre$counts
  log <- resume(out, doc, files, counts)
  kept <- file_names %in% log$file
  if (any(kept)) {
    say(sprintf("%-11s", "resume"), sum(kept), " of ", length(files), " files kept from an earlier run")
  }
  replace_file(file.path(out, ORDER_COPY), function(tmp) writeBin(bytes, tmp))
  write_log(log, out)
  run$log <- log
  tracker <- progress_tracker(
    counts,
    vapply(files, function(f) length(f$columns), 0L)
  )
  for (i in which(kept)) {
    tracker$skip(i)
  }
  layout <- row_layout(file_names, format)
  run$layout <- layout
  note(layout$head())
  sequence <- order(file_names, method = "radix")
  todo <- sequence[!kept[sequence]]
  for (k in seq_along(todo)) {
    i <- todo[k]
    f <- files[[i]]
    # Its place among all the order's files, kept ones first.
    place <- sum(kept) + k
    run$file <- f$name
    run$place <- place
    tracker$start()
    extracted <- extract_file(
      con,
      f,
      resolved[[i]],
      format,
      out,
      function(rows) {
        if (tracker$due()) {
          note(layout$progress(
            place,
            f$name,
            if (is.na(counts[i])) with_commas(rows) else paste(with_commas(rows), "of", with_commas(counts[i])),
            clock_text(tracker$elapsed()),
            left_text(tracker$left(i, rows))
          ))
        }
      }
    )
    rows <- extracted$rows
    written <- file.size(file.path(out, f$name))
    # Rows the filter leaves out for a NULL or unreadable period value, counted
    # so the order's requester learns of them (an aggregate; NA, an empty field,
    # for an unfiltered file).
    dropped <- if (is.null(f$filter)) {
      NA_real_
    } else {
      as.numeric(
        query(
          con,
          null_period_batch(f, resolved[[i]])$sql,
          paste0("counting ", f$name, "'s rows without a period value")
        )$n
      )
    }
    # After the count above, so its time is behind the time left shown here.
    # The rows written, without the separators, if the report's line fails.
    left <- tracker$finish(i, rows)
    if (
      !note(layout$row(place, f$name, with_commas(rows), bytes_text(written), clock_text(tracker$elapsed()), left_text(left)))
    ) {
      say("  ", f$name, ": ", format(rows, scientific = FALSE), " rows")
    }
    if (!is.na(dropped) && dropped > 0) {
      say(layout$note(
        "",
        paste(format(dropped, scientific = FALSE), "rows left out for a missing period value")
      ))
    }
    unit <- if (format == "dta") "ms" else "us"
    for (column in names(extracted$cut)) {
      say(layout$note(
        "",
        paste0(
          column,
          ": fraction cut to ",
          unit,
          " in ",
          with_commas(extracted$cut[[column]]),
          " values"
        )
      ))
    }
    log[nrow(log) + 1L, ] <- list(
      f$name,
      rows,
      counts[i],
      length(f$columns),
      describe_filter(f$filter),
      dropped,
      written
    )
    # Written after every file: it is the record a resumed run keeps files by.
    write_log(log, out)
    run$log <- log
  }
  run$file <- NULL
  log <- log[order(log$file, method = "radix"), , drop = FALSE]
  rownames(log) <- NULL
  write_log(log, out)
  log
}

# ---- Resuming -------------------------------------------------------------------------

# The copy of the order an unfinished export folder keeps, so that a later run can
# tell which of its files still stand. Removed when the order completes.
ORDER_COPY <- "order.json"
RUN_LOG <- "run_log.csv"
TMP <- ".tmp"

empty_log <- function() {
  data.frame(
    file = character(),
    rows = numeric(),
    rows_counted = numeric(),
    columns = integer(),
    filter = character(),
    rows_without_period = numeric(),
    bytes_written = numeric()
  )
}

# file.rename() that reports failure only by its value: on Windows it warns
# before returning FALSE, and an order's warning handler would fail the order
# on the warning instead of letting the caller recover.
rename <- function(from, to) isTRUE(suppressWarnings(file.rename(from, to)))

# Writes `path` through `write(tmp)` into <path>.tmp, then renames it over `path`,
# so the folder always holds a whole copy. Where the rename cannot replace an
# existing file, the old one is removed first: for that moment the folder has no
# copy, and a run stopped then resumes nothing. If that fails too, the .tmp copy
# stays and the order is refused.
replace_file <- function(path, write) {
  tmp <- paste0(path, TMP)
  write(tmp)
  if (!rename(tmp, path)) {
    unlink(path, expand = FALSE)
    if (!rename(tmp, path)) {
      refuse("setup", "cannot rename ", tmp, " to ", path)
    }
  }
}

write_log <- function(log, out) {
  replace_file(file.path(out, RUN_LOG), function(tmp) write_csv(log, tmp))
}

# The regular files in `out`, by their exact (case-sensitive) names.
regular_files <- function(out) {
  names <- list.files(out, all.files = TRUE, no.. = TRUE)
  names[!dir.exists(file.path(out, names))]
}

# The order copy and run log an earlier, unfinished run left in `out`: its
# manifest entries by output file name, and its log. NULL when either is missing
# or unreadable, and then nothing is kept.
read_prior <- function(out, present) {
  if (!all(c(ORDER_COPY, RUN_LOG) %in% present)) {
    return(NULL)
  }
  prior <- read_order(file.path(out, ORDER_COPY))
  entries <- stats::setNames(
    prior$files,
    vapply(prior$files, function(e) paste0(e$name, ".", prior$extract_format), "")
  )
  # As text, so an empty field stays "" in a text column; the numbers then read
  # an empty field as NA.
  log <- utils::read.csv(
    file.path(out, RUN_LOG),
    colClasses = "character",
    na.strings = character(),
    encoding = "UTF-8"
  )
  if (!identical(names(log), names(empty_log()))) {
    return(NULL)
  }
  for (column in c("rows", "rows_counted", "rows_without_period", "bytes_written")) {
    log[[column]] <- as.numeric(log[[column]])
  }
  log$columns <- as.integer(log$columns)
  list(entries = entries, log = log)
}

# Keeps the finished files of an earlier run of this order and clears every other
# file from `out`, returning the kept files' log rows. A file stands only if it is
# in the folder under its exact name, its manifest entry is unchanged (the whole
# entry, format included through its name), it is listed in the earlier log, and
# its source still counts the rows it was written with. The count is the only
# check on the source: a table changed without changing its row count is not
# noticed (DESIGN.md, "Resuming"). The order copy and the log stay until the
# caller replaces them, so a readable checkpoint is always there.
resume <- function(out, doc, files, counts) {
  log <- empty_log()
  present <- regular_files(out)
  prior <- report(read_prior(out, present))
  if (is.null(prior) && all(c(ORDER_COPY, RUN_LOG) %in% present)) {
    lost <- sum(vapply(files, `[[`, "", "name") %in% present)
    if (lost > 0L) {
      say(
        "  ",
        RUN_LOG,
        " is from another runner version: ",
        lost,
        " finished file(s) will be extracted again"
      )
    }
  }
  if (!is.null(prior)) {
    for (i in seq_along(files)) {
      name <- files[[i]]$name
      row <- prior$log[prior$log$file == name, , drop = FALSE]
      if (
        name %in% present &&
          nrow(row) == 1L &&
          identical(prior$entries[[name]], doc$files[[i]]) &&
          !is.na(counts[i]) &&
          !is.na(row$rows) &&
          row$rows == counts[i]
      ) {
        log <- rbind(log, row)
      }
    }
  }
  listed <- list.files(out, all.files = TRUE, no.. = TRUE)
  stale <- setdiff(listed, c(log$file, ORDER_COPY, RUN_LOG))
  unlink(file.path(out, stale), recursive = TRUE, expand = FALSE)
  left <- intersect(list.files(out, all.files = TRUE, no.. = TRUE), stale)
  if (length(left)) {
    refuse(
      "setup",
      "cannot clear ",
      paste(left, collapse = ", "),
      " from ",
      out,
      " (open in another program?)"
    )
  }
  rownames(log) <- NULL
  log
}

# The runner's own functions among the calls on the stack when an R error was
# raised, outermost first, then the call that raised it: where in the runner it
# failed. Function names only, since a call's arguments can hold row values; the
# raising call is cut to its function name too.
call_trace <- function(calls, e) {
  runner <- environment(call_trace)
  name <- function(cl) {
    f <- cl[[1]]
    if (is.name(f)) as.character(f) else paste(deparse(f), collapse = "")
  }
  names <- vapply(calls, name, "")
  own <- names[vapply(
    names,
    function(n) exists(n, envir = runner, inherits = FALSE, mode = "function"),
    TRUE
  )]
  raising <- conditionCall(e)
  c(own, if (!is.null(raising)) paste0(name(raising), "()"))
}

# The refusal when an order's dated export folder already exists: it is never
# replaced, so the .partial folder stays as it is and can be resumed.
refuse_existing <- function(final) {
  refuse(
    "setup",
    final,
    " already exists: move or delete it, then run again"
  )
}

# Gives the finished order's folder its dated name. The folder must hold exactly
# the order's files, the run log and the order copy; only then is the order copy
# removed and the folder renamed. A refusal before the rename leaves the .partial
# folder resumable; `run$stage` records how far it got, for the failure report.
publish <- function(out, final, expected, run) {
  if (file.exists(final)) {
    refuse_existing(final)
  }
  listed <- list.files(out, all.files = TRUE, no.. = TRUE)
  wanted <- c(expected, RUN_LOG, ORDER_COPY)
  if (!setequal(listed, wanted) || length(listed) != length(wanted)) {
    refuse(
      "setup",
      out,
      " does not hold exactly the order's files, ",
      RUN_LOG,
      " and ",
      ORDER_COPY,
      ": unexpected ",
      paste(setdiff(listed, wanted), collapse = ", "),
      "; missing ",
      paste(setdiff(wanted, listed), collapse = ", ")
    )
  }
  unlink(file.path(out, ORDER_COPY), expand = FALSE)
  if (file.exists(file.path(out, ORDER_COPY))) {
    refuse("setup", "cannot remove ", ORDER_COPY, " from ", out)
  }
  run$stage <- "complete"
  if (!rename(out, final)) {
    refuse(
      "setup",
      "cannot rename ",
      out,
      " to ",
      final
    )
  }
}

# After a failure, removes the file that was being written (<name>.partial) and
# any temporary copy (<name>.tmp) from `out`, so the finished files, the order
# copy and the run log remain. A temporary copy whose file is missing is the only
# whole copy left (replace_file removed the old one and could not rename): it is
# renamed back, never deleted. Returns the names it could not handle.
clean_partial <- function(out) {
  names <- list.files(out, pattern = "\\.(partial|tmp)$", all.files = TRUE)
  left <- character()
  for (name in names) {
    path <- file.path(out, name)
    whole <- sub("\\.tmp$", "", path)
    if (endsWith(name, TMP) && !file.exists(whole)) {
      if (!rename(path, whole)) left <- c(left, name)
    } else {
      unlink(path, expand = FALSE)
      if (file.exists(path)) left <- c(left, name)
    }
  }
  left
}

# A failure as a refusal: an unexpected R error becomes exit 8 with its message
# as the detail.
as_failure <- function(e) {
  if (inherits(e, "runner_refusal")) {
    return(e)
  }
  structure(
    class = c("runner_refusal", "error", "condition"),
    list(
      message = paste0("unexpected R error (", class(e)[1], ")"),
      call = NULL,
      code = EXIT[["extract"]],
      detail = conditionMessage(e)
    )
  )
}

stop_interrupted <- function() {
  say("stopped   interrupted")
  invokeRestart("abort")
}

run_order <- function(path, base, db) {
  nr <- tools::file_path_sans_ext(basename(path))
  stamp <- format(Sys.Date(), "%Y-%m-%d")
  out <- file.path(base, "export_data", paste0(nr, PARTIAL))
  final <- file.path(base, "export_data", paste0(nr, "_", stamp))
  # What the header and the failure report name: the order, its dated folder,
  # the file being extracted, and where an unexpected R error was raised.
  run <- new.env()
  run$nr <- nr
  run$final <- final
  started <- Sys.time()
  failure <- tryCatch(
    {
      # Checked first too, so a run does not extract for hours to be refused at
      # the end.
      if (file.exists(final)) {
        refuse_existing(final)
      }
      dir.create(out, recursive = TRUE, showWarnings = FALSE)
      if (!dir.exists(out)) {
        refuse("setup", "cannot create the export folder ", out)
      }
      # From here on the failure report cleans up and describes `out`.
      run$stage <- "open"
      # An R warning can mean a value was altered (a string R cannot
      # translate): it fails the order.
      log <- withCallingHandlers(
        extract_order(path, out, db, run),
        # Once the order is failing, a warning or error raised while stopping
        # (closing a file, clearing a result) is only noted: the first failure
        # is the one reported.
        warning = function(w) {
          if (!is.null(run$first)) {
            run$late <- c(run$late, conditionMessage(w))
            return(tryInvokeRestart("muffleWarning"))
          }
          refuse(
            "extract",
            "an R warning was raised",
            detail = conditionMessage(w)
          )
        },
        # An interrupt (Esc, Ctrl-C) is a failure too, so what its cleanup
        # raises is only noted; it then stops the whole run (below).
        interrupt = function(i) {
          if (is.null(run$first)) run$first <- i
        },
        error = function(e) {
          if (!is.null(run$first)) {
            run$late <- c(run$late, conditionMessage(e))
            return()
          }
          run$first <- e
          if (!inherits(e, "runner_refusal")) {
            run$trace <- report(call_trace(sys.calls(), e))
          }
        }
      )
      publish(out, final, log$file, run)
      run$stage <- "published"
      run$done <- log
      NULL
    },
    # An interrupt stops the whole run, no later order starts, even when its
    # cleanup raised an error.
    interrupt = function(i) stop_interrupted(),
    error = function(e) {
      if (inherits(run$first, "interrupt")) {
        stop_interrupted()
      }
      as_failure(if (is.null(run$first)) e else run$first)
    }
  )
  if (is.null(failure)) {
    archive <- file.path(base, "orders", "archive")
    dir.create(archive, showWarnings = FALSE)
    target <- file.path(archive, paste0(nr, "_", stamp, ".json"))
    # file.rename refuses an existing target on Windows: copy, then remove.
    if (file.copy(path, target, overwrite = TRUE)) {
      if (file.remove(path)) {
        log <- run$done
        if (
          !note(
            "done   ", files_text(nrow(log)), "   ",
            with_commas(sum(log$rows)), " rows   ",
            bytes_text(sum(log$bytes_written, na.rm = TRUE)), "   ",
            clock_text(seconds_since(started)), "   exit 0"
          )
        ) {
          say("done: ", files_text(nrow(log)))
        }
        return(EXIT[["ok"]])
      }
      # The order stays in orders/; no stale copy beside it in archive/.
      unlink(target)
    }
    failure <- structure(
      class = c("runner_refusal", "error", "condition"),
      list(
        message = "the export succeeded but the order could not be archived",
        call = NULL,
        code = EXIT[["setup"]]
      )
    )
  }
  stage <- if (is.null(run$stage)) "none" else run$stage
  left <- if (stage %in% c("open", "complete")) clean_partial(out) else character()
  if (!isTRUE(run$headed)) {
    say("order ", nr)
  }
  failed <- paste0(
    "FAILED (exit code ",
    failure$code,
    "): ",
    conditionMessage(failure)
  )
  # In the file's row when a file was being extracted.
  say(if (!is.null(run$file) && !is.null(run$layout)) run$layout$text(run$place, run$file, failed) else paste0("  ", failed))
  error_block(failure$detail, run$trace, run$late)
  # What is left, and what the coordinator does next.
  finished <- if (is.null(run$log)) 0L else nrow(run$log)
  if (stage == "open" && finished > 0L) {
    say("  the next run resumes ", finished, " finished file(s) kept in ", out)
  } else if (stage == "complete") {
    say("  the export is complete in ", out, ": rename it to ", final, " by hand")
  } else if (stage == "published") {
    say("  the export is complete in ", final, "; the order could not be archived")
  }
  if (length(left)) {
    say("  WARNING: could not remove or restore ", paste(left, collapse = ", "))
  }
  # The order's closing line, as `done` closes a finished one.
  log <- run$log
  closed <- note(
    "stopped   ",
    if (!is.null(run$total)) paste0(finished, " of ", files_text(run$total), "   "),
    if (finished > 0L) {
      paste0(
        with_commas(sum(log$rows)), " rows   ",
        bytes_text(sum(log$bytes_written, na.rm = TRUE)), "   "
      )
    },
    clock_text(seconds_since(started)),
    "   exit ",
    failure$code
  )
  if (!closed) {
    say("stopped   exit ", failure$code)
  }
  failure$code
}

# ---- Main ---------------------------------------------------------------------------

# Runs every order independently and returns the exit code of the first order
# (in name order) that failed, else 0.
main <- function() {
  installed <- function(pkg) nzchar(system.file(package = pkg))
  install_missing(PACKAGES[!vapply(PACKAGES, installed, TRUE)])
  for (pkg in PACKAGES) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      say(
        "R package ",
        pkg,
        " is missing and could not be installed into R's default library"
      )
      return(EXIT[["setup"]])
    }
  }
  config <<- read_config()
  if (is.character(config)) {
    say(config)
    return(EXIT[["setup"]])
  }
  orders_dir <- file.path(config$base, "orders")
  if (!dir.exists(orders_dir)) {
    say("export root has no orders folder: ", orders_dir)
    return(EXIT[["setup"]])
  }
  orders <- sort(
    list.files(
      orders_dir,
      pattern = "\\.json$",
      ignore.case = TRUE,
      full.names = TRUE
    ),
    method = "radix"
  )
  if (!length(orders)) {
    say("no orders in ", orders_dir)
    return(EXIT[["ok"]])
  }
  db <- new.env()
  db$con <- NULL
  status <- EXIT[["ok"]]
  failed <- 0L
  for (path in orders) {
    code <- run_order(path, config$base, db)
    failed <- failed + (code != EXIT[["ok"]])
    if (status == EXIT[["ok"]]) status <- code
  }
  attr(status, "orders") <- c(length(orders), failed)
  if (!is.null(db$con)) {
    tryCatch(
      DBI::dbDisconnect(db$con),
      error = function(e) say("  also, while stopping: ", conditionMessage(e)),
      warning = function(w) say("  also, while stopping: ", conditionMessage(w))
    )
  }
  status
}

status <- main()
# The run's last line, whatever stopped it: the exit code a batch job ends with.
local({
  orders <- attr(status, "orders")
  say(
    "finished   ",
    if (!is.null(orders)) {
      paste0(
        orders[1], if (orders[1] == 1L) " order" else " orders", "   ",
        orders[2], " failed   "
      )
    },
    "exit ", status
  )
})
status <- as.integer(status)
# Under source() in an interactive session, quitting would end the data
# manager's R session.
if (!interactive()) {
  quit(save = "no", status = status)
}
