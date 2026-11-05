options(echo = FALSE) # R CMD BATCH: keep the script out of the .Rout (non-interactive only)

# swecov_extract.R: the SWECOV order extract runner, run by hand inside MONA.
#
# For every order manifest in <base>/orders/*.json (the order.json the catalog
# writes, renamed to the project number), extract the ordered columns from
# SWECOV's holdings into <base>/export_data/<nr>_<YYYY-MM-DD>/: one file per
# manifest file, in the order's extract_format (Stata .dta, Parquet or CSV), plus
# run_log.csv. A success moves the order to
# orders/archive/<nr>_<YYYY-MM-DD>.json; a failure leaves it in orders/ with
# error_log.csv in its export folder. A rerun overwrites.
#
# Run: source("swecov_extract.R") in R on MONA, `Rscript swecov_extract.R`, or
# `R CMD BATCH swecov_extract.R` on the batch server.
# The contract it reads is crates/DESIGN.md, "MONA order file"; the rationale and
# the exit codes are reg_mona_runner/DESIGN.md.
#
# Packages: base R (>= 4.2, UTF-8 native), DBI, odbc and jsonlite, plus haven for
# a .dta order and arrow for a .parquet one; any missing is installed into R's
# default library first (haven and arrow when an order needs them). Row values
# are never printed or logged: only file names, row and column counts and
# manifest fields are.
#
# This file is ASCII only: a non-ASCII name reaches SQL Server as NCHAR() code
# points, never as R string bytes (non-ASCII identifiers sent as text fail on MONA).

# ---- Steward configuration: everything SWECOV-specific is here ----------------

config <- list(
  # The manifest's provenance.steward this deployment serves.
  steward = "swecov",
  # The export root holding orders/, orders/archive/ and export_data/.
  base = "\\\\micro.intra\\projekt\\P1105$\\P1105_Gem\\subproject_export",
  connect = function() {
    DBI::dbConnect(
      odbc::odbc(),
      dsn = "P1105",
      Driver = "ODBC Driver 17 for SQL Server",
      Server = "MONASQL",
      Database = "P1105",
      encoding = "UTF-8",
      bigint = "character"
    )
  }
)

# Test override (a process boundary): another export root. Unset on MONA.
if (nzchar(Sys.getenv("SWECOV_EXTRACT_BASE"))) {
  config$base <- Sys.getenv("SWECOV_EXTRACT_BASE")
}

# ---- Exit codes (stable; reg_mona_runner/DESIGN.md) ---------------------------

EXIT <- c(
  ok = 0L,
  setup = 2L, # export root, packages or database connection
  json = 3L, # the order is not JSON
  version = 4L, # manifest version is not 4
  mode = 5L, # not a steward_holdings order for this steward
  manifest = 6L, # unknown field, missing or malformed field, unsafe file name
  # table or column not found in its schema, a column stored otherwise than the
  # order expects, a period column of the wrong type, or a column the order's
  # format cannot hold (its SQL type or, for .dta, its name)
  resolve = 7L,
  extract = 8L # the query or the file write failed, or a value the format cannot hold
)

FETCH_ROWS <- 100000L
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

refuse <- function(code, ...) {
  stop(structure(
    class = c("runner_refusal", "error", "condition"),
    list(message = paste0(...), call = NULL, code = EXIT[[code]])
  ))
}

say <- function(...) cat(paste0(...), "\n", sep = "")

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
    c("stata_name", "slot")
  )
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
  if (!(is.numeric(v) && length(v) == 1L && !is.na(v) && v == 4)) {
    refuse(
      "version",
      "manifest version ",
      format(v),
      " is not supported; this runner reads version 4"
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

# Run a query; a driver error is reported by SQLSTATE only, because its message
# can quote a row value (a conversion error names the value it failed on).
query <- function(con, sql, what) {
  tryCatch(
    DBI::dbGetQuery(con, sql),
    error = function(e) refuse("extract", what, " failed", sqlstate(e))
  )
}

# The SQLSTATE from odbc's own state marker ("nanodbc/nanodbc.cpp:N: XXXXX: " or
# "ODBC failed with error XXXXX from "), first match only; the rest of the
# message can quote a row value and is never kept. A value would have to spell
# that marker to be mistaken for it, and would still yield five characters.
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
  paste0(
    " (",
    if (!is.na(state)) paste0("SQLSTATE ", state, "; "),
    "driver message withheld: it may quote row values)"
  )
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
# and a label too long for its full name (.dta). A CSV holds anything as text.
check_format <- function(file, types, format) {
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
# an 18-digit id never passes through a double.
select_expr <- function(type, max_length) {
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
# column as an integer (TRY_CAST, so an unparseable value is NULL and excluded);
# date reads it as a day, so a datetime keeps every time of its day.
period_key <- function(encoding, data_type, table, column) {
  if (encoding == "date" && data_type %in% c(DATE_TYPES, TEXT_TYPES)) {
    return(list(expr = "TRY_CAST(@col AS date)", type = "date"))
  }
  if (encoding != "date" && data_type %in% c(NUMBER_TYPES, TEXT_TYPES)) {
    return(list(expr = "TRY_CAST(@col AS bigint)", type = "bigint"))
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

# One batch per file: the SELECT is assembled server-side from QUOTENAME()d
# identifiers and run by sp_executesql with the filter bounds as parameters,
# bound from R.
select_batch <- function(file, types) {
  quoted <- function(name) paste0("QUOTENAME(", sql_text(name), ")")
  # A template's fixed text as N'...' literals around the quoted column at each
  # @col; the templates are this script's constants, never manifest text.
  splice <- function(template, col) {
    parts <- strsplit(paste0(template, " "), "@col", fixed = TRUE)[[1]]
    parts[length(parts)] <- sub(" $", "", parts[length(parts)])
    paste(paste0("N'", parts, "'"), collapse = paste0(" + ", col, " + "))
  }
  selected <- vapply(
    seq_along(file$columns),
    function(j) {
      splice(
        select_expr(types$type[j], types$max_length[j]),
        quoted(file$columns[j])
      )
    },
    ""
  )
  filter_type <- types$type[length(types$type)]
  select <- paste0(
    "N'SELECT ' + ",
    paste(selected, collapse = " + N', ' + "),
    " + N' FROM ' + ",
    quoted(file$schema),
    " + N'.' + ",
    quoted(file$table)
  )
  declared <- character()
  params <- list()
  f <- file$filter
  if (!is.null(f)) {
    key <- period_key(f$encoding, filter_type, file$table, f$column)
    key_sql <- splice(key$expr, quoted(f$column))
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
            key_sql,
            " + N' ",
            if (side == "from") ">=" else "<=",
            " ",
            name,
            "'"
          )
        )
      }
      if (!length(parts)) {
        parts <- paste0(key_sql, " + N' IS NOT NULL'")
      }
      terms <- c(
        terms,
        paste0("N'(' + ", paste(parts, collapse = " + N' AND ' + "), " + N')'")
      )
    }
    select <- paste0(
      select,
      " + N' WHERE ' + ",
      paste(terms, collapse = " + N' OR ' + ")
    )
  }
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

# One CSV field per value: numbers in fixed notation to 15 significant digits
# (no 1e+15 for a numeric id), days as YYYY-MM-DD, times as YYYY-MM-DD HH:MM:SS
# in UTC, text as UTF-8, NULL as an empty field.
csv_field <- function(x) {
  out <- if (inherits(x, "POSIXct")) {
    format(x, "%Y-%m-%d %H:%M:%S", tz = "UTC")
  } else if (inherits(x, "Date")) {
    format(x, "%Y-%m-%d")
  } else if (is.double(x)) {
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

# A writer for one output file: write(chunk) takes each fetched chunk (the first
# may have no rows), finish() completes the file, release() frees what an
# unfinished file holds (the failed order's folder is then cleared). `types` are
# the file's columns' resolve_columns() facts, in column order; `names` are the
# names they are written under, which only a .dta order renames (plan_files).
csv_writer <- function(path, columns, types, names) {
  sink <- file(path, open = "wb")
  writeBin(csv_header(columns), sink)
  state <- new.env()
  state$open <- TRUE
  list(
    write = function(chunk) if (nrow(chunk)) writeBin(csv_bytes(chunk), sink),
    finish = function() {
      close(sink)
      state$open <- FALSE
    },
    release = function() if (state$open) close(sink)
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
      sink$close()
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
  list(
    write = function(chunk) {
      if (!nrow(chunk)) {
        return(invisible())
      }
      arrays <- lapply(seq_along(chunk), function(j) {
        x <- chunk[[j]]
        if (inherits(x, "POSIXct")) {
          attr(x, "tzone") <- "UTC"
        }
        # bigint and decimal arrive as text: arrow parses them exactly, or fails.
        arrow::Array$create(x)$cast(schema[[j]]$type)
      })
      writer$WriteTable(
        arrow::Table$create(
          !!!stats::setNames(arrays, columns),
          schema = schema
        ),
        chunk_size = FETCH_ROWS
      )
    },
    finish = close_all,
    release = close_all
  )
}

# One column as .dta storage (reg_mona_runner/DESIGN.md, "Types per format").
# A value Stata cannot hold exactly is refused, never rounded or cut.
dta_column <- function(x, type, precision, column, what) {
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
  what <- paste0("writing ", basename(path))
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
            what
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
    }
  )
}

WRITERS <- list(dta = dta_writer, parquet = parquet_writer, csv = csv_writer)

extract_file <- function(con, file, types, format, out) {
  batch <- select_batch(file, types)
  n <- length(file$columns)
  path <- file.path(out, file$name)
  column_types <- lapply(types, `[`, seq_len(n))
  writer <- WRITERS[[format]](path, file$columns, column_types, file$names)
  on.exit(writer$release(), add = TRUE)
  what <- paste0("extracting ", file$name)
  res <- tryCatch(
    if (length(batch$params)) {
      DBI::dbSendQuery(con, batch$sql, params = batch$params)
    } else {
      DBI::dbSendQuery(con, batch$sql)
    },
    error = function(e) refuse("extract", what, " failed", sqlstate(e))
  )
  on.exit(DBI::dbClearResult(res), add = TRUE, after = FALSE)
  rows <- 0
  repeat {
    chunk <- tryCatch(
      DBI::dbFetch(res, n = FETCH_ROWS),
      error = function(e) refuse("extract", what, " failed", sqlstate(e))
    )
    if (ncol(chunk) != n) {
      refuse("extract", what, " returned an unexpected column count")
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
    if (DBI::dbHasCompleted(res)) break
  }
  writer$finish()
  rows
}

# ---- One order ----------------------------------------------------------------------

read_order <- function(path) {
  text <- rawToChar(readBin(path, "raw", file.size(path)))
  Encoding(text) <- "UTF-8"
  tryCatch(
    jsonlite::parse_json(text, simplifyVector = FALSE),
    error = function(e) refuse("json", "the order is not valid JSON")
  )
}

connection <- function(db) {
  if (is.null(db$con)) {
    if (!is.null(db$failed)) {
      refuse("setup", db$failed)
    }
    db$con <- tryCatch(config$connect(), error = function(e) {
      db$failed <- paste0("cannot connect to the database", sqlstate(e))
      refuse("setup", db$failed)
    })
  }
  db$con
}

extract_order <- function(path, out, db) {
  doc <- check_manifest(read_order(path))
  format <- doc$extract_format
  files <- plan_files(doc$files, format)
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
    # The filter column last, so select_batch finds its type there.
    types <- resolve_columns(
      con,
      f$schema,
      f$table,
      c(f$columns, f$filter$column)
    )
    check_storage(f, types)
    if (!is.null(f$filter)) {
      period_key(
        f$filter$encoding,
        types$type[length(types$type)],
        f$table,
        f$filter$column
      )
    }
    check_format(f, types, format)
    types
  })
  log <- data.frame(
    file = character(),
    rows = numeric(),
    columns = integer(),
    filter = character()
  )
  for (i in order(vapply(files, `[[`, "", "name"), method = "radix")) {
    f <- files[[i]]
    rows <- extract_file(con, f, resolved[[i]], format, out)
    say("  ", f$name, ": ", format(rows, scientific = FALSE), " rows")
    log[nrow(log) + 1L, ] <- list(
      f$name,
      rows,
      length(f$columns),
      describe_filter(f$filter)
    )
  }
  write_csv(log, file.path(out, "run_log.csv"))
  nrow(log)
}

run_order <- function(path, base, db) {
  nr <- tools::file_path_sans_ext(basename(path))
  stamp <- format(Sys.Date(), "%Y-%m-%d")
  out <- file.path(base, "export_data", paste0(nr, "_", stamp))
  say("order ", nr, ":")
  # An empty export folder, or FALSE when a file survives (on Windows, one open
  # in another program).
  reset <- function() {
    unlink(out, recursive = TRUE)
    dir.create(out, recursive = TRUE, showWarnings = FALSE)
    dir.exists(out) && !length(list.files(out, all.files = TRUE, no.. = TRUE))
  }
  failure <- tryCatch(
    {
      if (!reset()) {
        refuse("setup", "cannot clear the export folder ", out)
      }
      # An R warning can quote a value (a string R cannot translate): it fails
      # the order instead of reaching the console.
      n <- withCallingHandlers(
        extract_order(path, out, db),
        warning = function(w) {
          refuse(
            "extract",
            "an R warning was raised (withheld: it may quote row values)"
          )
        }
      )
      say("  done: ", n, " file(s) in ", out)
      NULL
    },
    runner_refusal = function(e) e,
    # Withheld like a driver message: an R error, and its call (do.call inlines
    # its arguments), can quote a value it failed on.
    error = function(e) {
      structure(
        class = c("runner_refusal", "error", "condition"),
        list(
          message = paste0(
            "unexpected R error (",
            class(e)[1],
            "; message withheld: it may quote row values)"
          ),
          call = NULL,
          code = EXIT[["extract"]]
        )
      )
    }
  )
  if (is.null(failure)) {
    archive <- file.path(base, "orders", "archive")
    dir.create(archive, showWarnings = FALSE)
    target <- file.path(archive, paste0(nr, "_", stamp, ".json"))
    # file.rename refuses an existing target on Windows: copy, then remove.
    if (file.copy(path, target, overwrite = TRUE)) {
      if (file.remove(path)) {
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
  # A failed order leaves no partial files, only its error log.
  if (!reset()) {
    say("  WARNING: files remain in ", out, ": delete them before using it")
  }
  write_csv(
    data.frame(code = failure$code, message = conditionMessage(failure)),
    file.path(out, "error_log.csv")
  )
  say("  FAILED (exit code ", failure$code, "): ", conditionMessage(failure))
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
  for (path in orders) {
    code <- run_order(path, config$base, db)
    if (status == EXIT[["ok"]]) status <- code
  }
  if (!is.null(db$con)) {
    DBI::dbDisconnect(db$con)
  }
  status
}

status <- main()
# Under source() in an interactive session, quitting would end the data
# manager's R session: report instead.
if (interactive()) {
  say("finished with exit code ", status)
} else {
  quit(save = "no", status = status)
}
