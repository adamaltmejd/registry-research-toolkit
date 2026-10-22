# swecov_extract.R: the SWECOV order extract runner, run by hand inside MONA.
#
# For every order manifest in <base>/orders/*.json (the order.json the catalog
# writes, renamed to the project number), extract the ordered columns from
# SWECOV's holdings into <base>/export_data/<nr>_<YYYY-MM-DD>/: one CSV per
# manifest file plus run_log.csv. A success moves the order to
# orders/archive/<nr>_<YYYY-MM-DD>.json; a failure leaves it in orders/ with
# error_log.csv in its export folder. A rerun overwrites.
#
# Run: source("swecov_extract.R") in R on MONA, or `Rscript swecov_extract.R`.
# The contract it reads is crates/DESIGN.md, "Order manifest"; the rationale and
# the exit codes are reg_mona_runner/DESIGN.md.
#
# Packages: base R (>= 4.2, UTF-8 native), DBI, odbc and jsonlite, all assumed
# preinstalled on MONA. Row values are never printed or logged: only file names,
# row and column counts and manifest fields are.
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

# Test overrides (process boundaries): another export root, and an ODBC
# connection string instead of the DSN. Unset on MONA.
if (nzchar(Sys.getenv("SWECOV_EXTRACT_BASE"))) {
  config$base <- Sys.getenv("SWECOV_EXTRACT_BASE")
}
if (nzchar(Sys.getenv("SWECOV_EXTRACT_CONNECTION"))) {
  config$connect <- function() {
    DBI::dbConnect(
      odbc::odbc(),
      .connection_string = Sys.getenv("SWECOV_EXTRACT_CONNECTION"),
      bigint = "character"
    )
  }
}

# ---- Exit codes (stable; reg_mona_runner/DESIGN.md) ---------------------------

EXIT <- c(
  ok = 0L,
  setup = 2L, # export root, packages or database connection
  json = 3L, # the order is not JSON
  version = 4L, # manifest version is not 1
  mode = 5L, # not a steward_holdings order for this steward
  manifest = 6L, # unknown field, missing or malformed field, unsafe file name
  resolve = 7L, # table or column not found once, or a period column of the wrong type
  extract = 8L # the query or the file write failed
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

refuse <- function(code, ...) {
  stop(structure(
    class = c("runner_refusal", "error", "condition"),
    list(message = paste0(...), call = NULL, code = EXIT[[code]])
  ))
}

say <- function(...) cat(paste0(...), "\n", sep = "")

# ---- Manifest validation --------------------------------------------------------

is_object <- function(x) is.list(x) && !is.null(names(x))
is_array <- function(x) is.list(x) && is.null(names(x))
is_string <- function(x) is.character(x) && length(x) == 1L && !is.na(x)

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
  if (!is_string(f$encoding) || !(f$encoding %in% ENCODINGS)) {
    refuse(
      "manifest",
      where,
      ".encoding is not one of ",
      paste(ENCODINGS, collapse = ", ")
    )
  }
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

check_entry <- function(e, where) {
  check_object(
    e,
    where,
    c("source", "logical", "requested_period", "physical"),
    c("key", "filter")
  )
  check_string(e$source, paste0(where, ".source"))
  check_string(e$requested_period, paste0(where, ".requested_period"))
  if (!is.null(e$key) && !isTRUE(e$key)) {
    refuse("manifest", where, ".key is present but not true")
  }
  lg <- e$logical
  check_object(
    lg,
    paste0(where, ".logical"),
    c("provider", "register", "variant", "variable", "representation")
  )
  for (k in names(lg)) {
    check_string(lg[[k]], paste0(where, ".logical.", k))
  }
  ph <- e$physical
  check_object(
    ph,
    paste0(where, ".physical"),
    c("edition", "table", "column"),
    c("partition", "projection", "member")
  )
  check_string(ph$edition, paste0(where, ".physical.edition"))
  check_name(ph$table, paste0(where, ".physical.table"))
  check_name(ph$column, paste0(where, ".physical.column"))
  for (k in c("partition", "projection")) {
    if (!is.null(ph[[k]])) check_string(ph[[k]], paste0(where, ".physical.", k))
  }
  if (!is.null(ph$member)) {
    m <- ph$member
    check_object(
      m,
      paste0(where, ".physical.member"),
      character(),
      c("position", "year", "month")
    )
    if (length(m) != 1L) {
      refuse("manifest", where, ".physical.member must have exactly one field")
    }
  }
  if (!is.null(e$filter)) {
    check_filter(e$filter, paste0(where, ".filter"))
  }
  e
}

# The whole manifest, in the order of the stable exit codes: version, then mode,
# then shape.
check_manifest <- function(doc) {
  if (!is_object(doc)) {
    refuse("manifest", "the order is not a JSON object")
  }
  v <- doc$version
  if (!(is.numeric(v) && length(v) == 1L && !is.na(v) && v == 1)) {
    refuse(
      "version",
      "manifest version ",
      format(v),
      " is not supported; this runner reads version 1"
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
    c("version", "provenance", "entries", "clips")
  )
  provenance_fields <- c(
    "mode",
    "steward",
    "project_name",
    "project_schema_version",
    "project_reg_meta_version",
    "project_hash",
    "catalog_schema_version",
    "catalog_generation_id",
    "artifact_kind"
  )
  check_object(p, "provenance", provenance_fields)
  for (k in provenance_fields) {
    check_string(p[[k]], paste0("provenance.", k))
  }
  if (!is_array(doc$clips)) {
    refuse("manifest", "clips is not an array")
  }
  for (i in seq_along(doc$clips)) {
    at <- sprintf("clips[%d]", i - 1L)
    clip <- doc$clips[[i]]
    check_object(
      clip,
      at,
      c("source", "variable", "requested_period", "ordered_period")
    )
    for (k in names(clip)) {
      check_string(clip[[k]], paste0(at, ".", k))
    }
  }
  if (!is_array(doc$entries) || !length(doc$entries)) {
    refuse("manifest", "entries is not a non-empty array")
  }
  for (i in seq_along(doc$entries)) {
    check_entry(doc$entries[[i]], sprintf("entries[%d]", i - 1L))
  }
  doc
}

# ---- Output files -----------------------------------------------------------------

# One file per variant, physical table and table edition, named in slug spelling
# (crates/DESIGN.md, "Extraction keeps delivery topology"). Columns are the
# distinct physical columns in entry order; every entry of a file carries the
# same filter or none.
plan_files <- function(entries) {
  files <- list()
  for (i in seq_along(entries)) {
    e <- entries[[i]]
    lg <- e$logical
    ph <- e$physical
    placement <- c(ph$partition, ph$projection)
    if (length(placement) > 1L) {
      refuse(
        "manifest",
        sprintf("entries[%d]", i - 1L),
        ".physical has both a partition and a projection"
      )
    }
    name <- paste0(
      paste(c(lg$register, lg$variant, placement, ph$edition), collapse = "_"),
      ".csv"
    )
    # The placement is part of the file's identity, so two entries of one table
    # with different placements never merge silently (the name clash check below
    # refuses them instead).
    id <- paste(
      lg$provider,
      lg$register,
      lg$variant,
      ph$table,
      ph$edition,
      paste(placement, collapse = ""),
      sep = "\r"
    )
    f <- files[[id]]
    if (is.null(f)) {
      # Slug spelling only: no path separator, drive or parent reference.
      if (!grepl("^[A-Za-z0-9_][A-Za-z0-9_.,-]*$", name)) {
        refuse("manifest", "output file name ", name, " is not slug spelling")
      }
      f <- list(
        name = name,
        table = ph$table,
        columns = character(),
        filter = e$filter,
        first = i
      )
    } else if (!identical(f$filter, e$filter)) {
      refuse(
        "manifest",
        sprintf("entries[%d]", i - 1L),
        " has a row filter that differs from its file's (",
        f$name,
        ")"
      )
    }
    f$columns <- union(f$columns, ph$column)
    files[[id]] <- f
  }
  names(files) <- NULL
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
  state <- regmatches(
    msg,
    regexec(
      "(?:nanodbc/nanodbc\\.cpp:[0-9]+: |ODBC failed with error )([0-9A-Z]{5})(?:: | from )",
      msg,
      perl = TRUE
    )
  )[[1]]
  paste0(
    " (",
    if (length(state)) paste0("SQLSTATE ", state[2], "; "),
    "driver message withheld: it may quote row values)"
  )
}

# The schema holding `table`, as its id: exactly one, matched byte-exact.
resolve_schema <- function(con, table) {
  hits <- query(
    con,
    paste0(
      "SELECT CAST(SCHEMA_ID(TABLE_SCHEMA) AS int) AS schema_id, TABLE_SCHEMA AS schema_name ",
      "FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ",
      sql_text(table),
      " COLLATE Latin1_General_BIN2"
    ),
    paste0("looking up table ", table)
  )
  if (nrow(hits) != 1L) {
    refuse(
      "resolve",
      "table ",
      table,
      if (nrow(hits)) {
        paste0(
          " exists in more than one schema (",
          paste(sort(hits$schema_name), collapse = ", "),
          ")"
        )
      } else {
        " does not exist"
      }
    )
  }
  hits$schema_id[[1]]
}

# Each of `columns` on the resolved table: its DATA_TYPE and
# CHARACTER_MAXIMUM_LENGTH (-1 for max). A missing column is refused.
resolve_columns <- function(con, schema_id, table, columns) {
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
      "CAST(c.CHARACTER_MAXIMUM_LENGTH AS int) AS max_length FROM (VALUES ",
      values,
      ") AS v(i, n) LEFT JOIN INFORMATION_SCHEMA.COLUMNS AS c ON c.TABLE_SCHEMA = SCHEMA_NAME(",
      as.integer(schema_id),
      ") AND c.TABLE_NAME = ",
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
      table,
      " has no column ",
      paste(missing, collapse = ", ")
    )
  }
  list(type = tolower(found$data_type), max_length = found$max_length)
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
select_batch <- function(file, schema_id, types) {
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
    " + N' FROM ' + QUOTENAME(SCHEMA_NAME(",
    as.integer(schema_id),
    ")) + N'.' + ",
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
    do.call(paste, c(fields, sep = ",")),
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

extract_file <- function(con, file, schema_id, types, out) {
  batch <- select_batch(file, schema_id, types)
  path <- file.path(out, file$name)
  sink <- file(path, open = "wb")
  on.exit(close(sink), add = TRUE)
  writeBin(csv_header(file$columns), sink)
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
    if (ncol(chunk) != length(file$columns)) {
      refuse("extract", what, " returned an unexpected column count")
    }
    if (nrow(chunk)) {
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
      writeBin(csv_bytes(chunk), sink)
      rows <- rows + nrow(chunk)
    }
    if (DBI::dbHasCompleted(res)) break
  }
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
    for (pkg in c("DBI", "odbc")) {
      if (!requireNamespace(pkg, quietly = TRUE)) {
        db$failed <- paste0("R package ", pkg, " is not installed")
        refuse("setup", db$failed)
      }
    }
    db$con <- tryCatch(config$connect(), error = function(e) {
      db$failed <- paste0("cannot connect to the database", sqlstate(e))
      refuse("setup", db$failed)
    })
  }
  db$con
}

extract_order <- function(path, out, db) {
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    refuse("setup", "R package jsonlite is not installed")
  }
  doc <- check_manifest(read_order(path))
  files <- plan_files(doc$entries)
  con <- connection(db)
  # Resolve every file before writing any, so a missing table fails fast.
  resolved <- lapply(files, function(f) {
    schema_id <- resolve_schema(con, f$table)
    # The filter column last, so select_batch finds its type there.
    types <- resolve_columns(
      con,
      schema_id,
      f$table,
      c(f$columns, f$filter$column)
    )
    if (!is.null(f$filter)) {
      period_key(
        f$filter$encoding,
        types$type[length(types$type)],
        f$table,
        f$filter$column
      )
    }
    list(schema_id = schema_id, types = types)
  })
  log <- data.frame(
    file = character(),
    rows = numeric(),
    columns = integer(),
    filter = character()
  )
  for (i in order(vapply(files, `[[`, "", "name"), method = "radix")) {
    f <- files[[i]]
    rows <- extract_file(
      con,
      f,
      resolved[[i]]$schema_id,
      resolved[[i]]$types,
      out
    )
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
  # A failed order leaves no partial CSVs, only its error log.
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
