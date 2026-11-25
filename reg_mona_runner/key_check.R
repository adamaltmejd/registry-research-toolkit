options(echo = FALSE) # R CMD BATCH: keep the script out of the .Rout (non-interactive only)

# key_check.R: a counts-only check of the key-like columns in a steward's
# database, run by hand inside MONA by the coordinator.
#
# Lists every column from INFORMATION_SCHEMA.COLUMNS, keeps those whose names
# match the settings below (SCB lopnummer keys, and other key-like names for
# information), and scans each table once, counting for all its matched columns
# in one SELECT: rows, nulls, values that do not convert to bigint, and the
# storage-specific shapes that decide whether extract.R can write an SCB
# lopnummer as int64. It writes key_check.csv: names, SQL types and counts only,
# every nonzero count rounded up to a multiple of 5. No value leaves the
# database, not even a min or max: only lengths, and only over at least 5 values.
#
# Run: source("key_check.R") in R on MONA, `Rscript key_check.R`, or
# `R CMD BATCH key_check.R`, beside the steward's extract-config.json (its
# `odbc` connection only; REG_EXTRACT_CONFIG names another path). Packages: base
# R (>= 4.2), DBI, odbc and jsonlite, installed first when missing. Rationale:
# reg_mona_runner/DESIGN.md, "Key check".
#
# This file is ASCII only: a non-ASCII name reaches SQL Server as NCHAR() code
# points, never as R string bytes (non-ASCII identifiers sent as text fail on MONA).

# ---- Settings: the only part the coordinator edits ---------------------------------

# SCB lopnummer columns, as the steward's [[lopnr]] entries name them: exact
# column names and column-name prefixes, both case-insensitive. Example:
# SCB_KEY_NAMES <- c("LopNr"); SCB_KEY_PREFIXES <- c("P1234_LopNr_")
SCB_KEY_NAMES <- c("LopNr")
SCB_KEY_PREFIXES <- character()

# Other key-like columns, reported for information (holders' own serials):
# regular expressions on the column name, case-insensitive. A column that is
# an SCB key is not listed again here.
OTHER_KEY_PATTERNS <- c("lopnr", "id$")

# Optional filters: exact schema names (empty: every schema), a regular
# expression on the table name ("": every table), and which kinds of table to
# scan ("VIEW" scans the view's query, so a view over a scanned table repeats
# that scan).
SCHEMAS <- character()
TABLE_PATTERN <- ""
TABLE_TYPES <- c("BASE TABLE", "VIEW")

# The output file, written in R's working directory unless a path is given.
OUTPUT <- "key_check.csv"

# ---- Constants ----------------------------------------------------------------------

EXIT <- c(ok = 0L, setup = 2L, failed = 8L)

# Every nonzero count is rounded up to a multiple of this (zero stays exact),
# and a length is left out when fewer values than this support it (table_rows).
DISCLOSURE_FLOOR <- 5L

INTEGER_TYPES <- c("bit", "tinyint", "smallint", "int", "bigint")
EXACT_TYPES <- c("decimal", "numeric", "money", "smallmoney")
FLOAT_TYPES <- c("float", "real")
TEXT_TYPES <- c("char", "varchar", "nchar", "nvarchar", "text", "ntext")
# The storage extract.R writes an SCB lopnummer from (LOPNR_TYPES and
# lopnr_storage_ok in extract.R, where float(1..24) is real here): a key stored
# otherwise (bit, money, real, a date) is refused there. Kept in step by hand.
LOPNR_TYPES <- c(
  "tinyint",
  "smallint",
  "int",
  "bigint",
  "decimal",
  "numeric",
  "float",
  "char",
  "varchar",
  "nchar",
  "nvarchar",
  "text",
  "ntext"
)

# Every count the CSV can hold, in its column order. A count that does not
# apply to a column's storage is empty.
COUNTS <- c(
  "rows",
  "nulls",
  "not_bigint",
  "not_whole",
  "beyond_exact",
  "leading_zeros",
  "spaces",
  "not_digits"
)

# A text value, spaces trimmed, without one leading minus.
UNSIGNED <- paste0(
  "(CASE WHEN LTRIM(RTRIM(@col)) LIKE N'-%' THEN STUFF(LTRIM(RTRIM(@col)), 1, 1, N'')",
  " ELSE LTRIM(RTRIM(@col)) END)"
)

# The T-SQL per measure, @col standing for the quoted column. Each is an
# aggregate over the one scan. A text value is checked as stored, with
# Latin1_General_BIN2 so [0-9] is ASCII digits only, whatever the collation.
MEASURES <- list(
  nulls = "COUNT_BIG(CASE WHEN @col IS NULL THEN 1 END)",
  not_bigint = "COUNT_BIG(CASE WHEN @col IS NOT NULL AND TRY_CAST(@col AS bigint) IS NULL THEN 1 END)",
  # TRY_CAST of a (max) text value over 8000 bytes fails the query rather than
  # returning NULL: only the first 40 characters are cast, and a longer value
  # (no bigint has more than 20) counts as not converting.
  not_bigint_text = paste0(
    "COUNT_BIG(CASE WHEN @col IS NOT NULL AND (LEN(@col) > 40",
    " OR TRY_CAST(CAST(LEFT(@col, 40) AS nvarchar(40)) AS bigint) IS NULL) THEN 1 END)"
  ),
  not_whole = "COUNT_BIG(CASE WHEN @col <> ROUND(@col, 0, 1) THEN 1 END)",
  beyond_float = "COUNT_BIG(CASE WHEN ABS(@col) >= 9007199254740992E0 THEN 1 END)",
  beyond_real = "COUNT_BIG(CASE WHEN ABS(@col) >= 16777216E0 THEN 1 END)",
  # extract.R reads text as an optional minus and digits (lopnr_digits), so the
  # minus is skipped before the leading-zero and digit checks.
  leading_zeros = paste0(
    "COUNT_BIG(CASE WHEN LTRIM(@col) COLLATE Latin1_General_BIN2 LIKE N'0[0-9]%'",
    " OR LTRIM(@col) COLLATE Latin1_General_BIN2 LIKE N'-0[0-9]%' THEN 1 END)"
  ),
  # A char or nchar value is padded with trailing spaces by its type, which
  # extract.R trims: only leading spaces count there.
  spaces_fixed = "COUNT_BIG(CASE WHEN @col LIKE N' %' THEN 1 END)",
  spaces = "COUNT_BIG(CASE WHEN @col LIKE N' %' OR DATALENGTH(@col) > DATALENGTH(RTRIM(@col)) THEN 1 END)",
  not_digits = paste0(
    "COUNT_BIG(CASE WHEN ", UNSIGNED, " COLLATE Latin1_General_BIN2 LIKE N'%[^0-9]%'",
    " OR ", UNSIGNED, " = N'' THEN 1 END)"
  ),
  min_text = "MIN(LEN(@col))",
  max_text = "MAX(LEN(@col))",
  min_number = "MIN(LEN(CONVERT(varchar(20), TRY_CAST(@col AS bigint))))",
  max_number = "MAX(LEN(CONVERT(varchar(20), TRY_CAST(@col AS bigint))))"
)

# The measures a column of SQL type `type` gets, named by the CSV field each fills.
measures_for <- function(type) {
  number <- c(
    not_bigint = "not_bigint",
    min_length = "min_number",
    max_length = "max_number"
  )
  switch(
    type_class(type),
    integer = c(nulls = "nulls", number),
    exact = c(nulls = "nulls", number, not_whole = "not_whole"),
    float = c(
      nulls = "nulls",
      number,
      not_whole = "not_whole",
      beyond_exact = if (type == "real") "beyond_real" else "beyond_float"
    ),
    text = c(
      nulls = "nulls",
      not_bigint = "not_bigint_text",
      leading_zeros = "leading_zeros",
      spaces = if (type %in% c("char", "nchar")) "spaces_fixed" else "spaces",
      not_digits = "not_digits",
      min_length = "min_text",
      max_length = "max_text"
    ),
    c(nulls = "nulls")
  )
}

type_class <- function(type) {
  if (type %in% INTEGER_TYPES) {
    "integer"
  } else if (type %in% EXACT_TYPES) {
    "exact"
  } else if (type %in% FLOAT_TYPES) {
    "float"
  } else if (type %in% TEXT_TYPES) {
    "text"
  } else {
    "other"
  }
}

# ---- Console --------------------------------------------------------------------------

# Same as extract.R's: flushed after every line, since a batch run buffers.
say <- function(...) {
  cat(paste0(...), "\n", sep = "")
  flush(stdout())
  flush.console()
}

# The driver's full message can quote a value: it goes to the console only,
# which stays on MONA, never to key_check.csv.
error_block <- function(detail) {
  say("  ---- error detail: may contain row values, do not copy off MONA ----")
  for (line in unlist(strsplit(detail, "\n", fixed = TRUE))) {
    say("  ", line)
  }
  say("  ---- end of error detail ----")
}

with_commas <- function(n) formatC(n, format = "f", digits = 0, big.mark = ",")

clock_text <- function(seconds) {
  s <- round(seconds)
  sprintf("%d:%02d", s %/% 60, s %% 60)
}

# ---- Packages and connection --------------------------------------------------------

PACKAGES <- c("DBI", "odbc", "jsonlite")

# Same helper in reg_mona_runner/extract.R and reg_export_gate/export_gate.R:
# each script is uploaded to MONA alone. Installs each package into R's default
# library from the mirror R's options name, only when that library is writable
# and a mirror is set; the caller checks again and names a package still missing.
install_missing <- function(pkgs) {
  lib <- .libPaths()[1]
  repos <- getOption("repos")
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

# The `odbc` object of the steward's extract-config.json (extract.R validates
# the rest of that file; this script needs only the connection), or a message.
read_odbc <- function() {
  path <- Sys.getenv("REG_EXTRACT_CONFIG", "extract-config.json")
  if (!file.exists(path)) {
    return(paste0("no steward configuration: ", path))
  }
  cfg <- tryCatch(
    jsonlite::read_json(path, simplifyVector = FALSE),
    error = function(e) NULL
  )
  odbc <- if (is.list(cfg)) cfg$odbc
  ok <- is.list(odbc) && length(odbc) && !is.null(names(odbc)) &&
    all(nzchar(names(odbc))) &&
    all(vapply(odbc, function(x) is.character(x) && length(x) == 1L && !is.na(x), TRUE))
  if (!ok) {
    return(paste0("steward configuration ", path, " has no \"odbc\": {name: string}"))
  }
  odbc
}

# The SQLSTATE from odbc's state marker, first error state only (as extract.R).
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
  state <- states[!startsWith(states, "00") & !startsWith(states, "01")][1]
  if (is.na(state)) "R error" else paste0("SQLSTATE ", state)
}

# ---- SQL --------------------------------------------------------------------------

# Same as extract.R's: `s` as an ASCII-only T-SQL nvarchar expression, printable
# ASCII runs as N'...' literals, every other UTF-16 code unit as NCHAR(n). The
# expression is a value, so a name never becomes SQL text.
sql_text <- function(s) {
  points <- utf8ToInt(enc2utf8(s))
  if (anyNA(points)) {
    stop("a name is not valid UTF-8")
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

sql_quoted <- function(name) paste0("QUOTENAME(", sql_text(name), ")")

# A template's fixed text as N'...' literals (its quotes doubled) around the
# quoted column at each @col; the templates are this script's constants.
sql_splice <- function(template, col) {
  parts <- strsplit(paste0(template, " "), "@col", fixed = TRUE)[[1]]
  parts[length(parts)] <- sub(" $", "", parts[length(parts)])
  parts <- gsub("'", "''", parts, fixed = TRUE)
  paste(paste0("N'", parts, "'"), collapse = paste0(" + ", col, " + "))
}

# The column as T-SQL text; legacy text and ntext as varchar(max) and
# nvarchar(max), which LEN, LTRIM and TRY_CAST take.
column_expr <- function(name, type) {
  wide <- c(text = "varchar(max)", ntext = "nvarchar(max)")[type]
  if (is.na(wide)) {
    return(sql_quoted(name))
  }
  paste0("N'CAST(' + ", sql_quoted(name), " + N' AS ", wide, ")'")
}

# simplify: one SELECT per table, never split, so SQL Server's 4,096-column
# limit caps a table at about 580 matched text columns (7 measures each); split
# the batch if a table with more matched columns fails on that limit.
# The one SELECT for a table: COUNT_BIG(*) as m0, then each column's measures as
# m1, m2, ... (positional aliases, so no name is spliced as an alias). Returns
# the batch and, per measure, the column index and CSV field it fills.
table_batch <- function(schema, table, columns, types) {
  parts <- "N'SELECT COUNT_BIG(*) AS m0'"
  slots <- data.frame(column = integer(), field = character())
  for (j in seq_along(columns)) {
    wanted <- measures_for(types[j])
    for (field in names(wanted)) {
      k <- nrow(slots) + 1L
      parts <- c(
        parts,
        paste0(
          "N', ' + ",
          sql_splice(
            paste0(MEASURES[[wanted[[field]]]], " AS m", k),
            column_expr(columns[j], types[j])
          )
        )
      )
      slots[k, ] <- list(j, field)
    }
  }
  select <- paste0(
    paste(parts, collapse = " + "),
    " + N' FROM ' + ",
    sql_quoted(schema),
    " + N'.' + ",
    sql_quoted(table)
  )
  list(
    sql = paste0(
      "SET NOCOUNT ON; DECLARE @sql nvarchar(max) = CAST(N'' AS nvarchar(max)) + ",
      select,
      "; EXEC sp_executesql @sql;"
    ),
    slots = slots
  )
}

# ---- Selection ------------------------------------------------------------------------

# "scb_key", "other_key" or NA per column name.
key_group <- function(names) {
  low <- tolower(names)
  scb <- low %in% tolower(SCB_KEY_NAMES) |
    Reduce(`|`, lapply(tolower(SCB_KEY_PREFIXES), startsWith, x = low), FALSE)
  other <- Reduce(
    `|`,
    lapply(OTHER_KEY_PATTERNS, grepl, x = names, ignore.case = TRUE, perl = TRUE),
    FALSE
  )
  ifelse(scb, "scb_key", ifelse(other, "other_key", NA_character_))
}

list_columns <- function(con) {
  cols <- DBI::dbGetQuery(
    con,
    paste0(
      "SELECT c.TABLE_SCHEMA AS table_schema, c.TABLE_NAME AS table_name, ",
      "c.COLUMN_NAME AS column_name, c.DATA_TYPE AS data_type, ",
      "CAST(c.ORDINAL_POSITION AS int) AS position, t.TABLE_TYPE AS table_type ",
      "FROM INFORMATION_SCHEMA.COLUMNS AS c JOIN INFORMATION_SCHEMA.TABLES AS t ",
      "ON t.TABLE_CATALOG = c.TABLE_CATALOG AND t.TABLE_SCHEMA = c.TABLE_SCHEMA ",
      "AND t.TABLE_NAME = c.TABLE_NAME"
    )
  )
  cols$data_type <- tolower(cols$data_type)
  cols$group <- key_group(cols$column_name)
  keep <- !is.na(cols$group) & cols$table_type %in% TABLE_TYPES
  if (length(SCHEMAS)) {
    keep <- keep & cols$table_schema %in% SCHEMAS
  }
  if (nzchar(TABLE_PATTERN)) {
    keep <- keep & grepl(TABLE_PATTERN, cols$table_name, perl = TRUE)
  }
  cols <- cols[keep, ]
  cols[
    order(cols$table_schema, cols$table_name, cols$position, method = "radix"),
  ]
}

# ---- Output ---------------------------------------------------------------------------

FIELDS <- c(
  "schema",
  "table",
  "column",
  "group",
  "sql_type",
  "status",
  COUNTS,
  "min_length",
  "max_length"
)

# A count as written: empty when it does not apply, 0 when zero, else rounded
# up to the next multiple of DISCLOSURE_FLOOR. Rounding every count up, the
# table's rows too, means no difference of published counts is an exact small
# count (rows 6, nulls 1, not_bigint 5 publish as 10, 5, 5), while zero stays
# exact.
count_text <- function(n) {
  ifelse(
    is.na(n),
    "",
    ifelse(
      n == 0,
      "0",
      format(ceiling(n / DISCLOSURE_FLOOR) * DISCLOSURE_FLOOR, scientific = FALSE, trim = TRUE)
    )
  )
}

csv_field <- function(x) {
  x <- enc2utf8(as.character(x))
  quote <- grepl("[\",\r\n]", x)
  x[quote] <- paste0("\"", gsub("\"", "\"\"", x[quote], fixed = TRUE), "\"")
  x
}

csv_lines <- function(rows) {
  if (!nrow(rows)) {
    return(character())
  }
  do.call(paste, c(lapply(rows[FIELDS], csv_field), sep = ","))
}

append_lines <- function(lines, path, append = TRUE) {
  con <- file(path, if (append) "ab" else "wb")
  on.exit(close(con))
  writeLines(enc2utf8(lines), con, useBytes = TRUE)
}

csv_header <- function(database) {
  c(
    "# key_check.csv: counts of the shape of key-like columns, made by key_check.R.",
    paste0("# Database ", database, ", ", format(Sys.time(), "%Y-%m-%d %H:%M"), "."),
    "# Aggregates only: no value; every nonzero count is rounded up to a multiple of 5; a length is empty unless at least 5 values support it.",
    "# Read: reg_mona_runner/README.md, \"Key check\". Skip lines starting with #.",
    paste(FIELDS, collapse = ",")
  )
}

# One table's CSV rows: counts as written. A length is shown only when the
# values it is over, counted exactly, number at least DISCLOSURE_FLOOR (a
# number's length is over the values that convert to bigint, a text's over
# every non-null value).
table_rows <- function(cols, status, result = NULL, slots = NULL) {
  rows <- data.frame(
    schema = cols$table_schema,
    table = cols$table_name,
    column = cols$column_name,
    group = cols$group,
    sql_type = cols$data_type,
    status = status
  )
  values <- matrix(
    NA_real_,
    nrow(cols),
    length(COUNTS) + 2L,
    dimnames = list(NULL, c(COUNTS, "min_length", "max_length"))
  )
  if (!is.null(result)) {
    values[, "rows"] <- as.numeric(result[[1L]])
    for (k in seq_len(nrow(slots))) {
      values[slots$column[k], slots$field[k]] <- as.numeric(result[[k + 1L]])
    }
    number <- vapply(cols$data_type, type_class, "") %in% c("integer", "exact", "float")
    unconverted <- ifelse(number, values[, "not_bigint"], 0)
    hidden <- values[, "rows"] - values[, "nulls"] - unconverted < DISCLOSURE_FLOOR
    values[hidden, c("min_length", "max_length")] <- NA
  }
  for (field in COUNTS) {
    rows[[field]] <- count_text(values[, field])
  }
  for (field in c("min_length", "max_length")) {
    rows[[field]] <- ifelse(is.na(values[, field]), "", format(values[, field], trim = TRUE))
  }
  rows
}

# The SCB key columns that extract.R would not write as distinct int64 keys:
# storage extract.R refuses, any value that does not convert, or a leading zero
# (the key's digits change, so two keys may collide).
needs_check <- function(rows) {
  scb <- rows[rows$group == "scb_key" & rows$status == "ok", ]
  shape <- c("not_bigint", "not_whole", "beyond_exact", "leading_zeros", "spaces", "not_digits")
  bad <- Reduce(`|`, lapply(shape, function(f) !scb[[f]] %in% c("", "0")), FALSE) |
    !scb$sql_type %in% LOPNR_TYPES
  scb$column[bad]
}

# ---- Main -----------------------------------------------------------------------------

scan_table <- function(con, cols) {
  label <- paste0(cols$table_schema[1], ".", cols$table_name[1])
  started <- Sys.time()
  batch <- tryCatch(
    table_batch(cols$table_schema[1], cols$table_name[1], cols$column_name, cols$data_type),
    error = function(e) e
  )
  result <- if (inherits(batch, "error")) {
    batch
  } else {
    tryCatch(
      DBI::dbGetQuery(con, batch$sql),
      error = function(e) e,
      warning = function(w) w
    )
  }
  elapsed <- clock_text(as.numeric(difftime(Sys.time(), started, units = "secs")))
  if (inherits(result, "condition")) {
    status <- paste0("failed: ", sqlstate(result))
    say("  ", label, "   ", nrow(cols), " columns   FAILED (", sqlstate(result), ")   ", elapsed)
    error_block(conditionMessage(result))
    return(table_rows(cols, status))
  }
  rows <- table_rows(cols, "ok", result, batch$slots)
  check <- needs_check(rows)
  say(
    "  ",
    label,
    "   ",
    with_commas(as.numeric(result[[1L]])),
    " rows   ",
    nrow(cols),
    if (nrow(cols) == 1L) " column   " else " columns   ",
    elapsed,
    if (length(check)) paste0("   check: ", paste(check, collapse = ", "))
  )
  rows
}

main <- function() {
  installed <- function(pkg) nzchar(system.file(package = pkg))
  install_missing(PACKAGES[!vapply(PACKAGES, installed, TRUE)])
  for (pkg in PACKAGES) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      say("R package ", pkg, " is missing and could not be installed into R's default library")
      return(EXIT[["setup"]])
    }
  }
  odbc <- read_odbc()
  if (is.character(odbc)) {
    say(odbc)
    return(EXIT[["setup"]])
  }
  con <- tryCatch(
    do.call(
      DBI::dbConnect,
      c(list(odbc::odbc()), odbc, list(encoding = "UTF-8", bigint = "numeric"))
    ),
    error = function(e) e
  )
  if (inherits(con, "error")) {
    say("cannot connect to the database (", sqlstate(con), ")")
    error_block(conditionMessage(con))
    return(EXIT[["setup"]])
  }
  on.exit(tryCatch(DBI::dbDisconnect(con), error = function(e) NULL))
  setup <- tryCatch(
    list(
      cols = list_columns(con),
      database = DBI::dbGetQuery(con, "SELECT DB_NAME() AS db")$db
    ),
    error = function(e) e
  )
  if (inherits(setup, "error")) {
    say("cannot list the columns (", sqlstate(setup), ")")
    error_block(conditionMessage(setup))
    return(EXIT[["setup"]])
  }
  cols <- setup$cols
  tables <- unique(cols[c("table_schema", "table_name")])
  say(
    "key check   ",
    setup$database,
    "   ",
    nrow(tables),
    " tables   ",
    sum(cols$group == "scb_key"),
    " SCB key columns   ",
    sum(cols$group == "other_key"),
    " other key columns"
  )
  append_lines(csv_header(setup$database), OUTPUT, append = FALSE)
  failed <- 0L
  for (i in seq_len(nrow(tables))) {
    mine <- cols[
      cols$table_schema == tables$table_schema[i] & cols$table_name == tables$table_name[i],
    ]
    rows <- scan_table(con, mine)
    failed <- failed + any(startsWith(rows$status, "failed"))
    # Written after every table, so a long run that stops keeps what it counted.
    append_lines(csv_lines(rows), OUTPUT)
  }
  status <- if (failed) EXIT[["failed"]] else EXIT[["ok"]]
  attr(status, "tables") <- c(nrow(tables), failed)
  status
}

status <- main()
local({
  tables <- attr(status, "tables")
  say(
    "finished   ",
    if (!is.null(tables)) paste0(tables[1], " tables   ", tables[2], " failed   ", OUTPUT, "   "),
    "exit ",
    status
  )
})
status <- as.integer(status)
if (!interactive()) {
  quit(save = "no", status = status)
}
