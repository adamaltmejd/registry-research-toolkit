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
# every count rounded up to the next multiple of 10, a true 0 published as 0.
# No value leaves the database. The console, which stays on MONA, holds the
# int64 conversion check on the true counts and each column's lengths.
#
# Run: source("key_check.R") in R on MONA, `Rscript key_check.R`, or
# `R CMD BATCH key_check.R`, beside the steward's extract-config.json (its
# `odbc` connection only; REG_EXTRACT_CONFIG names another path). Packages:
# base R (>= 4.2), DBI, odbc and jsonlite, installed first when missing. Rationale:
# reg_mona_runner/DESIGN.md, "Key check".
#
# This file is ASCII only: a non-ASCII name reaches SQL Server as NCHAR() code
# points, never as R string bytes (non-ASCII identifiers sent as text fail on MONA).

# ---- Settings: the only part the coordinator edits ---------------------------------

# SCB lopnummer columns, as the steward's [[lopnr]] entries name them: exact
# column names, column-name prefixes, and single columns of single tables (the
# table by name, in every schema), ASCII case ignored. Example:
# SCB_KEY_NAMES <- c("LopNr"); SCB_KEY_PREFIXES <- c("P1234_LopNr_")
# SCB_KEY_COLUMNS <- list(c(table = "P1234_Extra", column = "PersonLopNr"))
SCB_KEY_NAMES <- c("LopNr")
SCB_KEY_PREFIXES <- character()
SCB_KEY_COLUMNS <- list()

# Other key-like columns, reported for information (holders' own serials):
# regular expressions on the column name, case-insensitive. A column that is
# an SCB key is not listed again here.
OTHER_KEY_PATTERNS <- c("lopnr", "id$")

# Optional filters: exact schema names (empty: every schema) and a regular
# expression on the table name ("": every table). Only base tables are
# scanned, never views.
SCHEMAS <- character()
TABLE_PATTERN <- ""

# The output file, written in R's working directory unless a path is given.
OUTPUT <- "key_check.csv"

# ---- Constants ----------------------------------------------------------------------

EXIT <- c(ok = 0L, setup = 2L, failed = 8L)

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

# Whether a text value, spaces trimmed, starts with an ASCII minus (binary, so
# no collation reads a full-width hyphen as one).
NEGATIVE <- "LTRIM(RTRIM(@col)) COLLATE Latin1_General_BIN2 LIKE N'-%'"
# A text value, spaces trimmed, without one leading minus.
UNSIGNED <- paste0(
  "(CASE WHEN ", NEGATIVE, " THEN STUFF(LTRIM(RTRIM(@col)), 1, 1, N'')",
  " ELSE LTRIM(RTRIM(@col)) END)"
)
# Whether that value is a signed digit string, the only text extract.R reads as
# a key (lopnr_digits).
DIGIT_STRING <- paste0(
  "(", UNSIGNED, " COLLATE Latin1_General_BIN2 NOT LIKE N'%[^0-9]%' AND ", UNSIGNED, " <> N'')"
)
# The text TRY_CAST is given: for a signed digit string, its sign and its digits
# without leading zeros, as lopnr_digits reads it (all zeros as 0); any other
# value as it is, trimmed, so stripping zeros cannot make one convert (0-1 is no
# -1). No concatenation: on an (n)varchar(n) at its full length it would
# truncate.
SIGN <- paste0("(CASE WHEN ", NEGATIVE, " THEN N'-' ELSE N'' END)")
FIRST_NONZERO <- paste0("PATINDEX(N'%[^0]%', ", UNSIGNED, " COLLATE Latin1_General_BIN2)")
STRIPPED <- paste0(
  "(CASE WHEN ", FIRST_NONZERO, " = 0 THEN N'0' ELSE SUBSTRING(", UNSIGNED, ", ",
  FIRST_NONZERO, ", 2147483647) END)"
)
CAST_DIGITS <- paste0(
  "(CASE WHEN ", DIGIT_STRING, " THEN ", STRIPPED, " ELSE LTRIM(RTRIM(@col)) END)"
)
CAST_SIGN <- paste0("(CASE WHEN ", DIGIT_STRING, " THEN ", SIGN, " ELSE N'' END)")

# Whether a (non-null) text value, as CAST_DIGITS reads it, does not convert to bigint.
NOT_BIGINT_TEXT <- paste0(
  "(LEN(", CAST_DIGITS, ") > 40 OR TRY_CAST(CAST(", CAST_SIGN, " + LEFT(", CAST_DIGITS, ", 40)",
  " AS nvarchar(41)) AS bigint) IS NULL)"
)
# A value that extract.R writes as a key: a digit string that converts, with no
# leading space (and no trailing one, unless the type pads: `padded`).
CONVERTS <- function(padded) {
  paste0(
    DIGIT_STRING, " AND NOT ", NOT_BIGINT_TEXT, " AND @col NOT LIKE N' %'",
    if (!padded) " AND DATALENGTH(@col) = DATALENGTH(RTRIM(@col))"
  )
}
# A value that converts but changes representation: extract.R reads a leading
# zero or -0 away. A value it refuses (01X) is no change, only a failure.
LEADING_ZEROS <- function(padded) {
  paste0(
    "COUNT_BIG(CASE WHEN (@col COLLATE Latin1_General_BIN2 LIKE N'0[0-9]%'",
    " OR @col COLLATE Latin1_General_BIN2 LIKE N'-0[0-9]%'",
    # -0 is written as 0, a change of representation like a leading zero.
    " OR @col COLLATE Latin1_General_BIN2 = N'-0') AND ", CONVERTS(padded), " THEN 1 END)"
  )
}

# The T-SQL per measure, @col standing for the quoted column. Each is an
# aggregate over the one scan. A text value is checked as stored, with
# Latin1_General_BIN2 so [0-9] is ASCII digits only, whatever the collation.
MEASURES <- list(
  nulls = "COUNT_BIG(CASE WHEN @col IS NULL THEN 1 END)",
  not_bigint = "COUNT_BIG(CASE WHEN @col IS NOT NULL AND TRY_CAST(@col AS bigint) IS NULL THEN 1 END)",
  # TRY_CAST of a (max) text value over 8000 bytes fails the query rather than
  # returning NULL: at most 40 characters of CAST_DIGITS are cast, and a longer
  # value (no bigint has more than 19 digits) counts as not converting. A
  # zero-padded key is a leading_zeros count, not this one.
  not_bigint_text = paste0(
    "COUNT_BIG(CASE WHEN @col IS NOT NULL AND ", NOT_BIGINT_TEXT, " THEN 1 END)"
  ),
  not_whole = "COUNT_BIG(CASE WHEN @col <> ROUND(@col, 0, 1) THEN 1 END)",
  beyond_float = "COUNT_BIG(CASE WHEN ABS(@col) >= 9007199254740992E0 THEN 1 END)",
  beyond_real = "COUNT_BIG(CASE WHEN ABS(@col) >= 16777216E0 THEN 1 END)",
  # extract.R reads text as an optional minus and digits (lopnr_digits), so the
  # minus is skipped before the leading-zero and digit checks.
  leading_zeros = LEADING_ZEROS(FALSE),
  leading_zeros_fixed = LEADING_ZEROS(TRUE),
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
  # Digits only: a negative number's minus is not counted.
  min_number = "MIN(LEN(REPLACE(CONVERT(varchar(20), TRY_CAST(@col AS bigint)), '-', '')))",
  max_number = "MAX(LEN(REPLACE(CONVERT(varchar(20), TRY_CAST(@col AS bigint)), '-', '')))"
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
      leading_zeros = if (type %in% c("char", "nchar")) "leading_zeros_fixed" else "leading_zeros",
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

# decimal.mark is explicit, as in extract.R: under options(OutDec = ","),
# formatC() warns, and a run with warn = 2 would abort the scan.
with_commas <- function(n) {
  formatC(n, format = "f", digits = 0, big.mark = ",", decimal.mark = ".")
}

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

# ASCII case folded, as SQL Server and the holdings reader's [[lopnr]] match.
ascii_lower <- function(x) chartr(paste(LETTERS, collapse = ""), paste(letters, collapse = ""), x)

# A physical name as the holdings reader matches it: trimmed. Matching only;
# SQL and the report keep the name as stored.
match_name <- function(x) trimws(x, whitespace = "[\\h\\v]")

# Why the SCB key settings cannot be used (an empty name, prefix, table or
# column, which the holdings reader refuses too), or NULL. An empty vector is
# fine: it selects nothing.
selector_problem <- function() {
  values <- c(SCB_KEY_NAMES, SCB_KEY_PREFIXES, unlist(SCB_KEY_COLUMNS))
  pairs <- vapply(SCB_KEY_COLUMNS, function(tc) {
    is.character(tc) && setequal(names(tc), c("table", "column")) && length(tc) == 2L
  }, TRUE)
  if (!is.character(values) || anyNA(values) || any(!nzchar(values)) || !all(pairs)) {
    return(paste(
      "SCB_KEY_NAMES, SCB_KEY_PREFIXES and SCB_KEY_COLUMNS take non-empty names;",
      "each SCB_KEY_COLUMNS entry is c(table = ..., column = ...)"
    ))
  }
  NULL
}

# The SCB key selectors, matched as the holdings reader's [[lopnr]] entries
# match (crates/reg-readers/src/holdings.rs, Lopnr::marks): a name prefix, an
# exact name, or one column of one table (in every schema). A label per
# selector and a function of (tables, names) marking the columns it selects.
scb_selectors <- function() {
  named <- lapply(SCB_KEY_NAMES, function(n) {
    list(label = paste0("name ", n), marks = function(t, c) ascii_lower(c) == ascii_lower(n))
  })
  prefixed <- lapply(SCB_KEY_PREFIXES, function(p) {
    list(label = paste0("prefix ", p), marks = function(t, c) startsWith(ascii_lower(c), ascii_lower(p)))
  })
  columns <- lapply(SCB_KEY_COLUMNS, function(tc) {
    list(
      label = paste0("table ", tc[["table"]], " column ", tc[["column"]]),
      marks = function(t, c) {
        ascii_lower(t) == ascii_lower(tc[["table"]]) & ascii_lower(c) == ascii_lower(tc[["column"]])
      }
    )
  })
  c(named, prefixed, columns)
}

# Every base-table column with its group ("scb_key", "other_key" or NA) and
# the labels of the SCB selectors that matched no column at all, judged before
# the optional scan filters, as the holdings reader refuses an unused [[lopnr]].
list_columns <- function(con) {
  cols <- DBI::dbGetQuery(
    con,
    paste0(
      "SELECT c.TABLE_SCHEMA AS table_schema, c.TABLE_NAME AS table_name, ",
      "c.COLUMN_NAME AS column_name, c.DATA_TYPE AS data_type, ",
      "CAST(c.ORDINAL_POSITION AS int) AS position ",
      "FROM INFORMATION_SCHEMA.COLUMNS AS c JOIN INFORMATION_SCHEMA.TABLES AS t ",
      "ON t.TABLE_CATALOG = c.TABLE_CATALOG AND t.TABLE_SCHEMA = c.TABLE_SCHEMA ",
      "AND t.TABLE_NAME = c.TABLE_NAME WHERE t.TABLE_TYPE = N'BASE TABLE'"
    )
  )
  cols$data_type <- tolower(cols$data_type)
  selectors <- scb_selectors()
  hits <- lapply(selectors, function(s) {
    s$marks(match_name(cols$table_name), match_name(cols$column_name))
  })
  scb <- Reduce(`|`, hits, rep(FALSE, nrow(cols)))
  other <- Reduce(
    `|`,
    lapply(OTHER_KEY_PATTERNS, grepl, x = cols$column_name, ignore.case = TRUE, perl = TRUE),
    rep(FALSE, nrow(cols))
  )
  cols$group <- ifelse(scb, "scb_key", ifelse(other, "other_key", NA_character_))
  unused <- vapply(selectors, `[[`, "", "label")[!vapply(hits, any, TRUE)]
  keep <- !is.na(cols$group)
  if (length(SCHEMAS)) {
    keep <- keep & cols$table_schema %in% SCHEMAS
  }
  if (nzchar(TABLE_PATTERN)) {
    keep <- keep & grepl(TABLE_PATTERN, cols$table_name, perl = TRUE)
  }
  cols <- cols[keep, ]
  list(
    cols = cols[order(cols$table_schema, cols$table_name, cols$position, method = "radix"), ],
    unused = unused
  )
}

# ---- Output ---------------------------------------------------------------------------

# The CSV's fields: counts only. Lengths go to the console: a blank length
# would say how many values support it, and beside a published rows count that
# can pin a small true count.
FIELDS <- c("schema", "table", "column", "group", "sql_type", "status", COUNTS)

# ---- Disclosure -----------------------------------------------------------------------

# The published counts (maintainer ruling 2026-11-27, #2680): each true count
# rounded up to the next multiple of 10, so a true 0 publishes 0 and 1 to 10
# publish 10. A published 0 is exact. No cell key, no salt. NA
# (does not apply) stays NA.
publish_counts <- function(counts) {
  ceiling(counts / 10) * 10
}

count_text <- function(n) {
  ifelse(is.na(n), "", format(n, scientific = FALSE, trim = TRUE))
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

# Opens, writes and closes `path`: NULL, or the message of the first failure
# (open, write or close), so main() can end through its exit path.
append_lines <- function(lines, path, append = TRUE) {
  tryCatch(
    {
      con <- file(path, if (append) "ab" else "wb")
      tryCatch(
        writeLines(enc2utf8(lines), con, useBytes = TRUE),
        finally = close(con)
      )
      NULL
    },
    error = conditionMessage,
    warning = conditionMessage
  )
}

csv_header <- function(database) {
  c(
    "# key_check.csv: counts of the shape of key-like columns, made by key_check.R.",
    paste0("# Database ", database, ", ", format(Sys.time(), "%Y-%m-%d %H:%M"), "."),
    paste0(
      "# Aggregates only, no value. Every count is rounded up to the next multiple of 10:",
      " a published 0 is a true 0, and 10 stands for 1 to 10. The int64 conversion check",
      " and the lengths are on the run's console only, never here."
    ),
    "# Read: reg_mona_runner/README.md, \"Key check\". Skip lines starting with #.",
    paste(FIELDS, collapse = ",")
  )
}

# One table's true counts and lengths, a matrix by column and field, from the
# SELECT's result.
table_counts <- function(cols, result, slots) {
  values <- matrix(
    NA_real_,
    nrow(cols),
    length(COUNTS) + 2L,
    dimnames = list(NULL, c(COUNTS, "min_length", "max_length"))
  )
  values[, "rows"] <- as.numeric(result[[1L]])
  for (k in seq_len(nrow(slots))) {
    values[slots$column[k], slots$field[k]] <- as.numeric(result[[k + 1L]])
  }
  values
}

# One table's CSV rows: the counts rounded. A failed table (no values) has
# its status and no counts.
publish_rows <- function(cols, status, values = NULL) {
  rows <- data.frame(
    schema = cols$table_schema,
    table = cols$table_name,
    column = cols$column_name,
    group = cols$group,
    sql_type = cols$data_type,
    status = status
  )
  published <- if (!is.null(values)) publish_counts(values[, COUNTS, drop = FALSE])
  for (field in COUNTS) {
    rows[[field]] <- if (is.null(published)) "" else count_text(published[, field])
  }
  rows
}

# The SCB key columns that do not pass the int64 conversion check, on the true
# counts (console only), in two lists: `fails`, storage extract.R refuses or a
# value it refuses; `changes`, a leading zero or -0, which extract.R writes
# without it (the key's digits change, so two keys may collide).
conversion_failures <- function(cols, values) {
  refused <- c("not_bigint", "not_whole", "beyond_exact", "spaces", "not_digits")
  scb <- cols$group == "scb_key"
  fails <- rowSums(values[, refused, drop = FALSE] > 0, na.rm = TRUE) > 0 |
    !cols$data_type %in% LOPNR_TYPES
  changes <- !is.na(values[, "leading_zeros"]) & values[, "leading_zeros"] > 0
  list(fails = cols$column_name[scb & fails], changes = cols$column_name[scb & changes])
}

# The console verdict for those lists.
conversion_verdict <- function(failures) {
  parts <- c(
    if (length(failures$fails)) paste0("FAILS ", paste(failures$fails, collapse = ", ")),
    if (length(failures$changes)) {
      paste0(
        "CHANGES REPRESENTATION, possible collision (leading zeros or -0) ",
        paste(failures$changes, collapse = ", ")
      )
    }
  )
  if (length(parts)) paste(parts, collapse = "; ") else "all convert"
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
    return(publish_rows(cols, status))
  }
  values <- table_counts(cols, result, batch$slots)
  failures <- conversion_failures(cols, values)
  say(
    "  ",
    label,
    "   ",
    with_commas(as.numeric(result[[1L]])),
    " rows   ",
    nrow(cols),
    if (nrow(cols) == 1L) " column   " else " columns   ",
    elapsed,
    if (any(cols$group == "scb_key")) {
      # The exact verdict, on true counts: console only, never in the CSV.
      paste0(
        "   int64 conversion check (true counts, console only): ",
        conversion_verdict(failures)
      )
    }
  )
  # Lengths on true values, console only (see FIELDS).
  for (j in which(!is.na(values[, "min_length"]))) {
    say(
      "    ", cols$column_name[j], "   length ",
      values[j, "min_length"], " to ", values[j, "max_length"]
    )
  }
  publish_rows(cols, "ok", values)
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
  problem <- selector_problem()
  if (!is.null(problem)) {
    say(problem)
    return(EXIT[["setup"]])
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
    c(
      list_columns(con),
      list(database = DBI::dbGetQuery(con, "SELECT DB_NAME() AS db")$db)
    ),
    error = function(e) e
  )
  if (inherits(setup, "error")) {
    say("cannot list the columns (", sqlstate(setup), ")")
    error_block(conditionMessage(setup))
    return(EXIT[["setup"]])
  }
  if (length(setup$unused)) {
    for (label in setup$unused) {
      say("SCB key selector ", label, " matches no column of any base table")
    }
    say("correct or remove those selectors (SCB_KEY_NAMES, SCB_KEY_PREFIXES, SCB_KEY_COLUMNS)")
    return(EXIT[["setup"]])
  }
  cols <- setup$cols
  tables <- unique(cols[c("table_schema", "table_name")])
  if (!nrow(tables)) {
    # Before OUTPUT is opened, so a previous report is never replaced by an empty one.
    say(
      "no table selected: no base table has a matching key column under SCHEMAS = ",
      if (length(SCHEMAS)) paste(SCHEMAS, collapse = ", ") else "(every schema)",
      " and TABLE_PATTERN = ",
      if (nzchar(TABLE_PATTERN)) paste0("\"", TABLE_PATTERN, "\"") else "(every table)"
    )
    return(EXIT[["setup"]])
  }
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
  say(
    "  the int64 conversion check is what extract.R needs to write a key as int64; a .dta",
    " order also refuses a key at or above 2^53 (9,007,199,254,740,992), which this check",
    " does not test"
  )
  write_failure <- function(message) {
    say("cannot write ", OUTPUT, ": ", message)
  }
  problem <- append_lines(csv_header(setup$database), OUTPUT, append = FALSE)
  if (!is.null(problem)) {
    write_failure(problem)
    return(EXIT[["setup"]])
  }
  failed <- 0L
  complete <- NULL
  for (i in seq_len(nrow(tables))) {
    mine <- cols[
      cols$table_schema == tables$table_schema[i] & cols$table_name == tables$table_name[i],
    ]
    rows <- scan_table(con, mine)
    table_failed <- any(startsWith(rows$status, "failed"))
    failed <- failed + table_failed
    # Written after every table, so a long run that stops keeps what it counted.
    problem <- append_lines(csv_lines(rows), OUTPUT)
    if (!is.null(problem)) {
      write_failure(problem)
      say(
        "  ", OUTPUT, " may hold part of ", tables$table_schema[i], ".", tables$table_name[i],
        "; the last table written whole is ",
        if (is.null(complete)) "none" else complete
      )
      status <- EXIT[["failed"]]
      attr(status, "tables") <- c(i, failed + !table_failed)
      return(status)
    }
    complete <- paste0(tables$table_schema[i], ".", tables$table_name[i])
  }
  status <- if (failed) EXIT[["failed"]] else EXIT[["ok"]]
  attr(status, "tables") <- c(nrow(tables), failed)
  status
}

# Runs when the script is run or sourced into the global environment
# (Rscript, R CMD BATCH, source()); loading it into another environment, as the
# tests do, only defines it.
if (identical(environment(), globalenv())) {
  status <- main()
  tables <- attr(status, "tables")
  say(
    "finished   ",
    if (!is.null(tables)) paste0(tables[1], " tables   ", tables[2], " failed   ", OUTPUT, "   "),
    "exit ",
    status
  )
  # Under source() in an interactive session, quitting would end the session.
  if (!interactive()) {
    quit(save = "no", status = as.integer(status))
  }
}
