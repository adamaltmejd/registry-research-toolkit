# The extract against SQL Server (CI's service container). SWECOV_TEST_CONNECTION
# is an ODBC connection string to the server without a database; the fixture
# (tests/mssql/fixture.sql) is loaded into a fresh database. One runner process
# runs every order, so one order's failure must not stop the others.

connection <- Sys.getenv("SWECOV_TEST_CONNECTION")

load_fixture <- function() {
  server <- DBI::dbConnect(odbc::odbc(), .connection_string = connection)
  on.exit(DBI::dbDisconnect(server))
  DBI::dbExecute(server, "DROP DATABASE IF EXISTS runner_fixture")
  DBI::dbExecute(server, "CREATE DATABASE runner_fixture")
  db <- paste0(connection, "Database=runner_fixture;")
  con <- DBI::dbConnect(odbc::odbc(), .connection_string = db)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  sql <- readLines(
    file.path(runner_dir, "tests", "mssql", "fixture.sql"),
    encoding = "UTF-8"
  )
  batch <- cumsum(sql == "GO")
  for (lines in split(sql[sql != "GO"], batch[sql != "GO"])) {
    text <- paste(lines, collapse = "\n")
    if (nzchar(trimws(gsub("--[^\n]*", "", text)))) DBI::dbExecute(con, text)
  }
  db
}

# A binary extract as its oracle spells it: each column's storage type (the R
# class haven reads a .dta column as, or the Parquet type) and each row's values
# as text, "<NULL>" for a missing value. Rendered here, not by the runner.
read_binary <- function(path) {
  if (endsWith(path, ".dta")) {
    data <- as.data.frame(haven::read_dta(path))
    types <- lapply(data, function(x) class(x)[1])
    text <- lapply(data, function(x) {
      if (inherits(x, "POSIXct")) {
        format(x, "%Y-%m-%d %H:%M:%S", tz = "UTC")
      } else {
        as.character(x)
      }
    })
  } else {
    table <- arrow::read_parquet(path, as_data_frame = FALSE)
    types <- stats::setNames(
      lapply(table$schema$fields, function(f) f$type$ToString()),
      names(table)
    )
    text <- lapply(names(table), function(n) {
      as.vector(table[[n]]$cast(arrow::utf8()))
    })
  }
  text <- lapply(text, function(x) ifelse(is.na(x), "<NULL>", x))
  list(types = types, rows = do.call(paste, c(text, sep = "\r")))
}

# Every expected file, byte for byte except row order (the extract has no
# ORDER BY): the header line exactly, then the same multiset of rows. A .dta or
# .parquet file's oracle is <file>.json: its column types and rows, null for a
# missing value (read_binary).
expect_export <- function(base, case) {
  expected <- file.path(cases_dir, case, "expected")
  out <- export_dir(base, case)
  expect_setequal(list.files(out), sub("\\.json$", "", list.files(expected)))
  for (name in list.files(expected, pattern = "\\.json$")) {
    want <- jsonlite::read_json(file.path(expected, name))
    got <- read_binary(file.path(out, sub("\\.json$", "", name)))
    expect_identical(got$types, want$types, label = paste(case, name, "types"))
    rows <- vapply(
      want$rows,
      function(r) {
        paste(
          vapply(r, function(v) if (is.null(v)) "<NULL>" else v, ""),
          collapse = "\r"
        )
      },
      ""
    )
    expect_identical(sort(got$rows), sort(rows), label = paste(case, name))
  }
  for (name in list.files(expected, pattern = "\\.csv$")) {
    got <- readLines(file.path(out, name), encoding = "UTF-8")
    want <- readLines(file.path(expected, name), encoding = "UTF-8")
    expect_identical(got[1], want[1], label = paste(case, name, "header"))
    expect_identical(sort(got[-1]), sort(want[-1]), label = paste(case, name))
  }
  expect_false(file.exists(file.path(base, "orders", paste0(case, ".json"))))
  expect_true(file.exists(file.path(
    base,
    "orders",
    "archive",
    paste0(case, "_", format(Sys.Date(), "%Y-%m-%d"), ".json")
  )))
}

test_that("orders extract their files from SQL Server", {
  # Skipped locally (no SQL Server); in CI a missing connection is a failure.
  if (!nzchar(connection)) {
    if (nzchar(Sys.getenv("CI"))) {
      fail("SWECOV_TEST_CONNECTION is not set in CI")
    }
    skip("SWECOV_TEST_CONNECTION is not set")
  }
  db <- load_fixture()
  extracted <- c(
    # Year filters on an int column: one range, two open-ended ranges joined by
    # OR (one file from two sources), an open upper bound, and no filter.
    "row-filter",
    "row-filter-disjoint",
    "row-filter-open-ended",
    "row-filter-whole-table",
    # File naming and topology: partition and projection placement, a held key
    # column, CSV quoting, one table serving two variants as two files.
    "partitions",
    "projections",
    "column-windows",
    # Compact JSON; non-ASCII varchar text and an 18-digit decimal id that a
    # double would round; yyyymm on int, yyyymmdd on float in schema P1105 with
    # non-ASCII names, year on varchar with an unparseable value, date on
    # datetime2 keeping the bound day's last second; bounds finer than the
    # encoding widen.
    "encodings",
    # One case per extract format over one table of every mapped SQL type: a
    # leading-zero code, a pre-1960 day, a 12-digit bigint id, an 18-digit and a
    # scaled decimal, a NULL row and an empty string keep their values and take
    # the format's storage type.
    "types-dta",
    "types-parquet",
    "types-csv"
  )
  refused <- c(
    `missing-table` = 7L,
    `ambiguous-table` = 7L,
    # A .dta order of a column whose name is over Stata's 32 characters.
    `stata-name` = 7L,
    `broken-view` = 8L
  )
  base <- export_root(stats::setNames(
    lapply(c(extracted, names(refused)), case_order),
    c(extracted, names(refused))
  ))
  run <- run_runner(base, db)
  # The first failing order in name order (ambiguous-table) sets the exit code.
  expect_identical(run$status, 7L)
  for (case in extracted) {
    expect_export(base, case)
  }
  for (case in names(refused)) {
    expect_refused(base, case, refused[[case]])
  }
  # The driver's message is withheld: it can quote a row value.
  broken <- utils::read.csv(file.path(
    export_dir(base, "broken-view"),
    "error_log.csv"
  ))
  # odbc reports only the success state 00000 for this division by zero, which
  # the runner drops, so nothing of the driver's message survives.
  expect_match(
    broken$message,
    " failed \\(driver message withheld: it may quote row values\\)$"
  )
})
