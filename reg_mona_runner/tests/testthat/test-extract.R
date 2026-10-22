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

# Every expected file, byte for byte except row order (the extract has no
# ORDER BY): the header line exactly, then the same multiset of rows.
expect_export <- function(base, case) {
  expected <- file.path(cases_dir, case, "expected")
  out <- export_dir(base, case)
  expect_setequal(list.files(out), list.files(expected))
  for (name in list.files(expected)) {
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
    # Compact JSON; yyyymm on int, yyyymmdd on float in schema P1105 with
    # non-ASCII names, year on varchar with an unparseable value, date on
    # datetime2 keeping the bound day's last second; bounds finer than the
    # encoding widen.
    "encodings"
  )
  refused <- c(`missing-table` = 7L, `ambiguous-table` = 7L, `broken-view` = 8L)
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
  expect_match(broken$message, "driver message withheld", fixed = TRUE)
})
