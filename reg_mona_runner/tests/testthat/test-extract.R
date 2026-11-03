# The extract against a SQL Server, when one is given. SWECOV_TEST_CONNECTION
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
# class haven reads a .dta column as and its Stata display format, which tells a
# long, %12.0g, from a double, or the Parquet type) and each row's values as
# text, "<NULL>" for a missing value, and a .dta file's variable labels.
# Rendered here, not by the runner.
read_binary <- function(path) {
  labels <- NULL
  if (endsWith(path, ".dta")) {
    data <- as.data.frame(haven::read_dta(path))
    types <- lapply(data, function(x) {
      paste(class(x)[1], attr(x, "format.stata"))
    })
    labels <- unlist(lapply(data, attr, "label"))
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
  list(
    types = types,
    labels = labels,
    rows = do.call(paste, c(text, sep = "\r"))
  )
}

# Every expected file, byte for byte except row order (the extract has no
# ORDER BY): the header line exactly, then the same multiset of rows. A .dta or
# .parquet file's oracle is <file>.json: its column types, a .dta file's
# variable labels where it has any, and its rows, null for a missing value
# (read_binary).
expect_export <- function(base, case) {
  expected <- file.path(cases_dir, case, "expected")
  out <- export_dir(base, case)
  expect_setequal(list.files(out), sub("\\.json$", "", list.files(expected)))
  for (name in list.files(expected, pattern = "\\.json$")) {
    want <- jsonlite::read_json(file.path(expected, name))
    got <- read_binary(file.path(out, sub("\\.json$", "", name)))
    expect_identical(got$types, want$types, label = paste(case, name, "types"))
    expect_identical(
      as.list(got$labels),
      if (is.null(want$labels)) list() else want$labels,
      label = paste(case, name, "labels")
    )
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
  if (!nzchar(connection)) {
    skip("SWECOV_TEST_CONNECTION is not set")
  }
  db <- load_fixture()
  extracted <- c(
    # The materialize goldens, as the server writes them, over the fixture's
    # steward tables: year filters on an int column, two ranges joined by OR
    # around a gap, an open bound on either side, and none (every row, the NULL
    # year too).
    "pooled-disjoint-periods",
    "key-only",
    "shared-file-union",
    "shared-file-whole-delivery",
    # Topology: partition members as their own files beside a filtered pool, a
    # projection carrying its parent's key, and a .dta column that is no Stata
    # variable name, written under the order's alias and labelled with its full
    # name.
    "partition-members",
    "projection-key-placement",
    "dta-stata-alias",
    # Compact JSON; non-ASCII varchar text and an 18-digit decimal id that a
    # double would round; yyyymm on int, yyyymmdd on float in schema P1105 with
    # non-ASCII names, year on varchar with an unparseable value, date on
    # datetime2 keeping the bound day's last second; bounds finer than the
    # encoding widen.
    "encodings",
    # One case per extract format over one table of every mapped SQL type: a
    # leading-zero code, a pre-1960 day, a 12-digit bigint id, an 18-digit and a
    # scaled decimal, a NULL row and an empty string keep their values and take
    # the format's storage type. types-csv's table is timeless (delivery null).
    "types-dta",
    "types-parquet",
    "types-csv"
  )
  refused <- c(
    # A table that exists, but in another schema than the order names.
    `missing-table` = 7L,
    # A column whose storage drifted from the order's: refused before any row
    # is read, never coerced.
    `storage-drift` = 7L,
    # A .dta order of a bigint id no double holds: refused, never rounded.
    `inexact-dta` = 8L,
    `broken-view` = 8L
  )
  on_disk <- setdiff(c(extracted, names(refused)), "storage-drift")
  base <- export_root(
    stats::setNames(lapply(on_disk, case_order), on_disk),
    text = list(
      `storage-drift` = order_text("shared-file-whole-delivery", function(doc) {
        doc$files[[1]]$columns[[2]]$storage$length <- 20L
        doc
      })
    )
  )
  run <- run_runner(base, db)
  # The first failing order in name order (broken-view) sets the exit code.
  expect_identical(run$status, 8L)
  for (case in extracted) {
    expect_export(base, case)
  }
  for (case in names(refused)) {
    expect_refused(base, case, refused[[case]])
  }
  # The drift refusal names the file, the column and both storages, never a
  # value.
  drift <- utils::read.csv(file.path(
    export_dir(base, "storage-drift"),
    "error_log.csv"
  ))
  expect_identical(
    drift$message,
    paste0(
      "pool.dta: column Late of dbo.Pool is stored as nvarchar (length 10, ",
      "precision null, scale null) but the order expects nvarchar (length 20, ",
      "precision null, scale null)"
    )
  )
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
  # The refusal names the column and the bound, never the value (and is not
  # another extract failure, which shares exit 8).
  inexact <- readLines(file.path(
    export_dir(base, "inexact-dta"),
    "error_log.csv"
  ))
  expect_true(any(grepl(
    "column LopNr holds a value beyond 2^53",
    inexact,
    fixed = TRUE
  )))
  expect_false(any(grepl("9007199254740993", inexact, fixed = TRUE)))
})
