# Drives swecov_extract.R as the data manager does, as a process: an export root
# with orders/ in, exit code, CSVs and logs out. The only overrides are the
# script's process boundaries (export root and database connection).

runner_dir <- normalizePath(file.path(testthat::test_path(), "..", ".."))
repo_dir <- dirname(runner_dir)
cases_dir <- file.path(runner_dir, "tests", "cases")

# A case's order: its own order.json, else the conformance order golden of the
# same name (the manifest the Rust materializer writes, unchanged).
case_order <- function(case) {
  own <- file.path(cases_dir, case, "order.json")
  if (file.exists(own)) {
    return(own)
  }
  file.path(repo_dir, "conformance", "cases", "order", case, "order.json")
}

# A fresh export root whose orders/ holds `orders`: name -> order file path, or
# name -> raw text.
export_root <- function(orders = list(), text = list()) {
  base <- tempfile("export-")
  dir.create(file.path(base, "orders"), recursive = TRUE)
  for (name in names(orders)) {
    file.copy(orders[[name]], file.path(base, "orders", paste0(name, ".json")))
  }
  for (name in names(text)) {
    writeBin(
      charToRaw(enc2utf8(text[[name]])),
      file.path(base, "orders", paste0(name, ".json"))
    )
  }
  base
}

run_runner <- function(base, connection = "") {
  output <- tempfile("runner-", fileext = ".log")
  status <- system2(
    file.path(R.home("bin"), "Rscript"),
    shQuote(file.path(runner_dir, "swecov_extract.R")),
    # system2 pastes env into a shell command: quote the values.
    env = c(
      paste0("SWECOV_EXTRACT_BASE=", shQuote(base)),
      paste0("SWECOV_EXTRACT_CONNECTION=", shQuote(connection))
    ),
    stdout = output,
    stderr = output
  )
  list(status = status, output = readLines(output, warn = FALSE))
}

export_dir <- function(base, name) {
  file.path(
    base,
    "export_data",
    paste0(name, "_", format(Sys.Date(), "%Y-%m-%d"))
  )
}

# A failed order: the code in its error log, the order still in orders/ and no
# CSV but the log in its export folder.
expect_refused <- function(base, name, code) {
  out <- export_dir(base, name)
  expect_identical(list.files(out), "error_log.csv")
  log <- utils::read.csv(file.path(out, "error_log.csv"))
  expect_identical(log$code, code)
  expect_true(file.exists(file.path(base, "orders", paste0(name, ".json"))))
}

# A compact rewrite of an order golden with one change applied.
order_text <- function(case, change) {
  doc <- jsonlite::read_json(case_order(case))
  as.character(jsonlite::toJSON(change(doc), auto_unbox = TRUE, null = "null"))
}
