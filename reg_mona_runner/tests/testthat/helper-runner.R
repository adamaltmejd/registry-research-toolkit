# Drives swecov_extract.R as the data manager does, as a process: an export root
# with orders/ in, exit code, CSVs and logs out. The only overrides are the
# script's process boundary (the export root); no database connection is given.

runner_dir <- normalizePath(file.path(testthat::test_path(), "..", ".."))
repo_dir <- dirname(runner_dir)

materialize_dir <- file.path(repo_dir, "conformance", "cases", "materialize")

# A materialize golden as the order file the server writes: the golden omits the
# provenance ids that vary per artifact (conformance/test_materialize.py strips
# them), so they are filled with placeholders of their shape, and the steward is
# `steward` (the goldens' own is the fixture's).
golden_order <- function(case, steward = "swecov") {
  doc <- jsonlite::read_json(file.path(materialize_dir, case, "manifest.json"))
  id <- strrep("0", 64L)
  p <- doc$provenance
  p$steward <- steward
  p$catalog$generation <- id
  p$holdings$generation <- id
  p$base_generation <- id
  p$project$project_hash <- id
  doc$provenance <- p
  path <- tempfile(paste0(case, "-"), fileext = ".json")
  writeLines(
    jsonlite::toJSON(doc, auto_unbox = TRUE, null = "null", pretty = TRUE),
    path,
    useBytes = TRUE
  )
  path
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

run_runner <- function(base) {
  output <- tempfile("runner-", fileext = ".log")
  status <- system2(
    file.path(R.home("bin"), "Rscript"),
    shQuote(file.path(runner_dir, "swecov_extract.R")),
    # system2 pastes env into a shell command: quote the values.
    env = c(
      paste0("SWECOV_EXTRACT_BASE=", shQuote(base)),
      "SWECOV_EXTRACT_CONNECTION="
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

# A compact rewrite of a case's order with one change applied.
order_text <- function(case, change, steward = "swecov") {
  doc <- jsonlite::read_json(golden_order(case, steward))
  as.character(jsonlite::toJSON(change(doc), auto_unbox = TRUE, null = "null"))
}
