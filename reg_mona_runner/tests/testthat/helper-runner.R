# Drives extract.R as the data manager does, as a process: an export root with
# orders/ in, exit code, CSVs and logs out. The only input besides the orders is the
# steward configuration file (REG_EXTRACT_CONFIG), naming the export root and a DSN
# that never connects: no test reaches the database.

runner_dir <- normalizePath(file.path(testthat::test_path(), "..", ".."))
repo_dir <- dirname(runner_dir)

materialize_dir <- file.path(repo_dir, "conformance", "cases", "materialize")

# The test deployment's steward id.
STEWARD <- "test-steward"

# A materialize golden as the order file the server writes: the golden omits the
# provenance ids that vary per artifact (the conformance `materialize` runner strips
# them), so they are filled with placeholders of their shape, and the steward is
# `steward` (the goldens' own is the fixture's).
golden_order <- function(case, steward = STEWARD) {
  doc <- jsonlite::read_json(file.path(materialize_dir, case, "manifest.json"))
  id <- strrep("0", 64L)
  p <- doc$provenance
  p$steward <- steward
  p$catalog$generation <- id
  p$holdings$generation <- id
  p$base_generation <- id
  p$project$project_hash <- id
  doc$provenance <- p
  doc
}

# A fresh export root whose orders/ holds `text`: name -> raw order text.
export_root <- function(text = list()) {
  base <- tempfile("export-")
  dir.create(file.path(base, "orders"), recursive = TRUE)
  jsonlite::write_json(
    list(steward = STEWARD, base = base, odbc = list(dsn = "unreachable")),
    file.path(base, "extract-config.json"),
    auto_unbox = TRUE
  )
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
    shQuote(file.path(runner_dir, "extract.R")),
    # system2 pastes env into a shell command: quote the values.
    env = paste0(
      "REG_EXTRACT_CONFIG=",
      shQuote(file.path(base, "extract-config.json"))
    ),
    stdout = output,
    stderr = output
  )
  list(status = status, output = readLines(output, warn = FALSE))
}

# A failed order: its code on the console, the order still in orders/, an empty
# unfinished export folder (no file was written) and no finished one.
expect_refused <- function(base, name, code, output) {
  expect_match(
    output,
    paste0("FAILED (exit code ", code, ")"),
    fixed = TRUE,
    all = FALSE
  )
  exports <- file.path(base, "export_data")
  expect_identical(list.files(exports), paste0(name, ".partial"))
  expect_length(list.files(file.path(exports, paste0(name, ".partial"))), 0L)
  expect_true(file.exists(file.path(base, "orders", paste0(name, ".json"))))
}

# A compact rewrite of a case's order with one change applied.
order_text <- function(case, change, steward = STEWARD) {
  doc <- change(golden_order(case, steward))
  as.character(jsonlite::toJSON(doc, auto_unbox = TRUE, null = "null"))
}
