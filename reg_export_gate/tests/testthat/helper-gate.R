# Runs the case corpus (tests/cases/*/case.json) against export_gate.R as a
# pipeline's last step calls it: a candidate staging folder in, the output
# folder, the diagnostics report and the refusal code out. The corpus format is
# reg_export_gate/DESIGN.md, "Case corpus"; the future Rust binary runs the same
# cases, so everything a case asserts is in its JSON.

gate_dir <- normalizePath(file.path(testthat::test_path(), "..", ".."))
cases_dir <- file.path(gate_dir, "tests", "cases")
gate <- new.env()
sys.source(file.path(gate_dir, "export_gate.R"), envir = gate)

ARROW_TYPES <- list(
  int32 = arrow::int32(),
  int64 = arrow::int64(),
  double = arrow::float64(),
  string = arrow::utf8(),
  bool = arrow::boolean()
)

json_values <- function(values) {
  unlist(lapply(values, function(v) if (is.null(v)) NA else v))
}

# Writes a case's folder description: per name, a Parquet file from typed
# columns (with optional key-value metadata), raw text, or a folder.
materialize <- function(dir, files) {
  dir.create(dir)
  for (name in names(files)) {
    spec <- files[[name]]
    path <- file.path(dir, name)
    if (isTRUE(spec$folder)) {
      dir.create(path)
    } else if (!is.null(spec$text)) {
      writeBin(charToRaw(spec$text), path)
    } else {
      arrays <- lapply(spec$columns, function(col) {
        arrow::Array$create(
          json_values(col$values),
          type = ARROW_TYPES[[col$type]]
        )
      })
      names(arrays) <- vapply(spec$columns, function(col) col$name, "")
      table <- do.call(arrow::Table$create, arrays)
      for (key in names(spec$metadata)) {
        table$metadata[[key]] <- spec$metadata[[key]]
      }
      arrow::write_parquet(table, path)
    }
  }
}

case_path <- function(case_dir, path) {
  if (is.null(path)) NULL else file.path(case_dir, path)
}

# Every string value a case writes; none may appear in its report.
candidate_strings <- function(files) {
  values <- unlist(lapply(files, function(spec) {
    c(
      unlist(lapply(spec$columns, function(col) {
        if (col$type == "string") json_values(col$values)
      })),
      unlist(spec$metadata)
    )
  }))
  values[!is.na(values)]
}

finding_keys <- function(findings) {
  sort(vapply(
    findings,
    function(f) {
      paste(
        if (is.null(f$file)) "-" else f$file,
        if (is.null(f$column)) "-" else f$column,
        f$rule,
        sep = "|"
      )
    },
    ""
  ))
}

run_case <- function(name) {
  case_dir <- file.path(cases_dir, name)
  case <- jsonlite::read_json(file.path(case_dir, "case.json"))
  root <- tempfile("gate-")
  dir.create(root)
  staging <- file.path(root, "staging")
  output <- file.path(root, "output")
  materialize(staging, case$candidate)
  if (!is.null(case$output_before)) {
    materialize(output, case$output_before)
  }
  schema <- case_path(case_dir, case$schema)
  policy <- case_path(case_dir, case$policy)
  code <- tryCatch(
    {
      utils::capture.output(gate$export_gate(staging, output, schema, policy))
      "pass"
    },
    export_gate_refusal = function(e) e$code
  )
  report_path <- file.path(root, ".diagnostics", "export_gate_report.json")
  list(
    case = case,
    code = code,
    staging = staging,
    output = output,
    schema = schema,
    policy = policy,
    report = jsonlite::read_json(report_path),
    report_text = readChar(report_path, file.size(report_path), useBytes = TRUE)
  )
}

listing <- function(dir) {
  sort(list.files(dir, all.files = TRUE, no.. = TRUE, include.dirs = TRUE))
}

expect_case <- function(name) {
  run <- run_case(name)
  expect <- run$case$expect
  for (value in candidate_strings(run$case$candidate)) {
    expect_false(grepl(value, run$report_text, fixed = TRUE), label = value)
  }
  if (expect$result == "refused") {
    expect_identical(run$code, expect$code)
    expect_identical(run$report$code, expect$code)
    expect_identical(
      finding_keys(run$report$findings),
      finding_keys(expect$findings)
    )
    # A refusal leaves staging as it was and no export in the output folder.
    expect_identical(listing(run$staging), sort(names(run$case$candidate)))
    if (identical(expect$output, "unchanged")) {
      expect_identical(listing(run$output), sort(names(run$case$output_before)))
    } else {
      expect_false(file.exists(run$output))
    }
    return(invisible())
  }
  expect_identical(run$code, "pass")
  expect_identical(run$report$result, "pass")
  expect_length(run$report$findings, 0L)
  expect_false(file.exists(run$staging))
  declared <- vapply(
    jsonlite::read_json(run$schema)$files,
    function(f) f$name,
    ""
  )
  expect_identical(listing(run$output), sort(c(declared, "receipt.json")))
  # The receipt's hashes are those of the files as they landed, recomputed here.
  receipt <- jsonlite::read_json(file.path(run$output, "receipt.json"))
  expect_identical(
    names(receipt),
    c("format", "format_version", "gate", "schema", "policy", "files")
  )
  expect_identical(receipt$gate$version, gate$EXPORT_GATE_VERSION)
  expect_identical(
    receipt$schema$sha256,
    unname(tools::sha256sum(run$schema))
  )
  expect_identical(
    receipt$policy$sha256,
    unname(tools::sha256sum(run$policy))
  )
  expect_identical(vapply(receipt$files, function(f) f$name, ""), declared)
  expect_identical(
    vapply(receipt$files, function(f) f$sha256, ""),
    unname(tools::sha256sum(file.path(run$output, declared)))
  )
}
