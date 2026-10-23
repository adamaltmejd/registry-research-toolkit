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

# JSON null is a null cell; in a double column the string "NaN" is NaN.
json_values <- function(col) {
  unlist(lapply(col$values, function(v) {
    if (is.null(v)) {
      NA
    } else if (col$type == "double" && identical(v, "NaN")) {
      NaN
    } else {
      v
    }
  }))
}

# Splices `text` into a Parquet file just before its footer. The footer's
# offsets are absolute and its length is unchanged, so readers still read the
# file; the bytes ride along unread.
splice_before_footer <- function(path, text) {
  bytes <- readBin(path, "raw", file.size(path))
  n <- length(bytes)
  footer <- readBin(
    bytes[(n - 7L):(n - 4L)],
    "integer",
    size = 4L,
    endian = "little"
  )
  at <- n - 8L - footer
  writeBin(c(bytes[seq_len(at)], charToRaw(text), bytes[(at + 1L):n]), path)
}

# Writes a case's folder description: per name, a Parquet file from typed
# columns (with optional file and column key-value metadata and bytes spliced
# before the footer), raw text, or a folder.
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
        arrow::Array$create(json_values(col), type = ARROW_TYPES[[col$type]])
      })
      names(arrays) <- vapply(spec$columns, function(col) col$name, "")
      table <- do.call(arrow::Table$create, arrays)
      for (key in names(spec$metadata)) {
        table$metadata[[key]] <- spec$metadata[[key]]
      }
      schema <- table$schema
      for (i in seq_along(spec$columns)) {
        meta <- spec$columns[[i]]$metadata
        if (!is.null(meta)) {
          schema <- schema$SetField(i - 1L, schema[[i]]$WithMetadata(meta))
        }
      }
      arrow::write_parquet(table$cast(schema), path)
      if (!is.null(spec$inner_bytes)) {
        splice_before_footer(path, spec$inner_bytes)
      }
    }
  }
}

case_path <- function(case_dir, path) {
  if (is.null(path)) NULL else file.path(case_dir, path)
}

# Text a candidate carries outside its column values (file and column
# metadata, spliced bytes): never in the report, never in an export.
hidden_strings <- function(files) {
  unlist(lapply(files, function(spec) {
    c(
      names(spec$metadata),
      unlist(spec$metadata),
      unlist(lapply(spec$columns, function(col) unlist(col$metadata))),
      spec$inner_bytes
    )
  }))
}

# Every string value, and every file or column name the schema does not
# declare: the author's, so never in the report.
author_strings <- function(files, schema) {
  declared <- if (!is.null(schema) && file.exists(schema)) {
    tryCatch(jsonlite::read_json(schema)$files, error = function(e) list())
  }
  declared_files <- vapply(declared, function(f) f$name, "")
  names_of <- function(cols) vapply(cols, function(col) col$name, "")
  values <- unlist(lapply(names(files), function(name) {
    spec <- files[[name]]
    want <- unlist(lapply(declared, function(f) {
      if (identical(f$name, name)) names_of(f$columns)
    }))
    c(
      if (!name %in% c(declared_files, "receipt.json")) name,
      setdiff(names_of(spec$columns), want),
      unlist(lapply(spec$columns, function(col) {
        if (col$type == "string") json_values(col)
      }))
    )
  }))
  values[!is.na(values)]
}

finding_keys <- function(findings) {
  key <- function(v) if (is.null(v)) "-" else as.character(v)
  sort(vapply(
    findings,
    function(f) {
      paste(key(f$file), key(f$column), key(f$position), f$rule, sep = "|")
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
  staged <- file_hashes(staging)
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
    staged = staged,
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

read_raw <- function(path) readBin(path, "raw", file.size(path))

# The MD5 of each file in `dir` (folders skipped), to show it was left as it was.
file_hashes <- function(dir) {
  paths <- file.path(dir, listing(dir))
  tools::md5sum(paths[!dir.exists(paths)])
}

expect_case <- function(name) {
  run <- run_case(name)
  expect <- run$case$expect
  for (value in c(
    author_strings(run$case$candidate, run$schema),
    hidden_strings(run$case$candidate)
  )) {
    expect_false(grepl(value, run$report_text, fixed = TRUE), label = value)
  }
  # Pass or refusal, staging is left as it was.
  expect_identical(listing(run$staging), sort(names(run$case$candidate)))
  expect_identical(
    file_hashes(run$staging),
    run$staged
  )
  if (expect$result == "refused") {
    expect_identical(run$code, expect$code)
    expect_identical(run$report$code, expect$code)
    expect_identical(
      finding_keys(run$report$findings),
      finding_keys(expect$findings)
    )
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
  declared <- vapply(
    jsonlite::read_json(run$schema)$files,
    function(f) f$name,
    ""
  )
  expect_identical(listing(run$output), sort(c(declared, "receipt.json")))
  # The export holds the checked data only: no key-value metadata and none of
  # the bytes the candidate carried beside its values.
  for (file in declared) {
    path <- file.path(run$output, file)
    bytes <- read_raw(path)
    for (value in hidden_strings(run$case$candidate)) {
      expect_length(grepRaw(value, bytes, fixed = TRUE), 0L)
    }
    table <- arrow::read_parquet(path, as_data_frame = FALSE)
    expect_length(table$metadata, 0L)
    for (field in table$schema$fields) {
      expect_false(field$HasMetadata)
    }
  }
  # The receipt's hashes are those of the files as they landed, recomputed here.
  receipt_path <- file.path(run$output, "receipt.json")
  receipt <- jsonlite::read_json(receipt_path)
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
  # Deterministic: a second run over the same candidate writes the same bytes.
  again <- run_case(name)
  expect_identical(
    read_raw(file.path(again$output, "receipt.json")),
    read_raw(receipt_path)
  )
}
