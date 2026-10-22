# export_gate.R: the export gate's check for MONA (reg_export_gate).
#
# A pipeline's last step calls
#
#   export_gate(staging, output, schema, policy)
#
# over the complete candidate output in `staging`. It checks, in order: the
# inventory, each file's structure, the disclosure rules and an identifier
# backstop. Only if every file passes does it write receipt.json into staging
# and rename staging to `output`, so the files and the receipt appear together.
# On any refusal there is no `output` folder and the call signals an error of class
# "export_gate_refusal" whose `code` is the refusal category. Findings name a
# file, a column and a rule, never a value; they go to
# .diagnostics/export_gate_report.json beside `output`, never into it.
#
# Rationale, the schema and policy formats and the refusal codes:
# reg_export_gate/DESIGN.md. Packages: base R (>= 4.2), jsonlite and arrow,
# plus SHA-256 from tools::sha256sum (R >= 4.5) or else the digest package.
#
# Fixed toolkit code: copy it unchanged. Its version is in every receipt.

EXPORT_GATE_VERSION <- "0.1.0"

export_gate <- local({
  RECEIPT <- "receipt.json"
  REPORT <- "export_gate_report.json"
  # Schema type -> the Arrow types a Parquet column of that type may have.
  TYPES <- list(
    integer = c("int32", "int64"),
    double = "double",
    string = c("string", "large_string"),
    boolean = "bool",
    date = "date32[day]"
  )
  ROLES <- c("dimension", "count", "person_count", "statistic")
  COUNT_ROLES <- c("count", "person_count")
  SLUG <- "^[a-z0-9][a-z0-9_-]*$"
  FILE_NAME <- "^[a-z0-9][a-z0-9_-]*[.]parquet$"
  COLUMN_NAME <- "^[a-z][a-z0-9_]*$"
  # Personnummer and samordningsnummer shapes (day + 60), with or without the
  # century and the - or + separator, not inside a longer digit run. A backstop
  # only: organisationsnummer share the shape and also match.
  IDENTIFIER <- paste0(
    "(?<![0-9])(?:19|20)?[0-9]{2}(?:0[1-9]|1[0-2])",
    "(?:0[1-9]|[12][0-9]|3[01]|6[1-9]|[78][0-9]|9[01])[-+]?[0-9]{4}(?![0-9])"
  )

  finding <- function(rule, file = NA_character_, column = NA_character_) {
    list(file = file, column = column, rule = rule)
  }

  # Ends the check with a refusal; caught in export_gate() below.
  refuse <- function(code, findings) {
    stop(structure(
      class = c("export_gate_stop", "condition"),
      list(message = code, call = NULL, code = code, findings = findings)
    ))
  }

  say <- function(...) cat(paste0(...), "\n", sep = "")

  is_string <- function(x) is.character(x) && length(x) == 1L && !is.na(x)
  is_count <- function(x) {
    is.numeric(x) && length(x) == 1L && !is.na(x) && x == round(x) && x >= 1
  }
  is_object <- function(x) is.list(x) && !is.null(names(x))
  is_array <- function(x) is.list(x) && is.null(names(x)) && length(x) > 0L
  closed <- function(x, required, optional = character()) {
    is_object(x) &&
      all(required %in% names(x)) &&
      all(names(x) %in% c(required, optional))
  }

  sha256 <- function(bytes) {
    tools_ns <- asNamespace("tools")
    if (exists("sha256sum", envir = tools_ns, inherits = FALSE)) {
      return(unname(get("sha256sum", envir = tools_ns)(bytes = bytes)))
    }
    digest::digest(bytes, algo = "sha256", serialize = FALSE)
  }
  has_sha256 <- function() {
    exists("sha256sum", envir = asNamespace("tools"), inherits = FALSE) ||
      requireNamespace("digest", quietly = TRUE)
  }

  read_bytes <- function(path) readBin(path, "raw", n = file.size(path))

  # A JSON file's parsed content and its SHA-256, or NULL if it is unreadable.
  read_json_file <- function(path) {
    if (!is_string(path) || !file.exists(path) || dir.exists(path)) {
      return(NULL)
    }
    tryCatch(
      {
        bytes <- read_bytes(path)
        doc <- jsonlite::parse_json(rawToChar(bytes), simplifyVector = FALSE)
        list(doc = doc, sha256 = sha256(bytes))
      },
      error = function(e) NULL,
      warning = function(w) NULL
    )
  }

  # ---- Setup: schema and policy ---------------------------------------------

  valid_columns <- function(columns) {
    if (!is_array(columns)) {
      return(FALSE)
    }
    shape <- vapply(
      columns,
      function(col) {
        closed(col, c("name", "type", "role"), c("support", "description")) &&
          is_string(col$name) &&
          grepl(COLUMN_NAME, col$name) &&
          is_string(col$type) &&
          col$type %in% names(TYPES) &&
          is_string(col$role) &&
          col$role %in% ROLES &&
          # A count is a whole number; its suppression marker is null.
          (!col$role %in% COUNT_ROLES || col$type == "integer")
      },
      NA
    )
    if (!all(shape)) {
      return(FALSE)
    }
    names <- vapply(columns, function(col) col$name, "")
    counts <- names[vapply(
      columns,
      function(col) col$role %in% COUNT_ROLES,
      NA
    )]
    # A statistic names the count it rests on, and only a statistic does.
    supported <- vapply(
      columns,
      function(col) {
        if (col$role != "statistic") {
          return(is.null(col$support))
        }
        is_string(col$support) && col$support %in% counts
      },
      NA
    )
    !anyDuplicated(names) && all(supported)
  }

  load_schema <- function(path) {
    where <- if (is_string(path)) basename(path) else NA_character_
    read <- read_json_file(path)
    if (is.null(read)) {
      refuse("setup", list(finding("schema_unreadable", where)))
    }
    s <- read$doc
    if (!is_object(s) || !identical(s$format, "reg-export-gate/schema")) {
      refuse("setup", list(finding("schema_invalid", where)))
    }
    if (!identical(s$format_version, 1L)) {
      refuse("setup", list(finding("schema_version", where)))
    }
    if (
      is_array(s$files) &&
        any(vapply(
          s$files,
          function(f) is_object(f) && identical(f$name, RECEIPT),
          NA
        ))
    ) {
      refuse("setup", list(finding("schema_reserved_name", where)))
    }
    ok <- closed(
      s,
      c("format", "format_version", "name", "version", "files"),
      "description"
    ) &&
      is_string(s$name) &&
      grepl(SLUG, s$name) &&
      is_count(s$version) &&
      is_array(s$files) &&
      all(vapply(
        s$files,
        function(f) {
          closed(f, c("name", "columns"), "description") &&
            is_string(f$name) &&
            grepl(FILE_NAME, f$name) &&
            valid_columns(f$columns)
        },
        NA
      ))
    if (!ok || anyDuplicated(vapply(s$files, function(f) f$name, ""))) {
      refuse("setup", list(finding("schema_invalid", where)))
    }
    list(
      name = s$name,
      version = s$version,
      files = s$files,
      sha256 = read$sha256
    )
  }

  load_policy <- function(path, schema) {
    where <- if (is_string(path)) basename(path) else NA_character_
    if (!is_string(path) || !file.exists(path)) {
      refuse("setup", list(finding("policy_missing", where)))
    }
    read <- read_json_file(path)
    if (is.null(read)) {
      refuse("setup", list(finding("policy_unreadable", where)))
    }
    p <- read$doc
    if (!is_object(p) || !identical(p$format, "reg-export-gate/policy")) {
      refuse("setup", list(finding("policy_invalid", where)))
    }
    if (!identical(p$format_version, 1L)) {
      refuse("setup", list(finding("policy_version", where)))
    }
    ok <- closed(
      p,
      c(
        "format",
        "format_version",
        "name",
        "version",
        "schema",
        "min_cell_count"
      ),
      "description"
    ) &&
      is_string(p$name) &&
      grepl(SLUG, p$name) &&
      is_count(p$version) &&
      closed(p$schema, c("name", "version")) &&
      is_string(p$schema$name) &&
      is_count(p$schema$version) &&
      is_count(p$min_cell_count)
    if (!ok) {
      refuse("setup", list(finding("policy_invalid", where)))
    }
    if (p$schema$name != schema$name || p$schema$version != schema$version) {
      refuse("setup", list(finding("policy_schema_mismatch", where)))
    }
    list(
      name = p$name,
      version = p$version,
      min_cell_count = p$min_cell_count,
      sha256 = read$sha256
    )
  }

  # ---- The four checks --------------------------------------------------------

  # The staging folder holds exactly the schema's files: none missing, none
  # extra, no hidden files, folders or links, no receipt of its own.
  check_inventory <- function(staging, schema) {
    expected <- vapply(schema$files, function(f) f$name, "")
    present <- list.files(
      staging,
      all.files = TRUE,
      no.. = TRUE,
      include.dirs = TRUE
    )
    findings <- list()
    for (name in present) {
      path <- file.path(staging, name)
      rule <- if (startsWith(name, ".")) {
        "hidden_file"
      } else if (name == RECEIPT) {
        "reserved_name"
      } else if (dir.exists(path) || nzchar(Sys.readlink(path))) {
        "not_a_file"
      } else if (!name %in% expected) {
        "extra_file"
      }
      if (!is.null(rule)) {
        findings[[length(findings) + 1L]] <- finding(rule, name)
      }
    }
    for (name in setdiff(expected, present)) {
      findings[[length(findings) + 1L]] <- finding("missing_file", name)
    }
    if (length(findings)) refuse("inventory", findings)
  }

  # Reads each file once: the bytes it hashes are the bytes it checks.
  read_candidates <- function(staging, schema) {
    lapply(schema$files, function(f) {
      bytes <- read_bytes(file.path(staging, f$name))
      table <- tryCatch(
        arrow::read_parquet(bytes, as_data_frame = FALSE),
        error = function(e) NULL,
        warning = function(w) NULL
      )
      list(spec = f, sha256 = sha256(bytes), table = table)
    })
  }

  check_structure <- function(candidates) {
    findings <- list()
    for (cand in candidates) {
      file <- cand$spec$name
      if (is.null(cand$table)) {
        findings <- c(findings, list(finding("unreadable", file)))
        next
      }
      want <- vapply(cand$spec$columns, function(col) col$name, "")
      have <- names(cand$table)
      for (col in setdiff(want, have)) {
        findings <- c(findings, list(finding("missing_column", file, col)))
      }
      for (col in setdiff(have, want)) {
        findings <- c(findings, list(finding("extra_column", file, col)))
      }
      if (setequal(want, have) && !identical(want, have)) {
        findings <- c(findings, list(finding("column_order", file)))
      }
      for (col in cand$spec$columns) {
        if (col$name %in% have) {
          type <- cand$table$schema$GetFieldByName(col$name)$type$ToString()
          if (!type %in% TYPES[[col$type]]) {
            findings <- c(
              findings,
              list(finding("column_type", file, col$name))
            )
          }
        }
      }
    }
    if (length(findings)) refuse("structure", findings)
  }

  # Base is.na() on the converted column, not an Arrow compute call, so the
  # check runs on older arrow releases too. A NaN statistic counts as null.
  is_null <- function(column) is.na(as.vector(column))

  # Every count is at least the policy's minimum or null (suppressed), and a
  # statistic is null wherever the count it rests on is suppressed.
  check_disclosure <- function(candidates, policy) {
    findings <- list()
    for (cand in candidates) {
      file <- cand$spec$name
      table <- cand$table
      for (col in cand$spec$columns) {
        if (col$role %in% COUNT_ROLES) {
          values <- as.vector(table[[col$name]]$cast(arrow::float64()))
          if (any(values[!is.na(values)] < policy$min_cell_count)) {
            findings <- c(
              findings,
              list(finding("min_cell_count", file, col$name))
            )
          }
        } else if (col$role == "statistic") {
          if (
            any(!is_null(table[[col$name]]) & is_null(table[[col$support]]))
          ) {
            findings <- c(
              findings,
              list(finding("unsupported_statistic", file, col$name))
            )
          }
        }
      }
    }
    if (length(findings)) refuse("disclosure", findings)
  }

  # String values, and the file's key-value metadata, hold no identifier.
  check_backstop <- function(candidates) {
    findings <- list()
    for (cand in candidates) {
      file <- cand$spec$name
      for (col in cand$spec$columns) {
        if (col$type == "string") {
          values <- as.vector(cand$table[[col$name]])
          if (any(grepl(IDENTIFIER, values[!is.na(values)], perl = TRUE))) {
            findings <- c(
              findings,
              list(finding("identifier_pattern", file, col$name))
            )
          }
        }
      }
      meta <- unlist(cand$table$metadata)
      if (any(grepl(IDENTIFIER, c(names(meta), meta), perl = TRUE))) {
        findings <- c(
          findings,
          list(finding("identifier_pattern_metadata", file))
        )
      }
    }
    if (length(findings)) refuse("backstop", findings)
  }

  # ---- Receipt, report and the move -------------------------------------------

  write_json <- function(x, path) {
    text <- jsonlite::toJSON(x, auto_unbox = TRUE, pretty = TRUE, null = "null")
    writeBin(charToRaw(paste0(enc2utf8(as.character(text)), "\n")), path)
  }

  receipt <- function(schema, policy, candidates) {
    list(
      format = "reg-export-gate/receipt",
      format_version = 1L,
      gate = list(implementation = "R", version = EXPORT_GATE_VERSION),
      schema = list(
        name = schema$name,
        version = schema$version,
        sha256 = schema$sha256
      ),
      policy = list(
        name = policy$name,
        version = policy$version,
        sha256 = policy$sha256
      ),
      files = lapply(candidates, function(cand) {
        list(name = cand$spec$name, sha256 = cand$sha256)
      })
    )
  }

  # Best effort: a report that cannot be written does not change the result.
  write_report <- function(dir, result, code, findings) {
    if (!requireNamespace("jsonlite", quietly = TRUE)) {
      return(invisible(FALSE))
    }
    tryCatch(
      {
        dir.create(dir, showWarnings = FALSE)
        write_json(
          list(
            gate_version = EXPORT_GATE_VERSION,
            result = result,
            code = code,
            findings = lapply(findings, function(f) {
              lapply(f, function(v) if (is.na(v)) NULL else v)
            })
          ),
          file.path(dir, REPORT)
        )
        invisible(TRUE)
      },
      error = function(e) invisible(FALSE),
      warning = function(w) invisible(FALSE)
    )
  }

  # The prior export in `output` (receipt and files) or an empty folder is
  # removed before anything is checked, so a refused run never leaves an old
  # export looking current. Anything else there is not the gate's to delete.
  clear_output <- function(output) {
    if (!file.exists(output)) {
      return(invisible())
    }
    present <- list.files(output, all.files = TRUE, no.. = TRUE)
    if (!dir.exists(output) || (length(present) && !RECEIPT %in% present)) {
      refuse("setup", list(finding("output_not_gate_export")))
    }
    unlink(output, recursive = TRUE)
    if (file.exists(output)) {
      refuse("setup", list(finding("output_not_removable")))
    }
  }

  check_paths <- function(staging, output) {
    if (!is_string(staging) || !dir.exists(staging)) {
      refuse("setup", list(finding("staging_missing")))
    }
    if (!is_string(output) || !dir.exists(dirname(output))) {
      refuse("setup", list(finding("output_parent_missing")))
    }
    s <- normalizePath(staging, winslash = "/")
    o <- file.path(
      normalizePath(dirname(output), winslash = "/"),
      basename(output)
    )
    inside <- function(a, b) a == b || startsWith(a, paste0(b, "/"))
    if (inside(o, s) || inside(s, o)) {
      refuse("setup", list(finding("paths_overlap")))
    }
  }

  check_packages <- function() {
    missing <- c(
      if (!requireNamespace("jsonlite", quietly = TRUE)) "jsonlite_missing",
      if (!requireNamespace("arrow", quietly = TRUE)) "arrow_missing",
      if (!has_sha256()) "sha256_missing"
    )
    if (length(missing)) {
      refuse("setup", lapply(missing, finding))
    }
  }

  run <- function(staging, output, schema_path, policy_path) {
    check_paths(staging, output)
    clear_output(output)
    check_packages()
    schema <- load_schema(schema_path)
    policy <- load_policy(policy_path, schema)
    check_inventory(staging, schema)
    candidates <- read_candidates(staging, schema)
    check_structure(candidates)
    check_disclosure(candidates, policy)
    check_backstop(candidates)
    receipt_path <- file.path(staging, RECEIPT)
    write_json(receipt(schema, policy, candidates), receipt_path)
    # One rename moves the files and the receipt together. Staging and output
    # must be on the same volume; the gate never copies.
    # file.rename() warns on failure; the failure is the refusal.
    if (!suppressWarnings(file.rename(staging, output))) {
      unlink(receipt_path)
      refuse("setup", list(finding("move_failed")))
    }
    length(candidates)
  }

  function(staging, output, schema, policy) {
    diagnostics <- if (is_string(output)) {
      file.path(dirname(output), ".diagnostics")
    }
    outcome <- tryCatch(
      withCallingHandlers(
        list(moved = run(staging, output, schema, policy)),
        # An R warning can quote a value: it refuses instead of printing.
        warning = function(w) {
          refuse("setup", list(finding("internal_warning")))
        }
      ),
      export_gate_stop = function(s) s,
      # Reported by its class alone: R and Arrow messages can quote values.
      error = function(e) {
        list(
          code = "setup",
          findings = list(finding(paste0("internal_error:", class(e)[1L])))
        )
      }
    )
    stopped <- if (is.null(outcome$moved)) outcome
    moved <- outcome$moved
    if (is.null(stopped)) {
      write_report(diagnostics, "pass", NULL, list())
      say("export gate ", EXPORT_GATE_VERSION, ": pass, ", moved, " files")
      say("  moved with receipt.json to ", output)
      return(invisible("pass"))
    }
    write_report(diagnostics, "refused", stopped$code, stopped$findings)
    say("export gate ", EXPORT_GATE_VERSION, ": refused (", stopped$code, ")")
    for (f in stopped$findings) {
      say(
        "  ",
        f$rule,
        if (!is.na(f$file)) paste0(" file=", f$file),
        if (!is.na(f$column)) paste0(" column=", f$column)
      )
    }
    stop(structure(
      class = c("export_gate_refusal", "error", "condition"),
      list(
        message = paste0(
          "export gate refused (",
          stopped$code,
          "); report in ",
          file.path(diagnostics, REPORT)
        ),
        call = NULL,
        code = stopped$code
      )
    ))
  }
})
