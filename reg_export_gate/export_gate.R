# export_gate.R: the export gate's check for MONA (reg_export_gate).
#
# A pipeline's last step calls
#
#   export_gate(staging, output, schema, policy)
#
# over the complete candidate output in `staging`. It checks, in order: the
# inventory, each file's structure, the disclosure rules and an identifier
# backstop. Only if every file passes does it write each checked table afresh
# (the decoded data only, never the author's bytes) with receipt.json into a new
# folder beside `output` and rename that folder to `output`, so the files and
# the receipt appear together. Staging is left as it was. On any refusal there
# is no `output` folder and the call signals an error of class
# "export_gate_refusal" whose `code` is the refusal category. Findings name a
# schema file or column, or the position of one the schema does not declare,
# and a rule, never a value; they go to .diagnostics/export_gate_report.json
# beside `output`, never into it.
#
# Rationale, the schema and policy formats and the refusal codes:
# reg_export_gate/DESIGN.md. Packages: base R (>= 4.2), jsonlite and arrow (>= 9),
# plus SHA-256 from tools::sha256sum (R >= 4.5) or else the digest package. A
# call installs any that are missing or too old into R's default library first.
#
# Fixed toolkit code: copy it unchanged. Its version is in every receipt.

EXPORT_GATE_VERSION <- "0.3.0"

export_gate <- local({
  RECEIPT <- "receipt.json"
  ARROW_FLOOR <- "9.0.0"
  REPORT <- "export_gate_report.json"
  # The size caps (maintainer, 2026-11-04), the Rust gate's: a staging file and
  # its rewrite are at most 8 MiB, a staging file at most 100,000 rows.
  MAX_FILE_BYTES <- 8 * 1024 * 1024
  MAX_ROWS <- 100000
  # Schema type -> the Arrow types a Parquet column of that type may have.
  TYPES <- list(
    integer = c("int32", "int64"),
    double = "double",
    string = c("string", "large_string"),
    boolean = "bool",
    date = "date32[day]"
  )
  # Schema type -> the one Arrow type the gate writes it as (functions: arrow
  # is checked for at run time, not when the file is sourced).
  EXPORT_TYPES <- list(
    integer = function() arrow::int64(),
    double = function() arrow::float64(),
    string = function() arrow::utf8(),
    boolean = function() arrow::boolean(),
    date = function() arrow::date32()
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

  # `file` and `column` are names the schema declares (or receipt.json). A file
  # or column it does not declare is located by `position` instead, since its
  # name is the author's and can be a value.
  finding <- function(
    rule,
    file = NA_character_,
    column = NA_character_,
    position = NA_integer_
  ) {
    list(file = file, column = column, position = position, rule = rule)
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
  # extra, no hidden files, folders or links, no receipt of its own. A link is
  # caught where Sys.readlink() sees one (not on Windows); the rewrite below is
  # what guarantees only checked data leaves.
  check_inventory <- function(staging, schema) {
    expected <- vapply(schema$files, function(f) f$name, "")
    # Byte order, not the locale's, so a position means the same everywhere.
    present <- sort(
      list.files(staging, all.files = TRUE, no.. = TRUE, include.dirs = TRUE),
      method = "radix"
    )
    findings <- list()
    for (i in seq_along(present)) {
      name <- present[[i]]
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
        findings[[length(findings) + 1L]] <- if (
          name %in% c(expected, RECEIPT)
        ) {
          finding(rule, name)
        } else {
          finding(rule, position = i)
        }
      }
    }
    for (name in setdiff(expected, present)) {
      findings[[length(findings) + 1L]] <- finding("missing_file", name)
    }
    if (length(findings)) refuse("inventory", findings)
  }

  # Each schema file, read once into memory (at most the byte cap plus one
  # byte) and parsed from those bytes, so no file handle stays open in staging.
  # The order is the Rust gate's: the byte cap, the footer's row count, the
  # columns against the schema, then the rows, counted again as decoded. The
  # result is a candidate with its columns (one per schema column, in schema
  # order) or its structure findings.
  read_candidate <- function(staging, spec) {
    file <- spec$name
    fail <- function(rule) {
      list(spec = spec, findings = list(finding(rule, file)))
    }
    quiet <- function(expr) {
      tryCatch(expr, error = function(e) NULL, warning = function(w) NULL)
    }
    bytes <- quiet({
      con <- file(file.path(staging, file), "rb")
      on.exit(close(con))
      readBin(con, "raw", n = MAX_FILE_BYTES + 1)
    })
    if (is.null(bytes)) {
      return(fail("unreadable"))
    }
    if (length(bytes) > MAX_FILE_BYTES) {
      return(fail("too_large"))
    }
    reader <- quiet(arrow::ParquetFileReader$create(bytes))
    if (is.null(reader)) {
      return(fail("unreadable"))
    }
    # simplify: the footer's total only; the Rust gate also checks each row
    # group's count and their sum before decoding. The recount after decoding
    # below catches a footer that understates, at the cost of decoding it.
    if (reader$num_rows > MAX_ROWS) {
      return(fail("too_large"))
    }
    schema <- quiet(reader$GetSchema())
    if (is.null(schema)) {
      return(fail("unreadable"))
    }
    findings <- check_columns(spec, schema)
    if (length(findings)) {
      return(list(spec = spec, findings = findings))
    }
    table <- quiet(reader$ReadTable())
    if (is.null(table)) {
      return(fail("unreadable"))
    }
    if (table$num_rows > MAX_ROWS) {
      return(fail("too_large"))
    }
    have <- names(table)
    columns <- lapply(spec$columns, function(col) {
      column <- table$column(match(col$name, have) - 1L)
      if (is_dictionary(column$type)) {
        column$cast(column$type$value_type)
      } else {
        column
      }
    })
    names(columns) <- vapply(spec$columns, function(col) col$name, "")
    list(spec = spec, columns = columns)
  }

  # arrow R restores the author's Arrow types from the file's ARROW:schema
  # entry and has no switch to skip it; the Rust gate ignores it and takes
  # types from the Parquet types alone. The one hint that matters for an
  # accepted type is a dictionary (an R factor, a pandas category): its
  # Parquet type is its value type, so the gate reads it as that.
  is_dictionary <- function(type) type$id == arrow::Type$DICTIONARY
  field_type <- function(type) {
    if (is_dictionary(type)) type$value_type$ToString() else type$ToString()
  }

  # The file's columns are the schema's, in its order, each of an accepted
  # type. A duplicate name is column_order; its type is the first column's.
  check_columns <- function(spec, schema) {
    file <- spec$name
    findings <- list()
    want <- vapply(spec$columns, function(col) col$name, "")
    have <- names(schema)
    for (col in setdiff(want, have)) {
      findings <- c(findings, list(finding("missing_column", file, col)))
    }
    for (i in which(!have %in% want)) {
      findings <- c(findings, list(finding("extra_column", file, position = i)))
    }
    if (setequal(want, have) && !identical(want, have)) {
      findings <- c(findings, list(finding("column_order", file)))
    }
    for (col in spec$columns) {
      at <- match(col$name, have)
      if (!is.na(at) && !field_type(schema[[at]]$type) %in% TYPES[[col$type]]) {
        findings <- c(findings, list(finding("column_type", file, col$name)))
      }
    }
    findings
  }

  check_structure <- function(staging, schema) {
    candidates <- lapply(schema$files, function(f) read_candidate(staging, f))
    findings <- unlist(lapply(candidates, `[[`, "findings"), recursive = FALSE)
    if (length(findings)) {
      refuse("structure", findings)
    }
    candidates
  }

  # Null-ness is Arrow's validity, which is what the rewrite writes, never R's
  # NA conversion: R reads some valid values as NA (a NaN whose low word is
  # 1954, int32 INT_MIN, the int64 NA sentinel). So these rules use Arrow
  # compute functions, and such a NaN is a NaN, such an integer a value.
  any_true <- function(x) any(as.vector(x), na.rm = TRUE)
  is_null <- function(column) as.vector(arrow::call_function("is_null", column))

  # Every count is at least the policy's minimum or null (suppressed), a
  # statistic is null wherever the count it rests on is suppressed, and no
  # double is NaN: a NaN carries about 51 payload bits through Parquet and the
  # rewrite, so a cell that looks suppressed could hold a value.
  check_disclosure <- function(candidates, policy) {
    findings <- list()
    # A minimum past int64 (2^63 and up) is above every count; Arrow would
    # clamp it to INT64_MAX, letting that count pass.
    above_all <- policy$min_cell_count >= 2^63
    minimum <- if (!above_all) {
      arrow::Scalar$create(policy$min_cell_count, arrow::int64())
    }
    for (cand in candidates) {
      file <- cand$spec$name
      columns <- cand$columns
      for (col in cand$spec$columns) {
        column <- columns[[col$name]]
        if (
          col$type == "double" &&
            any_true(arrow::call_function("is_nan", column))
        ) {
          findings <- c(findings, list(finding("nan_value", file, col$name)))
        }
        if (col$role %in% COUNT_ROLES) {
          # A negative count is below any minimum.
          below <- if (above_all) {
            !is_null(column)
          } else {
            arrow::call_function("less", column$cast(arrow::int64()), minimum)
          }
          if (any_true(below)) {
            findings <- c(
              findings,
              list(finding("min_cell_count", file, col$name))
            )
          }
        } else if (col$role == "statistic") {
          if (any(!is_null(column) & is_null(columns[[col$support]]))) {
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

  # A column's values as the digits and text the identifier scan reads: strings
  # as they are, integers and dates (as day numbers) in full, and doubles by
  # their whole-number part, since an identifier stored as a number is whole
  # and the digits of a fraction would match by chance.
  scan_text <- function(column, type) {
    switch(
      type,
      string = as.vector(column),
      integer = as.vector(column$cast(arrow::utf8())),
      date = as.vector(column$cast(arrow::int32())$cast(arrow::utf8())),
      double = {
        x <- as.vector(column)
        format(trunc(x[is.finite(x)]), scientific = FALSE, trim = TRUE)
      },
      character()
    )
  }

  # No value, text or number, has an identifier's shape. The files' key-value
  # metadata is not scanned: the gate never exports it.
  check_backstop <- function(candidates) {
    findings <- list()
    for (cand in candidates) {
      file <- cand$spec$name
      for (col in cand$spec$columns) {
        values <- scan_text(cand$columns[[col$name]], col$type)
        if (any(grepl(IDENTIFIER, values[!is.na(values)], perl = TRUE))) {
          findings <- c(
            findings,
            list(finding("identifier_pattern", file, col$name))
          )
        }
      }
    }
    if (length(findings)) refuse("backstop", findings)
  }

  # ---- Receipt, report and the move -------------------------------------------

  write_json <- function(x, path) {
    text <- jsonlite::toJSON(x, auto_unbox = TRUE, pretty = TRUE, null = "null")
    writeBin(charToRaw(paste0(enc2utf8(as.character(text)), "\n")), path)
  }

  receipt <- function(schema, policy, written) {
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
      files = written
    )
  }

  # Writes a checked table afresh: new arrays of the schema's export types in
  # the schema's column order, so no key-value metadata, padding or encoding
  # choice of the author's file reaches the export. Every byte-affecting option
  # is pinned, so the same data writes the same bytes under one arrow release.
  # Arrow still adds its own ARROW:schema entry (derived from the schema built
  # here) and a created_by string naming its version.
  write_checked <- function(cand, path) {
    columns <- lapply(cand$spec$columns, function(col) {
      type <- EXPORT_TYPES[[col$type]]()
      chunks <- cand$columns[[col$name]]$cast(type)$chunks
      do.call(arrow::concat_arrays, c(chunks, list(type = type)))
    })
    names(columns) <- vapply(cand$spec$columns, function(col) col$name, "")
    arrow::write_parquet(
      do.call(arrow::Table$create, columns),
      path,
      chunk_size = 1048576L,
      version = "2.4",
      compression = "uncompressed",
      use_dictionary = FALSE,
      write_statistics = FALSE
    )
    bytes <- read_bytes(path)
    list(name = cand$spec$name, sha256 = sha256(bytes), size = length(bytes))
  }

  # Writes the checked tables and the receipt into a new folder beside
  # `output`, then renames it to `output`: the files and the receipt appear
  # together or not at all. The folder is the gate's own, so on any failure it
  # is removed whole.
  export <- function(output, schema, policy, candidates) {
    fresh <- tempfile(".export-gate-", tmpdir = dirname(output))
    if (!dir.create(fresh, showWarnings = FALSE)) {
      refuse("setup", list(finding("export_not_writable")))
    }
    done <- FALSE
    on.exit(if (!done) unlink(fresh, recursive = TRUE), add = TRUE)
    written <- lapply(candidates, function(cand) {
      write_checked(cand, file.path(fresh, cand$spec$name))
    })
    # The rewrite is uncompressed, so a compressed staging file under the cap
    # can grow past it: such a file is too_large too.
    big <- Filter(function(w) w$size > MAX_FILE_BYTES, written)
    if (length(big)) {
      refuse("structure", lapply(big, function(w) finding("too_large", w$name)))
    }
    written <- lapply(written, function(w) w[c("name", "sha256")])
    write_json(receipt(schema, policy, written), file.path(fresh, RECEIPT))
    # file.rename() warns on failure; the failure is the refusal.
    if (!suppressWarnings(file.rename(fresh, output))) {
      refuse("setup", list(finding("move_failed")))
    }
    done <- TRUE
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

  # The data file names a gate receipt lists, or NULL if `path` is not one.
  receipt_files <- function(path) {
    r <- read_json_file(path)$doc
    if (
      !is_object(r) ||
        !identical(r$format, "reg-export-gate/receipt") ||
        !is_array(r$files)
    ) {
      return(NULL)
    }
    names <- vapply(
      r$files,
      function(f) {
        ok <- is_object(f) && is_string(f$name) && grepl(FILE_NAME, f$name)
        if (ok) f$name else NA_character_
      },
      ""
    )
    if (anyNA(names)) NULL else names
  }

  # The prior export in `output` or an empty folder is removed before anything
  # is checked, so a refused run never leaves an old export looking current. A
  # prior export is a gate receipt and only files it lists, no folders; the
  # gate deletes exactly those. Anything else is not the gate's to delete.
  clear_output <- function(output) {
    if (!file.exists(output)) {
      return(invisible())
    }
    if (!dir.exists(output)) {
      refuse("setup", list(finding("output_not_gate_export")))
    }
    listing <- function() {
      list.files(output, all.files = TRUE, no.. = TRUE, include.dirs = TRUE)
    }
    present <- listing()
    if (length(present)) {
      listed <- receipt_files(file.path(output, RECEIPT))
      paths <- file.path(output, present)
      if (
        is.null(listed) ||
          !all(present %in% c(RECEIPT, listed)) ||
          any(dir.exists(paths))
      ) {
        refuse("setup", list(finding("output_not_gate_export")))
      }
      # A file that will not go is caught by the listing below.
      suppressWarnings(file.remove(paths))
    }
    if (length(listing())) {
      refuse("setup", list(finding("output_not_removable")))
    }
    # The folder was just listed empty. R has no portable removal of an empty
    # folder alone: unlink() skips folders unless recursive, and file.remove()
    # refuses them on Windows.
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
    # An existing output is resolved on disk, so another spelling of staging
    # (case, Unicode form, a link, a Windows short name) cannot pass as a
    # different folder and be emptied by clear_output(). An output that does
    # not exist yet cannot be staging.
    o <- if (dir.exists(output)) {
      normalizePath(output, winslash = "/")
    } else {
      file.path(
        normalizePath(dirname(output), winslash = "/"),
        basename(output)
      )
    }
    inside <- function(a, b) a == b || startsWith(a, paste0(b, "/"))
    if (inside(o, s) || inside(s, o)) {
      refuse("setup", list(finding("paths_overlap")))
    }
  }

  # The version R would use: a loaded namespace's own, else the installed one's,
  # else NULL. A loaded namespace is not replaced by installing over it.
  package_version_of <- function(pkg) {
    if (isNamespaceLoaded(pkg)) {
      getNamespaceVersion(pkg)
    } else if (nzchar(system.file(package = pkg))) {
      utils::packageVersion(pkg)
    }
  }

  # Same helper in reg_mona_runner/extract.R: each script is uploaded to
  # MONA alone. Installs each package into R's default library (.libPaths()[1])
  # from the repository R's options name (MONA's own mirror), so neither `lib`
  # nor `repos` is set. It installs only when that library is writable and a
  # mirror is set: otherwise install.packages() would prompt (personal library,
  # mirror menu) or add a library path. A failure is not reported here: the
  # caller checks the packages again and refuses, naming the package (one that
  # loads with a warning is internal_warning). Errors and warnings are caught:
  # run()'s handlers would otherwise report a failed install as internal_error
  # or internal_warning, without the package's name.
  install_missing <- function(pkgs) {
    lib <- .libPaths()[1]
    repos <- getOption("repos")
    # install.packages()'s own test: on Windows file.access() ignores ACLs,
    # so R creates a test folder there instead.
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

  # concat_arrays() and write_parquet(version = "2.4") arrived in arrow 8 and 9.
  check_packages <- function() {
    arrow_ok <- function() {
      v <- package_version_of("arrow")
      !is.null(v) && package_version(v) >= ARROW_FLOOR
    }
    install_missing(c(
      if (is.null(package_version_of("jsonlite"))) "jsonlite",
      # An arrow loaded below the floor cannot be replaced in this session.
      if (!arrow_ok() && !isNamespaceLoaded("arrow")) "arrow",
      if (!has_sha256()) "digest"
    ))
    missing <- c(
      if (!requireNamespace("jsonlite", quietly = TRUE)) "jsonlite_missing",
      if (!requireNamespace("arrow", quietly = TRUE)) {
        "arrow_missing"
      } else if (!arrow_ok()) {
        "arrow_too_old"
      },
      if (!has_sha256()) "sha256_missing"
    )
    if (length(missing)) {
      refuse("setup", lapply(missing, finding))
    }
  }

  run <- function(staging, output, schema_path, policy_path) {
    check_paths(staging, output)
    check_packages()
    clear_output(output)
    schema <- load_schema(schema_path)
    policy <- load_policy(policy_path, schema)
    check_inventory(staging, schema)
    candidates <- check_structure(staging, schema)
    check_disclosure(candidates, policy)
    check_backstop(candidates)
    export(output, schema, policy, candidates)
    length(candidates)
  }

  function(staging, output, schema, policy) {
    diagnostics <- if (is_string(output)) {
      file.path(dirname(output), ".diagnostics")
    }
    outcome <- tryCatch(
      withCallingHandlers(
        list(written = run(staging, output, schema, policy)),
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
    stopped <- if (is.null(outcome$written)) outcome
    written <- outcome$written
    if (is.null(stopped)) {
      write_report(diagnostics, "pass", NULL, list())
      say("export gate ", EXPORT_GATE_VERSION, ": pass, ", written, " files")
      say("  written with receipt.json to ", output)
      return(invisible("pass"))
    }
    write_report(diagnostics, "refused", stopped$code, stopped$findings)
    say("export gate ", EXPORT_GATE_VERSION, ": refused (", stopped$code, ")")
    for (f in stopped$findings) {
      say(
        "  ",
        f$rule,
        if (!is.na(f$file)) paste0(" file=", f$file),
        if (!is.na(f$column)) paste0(" column=", f$column),
        if (!is.na(f$position)) paste0(" position=", f$position)
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
