# Operator-only: copy exactly reviewed ZIP bytes into model-readable storage.
#
# This records human approval, not disclosure clearance or OS access isolation.
#
# Usage: Rscript approve_release.R SOURCE --destination DIR --sha256 HEX --commit HEX
#   --sha256       Hash explicitly approved after review; do not compute inline.
#   --commit       Execution commit checked during review.
#
# Prints APPROVED_FOR_MODEL_ACCESS on success; on any failure prints
# APPROVAL_FAILED to stderr and exits 2. Needs base R; SHA-256 comes from
# tools::sha256sum (R >= 4.5) or else the digest package, as in export_gate.R.

MAX_BYTES <- 64 * 1024 * 1024

sha256 <- function(bytes) {
  tools_ns <- asNamespace("tools")
  if (exists("sha256sum", envir = tools_ns, inherits = FALSE)) {
    return(unname(get("sha256sum", envir = tools_ns)(bytes = bytes)))
  }
  if (!requireNamespace("digest", quietly = TRUE)) {
    stop("SHA-256 needs R >= 4.5 (tools::sha256sum) or the digest package")
  }
  digest::digest(bytes, algo = "sha256", serialize = FALSE)
}

# Python's non-strict Path.resolve(): resolve symlinks component by component
# and collapse `..` even below a missing component. normalizePath(mustWork =
# FALSE) returns a path with a missing component unchanged, `..` included.
canonical <- function(path) {
  path <- gsub("\\", "/", path, fixed = TRUE)
  if (!grepl("^(/|[A-Za-z]:/)", path)) {
    path <- paste0(getwd(), "/", path)
  }
  parts <- strsplit(path, "/", fixed = TRUE)[[1]]
  resolved <- paste0(parts[[1]], "/")
  for (part in parts[-1]) {
    if (part %in% c("", ".")) {
      next
    }
    if (part == "..") {
      resolved <- dirname(resolved)
      next
    }
    resolved <- paste0(sub("/$", "", resolved), "/", part)
    if (file.exists(resolved)) {
      resolved <- normalizePath(resolved, winslash = "/", mustWork = TRUE)
    }
  }
  resolved <- sub("(.)/+$", "\\1", resolved)
  if (.Platform$OS.type == "windows") tolower(resolved) else resolved
}

approve <- function(source, destination, expected_sha256, commit) {
  if (
    !grepl("^[0-9a-f]{64}$", expected_sha256) ||
      !grepl("^[0-9a-f]{40}$", commit)
  ) {
    stop("INVALID_REVIEW_REFERENCE")
  }
  # R's file functions expand a leading `~`; expand once so the guard checks the
  # paths the IO below actually touches.
  source <- path.expand(source)
  destination <- path.expand(destination)
  src <- canonical(source)
  dst <- canonical(destination)
  prefix <- if (endsWith(dst, "/")) dst else paste0(dst, "/")
  if (src == dst || startsWith(src, prefix)) {
    stop("REVIEW_AND_APPROVED_STORAGE_MUST_DIFFER")
  }
  # Read once, bounded, before creating anything model-readable. Hash mismatch
  # must never expose temporary unapproved bytes in the destination.
  stream <- file(source, "rb")
  data <- tryCatch(
    readBin(stream, "raw", n = MAX_BYTES + 1),
    finally = close(stream)
  )
  if (length(data) > MAX_BYTES || sha256(data) != expected_sha256) {
    stop("REVIEWED_BYTES_CHANGED_OR_TOO_LARGE")
  }
  # Every value is validated hex, a fixed string or a timestamp, so no JSON
  # escaping is needed.
  receipt <- sprintf(
    paste0(
      "{\n",
      "  \"purpose\": \"human-approved-for-model-access\",\n",
      "  \"sha256\": \"%s\",\n",
      "  \"source_commit\": \"%s\",\n",
      "  \"approved_at\": \"%s\"\n",
      "}"
    ),
    expected_sha256,
    commit,
    format(Sys.time(), "%Y-%m-%dT%H:%M:%OS6+00:00", tz = "UTC")
  )
  dir.create(destination, recursive = TRUE, showWarnings = FALSE)
  if (!dir.exists(destination)) {
    stop("destination not created")
  }
  # A reader must match receipt to ZIP. An interrupted two-file replacement
  # can leave a mismatch; that must fail closed rather than reuse approval.
  # Temporary files go in a fresh private directory, refused if the name
  # exists, so a link planted at a temporary name is never followed.
  staging <- tempfile(".approve-", tmpdir = destination)
  if (!dir.create(staging, showWarnings = FALSE, mode = "0700")) {
    stop("staging directory not created")
  }
  on.exit(unlink(staging, recursive = TRUE))
  payloads <- list("current.zip" = data, "approval.json" = charToRaw(receipt))
  for (name in names(payloads)) {
    temporary <- file.path(staging, name)
    writeBin(payloads[[name]], temporary)
    Sys.chmod(temporary, "640", use_umask = FALSE)
    if (!file.rename(temporary, file.path(destination, name))) {
      stop("replace failed")
    }
  }
  invisible(receipt)
}

usage <- function() {
  "usage: Rscript approve_release.R SOURCE --destination DIR --sha256 HEX --commit HEX\n"
}

parse_args <- function(args) {
  options <- c("--destination", "--sha256", "--commit")
  out <- list()
  positional <- character()
  i <- 1
  while (i <= length(args)) {
    arg <- args[[i]]
    key <- sub("=.*$", "", arg)
    if (key %in% options) {
      if (grepl("=", arg, fixed = TRUE)) {
        value <- sub("^[^=]*=", "", arg)
      } else if (i < length(args)) {
        i <- i + 1
        value <- args[[i]]
      } else {
        return(NULL)
      }
      out[[sub("^--", "", key)]] <- value
    } else if (startsWith(arg, "-")) {
      return(NULL)
    } else {
      positional <- c(positional, arg)
    }
    i <- i + 1
  }
  if (
    length(positional) != 1 || !all(sub("^--", "", options) %in% names(out))
  ) {
    return(NULL)
  }
  out$source <- positional
  out
}

main <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  if (any(args %in% c("-h", "--help"))) {
    cat(usage())
    quit(status = 0)
  }
  parsed <- parse_args(args)
  if (is.null(parsed)) {
    cat(usage(), file = stderr())
    quit(status = 2)
  }
  ok <- tryCatch(
    {
      approve(parsed$source, parsed$destination, parsed$sha256, parsed$commit)
      TRUE
    },
    error = function(e) FALSE,
    warning = function(w) FALSE
  )
  if (!ok) {
    cat("APPROVAL_FAILED\n", file = stderr())
    quit(status = 2)
  }
  cat("APPROVED_FOR_MODEL_ACCESS\n")
}

main()
