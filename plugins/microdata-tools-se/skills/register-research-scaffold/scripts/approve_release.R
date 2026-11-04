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
  digest::digest(bytes, algo = "sha256", serialize = FALSE)
}

canonical <- function(path) {
  path <- normalizePath(path, winslash = "/", mustWork = FALSE)
  path <- sub("/+$", "", path)
  if (.Platform$OS.type == "windows") tolower(path) else path
}

approve <- function(source, destination, expected_sha256, commit) {
  if (
    !grepl("^[0-9a-f]{64}$", expected_sha256) ||
      !grepl("^[0-9a-f]{40}$", commit)
  ) {
    stop("INVALID_REVIEW_REFERENCE")
  }
  src <- canonical(source)
  dst <- canonical(destination)
  if (src == dst || startsWith(src, paste0(dst, "/"))) {
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
  old_umask <- Sys.umask("077")
  on.exit(Sys.umask(old_umask))
  payloads <- list("current.zip" = data, "approval.json" = charToRaw(receipt))
  for (name in names(payloads)) {
    temporary <- tempfile(tmpdir = destination)
    tryCatch(
      {
        writeBin(payloads[[name]], temporary)
        Sys.chmod(temporary, "640", use_umask = FALSE)
        if (!file.rename(temporary, file.path(destination, name))) {
          stop("replace failed")
        }
      },
      finally = unlink(temporary)
    )
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
