# Shared helpers: data IO, assertions, target factories
#
# These functions are used throughout the pipeline. They are generic and not
# project-specific. If you add project-specific helpers, put them in a
# separate file (e.g. data_processing.R) and source() it from pipeline.R.

# -- Column normalization ------------------------------------------------------

#' Normalize MONA column-name casing
#'
#' Most MONA exports use `P{nnn}_LopNr_*` (camel-case), but a handful of
#' files ship with lowercase `P{nnn}_Lopnr_*` (the Distansutb survey and
#' Skolkod key are the known offenders). Joining across both forms silently
#' drops rows. This helper renames any `_Lopnr_` column to `_LopNr_` in
#' place, so downstream code can assume a single canonical form.
#'
#' `read_data()` calls this automatically after loading. Call it explicitly
#' on any data.table you built some other way.
#'
#' @param .dt A data.table (modified in place).
#' @return `.dt` invisibly, for use in pipes.
normalize_mona_cols <- function(.dt) {
  old <- names(.dt)
  new <- sub("_Lopnr_", "_LopNr_", old)
  if (any(old != new)) {
    setnames(.dt, old, new)
  }
  invisible(.dt)
}

# -- Data IO -------------------------------------------------------------------

#' Read one or more CSV/TSV files into a data.table
#'
#' Wraps data.table::fread with defaults suited for MONA registry data:
#' - Reads multiple files and row-binds them
#' - Supports column selection to avoid loading unnecessary data
#' - Respects EXPORT_ROWS option for quick local testing
#' - Normalizes `_Lopnr_` -> `_LopNr_` column casing (see normalize_mona_cols)
#'
#' When `cols` is provided, column names are matched against the file
#' header in both canonical (`_LopNr_`) and lowercase (`_Lopnr_`) form so
#' the user can always pass canonical names regardless of which file
#' variant they are reading.
#'
#' @param path Character vector of file paths. Multiple paths are read and
#'   bound together (rbindlist with fill = TRUE).
#' @param key Character vector of columns to set as data.table key.
#' @param cols Character vector of column names to read (canonical form,
#'   i.e. `_LopNr_` not `_Lopnr_`). NULL reads all columns.
#' @param bind If TRUE (default), rbindlist multiple files into one table.
#' @param idcol If non-NULL and bind is TRUE, adds a column with the source
#'   file path. Useful for tracing which file a row came from.
#' @param nrows Max rows to read per file. Defaults to Inf, but respects
#'   options("EXPORT_ROWS") for quick local iteration.
#' @param ... Additional arguments passed to data.table::fread (e.g.
#'   encoding = "Latin-1" for MONA files).
#'
#' @return A data.table (or list of data.tables if bind = FALSE).
read_data <- function(
  path,
  key = NULL,
  cols = NULL,
  bind = TRUE,
  idcol = NULL,
  nrows = getOption("EXPORT_ROWS", default = Inf),
  ...
) {
  library(data.table)

  .dt <- lapply(path, function(p) {
    select <- cols
    if (!is.null(cols)) {
      hdr <- names(fread(p, nrows = 0L))
      variants <- sub("_LopNr_", "_Lopnr_", cols)
      select <- unique(c(cols, variants))
      select <- select[select %in% hdr]
    }
    dt <- fread(p, select = select, key = key, nrows = nrows, ...)
    normalize_mona_cols(dt)
  })

  if (bind && is.list(.dt)) {
    if (!is.null(idcol)) {
      names(.dt) <- path
    }
    .dt <- rbindlist(.dt, fill = TRUE, idcol = idcol)
  }

  return(.dt)
}

# -- Assertions ----------------------------------------------------------------

#' Assert that a data.table is unique by the given columns
#'
#' Use this as a guard after merges and data processing steps. If the table
#' has duplicate rows by the key columns, it stops with an informative error.
#' This catches a common class of bugs: merges that silently multiply rows.
#'
#' @param .dt A data.table.
#' @param cols Character vector of column names that should uniquely identify
#'   each row.
#' @param label A human-readable label for the table (used in the error
#'   message, e.g. "students after merge").
#' @param na_rm If TRUE (default), rows with NA in any key column are excluded
#'   from the uniqueness check. Set to FALSE if NAs in keys are meaningful.
#'
#' @return .dt unchanged (invisible pass-through for piping).
assert_unique_by <- function(.dt, cols, label, na_rm = TRUE) {
  keys <- .dt[, ..cols]

  if (na_rm) {
    keys <- keys[stats::complete.cases(keys)]
  }

  if (anyDuplicated(keys) == 0L) {
    return(.dt)
  }

  stop(
    sprintf("%s is not unique by `%s`.", label, paste(cols, collapse = ", ")),
    call. = FALSE
  )
}

# -- Text output ---------------------------------------------------------------

#' Write lines to a file with UTF-8 encoding
#'
#' Ensures consistent encoding for text outputs (logs, markdown reports).
#' Returns the file path so it can be used as a targets file target.
save_text <- function(lines, fn) {
  writeLines(enc2utf8(lines), fn, useBytes = TRUE)
  fn
}

# -- Target factories ----------------------------------------------------------
#
# These create targets::tar_target() calls with standard output patterns.
# They reduce boilerplate for the common case of "compute a table/plot and
# save it to the output directory."
#
# Usage in pipeline.R:
#   tar_save_csv(my_summary, compute_summary(raw_data))
#   tar_save_plot(my_figure, make_figure(analysis_data))

#' Create a target that computes a data.table and saves it as CSV
#'
#' Produces two targets:
#'   1. {name}     -- the data.table (format: fst_dt for fast caching)
#'   2. {name}_f   -- the CSV file path (format: file, tracks the output)
#'
#' The CSV is written to {output_dir}/tables/{name}.csv.
tar_save_csv <- function(name, cmd) {
  tar_name_cmd <- deparse(substitute(name))
  tar_name_file <- paste0(tar_name_cmd, "_f")
  filename <- paste0(tar_name_cmd, ".csv")
  sym_data <- as.symbol(tar_name_cmd)

  list(
    targets::tar_target_raw(
      tar_name_cmd,
      substitute(cmd),
      format = "fst_dt"
    ),
    targets::tar_target_raw(
      tar_name_file,
      substitute(
        {
          fn <- file.path(output_dir, "tables", filename)
          data.table::fwrite(data, fn)
          fn
        },
        env = list(data = sym_data, filename = filename)
      ),
      format = "file"
    )
  )
}

#' Create a target that produces a ggplot and saves it in multiple formats
#'
#' The plot is saved to {output_dir}/plots/{name}.{ext} for each format.
#' Default formats: SVG (vector, good for papers) and PNG (raster, good for
#' quick review). Calls plot_theme() before rendering.
#'
#' @param name Target name (unquoted).
#' @param cmd Expression that returns a ggplot object.
#' @param format Character vector of file extensions (default: c(".svg", ".png")).
#' @param ... Additional arguments passed to save_plot (e.g. width, height).
tar_save_plot <- function(name, cmd, format = c(".svg", ".png"), ...) {
  target_name <- deparse(substitute(name))
  filenames <- paste0(target_name, format)

  targets::tar_target_raw(
    target_name,
    substitute({
      plot_theme()
      files <- file.path(output_dir, "plots", filenames)
      p <- cmd
      sapply(files, save_plot, .plot = p, ...)
    }),
    format = "file"
  )
}
