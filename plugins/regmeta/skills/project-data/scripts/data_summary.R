# Data summary: which files a delivery holds, their columns, types and row counts.
# It writes no observed value. Submit it as it is on the server that holds the data
# (on MONA: upload it, submit it to the batch runner); it takes no arguments and writes
# data_summary.csv to the working directory. Review that file, then send it out.

# ---- Settings (written by the project-data skill; nothing here is edited) ----
DATA_DIR <- "" # the delivered files' absolute folder, forward slashes, never relative
# -----------------------------------------------------------------------------

# A scan that fails leaves no summary from an earlier one.
out <- file.path(getwd(), "data_summary.csv")
unlink(out)
if (file.exists(out)) {
  stop("could not remove the previous ", out)
}
if (!nzchar(DATA_DIR) || !dir.exists(DATA_DIR)) {
  stop("DATA_DIR does not exist: '", DATA_DIR, "'")
}
need <- function(package) {
  if (!requireNamespace(package, quietly = TRUE)) {
    install.packages(package)
  }
  if (!requireNamespace(package, quietly = TRUE)) {
    stop("could not install ", package)
  }
}

# One file's columns: name, type and position, and the file's row count.
columns <- function(path, type) {
  if (type == "parquet") {
    need("arrow")
    reader <- arrow::ParquetFileReader$create(path)
    schema <- reader$GetSchema()
    list(
      names = schema$names,
      rows = reader$num_rows,
      types = vapply(schema$fields, function(f) f$type$ToString(), "")
    )
  } else if (type %in% c("dta", "sas7bdat")) {
    need("haven")
    read <- if (type == "dta") haven::read_dta else haven::read_sas
    head <- read(path, n_max = 0)
    list(
      names = names(head),
      rows = nrow(read(path, col_select = 1)),
      types = vapply(head, function(x) class(x)[1], "")
    )
  } else {
    # Delimited text: types are guessed from the first 1000 rows; rows are lines - 1.
    first <- readLines(path, n = 1, warn = FALSE)
    sep <- if (grepl("\t", first)) {
      "\t"
    } else if (grepl(";", first)) {
      ";"
    } else {
      ","
    }
    head <- utils::read.table(
      path,
      sep = sep,
      header = TRUE,
      nrows = 1000,
      quote = "\"",
      check.names = FALSE,
      comment.char = "",
      fill = TRUE
    )
    # Records, not lines: a quoted field can hold a line break.
    # simplify: count.fields holds one integer per line; count in chunks if a
    # delimited file passes a few hundred million lines.
    fields <- utils::count.fields(
      path,
      sep = sep,
      quote = "\"",
      comment.char = "",
      blank.lines.skip = FALSE
    )
    list(
      names = names(head),
      rows = sum(!is.na(fields)) - 1,
      types = vapply(head, function(x) class(x)[1], "")
    )
  }
}

# Paths are written relative to DATA_DIR: the folders above it are not the delivery.
files <- list.files(DATA_DIR, recursive = TRUE, full.names = TRUE)
folder <- dirname(list.files(DATA_DIR, recursive = TRUE))
types <- tolower(tools::file_ext(files))
known <- types %in% c("parquet", "dta", "sas7bdat", "csv", "txt", "tsv")
summary <- do.call(
  rbind,
  lapply(which(known), function(i) {
    cat(format(Sys.time(), "%H:%M:%S"), basename(files[i]), "\n")
    cols <- columns(files[i], types[i])
    data.frame(
      table_schema = folder[i],
      table_name = basename(files[i]),
      table_type = types[i],
      column_name = cols$names,
      data_type = unname(cols$types),
      character_maximum_length = NA,
      numeric_precision = NA,
      numeric_scale = NA,
      ordinal_position = seq_along(cols$names),
      row_count = cols$rows
    )
  })
)
# A file of another type is listed by name only.
blank <- function(value) rep(value, sum(!known))
other <- data.frame(
  table_schema = folder[!known],
  table_name = basename(files[!known]),
  table_type = types[!known],
  column_name = blank(NA_character_),
  data_type = blank(NA_character_),
  character_maximum_length = blank(NA),
  numeric_precision = blank(NA),
  numeric_scale = blank(NA),
  ordinal_position = blank(NA_integer_),
  row_count = blank(NA_real_)
)
utils::write.csv(
  rbind(summary, other),
  out,
  row.names = FALSE,
  na = "",
  fileEncoding = "UTF-8"
)
cat("wrote", out, "\n")
