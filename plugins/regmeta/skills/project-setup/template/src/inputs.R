# The delivered files, from the order answer (project-data skill): each source's files
# by delivery year (never parsed from a file name) and its stored column names by the
# project's names. Mock data mirrors them.
SOURCES <- list(
  # <source> = list(
  #   files = c("<year>" = "<file>.parquet"),
  #   columns = c(person_id = "<stored name>", <name> = "<stored name>")
  # )
)

# One source's files, by year. As a `format = "file"` target it makes the steps that
# read them run again when a file changes (a new delivery, regenerated mock data).
source_files <- function(name) {
  check_inputs()
  source <- SOURCES[[name]]
  if (is.null(source)) stop("no source named ", name, " in SOURCES")
  file.path(Sys.getenv("REGMETA_DATA"), unname(source$files))
}

# One source as a data frame with the project's column names and `year`.
# simplify: Parquet only; add a reader per format when a delivery needs one.
read_source <- function(name, files = source_files(name)) {
  source <- SOURCES[[name]]
  frames <- lapply(seq_along(files), function(i) {
    year <- names(source$files)[[i]]
    d <- as.data.frame(arrow::read_parquet(files[[i]],
                                           col_select = unname(source$columns)))
    d <- setNames(d[source$columns], names(source$columns))
    d$year <- as.integer(year)
    d
  })
  do.call(rbind, frames)
}

# Every file SOURCES names exists; other files in the folder are never read. A mock
# file is listed in reg-mock's manifest and carries its "reg_mock" metadata; a real
# file does not carry it.
check_inputs <- function() {
  dir <- Sys.getenv("REGMETA_DATA")
  mock <- Sys.getenv("REGMETA_MODE") == "mock"
  files <- unlist(lapply(SOURCES, function(s) unname(s$files)))
  if (anyDuplicated(files)) stop("SOURCES names a file twice")
  absent <- files[!file.exists(file.path(dir, files))]
  if (length(absent)) stop("not in the input folder: ", paste(absent, collapse = ", "))
  if (mock) {
    listed <- vapply(jsonlite::read_json(file.path(dir, "mock_manifest.json"))$files,
                     function(f) f$file, "")
    if (!all(files %in% listed)) stop("a mock input file is not in mock_manifest.json")
  }
  for (file in files) {
    # No columns are read: only the file's metadata.
    meta <- arrow::read_parquet(file.path(dir, file), col_select = character(),
                                as_data_frame = FALSE)$metadata
    labelled <- "reg_mock" %in% names(meta)
    if (labelled != mock) stop(file, ": the mock label does not match the mode")
  }
  invisible(TRUE)
}
