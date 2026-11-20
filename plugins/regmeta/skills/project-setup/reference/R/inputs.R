# Mode and source adapter. The mode is named on the command line; its input folder
# must exist, and a mock run refuses where real data is present. No fallback.
check_mode <- function(mode) {
  if (!dir.exists(c(real = REAL_DIR, mock = MOCK_DIR)[[mode]])) {
    stop("the ", mode, " input folder does not exist")
  }
  if (mode == "mock" && dir.exists(REAL_DIR)) stop("a mock run where real data exists")
  message("==== MODE: ", toupper(mode), " ====")
}

# LISA Individer, exactly the files LISA_FILES names, each with its delivery year,
# columns renamed from LISA_COLUMNS to the project's names. A mock file must be
# listed in reg-mock's manifest and carry its "reg_mock" metadata; a real file must
# not carry it.
read_lisa <- function(mode) {
  dir <- if (mode == "real") REAL_DIR else MOCK_DIR
  if (anyDuplicated(LISA_FILES) || anyDuplicated(names(LISA_FILES))) {
    stop("LISA_FILES repeats a file or a year")
  }
  # A missing or extra file would change the results unseen by the gate.
  if (!setequal(list.files(dir, pattern = "\\.parquet$"), LISA_FILES)) {
    stop("the input folder's Parquet files differ from LISA_FILES")
  }
  if (mode == "mock") {
    listed <- vapply(jsonlite::read_json(file.path(dir, "mock_manifest.json"))$files,
                     function(f) f$file, "")
    if (!all(LISA_FILES %in% listed)) stop("a mock input file is not in mock_manifest.json")
  }
  frames <- lapply(names(LISA_FILES), function(year) {
    t <- arrow::read_parquet(file.path(dir, LISA_FILES[[year]]), as_data_frame = FALSE)
    if (("reg_mock" %in% names(t$metadata)) != (mode == "mock")) {
      stop("an input file's mock label does not match the mode")
    }
    d <- setNames(as.data.frame(t)[LISA_COLUMNS], names(LISA_COLUMNS))
    d$year <- as.integer(year)
    d
  })
  do.call(rbind, frames)
}
