# Mode and source adapter. The mode is chosen once, from which input folder exists,
# and never falls back to the other.
select_mode <- function() {
  mode <- if (dir.exists(REAL_DIR)) "real" else if (dir.exists(MOCK_DIR)) "mock" else
    stop("neither the real nor the mock input folder exists")
  message("==== MODE: ", toupper(mode), " ====")
  mode
}

# LISA Individer, exactly the files LISA_FILES names, columns renamed from
# LISA_COLUMNS to the project's names. A mock file must be listed in reg-mock's
# manifest and carry its "reg_mock" metadata; a real file must not carry it.
read_lisa <- function(mode) {
  dir <- if (mode == "real") REAL_DIR else MOCK_DIR
  # A missing or extra year would change the results unseen by the gate.
  if (!setequal(list.files(dir, pattern = "\\.parquet$"), LISA_FILES)) {
    stop("the input folder's Parquet files differ from LISA_FILES")
  }
  if (mode == "mock") {
    listed <- vapply(jsonlite::read_json(file.path(dir, "mock_manifest.json"))$files,
                     function(f) f$file, "")
    if (!all(LISA_FILES %in% listed)) stop("a mock input file is not in mock_manifest.json")
  }
  frames <- lapply(LISA_FILES, function(f) {
    t <- arrow::read_parquet(file.path(dir, f), as_data_frame = FALSE)
    if (("reg_mock" %in% names(t$metadata)) != (mode == "mock")) {
      stop("an input file's mock label does not match the mode")
    }
    d <- setNames(as.data.frame(t)[LISA_COLUMNS], names(LISA_COLUMNS))
    d$year <- as.integer(sub("\\D+", "", sub("\\.parquet$", "", f)))
    d
  })
  do.call(rbind, frames)
}
