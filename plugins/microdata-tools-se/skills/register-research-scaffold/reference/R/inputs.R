# Mode and source adapter. The mode is chosen once, from which input folder exists,
# and never falls back to the other.
select_mode <- function() {
  mode <- if (dir.exists(REAL_DIR)) "real" else if (dir.exists(MOCK_DIR)) "mock" else
    stop("neither the real nor the mock input folder exists")
  message("==== MODE: ", toupper(mode), " ====")
  mode
}

# LISA Individer, one Parquet file per year (project_data.json, source "lisa").
# A mock file must be listed in reg-mock's manifest and carry its "reg_mock"
# metadata; a real file must not carry it.
read_lisa <- function(mode) {
  dir <- if (mode == "real") REAL_DIR else MOCK_DIR
  files <- list.files(dir, pattern = "^scb_lisa_individer_[0-9]{4}\\.parquet$")
  if (length(files) == 0) stop("no LISA files in the ", mode, " input folder")
  if (mode == "mock") {
    listed <- vapply(jsonlite::read_json(file.path(dir, "mock_manifest.json"))$files,
                     function(f) f$file, "")
    if (!all(files %in% listed)) stop("a mock input file is not in mock_manifest.json")
  }
  frames <- lapply(files, function(f) {
    t <- arrow::read_parquet(file.path(dir, f), as_data_frame = FALSE)
    if (("reg_mock" %in% names(t$metadata)) != (mode == "mock")) {
      stop("an input file's mock label does not match the mode")
    }
    d <- as.data.frame(t)[c("PersonNr", "Kon", "DispInkHB04")]
    d$year <- as.integer(sub("\\D+", "", sub("\\.parquet$", "", f)))
    d
  })
  do.call(rbind, frames)
}
