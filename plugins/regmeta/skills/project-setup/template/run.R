# The runner. Submit this file as it is: it takes no arguments.
#   On MONA:  upload it once to the project folder, submit it to the batch runner.
#   Locally:  Rscript run.R   (mock data: the project's .Rprofile sets REGMETA_MOCK;
#             without that file, REGMETA_MOCK=1 Rscript run.R)
# A run is real unless REGMETA_MOCK is set. Order: prepare -> pipeline (src/) ->
# staging -> export gate, with only the review copies and the manifest after it.
# Each batch uploads every file in src/; this file does not change between batches.

# ---- Settings (written by project-setup; nothing here is edited on MONA) ----
DATA_DIR <- "" # the delivered files' absolute folder on the server that holds them
SCHEMA <- "aggregates.schema.json" # in src/
POLICY <- "aggregates.policy.json" # in src/
# The pinned files in src/: SHA-256 of each, line endings normalized to LF.
PINS <- c(
  "export_gate.R" = "51d6f810ef0c93faccb69f748382b6cdce93243af838788f9165e094a103869a",
  "aggregates.schema.json" = "8160f4cb2250b5d22e609bf0a58935a221bd61ec407dd0403e2ccdd0537f1f93",
  "aggregates.policy.json" = "e3c0835c7963efa39f035216087c08b7558ab5f3887c3ae7bbae2ed0f0098f5c",
  "cell_key.R" = "585080051893308794fd43a298a867236e6861d1eb2b1c791b7d5aac660e197e"
)
# Every package the run loads. Locally the same names are in rproject.toml (rv).
PACKAGES <- c("targets", "arrow", "jsonlite", "openssl")
# -----------------------------------------------------------------------------

say <- function(...) cat(format(Sys.time(), "%H:%M:%S"), ..., "\n")

# The project folder is where this file is, under Rscript and under a batch runner.
project_dir <- function() {
  args <- commandArgs(FALSE)
  file <- sub("^--file=", "", args[grepl("^--file=", args)])
  if (!length(file) && "-f" %in% args) {
    file <- args[match("-f", args) + 1]
  }
  dir <- if (length(file)) dirname(normalizePath(file[1])) else getwd()
  if (!dir.exists(file.path(dir, "src"))) {
    stop(
      "no src/ folder beside run.R in ",
      dir,
      ": start run.R from the project folder"
    )
  }
  dir
}
setwd(project_dir())
say("project folder:", getwd())

# ---- Prepare ----
mock <- nzchar(Sys.getenv("REGMETA_MOCK"))
mode <- if (mock) "mock" else "real"
say("==== MODE:", toupper(mode), "====")
if (mock && nzchar(DATA_DIR) && dir.exists(DATA_DIR)) {
  stop("a mock run where the delivered data exists (", DATA_DIR, ")")
}
data_dir <- if (mock) file.path(".regmeta", "mock-data") else DATA_DIR
if (!nzchar(data_dir) || !dir.exists(data_dir)) {
  stop(
    "the ",
    mode,
    " input folder does not exist: '",
    data_dir,
    "'",
    if (!mock) ". For a mock run, start it as: REGMETA_MOCK=1 Rscript run.R"
  )
}

missing <- PACKAGES[
  !vapply(PACKAGES, requireNamespace, logical(1), quietly = TRUE)
]
if (file.exists("rproject.toml")) {
  # Locally rv owns the library. A package named in only one of the two lists would
  # first fail on the server, after an upload.
  # simplify: reads one package per line of `dependencies = [...]`, as rv writes it.
  toml <- sub("#.*", "", readLines("rproject.toml", warn = FALSE))
  from <- grep("^dependencies\\s*=\\s*\\[", toml)
  to <- from + grep("\\]\\s*$", toml[from:length(toml)])[1] - 1
  deps <- grep("\"", toml[from:to], value = TRUE)
  named <- ifelse(
    grepl("name\\s*=", deps),
    sub(".*name\\s*=\\s*\"([^\"]+)\".*", "\\1", deps),
    sub("^[^\"]*\"([^\"]+)\".*", "\\1", deps)
  )
  if (!setequal(named, PACKAGES)) {
    stop(
      "PACKAGES in run.R and dependencies in rproject.toml differ: ",
      paste(
        union(setdiff(named, PACKAGES), setdiff(PACKAGES, named)),
        collapse = ", "
      )
    )
  }
  if (length(missing)) {
    stop("not installed: ", paste(missing, collapse = ", "), ". Run: rv sync")
  }
}
if (length(missing)) {
  say("installing packages:", paste(missing, collapse = ", "))
  install.packages(missing)
  missing <- missing[
    !vapply(missing, requireNamespace, logical(1), quietly = TRUE)
  ]
  if (length(missing)) {
    stop("could not install: ", paste(missing, collapse = ", "))
  }
}

# A file's identity: SHA-256 of its bytes without carriage returns, so an upload that
# rewrites line endings does not change it.
sha256 <- function(bytes) paste(as.character(openssl::sha256(bytes)))
file_hash <- function(path) {
  bytes <- readBin(path, "raw", file.size(path))
  sha256(bytes[bytes != as.raw(13)])
}
for (name in names(PINS)) {
  path <- file.path("src", name)
  if (!file.exists(path)) {
    stop("src/", name, " is missing: upload every file in src/")
  }
  if (file_hash(path) != PINS[[name]]) {
    stop("src/", name, " is not the pinned file")
  }
}
say("pinned files: ok")

staging <- file.path(".regmeta", "staging")
export <- file.path("output", "export")
review <- file.path("output", "review")
manifest <- file.path("output", "run_manifest.json")
dir.create(
  file.path(".regmeta", "keys"),
  recursive = TRUE,
  showWarnings = FALSE
)
dir.create("output", showWarnings = FALSE)
# Nothing from an earlier run may look current, whatever fails below.
unlink(c(export, review, manifest), recursive = TRUE)
if (any(file.exists(c(export, review, manifest)))) {
  stop("could not remove the previous run's output")
}

# ---- Pipeline ----
# src/pipeline.R declares the steps as targets and ends in the target `release`: the
# disclosure-treated tables, named by the schema's file names. Unchanged steps are
# skipped; their results are in .regmeta/targets/.
Sys.setenv(
  REGMETA_MODE = mode,
  REGMETA_DATA = normalizePath(data_dir),
  REGMETA_KEYS = file.path(getwd(), ".regmeta", "keys", "cell_keys.csv")
)
# One store per mode: a real run never reuses a mock run's results.
store <- file.path(".regmeta", "targets", mode)
say("pipeline: start")
targets::tar_make(script = file.path("src", "pipeline.R"), store = store)
release <- targets::tar_read_raw("release", store = store)
say("pipeline: done")

source(file.path("src", "staging.R"))
source(file.path("src", "cell_key.R"))
source(file.path("src", "export_gate.R"))
write_staging(release, file.path("src", SCHEMA), staging)

# ---- Export gate: the last step. A refusal stops the run here. ----
export_gate(
  staging = staging,
  output = export,
  schema = file.path("src", SCHEMA),
  policy = file.path("src", POLICY)
)

# ---- After a pass: reading copies and the record of what ran. Neither leaves. ----
dir.create(review)
for (file in list.files(export, pattern = "\\.parquet$")) {
  write.csv(
    arrow::read_parquet(file.path(export, file)),
    file.path(review, sub("\\.parquet$", ".csv", file)),
    row.names = FALSE
  )
}
# Sorted by bytes, so the id does not depend on the machine's locale.
ran <- c("run.R", file.path("src", sort(list.files("src"), method = "radix")))
hashes <- vapply(ran, file_hash, "")
source_id <- substr(
  sha256(charToRaw(paste0(ran, ":", hashes, collapse = "\n"))),
  1,
  8
)
keys <- Sys.getenv("REGMETA_KEYS")
jsonlite::write_json(
  list(
    mode = mode,
    time = format(Sys.time(), tz = "UTC", usetz = TRUE),
    source_id = source_id,
    files = as.list(hashes),
    export_gate = EXPORT_GATE_VERSION,
    cell_key = CELL_KEY_VERSION,
    r = as.character(getRversion()),
    packages = lapply(setNames(nm = PACKAGES), function(p) {
      as.character(packageVersion(p))
    }),
    key_epoch = if (file.exists(keys)) ck_keys_read(keys)$key_epoch[[1]] else NA
  ),
  manifest,
  auto_unbox = TRUE,
  pretty = TRUE
)
say("export:", export, "| reading copies:", review)
say("==== SOURCE ID:", source_id, "====")
