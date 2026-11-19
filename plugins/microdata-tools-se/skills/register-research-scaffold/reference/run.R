# The batch: inputs -> analysis -> disclosure -> staging -> export gate, in that
# order, with nothing after the gate. Run from the project folder:
# Rscript run.R mock|real

# First, using nothing sourced: no earlier export of this mode may look current.
mode <- commandArgs(trailingOnly = TRUE)[1]
if (!mode %in% c("mock", "real")) stop("usage: Rscript run.R mock|real")
run <- file.path("out", mode)
old <- file.path(run, c("export", "run_record.json"))
unlink(old, recursive = TRUE)
if (any(file.exists(old))) stop("could not remove the previous export")

source("config.R")
source("R/inputs.R")
check_mode(mode)
dir.create(run, recursive = TRUE, showWarnings = FALSE)
source("export_gate/cell_key.R")
source("export_gate/export_gate.R")
for (f in list.files("R", full.names = TRUE)) source(f)

# On MONA there is no .git: the commit comes from SOURCE_COMMIT in the upload.
commit <- if (mode == "real") readLines("SOURCE_COMMIT", n = 1) else
  system2("git", c("rev-parse", "HEAD"), stdout = TRUE)

persons <- income_by_sex(read_lisa(mode))
released <- disclose(persons, table = "income_by_sex", keys = KEY_FILE[[mode]])
write_staging(released$tables, SCHEMA, file.path(run, "staging"))

# The run record stays beside the export, never in it.
jsonlite::write_json(list(
  mode = mode, time = format(Sys.time(), tz = "UTC", usetz = TRUE), commit = commit,
  key_epoch = released$key_epoch, export_gate = EXPORT_GATE_VERSION,
  cell_key = CELL_KEY_VERSION
), file.path(run, "run_record.json"), auto_unbox = TRUE, pretty = TRUE)

export_gate(staging = file.path(run, "staging"), output = file.path(run, "export"),
            schema = SCHEMA, policy = POLICY)
