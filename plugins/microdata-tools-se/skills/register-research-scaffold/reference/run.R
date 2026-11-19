# The batch: inputs -> analysis -> disclosure -> staging -> export gate, in that
# order, with nothing after the gate. Run from the project folder: Rscript run.R
source("config.R")
source("export_gate/cell_key.R")
source("export_gate/export_gate.R")
for (f in list.files("R", full.names = TRUE)) source(f)

mode <- select_mode()
run <- file.path("out", mode)
dir.create(run, recursive = TRUE, showWarnings = FALSE)
# A failed run must not leave an earlier export looking current.
unlink(file.path(run, c("export", "run_record.json")), recursive = TRUE)

persons <- income_by_sex(read_lisa(mode))
released <- disclose(persons, table = "income_by_sex", keys = KEY_FILE[[mode]])
write_staging(released$tables, SCHEMA, file.path(run, "staging"))

# The run record stays beside the export, never in it.
jsonlite::write_json(list(
  mode = mode, time = format(Sys.time(), tz = "UTC", usetz = TRUE),
  commit = tryCatch(system2("git", c("rev-parse", "HEAD"), stdout = TRUE, stderr = FALSE),
                    error = function(e) NA, warning = function(w) NA),
  key_epoch = released$key_epoch, export_gate = EXPORT_GATE_VERSION,
  cell_key = CELL_KEY_VERSION
), file.path(run, "run_record.json"), auto_unbox = TRUE, pretty = TRUE)

export_gate(staging = file.path(run, "staging"), output = file.path(run, "export"),
            schema = SCHEMA, policy = POLICY)
