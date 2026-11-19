# Once per project and mode: the cell-key record keys for every person in the
# input (key epoch 1). On MONA the researcher runs it once on the full extract.
# Never rerun for a new epoch without the researcher's explicit decision.
source("config.R")
source("export_gate/cell_key.R")
source("R/inputs.R")
mode <- select_mode()
dir.create(dirname(KEY_FILE[[mode]]), recursive = TRUE, mode = "0700", showWarnings = FALSE)
ck_keys_create(unique(read_lisa(mode)$person_id), KEY_FILE[[mode]])
