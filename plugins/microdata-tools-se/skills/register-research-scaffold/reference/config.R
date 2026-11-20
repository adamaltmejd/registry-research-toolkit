# Machine paths, all outside the project folder. The researcher sets REAL_DIR and
# the real key file on MONA; locally only the mock paths exist. The mock folders
# carry the project folder's name (run.R and make_keys.R run from that folder; the
# physical name, as `pwd -P` gives it), so two projects under one parent folder
# never share them.
PROJECT <- basename(getwd())
REAL_DIR <- "<the folder the delivered files are in; on MONA the project's PXXXX_Data>"
MOCK_DIR <- file.path("..", paste0(PROJECT, "-mock")) # reg-mock generate --out
KEY_FILE <- c(
  real = "<on MONA the project's PXXXX_Gem>/.regmeta/cell_keys.csv",
  mock = file.path("..", paste0(PROJECT, "-mock-keys"), "cell_keys.csv")
)
SCHEMA <- "export_gate/aggregates.schema.json"
POLICY <- "export_gate/aggregates.policy.json"

# The delivery, from the order answer (order, or materialize_order): every file
# it delivers, named by the delivery year the answer gives it (never parsed from
# the file name), and the stored column names. reg-mock mirrors them.
LISA_FILES <- c("2021" = "scb_lisa_individer_2021.parquet",
                "2022" = "scb_lisa_individer_2022.parquet")
LISA_COLUMNS <- c(person_id = "PersonNr", sex = "Kon", income = "DispInk04")
