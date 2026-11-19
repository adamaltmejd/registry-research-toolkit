# Machine paths, all outside the project folder. The researcher sets REAL_DIR and
# the real key file on MONA; locally only the mock paths exist.
REAL_DIR <- "<the folder the steward's extract lands in>"
MOCK_DIR <- "../reference-mock" # reg-mock generate --out
KEY_FILE <- c(
  real = "<a folder on MONA only the researcher can read>/cell_keys.csv",
  mock = "../reference-mock-keys/cell_keys.csv"
)
SCHEMA <- "export_gate/aggregates.schema.json"
POLICY <- "export_gate/aggregates.policy.json"

# The delivery, from the order answer (a steward's: materialize_order): every file
# it delivers, named by the delivery year the answer gives it (never parsed from
# the file name), and the stored column names. reg-mock mirrors them.
LISA_FILES <- c("2021" = "scb_lisa_individer_2021.parquet",
                "2022" = "scb_lisa_individer_2022.parquet")
LISA_COLUMNS <- c(person_id = "PersonNr", sex = "Kon", income = "DispInk04")
