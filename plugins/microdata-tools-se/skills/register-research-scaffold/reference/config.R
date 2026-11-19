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

# The delivery: every file the order delivers and its stored column names, from the
# order answer (a steward's: materialize_order). reg-mock mirrors them.
LISA_FILES <- sprintf("scb_lisa_individer_%d.parquet", 2021:2022)
LISA_COLUMNS <- c(person_id = "PersonNr", sex = "Kon", income = "DispInkHB04")
