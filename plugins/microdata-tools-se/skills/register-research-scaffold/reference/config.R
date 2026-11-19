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
