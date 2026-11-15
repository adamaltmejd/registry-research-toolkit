# One test per corpus case and gate; each case.json names the regression it
# pins in its description and holds its expected result. The R gate always
# runs; the Rust binary when REG_EXPORT_GATE names it.
#
# A case's optional gate_min is the oldest gate version that implements the
# behavior it pins. The R gate lags the binary it imitates, so a case past
# EXPORT_GATE_VERSION is skipped for R, never passed or failed. The binary
# named by REG_EXPORT_GATE is expected to be at least every case's gate_min.

gates <- c(
  "R",
  if (nzchar(Sys.getenv("REG_EXPORT_GATE"))) Sys.getenv("REG_EXPORT_GATE")
)

for (name in list.dirs(cases_dir, full.names = FALSE, recursive = FALSE)) {
  gate_min <- jsonlite::read_json(file.path(cases_dir, name, "case.json"))$gate_min
  for (gate_bin in gates) {
    label <- if (gate_bin == "R") "R" else "rust"
    test_that(paste0(name, " [", label, "]"), {
      if (gate_bin == "R" && !is.null(gate_min) &&
        package_version(gate_min) > package_version(gate$EXPORT_GATE_VERSION)) {
        skip(paste("needs gate", gate_min))
      }
      expect_case(name, gate_bin)
    })
  }
}
