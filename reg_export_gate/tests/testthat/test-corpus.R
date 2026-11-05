# One test per corpus case and gate; each case.json names the regression it
# pins in its description and holds its expected result. The R gate always
# runs; the Rust binary when REG_EXPORT_GATE names it.

gates <- c(
  "R",
  if (nzchar(Sys.getenv("REG_EXPORT_GATE"))) Sys.getenv("REG_EXPORT_GATE")
)

for (name in list.dirs(cases_dir, full.names = FALSE, recursive = FALSE)) {
  for (gate_bin in gates) {
    label <- if (gate_bin == "R") "R" else "rust"
    test_that(paste0(name, " [", label, "]"), expect_case(name, gate_bin))
  }
}
