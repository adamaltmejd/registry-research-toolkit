# One test per corpus case; each case.json names the regression it pins in its
# description and holds its expected result.

# Cases for rules the Rust gate (the reference) has and this one does not yet;
# they run here once export_gate.R imitates the binary
# (crates/reg-export-gate/DESIGN.md, "Plan").
PENDING <- c("structure-too-many-bytes", "structure-too-many-rows")

for (name in list.dirs(cases_dir, full.names = FALSE, recursive = FALSE)) {
  test_that(name, {
    if (name %in% PENDING) skip("not in export_gate.R yet")
    expect_case(name)
  })
}
