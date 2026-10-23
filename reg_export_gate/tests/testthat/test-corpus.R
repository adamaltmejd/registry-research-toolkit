# One test per corpus case; each case.json names the regression it pins in its
# description and holds its expected result.

for (name in list.dirs(cases_dir, full.names = FALSE, recursive = FALSE)) {
  test_that(name, expect_case(name))
}
