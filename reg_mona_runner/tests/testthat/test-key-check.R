# key_check.R's published counts (reg_mona_runner/DESIGN.md, "Key check"): no
# SQL Server here, so the script's definitions are loaded without its run (it
# runs only in the global environment), and one table's CSV rows are built
# from given true counts.

key_check <- new.env()
sys.source(file.path(runner_dir, "key_check.R"), key_check)

test_that("published counts round up to 10 and a true 0 stays 0", {
  # Expected values from the maintainer's 2026-11-27 ruling (#2680), not the
  # script: 1 -> 10, 10 -> 10, 11 -> 20, 0 -> 0; a count that does not apply
  # stays empty; no lengths in the CSV. Fails if a true 1 to 10 publishes as 0
  # (a published 0 would no longer mean none failed), if rounding goes down or
  # to nearest, or if a length reaches the CSV.
  cols <- data.frame(
    table_schema = "dbo",
    table_name = "T",
    column_name = c("A", "B"),
    data_type = c("varchar", "bigint"),
    group = "scb_key"
  )
  values <- matrix(
    NA_real_, 2L, 10L,
    dimnames = list(NULL, c(key_check$COUNTS, "min_length", "max_length"))
  )
  values[, "rows"] <- 11
  values[1L, c("nulls", "not_bigint", "leading_zeros", "spaces", "not_digits")] <- c(1, 10, 0, 0, 10)
  values[1L, c("min_length", "max_length")] <- c(9, 12)
  values[2L, c("nulls", "not_bigint", "min_length", "max_length")] <- c(0, 0, 1, 7)
  rows <- key_check$publish_rows(cols, "ok", values)
  expect_identical(
    key_check$csv_lines(rows),
    c(
      "dbo,T,A,scb_key,varchar,ok,20,10,10,,,0,0,10",
      "dbo,T,B,scb_key,bigint,ok,20,0,0,,,,,"
    )
  )
})
