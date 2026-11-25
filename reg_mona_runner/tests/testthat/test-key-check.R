# key_check.R's export rule for lengths (reg_mona_runner/DESIGN.md, "Key
# check"): no SQL Server here, so the script's definitions are loaded without
# its run and one table's CSV rows are built from given aggregates.

key_check <- local({
  exprs <- parse(file.path(runner_dir, "key_check.R"), keep.source = FALSE)
  run <- which(vapply(exprs, identical, TRUE, quote(status <- main())))
  env <- new.env()
  for (e in exprs[seq_len(run - 1L)]) eval(e, env)
  env
})

test_that("a length is exported only over 5 supporting values and beside no masked count", {
  # Fails if a number's length support ignores not_bigint (A: 4 values convert),
  # or if lengths are shown beside a masked nulls they would bound (B: rows 10,
  # nulls <5, a length shown says nulls is 1 to 4 and support at least 5 -- with
  # rows 6 it would pin nulls at 1). C and D, with no masked count, keep theirs.
  cols <- data.frame(
    table_schema = "dbo",
    table_name = "T",
    column_name = c("A", "B", "C", "D"),
    data_type = c("decimal", "varchar", "varchar", "bigint"),
    group = "scb_key"
  )
  batch <- key_check$table_batch("dbo", "T", cols$column_name, cols$data_type)
  given <- list(
    A = c(nulls = 0, not_bigint = 6, not_whole = 0, min_length = 9, max_length = 9),
    B = c(nulls = 1, not_bigint = 0, leading_zeros = 0, spaces = 0, not_digits = 0,
          min_length = 9, max_length = 9),
    C = c(nulls = 0, not_bigint = 0, leading_zeros = 0, spaces = 0, not_digits = 0,
          min_length = 8, max_length = 10),
    D = c(nulls = 0, not_bigint = 0, min_length = 1, max_length = 7)
  )
  result <- c(
    list(m0 = 10),
    lapply(seq_len(nrow(batch$slots)), function(k) {
      given[[batch$slots$column[k]]][[batch$slots$field[k]]]
    })
  )
  rows <- key_check$table_rows(cols, "ok", result, batch$slots)
  expect_identical(rows$nulls, c("0", "<5", "0", "0"))
  expect_identical(rows$min_length, c("", "", "8", "1"))
  expect_identical(rows$max_length, c("", "", "10", "7"))
})
