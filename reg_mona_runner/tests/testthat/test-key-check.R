# key_check.R's export rule (reg_mona_runner/DESIGN.md, "Key check"): no SQL
# Server here, so the script's definitions are loaded without its run and one
# table's CSV rows are built from given aggregates.

key_check <- local({
  exprs <- parse(file.path(runner_dir, "key_check.R"), keep.source = FALSE)
  run <- which(vapply(exprs, identical, TRUE, quote(status <- main())))
  env <- new.env()
  for (e in exprs[seq_len(run - 1L)]) eval(e, env)
  env
})

test_that("counts round up to multiples of 5 and lengths need 5 supporting values", {
  # Fails if a small count is published exactly or masked as <5: B's rows 6,
  # nulls 1 and not_bigint 5 (not_bigint counts only non-null values) would
  # give nulls = 1 by difference, so all three must publish as 10, 5, 5, while
  # a zero stays 0. Also fails if a number's length support ignores not_bigint
  # (A: 4 values convert, so no length).
  cols <- data.frame(
    table_schema = "dbo",
    table_name = "T",
    column_name = c("A", "B", "C"),
    data_type = c("decimal", "varchar", "bigint"),
    group = "scb_key"
  )
  batch <- key_check$table_batch("dbo", "T", cols$column_name, cols$data_type)
  given <- list(
    A = c(nulls = 0, not_bigint = 2, not_whole = 0, min_length = 9, max_length = 9),
    B = c(nulls = 1, not_bigint = 5, leading_zeros = 0, spaces = 0, not_digits = 5,
          min_length = 9, max_length = 12),
    C = c(nulls = 0, not_bigint = 0, min_length = 1, max_length = 7)
  )
  result <- c(
    list(m0 = 6),
    lapply(seq_len(nrow(batch$slots)), function(k) {
      given[[batch$slots$column[k]]][[batch$slots$field[k]]]
    })
  )
  rows <- key_check$table_rows(cols, "ok", result, batch$slots)
  header <- key_check$csv_header("db")
  expect_identical(
    c(header[length(header)], key_check$csv_lines(rows)),
    c(
      paste0(
        "schema,table,column,group,sql_type,status,rows,nulls,not_bigint,not_whole,",
        "beyond_exact,leading_zeros,spaces,not_digits,min_length,max_length"
      ),
      "dbo,T,A,scb_key,decimal,ok,10,0,5,0,,,,,,",
      "dbo,T,B,scb_key,varchar,ok,10,5,5,,,0,0,5,9,12",
      "dbo,T,C,scb_key,bigint,ok,10,0,0,,,,,,1,7"
    )
  )
})
