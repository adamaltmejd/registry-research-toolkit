# key_check.R's published counts (reg_mona_runner/DESIGN.md, "Key check"): no
# SQL Server here, so the script's definitions are loaded without its run, with
# the plugin's cell_key.R, and one table's CSV rows are built from given true
# counts.

key_check <- local({
  exprs <- parse(file.path(runner_dir, "key_check.R"), keep.source = FALSE)
  run <- which(vapply(exprs, identical, TRUE, quote(status <- main())))
  env <- new.env()
  for (e in exprs[seq_len(run - 1L)]) eval(e, env)
  env
})

cell_key_r <- file.path(
  repo_dir, "plugins", "regmeta", "skills", "project-setup", "scripts", "cell_key.R"
)

test_that("published counts follow the salted cell keys and the p-table, one draw per column and count", {
  # Expected values computed outside the script: openssl HMAC-SHA256 under the
  # salt of "3:dbo|1:T|4:rows|1:6", "3:dbo|1:T|8:column:A|1:1" and
  # "3:dbo|1:T|8:column:A|1:5", first 7 hex digits mod 2^26 over 2^26, gives
  # keys 0.31595092, 0.75716889 and 0.20261306; in cell_key.R's p-table, row 6
  # [0, 0.38893783) is v = -1, row 1 [0, 0.81486748) is v = -1, row 5
  # [0.17591612, 0.51907381) is v = 0. So rows 6 -> 5, nulls 1 -> 0, and both
  # counts of 5 in column A -> 5; true zeros -> 0.
  # Fails if the key's message, salt use or lookup changes, if a rerun draws
  # other noise (reruns could be averaged), or if two measures with the same
  # count in one column draw separately (they could be averaged).
  ck <- new.env()
  sys.source(cell_key_r, ck)
  salt <- strrep("ab", 32L)
  disclosure <- list(salt = salt, ck = ck, pt = ck$ck_ptable())
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
  values[, "rows"] <- 6
  values[1L, c("nulls", "not_bigint", "leading_zeros", "spaces", "not_digits")] <- c(1, 5, 0, 0, 5)
  values[1L, c("min_length", "max_length")] <- c(9, 12)
  values[2L, c("nulls", "not_bigint", "min_length", "max_length")] <- c(0, 0, 1, 7)
  rows <- key_check$publish_rows(cols, "ok", values, disclosure)
  expect_identical(
    key_check$csv_lines(rows),
    c(
      "dbo,T,A,scb_key,varchar,ok,5,0,5,,,0,0,5,9,12",
      "dbo,T,B,scb_key,bigint,ok,5,0,0,,,,,,1,7"
    )
  )
  expect_identical(rows, key_check$publish_rows(cols, "ok", values, disclosure))
})
