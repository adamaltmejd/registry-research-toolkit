# key_check.R's published counts (reg_mona_runner/DESIGN.md, "Key check"): no
# SQL Server here, so the script's definitions are loaded without its run (it
# runs only in the global environment), with the plugin's cell_key.R, and one
# table's CSV rows are built from given true counts.

key_check <- new.env()
sys.source(file.path(runner_dir, "key_check.R"), key_check)

cell_key_r <- file.path(
  repo_dir, "plugins", "regmeta", "skills", "project-setup", "scripts", "cell_key.R"
)

test_that("published counts follow the salted count-only cell keys and the p-table", {
  # Expected values computed outside the script: openssl HMAC-SHA256 under the
  # salt of "6", "1" and "5", first 7 hex digits mod 2^26 over 2^26, gives keys
  # 0.50576788, 0.90691589 and 0.79734258; in cell_key.R's p-table, row 6
  # [0.38893783, 0.77787565) is v = 0, row 1 [0.81486748, 0.92566260) is v = 4,
  # row 5 [0.75578428, 0.89377079) is v = 2. So rows 6 -> 6, nulls 1 -> 5, both
  # counts of 5 -> 7; true zeros -> 0. No lengths in the CSV.
  # Fails if the key's message, salt use or lookup changes, if a rerun draws
  # other noise, or if equal counts anywhere draw separately (averaging them
  # would recover the count).
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
      "dbo,T,A,scb_key,varchar,ok,6,5,7,,,0,0,7",
      "dbo,T,B,scb_key,bigint,ok,6,0,0,,,,,"
    )
  )
  expect_identical(rows, key_check$publish_rows(cols, "ok", values, disclosure))
})
