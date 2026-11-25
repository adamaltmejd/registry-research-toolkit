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

test_that("published counts are the p-table's, the same on a rerun, keyed by salt and count", {
  # Fails if a count is published unperturbed or outside its p-table row, if a
  # rerun with the same salt publishes other noise (reruns could be averaged),
  # or if the key ignores the salt or the true count (a count one higher would
  # share its noise, so two counts could be differenced).
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
  # True counts: rows 6; A: nulls 1, not_bigint 5, the rest 0; B: all 0.
  values <- matrix(
    NA_real_, 2L, 10L,
    dimnames = list(NULL, c(key_check$COUNTS, "min_length", "max_length"))
  )
  values[, "rows"] <- 6
  values[1L, c("nulls", "not_bigint", "leading_zeros", "spaces", "not_digits")] <- c(1, 5, 0, 0, 0)
  values[1L, c("min_length", "max_length")] <- c(9, 12)
  values[2L, c("nulls", "not_bigint", "min_length", "max_length")] <- c(0, 0, 1, 7)
  rows <- key_check$publish_rows(cols, "ok", values, disclosure)
  expect_identical(rows, key_check$publish_rows(cols, "ok", values, disclosure))

  # The p-table's possible outputs (true count + v over the row's v), by hand:
  # row 1: 0, 5, 6; row 5: 0, 5..10; row 6: 5..11; row 0: 0.
  expect_true(rows$rows[[1]] %in% as.character(5:11))
  expect_identical(rows$rows[[1]], rows$rows[[2]])
  expect_true(rows$nulls[[1]] %in% c("0", "5", "6"))
  expect_true(rows$not_bigint[[1]] %in% as.character(c(0, 5:10)))
  expect_identical(c(rows$leading_zeros[[1]], rows$nulls[[2]], rows$not_bigint[[2]]), c("0", "0", "0"))
  expect_identical(c(rows$not_whole[[2]], rows$min_length, rows$max_length), c("", "9", "1", "12", "7"))

  key <- function(salt, n) {
    key_check$cell_keys(salt, "dbo", "T", "A", "nulls", n, ck$CK_MODULUS)
  }
  expect_identical(key(salt, 1), key(salt, 1))
  expect_false(key(salt, 1) == key(strrep("cd", 32L), 1))
  expect_false(key(salt, 1) == key(salt, 2))
})
