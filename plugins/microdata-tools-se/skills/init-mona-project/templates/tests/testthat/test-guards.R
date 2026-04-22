# Guards: encoding safety and core helper smoke tests
#
# These tests run locally before uploading code to MONA. The ASCII guard
# is the most important: MONA runs Windows with a locale-dependent encoding,
# and non-ASCII bytes in R source files cause silent corruption or parse
# errors. The rule is simple: all R files uploaded to MONA must be pure ASCII.
#
# For Swedish characters in strings (variable names, grep patterns, labels),
# use \uXXXX escapes. Common ones:
#   \u00C4  A with diaeresis   (Ae)
#   \u00D6  O with diaeresis   (Oe)
#   \u00E5  a with ring above  (aa)
#   \u00E4  a with diaeresis   (ae)
#   \u00F6  o with diaeresis   (oe)
#   \u00C5  A with ring above  (Aa)

library(here)
library(testthat)

local_edition(3)

source(here::here("src", "helpers.R"))

# -- ASCII guard ---------------------------------------------------------------

non_ascii_lines <- function(path) {
  lines <- readLines(path, warn = FALSE, encoding = "UTF-8")
  bad_lines <- which(vapply(
    lines,
    function(line) any(utf8ToInt(line) > 127L),
    logical(1)
  ))

  if (length(bad_lines) == 0L) {
    return(character())
  }

  vapply(
    bad_lines,
    function(i) sprintf("%s:%d: %s", path, i, lines[[i]]),
    character(1)
  )
}

test_that("MONA-uploaded R files are ASCII-only", {
  files <- c(
    here::here("run.R"),
    sort(list.files(
      here::here("src"),
      pattern = "\\.R$",
      full.names = TRUE
    ))
  )
  files <- files[file.exists(files)]

  bad <- unlist(lapply(files, non_ascii_lines), use.names = FALSE)

  expect(
    length(bad) == 0L,
    paste(
      "Non-ASCII characters found (use \\uXXXX escapes for MONA compat):\n",
      paste(bad, collapse = "\n")
    )
  )
})

# -- Helper smoke tests --------------------------------------------------------

test_that("assert_unique_by passes on unique data", {
  dt <- data.table::data.table(id = 1:5, val = letters[1:5])
  expect_silent(assert_unique_by(dt, "id", "test"))
})

test_that("assert_unique_by catches duplicates", {
  dt <- data.table::data.table(id = c(1, 1, 2), val = letters[1:3])
  expect_error(assert_unique_by(dt, "id", "test"), "not unique")
})

test_that("assert_unique_by ignores NA keys by default", {
  dt <- data.table::data.table(id = c(1, 2, NA, NA), val = letters[1:4])
  expect_silent(assert_unique_by(dt, "id", "test"))
  expect_error(assert_unique_by(dt, "id", "test", na_rm = FALSE), "not unique")
})
