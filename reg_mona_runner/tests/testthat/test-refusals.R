# Refusals that fire before the runner touches a database: one case per exit
# code, each asserting the code and that the order stays in orders/. No
# connection is configured, so an order that got as far as connecting would
# fail with 2 (setup) instead of its own code.

test_that("an export root without orders/ is a setup failure (2)", {
  # Regression: the runner treats a mistyped base path as "no orders" and exits 0.
  base <- tempfile("export-")
  dir.create(base)
  expect_identical(run_runner(base)$status, 2L)
})

test_that("an order that is not JSON is refused (3)", {
  # Regression: a truncated upload reaches the shape checks or the database.
  base <- export_root(text = list(`900` = "{\"version\": 2, \"entries\": ["))
  expect_identical(run_runner(base)$status, 3L)
  expect_refused(base, "900", 3L)
})

test_that("a manifest version other than 2 is refused (4)", {
  # Regression: a retired v1 manifest, which names no extract format, is read
  # with v2 semantics.
  base <- export_root(
    text = list(
      `900` = order_text("row-filter", function(doc) {
        doc$version <- 1L
        doc$extract_format <- NULL
        doc
      })
    )
  )
  expect_identical(run_runner(base)$status, 4L)
  expect_refused(base, "900", 4L)
})

test_that("a global-fallback order is refused (5)", {
  # Regression: blank physical tables reach SQL. The golden is used unchanged.
  base <- export_root(list(`900` = case_order("open-ended")))
  expect_identical(run_runner(base)$status, 5L)
  expect_refused(base, "900", 5L)
})

test_that("an unknown field anywhere in a compact manifest is refused (6)", {
  # Regression: an unknown field nested in a filter range is ignored, so a
  # manifest shape this runner does not understand extracts the wrong rows.
  base <- export_root(
    text = list(
      `900` = order_text("row-filter", function(doc) {
        doc$entries[[1]]$filter$ranges[[1]]$step <- "month"
        doc
      })
    )
  )
  expect_identical(run_runner(base)$status, 6L)
  expect_refused(base, "900", 6L)
})

test_that("an output file named like the run log is refused (6)", {
  # Regression: a csv file named run_log.csv is overwritten by the run log, so
  # the order succeeds with its data gone.
  base <- export_root(
    text = list(
      `900` = order_text("row-filter", function(doc) {
        for (i in seq_along(doc$entries)) {
          doc$entries[[i]]$physical$file <- "Run_Log"
        }
        doc
      })
    )
  )
  expect_identical(run_runner(base)$status, 6L)
  expect_refused(base, "900", 6L)
})
