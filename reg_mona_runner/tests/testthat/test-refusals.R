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
  base <- export_root(text = list(`900` = "{\"version\": 4, \"files\": ["))
  expect_identical(run_runner(base)$status, 3L)
  expect_refused(base, "900", 3L)
})

test_that("a manifest version other than 4 is refused (4)", {
  # Regression: a retired v3 order, whose entries this runner no longer reads,
  # fails as malformed (6) instead of as stale, or a v4 shape is read under
  # another version's semantics.
  base <- export_root(
    text = list(
      `900` = order_text("pooled-disjoint-periods", function(doc) {
        doc$version <- 3L
        doc
      })
    )
  )
  expect_identical(run_runner(base)$status, 4L)
  expect_refused(base, "900", 4L)
})

test_that("another steward's order is refused (5)", {
  # Regression: an order made against another steward's holdings reaches SQL and
  # extracts whatever same-named tables this server holds. The golden keeps the
  # steward the server wrote (the fixture's).
  base <- export_root(
    text = list(`900` = order_text("key-only", identity, steward = "fixture"))
  )
  expect_identical(run_runner(base)$status, 5L)
  expect_refused(base, "900", 5L)
})

test_that("an unknown field anywhere in a compact manifest is refused (6)", {
  # Regression: an unknown field nested in a filter range is ignored, so a
  # manifest shape this runner does not understand extracts the wrong rows.
  base <- export_root(
    text = list(
      `900` = order_text("pooled-disjoint-periods", function(doc) {
        doc$files[[1]]$row_filter$ranges[[2]]$step <- "month"
        doc
      })
    )
  )
  expect_identical(run_runner(base)$status, 6L)
  expect_refused(base, "900", 6L)
})

test_that("an unknown top-level field is refused (6)", {
  # Regression: the manifest's own key set is not closed, so an order carrying a
  # section this runner does not know (a newer writer's, say) extracts as if it
  # were absent.
  base <- export_root(
    text = list(
      `900` = order_text("key-only", function(doc) {
        doc$suppress <- list("lopnr")
        doc
      })
    )
  )
  expect_identical(run_runner(base)$status, 6L)
  expect_refused(base, "900", 6L)
})

test_that("a column with a null storage type is refused (6)", {
  # Regression: a null type passes as "any storage", so a drifted column is
  # extracted without the check the order promises.
  base <- export_root(
    text = list(
      `900` = order_text("key-only", function(doc) {
        doc$files[[1]]$columns[[1]]$storage["type"] <- list(NULL)
        doc
      })
    )
  )
  expect_identical(run_runner(base)$status, 6L)
  expect_refused(base, "900", 6L)
})

test_that("an output file named like the run log is refused (6)", {
  # Regression: a file whose stem is the run log's, in any case and with any
  # extension, is overwritten by run_log.csv or overwrites it, so the order
  # succeeds with its data gone.
  base <- export_root(
    text = list(
      `900` = order_text("pooled-disjoint-periods", function(doc) {
        doc$files[[1]]$name <- "Run_Log"
        doc
      })
    )
  )
  expect_identical(run_runner(base)$status, 6L)
  expect_refused(base, "900", 6L)
})

test_that("two columns written under one Stata name are refused (6)", {
  # Regression: a .dta file holds two variables of one name, which Stata cannot
  # read, or haven keeps one column's data under the other's name. Here an alias
  # takes the name another column is written under as its own.
  base <- export_root(
    text = list(
      `900` = order_text("dta-stata-alias", function(doc) {
        doc$files[[1]]$columns[[1]]$stata_name <- "Id"
        doc
      })
    )
  )
  expect_identical(run_runner(base)$status, 6L)
  expect_refused(base, "900", 6L)
})
