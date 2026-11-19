# The one disclosure stage: suppress, perturb, round (export_gate/ policy, floor 5).
# Every published person count comes from ck_perturb() over the persons behind the
# cell; n_true stays true and check-only.
MIN_CELL <- 5L

disclose <- function(persons, table, keys) {
  by <- c("year", "group_1")
  counts <- ck_perturb(persons, by, "person_id", keys, MIN_CELL)
  epoch <- attr(counts, "key_epoch")
  counts$table <- table

  # A mean rests on the persons with a value, so its support is counted on them.
  valued <- persons[!is.na(persons$income), ]
  stats <- ck_perturb(valued, by, "person_id", keys, MIN_CELL)
  means <- aggregate(income ~ year + group_1, valued, mean)
  stats <- merge(stats, means, by = by, all.x = TRUE, sort = TRUE)
  stats$value <- ifelse(stats$n_true < MIN_CELL, NA_real_, signif(stats$income, 4))
  stats$table <- table
  stats$statistic <- "mean_disposable_income"

  list(
    tables = list("counts.parquet" = counts, "statistics.parquet" = stats),
    key_epoch = epoch
  )
}
