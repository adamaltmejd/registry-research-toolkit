# q001: individual disposable income by sex and year.

# The analysis returns person-level rows (one per person and cell, with the values the
# statistics need), never counts or statistics.
income_by_sex <- function(lisa) {
  # Assertions run on mock and real data alike.
  stopifnot(!anyNA(lisa$person_id), !anyDuplicated(lisa[c("person_id", "year")]))
  sex <- unname(c("1" = "man", "2" = "woman")[as.character(lisa$sex)])
  data.frame(
    person_id = lisa$person_id,
    year = lisa$year,
    group_1 = ifelse(is.na(sex), "unknown", sex),
    income = lisa$income
  )
}

# The disclosure stage for this question: suppress, perturb, round.
disclose_income_by_sex <- function(persons) {
  by <- c("year", "group_1")
  counts <- perturbed_counts(persons, by)
  counts$table <- "income_by_sex"

  # A mean rests on the persons with a value, so its support is counted on them.
  valued <- persons[!is.na(persons$income), ]
  stats <- perturbed_counts(valued, by)
  means <- aggregate(income ~ year + group_1, valued, mean)
  stats <- merge(stats, means, by = by, all.x = TRUE, sort = TRUE)
  stats$value <- ifelse(stats$n_true < MIN_CELL, NA_real_, signif(stats$income, 4))
  stats$table <- "income_by_sex"
  stats$statistic <- "mean_disposable_income"

  list("counts.parquet" = counts, "statistics.parquet" = stats)
}
