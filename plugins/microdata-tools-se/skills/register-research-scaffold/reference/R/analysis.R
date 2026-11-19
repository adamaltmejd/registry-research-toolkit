# Q001: individual disposable income by sex and year. An analysis returns
# person-level rows (one per person and cell, with the values the statistics need),
# never counts or statistics: only the disclosure stage turns persons into
# publishable numbers.
income_by_sex <- function(lisa) {
  stopifnot(!anyNA(lisa$person_id), !anyDuplicated(lisa[c("person_id", "year")]))
  sex <- unname(c("1" = "man", "2" = "woman")[as.character(lisa$sex)])
  data.frame(
    person_id = lisa$person_id,
    year = lisa$year,
    group_1 = ifelse(is.na(sex), "unknown", sex),
    income = lisa$income
  )
}
