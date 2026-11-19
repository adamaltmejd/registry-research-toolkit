# Q001: disposable income by sex and year. An analysis returns person-level rows
# (one per person and cell, with the values the statistics need), never counts or
# statistics: only the disclosure stage turns persons into publishable numbers.
income_by_sex <- function(lisa) {
  stopifnot(!anyNA(lisa$PersonNr), !anyDuplicated(lisa[c("PersonNr", "year")]))
  sex <- unname(c("1" = "man", "2" = "woman")[as.character(lisa$Kon)])
  data.frame(
    PersonNr = lisa$PersonNr,
    year = lisa$year,
    group_1 = ifelse(is.na(sex), "unknown", sex),
    income = lisa$DispInkHB04
  )
}
