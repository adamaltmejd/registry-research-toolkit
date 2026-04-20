#!/usr/bin/env Rscript
library(here)
library(testthat)
testthat::test_dir(here::here("tests", "testthat"), stop_on_failure = TRUE)
