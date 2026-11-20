# The pipeline's steps, as targets (run.R builds them; unchanged steps are skipped).
# One target per input, one per analysis, one per disclosed result, and `release`
# last: every disclosed result, bound into the schema's files.
for (f in setdiff(list.files("src", pattern = "\\.R$"), c("pipeline.R", "export_gate.R"))) {
  source(file.path("src", f))
}

list(
  targets::tar_target(lisa_files, source_files("lisa"), format = "file"),
  targets::tar_target(lisa, read_source("lisa", lisa_files)),
  targets::tar_target(income_rows, income_by_sex(lisa)),
  targets::tar_target(income_result, disclose_income_by_sex(income_rows)),
  targets::tar_target(release, release_tables(income_result))
)
