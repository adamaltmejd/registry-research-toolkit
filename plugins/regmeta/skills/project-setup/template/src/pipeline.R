# The pipeline's steps, as targets (run.R builds them; unchanged steps are skipped).
# One target per input, one per analysis, one per disclosed result, and `release`
# last: every disclosed result, bound into the schema's files.
for (f in setdiff(
  list.files("src", pattern = "\\.R$"),
  c("pipeline.R", "export_gate.R")
)) {
  source(file.path("src", f))
}

always <- targets::tar_cue(mode = "always")
list(
  targets::tar_target(cell_key_epoch, key_epoch(), cue = always),
  # No analyses yet. The `research` skill adds, for each question:
  #   targets::tar_target(<input>_files, source_files("<source>"), format = "file",
  #                       cue = always),
  #   targets::tar_target(<input>, read_source("<source>", <input>_files)),
  #   targets::tar_target(<rows>, <analysis>(<input>)),   # person-level rows
  #   targets::tar_target(<result>, <disclose>(<rows>, cell_key_epoch)),
  # and names each <result> in release_tables() below.
  targets::tar_target(release, release_tables())
)
