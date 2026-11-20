# The disclosure stage's shared parts. Only a disclosure function makes a publishable
# number, in this order: suppress, perturb, round (the policy in src/, floor 5).
MIN_CELL <- 5L

# Perturbed person counts by `by`, from person-level rows. The key file is made on the
# first run and gains keys for new persons; existing keys never change, and a new key
# epoch is only ever the researcher's decision. `n_true` is check-only.
perturbed_counts <- function(persons, by, id = "person_id") {
  keys <- Sys.getenv("REGMETA_KEYS")
  ids <- unique(persons[[id]])
  if (file.exists(keys)) {
    added <- ck_keys_extend(ids, keys)
    if (added > 0) message("cell keys: added ", added, " persons")
  } else {
    message(
      "==== CELL KEYS: creating a new key file, epoch 1 (",
      keys,
      ") ===="
    )
    ck_keys_create(ids, keys)
  }
  ck_perturb(persons, by, id, keys, MIN_CELL)
}

# The key file's epoch, as a target that always runs and that every disclosure step
# takes: after the researcher makes a new epoch, no result from the old keys is reused.
# A key file not yet made will be epoch 1.
key_epoch <- function() {
  keys <- Sys.getenv("REGMETA_KEYS")
  if (file.exists(keys)) ck_keys_read(keys)$key_epoch[[1]] else 1L
}

# Every disclosed result, bound into the schema's files. A result is a list of data
# frames named by schema file ("counts.parquet", "statistics.parquet", ...).
release_tables <- function(...) {
  results <- list(...)
  files <- unique(unlist(lapply(results, names)))
  setNames(
    lapply(files, function(file) {
      do.call(rbind, lapply(results, function(r) r[[file]]))
    }),
    files
  )
}
