# The disclosure stage's shared parts. Only a disclosure function makes a publishable
# number, in this order: suppress, perturb, round (the policy in src/, floor 5).
MIN_CELL <- 5L

# The marker that a key file has been made here. It sits outside keys/, so a key file
# that goes missing is noticed.
keys_made <- function() {
  file.path(dirname(dirname(Sys.getenv("REGMETA_KEYS"))), "cell_keys.made")
}

# The key file's identity, as a target that always runs and that every disclosure step
# takes: a new epoch or a new key file runs those steps again. A key file that was made
# and is gone stops the run: new keys would give tables already published new noise,
# and only the researcher decides that.
key_state <- function() {
  keys <- Sys.getenv("REGMETA_KEYS")
  if (!file.exists(keys)) {
    if (file.exists(keys_made())) {
      stop(
        "the cell-key file is missing (",
        keys,
        "). Restore it. To start over with new keys, the researcher deletes ",
        keys_made()
      )
    }
    return("none")
  }
  made <- if (file.exists(keys_made())) readLines(keys_made()) else ""
  paste(ck_keys_read(keys)$key_epoch[[1]], made)
}

# Perturbed person counts by `by`, from person-level rows. The key file is made on the
# first run and gains keys for new persons; existing keys never change, and a new key
# epoch is only ever the researcher's decision. `n_true` is check-only.
perturbed_counts <- function(persons, by, id = "person_id") {
  keys <- Sys.getenv("REGMETA_KEYS")
  ids <- unique(persons[[id]])
  key_state()
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
    writeLines(format(Sys.time(), tz = "UTC", usetz = TRUE), keys_made())
  }
  ck_perturb(persons, by, id, keys, MIN_CELL)
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
