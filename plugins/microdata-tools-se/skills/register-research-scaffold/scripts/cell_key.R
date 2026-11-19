# Cell-key perturbation of person counts, for a pipeline's disclosure stage on
# MONA. A project copies this file unchanged and records its SHA-256, like
# export_gate.R. Base R only.
#
# CELL_KEY_VERSION names this file's method; a project pins it.
#
# The method (toolkit issue #2524; reg_export_gate/DESIGN.md, "The shipped
# files"): every person has a secret record key; a cell's key is the sum of its
# distinct persons' record keys, modulo 1; the cell's true count and key select
# a perturbation from the p-table. Same keys and same data give the same output,
# whatever the row order. A cell with one person more gets an unrelated key, so
# differencing two tables does not recover a person.
#
# Record keys are integers in [0, 2^26), stored in the key file; a cell key is
# (sum mod 2^26) / 2^26. Integer keys make the sum exact (below 2^53 for up to
# 2^27 persons per cell), so row order cannot change it. 2^-26 (1.5e-8) is
# finer than the p-table's 8-digit interval bounds.
#
# Functions:
#   ck_keys_create(ids, path, new_epoch = FALSE)  write the key file
#   ck_keys_extend(ids, path)                     add keys for new persons
#   ck_perturb(data, by, id, keys)                perturbed person counts
#
# The key file stays on MONA, never in Git, never exported. Its epoch is
# returned with every result (attribute "key_epoch"); the pipeline writes it to
# the project's run record on MONA, never to the export.

CELL_KEY_VERSION <- "0.1.0"

CK_MODULUS <- 2^26

# The p-table: D = 5, V = 6.5, js = 4 (reg_export_gate/schemas/
# aggregates.policy.json and DESIGN.md, maintainer 2026-11-05 and 2026-11-17).
# Columns: i (true count; the row for the largest i serves every larger count),
# v (perturbation), p_int_lb and p_int_ub (the cell-key interval selecting v,
# lower bound inclusive).
# PENDING: generated with the ptable package once it is installed; until then
# ck_perturb() refuses.
CK_PTABLE <- NULL
CK_PTABLE_SHA256 <- NULL

ck_stop <- function(code, message) {
  stop(structure(
    class = c("cell_key_refusal", "error", "condition"),
    list(message = paste0(code, ": ", message), call = NULL, code = code)
  ))
}

# A secret draw from R's generator. The key file, not the seed, is the secret:
# no seed is set or recorded.
ck_draw <- function(n) {
  as.integer(sample.int(CK_MODULUS, n, replace = TRUE) - 1L)
}

ck_ids <- function(ids) {
  ids <- unique(as.character(ids))
  if (anyNA(ids) || any(ids == "")) {
    ck_stop("cell_key_bad_id", "a person id is missing or empty")
  }
  ids
}

ck_keys_write <- function(keys, path) {
  tmp <- paste0(path, ".tmp")
  utils::write.csv(keys, tmp, row.names = FALSE)
  if (!file.rename(tmp, path)) {
    ck_stop("cell_key_write_failed", "could not write the key file")
  }
}

#' Read and check a key file: columns key_epoch, id, key.
ck_keys_read <- function(path) {
  if (!file.exists(path)) {
    ck_stop("cell_key_no_key_file", "no key file at the given path")
  }
  keys <- utils::read.csv(
    path,
    colClasses = c(key_epoch = "integer", id = "character", key = "integer")
  )
  ok <- identical(names(keys), c("key_epoch", "id", "key")) &&
    nrow(keys) > 0 &&
    !anyNA(keys) &&
    length(unique(keys$key_epoch)) == 1 &&
    keys$key_epoch[[1]] >= 1 &&
    !anyDuplicated(keys$id) &&
    all(keys$key >= 0 & keys$key < CK_MODULUS)
  if (!ok) {
    ck_stop("cell_key_bad_key_file", "the key file is not a valid key file")
  }
  keys
}

#' Write a key file for `ids`, epoch 1. If the file exists, refuses unless
#' `new_epoch = TRUE`, which replaces every key and raises the epoch by one:
#' tables under different epochs carry independent noise, so publishing the
#' same table under two epochs lets a reader average the noise away. Only on
#' the researcher's explicit request.
ck_keys_create <- function(ids, path, new_epoch = FALSE) {
  ids <- ck_ids(ids)
  epoch <- 1L
  if (file.exists(path)) {
    if (!isTRUE(new_epoch)) {
      ck_stop(
        "cell_key_file_exists",
        "a key file exists; a new key epoch needs new_epoch = TRUE"
      )
    }
    epoch <- ck_keys_read(path)$key_epoch[[1]] + 1L
  }
  ck_keys_write(
    data.frame(key_epoch = epoch, id = ids, key = ck_draw(length(ids))),
    path
  )
  invisible(epoch)
}

#' Add keys for persons in `ids` the key file lacks, in the same epoch.
#' Existing keys never change, so earlier tables stay consistent.
ck_keys_extend <- function(ids, path) {
  keys <- ck_keys_read(path)
  new <- setdiff(ck_ids(ids), keys$id)
  if (length(new) > 0) {
    keys <- rbind(
      keys,
      data.frame(key_epoch = keys$key_epoch[[1]], id = new, key = ck_draw(length(new)))
    )
    ck_keys_write(keys, path)
  }
  invisible(length(new))
}

# Every property the export gate and the policy rely on, checked before use:
# each true count's probabilities sum to 1 over contiguous intervals covering
# [0, 1), the noise is unbiased, at most D, never publishes 1..js and never a
# negative count.
ck_ptable_check <- function(pt, d = 5L, js = 4L) {
  bad <- function(what) ck_stop("cell_key_bad_ptable", what)
  if (is.null(pt) || !is.data.frame(pt)) {
    bad("the p-table is not filled in")
  }
  for (i in sort(unique(pt$i))) {
    r <- pt[pt$i == i, ]
    r <- r[order(r$p_int_lb), ]
    p <- r$p_int_ub - r$p_int_lb
    if (r$p_int_lb[[1]] != 0 || r$p_int_ub[[nrow(r)]] != 1 ||
        any(r$p_int_lb[-1] != r$p_int_ub[-nrow(r)]) || any(p <= 0)) {
      bad(paste("intervals of true count", i))
    }
    if (abs(sum(p * r$v)) > 1e-6) bad(paste("bias at true count", i))
    if (any(abs(r$v) > d)) bad(paste("noise beyond D at true count", i))
    out <- i + r$v
    if (any(out < 0) || (i > 0 && any(out >= 1 & out <= js)) ||
        (i == 0 && any(out != 0))) {
      bad(paste("published value outside the policy at true count", i))
    }
  }
  invisible(pt)
}

# One integer code per distinct combination of the `by` columns, NA its own
# value in each column, so a string "NA" and a missing value never merge.
ck_cells <- function(frame, by) {
  codes <- lapply(frame[by], function(x) as.integer(addNA(factor(x), ifany = TRUE)))
  key <- do.call(paste, c(codes, sep = "."))
  match(key, unique(key))
}

#' Perturbed person counts by `by`.
#'
#' `data` holds one row per person and cell at least (duplicates are dropped:
#' a cell counts distinct persons); `id` names its person-id column; `keys` is
#' the key file's path. Returns one row per non-empty cell: the `by` columns,
#' `n_true` (the true distinct persons, check-only: never published) and
#' `n_persons` (perturbed; NA where it falls below `min_cell_count`, which the
#' gate refuses, a perturbed 0 included). Attribute "key_epoch": write it to
#' the run record.
#'
#' `min_cell_count` is the policy's (5 in aggregates-min5); pass the project
#' policy's value if it sets its own.
ck_perturb <- function(data, by, id, keys, min_cell_count = 5L) {
  pt <- ck_ptable_check(CK_PTABLE)
  if (!all(c(by, id) %in% names(data))) {
    ck_stop("cell_key_bad_column", "a by or id column is not in data")
  }
  persons <- unique(data[c(by, id)])
  ids <- as.character(persons[[id]])
  if (anyNA(ids)) {
    ck_stop("cell_key_bad_id", "a person id is missing")
  }
  k <- ck_keys_read(keys)
  record_key <- k$key[match(ids, k$id)]
  missing <- length(unique(ids[is.na(record_key)]))
  if (missing > 0) {
    ck_stop(
      "cell_key_missing_ids",
      paste(missing, "persons have no record key; add them with ck_keys_extend()")
    )
  }

  cell <- ck_cells(persons, by)
  first <- !duplicated(cell)
  out <- persons[first, by, drop = FALSE]
  out <- out[order(cell[first]), , drop = FALSE]
  n_true <- tabulate(cell)
  # Exact: integer keys below 2^26, sums below 2^53.
  cell_key <- (rowsum(as.double(record_key), cell, reorder = TRUE)[, 1] %% CK_MODULUS) /
    CK_MODULUS

  row <- pmin(n_true, max(pt$i))
  v <- integer(length(n_true))
  for (i in unique(row)) {
    r <- pt[pt$i == i, ]
    r <- r[order(r$p_int_lb), ]
    at <- row == i
    v[at] <- r$v[findInterval(cell_key[at], r$p_int_lb)]
  }
  published <- n_true + v
  published[published < min_cell_count] <- NA_integer_

  out$n_true <- as.integer(n_true)
  out$n_persons <- as.integer(published)
  # Rows in `by` order, not input order, so a rerun on reordered data is identical.
  out <- out[do.call(order, unname(as.list(out[by]))), , drop = FALSE]
  rownames(out) <- NULL
  attr(out, "key_epoch") <- k$key_epoch[[1]]
  out
}
