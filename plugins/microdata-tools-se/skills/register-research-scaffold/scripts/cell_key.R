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

# The p-table: D = 5, V = 6.5, js = 4 from the toolkit's default policy
# (reg_export_gate/schemas/aggregates.policy.json; reg_export_gate/DESIGN.md,
# "The shipped files", maintainer 2026-11-05 and 2026-11-17). The other
# parameters are ptable's defaults: optim = 1 (V is an upper bound on the
# variance) and mono = TRUE (probabilities fall away from no change). Generated
# with the ptable package 1.0.0 (Destatis, CRAN):
#
#   t <- as.data.frame(ptable::create_cnt_ptable(
#     D = 5, V = 6.5, js = 4, optim = 1, mono = TRUE)@pTable)
#   sprintf("%d,%d,%.8f,%.8f", t$i, t$v, t$p_int_lb, t$p_int_ub)
#
# under the header line below, one row per line, "\n" after each. Columns: i
# (true count; the row for the largest i, 10, serves every larger count), v
# (perturbation), p_int_lb and p_int_ub (the cell-key interval selecting v,
# lower bound inclusive). CK_PTABLE_SHA256 is the SHA-256 of CK_PTABLE_CSV's
# bytes after its leading newline; ck_perturb() refuses if they differ.
CK_PTABLE_SHA256 <- "0e37a26398ca07dc2abebeb21c913ce73c2af859398677a8fccd02b69f02ccb6"
CK_PTABLE_CSV <- "
i,v,p_int_lb,p_int_ub
0,0,0.00000000,1.00000000
1,-1,0.00000000,0.81486748
1,4,0.81486748,0.92566260
1,5,0.92566260,1.00000000
2,-2,0.00000000,0.61612076
2,3,0.61612076,0.92758478
2,4,0.92758478,0.99181142
2,5,0.99181142,1.00000000
3,-3,0.00000000,0.41629284
3,2,0.41629284,0.92395868
3,3,0.92395868,0.99476175
3,4,0.99476175,0.99981540
3,5,0.99981540,1.00000000
4,-4,0.00000000,0.27517727
4,1,0.27517727,0.72404470
4,2,0.72404470,0.91992950
4,3,0.91992950,0.98268868
4,4,0.98268868,0.99745075
4,5,0.99745075,1.00000000
5,-5,0.00000000,0.17591612
5,0,0.17591612,0.51907381
5,1,0.51907381,0.75578428
5,2,0.75578428,0.89377079
5,3,0.89377079,0.96174608
5,4,0.96174608,0.99004443
5,5,0.99004443,1.00000000
6,-1,0.00000000,0.38893783
6,0,0.38893783,0.77787565
6,1,0.77787565,0.89996913
6,2,0.89996913,0.95636328
6,3,0.95636328,0.98241134
6,4,0.98241134,0.99444277
6,5,0.99444277,1.00000000
7,-2,0.00000000,0.23237940
7,-1,0.23237940,0.46475880
7,0,0.46475880,0.69713821
7,1,0.69713821,0.80817234
7,2,0.80817234,0.88492368
7,3,0.88492368,0.93797735
7,4,0.93797735,0.97465021
7,5,0.97465021,1.00000000
8,-3,0.00000000,0.15942099
8,-2,0.15942099,0.31884198
8,-1,0.31884198,0.47826297
8,0,0.47826297,0.63768396
8,1,0.63768396,0.73868433
8,2,0.73868433,0.82283043
8,3,0.82283043,0.89293478
8,4,0.89293478,0.95134058
8,5,0.95134058,1.00000000
9,-4,0.00000000,0.09008448
9,-3,0.09008448,0.19906494
9,-2,0.19906494,0.32240818
9,-1,0.32240818,0.45239562
9,0,0.45239562,0.58238307
9,1,0.58238307,0.70228426
9,2,0.70228426,0.80624329
9,3,0.80624329,0.89057075
9,4,0.89057075,0.95456541
9,5,0.95456541,1.00000000
10,-5,0.00000000,0.03702695
10,-4,0.03702695,0.09682611
10,-3,0.09682611,0.18364426
10,-2,0.18364426,0.29695327
10,-1,0.29695327,0.42989361
10,0,0.42989361,0.57010639
10,1,0.57010639,0.70304673
10,2,0.70304673,0.81635575
10,3,0.81635575,0.90317390
10,4,0.90317390,0.96297305
10,5,0.96297305,1.00000000
"

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

# SHA-256 as export_gate.R gets it: tools::sha256sum (R >= 4.5), else digest.
ck_sha256 <- function(text) {
  bytes <- charToRaw(enc2utf8(text))
  tools_ns <- asNamespace("tools")
  if (exists("sha256sum", envir = tools_ns, inherits = FALSE)) {
    return(unname(get("sha256sum", envir = tools_ns)(bytes = bytes)))
  }
  if (!requireNamespace("digest", quietly = TRUE)) {
    ck_stop("cell_key_sha256_missing", "SHA-256 needs R >= 4.5 or the digest package")
  }
  digest::digest(bytes, algo = "sha256", serialize = FALSE)
}

# The embedded p-table, after its hash and every policy property are checked.
ck_ptable <- function() {
  text <- sub("^\n", "", CK_PTABLE_CSV)
  if (!identical(ck_sha256(text), CK_PTABLE_SHA256)) {
    ck_stop("cell_key_bad_ptable", "the p-table does not match its SHA-256")
  }
  ck_ptable_check(utils::read.csv(text = text))
}

# Every property the export gate and the policy rely on, checked before use:
# each true count's probabilities sum to 1 over contiguous intervals covering
# [0, 1), every true count from 0 up has a row, the noise is unbiased, its
# variance at most V, it is at most D, and it never publishes 1..js or a
# negative count.
ck_ptable_check <- function(pt, d = 5L, variance = 6.5, js = 4L) {
  bad <- function(what) ck_stop("cell_key_bad_ptable", what)
  if (!identical(sort(unique(pt$i)), seq(0, max(pt$i)))) bad("missing true counts")
  for (i in sort(unique(pt$i))) {
    r <- pt[pt$i == i, ]
    r <- r[order(r$p_int_lb), ]
    p <- r$p_int_ub - r$p_int_lb
    if (r$p_int_lb[[1]] != 0 || r$p_int_ub[[nrow(r)]] != 1 ||
        any(r$p_int_lb[-1] != r$p_int_ub[-nrow(r)]) || any(p <= 0)) {
      bad(paste("intervals of true count", i))
    }
    if (abs(sum(p * r$v)) > 1e-6) bad(paste("bias at true count", i))
    if (sum(p * r$v^2) > variance + 1e-6) bad(paste("variance above V at true count", i))
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
  pt <- ck_ptable()
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
