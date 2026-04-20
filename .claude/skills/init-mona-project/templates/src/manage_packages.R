#!/usr/bin/env Rscript
#
# Package management for MONA
#
# renv does not work on MONA (no internet, shared library paths). Instead,
# this script manages packages using MONA's local CRAN mirror. Run it once
# after first upload, and again after adding new packages to the list.
#
# What it does:
#   1. Checks which managed packages are missing or outdated
#   2. Detects packages built on a different R version (common after R upgrades
#      on MONA) and reinstalls them
#   3. Installs/upgrades everything from the configured CRAN repository
#
# Usage on MONA:
#   Rscript src/manage_packages.R
#
# To add a new package: add it to managed_packages below, then rerun.

managed_packages <- unique(c(
  # pipeline infrastructure
  "here",
  "targets",
  "data.table",
  "bit64",
  "lubridate",
  "qs",
  # validation
  "assertr",
  "testthat",
  # output: tables
  "tinytable",
  # output: plots
  "ggplot2",
  "svglite",
  "ragg"
))

# -- Version detection ---------------------------------------------------------

current_r_minor <- paste(
  R.version$major,
  strsplit(R.version$minor, ".", fixed = TRUE)[[1]][1],
  sep = "."
)

built_r_minor <- function(x) {
  x <- as.character(x)
  x[is.na(x)] <- ""
  x <- sub("^R\\s+", "", x)
  x <- trimws(sub(";.*$", "", x))
  parts <- strsplit(x, ".", fixed = TRUE)

  vapply(
    parts,
    FUN.VALUE = character(1),
    function(part) {
      if (length(part) < 2L) {
        return(NA_character_)
      }

      if (!all(grepl("^[0-9]+$", part[1:2]))) {
        return(NA_character_)
      }

      paste(part[1:2], collapse = ".")
    }
  )
}

# -- State snapshot ------------------------------------------------------------

snapshot_state <- function() {
  installed <- as.data.frame(
    installed.packages(noCache = TRUE),
    stringsAsFactors = FALSE
  )
  installed$built_r_minor <- built_r_minor(installed$Built)

  outdated <- old.packages(checkBuilt = FALSE)

  list(
    installed = installed,
    available_packages = rownames(available.packages()),
    outdated_packages = if (is.null(outdated)) {
      character(0)
    } else {
      rownames(outdated)
    }
  )
}

build_managed_status <- function(installed, outdated_packages) {
  managed_status <- merge(
    data.frame(package = managed_packages, stringsAsFactors = FALSE),
    installed[, c("Package", "Version", "Built", "built_r_minor")],
    by.x = "package",
    by.y = "Package",
    all.x = TRUE
  )

  managed_status$installed <- !is.na(managed_status$Version)
  managed_status$built_on_current_r <- with(
    managed_status,
    ifelse(
      installed,
      is.na(built_r_minor) | built_r_minor == current_r_minor,
      NA
    )
  )
  managed_status$outdated <- managed_status$package %in% outdated_packages

  managed_status
}

# -- Run -----------------------------------------------------------------------

state <- snapshot_state()

managed_status <- build_managed_status(
  installed = state$installed,
  outdated_packages = state$outdated_packages
)

# Report current state
cat("Current R major.minor:", current_r_minor, "\n\n")
cat("Managed package status:\n")
print(
  managed_status[, c(
    "package",
    "installed",
    "Version",
    "Built",
    "built_on_current_r",
    "outdated"
  )],
  row.names = FALSE
)

# Find all packages built on a different R version (not just managed ones).
# These can cause hard-to-debug errors.
built_mismatch <- state$installed[
  !is.na(state$installed$built_r_minor) &
    state$installed$built_r_minor != current_r_minor,
  c("Package", "Version", "Built", "Priority")
]

cat("\nInstalled packages built on a different R major.minor:\n")
if (nrow(built_mismatch)) {
  print(built_mismatch[order(built_mismatch$Package), ], row.names = FALSE)
} else {
  cat("None.\n")
}

# Rebuild packages with R version mismatch (skip base/recommended and
# packages not available on the configured repository)
rebuild_packages <- sort(unique(
  built_mismatch$Package[!built_mismatch$Priority %in% c("base", "recommended")]
))
rebuild_skipped <- setdiff(rebuild_packages, state$available_packages)
rebuild_installable <- intersect(rebuild_packages, state$available_packages)

if (length(rebuild_skipped)) {
  message("\nSkipping wrong-R packages not available on configured repos:")
  print(rebuild_skipped)
}

if (length(rebuild_installable)) {
  message("\nReinstalling packages built on a different R version:")
  print(rebuild_installable)
  install.packages(rebuild_installable)
}

# Refresh state after rebuilds
state <- snapshot_state()
managed_status <- build_managed_status(
  installed = state$installed,
  outdated_packages = state$outdated_packages
)

# Install/upgrade managed packages that are missing, wrong-R, or outdated
managed_missing_from_repos <- setdiff(
  managed_packages,
  state$available_packages
)
managed_installable <- managed_status$package[
  !managed_status$installed |
    !managed_status$built_on_current_r |
    managed_status$outdated
]
managed_installable <- sort(unique(
  setdiff(managed_installable, managed_missing_from_repos)
))

if (length(managed_missing_from_repos)) {
  message("\nSkipping managed packages not available on configured repos:")
  print(sort(managed_missing_from_repos))
}

message("\nInstalling/upgrading managed packages:")
print(managed_installable)

if (length(managed_installable)) {
  install.packages(managed_installable)
}
