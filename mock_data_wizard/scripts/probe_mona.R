# MONA probe for the mock_data_wizard DuckDB rework.
#
# Runs a battery of tests checking whether assumptions in the planned
# rewrite hold on the MONA batch client. Writes everything to a single
# log file in the working directory, designed to be exported and shared.
#
# PII SAFETY (mandatory invariant for anything exported from MONA):
#   - No row-level data is ever logged or printed.
#   - Schema metadata (column names + types) IS logged.
#   - Aggregate statistics (count, min/max/mean/sd) on a real column
#     ARE logged -- same disclosure level as the production tool.
#   - Frequency tables on real columns are NOT logged (cell values
#     would be unsuppressed). Only the row count of the result is.
#   - The batch server echoes the script source AND captures stderr
#     into the .Rout file, so both source and message() output must
#     be PII-clean. Probes never `print()` query results at top level.
#
# Usage on the MONA batch client:
#   The exact invocation is decided by the batch server. The script
#   is self-contained and writes mdw_probe_<timestamp>.log alongside
#   itself; export that log AND/OR the .Rout file.
#
# Optional: to also test connecting MS SQL through DuckDB's ODBC scanner,
# set the project DSN in the variable below before running. Leave blank
# to skip the SQL tests (the file-side tests still tell us most of what
# we need).
PROJECT_DSN <- "P1105"            # Empty -> skip MS SQL probes.
SAMPLE_TABLE <- "Individ_2018"    # ~8M rows. Empty -> skip table probe.

# ---- 0. setup --------------------------------------------------------

ts <- format(Sys.time(), "%Y%m%d_%H%M%S")
log_path <- file.path(getwd(), sprintf("mdw_probe_%s.log", ts))
log_con <- file(log_path, open = "wt", encoding = "UTF-8")

log_line <- function(...) {
  msg <- paste0(...)
  writeLines(msg, log_con); flush(log_con)
  message(msg)
}

log_section <- function(title) {
  log_line("")
  log_line(strrep("=", 70))
  log_line(title)
  log_line(strrep("=", 70))
}

# Wrap a probe so one failure doesn't kill the run. Records ok/fail +
# the message + elapsed seconds. Result also returned in case caller
# wants to use it.
probe <- function(name, expr) {
  log_line("")
  log_line(sprintf("[probe] %s", name))
  t0 <- Sys.time()
  out <- tryCatch(
    list(ok = TRUE, value = force(expr)),
    error   = function(e) list(ok = FALSE, value = NULL, msg = conditionMessage(e)),
    warning = function(w) list(ok = TRUE,  value = NULL, msg = conditionMessage(w))
  )
  dt <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  status <- if (isTRUE(out$ok)) "OK" else "FAIL"
  log_line(sprintf("  -> %s (%.2fs)", status, dt))
  if (!is.null(out$msg)) log_line(sprintf("     %s", out$msg))
  invisible(out)
}

# ---- 1. environment --------------------------------------------------

log_section("1. ENVIRONMENT")
log_line(sprintf("R version:       %s", R.version.string))
log_line(sprintf("Platform:        %s", R.version$platform))
log_line(sprintf("OS:              %s", Sys.info()[["sysname"]]))
log_line(sprintf("Machine:         %s", Sys.info()[["machine"]]))
log_line(sprintf("Locale:          %s", Sys.getlocale()))
log_line(sprintf("getwd():         %s", getwd()))
log_line(sprintf("tempdir():       %s", tempdir()))
log_line(sprintf("R_LIBS_USER:     %s", Sys.getenv("R_LIBS_USER")))
log_line(sprintf(".libPaths()[1]:  %s", .libPaths()[1]))
log_line(sprintf("CRAN mirror:     %s", getOption("repos")[["CRAN"]]))
log_line(sprintf("Probe log file:  %s", log_path))

# Free space in tempdir, getwd(), and the project share. Critical for
# the DuckDB rework: DuckDB spills sort/hash to temp_directory when
# working memory exceeds memory_limit. If C:\Windows\TEMP is small,
# we need to point temp_directory somewhere bigger.
free_space_mb <- function(path) {
  out <- tryCatch({
    if (.Platform$OS.type == "windows") {
      drv <- substr(normalizePath(path, winslash = "\\", mustWork = FALSE), 1, 2)
      r <- tryCatch(
        system2("cmd", c("/C", "fsutil", "volume", "diskfree", drv), stdout = TRUE, stderr = TRUE),
        error = function(e) character()
      )
      if (length(r) > 0) {
        # fsutil prints lines with raw byte counts; pick the "available" one
        line <- grep("avail", r, ignore.case = TRUE, value = TRUE)[1]
        bytes <- as.numeric(gsub("[^0-9]", "", line))
        if (!is.na(bytes) && bytes > 0) return(bytes / 1024^2)
      }
      # fallback: list().. cheap heuristic
      NA_real_
    } else {
      r <- system2("df", c("-Pm", shQuote(path)), stdout = TRUE)
      as.numeric(strsplit(r[2], "\\s+")[[1]][4])
    }
  }, error = function(e) NA_real_)
  out
}

for (p in c(tempdir(), getwd())) {
  mb <- free_space_mb(p)
  if (is.na(mb)) {
    log_line(sprintf("Free space in   %-50s : <unknown>", p))
  } else {
    log_line(sprintf("Free space in   %-50s : %.1f MB (%.2f GB)",
                     p, mb, mb / 1024))
  }
}

# ---- 2. CRAN reachability + already-installed packages ---------------

log_section("2. CRAN AND INSTALLED PACKAGES")

probe("getOption('repos') resolves", {
  repos <- getOption("repos")
  if (is.null(repos) || identical(unname(repos[["CRAN"]]), "@CRAN@")) {
    stop("CRAN mirror is the @CRAN@ sentinel (would prompt interactively)")
  }
  log_line(sprintf("     CRAN -> %s", repos[["CRAN"]]))
  TRUE
})

probe("available.packages() reaches CRAN", {
  ap <- available.packages()
  log_line(sprintf("     %d packages visible on CRAN", nrow(ap)))
  for (p in c("duckdb", "DBI", "odbc", "data.table", "jsonlite", "arrow")) {
    if (p %in% rownames(ap)) {
      log_line(sprintf("     %-12s available, version %s", p, ap[p, "Version"]))
    } else {
      log_line(sprintf("     %-12s NOT FOUND on this CRAN mirror", p))
    }
  }
  TRUE
})

for (pkg in c("DBI", "odbc", "data.table", "jsonlite", "duckdb", "arrow")) {
  probe(sprintf("'%s' already installed", pkg), {
    if (pkg %in% rownames(installed.packages())) {
      v <- as.character(packageVersion(pkg))
      log_line(sprintf("     installed, version %s", v))
      TRUE
    } else {
      log_line("     NOT installed")
      FALSE
    }
  })
}

# ---- 3. install duckdb if missing ------------------------------------

log_section("3. DUCKDB INSTALL")

probe("install.packages('duckdb')", {
  if ("duckdb" %in% rownames(installed.packages())) {
    log_line("     already installed, skipping install")
    return(TRUE)
  }
  install.packages("duckdb", quiet = TRUE)
  if (!"duckdb" %in% rownames(installed.packages())) {
    stop("install.packages succeeded silently but package not found")
  }
  log_line(sprintf("     installed version %s", as.character(packageVersion("duckdb"))))
  TRUE
})

probe("library(duckdb) loads", {
  suppressPackageStartupMessages(library(duckdb))
  log_line(sprintf("     duckdb version %s", as.character(packageVersion("duckdb"))))
  TRUE
})

# ---- 4. duckdb basic functionality -----------------------------------

log_section("4. DUCKDB BASIC")

duck_con <- NULL

probe("open in-memory DuckDB connection", {
  duck_con <<- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  TRUE
})

probe("trivial SELECT", {
  r <- DBI::dbGetQuery(duck_con, "SELECT 1 AS x, 'hi' AS y")
  log_line(sprintf("     got %d row(s), %d col(s)", nrow(r), ncol(r)))
  TRUE
})

probe("server-side aggregations: STDDEV / PERCENTILE_CONT / APPROX_COUNT_DISTINCT", {
  DBI::dbExecute(duck_con, "CREATE TABLE t AS SELECT range AS n FROM range(0, 100000)")
  r <- DBI::dbGetQuery(duck_con, "
    SELECT
      MIN(n) AS min_n, MAX(n) AS max_n, AVG(n) AS mean_n,
      STDDEV(n) AS sd_n,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY n) AS p50,
      APPROX_COUNT_DISTINCT(n) AS approx_nd
    FROM t
  ")
  log_line(sprintf("     min=%.0f max=%.0f mean=%.2f sd=%.2f p50=%.0f approx_nd=%d",
                   r$min_n, r$max_n, r$mean_n, r$sd_n, r$p50, r$approx_nd))
  DBI::dbExecute(duck_con, "DROP TABLE t")
  TRUE
})

probe("USING SAMPLE for classification", {
  DBI::dbExecute(duck_con, "CREATE TABLE t AS SELECT range AS n FROM range(0, 1000000)")
  r <- DBI::dbGetQuery(duck_con, "SELECT * FROM t USING SAMPLE 1000 ROWS")
  log_line(sprintf("     sample returned %d rows", nrow(r)))
  DBI::dbExecute(duck_con, "DROP TABLE t")
  TRUE
})

probe("rework policy: temp_directory + exact aggregates (no memory_limit override)", {
  # The actual config the rework will set. We deliberately do NOT
  # override memory_limit -- the batch server has 150-200GB RAM and
  # DuckDB's default (80% of available) gives ~120GB to play with,
  # plenty for exact aggregation on any single-CSV workload we'll see.
  DBI::dbExecute(duck_con, sprintf("SET temp_directory = '%s'",
                                   gsub("\\\\", "/", tempdir())))
  DBI::dbExecute(duck_con, "SET preserve_insertion_order = false")
  # Report effective memory_limit so we have a record per run.
  ml <- DBI::dbGetQuery(duck_con, "SELECT current_setting('memory_limit') AS mem")
  log_line(sprintf("     effective memory_limit = %s", ml$mem))
  DBI::dbExecute(duck_con, "CREATE TABLE t AS SELECT range AS n FROM range(0, 1000000)")
  r <- DBI::dbGetQuery(duck_con, "
    SELECT
      COUNT(DISTINCT n) AS exact_nd,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY n) AS exact_p50,
      PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY n) AS exact_p99
    FROM t
  ")
  log_line(sprintf("     exact_nd=%d  exact_p50=%.0f  exact_p99=%.0f",
                   r$exact_nd, r$exact_p50, r$exact_p99))
  DBI::dbExecute(duck_con, "DROP TABLE t")
  TRUE
})

# ---- 5. duckdb CSV reading -------------------------------------------

log_section("5. DUCKDB CSV READING")

# Build a small fully-synthetic CSV in tempdir. Neutral identifiers
# only -- nothing personnummer-shaped, even though it's all generated.
csv_path <- file.path(tempdir(), "mdw_probe_sample.csv")
df <- data.frame(
  id    = sprintf("ROW%07d", seq_len(1000L)),
  value = as.integer(round(rnorm(1000, 50, 15))),
  group = sample(c("A", "B", "C"), 1000, TRUE),
  stringsAsFactors = FALSE
)
write.csv(df, csv_path, row.names = FALSE)
log_line(sprintf("Wrote test CSV: %s (%d rows)", csv_path, nrow(df)))

probe("read_csv_auto on a small CSV", {
  q <- sprintf("SELECT COUNT(*) AS n FROM read_csv_auto('%s')",
               gsub("'", "''", csv_path))
  r <- DBI::dbGetQuery(duck_con, q)
  log_line(sprintf("     count via read_csv_auto: %d", r$n))
  TRUE
})

probe("CREATE VIEW over read_csv_auto + aggregate", {
  DBI::dbExecute(duck_con, sprintf(
    "CREATE OR REPLACE VIEW vcsv AS SELECT * FROM read_csv_auto('%s')",
    gsub("'", "''", csv_path)
  ))
  r <- DBI::dbGetQuery(duck_con, "
    SELECT \"group\" AS g,
           COUNT(*) AS n,
           AVG(value) AS mean_value
    FROM vcsv GROUP BY \"group\" ORDER BY g
  ")
  # Synthetic data, safe to log values
  log_line(sprintf("     %d groups", nrow(r)))
  for (i in seq_len(nrow(r))) {
    log_line(sprintf("     %s: n=%d mean=%.2f", r$g[i], r$n[i], r$mean_value[i]))
  }
  TRUE
})

probe("DESCRIBE on the view (column names + types)", {
  r <- DBI::dbGetQuery(duck_con, "DESCRIBE vcsv")
  log_line(sprintf("     %d columns", nrow(r)))
  for (i in seq_len(nrow(r))) {
    log_line(sprintf("     %-20s %s", r$column_name[i], r$column_type[i]))
  }
  TRUE
})

# ---- 6. duckdb large-CSV memory behaviour ----------------------------

log_section("6. DUCKDB LARGE-CSV MEMORY BEHAVIOUR")

# 5M rows, ~100MB CSV. Tests that DuckDB streams instead of materialising,
# and that the rework's chosen aggregate functions all work at scale.
# Uses data.table::fwrite for reliable large-CSV output (the previous
# hand-rolled writeLines version produced a corrupt row ~5M into the file).
big_csv <- file.path(tempdir(), "mdw_probe_big.csv")
probe("write 5M-row CSV via data.table::fwrite", {
  if (!"data.table" %in% loadedNamespaces()) {
    suppressPackageStartupMessages(library(data.table))
  }
  set.seed(1)
  N <- 5000000L
  big_dt <- data.table::data.table(
    id    = sprintf("ID%07d", seq_len(N)),
    age   = sample(0:100, N, TRUE),
    group = sample(c("A","B","C","D"), N, TRUE)
  )
  data.table::fwrite(big_dt, big_csv)
  rm(big_dt); invisible(gc(verbose = FALSE))
  log_line(sprintf("     wrote %s (%.1f MB)",
                   big_csv, file.info(big_csv)$size / 1024^2))
  TRUE
})

probe("aggregate 5M-row CSV (rework policy: exact aggs, default memory_limit)", {
  before <- gc(reset = TRUE)
  t0 <- Sys.time()
  q <- sprintf("
    SELECT
      COUNT(*) AS n,
      COUNT(DISTINCT id) AS nd_id,
      AVG(age) AS mean_age,
      STDDEV(age) AS sd_age,
      PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY age) AS p50,
      PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY age) AS p99
    FROM read_csv_auto('%s')
  ", gsub("'", "''", big_csv))
  r <- DBI::dbGetQuery(duck_con, q)
  secs <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  after <- gc()
  used_mb <- sum(after[, 2]) - sum(before[, 2])
  log_line(sprintf("     n=%d nd_id=%d mean_age=%.2f sd_age=%.2f p50=%.1f p99=%.1f",
                   r$n, r$nd_id, r$mean_age, r$sd_age, r$p50, r$p99))
  log_line(sprintf("     elapsed=%.2fs  R heap delta=%+.1f MB", secs, used_mb))
  # Check if anything spilled to temp_directory while the query ran.
  spill_files <- list.files(tempdir(), pattern = "duckdb.*\\.tmp$",
                            recursive = TRUE, full.names = TRUE)
  log_line(sprintf("     duckdb temp files left in tempdir: %d", length(spill_files)))
  TRUE
})

# ---- 7. duckdb community odbc extension (the big question) -----------

log_section("7. DUCKDB ODBC SCANNER EXTENSION (COMMUNITY)")

probe("INSTALL odbc_scanner FROM community", {
  DBI::dbExecute(duck_con, "INSTALL odbc_scanner FROM community")
  TRUE
})

probe("LOAD odbc_scanner", {
  DBI::dbExecute(duck_con, "LOAD odbc_scanner")
  TRUE
})

probe("list installed extensions", {
  r <- DBI::dbGetQuery(duck_con, "SELECT extension_name, installed, loaded FROM duckdb_extensions() WHERE installed OR loaded")
  for (i in seq_len(nrow(r))) {
    log_line(sprintf("     %-20s installed=%s loaded=%s",
                     r$extension_name[i], r$installed[i], r$loaded[i]))
  }
  TRUE
})

# ---- 8. odbc / DBI baseline + DSN inventory --------------------------

log_section("8. ODBC / DBI BASELINE")

probe("library(DBI) + library(odbc)", {
  suppressPackageStartupMessages({ library(DBI); library(odbc) })
  log_line(sprintf("     DBI %s, odbc %s",
                   as.character(packageVersion("DBI")),
                   as.character(packageVersion("odbc"))))
  TRUE
})

probe("odbc::odbcListDrivers()", {
  r <- odbc::odbcListDrivers()
  log_line(sprintf("     %d drivers", nrow(r)))
  for (i in seq_len(nrow(r))) log_line(sprintf("     %s", r$name[i]))
  TRUE
})

probe("odbc::odbcListDataSources()", {
  r <- odbc::odbcListDataSources()
  log_line(sprintf("     %d DSNs", nrow(r)))
  for (i in seq_len(nrow(r))) log_line(sprintf("     %s [%s]", r$name[i], r$description[i]))
  TRUE
})

# ---- 9. MS SQL probe (if DSN provided) -------------------------------

log_section("9. MS SQL VIA ODBC (optional)")

if (nzchar(PROJECT_DSN)) {
  log_line(sprintf("Using DSN: %s", PROJECT_DSN))

  sql_con <- NULL
  probe(sprintf("DBI::dbConnect(odbc::odbc(), dsn='%s')", PROJECT_DSN), {
    sql_con <<- DBI::dbConnect(odbc::odbc(), dsn = PROJECT_DSN)
    TRUE
  })

  if (!is.null(sql_con)) {
    probe("SELECT 1 against MS SQL", {
      r <- DBI::dbGetQuery(sql_con, "SELECT 1 AS x")
      log_line(sprintf("     ok, got %d row(s)", nrow(r)))
      TRUE
    })

    probe("MS SQL @@VERSION", {
      r <- DBI::dbGetQuery(sql_con, "SELECT @@VERSION AS v")
      log_line(sprintf("     %s", substr(as.character(r$v), 1, 200)))
      TRUE
    })

    probe("server-side aggregation on MS SQL: STDEV + PERCENTILE_CONT", {
      r <- DBI::dbGetQuery(sql_con, "
        SELECT
          STDEV(CAST(n AS FLOAT)) AS sd_n,
          PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CAST(n AS FLOAT)) OVER () AS p50
        FROM (SELECT 1 n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5) t
      ")
      log_line(sprintf("     sd=%.4f p50=%.2f", r$sd_n[1], r$p50[1]))
      TRUE
    })

    probe("server-side APPROX_COUNT_DISTINCT (SQL Server 2019+)", {
      r <- DBI::dbGetQuery(sql_con, "
        SELECT APPROX_COUNT_DISTINCT(n) AS nd
        FROM (SELECT 1 n UNION ALL SELECT 2 UNION ALL SELECT 1 UNION ALL SELECT 3) t
      ")
      log_line(sprintf("     nd=%d", r$nd[1]))
      TRUE
    })

    if (nzchar(SAMPLE_TABLE)) {
      probe(sprintf("COUNT(*) on table %s (server-side)", SAMPLE_TABLE), {
        q <- sprintf("SELECT COUNT_BIG(*) AS n FROM %s", SAMPLE_TABLE)
        r <- DBI::dbGetQuery(sql_con, q)
        log_line(sprintf("     n_rows = %s", format(r$n[1], scientific = FALSE)))
        TRUE
      })

      # NOTE: deliberately no `SELECT TOP N *` probe. We can confirm the
      # table is queryable via INFORMATION_SCHEMA + COUNT_BIG without
      # ever pulling row-level data into R memory.

      sample_cols <- NULL
      probe(sprintf("INFORMATION_SCHEMA.COLUMNS for %s", SAMPLE_TABLE), {
        q <- sprintf("
          SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
          FROM INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_NAME = '%s'
          ORDER BY ORDINAL_POSITION
        ", gsub("'", "''", SAMPLE_TABLE))
        r <- DBI::dbGetQuery(sql_con, q)
        sample_cols <<- r
        log_line(sprintf("     %d columns", nrow(r)))
        for (i in seq_len(min(nrow(r), 30))) {
          log_line(sprintf("     %-30s %s", r$COLUMN_NAME[i], r$DATA_TYPE[i]))
        }
        if (nrow(r) > 30) log_line(sprintf("     ... (%d more)", nrow(r) - 30))
        TRUE
      })

      # The real fallback-path question: does server-side aggregation on
      # an 8M-row table return a small result with bounded R memory?
      # PII safety: we run MIN/MAX/AVG/STDEV against the chosen column
      # to exercise the aggregation pipeline, but the resulting numbers
      # are NOT logged. We log only the row count, elapsed time, R heap
      # delta, and (boolean) whether each aggregate came back finite.
      probe(sprintf("server-side COUNT_BIG + STDEV on %s (timing/memory only)", SAMPLE_TABLE), {
        numeric_types <- c("int", "bigint", "smallint", "tinyint", "decimal",
                           "numeric", "float", "real", "money", "smallmoney")
        num_col <- if (!is.null(sample_cols)) {
          hits <- sample_cols$COLUMN_NAME[tolower(sample_cols$DATA_TYPE) %in% numeric_types]
          if (length(hits) > 0) hits[1] else NA
        } else NA

        q <- if (!is.na(num_col)) {
          sprintf("
            SELECT
              COUNT_BIG(*) AS n,
              MIN(CAST([%s] AS FLOAT)) AS min_v,
              MAX(CAST([%s] AS FLOAT)) AS max_v,
              AVG(CAST([%s] AS FLOAT)) AS mean_v,
              STDEV(CAST([%s] AS FLOAT)) AS sd_v
            FROM %s
          ", num_col, num_col, num_col, num_col, SAMPLE_TABLE)
        } else {
          sprintf("SELECT COUNT_BIG(*) AS n FROM %s", SAMPLE_TABLE)
        }
        before <- gc(reset = TRUE)
        t0 <- Sys.time()
        r <- DBI::dbGetQuery(sql_con, q)
        secs <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
        after <- gc()
        used_mb <- sum(after[, 2]) - sum(before[, 2])
        all_finite <- if (!is.na(num_col) && "min_v" %in% names(r)) {
          all(is.finite(c(r$min_v[1], r$max_v[1], r$mean_v[1], r$sd_v[1])))
        } else NA
        # Drop the result frame before logging anything so it can't
        # leak via subsequent error formatters.
        n_rows <- r$n[1]
        rm(r); invisible(gc(verbose = FALSE))
        log_line(sprintf("     col='%s'  n_rows=%s  elapsed=%.2fs  R-heap-delta=%+.1f MB  aggs_finite=%s",
                         if (!is.na(num_col)) num_col else "<count only>",
                         format(n_rows, scientific = FALSE), secs, used_mb,
                         if (is.na(all_finite)) "n/a" else as.character(all_finite)))
        TRUE
      })

      probe(sprintf("server-side GROUP BY on %s (count-only, no values logged)", SAMPLE_TABLE), {
        # Pick a likely categorical column: short char/varchar.
        # PII safety: the cell values and counts are NEVER logged.
        # We log only the column name (schema) and the number of
        # groups returned, which is what the rework actually needs
        # to know -- "does GROUP BY return a small bounded result?"
        cat_col <- if (!is.null(sample_cols)) {
          short_chars <- sample_cols[
            tolower(sample_cols$DATA_TYPE) %in% c("char", "varchar", "nchar", "nvarchar") &
              (is.na(sample_cols$CHARACTER_MAXIMUM_LENGTH) |
                 sample_cols$CHARACTER_MAXIMUM_LENGTH <= 10),
          ]
          if (nrow(short_chars) > 0) short_chars$COLUMN_NAME[1] else NA
        } else NA

        if (is.na(cat_col)) {
          log_line("     no obvious categorical column found, skipping")
          return(TRUE)
        }
        # COUNT(DISTINCT) returns a single number, not the values.
        # Bounds the result to one row regardless of cardinality.
        q <- sprintf("SELECT COUNT(DISTINCT [%s]) AS nd FROM %s",
                     cat_col, SAMPLE_TABLE)
        before <- gc(reset = TRUE)
        t0 <- Sys.time()
        r <- DBI::dbGetQuery(sql_con, q)
        secs <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
        after <- gc()
        used_mb <- sum(after[, 2]) - sum(before[, 2])
        log_line(sprintf("     col='%s'  n_distinct=%s  elapsed=%.2fs  R-heap-delta=%+.1f MB",
                         cat_col, format(r$nd[1], scientific = FALSE),
                         secs, used_mb))
        rm(r); invisible(gc(verbose = FALSE))
        TRUE
      })
    }

    # The actual unification question: can DuckDB query MS SQL via the
    # odbc_scanner extension? Only meaningful if section 7 succeeded.
    duck_attached <- FALSE
    probe("DuckDB ATTACH MS SQL via odbc_scanner", {
      attach_sql <- sprintf("ATTACH 'DSN=%s' AS sqldb (TYPE odbc)", PROJECT_DSN)
      DBI::dbExecute(duck_con, attach_sql)
      r <- DBI::dbGetQuery(duck_con, "SELECT 1 AS x")
      duck_attached <<- TRUE
      log_line(sprintf("     attached + queried, got %d row(s)", nrow(r)))
      TRUE
    })

    if (duck_attached && nzchar(SAMPLE_TABLE)) {
      # Cross-engine sanity: same aggregation through DuckDB->odbc bridge.
      probe(sprintf("DuckDB->ODBC: COUNT(*) on sqldb.%s", SAMPLE_TABLE), {
        before <- gc(reset = TRUE)
        t0 <- Sys.time()
        r <- DBI::dbGetQuery(duck_con,
                             sprintf("SELECT COUNT(*) AS n FROM sqldb.%s", SAMPLE_TABLE))
        secs <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
        after <- gc()
        used_mb <- sum(after[, 2]) - sum(before[, 2])
        log_line(sprintf("     n=%s  elapsed=%.2fs  R-heap-delta=%+.1f MB",
                         format(r$n[1], scientific = FALSE), secs, used_mb))
        TRUE
      })
    }

    tryCatch(DBI::dbDisconnect(sql_con), error = function(e) NULL)
  }
} else {
  log_line("PROJECT_DSN is empty -- skipping MS SQL probes.")
  log_line("Edit the top of this script to set PROJECT_DSN to your project DSN")
  log_line("(e.g. 'P1105') and SAMPLE_TABLE to a small view to enable section 9.")
}

# ---- 10. cleanup -----------------------------------------------------

log_section("10. CLEANUP")

probe("disconnect DuckDB", {
  DBI::dbDisconnect(duck_con, shutdown = TRUE)
  TRUE
})

probe("remove probe CSVs", {
  for (p in c(csv_path, big_csv)) {
    if (file.exists(p)) file.remove(p)
  }
  TRUE
})

log_line("")
log_line(strrep("=", 70))
log_line("DONE")
log_line(sprintf("Log written to: %s", log_path))
log_line(strrep("=", 70))

close(log_con)
