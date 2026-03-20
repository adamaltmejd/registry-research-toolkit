"""Generate R script for extracting aggregate stats from MONA project data."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PureWindowsPath
from textwrap import dedent

from .stats import CONTRACT_VERSION

# ── Name-based column classification ──────────────────────────────────────
#
# Columns are classified by matching their name against known SCB patterns.
# These run before any data-driven heuristics. First match wins.
#
# Patterns are case-insensitive R regexes matched with grepl().
# Each CategoricalPattern has a max_distinct cap — if the column has more
# distinct values than the cap, the match is ignored (likely a false positive).


@dataclass(frozen=True)
class IdPattern:
    pattern: str  # R regex, case-insensitive
    exclude: str | None = None


@dataclass(frozen=True)
class CategoricalPattern:
    pattern: str  # R regex, case-insensitive
    max_distinct: int  # skip match if n_distinct exceeds this
    exclude: str | None = None


ID_PATTERNS: list[IdPattern] = [
    IdPattern("lopnr"),  # MONA record linkage key
]

CATEGORICAL_PATTERNS: list[CategoricalPattern] = [
    CategoricalPattern(
        "kommun", max_distinct=500, exclude="kommunikation"
    ),  # municipality ~290
    CategoricalPattern("ssyk", max_distinct=1000),  # occupation (SSYK) ~400 at 4-digit
    CategoricalPattern("sun2000", max_distinct=1000),  # education (SUN2000) ~600
    CategoricalPattern("sun2020", max_distinct=1000),  # education (SUN2020) ~600
    CategoricalPattern(
        "sni(\\\\d|_|$)", max_distinct=1500
    ),  # industry (SNI) ~800 at 5-digit
    CategoricalPattern(
        "(fodelse|fodelses?)land", max_distinct=300
    ),  # country of birth ~230
    CategoricalPattern(
        "medb(orgarskap)?", max_distinct=300
    ),  # citizenship ~230
]

# ── Data-driven classification thresholds ─────────────────────────────────
#
# Applied when no name pattern matches. A column is categorical if
# n_distinct <= min(FREQ_CAP, n_rows * FREQ_RATIO). Otherwise numeric
# columns with near-unique values are classified as ID.

FREQ_CAP = 50  # absolute max distinct values to still count as categorical
FREQ_RATIO = 0.01  # relative max (fraction of n_rows)
NUMERIC_ID_RATIO = 0.95  # numeric ID if n_distinct > ratio × n_rows ...
NUMERIC_ID_MIN = 100  # ... and n_distinct > this minimum
STRING_ID_RATIO = 0.5  # string ID if n_distinct > ratio × n_rows ...
STRING_ID_MIN = 100  # ... and n_distinct > this minimum

# ── Disclosure control ────────────────────────────────────────────────────
#
# Applied when summarizing columns to prevent leaking individual-level data.
# Categorical: values with count < SUPPRESS_K are merged into "_other".
# Numeric: all aggregate stats (min, max, mean, sd, quantiles) are perturbed
# by uniform noise in [-NOISE_PCT, +NOISE_PCT] relative to the true value.

SUPPRESS_K = 5  # k-anonymity threshold for categorical frequency tables
NOISE_PCT = 0.005  # ±0.5% relative noise on numeric aggregates

# ── Date detection ────────────────────────────────────────────────────────
#
# A string column is classified as date if >CLASSIFY_THRESHOLD of a 200-row
# sample parses successfully. When summarizing, >SUMMARIZE_THRESHOLD must
# parse to extract min/max (lower bar since we already committed to date type).

DATE_FORMATS = ["%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y", "%Y%m%d"]
DATE_CLASSIFY_THRESHOLD = 0.8
DATE_SUMMARIZE_THRESHOLD = 0.5

# ── File scanning ─────────────────────────────────────────────────────────

FILE_PATTERN = "\\\\.(csv|CSV|txt|TXT)$"


# ── R code generation helpers ─────────────────────────────────────────────


def _build_r_id_check() -> str:
    """Generate R code for name-based ID detection."""
    lines = []
    for p in ID_PATTERNS:
        cond = f'grepl("{p.pattern}", col_name, ignore.case = TRUE)'
        if p.exclude:
            cond += f' && !grepl("{p.exclude}", col_name, ignore.case = TRUE)'
        lines.append(f"  if ({cond}) return(TRUE)")
    lines.append("  FALSE")
    return "\n".join(lines)


def _build_r_categorical_check() -> str:
    """Generate R code for name-based categorical detection with caps."""
    lines = []
    for p in CATEGORICAL_PATTERNS:
        perl = ", perl = TRUE" if "\\" in p.pattern else ""
        cond = f'grepl("{p.pattern}", col_name, ignore.case = TRUE{perl})'
        if p.exclude:
            cond += f' && !grepl("{p.exclude}", col_name, ignore.case = TRUE)'
        lines.append(f"  if ({cond}) return({p.max_distinct}L)")
    lines.append("  NA_integer_")
    return "\n".join(lines)


def _format_r_paths(paths: list[str]) -> str:
    """Format paths as an R character vector, preserving Windows backslashes."""
    escaped = []
    for p in paths:
        win = str(PureWindowsPath(p))
        escaped.append('"' + win.replace("\\", "\\\\") + '"')
    return "c(\n  " + ",\n  ".join(escaped) + "\n)"


def _format_r_string_vec(items: list[str]) -> str:
    """Format a Python list of strings as an R character vector."""
    return "c(" + ", ".join(f'"{s}"' for s in items) + ")"


R_TEMPLATE = dedent("""\
    # ── mock-data-wizard stats extractor ──────────────────────────────
    # Generated by mock-data-wizard. Run this script on MONA to produce
    # aggregate statistics. NO individual-level data is exported.
    #
    # Output: see OUTPUT_PATH below
    # ──────────────────────────────────────────────────────────────────

    # Prevent .RData save on exit — only delete if we created it
    .had_rdata <- file.exists(".RData")
    .Last <- function() {{
      if (!.had_rdata && file.exists(".RData")) file.remove(".RData")
    }}

    if (!requireNamespace("data.table", quietly = TRUE))
      stop("Package 'data.table' is required. Install with: install.packages('data.table')")
    if (!requireNamespace("jsonlite", quietly = TRUE))
      stop("Package 'jsonlite' is required. Install with: install.packages('jsonlite')")

    library(data.table)
    library(jsonlite)

    # ── Configuration ─────────────────────────────────────────────────
    # Folders to scan for data files
    PROJECT_PATHS  <- {project_paths_r}

    # Column classification thresholds
    FREQ_CAP       <- {freq_cap}L        # max distinct values to classify as categorical
    FREQ_RATIO     <- {freq_ratio}       # ... or this fraction of n_rows, whichever is smaller
    NUMERIC_ID_RATIO <- {numeric_id_ratio}  # numeric col is ID if n_distinct > ratio * n_rows
    NUMERIC_ID_MIN <- {numeric_id_min}L     # ... and n_distinct exceeds this minimum
    STRING_ID_RATIO <- {string_id_ratio}    # string col is ID if n_distinct > ratio * n_rows
    STRING_ID_MIN  <- {string_id_min}L      # ... and n_distinct exceeds this minimum

    # Disclosure control — prevents leaking individual-level data
    SUPPRESS_K     <- {suppress_k}L      # merge categorical values with count < k into "_other"
    NOISE_PCT      <- {noise_pct}        # perturb numeric stats by +/- this fraction

    # Date detection
    DATE_FORMATS   <- {date_formats_r}
    DATE_CLASSIFY_THRESHOLD  <- {date_classify_threshold}  # fraction of sample that must parse as date
    DATE_SUMMARIZE_THRESHOLD <- {date_summarize_threshold}  # fraction needed to extract min/max

    # File scanning
    FILE_PATTERN   <- "{file_pattern}"   # regex for data files to process
    OUTPUT_PATH    <- file.path(getwd(), "stats.json")

    # ── Helpers ───────────────────────────────────────────────────────
    safe_read <- function(path) {{
      tryCatch(
        data.table::fread(path, nThread = 1L),
        error = function(e) {{
          message(sprintf("SKIP (read error): %s — %s", path, conditionMessage(e)))
          NULL
        }}
      )
    }}

    # Name-based ID detection
    is_known_id <- function(col_name) {{
    {id_check}
    }}

    # Name-based categorical detection (returns max n_distinct cap, or NA)
    known_categorical_cap <- function(col_name) {{
    {categorical_check}
    }}

    classify_column <- function(x, n_rows, col_name) {{
      if (is_known_id(col_name)) return("id")
      cap <- known_categorical_cap(col_name)
      if (!is.na(cap) && uniqueN(x, na.rm = TRUE) <= cap) return("categorical")

      nd <- uniqueN(x, na.rm = TRUE)
      threshold <- min(FREQ_CAP, as.integer(n_rows * FREQ_RATIO))
      if (threshold < 2L) threshold <- 2L

      if (is.logical(x)) return("categorical")

      if (is.numeric(x) || is.integer(x)) {{
        if (nd > n_rows * NUMERIC_ID_RATIO && nd > NUMERIC_ID_MIN) return("id")
        if (nd <= threshold) return("categorical")
        # Detect YYYYMMDD integer dates
        clean <- x[!is.na(x)]
        if (length(clean) > 0L && all(clean == floor(clean))) {{
          sample_n <- head(clean, 200L)
          if (all(sample_n >= 18000101 & sample_n <= 22001231)) {{
            parsed <- suppressWarnings(as.Date(as.character(sample_n), format = "%Y%m%d"))
            if (sum(!is.na(parsed)) > length(sample_n) * DATE_CLASSIFY_THRESHOLD) return("date")
          }}
        }}
        return("numeric")
      }}

      vals <- as.character(x[!is.na(x)])
      if (length(vals) == 0L) return("high_cardinality")

      # Try date detection
      sample_vals <- head(vals, 200L)
      for (fmt in DATE_FORMATS) {{
        parsed <- suppressWarnings(as.Date(sample_vals, format = fmt))
        if (sum(!is.na(parsed)) > length(sample_vals) * DATE_CLASSIFY_THRESHOLD) return("date")
      }}

      if (nd > n_rows * STRING_ID_RATIO && nd > STRING_ID_MIN) return("id")
      if (nd <= threshold) return("categorical")
      return("high_cardinality")
    }}

    # Add disclosure-control noise to a numeric value
    perturb <- function(val, is_int = FALSE) {{
      noise <- val * runif(1, -NOISE_PCT, NOISE_PCT)
      out <- val + noise
      if (is_int) out <- round(out)
      out
    }}

    summarize_column <- function(x, col_type, n_rows) {{
      null_count <- sum(is.na(x))
      null_rate  <- null_count / max(n_rows, 1L)
      nd         <- uniqueN(x, na.rm = TRUE)

      base <- list(
        nullable   = null_count > 0L,
        null_count = null_count,
        null_rate  = round(null_rate, 6),
        n_distinct = nd
      )

      stats <- list()

      if (col_type == "numeric") {{
        clean <- as.numeric(x[!is.na(x)])
        if (length(clean) > 0L) {{
          is_int <- all(clean == floor(clean))
          stats$numeric_subtype <- if (is_int) "integer" else "double"
          stats$min       <- perturb(min(clean), is_int)
          stats$max       <- perturb(max(clean), is_int)
          stats$mean      <- round(perturb(mean(clean)), 6)
          stats$sd        <- round(perturb(sd(clean)), 6)
          raw_q <- quantile(clean, probs = c(0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99), na.rm = TRUE)
          stats$quantiles <- as.list(sapply(raw_q, function(q) round(perturb(q, is_int), if (is_int) 0 else 6)))
          names(stats$quantiles) <- c("p01", "p05", "p25", "p50", "p75", "p95", "p99")
        }}
      }} else if (col_type == "categorical") {{
        tbl <- sort(table(as.character(x[!is.na(x)])), decreasing = TRUE)
        # Suppress rare values (k-anonymity) — merge into _other
        counts   <- as.integer(tbl)
        labels   <- names(tbl)
        keep     <- counts >= SUPPRESS_K
        freq_out <- as.list(counts[keep])
        names(freq_out) <- labels[keep]
        suppressed_total <- sum(counts[!keep])
        if (suppressed_total > 0L) {{
          freq_out[["_other"]] <- suppressed_total
        }}
        stats$frequencies <- freq_out
        stats$suppressed_below_k <- SUPPRESS_K
      }} else if (col_type == "high_cardinality") {{
        vals <- as.character(x[!is.na(x)])
        if (length(vals) > 0L) {{
          lens <- nchar(vals)
          stats$min_length <- min(lens)
          stats$max_length <- max(lens)
          stats$mean_length <- round(mean(lens), 1)
        }}
      }} else if (col_type == "date") {{
        parsed <- NULL
        for (fmt in DATE_FORMATS) {{
          attempt <- suppressWarnings(as.Date(as.character(x[!is.na(x)]), format = fmt))
          if (sum(!is.na(attempt)) > length(attempt) * DATE_SUMMARIZE_THRESHOLD) {{
            parsed <- attempt[!is.na(attempt)]
            stats$date_format <- fmt
            break
          }}
        }}
        if (!is.null(parsed) && length(parsed) > 0L) {{
          stats$min <- as.character(min(parsed))
          stats$max <- as.character(max(parsed))
        }}
      }} else if (col_type == "id") {{
        if (is.numeric(x) || is.integer(x)) {{
          stats$id_subtype <- "integer"
        }} else {{
          stats$id_subtype <- "string"
        }}
      }}

      base$stats <- stats
      base
    }}

    # ── Main ──────────────────────────────────────────────────────────
    csv_files <- character(0)
    for (pp in PROJECT_PATHS) {{
      pp_norm <- normalizePath(pp, mustWork = TRUE)
      found <- list.files(pp_norm, pattern = FILE_PATTERN,
                          full.names = TRUE, recursive = TRUE)
      csv_files <- c(csv_files, normalizePath(found, mustWork = FALSE))
    }}
    csv_files <- unique(csv_files)

    if (length(csv_files) == 0L) {{
      stop("No data files found in project paths.")
    }}

    message(sprintf("Found %d data file(s)", length(csv_files)))

    file_results <- list()
    all_columns  <- list()  # column_name -> list of file_names

    for (fp in csv_files) {{
      message(sprintf("Processing: %s", fp))
      dt <- safe_read(fp)
      if (is.null(dt) || nrow(dt) == 0L) next

      n_rows  <- nrow(dt)
      columns <- list()

      for (cname in names(dt)) {{
        col_type <- classify_column(dt[[cname]], n_rows, cname)
        col_summary <- summarize_column(dt[[cname]], col_type, n_rows)
        col_summary$column_name   <- cname
        col_summary$inferred_type <- col_type
        columns[[length(columns) + 1L]] <- col_summary

        # Track cross-file column presence
        if (is.null(all_columns[[cname]])) all_columns[[cname]] <- list(files = character(0), max_nd = 0L)
        all_columns[[cname]]$files <- c(all_columns[[cname]]$files, basename(fp))
        all_columns[[cname]]$max_nd <- max(all_columns[[cname]]$max_nd, col_summary$n_distinct)
      }}

      file_results[[length(file_results) + 1L]] <- list(
        file_name     = basename(fp),
        relative_path = fp,
        row_count     = n_rows,
        columns       = columns
      )
    }}

    # Shared columns: appear in 2+ files
    shared <- list()
    for (cname in names(all_columns)) {{
      info <- all_columns[[cname]]
      if (length(info$files) >= 2L) {{
        shared[[length(shared) + 1L]] <- list(
          column_name    = cname,
          files          = unique(info$files),
          max_n_distinct = info$max_nd
        )
      }}
    }}

    result <- list(
      contract_version = "{contract_version}",
      generated_at     = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
      project_paths    = as.list(PROJECT_PATHS),
      files            = file_results,
      shared_columns   = shared
    )

    jsonlite::write_json(result, OUTPUT_PATH, auto_unbox = TRUE, pretty = TRUE, na = "null")
    message(sprintf("Stats written to: %s", OUTPUT_PATH))

    # Clean up workspace so R has nothing to save
    .cleanup_rdata <- !.had_rdata && file.exists(".RData")
    rm(list = setdiff(ls(), c(".had_rdata", ".cleanup_rdata")))
    gc(verbose = FALSE)
    if (.cleanup_rdata) file.remove(".RData")
""")


def generate_script(
    project_paths: list[str],
    output_path: Path,
) -> Path:
    """Generate an R script that extracts aggregate stats from MONA data.

    Args:
        project_paths: UNC or local Windows paths to scan for CSV files.
        output_path: Where to write the R script.

    Returns:
        The path to the generated script.
    """
    if not project_paths:
        raise ValueError("At least one project path is required")

    script = R_TEMPLATE.format(
        project_paths_r=_format_r_paths(project_paths),
        freq_cap=FREQ_CAP,
        freq_ratio=FREQ_RATIO,
        numeric_id_ratio=NUMERIC_ID_RATIO,
        numeric_id_min=NUMERIC_ID_MIN,
        string_id_ratio=STRING_ID_RATIO,
        string_id_min=STRING_ID_MIN,
        suppress_k=SUPPRESS_K,
        noise_pct=NOISE_PCT,
        date_formats_r=_format_r_string_vec(DATE_FORMATS),
        date_classify_threshold=DATE_CLASSIFY_THRESHOLD,
        date_summarize_threshold=DATE_SUMMARIZE_THRESHOLD,
        file_pattern=FILE_PATTERN,
        contract_version=CONTRACT_VERSION,
        id_check=_build_r_id_check(),
        categorical_check=_build_r_categorical_check(),
    )

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(script, encoding="utf-8")
    return output_path
