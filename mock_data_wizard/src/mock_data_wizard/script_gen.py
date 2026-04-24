"""Generate R script for extracting aggregate stats from MONA project data."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PureWindowsPath

from .stats import CONTRACT_VERSION


def _strip_template_indent(template: str, indent: int = 4) -> str:
    """Strip a fixed left-margin from template lines.

    textwrap.dedent can't be used here: some of our template lines are R
    string literals that embed `\\n` escapes. Python sees those embedded
    newlines as real line breaks, and the continuation "lines" inside the
    R literal have 0 or 2 leading spaces -- which breaks dedent's common-
    prefix algorithm. We just strip the template's own left margin from
    lines that have it, and leave others (R-literal continuations) alone.
    """
    pad = " " * indent
    return "\n".join(
        (line[indent:] if line.startswith(pad) else line)
        for line in template.split("\n")
    )


# -- Name-based column classification --------------------------------------
#
# Columns are classified by matching their name against known SCB patterns.
# These run before any data-driven heuristics. First match wins.
#
# Patterns are case-insensitive R regexes matched with grepl().
# Each CategoricalPattern has a max_distinct cap -- if the column has more
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
    CategoricalPattern("medb(orgarskap)?", max_distinct=300),  # citizenship ~230
]

# -- Data-driven classification thresholds ---------------------------------
#
# Applied when no name pattern matches. A column is categorical if
# n_distinct <= min(FREQ_CAP, n_rows * FREQ_RATIO). Otherwise numeric
# columns with near-unique values are classified as ID.

FREQ_CAP = 50  # absolute max distinct values to still count as categorical
FREQ_RATIO = 0.01  # relative max (fraction of n_rows)
NUMERIC_ID_RATIO = 0.95  # numeric ID if n_distinct > ratio * n_rows ...
NUMERIC_ID_MIN = 100  # ... and n_distinct > this minimum
STRING_ID_RATIO = 0.5  # string ID if n_distinct > ratio * n_rows ...
STRING_ID_MIN = 100  # ... and n_distinct > this minimum

# -- Disclosure control ----------------------------------------------------
#
# Applied when summarizing columns to prevent leaking individual-level data.
# Categorical: values with count < SUPPRESS_K are merged into "_other".
# Numeric: all aggregate stats (min, max, mean, sd, quantiles) are perturbed
# by uniform noise in [-NOISE_PCT, +NOISE_PCT] relative to the true value.

SUPPRESS_K = 5  # k-anonymity threshold for categorical frequency tables
NOISE_PCT = 0.005  # +/-0.5% relative noise on numeric aggregates

# Minimum plausible population size before we flag a source as potentially
# re-identifiable even after k-anonymity. 20*SUPPRESS_K is a rule-of-thumb --
# below that, even suppressed aggregates may correspond to identifiable
# individuals, especially when the user has narrowed sources with a filter.
SMALL_POP_MULT = 20

# -- Date detection --------------------------------------------------------
#
# A string column is classified as date if >CLASSIFY_THRESHOLD of a 200-row
# sample parses successfully. When summarizing, >SUMMARIZE_THRESHOLD must
# parse to extract min/max (lower bar since we already committed to date type).

DATE_FORMATS = ["%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y", "%Y%m%d"]
DATE_CLASSIFY_THRESHOLD = 0.8
DATE_SUMMARIZE_THRESHOLD = 0.5

# -- File scanning ---------------------------------------------------------

FILE_PATTERN = "\\\\.(csv|CSV|txt|TXT)$"


# -- R code generation helpers ---------------------------------------------


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


def _format_r_path(path: str) -> str:
    """Format a single path as an R string literal, preserving Windows backslashes."""
    win = str(PureWindowsPath(path))
    return '"' + win.replace("\\", "\\\\") + '"'


def _format_r_string_vec(items: list[str]) -> str:
    """Format a Python list of strings as an R character vector."""
    return "c(" + ", ".join(f'"{s}"' for s in items) + ")"


def _build_sources_block(
    project_paths: list[str],
    sql_dsn: str | None = None,
) -> str:
    """Render the SOURCES <- list(...) block.

    Emits one `file_source(path=...)` per given path, and -- when `sql_dsn`
    is supplied -- a `sql_source(dsn=...)` as well. Both sources start in
    discovery mode; the user re-runs the script once it's edited down to
    what they actually want. If one source has nothing to offer (e.g. the
    project has no SQL), its discovery step fails gracefully and the
    suggested block simply omits it.
    """
    entries = [f"  file_source(path = {_format_r_path(p)})" for p in project_paths]
    if sql_dsn:
        entries.append(f'  sql_source(dsn = "{sql_dsn}")')
    return "SOURCES <- list(\n" + ",\n".join(entries) + "\n)"


R_TEMPLATE = _strip_template_indent("""\
    # -- mock-data-wizard stats extractor ------------------------------
    # Generated by mock-data-wizard. Run this script on MONA to produce
    # aggregate statistics. NO individual-level data is exported.
    #
    # Output: see OUTPUT_PATH below
    # ------------------------------------------------------------------

    # Prevent .RData save on exit -- only delete if we created it
    .had_rdata <- file.exists(".RData")
    .Last <- function() {{
      if (!.had_rdata && file.exists(".RData")) file.remove(".RData")
    }}

    # Try to load a package; if missing, attempt install.packages() once
    # and retry. MONA environments don't always have every package pre-
    # installed; we'd rather bootstrap than fail on first use. Honors
    # options("mdw.pkg_repo") if set, else getOption("repos"). Guards
    # against the unconfigured-"@CRAN@" sentinel to avoid triggering an
    # interactive mirror prompt in Batch mode.
    ensure_package <- function(pkg) {{
      if (requireNamespace(pkg, quietly = TRUE)) return(invisible(TRUE))
      repos <- getOption("mdw.pkg_repo", default = getOption("repos"))
      if (is.null(repos) || length(repos) == 0L || any(repos == "@CRAN@")) {{
        stop(sprintf(
          "Package '%s' is required but no CRAN mirror is configured. Either set one with options(repos = 'https://cloud.r-project.org') (or a MONA-internal mirror) and re-run, or install the package manually: install.packages('%s').",
          pkg, pkg
        ))
      }}
      message(sprintf("Package '%s' not installed -- attempting install.packages('%s')...", pkg, pkg))
      install_ok <- tryCatch({{
        install.packages(pkg, repos = repos, quiet = TRUE)
        TRUE
      }}, error = function(e) {{
        message(sprintf("  install failed: %s", conditionMessage(e)))
        FALSE
      }})
      if (!install_ok || !requireNamespace(pkg, quietly = TRUE)) {{
        stop(sprintf(
          "Package '%s' is required but could not be installed. On MONA, request installation via SCB, or install manually: install.packages('%s').",
          pkg, pkg
        ))
      }}
      invisible(TRUE)
    }}

    ensure_package("data.table")
    ensure_package("jsonlite")

    library(data.table)
    library(jsonlite)

    # Timestamped progress log. Uses message() (stderr) so the .Rout file
    # on batch clients shows lines as they happen rather than waiting on
    # stdout buffering.
    .mdw_log <- function(fmt, ...) {{
      message(sprintf("[%s] %s", format(Sys.time(), "%H:%M:%S"), sprintf(fmt, ...)))
    }}
    # Rows/cols readable in progress lines: 125430 -> "125,430".
    .mdw_num <- function(n) formatC(n, big.mark = ",", format = "d")
    # Seconds elapsed between two Sys.time() values.
    .mdw_secs <- function(t0, t1 = Sys.time()) as.numeric(difftime(t1, t0, units = "secs"))

    # -- Source constructors (defined before SOURCES so the block below
    # can call them) --------------------------------------------------

    file_source <- function(path, include = NULL, exclude = NULL, pattern = NULL,
                            all = FALSE) {{
      if (missing(path) || !is.character(path) || length(path) != 1L || !nzchar(path))
        stop("file_source(): `path` must be a single non-empty string")
      list(
        type    = "file",
        path    = path,
        include = include,
        exclude = exclude,
        pattern = pattern,
        all     = isTRUE(all)
      )
    }}

    # sql_source() -- read from a DBI-compatible database (MONA: MS SQL
    # Server via ODBC). Requires DBI and odbc R packages. Credentials are
    # taken from a Windows system DSN -- no raw passwords in this script.
    #
    # If `tables`, `pattern`, and `queries` are all NULL, the script runs
    # in discovery mode and prints suggested tables for you to paste back.
    sql_source <- function(dsn,
                           server      = NULL,
                           database    = NULL,
                           driver      = "ODBC Driver 17 for SQL Server",
                           encoding    = "CP1252",
                           tables      = NULL,
                           pattern     = NULL,
                           queries     = NULL,
                           schema      = NULL,
                           exclude_archived = TRUE,
                           include     = NULL,
                           exclude     = NULL,
                           where       = NULL,
                           select      = NULL,
                           normalize_names = TRUE,
                           all         = FALSE) {{
      if (missing(dsn) || !is.character(dsn) || length(dsn) != 1L || !nzchar(dsn))
        stop("sql_source(): `dsn` must be a single non-empty string")
      if (!is.null(queries) && (!is.null(tables) || !is.null(pattern)))
        stop("sql_source(): use either `queries` OR `tables`/`pattern`, not both")
      list(
        type             = "sql",
        dsn              = dsn,
        server           = server,
        database         = if (is.null(database)) dsn else database,
        driver           = driver,
        encoding         = encoding,
        tables           = tables,
        pattern          = pattern,
        queries          = queries,
        schema           = schema,
        exclude_archived = isTRUE(exclude_archived),
        include          = include,
        exclude          = exclude,
        where            = where,
        select           = select,
        normalize_names  = isTRUE(normalize_names),
        all              = isTRUE(all)
      )
    }}

    # ==================================================================
    # -- USER CONFIGURATION --------------------------------------------
    # ==================================================================
    #
    # Declare your data sources below. Each source is a call to one of
    # the constructors defined just above.
    #
    # Available constructors:
    #
    #   file_source(
    #     path    = "\\\\\\\\server\\\\share\\\\data",   # directory OR single file
    #     include = NULL,   # optional character vector of filenames to keep
    #     exclude = NULL,   # optional character vector of filenames to skip
    #     pattern = NULL    # optional regex override (default: csv/txt)
    #   )
    #
    #   sql_source(
    #     dsn     = "P1405",         # Windows System DSN name (no password here)
    #     tables  = c("dbo.persons", "dbo.events"),  # optional
    #     pattern = NULL,            # regex on view names; alternative to tables
    #     queries = NULL,            # named vector of raw SELECT statements
    #     where   = NULL,            # list(table = "year >= 2020") or scalar
    #     select  = NULL             # list(table = c("col1", "col2"))
    #   )
    #
    # DISCOVERY MODE: if a source has no tables / pattern / queries (sql)
    # or no include / exclude / pattern (file), the script runs discovery
    # for ALL sources and writes a ready-to-edit SOURCES block to a
    # timestamped file named mdw_sources_<timestamp>.R next to this
    # script. Re-run: the script auto-loads that file and uses it
    # (overriding the SOURCES block below). Edit the file to narrow down
    # to what you want, or delete it to re-discover.
    #
    # WANT EVERYTHING, NO DISCOVERY DANCE? Pass `all = TRUE` to opt out
    # of discovery:
    #   file_source(path = "...", all = TRUE)   # every CSV/TXT in path
    #   sql_source(dsn = "...", all = TRUE)     # every non-archived view
    #
    # PII REMINDER: narrowing data with include/exclude/where may reduce
    # the effective population. Combined with small cells, this can
    # weaken k-anonymity. The script warns if any source has fewer than
    # {small_pop_mult} * SUPPRESS_K rows.

    {sources_block}

    # Output path for stats.json -- defaults to current working directory.
    OUTPUT_PATH <- file.path(getwd(), "stats.json")

    # If a previously-written mdw_sources_<timestamp>.R file exists in the
    # working directory, load it and let it redefine SOURCES. This closes
    # the discovery loop: the user runs discovery once, edits the written
    # file to narrow it down, and re-runs without having to paste anything
    # back into this script. When multiple files match (from multiple
    # discovery runs), the latest by filename wins. Delete the file(s) to
    # re-run discovery.
    .mdw_sources_files <- sort(list.files(
      getwd(),
      pattern = "^mdw_sources_[0-9]{{8}}_[0-9]{{6}}\\\\.R$",
      full.names = TRUE
    ))
    .mdw_loaded_sources_file <- NULL
    if (length(.mdw_sources_files) > 0L) {{
      .mdw_loaded_sources_file <- .mdw_sources_files[length(.mdw_sources_files)]
      extra <- length(.mdw_sources_files) - 1L
      msg <- sprintf("Loading SOURCES from %s", .mdw_loaded_sources_file)
      if (extra > 0L) msg <- sprintf("%s (%d older file(s) present)", msg, extra)
      message(msg)
      source(.mdw_loaded_sources_file, local = FALSE)
    }}

    # ==================================================================
    # -- LIBRARY CODE -- do not edit below this line --------------------
    # ==================================================================

    # Column classification thresholds
    FREQ_CAP       <- {freq_cap}L        # max distinct values to classify as categorical
    FREQ_RATIO     <- {freq_ratio}       # ... or this fraction of n_rows, whichever is smaller
    NUMERIC_ID_RATIO <- {numeric_id_ratio}  # numeric col is ID if n_distinct > ratio * n_rows
    NUMERIC_ID_MIN <- {numeric_id_min}L     # ... and n_distinct exceeds this minimum
    STRING_ID_RATIO <- {string_id_ratio}    # string col is ID if n_distinct > ratio * n_rows
    STRING_ID_MIN  <- {string_id_min}L      # ... and n_distinct exceeds this minimum

    # Disclosure control -- prevents leaking individual-level data
    SUPPRESS_K     <- {suppress_k}L      # merge categorical values with count < k into "_other"
    NOISE_PCT      <- {noise_pct}        # perturb numeric stats by +/- this fraction
    SMALL_POP_MULT <- {small_pop_mult}L  # warn when a source has fewer than SMALL_POP_MULT * SUPPRESS_K rows

    # Date detection
    DATE_FORMATS   <- {date_formats_r}
    DATE_CLASSIFY_THRESHOLD  <- {date_classify_threshold}  # fraction of sample that must parse as date
    DATE_SUMMARIZE_THRESHOLD <- {date_summarize_threshold}  # fraction needed to extract min/max

    # Default file scan pattern (used by file_source when pattern is NULL)
    DEFAULT_FILE_PATTERN <- "{file_pattern}"

    # -- Discovery mode ------------------------------------------------
    # A source enters discovery mode when it has no filtering info at all.
    # file_source: no include, no exclude, no pattern.
    # sql_source:  no tables, no pattern, no queries.
    # `all = TRUE` on either constructor opts OUT of discovery -- the user
    # is explicitly saying "give me everything in this source."
    # If any source needs discovery, we run discovery for ALL sources,
    # print a suggested SOURCES block, and exit -- no stats.json is written.

    needs_discovery <- function(src) {{
      if (isTRUE(src$all)) return(FALSE)
      if (src$type == "file") {{
        return(is.null(src$include) && is.null(src$exclude) && is.null(src$pattern))
      }}
      if (src$type == "sql") {{
        return(is.null(src$tables) && is.null(src$pattern) && is.null(src$queries))
      }}
      FALSE
    }}

    # -- Source dispatch ------------------------------------------------
    # source_fetch(src) returns list of list(source_name, source_type,
    # source_detail, dt) -- one per physical table/file the source produced.

    source_fetch <- function(src) {{
      type <- src$type
      if (is.null(type)) stop("Source has no `type`: check your SOURCES block")
      if (type == "file") return(source_fetch_file(src))
      if (type == "sql")  return(source_fetch_sql(src))
      stop(sprintf("Unknown source type: %s", type))
    }}

    list_files_in_source <- function(src) {{
      pp_norm <- normalizePath(src$path, mustWork = TRUE)
      info <- file.info(pp_norm)
      pattern <- if (is.null(src$pattern)) DEFAULT_FILE_PATTERN else src$pattern
      if (!is.na(info$isdir) && info$isdir) {{
        found <- list.files(pp_norm, pattern = pattern,
                            full.names = TRUE, recursive = TRUE)
      }} else {{
        found <- pp_norm  # single-file source
      }}
      unique(normalizePath(found, mustWork = FALSE))
    }}

    source_fetch_file <- function(src) {{
      found <- list_files_in_source(src)
      if (!is.null(src$include)) {{
        found <- found[basename(found) %in% src$include]
      }}
      if (!is.null(src$exclude)) {{
        found <- found[!(basename(found) %in% src$exclude)]
      }}
      # Basenames become source_name, which must be unique. Two files with
      # the same basename in different subdirectories collide -- the user
      # must narrow `path =` to reach the specific file they want.
      names_vec <- basename(found)
      dupes <- unique(names_vec[duplicated(names_vec)])
      if (length(dupes) > 0L) {{
        stop(sprintf(
          "Duplicate file basename(s) in source '%s': %s. Narrow the `path =` argument to a subdirectory to select just one.",
          src$path, paste(dupes, collapse = ", ")
        ))
      }}
      n_found <- length(found)
      lapply(seq_along(found), function(i) {{
        fp <- found[[i]]
        t0 <- Sys.time()
        dt <- tryCatch(
          data.table::fread(fp, nThread = 1L),
          error = function(e) stop(sprintf("Failed to read %s: %s", fp, conditionMessage(e)))
        )
        if (is.null(dt) || nrow(dt) == 0L) {{
          .mdw_log("  read  %d/%d %s: empty, skipped", i, n_found, basename(fp))
          return(NULL)
        }}
        .mdw_log("  read  %d/%d %s: %s rows x %d cols (%.1fs)",
                 i, n_found, basename(fp), .mdw_num(nrow(dt)), ncol(dt), .mdw_secs(t0))
        list(
          source_name   = basename(fp),
          source_type   = "file",
          source_detail = list(path = fp),
          dt            = dt
        )
      }})
    }}

    # -- SQL dispatch ---------------------------------------------------

    # Test hook -- tests may override the connection constructor via
    # options(mdw.sql_connect = function(src) DBI::dbConnect(...)).
    # Production path: DBI::dbConnect(odbc::odbc(), ...) using src fields.
    sql_connect <- function(src) {{
      override <- getOption("mdw.sql_connect")
      if (is.function(override)) return(override(src))
      ensure_package("DBI")
      ensure_package("odbc")
      args <- list(
        drv      = odbc::odbc(),
        dsn      = src$dsn,
        Driver   = src$driver,
        Server   = src$server,
        Database = src$database,
        encoding = src$encoding
      )
      args <- args[!vapply(args, is.null, logical(1))]
      do.call(DBI::dbConnect, args)
    }}

    # Fully-qualified table list -- schemas + names in "schema.table" form
    # if the dialect supports schemas (MS SQL), else bare names (SQLite).
    sql_list_tables <- function(conn, src) {{
      override <- getOption("mdw.sql_list_tables")
      if (is.function(override)) return(override(conn, src))
      tryCatch({{
        rows <- DBI::dbGetQuery(
          conn,
          "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.tables WHERE TABLE_TYPE = 'VIEW'"
        )
        rows <- as.data.frame(rows)
        if (!is.null(src$schema)) rows <- rows[rows$TABLE_SCHEMA %in% src$schema, , drop = FALSE]
        sort(unique(paste(rows$TABLE_SCHEMA, rows$TABLE_NAME, sep = ".")))
      }}, error = function(e) {{
        # Dialects without information_schema (SQLite, etc.) -- fall back.
        sort(unique(DBI::dbListTables(conn)))
      }})
    }}

    # Strip schema prefix: "dbo.persons" -> "persons".
    strip_schema <- function(qualified) {{
      parts <- strsplit(qualified, ".", fixed = TRUE)[[1L]]
      parts[length(parts)]
    }}

    # Resolve tables to named list(alias = qualified_name). Enforces unique
    # aliases -- if two schemas share a table name, the user must supply an
    # explicit alias via named vector.
    resolve_table_aliases <- function(tables) {{
      if (is.null(tables) || length(tables) == 0L) return(list())
      nm <- names(tables)
      if (is.null(nm)) nm <- rep("", length(tables))
      aliases <- ifelse(nzchar(nm), nm, vapply(tables, strip_schema, character(1)))
      dupes <- aliases[duplicated(aliases)]
      if (length(dupes) > 0L) {{
        stop(sprintf(
          "Ambiguous table aliases: %s. Supply explicit aliases via a named vector, e.g. c(persons_dbo = 'dbo.persons', persons_p1 = 'P1105.persons').",
          paste(unique(dupes), collapse = ", ")
        ))
      }}
      setNames(as.list(tables), aliases)
    }}

    # Stata-safe column munging (mirrors common MONA post-processing):
    # replace "." with "_", strip trailing "_", translit to ASCII.
    normalize_column_names <- function(dt) {{
      nm <- names(dt)
      nm <- gsub(".", "_", nm, fixed = TRUE)
      nm <- gsub("_+$", "", nm)
      nm <- iconv(nm, "latin1", "ASCII//TRANSLIT")
      data.table::setnames(dt, nm)
      dt
    }}

    sql_build_query <- function(qualified_table, src) {{
      # Column projection: select may be list(alias = c("col1", "col2")).
      cols <- NULL
      if (!is.null(src$select)) {{
        alias <- strip_schema(qualified_table)
        if (!is.null(src$select[[alias]])) cols <- src$select[[alias]]
      }}
      col_clause <- if (is.null(cols)) "*" else paste(cols, collapse = ", ")

      q <- sprintf("SELECT %s FROM %s", col_clause, qualified_table)

      # WHERE can be a scalar (applied to all) or a named list per alias.
      where_clause <- NULL
      if (!is.null(src$where)) {{
        if (is.list(src$where)) {{
          alias <- strip_schema(qualified_table)
          where_clause <- src$where[[alias]]
        }} else if (is.character(src$where) && length(src$where) == 1L) {{
          where_clause <- src$where
        }}
      }}
      if (!is.null(where_clause) && nzchar(where_clause)) {{
        q <- sprintf("%s WHERE %s", q, where_clause)
      }}
      q
    }}

    sql_run_query <- function(conn, q, encoding) {{
      res <- DBI::dbSendQuery(conn, q)
      on.exit(DBI::dbClearResult(res), add = TRUE)
      data.table::as.data.table(DBI::dbFetch(res, n = -1L))
    }}

    source_fetch_sql <- function(src) {{
      conn <- sql_connect(src)
      on.exit(DBI::dbDisconnect(conn), add = TRUE)

      # Resolve the table list (either explicit, pattern-matched, or via raw queries).
      if (!is.null(src$queries)) {{
        # Custom queries mode -- each named entry becomes one source.
        qnames <- names(src$queries)
        if (is.null(qnames) || any(!nzchar(qnames)))
          stop("sql_source(): when using `queries`, supply a NAMED character vector")
        n_q <- length(src$queries)
        items <- lapply(seq_along(src$queries), function(i) {{
          t0 <- Sys.time()
          dt <- sql_run_query(conn, src$queries[[i]], src$encoding)
          if (src$normalize_names) dt <- normalize_column_names(dt)
          if (nrow(dt) == 0L) {{
            .mdw_log("  fetch %d/%d %s: empty, skipped", i, n_q, qnames[i])
            return(NULL)
          }}
          .mdw_log("  fetch %d/%d %s: %s rows x %d cols (%.1fs)",
                   i, n_q, qnames[i], .mdw_num(nrow(dt)), ncol(dt), .mdw_secs(t0))
          list(
            source_name   = qnames[i],
            source_type   = "sql",
            source_detail = list(dsn = src$dsn, database = src$database, query = src$queries[[i]]),
            dt            = dt
          )
        }})
        return(items)
      }}

      qualified <- if (!is.null(src$tables)) {{
        src$tables
      }} else if (!is.null(src$pattern)) {{
        # Pattern mode: discover views, filter by regex.
        all_tbls <- sql_list_tables(conn, src)
        if (src$exclude_archived) {{
          all_tbls <- all_tbls[!grepl("(^|\\\\.)x_", all_tbls)]
        }}
        pat <- paste(src$pattern, collapse = "|")
        all_tbls[grepl(pat, all_tbls, ignore.case = TRUE)]
      }} else if (isTRUE(src$all)) {{
        # All-tables mode: discover views and use every one.
        all_tbls <- sql_list_tables(conn, src)
        if (src$exclude_archived) {{
          all_tbls <- all_tbls[!grepl("(^|\\\\.)x_", all_tbls)]
        }}
        all_tbls
      }} else {{
        stop("sql_source(): provide one of `tables`, `pattern`, `queries`, or `all = TRUE`.")
      }}

      if (!is.null(src$include)) {{
        qualified <- qualified[vapply(qualified, strip_schema, character(1)) %in% src$include |
                               qualified %in% src$include]
      }}
      if (!is.null(src$exclude)) {{
        qualified <- qualified[!(vapply(qualified, strip_schema, character(1)) %in% src$exclude) &
                               !(qualified %in% src$exclude)]
      }}

      resolved <- resolve_table_aliases(qualified)
      if (length(resolved) == 0L) {{
        stop(sprintf("sql_source(dsn='%s'): no tables selected after filters.", src$dsn))
      }}

      aliases <- names(resolved)
      n_tbl <- length(aliases)
      lapply(seq_along(aliases), function(i) {{
        alias <- aliases[[i]]
        qualified_table <- resolved[[alias]]
        q <- sql_build_query(qualified_table, src)
        t0 <- Sys.time()
        dt <- sql_run_query(conn, q, src$encoding)
        if (src$normalize_names) dt <- normalize_column_names(dt)
        if (nrow(dt) == 0L) {{
          .mdw_log("  fetch %d/%d %s: empty, skipped", i, n_tbl, alias)
          return(NULL)
        }}
        .mdw_log("  fetch %d/%d %s: %s rows x %d cols (%.1fs)",
                 i, n_tbl, alias, .mdw_num(nrow(dt)), ncol(dt), .mdw_secs(t0))
        list(
          source_name   = alias,
          source_type   = "sql",
          source_detail = list(
            dsn       = src$dsn,
            database  = src$database,
            table     = qualified_table,
            query     = q
          ),
          dt            = dt
        )
      }})
    }}

    # -- Discovery: list available items per source without fetching data.
    # For files we return basenames; when two basenames collide (same file
    # name in different subdirectories), we warn and keep only one entry
    # since `include = c(name)` cannot disambiguate between them. The user
    # must narrow `path =` to a subdirectory to reach the shadowed file.
    discover_source <- function(src) {{
      if (src$type == "file") {{
        found <- tryCatch(list_files_in_source(src), error = function(e) character(0))
        if (length(found) == 0L) return(character(0))
        names_vec <- basename(found)
        dup_mask <- duplicated(names_vec)
        if (any(dup_mask)) {{
          dupes <- unique(names_vec[dup_mask])
          message(sprintf(
            "WARNING: %d file basename(s) appear in more than one subdirectory of '%s': %s. Only one entry per basename is listed; narrow `path =` to a subdirectory to reach the others.",
            length(dupes), src$path, paste(dupes, collapse = ", ")
          ))
        }}
        unique(names_vec)
      }} else if (src$type == "sql") {{
        conn <- sql_connect(src)
        on.exit(DBI::dbDisconnect(conn), add = TRUE)
        tbls <- sql_list_tables(conn, src)
        if (src$exclude_archived) tbls <- tbls[!grepl("(^|\\\\.)x_", tbls)]
        tbls
      }} else {{
        character(0)
      }}
    }}

    format_discovery_suggestion <- function(src, items) {{
      if (src$type == "file") {{
        body <- paste0('    "', items, '"', collapse = ",\n")
        sprintf(
          'file_source(\n  path = "%s",\n  include = c(\n%s\n  )\n)',
          src$path, body
        )
      }} else if (src$type == "sql") {{
        body <- paste0('    "', items, '"', collapse = ",\n")
        sprintf(
          'sql_source(\n  dsn = "%s",\n  tables = c(\n%s\n  )\n)',
          src$dsn, body
        )
      }} else {{
        "# (unknown source type)"
      }}
    }}

    # -- Column classification / summarization -------------------------

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
        # Suppress rare values (k-anonymity) -- merge into _other
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

    process_table <- function(dt) {{
      n_rows <- nrow(dt)
      columns <- list()
      for (cname in names(dt)) {{
        col_type <- classify_column(dt[[cname]], n_rows, cname)
        col_summary <- summarize_column(dt[[cname]], col_type, n_rows)
        col_summary$column_name   <- cname
        col_summary$inferred_type <- col_type
        columns[[length(columns) + 1L]] <- col_summary
      }}
      columns
    }}

    # ==================================================================
    # -- Main ----------------------------------------------------------
    # ==================================================================

    if (!is.list(SOURCES) || length(SOURCES) == 0L) {{
      stop("SOURCES is empty. Add at least one file_source(...) or sql_source(...) call in the USER CONFIGURATION block.")
    }}

    # Discovery mode: if any source has no filters, run discovery for ALL
    # sources, write a ready-to-edit SOURCES block to a timestamped file
    # (mdw_sources_<ts>.R), and exit. The next run finds that file,
    # sources it, and processes -- no copy-paste into this script.
    #
    # Writing to a file (rather than cat-ing to stdout) also dodges
    # Windows CP1252 console mangling of UTF-8 filenames and saves the
    # user from scrolling through hundreds of tables in the .Rout output.
    if (any(vapply(SOURCES, needs_discovery, logical(1)))) {{
      # If the user already has a mdw_sources file loaded, it's still in
      # discovery state -- which means they edited the file but didn't
      # narrow it (or the default still has discovery-triggering sources).
      # Don't silently write a new file over their work. Tell them what
      # to fix.
      if (!is.null(.mdw_loaded_sources_file)) {{
        stop(sprintf(
          "Loaded %s but SOURCES still has a source without an explicit list (include / tables / pattern / queries). Edit that file to narrow each source, or delete it to regenerate a fresh discovery.",
          .mdw_loaded_sources_file
        ))
      }}
      timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
      suggestion_path <- file.path(getwd(), sprintf("mdw_sources_%s.R", timestamp))
      lines <- character(0)
      lines <- c(lines, "# Discovered SOURCES from mock-data-wizard.")
      lines <- c(lines, "# Edit this file to narrow each source to the items you want,")
      lines <- c(lines, "# then re-run the extract script -- it will load this file")
      lines <- c(lines, "# automatically. Delete this file to re-discover.")
      lines <- c(lines, "")
      lines <- c(lines, "SOURCES <- list(")
      total_files <- 0L
      total_tables <- 0L
      for (i in seq_along(SOURCES)) {{
        src <- SOURCES[[i]]
        items <- tryCatch(discover_source(src), error = function(e) {{
          message(sprintf("  [%s] discovery failed: %s", src$type, conditionMessage(e)))
          character(0)
        }})
        if (length(items) == 0L) {{
          lines <- c(lines, sprintf("  # %s source produced no items during discovery", src$type))
        }} else {{
          if (src$type == "file") total_files  <- total_files  + length(items)
          if (src$type == "sql")  total_tables <- total_tables + length(items)
          snippet <- format_discovery_suggestion(src, items)
          snippet_lines <- strsplit(snippet, "\n", fixed = TRUE)[[1L]]
          indented <- paste0("  ", snippet_lines)
          if (i < length(SOURCES)) indented[length(indented)] <- paste0(indented[length(indented)], ",")
          lines <- c(lines, indented)
        }}
      }}
      lines <- c(lines, ")")

      # Force UTF-8 so non-ASCII basenames survive Windows console handling.
      writeLines(enc2utf8(lines), con = suggestion_path, useBytes = TRUE)

      cat("\n")
      cat("================================================================\n")
      cat("  DISCOVERY MODE -- no data was processed, no stats.json written.\n")
      cat("================================================================\n")
      cat(sprintf("  Found: %d file(s), %d SQL table(s)\n", total_files, total_tables))
      cat(sprintf("  Written to: %s\n", suggestion_path))
      cat("\n")
      cat("  Next step: open that file, edit the SOURCES list down to only\n")
      cat("  the files/tables you actually want, save, and re-run this\n")
      cat("  extract script -- it will load the file automatically.\n")
      cat("  (Delete the file to re-discover.)\n")
      cat("================================================================\n")
      # Clean up and exit without writing stats.json
      .cleanup_rdata <- !.had_rdata && file.exists(".RData")
      if (.cleanup_rdata) file.remove(".RData")
      quit(save = "no", status = 0L)
    }}

    # Extraction start banner -- first cat() after the batch-mode code
    # echo. Everything before this point was function/config setup only.
    .mdw_t0 <- Sys.time()
    cat("\n")
    cat("================================================================\n")
    cat(sprintf("  mock-data-wizard extraction -- started %s\n",
                format(.mdw_t0, "%Y-%m-%d %H:%M:%S")))
    cat(sprintf("  %d source(s) configured\n", length(SOURCES)))
    cat("================================================================\n\n")
    flush.console()

    source_results <- list()
    all_columns    <- list()  # column_name -> list(source_names = chr, max_nd = int)

    for (src_idx in seq_along(SOURCES)) {{
      src <- SOURCES[[src_idx]]
      src_desc <- if (src$type == "file") sprintf("file path='%s'", src$path)
                  else                    sprintf("sql dsn='%s'", src$dsn)
      .mdw_log("source %d/%d: %s", src_idx, length(SOURCES), src_desc)
      t_src <- Sys.time()
      items <- source_fetch(src)
      items <- Filter(Negate(is.null), items)
      .mdw_log("  fetched %d non-empty item(s) in %.1fs",
               length(items), .mdw_secs(t_src))

      for (item_idx in seq_along(items)) {{
        item <- items[[item_idx]]
        n_rows <- nrow(item$dt)

        if (n_rows < SMALL_POP_MULT * SUPPRESS_K) {{
          message(sprintf(
            "WARNING: source '%s' has only %d rows (< %d). Aggregates may be identifiable even after k-anonymity.",
            item$source_name, n_rows, SMALL_POP_MULT * SUPPRESS_K
          ))
        }}

        t_proc <- Sys.time()
        columns <- process_table(item$dt)
        .mdw_log("  stats %d/%d %s: %s rows, %d cols (%.1fs)",
                 item_idx, length(items), item$source_name,
                 .mdw_num(n_rows), length(columns), .mdw_secs(t_proc))

        for (col_summary in columns) {{
          cname <- col_summary$column_name
          if (is.null(all_columns[[cname]])) all_columns[[cname]] <- list(source_names = character(0), max_nd = 0L)
          all_columns[[cname]]$source_names <- c(all_columns[[cname]]$source_names, item$source_name)
          all_columns[[cname]]$max_nd <- max(all_columns[[cname]]$max_nd, col_summary$n_distinct)
        }}

        source_results[[length(source_results) + 1L]] <- list(
          source_name   = item$source_name,
          source_type   = item$source_type,
          source_detail = item$source_detail,
          row_count     = n_rows,
          columns       = columns
        )
      }}
    }}

    if (length(source_results) == 0L) {{
      stop("No data sources produced any tables. Check your SOURCES block.")
    }}

    # Shared columns: appear in 2+ sources
    shared <- list()
    for (cname in names(all_columns)) {{
      info <- all_columns[[cname]]
      if (length(info$source_names) >= 2L) {{
        shared[[length(shared) + 1L]] <- list(
          column_name    = cname,
          sources        = unique(info$source_names),
          max_n_distinct = info$max_nd
        )
      }}
    }}

    result <- list(
      contract_version = "{contract_version}",
      generated_at     = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
      sources          = source_results,
      shared_columns   = shared
    )

    jsonlite::write_json(result, OUTPUT_PATH, auto_unbox = TRUE, pretty = TRUE, na = "null")
    .mdw_log("stats.json written: %s", OUTPUT_PATH)

    cat("\n")
    cat("================================================================\n")
    cat(sprintf("  mock-data-wizard extraction -- done in %.1fs\n",
                .mdw_secs(.mdw_t0)))
    cat(sprintf("  %d source(s), %d table(s), %d shared column(s)\n",
                length(SOURCES), length(source_results), length(shared)))
    cat("================================================================\n")
    flush.console()

    # Clean up workspace so R has nothing to save
    .cleanup_rdata <- !.had_rdata && file.exists(".RData")
    rm(list = setdiff(ls(), c(".had_rdata", ".cleanup_rdata")))
    gc(verbose = FALSE)
    if (.cleanup_rdata) file.remove(".RData")
""")


def generate_script(
    project_paths: list[str],
    output_path: Path,
    *,
    sql_dsn: str | None = None,
) -> Path:
    """Generate an R script that extracts aggregate stats from MONA data.

    Args:
        project_paths: UNC or local Windows paths to scan for CSV files.
            Each path becomes one `file_source(...)` entry in the generated
            SOURCES block.
        output_path: Where to write the R script.
        sql_dsn: If set, also emit a `sql_source(dsn=...)` skeleton in
            SOURCES. On MONA, DSNs are named after the project number, so
            passing e.g. "P1405" covers the common case. On first run both
            sources attempt discovery; whichever doesn't apply to the
            project fails gracefully and is omitted from the suggestion.

    Returns:
        The path to the generated script.
    """
    if not project_paths:
        raise ValueError("At least one project path is required")

    script = R_TEMPLATE.format(
        sources_block=_build_sources_block(project_paths, sql_dsn=sql_dsn),
        freq_cap=FREQ_CAP,
        freq_ratio=FREQ_RATIO,
        numeric_id_ratio=NUMERIC_ID_RATIO,
        numeric_id_min=NUMERIC_ID_MIN,
        string_id_ratio=STRING_ID_RATIO,
        string_id_min=STRING_ID_MIN,
        suppress_k=SUPPRESS_K,
        noise_pct=NOISE_PCT,
        small_pop_mult=SMALL_POP_MULT,
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
