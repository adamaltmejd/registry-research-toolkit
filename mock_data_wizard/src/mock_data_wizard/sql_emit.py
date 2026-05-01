"""SQL emitter for the server-side aggregation rewrite.

Emits dialect-aware aggregation SQL for two backends:

- ``duckdb``: ``file_source`` aggregations against a registered VIEW over
  ``read_csv_auto(path)``. Lets us aggregate CSVs without ever materialising
  them in R memory.
- ``mssql``:  ``sql_source`` aggregations against the existing project
  ODBC views.

The emitter is pure string construction. It never opens a connection,
never sees data. The R extract script feeds these strings to DBI and
gets back tiny result frames where k-anonymity / noise post-processing
happens.

The dialects diverge in a small number of places. Each helper isolates
one divergence; the public emitters compose them.
"""

from __future__ import annotations

DUCKDB = "duckdb"
MSSQL = "mssql"
DIALECTS = (DUCKDB, MSSQL)

NUMERIC_QUANTILES = (0.01, 0.05, 0.25, 0.50, 0.75, 0.95, 0.99)


def _check_dialect(dialect: str) -> None:
    if dialect not in DIALECTS:
        raise ValueError(f"unknown dialect: {dialect!r} (expected one of {DIALECTS})")


def quote_ident(name: str, dialect: str) -> str:
    """Quote an identifier so it survives reserved-word collisions and spaces.

    DuckDB uses double-quoted identifiers (ANSI). MS SQL uses bracket
    quoting. Both escape an embedded close-quote by doubling it.
    """
    _check_dialect(dialect)
    if dialect == DUCKDB:
        return '"' + name.replace('"', '""') + '"'
    return "[" + name.replace("]", "]]") + "]"


def stddev_fn(dialect: str) -> str:
    """Standard deviation aggregate. DuckDB: STDDEV. T-SQL: STDEV."""
    _check_dialect(dialect)
    return "STDDEV" if dialect == DUCKDB else "STDEV"


def length_fn(col_expr: str, dialect: str) -> str:
    """String length. DuckDB: length(). T-SQL: LEN()."""
    _check_dialect(dialect)
    fn = "length" if dialect == DUCKDB else "LEN"
    return f"{fn}({col_expr})"


def double_cast(col_expr: str, dialect: str) -> str:
    """Cast to double-precision float.

    DuckDB ``FLOAT`` is single precision; ``DOUBLE`` is the double type.
    T-SQL ``FLOAT`` (alias for ``FLOAT(53)``) is double by default.
    """
    _check_dialect(dialect)
    type_name = "DOUBLE" if dialect == DUCKDB else "FLOAT"
    return f"CAST({col_expr} AS {type_name})"


def limit_clause(n: int, dialect: str) -> tuple[str, str]:
    """Row limiter rendered as (prefix, suffix) added to a SELECT.

    DuckDB uses trailing ``LIMIT N``; T-SQL uses leading ``TOP N``.
    Returned as a pair so callers can stick them in the right place.
    """
    _check_dialect(dialect)
    if dialect == DUCKDB:
        return ("", f"\nLIMIT {n}")
    return (f"TOP {n} ", "")


def sample_sql(table: str, dialect: str, n: int = 1000) -> str:
    """Sample N rows from the table.

    DuckDB has a built-in ``USING SAMPLE`` clause that's cheap on huge
    tables. T-SQL uses ``TOP N`` (no random shuffle, but good enough for
    type classification on a column-pattern basis).
    """
    _check_dialect(dialect)
    if dialect == DUCKDB:
        return f"SELECT * FROM {table} USING SAMPLE {int(n)} ROWS"
    return f"SELECT TOP {int(n)} * FROM {table}"


def count_rows_sql(table: str, dialect: str) -> str:
    """Total row count. Works in both dialects."""
    _check_dialect(dialect)
    return f"SELECT COUNT(*) AS n FROM {table}"


# -- Per-type aggregation emitters -----------------------------------------
#
# Each returns a SQL string that R executes and gets back a single-row
# result frame (except categorical_freqs_sql, which returns up to
# max_groups rows). Suppression and noise injection live in R.


def _common_count_aggs(qcol: str) -> str:
    """Count + null_count + n_distinct fragment, identical across types."""
    return (
        "  COUNT(*) AS n_total,\n"
        f"  SUM(CASE WHEN {qcol} IS NULL THEN 1 ELSE 0 END) AS null_count,\n"
        f"  COUNT(DISTINCT {qcol}) AS n_distinct"
    )


def numeric_aggs_sql(table: str, col: str, dialect: str) -> str:
    """Single-row scalar aggregates for a numeric column.

    Excludes percentiles (those need a separate query for T-SQL because
    PERCENTILE_CONT is window-only there). See ``numeric_quantiles_sql``.
    """
    _check_dialect(dialect)
    qcol = quote_ident(col, dialect)
    sd = stddev_fn(dialect)
    cast = double_cast(qcol, dialect)
    return (
        "SELECT\n"
        f"{_common_count_aggs(qcol)},\n"
        f"  MIN({qcol}) AS min_v,\n"
        f"  MAX({qcol}) AS max_v,\n"
        f"  AVG({cast}) AS mean_v,\n"
        f"  {sd}({cast}) AS sd_v\n"
        f"FROM {table}"
    )


def numeric_quantiles_sql(
    table: str,
    col: str,
    dialect: str,
    quantiles: tuple[float, ...] = NUMERIC_QUANTILES,
) -> str:
    """Quantile estimates for a numeric column.

    DuckDB: PERCENTILE_CONT is a true aggregate -- combine all quantiles
    in one query (single sort).

    T-SQL: PERCENTILE_CONT is window-only (illegal as plain aggregate).
    Wrap with TOP 1 ... OVER () so all quantiles materialise once and
    R receives a one-row result. The optimiser computes percentiles on
    the same sort.
    """
    _check_dialect(dialect)
    qcol = quote_ident(col, dialect)
    cast = double_cast(qcol, dialect)
    cols = []
    for q in quantiles:
        # Match the R contract's "p01"/"p50"/"p99"... naming
        alias = f"p{int(round(q * 100)):02d}"
        if dialect == DUCKDB:
            cols.append(
                f"  PERCENTILE_CONT({q}) WITHIN GROUP (ORDER BY {cast}) AS {alias}"
            )
        else:
            cols.append(
                f"  PERCENTILE_CONT({q}) WITHIN GROUP (ORDER BY {cast}) OVER () AS {alias}"
            )
    select_body = ",\n".join(cols)
    if dialect == DUCKDB:
        return f"SELECT\n{select_body}\nFROM {table}"
    # MS SQL: TOP 1 picks any row; OVER () makes percentile constant
    return f"SELECT TOP 1\n{select_body}\nFROM {table}"


def categorical_freqs_sql(
    table: str,
    col: str,
    dialect: str,
    max_groups: int = 200,
) -> str:
    """Frequency table for a low-cardinality categorical column.

    Returns up to ``max_groups`` rows of ``(val, n)`` ordered by ``n DESC``.
    Cell suppression (k-anonymity) is applied in R after this returns.
    """
    _check_dialect(dialect)
    qcol = quote_ident(col, dialect)
    prefix, suffix = limit_clause(max_groups, dialect)
    return (
        f"SELECT {prefix}{qcol} AS val, COUNT(*) AS n\n"
        f"FROM {table}\n"
        f"GROUP BY {qcol}\n"
        f"ORDER BY n DESC{suffix}"
    )


def high_cardinality_aggs_sql(table: str, col: str, dialect: str) -> str:
    """Length-based stats for a high-cardinality string column.

    Min/max/mean of ``LENGTH(col)`` plus the usual count/null/distinct
    triple. R uses these to seed placeholder generation (val_000001).
    """
    _check_dialect(dialect)
    qcol = quote_ident(col, dialect)
    length = length_fn(qcol, dialect)
    return (
        "SELECT\n"
        f"{_common_count_aggs(qcol)},\n"
        f"  MIN({length}) AS min_length,\n"
        f"  MAX({length}) AS max_length,\n"
        f"  AVG({double_cast(length, dialect)}) AS mean_length\n"
        f"FROM {table}"
    )


def date_aggs_sql(table: str, col: str, dialect: str) -> str:
    """Min/max plus count/null/distinct for a date column.

    Date-format detection (the YYYYMMDD vs YYYY-MM-DD parser dance) stays
    R-side because formats vary across SCB registries and we don't want
    to push that logic into SQL.
    """
    _check_dialect(dialect)
    qcol = quote_ident(col, dialect)
    return (
        "SELECT\n"
        f"{_common_count_aggs(qcol)},\n"
        f"  MIN({qcol}) AS min_v,\n"
        f"  MAX({qcol}) AS max_v\n"
        f"FROM {table}"
    )


def id_aggs_sql(table: str, col: str, dialect: str) -> str:
    """Count + null_count + n_distinct for an ID-like column.

    No min/max -- IDs are opaque tokens, not ordinals worth bounding.
    """
    _check_dialect(dialect)
    qcol = quote_ident(col, dialect)
    return f"SELECT\n{_common_count_aggs(qcol)}\nFROM {table}"


# -- Dispatch --------------------------------------------------------------

# Maps inferred col_type -> ordered list of (kind, emitter) pairs.
# R loops over the list, executes each query, and merges the results
# into the per-column summary dict that lands in stats.json.
COLUMN_TYPES = ("numeric", "categorical", "high_cardinality", "date", "id")


def queries_for_column(
    table: str, col: str, col_type: str, dialect: str
) -> dict[str, str]:
    """Return the dialect-aware SQL queries needed for a typed column.

    The returned dict's keys are stable identifiers (``"aggs"``,
    ``"quantiles"``, ``"freqs"``) so the R caller can dispatch on kind
    when merging results.
    """
    _check_dialect(dialect)
    if col_type == "numeric":
        return {
            "aggs": numeric_aggs_sql(table, col, dialect),
            "quantiles": numeric_quantiles_sql(table, col, dialect),
        }
    if col_type == "categorical":
        return {"freqs": categorical_freqs_sql(table, col, dialect)}
    if col_type == "high_cardinality":
        return {"aggs": high_cardinality_aggs_sql(table, col, dialect)}
    if col_type == "date":
        return {"aggs": date_aggs_sql(table, col, dialect)}
    if col_type == "id":
        return {"aggs": id_aggs_sql(table, col, dialect)}
    raise ValueError(f"unknown col_type: {col_type!r} (expected one of {COLUMN_TYPES})")
