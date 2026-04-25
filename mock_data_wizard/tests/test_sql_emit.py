"""Tests for the dialect-aware SQL emitter.

DuckDB tests run the emitted SQL against an in-process DuckDB engine,
which proves the strings actually parse and produce the expected result
schema. T-SQL tests verify string shape only (we don't have a MS SQL
running locally) -- correctness is delegated to the MONA probe runs.
"""

from __future__ import annotations

import duckdb
import pytest

from mock_data_wizard.sql_emit import (
    DUCKDB,
    MSSQL,
    NUMERIC_QUANTILES,
    categorical_freqs_sql,
    count_rows_sql,
    date_aggs_sql,
    double_cast,
    high_cardinality_aggs_sql,
    id_aggs_sql,
    length_fn,
    limit_clause,
    numeric_aggs_sql,
    numeric_quantiles_sql,
    queries_for_column,
    quote_ident,
    sample_sql,
    stddev_fn,
)


# -- Dialect adapters ------------------------------------------------------


def test_quote_ident_duckdb_uses_double_quotes():
    assert quote_ident("col", DUCKDB) == '"col"'


def test_quote_ident_mssql_uses_brackets():
    assert quote_ident("col", MSSQL) == "[col]"


def test_quote_ident_escapes_embedded_quote():
    # DuckDB doubles "; T-SQL doubles ]
    assert quote_ident('weird"name', DUCKDB) == '"weird""name"'
    assert quote_ident("weird]name", MSSQL) == "[weird]]name]"


def test_quote_ident_rejects_unknown_dialect():
    with pytest.raises(ValueError, match="unknown dialect"):
        quote_ident("x", "postgres")


def test_stddev_fn_dialects():
    assert stddev_fn(DUCKDB) == "STDDEV"
    assert stddev_fn(MSSQL) == "STDEV"


def test_length_fn_dialects():
    assert length_fn('"x"', DUCKDB) == 'length("x")'
    assert length_fn("[x]", MSSQL) == "LEN([x])"


def test_double_cast_uses_native_double_type():
    # DuckDB FLOAT is single-precision; we want DOUBLE.
    assert double_cast('"x"', DUCKDB) == 'CAST("x" AS DOUBLE)'
    # T-SQL FLOAT is FLOAT(53) = double precision by default.
    assert double_cast("[x]", MSSQL) == "CAST([x] AS FLOAT)"


def test_limit_clause_duckdb_is_trailing():
    prefix, suffix = limit_clause(50, DUCKDB)
    assert prefix == ""
    assert "LIMIT 50" in suffix


def test_limit_clause_mssql_is_leading():
    prefix, suffix = limit_clause(50, MSSQL)
    assert prefix.startswith("TOP 50")
    assert suffix == ""


# -- sample/count emitters: run against real DuckDB ------------------------


@pytest.fixture
def duck_with_table():
    """DuckDB connection with a 1000-row toy table covering all column kinds."""
    con = duckdb.connect(":memory:")
    con.execute(
        """
        CREATE TABLE t AS
        SELECT
          range AS num,
          (range % 5) AS cat,
          repeat('x', (range % 20) + 1) AS hi,
          DATE '2020-01-01' + INTERVAL (range % 365) DAY AS d,
          'ID' || lpad(range::VARCHAR, 6, '0') AS id_col
        FROM range(0, 1000)
        """
    )
    yield con
    con.close()


def test_sample_sql_duckdb_runs(duck_with_table):
    rows = duck_with_table.execute(sample_sql("t", DUCKDB, n=100)).fetchall()
    assert len(rows) == 100


def test_sample_sql_mssql_string_shape():
    sql = sample_sql("dbo.bigtable", MSSQL, n=1000)
    assert "TOP 1000" in sql
    assert "USING SAMPLE" not in sql


def test_count_rows_sql_runs(duck_with_table):
    r = duck_with_table.execute(count_rows_sql("t", DUCKDB)).fetchone()
    assert r == (1000,)


# -- numeric: aggs + quantiles ---------------------------------------------


def test_numeric_aggs_returns_expected_columns(duck_with_table):
    sql = numeric_aggs_sql("t", "num", DUCKDB)
    cur = duck_with_table.execute(sql)
    cols = [d[0] for d in cur.description]
    assert cols == [
        "n_total",
        "null_count",
        "n_distinct",
        "min_v",
        "max_v",
        "mean_v",
        "sd_v",
    ]
    row = cur.fetchone()
    assert row[0] == 1000  # n_total
    assert row[1] == 0  # null_count
    assert row[2] == 1000  # n_distinct
    assert row[3] == 0  # min
    assert row[4] == 999  # max
    assert abs(row[5] - 499.5) < 1e-6  # mean
    assert row[6] is not None  # sd


def test_numeric_aggs_handles_nulls(duck_with_table):
    duck_with_table.execute(
        "CREATE TABLE n AS SELECT * FROM (VALUES (1), (NULL), (3), (NULL), (5)) AS v(x)"
    )
    sql = numeric_aggs_sql("n", "x", DUCKDB)
    r = duck_with_table.execute(sql).fetchone()
    assert r[0] == 5  # n_total
    assert r[1] == 2  # null_count
    assert r[2] == 3  # n_distinct
    assert r[3] == 1
    assert r[4] == 5


def test_numeric_quantiles_runs_and_returns_all_p_columns(duck_with_table):
    sql = numeric_quantiles_sql("t", "num", DUCKDB)
    cur = duck_with_table.execute(sql)
    cols = [d[0] for d in cur.description]
    assert cols == [f"p{int(round(q * 100)):02d}" for q in NUMERIC_QUANTILES]
    row = cur.fetchone()
    assert abs(row[cols.index("p50")] - 499.5) < 1.0


def test_numeric_quantiles_mssql_uses_window_form():
    sql = numeric_quantiles_sql("dbo.t", "num", MSSQL)
    # Must wrap percentiles in OVER () (window) and use TOP 1 to dedupe rows
    assert "OVER ()" in sql
    assert sql.startswith("SELECT TOP 1")
    # Quote identifier with brackets
    assert "[num]" in sql
    # Cast to FLOAT, not DOUBLE
    assert "CAST([num] AS FLOAT)" in sql


def test_numeric_aggs_mssql_uses_stdev_and_brackets():
    sql = numeric_aggs_sql("dbo.t", "num", MSSQL)
    assert "STDEV" in sql
    assert "STDDEV" not in sql
    assert "[num]" in sql


# -- categorical -----------------------------------------------------------


def test_categorical_freqs_returns_value_count_pairs(duck_with_table):
    sql = categorical_freqs_sql("t", "cat", DUCKDB)
    rows = duck_with_table.execute(sql).fetchall()
    assert len(rows) == 5  # cat = range % 5
    counts = [n for _, n in rows]
    assert all(n == 200 for n in counts)
    # ordered by n DESC -- all equal, so just check we got the count column
    cols = [d[0] for d in duck_with_table.execute(sql).description]
    assert cols == ["val", "n"]


def test_categorical_freqs_respects_max_groups(duck_with_table):
    sql = categorical_freqs_sql("t", "num", DUCKDB, max_groups=10)
    rows = duck_with_table.execute(sql).fetchall()
    assert len(rows) == 10


def test_categorical_freqs_mssql_uses_top_prefix():
    sql = categorical_freqs_sql("dbo.t", "cat", MSSQL, max_groups=50)
    assert "TOP 50" in sql
    assert "LIMIT" not in sql
    assert "[cat]" in sql


# -- high cardinality ------------------------------------------------------


def test_high_cardinality_returns_length_stats(duck_with_table):
    sql = high_cardinality_aggs_sql("t", "hi", DUCKDB)
    cur = duck_with_table.execute(sql)
    cols = [d[0] for d in cur.description]
    assert cols == [
        "n_total",
        "null_count",
        "n_distinct",
        "min_length",
        "max_length",
        "mean_length",
    ]
    row = cur.fetchone()
    assert row[3] == 1  # min length
    assert row[4] == 20  # max length


def test_high_cardinality_mssql_uses_LEN():
    sql = high_cardinality_aggs_sql("dbo.t", "hi", MSSQL)
    assert "LEN([hi])" in sql
    assert "length(" not in sql


# -- date ------------------------------------------------------------------


def test_date_aggs_returns_min_max(duck_with_table):
    sql = date_aggs_sql("t", "d", DUCKDB)
    cur = duck_with_table.execute(sql)
    cols = [d[0] for d in cur.description]
    assert cols == ["n_total", "null_count", "n_distinct", "min_v", "max_v"]
    row = cur.fetchone()
    # min returns python datetime/date; compare the ISO date prefix
    assert str(row[3]).startswith("2020-01-01")


# -- id --------------------------------------------------------------------


def test_id_aggs_no_min_max(duck_with_table):
    sql = id_aggs_sql("t", "id_col", DUCKDB)
    cur = duck_with_table.execute(sql)
    cols = [d[0] for d in cur.description]
    assert cols == ["n_total", "null_count", "n_distinct"]
    assert "min_v" not in cols
    row = cur.fetchone()
    assert row[2] == 1000


# -- dispatch --------------------------------------------------------------


def test_queries_for_column_numeric_returns_aggs_and_quantiles():
    out = queries_for_column("t", "num", "numeric", DUCKDB)
    assert set(out.keys()) == {"aggs", "quantiles"}
    assert "PERCENTILE_CONT" in out["quantiles"]
    assert "STDDEV" in out["aggs"]


def test_queries_for_column_categorical_returns_freqs_only():
    out = queries_for_column("t", "cat", "categorical", DUCKDB)
    assert set(out.keys()) == {"freqs"}


def test_queries_for_column_id_returns_aggs_only():
    out = queries_for_column("t", "id_col", "id", DUCKDB)
    assert set(out.keys()) == {"aggs"}
    assert "min_v" not in out["aggs"]


def test_queries_for_column_unknown_type_raises():
    with pytest.raises(ValueError, match="unknown col_type"):
        queries_for_column("t", "x", "geometry", DUCKDB)


def test_queries_for_column_unknown_dialect_raises():
    with pytest.raises(ValueError, match="unknown dialect"):
        queries_for_column("t", "x", "numeric", "redshift")


# -- end-to-end on a CSV via read_csv_auto ---------------------------------
#
# The actual file_source path: register a view over read_csv_auto and run
# the same emitters. Confirms there's no quoting issue specific to the
# view-over-CSV pattern.


def test_emitters_work_against_read_csv_auto(tmp_path):
    csv = tmp_path / "data.csv"
    csv.write_text("id,age,grp\nA,21,x\nB,33,y\nC,44,x\nD,55,y\nE,21,z\n")
    con = duckdb.connect(":memory:")
    try:
        con.execute(f"CREATE VIEW v AS SELECT * FROM read_csv_auto('{csv.as_posix()}')")
        n = con.execute(count_rows_sql("v", DUCKDB)).fetchone()[0]
        assert n == 5

        agg = con.execute(numeric_aggs_sql("v", "age", DUCKDB)).fetchone()
        assert agg[3] == 21  # min age
        assert agg[4] == 55  # max age

        freqs = con.execute(categorical_freqs_sql("v", "grp", DUCKDB)).fetchall()
        # 3 distinct groups
        assert len(freqs) == 3
    finally:
        con.close()
