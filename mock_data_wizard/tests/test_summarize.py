"""Tests for summarize.summarize_column.

Each test wires a tiny in-process DuckDB table, calls summarize_column
with a deterministic RNG, and asserts the resulting per-column dict has
the right shape and values (within +/- NOISE_PCT for perturbed numerics).
"""

from __future__ import annotations

import random
from datetime import date

import duckdb
import pytest

from mock_data_wizard.summarize import (
    NOISE_PCT,
    OTHER_LABEL,
    SUPPRESS_K,
    _perturb,
    _suppress_below_k,
    _to_iso,
    small_pop_threshold,
    summarize_column,
)


@pytest.fixture
def conn():
    c = duckdb.connect()
    yield c
    c.close()


def _within(actual: float, expected: float, pct: float = NOISE_PCT) -> bool:
    if expected == 0:
        return abs(actual) <= pct
    return abs(actual - expected) <= abs(expected) * pct + 1e-9


# -- helpers --------------------------------------------------------------


def test_perturb_within_noise_band():
    rng = random.Random(0)
    for _ in range(50):
        out = _perturb(100.0, rng)
        assert _within(out, 100.0)


def test_perturb_int_returns_int():
    out = _perturb(42, random.Random(0), is_int=True)
    assert isinstance(out, int)
    assert _within(out, 42)


def test_perturb_none_passthrough():
    assert _perturb(None, random.Random(0)) is None


def test_to_iso_handles_date_datetime_and_strings():
    assert _to_iso(date(2020, 1, 5)) == "2020-01-05"
    assert _to_iso("2020-01-05 00:00:00") == "2020-01-05"
    assert _to_iso("20200105") == "2020-01-05"
    assert _to_iso(None) is None


def test_suppress_below_k_drops_null_and_folds_small():
    rows = [
        {"val": "A", "n": 100},
        {"val": "B", "n": 50},
        {"val": None, "n": 9},  # null group: drop
        {"val": "C", "n": 4},  # below k: fold
        {"val": "D", "n": 2},  # below k: fold
    ]
    out = _suppress_below_k(rows)
    assert out == {"A": 100, "B": 50, OTHER_LABEL: 6}


def test_suppress_below_k_no_other_when_all_pass():
    rows = [{"val": "A", "n": 10}, {"val": "B", "n": 8}]
    out = _suppress_below_k(rows)
    assert out == {"A": 10, "B": 8}
    assert OTHER_LABEL not in out


def test_small_pop_threshold():
    assert small_pop_threshold() == 100


# -- numeric --------------------------------------------------------------


def test_summarize_numeric_double(conn):
    conn.execute("CREATE TABLE t(x DOUBLE)")
    vals = [1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5]
    conn.executemany("INSERT INTO t VALUES (?)", [(v,) for v in vals])
    out = summarize_column(
        conn,
        table="t",
        col_name="x",
        col_type="numeric",
        n_rows=10,
        n_distinct=10,
        null_count=0,
        sample=vals,
        dialect="duckdb",
        rng=random.Random(0),
    )
    assert out["column_name"] == "x"
    assert out["inferred_type"] == "numeric"
    assert out["nullable"] is False
    assert out["null_count"] == 0
    assert out["null_rate"] == 0.0
    assert out["n_distinct"] == 10
    s = out["stats"]
    assert s["numeric_subtype"] == "double"
    assert _within(s["min"], 1.5)
    assert _within(s["max"], 10.5)
    assert _within(s["mean"], 6.0)
    assert s["sd"] is not None
    assert set(s["quantiles"]) == {"p01", "p05", "p25", "p50", "p75", "p95", "p99"}
    assert _within(s["quantiles"]["p50"], 6.0)


def test_summarize_numeric_integer_subtype(conn):
    conn.execute("CREATE TABLE t(x BIGINT)")
    vals = list(range(1, 21))
    conn.executemany("INSERT INTO t VALUES (?)", [(v,) for v in vals])
    out = summarize_column(
        conn,
        table="t",
        col_name="x",
        col_type="numeric",
        n_rows=20,
        n_distinct=20,
        null_count=0,
        sample=vals,
        dialect="duckdb",
        rng=random.Random(1),
    )
    s = out["stats"]
    assert s["numeric_subtype"] == "integer"
    assert isinstance(s["min"], int)
    assert isinstance(s["max"], int)
    assert isinstance(s["quantiles"]["p50"], int)


# -- categorical ----------------------------------------------------------


def test_summarize_categorical_applies_k_anonymity(conn):
    conn.execute("CREATE TABLE t(x VARCHAR)")
    rows = [("A",)] * 100 + [("B",)] * 30 + [("C",)] * 4 + [("D",)] * 2
    conn.executemany("INSERT INTO t VALUES (?)", rows)
    out = summarize_column(
        conn,
        table="t",
        col_name="x",
        col_type="categorical",
        n_rows=len(rows),
        n_distinct=4,
        null_count=0,
        sample=["A", "B", "C", "D"],
        dialect="duckdb",
    )
    s = out["stats"]
    assert s["frequencies"] == {"A": 100, "B": 30, OTHER_LABEL: 6}
    assert s["suppressed_below_k"] == SUPPRESS_K


def test_summarize_categorical_no_other_when_all_above_k(conn):
    conn.execute("CREATE TABLE t(x VARCHAR)")
    rows = [("A",)] * 50 + [("B",)] * 20 + [("C",)] * 10
    conn.executemany("INSERT INTO t VALUES (?)", rows)
    out = summarize_column(
        conn,
        table="t",
        col_name="x",
        col_type="categorical",
        n_rows=len(rows),
        n_distinct=3,
        null_count=0,
        sample=["A", "B", "C"],
        dialect="duckdb",
    )
    assert out["stats"]["frequencies"] == {"A": 50, "B": 20, "C": 10}
    assert OTHER_LABEL not in out["stats"]["frequencies"]


# -- high cardinality -----------------------------------------------------


def test_summarize_high_cardinality_lengths(conn):
    conn.execute("CREATE TABLE t(x VARCHAR)")
    rows = [("a",), ("bb",), ("ccc",), ("dddd",), ("eeeee",)]
    conn.executemany("INSERT INTO t VALUES (?)", rows)
    out = summarize_column(
        conn,
        table="t",
        col_name="x",
        col_type="high_cardinality",
        n_rows=5,
        n_distinct=5,
        null_count=0,
        sample=["a", "bb", "ccc", "dddd", "eeeee"],
        dialect="duckdb",
    )
    s = out["stats"]
    assert s["min_length"] == 1
    assert s["max_length"] == 5
    assert s["mean_length"] == 3.0


# -- date -----------------------------------------------------------------


def test_summarize_date_min_max_iso(conn):
    conn.execute("CREATE TABLE t(d DATE)")
    rows = [
        ("2020-01-01",),
        ("2020-06-15",),
        ("2021-03-10",),
        ("2022-12-31",),
    ]
    conn.executemany("INSERT INTO t VALUES (?)", rows)
    out = summarize_column(
        conn,
        table="t",
        col_name="d",
        col_type="date",
        n_rows=4,
        n_distinct=4,
        null_count=0,
        sample=[date(2020, 1, 1), date(2020, 6, 15)],
        dialect="duckdb",
    )
    assert out["stats"]["min"] == "2020-01-01"
    assert out["stats"]["max"] == "2022-12-31"


def test_summarize_date_records_format_when_string_sample(conn):
    conn.execute("CREATE TABLE t(d DATE)")
    conn.executemany("INSERT INTO t VALUES (?)", [("2020-01-01",), ("2020-06-15",)])
    out = summarize_column(
        conn,
        table="t",
        col_name="d",
        col_type="date",
        n_rows=2,
        n_distinct=2,
        null_count=0,
        sample=["2020-01-01", "2020-06-15"] * 50,
        dialect="duckdb",
    )
    assert out["stats"].get("date_format") == "%Y-%m-%d"


# -- id -------------------------------------------------------------------


def test_summarize_id_integer(conn):
    conn.execute("CREATE TABLE t(id BIGINT)")
    out = summarize_column(
        conn,
        table="t",
        col_name="id",
        col_type="id",
        n_rows=1000,
        n_distinct=1000,
        null_count=0,
        sample=[1, 2, 3],
        dialect="duckdb",
    )
    assert out["stats"]["id_subtype"] == "integer"


def test_summarize_id_string(conn):
    conn.execute("CREATE TABLE t(id VARCHAR)")
    out = summarize_column(
        conn,
        table="t",
        col_name="id",
        col_type="id",
        n_rows=1000,
        n_distinct=1000,
        null_count=0,
        sample=["A1", "B2", "C3"],
        dialect="duckdb",
    )
    assert out["stats"]["id_subtype"] == "string"


# -- nulls / shape --------------------------------------------------------


def test_summarize_records_null_count_and_rate(conn):
    conn.execute("CREATE TABLE t(x BIGINT)")
    conn.executemany("INSERT INTO t VALUES (?)", [(i,) for i in range(80)])
    out = summarize_column(
        conn,
        table="t",
        col_name="x",
        col_type="numeric",
        n_rows=100,
        n_distinct=80,
        null_count=20,
        sample=list(range(80)),
        dialect="duckdb",
        rng=random.Random(0),
    )
    assert out["nullable"] is True
    assert out["null_count"] == 20
    assert out["null_rate"] == 0.2


def test_summarize_unknown_col_type_raises(conn):
    with pytest.raises(ValueError, match="unknown col_type"):
        summarize_column(
            conn,
            table="t",
            col_name="x",
            col_type="bogus",
            n_rows=10,
            n_distinct=10,
            null_count=0,
            sample=[],
            dialect="duckdb",
        )
