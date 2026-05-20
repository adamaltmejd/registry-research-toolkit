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

from mock_data_wizard.spec import ColumnTypeOverride
from mock_data_wizard.summarize import (
    NOISE_PCT,
    OTHER_LABEL,
    SUPPRESS_K,
    _perturb,
    _suppress_below_k,
    _to_date,
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


def test_to_date_handles_date_datetime_and_strings():
    assert _to_date(date(2020, 1, 5)) == date(2020, 1, 5)
    assert _to_date("2020-01-05 00:00:00") == date(2020, 1, 5)
    assert _to_date("20200105") == date(2020, 1, 5)
    assert _to_date(None) is None


def test_to_date_uses_override_format():
    assert _to_date("2020.01.05", "%Y.%m.%d") == date(2020, 1, 5)


def test_to_date_returns_none_on_unparseable():
    assert _to_date("not-a-date") is None
    assert _to_date(42) is None


def test_suppress_below_k_drops_null_and_folds_small():
    # k=10: "C" (n=4), "D" (n=2) fold into _other = 6 < k -> dropped entirely.
    rows = [
        {"val": "A", "n": 100},
        {"val": "B", "n": 50},
        {"val": None, "n": 9},  # null group: drop
        {"val": "C", "n": 4},  # below k: fold
        {"val": "D", "n": 2},  # below k: fold
    ]
    out = _suppress_below_k(rows)
    assert out == {"A": 100, "B": 50}
    assert OTHER_LABEL not in out


def test_suppress_below_k_emits_other_when_bucket_passes():
    # _other = 6+5 = 11 >= k -> emitted as-is.
    rows = [
        {"val": "A", "n": 100},
        {"val": "B", "n": 50},
        {"val": "C", "n": 6},
        {"val": "D", "n": 5},
    ]
    out = _suppress_below_k(rows)
    assert out == {"A": 100, "B": 50, OTHER_LABEL: 11}


def test_suppress_below_k_no_other_when_all_pass():
    rows = [{"val": "A", "n": 50}, {"val": "B", "n": 30}]
    out = _suppress_below_k(rows)
    assert out == {"A": 50, "B": 30}
    assert OTHER_LABEL not in out


def test_suppress_below_k_drops_other_entirely_when_total_below_k():
    rows = [{"val": "A", "n": 100}, {"val": "B", "n": 1}, {"val": "C", "n": 1}]
    out = _suppress_below_k(rows)
    assert out == {"A": 100}
    assert OTHER_LABEL not in out


def test_suppress_below_k_honors_per_call_k_override():
    rows = [{"val": "A", "n": 100}, {"val": "B", "n": 30}, {"val": "C", "n": 8}]
    # Default k=10 -> C folds into _other=8, which is < k, so dropped.
    assert _suppress_below_k(rows) == {"A": 100, "B": 30}
    # Per-call k=5 -> C passes outright.
    assert _suppress_below_k(rows, suppress_k=5) == {"A": 100, "B": 30, "C": 8}


def test_small_pop_threshold():
    # SMALL_POP_MULT (20) * SUPPRESS_K (10) -> 200
    assert small_pop_threshold() == 200


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


def test_summarize_numeric_min_le_max_on_narrow_range(conn):
    """Independent +/- relative noise on min and max can swap them when
    min == max (or near it); the perturbed pair must be sorted."""
    conn.execute("CREATE TABLE t(x DOUBLE)")
    conn.execute("INSERT INTO t VALUES (42.0)")
    for seed in range(50):
        out = summarize_column(
            conn,
            table="t",
            col_name="x",
            col_type="numeric",
            n_rows=1,
            n_distinct=1,
            null_count=0,
            sample=[42.0],
            dialect="duckdb",
            rng=random.Random(seed),
        )
        s = out["stats"]
        assert s["min"] <= s["max"], f"seed {seed}: {s['min']} > {s['max']}"


def test_summarize_numeric_quantiles_are_monotonic(conn):
    """Independent +/- relative noise on adjacent quantiles can reorder
    them on tightly-clustered values; the output must remain
    non-decreasing."""
    conn.execute("CREATE TABLE t(x DOUBLE)")
    # All identical -> jitter alone drives any spread between quantiles.
    conn.executemany("INSERT INTO t VALUES (?)", [(1.0,)] * 50)
    for seed in range(20):
        out = summarize_column(
            conn,
            table="t",
            col_name="x",
            col_type="numeric",
            n_rows=50,
            n_distinct=1,
            null_count=0,
            sample=[1.0] * 50,
            dialect="duckdb",
            rng=random.Random(seed),
        )
        qs = out["stats"]["quantiles"]
        values = [qs[k] for k in ("p01", "p05", "p25", "p50", "p75", "p95", "p99")]
        assert values == sorted(values), f"seed {seed}: {values}"


# -- categorical ----------------------------------------------------------


def test_summarize_categorical_applies_k_anonymity(conn):
    conn.execute("CREATE TABLE t(x VARCHAR)")
    # _other = 4+2 = 6 < k=10 -> dropped entirely.
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
    assert s["frequencies"] == {"A": 100, "B": 30}
    assert OTHER_LABEL not in s["frequencies"]
    assert s["suppressed_below_k"] == SUPPRESS_K


def test_summarize_categorical_per_column_suppress_k_override(conn):
    """A higher per-column k is honored via options."""
    conn.execute("CREATE TABLE t(x VARCHAR)")
    rows = [("A",)] * 100 + [("B",)] * 15
    conn.executemany("INSERT INTO t VALUES (?)", rows)
    out = summarize_column(
        conn,
        table="t",
        col_name="x",
        col_type="categorical",
        n_rows=len(rows),
        n_distinct=2,
        null_count=0,
        sample=["A", "B"],
        dialect="duckdb",
        options={"suppress_k": 20},
    )
    s = out["stats"]
    assert s["frequencies"] == {"A": 100}
    assert s["suppressed_below_k"] == 20


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


# -- opaque ---------------------------------------------------------------


def test_summarize_opaque_lengths(conn):
    conn.execute("CREATE TABLE t(x VARCHAR)")
    rows = [("a",), ("bb",), ("ccc",), ("dddd",), ("eeeee",)]
    conn.executemany("INSERT INTO t VALUES (?)", rows)
    out = summarize_column(
        conn,
        table="t",
        col_name="x",
        col_type="opaque",
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


def test_summarize_date_min_max_iso_within_jitter_band(conn):
    from datetime import date as _date

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
        rng=random.Random(0),
    )
    # +/- 7 day jitter applied.
    min_d = _date.fromisoformat(out["stats"]["min"])
    max_d = _date.fromisoformat(out["stats"]["max"])
    assert abs((min_d - _date(2020, 1, 1)).days) <= 7
    assert abs((max_d - _date(2022, 12, 31)).days) <= 7
    # Re-running with same seed reproduces the same jitter.
    out2 = summarize_column(
        conn,
        table="t",
        col_name="d",
        col_type="date",
        n_rows=4,
        n_distinct=4,
        null_count=0,
        sample=[date(2020, 1, 1), date(2020, 6, 15)],
        dialect="duckdb",
        rng=random.Random(0),
    )
    assert out["stats"]["min"] == out2["stats"]["min"]
    assert out["stats"]["max"] == out2["stats"]["max"]


def test_summarize_date_min_le_max_on_narrow_range(conn):
    """Independent +/-7d jitter on same-day min and max could swap them;
    we sort the pair so the bound invariant holds."""
    conn.execute("CREATE TABLE t(d DATE)")
    conn.execute("INSERT INTO t VALUES ('2020-06-15')")
    for seed in range(20):
        out = summarize_column(
            conn,
            table="t",
            col_name="d",
            col_type="date",
            n_rows=1,
            n_distinct=1,
            null_count=0,
            sample=[date(2020, 6, 15)],
            dialect="duckdb",
            rng=random.Random(seed),
        )
        min_d = date.fromisoformat(out["stats"]["min"])
        max_d = date.fromisoformat(out["stats"]["max"])
        assert min_d <= max_d


def test_summarize_date_quantiles_are_monotonic(conn):
    """Independent jitter on per-quantile values can reorder them; the
    output must remain non-decreasing."""
    conn.execute("CREATE TABLE t(d DATE)")
    rows = [("2020-06-15",)] * 50  # all identical -> jitter alone drives spread
    conn.executemany("INSERT INTO t VALUES (?)", rows)
    out = summarize_column(
        conn,
        table="t",
        col_name="d",
        col_type="date",
        n_rows=50,
        n_distinct=1,
        null_count=0,
        sample=[r[0] for r in rows],
        dialect="duckdb",
        rng=random.Random(0),
    )
    qs = out["stats"]["quantiles"]
    values = [
        date.fromisoformat(qs[k])
        for k in ("p01", "p05", "p25", "p50", "p75", "p95", "p99")
    ]
    assert values == sorted(values)


def test_summarize_date_emits_python_quantiles_when_format_known(conn):
    conn.execute("CREATE TABLE t(d DATE)")
    rows = [(f"2020-{m:02d}-15",) for m in range(1, 13)]
    conn.executemany("INSERT INTO t VALUES (?)", rows)
    sample = [r[0] for r in rows] * 4  # 48 strings
    out = summarize_column(
        conn,
        table="t",
        col_name="d",
        col_type="date",
        n_rows=12,
        n_distinct=12,
        null_count=0,
        sample=sample,
        dialect="duckdb",
        rng=random.Random(0),
    )
    s = out["stats"]
    assert s.get("date_format") == "%Y-%m-%d"
    qs = s.get("quantiles")
    assert qs is not None
    assert set(qs) == {"p01", "p05", "p25", "p50", "p75", "p95", "p99"}
    for v in qs.values():
        d = date.fromisoformat(v)
        assert 2019 <= d.year <= 2021


def test_summarize_date_skips_quantiles_when_no_sample(conn):
    """Inline-override path leaves sample empty -> no quantiles emitted."""
    conn.execute("CREATE TABLE t(d DATE)")
    conn.executemany("INSERT INTO t VALUES (?)", [("2020-01-01",), ("2022-12-31",)])
    out = summarize_column(
        conn,
        table="t",
        col_name="d",
        col_type="date",
        n_rows=2,
        n_distinct=2,
        null_count=0,
        sample=[],
        override=ColumnTypeOverride(type="date", date_format="%Y-%m-%d"),
        dialect="duckdb",
        rng=random.Random(0),
    )
    assert out["stats"]["date_format"] == "%Y-%m-%d"
    assert "quantiles" not in out["stats"]


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


def test_summarize_id_integer_from_string_sample(conn):
    """The all_varchar=true CSV path (issue #40) hands the id branch a
    string sample even for genuine integer ids. An all-int-parseable
    string sample must still classify as ``integer`` so the generated
    mock data matches the LOPNR/PERSONNR pattern.
    """
    conn.execute("CREATE TABLE t(id VARCHAR)")
    out = summarize_column(
        conn,
        table="t",
        col_name="id",
        col_type="id",
        n_rows=1000,
        n_distinct=1000,
        null_count=0,
        sample=["1", "2", "00012345"],
        dialect="duckdb",
    )
    assert out["stats"]["id_subtype"] == "integer"


def test_summarize_id_string_when_string_sample_has_letters(conn):
    """A string sample that contains a non-digit value stays ``string``
    — the auto-detection only flips to ``integer`` when every value
    parses as an int."""
    conn.execute("CREATE TABLE t(id VARCHAR)")
    out = summarize_column(
        conn,
        table="t",
        col_name="id",
        col_type="id",
        n_rows=1000,
        n_distinct=1000,
        null_count=0,
        sample=["1", "2", "AB1234"],
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


def test_summarize_censors_small_null_count(conn):
    """0 < null_count < SUPPRESS_K -> omit null_count and null_rate."""
    conn.execute("CREATE TABLE t(x BIGINT)")
    conn.executemany("INSERT INTO t VALUES (?)", [(i,) for i in range(95)])
    out = summarize_column(
        conn,
        table="t",
        col_name="x",
        col_type="numeric",
        n_rows=100,
        n_distinct=95,
        null_count=5,  # < SUPPRESS_K=10
        sample=list(range(95)),
        dialect="duckdb",
        rng=random.Random(0),
    )
    assert out["nullable"] is True
    assert "null_count" not in out
    assert "null_rate" not in out


def test_summarize_keeps_null_count_when_zero(conn):
    """null_count == 0 stays in the dict (nullable=False is a real fact)."""
    conn.execute("CREATE TABLE t(x BIGINT)")
    conn.executemany("INSERT INTO t VALUES (?)", [(i,) for i in range(10)])
    out = summarize_column(
        conn,
        table="t",
        col_name="x",
        col_type="numeric",
        n_rows=10,
        n_distinct=10,
        null_count=0,
        sample=list(range(10)),
        dialect="duckdb",
        rng=random.Random(0),
    )
    assert out["nullable"] is False
    assert out["null_count"] == 0
    assert out["null_rate"] == 0.0


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
