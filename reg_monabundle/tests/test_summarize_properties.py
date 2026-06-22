"""Property-based tests for the disclosure-control leaves in ``summarize``.

The load-bearing invariant is the k-anonymity floor: ``_suppress_below_k`` must
never emit a count below ``suppress_k``, and the null group must always be
dropped — these are PII guards, asserted strictly here for *any* frequency
table. The example suite in ``test_summarize.py`` keeps the concrete cases.
"""

from __future__ import annotations

import math
import random
from datetime import date

from hypothesis import given, strategies as st
from reg_monabundle.runtime.summarize import (
    DATE_JITTER_DAYS,
    OTHER_LABEL,
    _detect_id_subtype,
    _jitter_date,
    _perturb,
    _suppress_below_k,
    _to_date,
)

# -- _suppress_below_k -------------------------------------------------------

# A frequency-table row: ``{"val": <group or None>, "n": <count>}``. ``val`` is
# None (the null group, always dropped) or an arbitrary value coerced via str();
# ``n`` is a non-negative count. The text arm excludes ``OTHER_LABEL`` — that
# string is a reserved output key (the suppressed-remainder bucket), so a row
# carrying it as a value would collide with that bucket and break the
# remainder-accounting invariant in ``test_suppress_k_other_iff_remainder_reaches_k``.
freq_rows = st.lists(
    st.fixed_dictionaries(
        {
            "val": st.one_of(
                st.none(),
                st.text().filter(lambda s: s != OTHER_LABEL),
                st.integers(),
                st.booleans(),
            ),
            "n": st.integers(min_value=0, max_value=10_000),
        }
    ),
    max_size=30,
)


@given(freq_rows, st.integers(min_value=1, max_value=50))
def test_suppress_k_floor_and_null_drop(rows: list[dict], suppress_k: int) -> None:
    """Every emitted count is >= suppress_k (the k-anonymity floor) and the
    null group is dropped."""
    out = _suppress_below_k(rows, suppress_k)
    for label, count in out.items():
        assert count >= suppress_k, f"{label}={count} < k={suppress_k}"
    # Null-drop is by identity (val is None), not by the string "None" — so the
    # invariant is that removing the null rows up front leaves the output
    # unchanged (a generated ``"None"``-string group is a legitimate emission).
    assert out == _suppress_below_k(
        [r for r in rows if r["val"] is not None], suppress_k
    )


@given(freq_rows, st.integers(min_value=1, max_value=50))
def test_suppress_k_other_iff_remainder_reaches_k(
    rows: list[dict], suppress_k: int
) -> None:
    """``_other`` appears iff the suppressed remainder is itself >= k."""
    suppressed_total = sum(
        int(r["n"]) for r in rows if r["val"] is not None and int(r["n"]) < suppress_k
    )
    out = _suppress_below_k(rows, suppress_k)
    if suppressed_total >= suppress_k:
        assert out.get(OTHER_LABEL) == suppressed_total
    else:
        assert OTHER_LABEL not in out


# -- _perturb ----------------------------------------------------------------

finite_numbers = st.one_of(
    st.integers(min_value=-(10**9), max_value=10**9),
    st.floats(allow_nan=False, allow_infinity=False, width=32),
)


@given(st.booleans())
def test_perturb_none_passthrough(is_int: bool) -> None:
    """None → None regardless of is_int."""
    assert _perturb(None, random.Random(0), is_int=is_int) is None


@given(finite_numbers, st.integers(), st.booleans())
def test_perturb_finite_and_deterministic(val: float, seed: int, is_int: bool) -> None:
    """Finite numeric in → finite numeric out; two freshly-seeded RNGs of the
    same seed give equal results."""
    out_a = _perturb(val, random.Random(seed), is_int=is_int)
    out_b = _perturb(val, random.Random(seed), is_int=is_int)
    assert out_a == out_b
    assert math.isfinite(float(out_a))


# -- _jitter_date ------------------------------------------------------------

dates = st.dates(min_value=date(1900, 1, 1), max_value=date(2099, 12, 31))


@given(dates, st.integers())
def test_jitter_date_bounded_and_deterministic(d: date, seed: int) -> None:
    """Result is a valid date within +/- DATE_JITTER_DAYS; deterministic under a
    seeded RNG."""
    out_a = _jitter_date(d, random.Random(seed))
    out_b = _jitter_date(d, random.Random(seed))
    assert out_a == out_b
    assert isinstance(out_a, date)
    assert abs((out_a - d).days) <= DATE_JITTER_DAYS


# -- _to_date / _detect_id_subtype totality ----------------------------------

arbitrary = st.one_of(
    st.none(),
    st.text(),
    # SQL-returned ints never exceed the driver's numeric range (BIGINT ~9.2e18,
    # DuckDB HUGEINT ~1.7e38), both far below float's ~1.8e308 overflow point, so
    # the float-overflow input that an unbounded ``st.integers()`` would feed
    # ``_detect_id_subtype`` -> ``_python_kind`` (``float(10**400)``) can't arrive
    # from a real column sample. Bound to the SQL BIGINT domain.
    st.integers(min_value=-(2**63), max_value=2**63 - 1),
    st.floats(allow_nan=False, allow_infinity=False),
    st.dates(),
    st.datetimes(),
)


@given(arbitrary, st.one_of(st.none(), st.text()))
def test_to_date_total(v: object, override_format: str | None) -> None:
    """Never raises on arbitrary input; returns a date or None."""
    out = _to_date(v, override_format)
    assert out is None or isinstance(out, date)


@given(st.lists(arbitrary))
def test_detect_id_subtype_total(sample: list[object]) -> None:
    """Never raises on arbitrary samples; returns 'integer' or 'string'."""
    assert _detect_id_subtype(sample) in ("integer", "string")


def test_to_date_malformed_format_falls_through() -> None:
    """A group-name-collision override format (re.error at compile) must fall
    through to the built-in parsers, not raise (regression)."""
    assert _to_date("2020-01-01", "%Y%Y") == date(2020, 1, 1)
