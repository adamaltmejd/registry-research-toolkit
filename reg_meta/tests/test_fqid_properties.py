"""Property-based tests for the FQID and period-token grammars.

Hypothesis stresses the auto-slug fold (NFKD→ASCII→lowercase→hyphenate→validate)
with full-Unicode input — the example suite in ``test_fqid.py`` pins specific
cases, these assert the structural invariants hold for *any* input. Generated
period tokens likewise pin the bounds renderer as an exact inverse.
"""

from __future__ import annotations

from datetime import date

from hypothesis import given, strategies as st
from reg_meta.fqid import (
    _SLUG_RE,
    derive_variable_slug,
    is_period,
    period_token_for_bounds,
    period_token_to_bounds,
)

# Full Unicode: diacritics, non-Latin scripts, control chars, whitespace — the
# fold has to survive all of it. Include None to exercise the empty-input guard.
slug_inputs = st.one_of(st.none(), st.text())
years = st.integers(min_value=1900, max_value=2099)
period_tokens = st.one_of(
    years.map(str),
    st.tuples(years, st.integers(min_value=1, max_value=12)).map(
        lambda value: f"{value[0]:04d}-{value[1]:02d}"
    ),
    st.dates(min_value=date(1900, 1, 1), max_value=date(2099, 12, 31)).map(
        date.isoformat
    ),
    st.tuples(st.sampled_from(("HT", "VT")), years).map(
        lambda value: f"{value[0]}{value[1]:04d}"
    ),
    years.map(lambda year: f"LA{year:04d}"),
    st.tuples(years, st.integers(min_value=1, max_value=4)).map(
        lambda value: f"{value[0]:04d}-Q{value[1]}"
    ),
    st.tuples(years, st.integers(min_value=1, max_value=2)).map(
        lambda value: f"{value[0]:04d}-H{value[1]}"
    ),
)


@given(period_tokens)
def test_period_token_bounds_round_trip(token: str) -> None:
    """Every generated form survives token → bounds → canonical token → bounds."""
    bounds = period_token_to_bounds(token)
    rendered = period_token_for_bounds(*bounds)
    assert is_period(rendered)
    assert period_token_to_bounds(rendered) == bounds


@given(slug_inputs)
def test_output_is_none_or_valid_slug(name: str | None) -> None:
    """Any non-None output fully matches the module slug grammar."""
    out = derive_variable_slug(name)
    if out is not None:
        assert _SLUG_RE.match(out), f"{out!r} from {name!r} fails _SLUG_RE"


@given(slug_inputs)
def test_idempotence(name: str | None) -> None:
    """Feeding a derived slug back in returns it unchanged."""
    out = derive_variable_slug(name)
    if out is not None:
        assert derive_variable_slug(out) == out


@given(slug_inputs)
def test_determinism(name: str | None) -> None:
    """Two calls on the same input agree."""
    assert derive_variable_slug(name) == derive_variable_slug(name)


@given(slug_inputs)
def test_output_is_ascii_lowercase(name: str | None) -> None:
    """Non-None output is pure-ASCII lowercase (NFKD case/diacritic fold)."""
    out = derive_variable_slug(name)
    if out is not None:
        assert out.isascii()
        assert out == out.lower()
