"""SOS code-list period notation."""

from __future__ import annotations

import pytest
from reg_meta_build.source_value_periods import value_period


@pytest.mark.parametrize(
    ("source", "start", "end"),
    [
        ("1982/1983-1989/1990", "1982-01-01", "1990-12-31"),
        ("1990/1991-1998", "1990-01-01", "1998-12-31"),
        ("1990-1991/1992", "1990-01-01", "1992-12-31"),
        ("1990", "1990-01-01", "1990-12-31"),
        ("1990-1998", "1990-01-01", "1998-12-31"),
        ("1990-", "1990-01-01", None),
    ],
)
def test_code_period_bounds(source: str, start: str, end: str | None) -> None:
    window = value_period(source)
    assert window is not None
    assert (window.status, window.start, window.end) == ("known", start, end)


@pytest.mark.parametrize("source", ["1990/1992-1998", "1990/1991-1998/2000", "invalid"])
def test_malformed_code_period_stays_unknown(source: str) -> None:
    window = value_period(source)
    assert window is not None
    assert window.status == "unknown"
