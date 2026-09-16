"""Exact machine-source code-validity grammar, separate from occurrence inference."""

from __future__ import annotations

import calendar
import re
from datetime import date

from reg_meta_build.normalization import normalize_token
from reg_meta_build.source_values import SourceValueWindow


def value_window(
    start: str | None, end: str | None, *, compact_dates: bool = False
) -> SourceValueWindow:
    """Interpret documented boundary cells; blank means explicitly open here.

    Callers must establish that the source format gives blank bounds this meaning.
    Missing files, missing columns and formula results are not calls to this parser.
    """

    def bound(raw: str | None, *, upper: bool) -> str | None:
        if raw is None or not raw.strip():
            return None
        value = normalize_token(raw)
        if compact_dates and value.isascii() and value.isdigit():
            if len(value) == 4:
                value += "-12-31" if upper else "-01-01"
            elif len(value) == 6:
                year, month = int(value[:4]), int(value[4:])
                day = calendar.monthrange(year, month)[1] if upper else 1
                value = f"{year:04d}-{month:02d}-{day:02d}"
            elif len(value) == 8:
                value = f"{value[:4]}-{value[4:6]}-{value[6:]}"
        parsed = date.fromisoformat(value)
        if parsed.isoformat() != value:
            raise ValueError("noncanonical source date")
        return value

    try:
        return SourceValueWindow(
            "known", bound(start, upper=False), bound(end, upper=True)
        )
    except ValueError, OverflowError:
        return SourceValueWindow("unknown")


def value_period(text: str | None) -> SourceValueWindow | None:
    """Read the documented YYYY, YYYY-YYYY and YYYY- code-period forms.

    None/blank is an absent row constraint, distinct from malformed supplied text.
    A code-list's multi-year validity is not evidence of delivered variable years.
    """
    if text is None or not text.strip():
        return None
    value = normalize_token(text)
    if match := re.fullmatch(r"([0-9]{4})(?:\s*[-–—]\s*([0-9]{4})?)?", value):
        first, last = match.groups()
        if last is None:
            last = first if re.fullmatch(r"[0-9]{4}", value) else None
        return value_window(first, last, compact_dates=True)
    return SourceValueWindow("unknown")


__all__ = ["value_period", "value_window"]
