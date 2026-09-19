"""Conservative source-period normalization for stage-one records."""

from __future__ import annotations

import calendar
import re
from datetime import date
from typing import Literal

from reg_meta.fqid import _YEAR

from .edition_bounds import edition_claims
from .normalization import normalize_text, normalize_token
from .source_records import ScopeInterval, TemporalScope

SourcePeriodIssue = Literal["pooled_period", "unparseable_period"]

_YEAR_TOKEN_RE = re.compile(rf"(?<!\d)({_YEAR})(?!\d)")
_ISO_DATE_SHAPE_RE = re.compile(rf"(?P<date>{_YEAR}-\d{{1,2}}-\d{{1,2}})\Z")
# The dash class every range reader shares: ASCII hyphen plus the Unicode
# dashes SCB types, one to three of them, with surrounding spacing handled at
# each use site.
_RANGE_DASH = r"[-‐‑‒–—―−]{1,3}"
_ISO_DATE_RANGE_SHAPE_RE = re.compile(
    rf"(?P<start>{_YEAR}-\d{{1,2}}-\d{{1,2}})\s*{_RANGE_DASH}\s*"
    rf"(?P<end>{_YEAR}-\d{{1,2}}-\d{{1,2}})\Z"
)
_SWEDISH_DATE_SHAPE_RE = re.compile(
    rf"(?P<day>\d{{1,2}})\s+(?P<month>\S+)\s+(?P<year>{_YEAR})\Z"
)
_AMBIGUOUS_NUMERIC_DATE_RE = re.compile(r"(?:(?:\d{1,4}[/\.]){2}\d{1,4}|\d{8})\Z")
_SWEDISH_MONTHS = {"oktober": 10, "jan": 1}

# A school year is one period, 1 July of the first year to 30 June of the
# second. A Deklarationsår denotes the beskattningsår named in parentheses.
# A bare `YYYY/YYYY+1` pair reads exactly like `Läsåret YYYY/YYYY+1` (Y-208).
_LÄSÅRET_RE = re.compile(rf"läsåret\s*({_YEAR})\s*/\s*({_YEAR})", re.IGNORECASE)
_SPLIT_YEAR_RE = re.compile(rf"({_YEAR})\s*/\s*({_YEAR})")
# Fiscal-year month ranges `YYYY-MM - YYYY-MM` (Y-208), sharing the range dash.
_MONTH_RANGE_RE = re.compile(
    rf"({_YEAR})-(\d{{1,2}})\s*{_RANGE_DASH}\s*({_YEAR})-(\d{{1,2}})"
)
# A full school year named by its terms (Y-208): only Höstterminen into next
# year's Vårterminen; any other term pairing is not a school year.
_TERM_RANGE_RE = re.compile(
    rf"höstterminen\s*({_YEAR})\s*{_RANGE_DASH}\s*vårterminen\s*({_YEAR})",
    re.IGNORECASE,
)
# A pooled school-year hull `Läsåren A/A+1 - C/C+1` (Y-208): whole school
# years, so the hull is resolvable delivery evidence unlike term edges.
_LÄSÅREN_RANGE_RE = re.compile(
    rf"läsåren\s*({_YEAR})\s*/\s*({_YEAR})\s*{_RANGE_DASH}\s*"
    rf"({_YEAR})\s*/\s*({_YEAR})",
    re.IGNORECASE,
)
_DEKLARATIONSÅR_RE = re.compile(
    rf"deklarationsår\s*({_YEAR})\s*\(\s*beskattningsår\s*({_YEAR})\s*\)",
    re.IGNORECASE,
)


def _exact_interval(value: str) -> tuple[tuple[str, str] | None, bool]:
    """Return an observed exact interval and whether text claimed that format.

    The boolean distinguishes an invalid source-authored date from unrelated text:
    malformed dates become unknown instead of silently widening to a calendar year.
    """
    if match := _ISO_DATE_RANGE_SHAPE_RE.fullmatch(value):
        try:
            start = date.fromisoformat(match.group("start"))
            end = date.fromisoformat(match.group("end"))
        except ValueError:
            return None, True
        if end < start:
            return None, True
        return (start.isoformat(), end.isoformat()), True

    if match := _ISO_DATE_SHAPE_RE.fullmatch(value):
        try:
            parsed = date.fromisoformat(match.group("date"))
        except ValueError:
            return None, True
        return (parsed.isoformat(), parsed.isoformat()), True

    if match := _SWEDISH_DATE_SHAPE_RE.fullmatch(value):
        month = _SWEDISH_MONTHS.get(normalize_token(match.group("month")).casefold())
        if month is None:
            return None, True
        try:
            parsed = date(
                year=int(match.group("year")),
                month=month,
                day=int(match.group("day")),
            )
        except ValueError:
            return None, True
        return (parsed.isoformat(), parsed.isoformat()), True

    return None, False


def _school_year_scope(first: int, second: int) -> TemporalScope:
    """The one school-year period, 1 July of the first year to 30 June of the
    second, carried on both scopes like the `Läsåret` branch."""
    interval = ScopeInterval(start=f"{first}-07-01", end=f"{second}-06-30")
    return TemporalScope(kind="intervals", intervals=(interval,))


def _whole_year_claims(claims: tuple[tuple[int, str, str], ...]) -> bool:
    """Whether every claim spans a whole calendar year (Y-202).

    The genuinely pooled shape pools whole delivery years; term/school-year
    labels carry sub-annual claim edges (HT/VT, Läsår) and stay unresolvable."""
    return bool(claims) and all(
        low.endswith("-01-01") and high.endswith("-12-31") for _, low, high in claims
    )


def source_scopes(
    version_name: str,
) -> tuple[TemporalScope, TemporalScope, SourcePeriodIssue | None]:
    """Interpret one SCB edition label without manufacturing annual evidence.

    Dates in edition labels keep day precision; they do not establish variable
    reference time. Existing edition-claim parsing supplies annual and subannual
    intervals. A `Läsåret YYYY/YYYY+1` school year — with or without the prefix —
    a `Höstterminen YYYY - Vårterminen YYYY+1` term range, and a `YYYY-MM -
    YYYY-MM` month range are each one period; a `Läsåren A/A+1 - C/C+1` hull is
    one pooled range over 1 July of the first year to 30 June of the last. A
    `Deklarationsår YYYY (beskattningsår YYYY-1)` naming its income year is the
    income year's one period. Remaining multi-year claims stay pooled because
    the stage-one record cannot safely assert independent annual availability.
    """
    label = normalize_text(version_name) or "<blank Registerversionnamn>"
    exact_interval, date_shaped = _exact_interval(label)
    if exact_interval is not None and exact_interval[0][:4] == exact_interval[1][:4]:
        edition_scope = TemporalScope(
            kind="intervals",
            intervals=(
                ScopeInterval(start=exact_interval[0][:4], end=exact_interval[0][:4]),
            ),
        )
        edition_period_scope = TemporalScope(
            kind="intervals",
            intervals=(ScopeInterval(start=exact_interval[0], end=exact_interval[1]),),
        )
        return edition_scope, edition_period_scope, None
    if exact_interval is not None:
        # A multi-year exact range pools to ONE state over the whole range
        # (Y-202): the range is carried on the scope, never re-parsed downstream.
        scope = TemporalScope(
            kind="pooled",
            label=label,
            pooled_start=exact_interval[0],
            pooled_end=exact_interval[1],
        )
        return scope, scope, "pooled_period"
    if date_shaped:
        scope = TemporalScope(kind="unknown", label=label)
        return scope, scope, "unparseable_period"
    if _AMBIGUOUS_NUMERIC_DATE_RE.fullmatch(label):
        scope = TemporalScope(kind="unknown", label=label)
        return scope, scope, "unparseable_period"

    if match := _LÄSÅRET_RE.fullmatch(label) or _SPLIT_YEAR_RE.fullmatch(label):
        first, second = int(match.group(1)), int(match.group(2))
        if second == first + 1:
            scope = _school_year_scope(first, second)
            return scope, scope, None
        scope = TemporalScope(kind="unknown", label=label)
        return scope, scope, "unparseable_period"

    if match := _MONTH_RANGE_RE.fullmatch(label):
        start_year, start_month, end_year, end_month = map(int, match.groups())
        if 1 <= start_month <= 12 and 1 <= end_month <= 12:
            start = date(start_year, start_month, 1)
            end = date(end_year, end_month, calendar.monthrange(end_year, end_month)[1])
            if end >= start:
                interval = ScopeInterval(start=start.isoformat(), end=end.isoformat())
                scope = TemporalScope(kind="intervals", intervals=(interval,))
                return scope, scope, None
        scope = TemporalScope(kind="unknown", label=label)
        return scope, scope, "unparseable_period"

    if match := _TERM_RANGE_RE.fullmatch(label):
        first, second = int(match.group(1)), int(match.group(2))
        if second == first + 1:
            scope = _school_year_scope(first, second)
            return scope, scope, None
        scope = TemporalScope(kind="unknown", label=label)
        return scope, scope, "unparseable_period"

    if match := _LÄSÅREN_RANGE_RE.fullmatch(label):
        first_start, first_end, last_start, last_end = map(int, match.groups())
        if (
            first_end == first_start + 1
            and last_end == last_start + 1
            and (last_start, last_end) > (first_start, first_end)
        ):
            scope = TemporalScope(
                kind="pooled",
                label=label,
                pooled_start=f"{first_start}-07-01",
                pooled_end=f"{last_end}-06-30",
            )
            return scope, scope, "pooled_period"
        # Not whole school years: fall through, keeping no bounds.

    deklaration = _DEKLARATIONSÅR_RE.fullmatch(label)
    if (
        deklaration is not None
        and int(deklaration.group(2)) == int(deklaration.group(1)) - 1
    ):
        year = int(deklaration.group(2))
        interval = ScopeInterval(start=f"{year}-01-01", end=f"{year}-12-31")
        scope = TemporalScope(kind="intervals", intervals=(interval,))
        return scope, scope, None
    if "deklarationsår" in label.casefold():
        scope = TemporalScope(kind="unknown", label=label)
        return scope, scope, "unparseable_period"

    claims = edition_claims(label)
    # A single fallback claim beside another disconnected year is ambiguous. The
    # edition grammar intentionally rejects it instead of selecting the first year.
    if len(claims) == 1 and len(set(_YEAR_TOKEN_RE.findall(label))) > 1:
        claims = ()
    if len(claims) == 1:
        year, low, high = claims[0]
        return (
            TemporalScope(
                kind="intervals",
                intervals=(ScopeInterval(start=str(year), end=str(year)),),
            ),
            TemporalScope(
                kind="intervals",
                intervals=(ScopeInterval(start=low, end=high),),
            ),
            None,
        )
    if claims:
        # Multi-year claims pool to ONE state over the claim hull (Y-202): the
        # first claim's start through the last claim's end. Interior per-year
        # structure stays on the claim key set; resolution never infers annual
        # availability inside the range. The hull is resolvable ONLY when every
        # claim is a whole calendar year — the genuinely pooled shape (e.g.
        # "2012 - 2014"). Term/school-year structure (Komvux HT/VT, Läsår
        # edges) stays unresolvable: its sub-annual edges are not whole-year
        # delivery evidence, so no range is carried and the scope keeps the
        # pre-Y-202 unsupported behavior.
        if _whole_year_claims(claims):
            scope = TemporalScope(
                kind="pooled",
                label=label,
                pooled_start=min(low for _, low, _ in claims),
                pooled_end=max(high for _, _, high in claims),
            )
        else:
            scope = TemporalScope(kind="pooled", label=label)
        return scope, scope, "pooled_period"

    scope = TemporalScope(kind="unknown", label=label)
    return scope, scope, "unparseable_period"


__all__ = ["SourcePeriodIssue", "source_scopes"]
