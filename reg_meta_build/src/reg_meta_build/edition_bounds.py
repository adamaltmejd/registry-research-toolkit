"""SCB register-version name to delivery-window parsing."""

from __future__ import annotations

import re
from functools import cache

from reg_meta.fqid import _YEAR, is_period, period_token_to_bounds
from reg_meta.queries import extract_year

from ._curation import fold_column

# Term phrase -> HT/VT prefix, year on either side. `hosttermin`/`vartermin`
# are NFKD-folded Swedish forms; compact `HT2024`/`VT 2024` is covered too.
_TERM_BOUND_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\bhosttermin(?:en)?\s+(\d{4})\b"), "HT"),
    (re.compile(r"\b(\d{4})\s+hosttermin(?:en)?\b"), "HT"),
    (re.compile(r"\bht\s*(\d{4})\b"), "HT"),
    (re.compile(r"\bvartermin(?:en)?\s+(\d{4})\b"), "VT"),
    (re.compile(r"\b(\d{4})\s+vartermin(?:en)?\b"), "VT"),
    (re.compile(r"\bvt\s*(\d{4})\b"), "VT"),
)

# Quarter: `kvartal N` / `kv N` / `kvN`, optionally a range `N-M` / `N- kv M`.
_QUARTER_BOUND_RE = re.compile(r"\bkv(?:artal)?\s*([1-4])(?:\s*-\s*(?:kv\s*)?([1-4]))?")

# Half-year: `Första/Andra halvåret YYYY` after fold_column normalization.
_HALF_BOUND_RE = re.compile(r"\b(forsta|andra)\s+halvar(?:et)?\s+(\d{4})\b")

# School year `A/B` — `Läsåret 2012/2013`, or twice for `Läsåren A/B - C/D`.
_SCHOOL_YEAR_RE = re.compile(rf"(?<!\d)({_YEAR})/({_YEAR})(?!\d)")

# Year range `A - B` / `A-B`, each endpoint optionally a full ISO date so the
# `1961-01-01 -- 2025-12-31` shape lands here too (dashes are normalized to
# ASCII before matching, so `––` reads as `--`).
_ISO_TAIL = r"(?:-\d{2}-\d{2})?"
_YEAR_RANGE_RE = re.compile(
    rf"(?<!\d)({_YEAR}){_ISO_TAIL}\s*-{{1,3}}\s*({_YEAR}){_ISO_TAIL}(?!\d)"
)

# `fold_column` NFKD-decomposes then drops non-ASCII, which DELETES an en/em dash
# rather than folding it: `1961-01-01 –– 2025-12-31` would read as two dates with
# no separator, and `2005 kvartal 2–4` as a lone Q2. Map the Unicode dashes onto
# ASCII `-` FIRST, in the one fold both readers below go through, so a name reads
# the same whichever dash character SCB happened to type.
_DASHES = str.maketrans(dict.fromkeys("‐‑‒–—―−", "-"))


def _fold_name(versionname: str) -> str:
    """An edition name folded for matching: Unicode dashes normalized to ASCII,
    then `fold_column`'s NFKD/ASCII/lowercase fold."""
    return fold_column(versionname.translate(_DASHES))


def _term_bounds(folded: str) -> list[tuple[int, tuple[str, str]]]:
    """Every HT/VT marker in a folded edition name as ``(year, (lo, hi))``.

    Markers outside the period grammar are dropped — `period_token_to_bounds`
    raises on `HT1850`, and a stray out-of-range term in a name like `HT 1850,
    version 2024` must not crash the build.
    """
    out: list[tuple[int, tuple[str, str]]] = []
    for pat, prefix in _TERM_BOUND_PATTERNS:
        for m in pat.finditer(folded):
            year = int(m.group(1))
            token = f"{prefix}{year:04d}"
            if is_period(token):
                out.append((year, period_token_to_bounds(token)))
    return out


def edition_bounds(versionname: str | None, year: int | None) -> tuple[str, str] | None:
    """Inclusive ISO ``(lo, hi)`` delivery window for an SCB edition name.

    ``year`` is the row's edition year (`extract_year(registerversionnamn)`). Only
    sub-annual markers whose own year equals ``year`` are narrowed; a marker naming
    a different year is ignored. Quarter markers carry no year and are expanded
    against ``year`` directly. With no matching marker, returns the full edition
    year. With ``year`` missing, returns ``None`` so callers can fall back to their
    own unknown-year behavior.

    This is the WITHIN-ONE-YEAR narrowing only. A name whose own span crosses a
    year boundary (a school year, a term or year range) is widened by
    `edition_claims`, which calls this for the single-year case.
    """
    if year is None:
        return None
    s = _fold_name(versionname) if versionname else ""
    if not s:
        return None
    ystr = f"{year:04d}"
    bounds: list[tuple[str, str]] = [b for y, b in _term_bounds(s) if y == year]
    for m in _QUARTER_BOUND_RE.finditer(s):
        for q in (m.group(1), m.group(2)):
            if q:
                bounds.append(period_token_to_bounds(f"{ystr}-Q{q}"))
    for m in _HALF_BOUND_RE.finditer(s):
        if m.group(2) == ystr:
            half = "1" if m.group(1) == "forsta" else "2"
            bounds.append(period_token_to_bounds(f"{ystr}-H{half}"))
    if bounds:
        return min(lo for lo, _ in bounds), max(hi for _, hi in bounds)
    return period_token_to_bounds(ystr)


def _school_year_span(folded: str) -> tuple[str, str] | None:
    """`Läsåret A/B` / `Läsåren A/B - C/D` → HT of the first year .. VT of the
    last.

    Every `A/B` in the name must be CONSECUTIVE; a non-consecutive pair is some
    other slash-joined number pair (a classification vintage, a fraction), not a
    school year, and the whole name is left to the single-year path.
    """
    pairs = [(int(a), int(b)) for a, b in _SCHOOL_YEAR_RE.findall(folded)]
    if not pairs or any(b != a + 1 for a, b in pairs):
        return None
    return (
        period_token_to_bounds(f"HT{pairs[0][0]:04d}")[0],
        period_token_to_bounds(f"VT{pairs[-1][1]:04d}")[1],
    )


def _term_span(folded: str) -> tuple[str, str] | None:
    """`Höstterminen A - Vårterminen B` / `HT A - VT B` → first term .. last term.

    Needs markers in TWO distinct years: a single term is `edition_bounds`'
    within-year narrowing, and an HT+VT pair inside one year already hulls to
    that year there.
    """
    terms = _term_bounds(folded)
    if len({year for year, _ in terms}) < 2:
        return None
    return min(lo for _, (lo, _) in terms), max(hi for _, (_, hi) in terms)


def _year_range_span(folded: str) -> tuple[str, str] | None:
    """`A - B` (or an ISO date range) → the full calendar years A..B.

    The endpoints' months and days are deliberately dropped: a claim window is
    period-token grained (`resolution.day_after_window_end` asserts its end is a
    month end), and the observed ISO ranges are whole years anyway
    (`1961-01-01 –– 2025-12-31`).

    No future-guard here: a range ending years from now is read like any other,
    because whether a register's names are forecast horizons is the caller's
    declared fact (`_PROJECTION_REGISTERS` in `sources/scb.py`).
    """
    m = _YEAR_RANGE_RE.search(folded)
    if m is None:
        return None
    lo_year, hi_year = int(m.group(1)), int(m.group(2))
    if hi_year <= lo_year:
        return None
    return (
        period_token_to_bounds(f"{lo_year:04d}")[0],
        period_token_to_bounds(f"{hi_year:04d}")[1],
    )


def _name_span(versionname: str, year: int) -> tuple[str, str] | None:
    """The full inclusive ISO span an edition name claims, or None for a name
    that spans at most its own edition year."""
    s = _fold_name(versionname)
    span = _school_year_span(s) or _term_span(s) or _year_range_span(s)
    # `extract_year` reads the name's FIRST year, so a name that really spans a
    # range starts at `year`. A span starting later means the first year is
    # something else — a collection year in front of the period it describes
    # ("Insamling 2019 avseende höstterminen 2020") — and that year is the
    # edition's own claim, so keep the single-year reading.
    if span is None or int(span[0][:4]) != year:
        return None
    return span


@cache
def vintage_claim(versionname: str | None) -> tuple[tuple[int, str, str], ...]:
    """The single ``(year, lo, hi)`` claim an edition name's own VINTAGE makes.

    The name's first year (`extract_year`), narrowed inside that year by
    `edition_bounds`. Two callers: the single-year fallback below, and a declared
    projection register, where the version IS the vintage
    (`_PROJECTION_REGISTERS` in `sources/scb.py`). Empty when the name carries no
    parseable year.
    """
    if not versionname:
        return ()
    year = extract_year(versionname)
    if year is None:
        return ()
    # A parsed `year` implies a non-empty name, so `edition_bounds` does not
    # return None here — but a claim MUST exist for every observed year, so
    # don't couple that guarantee to it.
    lo, hi = edition_bounds(versionname, year) or period_token_to_bounds(f"{year:04d}")
    return ((year, lo, hi),)


@cache
def edition_claims(versionname: str | None) -> tuple[tuple[int, str, str], ...]:
    """The ``(year, lo, hi)`` delivery claims an SCB edition name makes, ascending.

    One claim per calendar year the name spans, each an inclusive ISO window
    NESTED IN ITS YEAR (`resolution.Claim`'s contract). A single-year name yields
    exactly one claim, `edition_bounds`' window. A multi-year name — a year range,
    an ISO date range, a school year, a term range — yields one claim per year it
    covers, the first and last narrowed to the span's own edges and the interior
    years full: `Läsåret 2012/2013` is HT2012 then VT2013, `Komvux HT 1988 - VT
    2024` is HT1988, 1989..2023 whole, then VT2024.

    Contributing every spanned year (not just a hull) keeps the claim KEY SET
    carrying run/gap structure, so a gap between versions stays a gap and the
    coalescer's fusing rules are unchanged.

    A pure function of the NAME: no build year, corpus statistic or wall clock
    enters here, so a rebuild of the same corpus stays byte-identical and a name
    reads the same whatever else the corpus happens to contain. The projection
    policy is the caller's (`sources/scb.py::register_edition_claims`).

    Cached: the coalescer parses per instance row (~515K) over a corpus of a few
    thousand DISTINCT edition names, and the result is immutable.
    """
    if not versionname:
        return ()
    vintage = vintage_claim(versionname)
    if not vintage:
        return ()
    year, _, _ = vintage[0]
    span = _name_span(versionname, year)
    if span is None:
        return vintage
    lo, hi = span
    lo_year, hi_year = int(lo[:4]), int(hi[:4])
    claims: list[tuple[int, str, str]] = []
    for y in range(lo_year, hi_year + 1):
        y_lo, y_hi = period_token_to_bounds(f"{y:04d}")
        claims.append((y, lo if y == lo_year else y_lo, hi if y == hi_year else y_hi))
    return tuple(claims)
