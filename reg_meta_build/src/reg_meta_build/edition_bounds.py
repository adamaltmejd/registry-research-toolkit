"""SCB register-version name to delivery-window parsing."""

from __future__ import annotations

import re

from reg_meta.fqid import period_token_to_bounds

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


def edition_bounds(versionname: str | None, year: int | None) -> tuple[str, str] | None:
    """Inclusive ISO ``(lo, hi)`` delivery window for an SCB edition name.

    ``year`` is the row's edition year (`extract_year(registerversionnamn)`). Only
    sub-annual markers whose own year equals ``year`` are narrowed; a marker naming
    a different year is ignored. Quarter markers carry no year and are expanded
    against ``year`` directly. With no matching marker, returns the full edition
    year. With ``year`` missing, returns ``None`` so callers can fall back to their
    own unknown-year behavior.
    """
    if year is None:
        return None
    s = fold_column(versionname) if versionname else ""
    if not s:
        return None
    ystr = f"{year:04d}"
    bounds: list[tuple[str, str]] = []
    for pat, prefix in _TERM_BOUND_PATTERNS:
        for m in pat.finditer(s):
            if m.group(1) == ystr:
                bounds.append(period_token_to_bounds(f"{prefix}{ystr}"))
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
    return f"{ystr}-01-01", f"{ystr}-12-31"
