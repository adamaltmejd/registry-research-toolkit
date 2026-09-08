"""Shared free-text query-param gate + paging leaves for the read endpoints.

The query reaches FTS only as a BOUND parameter (no SQLi surface), so this gate
guards cost/abuse and the NUL byte sqlite rejects — not injection. Used as a
FastAPI dependency (`Depends(validate_text_query)`) by `routes/search.py` and
`routes/docs.py` so the `?q=` contract is identical across both.

`clamp_limit` is the one page-size rule every paged read shares (clamp, never
422 — a nonsense `?limit` is display intent, not a contract break), and
`matches_filter` is the substring matcher for the reads that filter in Python
rather than in FTS.
"""

from __future__ import annotations

import re
import unicodedata

from fastapi import HTTPException

# Cap the query length to bound work; the FTS builder neutralizes operators.
QUERY_MAX_LEN = 200


def validate_text_query(q: str) -> str:
    """Reject an over-long query or one carrying a NUL byte (sqlite raises on
    embedded NUL) with 422. A blank / whitespace / punctuation-only query is NOT
    an error — downstream FTS building yields no results for it."""
    if "\x00" in q:
        raise HTTPException(status_code=422, detail="query may not contain NUL")
    if len(q) > QUERY_MAX_LEN:
        raise HTTPException(
            status_code=422,
            detail=f"query too long (max {QUERY_MAX_LEN} characters)",
        )
    return q


def clamp_limit(limit: int, *, maximum: int) -> int:
    """A page size clamped to [1, `maximum`]. Clamping, not 422ing, is the
    convention across the read endpoints: `?limit` is display intent, and an
    out-of-range one has an obvious nearest answer."""
    return max(1, min(limit, maximum))


# The SPA's `foldText` (frontend/src/lib/catalog.ts), character for character:
# NFD-decompose, drop the combining diacritical marks block, lowercase — so a
# server-side filter answers "lon" for "Lön" exactly as the in-browser list
# filters do. SQLite's LIKE folds neither case beyond ASCII nor diacritics, which
# is why this runs in Python over the rows rather than in SQL.
_COMBINING_MARKS = re.compile(r"[\u0300-\u036f]")


def fold_text(value: str) -> str:
    """Fold for diacritic-blind, case-insensitive substring matching."""
    return _COMBINING_MARKS.sub("", unicodedata.normalize("NFD", value)).lower()


def matches_filter(needle: str, *haystacks: str | None) -> bool:
    """Whether any haystack CONTAINS the folded `needle`. An empty (or
    whitespace-only) needle matches everything — the unfiltered list. `%` and `_`
    are ordinary characters here: this is a substring test, not LIKE."""
    folded = fold_text(needle).strip()
    if not folded:
        return True
    return any(h is not None and folded in fold_text(h) for h in haystacks)
