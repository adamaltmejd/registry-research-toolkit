"""Shared free-text query-param gate for the read endpoints (search + docs).

The query reaches FTS only as a BOUND parameter (no SQLi surface), so this gate
guards cost/abuse and the NUL byte sqlite rejects — not injection. Used as a
FastAPI dependency (`Depends(validate_text_query)`) by `routes/search.py` and
`routes/docs.py` so the `?q=` contract is identical across both.
"""

from __future__ import annotations

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
