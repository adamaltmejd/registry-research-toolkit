"""ETag / Cache-Control middleware for the read endpoints.

DRY alternative to per-handler header wiring: one ASGI middleware stamps the
``ETag`` + ``Cache-Control`` headers (see DESIGN.md → ETag / Cache-Control
(etag.py + middleware.py)) on every GET read response and turns a
matching ``If-None-Match`` into a 304. Centralizing it here keeps the route
handlers free of caching boilerplate and guarantees the scheme is uniform across
``/api/context``, the ``/api/catalog`` root, the catch-all, and the 7 suffixed
sub-endpoints (the read surface A5.2a-ii ships).

Skips WRITE endpoints (``/api/project/*`` do NOT set ETag) — those land in
A5.2b. We gate on the request METHOD: only
``GET`` reads are cacheable; any other method is passed through untouched. (The
routes register GET only — FastAPI's ``@router.get`` does not auto-add HEAD, so a
HEAD 405s before reaching here; HEAD support, if wanted for cheap CDN
revalidation, is a deliberate later addition with its own body-stripping + test,
not an implicit claim here.) Combined with the read-only catalog surface today, a
method gate is sufficient and won't need editing when the write endpoints arrive.

The ETag is computed from the already-serialized response BODY bytes — FastAPI
emits the JSON deterministically (a fixed Pydantic model dump), and the
``?period`` / ``?variant`` query is part of the URL, so it's already part of the
cache key. The pure logic lives in ``etag.py``; this module is only the ASGI
plumbing (buffer the body, hash it, set headers, short-circuit a 304).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

import reg_meta
from reg_webapp.etag import cache_control_for, compute_etag, etag_matches

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from starlette.requests import Request

# Only GET reads get the read-cache treatment; everything else (the A5.2b
# write endpoints) passes through with no ETag. HEAD is intentionally NOT here:
# the routes register GET only, so a HEAD 405s before reaching this middleware —
# listing it would be a dead branch claiming support the routes don't provide.
_CACHEABLE_METHODS = frozenset({"GET"})


class ETagMiddleware(BaseHTTPMiddleware):
    """Stamp ETag + Cache-Control on GET reads; 304 on If-None-Match match."""

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        response = await call_next(request)
        if request.method not in _CACHEABLE_METHODS:
            return response
        # Only stamp successful reads — an error body (404/422/500) is not a
        # cacheable representation, and hashing it would hand the client a
        # validator for a transient error.
        if response.status_code != 200:
            return response

        body = await _read_body(response)
        steward_id = request.app.state.steward.id
        etag = compute_etag(body, reg_meta.__version__, steward_id)

        # dict() is safe here: read responses carry no repeated headers (no
        # Set-Cookie on a GET — auth is v2+, and A5.2b's write endpoints are
        # method-gated out above). Preserve the raw header list instead if a GET
        # ever emits a duplicate-key header.
        headers = dict(response.headers)
        headers["etag"] = etag
        # Per-route Cache-Control (three tiers, see cache_control_for): the
        # deployment-identity read (/api/context) revalidates every request so the
        # vintage footer can't serve a stale version after a deploy; the fold-bearing
        # /api/catalog/* and /api/search reads carry a short 60s window so curated
        # concept-group folds surface promptly for returning users; the
        # rebuild-stable /api/docs/* reads keep the 24h policy. Computed from
        # request.url.path (query stripped) once here so both the 200 and the
        # reused-`headers` 304 carry it.
        headers["cache-control"] = cache_control_for(request.url.path)

        if etag_matches(request.headers.get("if-none-match"), etag):
            # 304: no body. Keep the validating headers (ETag/Cache-Control) and
            # drop content-length/content-type for the empty entity.
            headers.pop("content-length", None)
            headers.pop("content-type", None)
            return Response(status_code=304, headers=headers)

        return Response(
            content=body,
            status_code=response.status_code,
            headers=headers,
            media_type=response.media_type,
        )


async def _read_body(response: Response) -> bytes:
    """Drain a response body into bytes so it can be hashed.

    ``BaseHTTPMiddleware`` always hands ``dispatch`` a ``_StreamingResponse``
    whose body is an async iterator (Starlette wraps the downstream response), so
    we buffer that iterator once. A plain ``Response`` (no iterator) is handled
    too for robustness, carrying its bytes on ``.body``.

    This buffering is provisional rather than free: classification responses at
    the current head can be several decoded megabytes. The pending v1 payload split
    will bound those responses, while the pending generation-token validator will
    avoid route execution entirely for a matching conditional read."""
    iterator = getattr(response, "body_iterator", None)
    if iterator is None:
        return bytes(response.body)
    chunks: list[bytes] = []
    async for chunk in iterator:
        chunks.append(chunk if isinstance(chunk, bytes) else chunk.encode("utf-8"))
    return b"".join(chunks)
