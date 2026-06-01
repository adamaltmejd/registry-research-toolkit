"""Write-endpoint cost protection: body-size cap + per-IP rate limit (§9.4).

§9.4 has no auth — the data is public-ish registry metadata and there's no
server-side user state. "Auth" is cost protection on the actual-work POST
endpoints (`/api/project/validate`, `/api/project/order`, `/api/bundle`). Two
stdlib-only ASGI middlewares, no new dependency (no slowapi):

- ``BodySizeLimitMiddleware`` — a STREAMING byte-count guard that 413s a request
  whose body exceeds ``MAX_BODY_BYTES`` (1 MB, §9.4). It counts the bytes as they
  arrive rather than trusting ``Content-Length`` — that header can lie, be absent
  on a chunked request, or be spoofed below the real size — so a body that
  *streams* past the cap is rejected even with a small/missing declared length.
- ``RateLimitMiddleware`` — an in-memory token bucket keyed on the client IP
  (``request.client.host``), ~``RATE_LIMIT_PER_MINUTE`` requests/min/IP (§9.4).
  IP-only by design (no session token — that adds a fingerprinting surface for
  anonymous public data, §9.4); a localStorage token is a later, opt-in concern.

Both gate ONLY the write methods (POST). Read GETs flow through untouched — they
are edge-cached (Cloudflare) and ETag-revalidated (``ETagMiddleware``), a
different and cheaper protection axis. The cap MUST run BEFORE a handler reads
the body, and the limiter before any work; ``app.py`` adds them so they wrap the
routers (see its middleware-ordering note).

Cloudflare fronts production with the same body cap + per-IP limits at the edge
(§9.4); these origin-side guards catch direct origin hits that bypass the edge.
The buckets are per-process in-memory (lost on restart, not shared across
replicas) — sufficient as the origin backstop behind the edge limiter; a shared
store (Redis) is a scale-out concern, not v1.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from typing import TYPE_CHECKING

from starlette.responses import JSONResponse, Response

if TYPE_CHECKING:
    from starlette.types import ASGIApp, Message, Receive, Scope, Send

# §9.4: 1 MB cap on write bodies — matches the bundle-output budget (§12) and is
# comfortably larger than any plausible project_data.json (the 200-column
# load-test fixture lands at tens of KB).
MAX_BODY_BYTES = 1024 * 1024

# §9.4: ~30 req/min/IP on the actual-work endpoints, stricter than the
# edge-cached reads. Modeled as a token bucket: capacity = the per-minute budget,
# refilled continuously at capacity/60 tokens per second so a steady drip is
# allowed but a burst beyond the bucket is 429'd.
RATE_LIMIT_PER_MINUTE = 30

# Only write methods are gated. GET/HEAD reads are edge-cached + ETag-revalidated
# (a separate, cheaper axis) and must pass through both middlewares untouched.
_WRITE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})


class BodySizeLimitMiddleware:
    """413 a write request whose body streams past ``MAX_BODY_BYTES`` (§9.4).

    Pure ASGI (not ``BaseHTTPMiddleware``) so it can intercept the request body
    stream BEFORE the route handler reads it: it wraps ``receive`` and tallies the
    bytes of each ``http.request`` chunk, short-circuiting with a 413 the moment
    the running total exceeds the cap. This is the STREAMING guard the spec asks
    for — it does NOT trust ``Content-Length`` (absent on chunked transfers,
    and spoofable), so an oversized chunked/under-declared body is still caught.
    """

    def __init__(self, app: ASGIApp, *, max_body_bytes: int = MAX_BODY_BYTES) -> None:
        self.app = app
        self.max_body_bytes = max_body_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # Only HTTP write methods are capped; reads (and lifespan/websocket
        # scopes) pass through with the original receive channel.
        if scope["type"] != "http" or scope.get("method") not in _WRITE_METHODS:
            await self.app(scope, receive, send)
            return

        total = 0
        too_large = False

        async def counting_receive() -> Message:
            nonlocal total, too_large
            message = await receive()
            if message["type"] == "http.request":
                total += len(message.get("body", b""))
                if total > self.max_body_bytes:
                    too_large = True
            return message

        # Buffer-and-count by draining the body ourselves up front: a handler that
        # never reads the body (or stops early) would otherwise let an oversized
        # stream slip the cap. We read until the cap trips OR the body completes,
        # then replay the captured chunks downstream so the handler still sees them
        # when the body is within budget.
        chunks: list[bytes] = []
        more = True
        while more:
            message = await counting_receive()
            if message["type"] == "http.request":
                chunks.append(message.get("body", b""))
                more = message.get("more_body", False)
                if too_large:
                    break
            elif message["type"] == "http.disconnect":
                more = False
            else:  # pragma: no cover — http scope yields only the two above
                more = False

        if too_large:
            response = _too_large_response(self.max_body_bytes)
            await response(scope, receive, send)
            return

        replay = _make_replay_receive(chunks, receive)
        await self.app(scope, replay, send)


def _make_replay_receive(chunks: list[bytes], receive: Receive) -> Receive:
    """A ``receive`` that replays the buffered (within-budget) body chunks, then
    falls back to the original channel for any trailing protocol messages
    (e.g. ``http.disconnect``). A ``deque`` (popleft is O(1)) — a list with
    ``pop(0)`` would be O(n²) replaying many chunks."""
    queue = deque(chunks)

    async def replay() -> Message:
        if queue:
            body = queue.popleft()
            return {
                "type": "http.request",
                "body": body,
                "more_body": bool(queue),
            }
        return await receive()

    return replay


def _too_large_response(limit: int) -> Response:
    return JSONResponse(
        status_code=413,
        content={
            "detail": (
                f"request body exceeds the {limit}-byte limit; "
                "project_data.json is expected to be well under 1 MB"
            )
        },
    )


class _TokenBucket:
    """A single IP's token bucket. ``capacity`` tokens, refilled continuously at
    ``refill_per_sec`` tokens/second (lazily, on each check). One token per
    request; an empty bucket means the IP is over budget. Not thread-locked
    itself — the owning middleware holds one lock around the whole map."""

    __slots__ = ("tokens", "updated")

    def __init__(self, capacity: float, now: float) -> None:
        # `now` is the caller's single monotonic reading — NOT a fresh
        # time.monotonic() here. A fresh bucket must have ZERO elapsed time on its
        # first check: if it sampled its own (later) clock reading, the first
        # _allow's `now - bucket.updated` would be NEGATIVE, refill the bucket by a
        # negative amount, and spuriously 429 the very first request.
        self.tokens = capacity
        self.updated = now


class RateLimitMiddleware:
    """In-memory per-IP token-bucket rate limiter for write endpoints (§9.4).

    Keyed on ``request.client.host`` (IP-only, §9.4). Pure ASGI (no body access
    needed — the decision is made from the method + IP before the handler runs, so
    no request-body buffering). A drained bucket returns 429. The bucket map is
    guarded by a single lock; the per-IP work is O(1) and the lock is held only
    for the arithmetic, so contention is negligible at this scale.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        per_minute: int = RATE_LIMIT_PER_MINUTE,
    ) -> None:
        self.app = app
        self.capacity = float(per_minute)
        self.refill_per_sec = per_minute / 60.0
        self._buckets: dict[str, _TokenBucket] = {}
        self._lock = threading.Lock()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope.get("method") not in _WRITE_METHODS:
            await self.app(scope, receive, send)
            return
        client = scope.get("client")
        # `client` is (host, port) or None (e.g. a test transport without a peer).
        # A missing host is bucketed under a single shared key rather than waved
        # through — fail closed for cost protection.
        ip = client[0] if client else "unknown"
        if not self._allow(ip):
            response = _rate_limited_response(int(self.capacity))
            await response(scope, receive, send)
            return
        await self.app(scope, receive, send)

    def _allow(self, ip: str) -> bool:
        """Consume one token for ``ip``; True if a token was available. Refills the
        bucket by elapsed time since its last check before consuming."""
        now = time.monotonic()
        with self._lock:
            bucket = self._buckets.get(ip)
            if bucket is None:
                # Seed `updated = now` (same reading used below) so the fresh
                # bucket has zero elapsed on this first check — see _TokenBucket.
                bucket = _TokenBucket(self.capacity, now)
                self._buckets[ip] = bucket
            elapsed = now - bucket.updated
            bucket.tokens = min(
                self.capacity, bucket.tokens + elapsed * self.refill_per_sec
            )
            bucket.updated = now
            if bucket.tokens < 1.0:
                return False
            bucket.tokens -= 1.0
            return True


def _rate_limited_response(per_minute: int) -> Response:
    """429 with the ACTUAL configured limit in the message (``create_app`` can
    override the default, so the number isn't hard-coded)."""
    return JSONResponse(
        status_code=429,
        content={
            "detail": (
                f"rate limit exceeded ({per_minute} requests/min/IP); retry shortly"
            )
        },
        headers={"Retry-After": "60"},
    )
