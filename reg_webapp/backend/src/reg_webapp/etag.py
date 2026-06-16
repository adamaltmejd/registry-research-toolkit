"""ETag + Cache-Control logic for the read endpoints.

See DESIGN.md → ETag / Cache-Control (etag.py + middleware.py). Pure,
FastAPI-free functions so the scheme is unit-testable in isolation; the
middleware (``middleware.py``) wires them onto every read response. The
scheme:

    ETag: "<reg_meta_version>-<steward_id>-<sha256(body)[:16]>"
    Cache-Control: public, max-age=86400, must-revalidate

Per-route exception: ``/api/context`` (the deployment-identity read the SPA
vintage footer renders) carries ``Cache-Control: no-cache`` instead — it must
revalidate on every request because it visibly asserts a version/date, so a
sub-24h-stale copy would lie after a deploy. ``cache_control_for`` picks the
policy per path; the ETag keeps that revalidation cheap (a 304 when unchanged,
a fresh 200 right after a deploy).

The body-hash component makes ``If-None-Match`` per-URL coherent (every URL —
including its ``?period`` / ``?variant`` query, already part of the URL — gets
its own ETag). The ``reg_meta_version`` + ``steward_id`` prefix isn't needed for
correctness (the hash disambiguates) but keeps ETags human-debuggable and
invalidates the whole keyspace when either axis changes (e.g. a DB rebuild on a
new reg_meta release).

``reg_meta_version`` is the INSTALLED package version (``reg_meta.__version__``,
the v1.x Model A release), NOT the DB ``schema_version`` manifest value.
"""

from __future__ import annotations

import hashlib

CACHE_CONTROL = "public, max-age=86400, must-revalidate"

# Always-revalidate policy for deployment-identity reads: the browser must
# revalidate every request, but the existing ETag makes that a cheap 304 when
# unchanged and a fresh 200 right after a deploy.
CACHE_CONTROL_REVALIDATE = "no-cache"

# Exact API paths that must revalidate every request because they visibly assert
# a version/date (a stale copy lies). Currently only the vintage-footer source.
REVALIDATE_ALWAYS_PATHS = frozenset({"/api/context"})

# 16 hex chars of the body sha256 — enough to make per-URL ETags
# collision-safe in practice while keeping the header short.
_HASH_PREFIX_LEN = 16


def cache_control_for(path: str) -> str:
    """The ``Cache-Control`` policy for a read endpoint by its API path.

    ``REVALIDATE_ALWAYS_PATHS`` get ``CACHE_CONTROL_REVALIDATE`` (revalidate every
    request — they assert a deploy version/date); every other read keeps the 24h
    ``CACHE_CONTROL``."""
    if path in REVALIDATE_ALWAYS_PATHS:
        return CACHE_CONTROL_REVALIDATE
    return CACHE_CONTROL


def compute_etag(body: bytes, reg_meta_version: str, steward_id: str) -> str:
    """The strong ETag for a response body: a quoted
    ``"<reg_meta_version>-<steward_id>-<sha256(body)[:16]>"``.

    Strong (no ``W/`` prefix) because the body is byte-deterministic for a given
    URL + DB build. The value is quoted per RFC 7232; ``etag_matches`` compares
    against the raw ``If-None-Match`` header value (which carries the quotes)."""
    digest = hashlib.sha256(body).hexdigest()[:_HASH_PREFIX_LEN]
    return f'"{reg_meta_version}-{steward_id}-{digest}"'


def _opaque_tag(tag: str) -> str:
    """Strip a leading ``W/`` weak-validator marker, leaving the quoted
    opaque-tag — the unit the weak comparison compares."""
    return tag.removeprefix("W/")


def etag_matches(if_none_match: str | None, etag: str) -> bool:
    """Whether an ``If-None-Match`` request header matches ``etag`` → serve 304.

    Uses the WEAK comparison function RFC 7232 Section 3.2 mandates for ``If-None-Match``
    (the opposite of ``If-Range``, which is strong): the leading ``W/`` weak
    marker is stripped from BOTH sides before comparing the quoted opaque-tag, so
    an intermediary that weakens our strong ETag to ``W/"…"`` (e.g. Cloudflare on
    a transform/compression) still revalidates to 304 instead of re-sending the
    full body. Handles the comma-separated list form and the ``*`` wildcard."""
    if not if_none_match:
        return False
    candidates = [tok.strip() for tok in if_none_match.split(",")]
    if "*" in candidates:
        return True
    target = _opaque_tag(etag)
    return any(_opaque_tag(c) == target for c in candidates)
