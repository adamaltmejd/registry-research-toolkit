"""Response-body validators bound to package, steward and generation.

Reads carry a 24-hour window. The full compiled generation invalidates the keyspace
even when a route body is unchanged. Conditional reads still execute the route before
hashing its serialized bytes.
"""

from __future__ import annotations

import hashlib

CACHE_CONTROL = "public, max-age=86400, must-revalidate"

# 16 hex chars of the body sha256 — enough to make per-URL ETags
# collision-safe in practice while keeping the header short.
_HASH_PREFIX_LEN = 16


def compute_etag(
    body: bytes,
    reg_meta_version: str,
    steward_id: str,
    generation_id: str,
) -> str:
    """Strong validator for the admitted artifact and body."""
    digest = hashlib.sha256(body).hexdigest()[:_HASH_PREFIX_LEN]
    return f'"{reg_meta_version}-{steward_id}-{generation_id}-{digest}"'


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
