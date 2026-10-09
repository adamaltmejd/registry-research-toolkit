"""Response-body validators bound to package, steward, generation and read scope.

Every read (the catalog's) has a 60-second window. The full compiled generation invalidates the keyspace even when a route body
is unchanged. Conditional reads still execute the route before hashing its serialized
bytes.
"""

from __future__ import annotations

import hashlib

# The window for the fold-bearing catalog reads. A fresh fold or steward
# catalog edit must surface promptly for a returning user whose browser holds the
# unversioned cached copy. The body-hash
# ETag already changes when the body changes, but a 24h window would let the
# browser serve its stale copy for a day WITHOUT revalidating. 60s
# forces revalidation soon (the ETag avoids retransmitting an unchanged body, but
# the current middleware still executes the route). We keep it `public`
# (NOT `no-cache`) so the Cloudflare edge stays cacheable: `CF-Cache-Status: HIT`
# and the #220 probe survive, which `no-cache` would break.
CACHE_CONTROL_SHORT = "public, max-age=60, must-revalidate"

# 16 hex chars of the body sha256 — enough to make per-URL ETags
# collision-safe in practice while keeping the header short.
_HASH_PREFIX_LEN = 16


def compute_etag(
    body: bytes,
    reg_meta_version: str,
    steward_id: str,
    generation_id: str,
    scope: str,
) -> str:
    """Strong validator for the admitted artifact, effective scope and body."""
    digest = hashlib.sha256(body).hexdigest()[:_HASH_PREFIX_LEN]
    return f'"{reg_meta_version}-{steward_id}-{generation_id}-{scope}-{digest}"'


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
