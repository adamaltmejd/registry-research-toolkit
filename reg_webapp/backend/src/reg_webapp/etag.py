"""Response-body validators bound to package, steward, generation and read scope.

Catalog reads have a 60-second window and document reads a 24-hour
window. The full compiled generation invalidates the keyspace even when a route body
is unchanged. Conditional reads still execute the route before hashing its serialized
bytes.
"""

from __future__ import annotations

import hashlib

CACHE_CONTROL = "public, max-age=86400, must-revalidate"

# Short window for fold-bearing reads (catalog). A fresh fold or steward
# catalog edit must surface promptly for a returning user whose browser holds the
# unversioned cached copy. The body-hash
# ETag already changes when the body changes, but the 24h `CACHE_CONTROL` window
# lets the browser serve its stale copy for a day WITHOUT revalidating. 60s
# forces revalidation soon (the ETag avoids retransmitting an unchanged body, but
# the current middleware still executes the route). We keep it `public`
# (NOT `no-cache`) so the Cloudflare edge stays cacheable: `CF-Cache-Status: HIT`
# and the #220 probe survive, which `no-cache` would break.
CACHE_CONTROL_SHORT = "public, max-age=60, must-revalidate"

# API path prefixes that get the short fold-bearing window. PREFIX (not exact)
# match. The catalog read surface is `/api/catalog`, `/api/catalog/{...}/variants`,
# the `{fqid:path}` suffixed sub-endpoints (states/predecessors/successors
# /dimensions/lineage/lineage_warnings), and the `/api/catalog/{fqid:path}`
# catch-all — all share the `/api/catalog` prefix. The doc-library search at
# `/api/docs/search` is rebuild-stable, so it stays on the 24h tier.
SHORT_CACHE_PATH_PREFIXES = ("/api/catalog",)

# 16 hex chars of the body sha256 — enough to make per-URL ETags
# collision-safe in practice while keeping the header short.
_HASH_PREFIX_LEN = 16


def cache_control_for(path: str) -> str:
    """The ``Cache-Control`` policy for a read endpoint by its API path.

    Two tiers:

    - ``SHORT_CACHE_PATH_PREFIXES`` (prefix match, the fold- or steward-dependent
      ``/api/catalog/*`` reads) → ``CACHE_CONTROL_SHORT``
      (60s): curated folds and steward catalog edits must surface promptly.
    - everything else (the rebuild-stable ``/api/docs/*`` reads) → the 24h
      ``CACHE_CONTROL``."""
    if path.startswith(SHORT_CACHE_PATH_PREFIXES):
        return CACHE_CONTROL_SHORT
    return CACHE_CONTROL


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
