"""Response-body validators bound to package, steward, generation and read scope.

The existing cache policies remain: context revalidates, catalog/search/stats
have a 60-second window, and document reads have a 24-hour window. The full
compiled generation invalidates the keyspace even when a route body is unchanged.
Conditional reads still execute the route before hashing its serialized bytes.
"""

from __future__ import annotations

import hashlib

CACHE_CONTROL = "public, max-age=86400, must-revalidate"

# Always-revalidate policy for deployment-identity reads: the browser must
# revalidate every request. The existing body-derived ETag avoids retransmitting
# an unchanged response but still executes the route; a deploy returns a fresh 200.
CACHE_CONTROL_REVALIDATE = "no-cache"

# Short window for fold-bearing reads (catalog + search) and steward-dependent
# stats. A fresh fold or steward catalog edit must surface promptly for a
# returning user whose browser holds the unversioned cached copy. The body-hash
# ETag already changes when the body changes, but the 24h `CACHE_CONTROL` window
# lets the browser serve its stale copy for a day WITHOUT revalidating. 60s
# forces revalidation soon (the ETag avoids retransmitting an unchanged body, but
# the current middleware still executes the route). We keep it `public`
# (NOT `no-cache`) so the Cloudflare edge stays cacheable: `CF-Cache-Status: HIT`
# and the #220 probe survive, which `no-cache` would break.
CACHE_CONTROL_SHORT = "public, max-age=60, must-revalidate"

# Exact API paths that must revalidate every request because they visibly assert
# a version/date (a stale copy lies). Currently only the vintage-footer source.
REVALIDATE_ALWAYS_PATHS = frozenset({"/api/context"})

# API path prefixes that get the short fold-bearing window. PREFIX (not exact)
# match. The catalog read surface is `/api/catalog`, `/api/catalog/{...}/variants`,
# the `{fqid:path}` suffixed sub-endpoints (states/predecessors/successors
# /dimensions/lineage/lineage_warnings), and the `/api/catalog/{fqid:path}`
# catch-all — all share the `/api/catalog` prefix. `/api/search` is the
# variable/code search route (routes/search.py); it embeds the same #322
# concept-group folds, so it shares the staleness gap and the short window (#506).
# `/api/stats` uses the compiled holdings artifact for filtered
# deployments (#726), so a same-id steward catalog redeploy needs a prompt
# revalidation opportunity too.
# The doc-library search lives at `/api/docs/search` (under the `/api/docs` prefix)
# and is rebuild-stable, so it correctly stays on the 24h tier: it does NOT start
# with any short-cache prefix here (`/api/docs/search`.startswith(`/api/search`)
# is False).
SHORT_CACHE_PATH_PREFIXES = ("/api/catalog", "/api/search", "/api/stats")

# 16 hex chars of the body sha256 — enough to make per-URL ETags
# collision-safe in practice while keeping the header short.
_HASH_PREFIX_LEN = 16


def cache_control_for(path: str) -> str:
    """The ``Cache-Control`` policy for a read endpoint by its API path.

    Three tiers, checked in order:

    - ``REVALIDATE_ALWAYS_PATHS`` (exact match, currently ``/api/context``) →
      ``CACHE_CONTROL_REVALIDATE`` (``no-cache``): revalidate every request, they
      assert a deploy version/date.
    - ``SHORT_CACHE_PATH_PREFIXES`` (prefix match, the fold- or steward-dependent
      ``/api/catalog/*``, ``/api/search``, and ``/api/stats`` reads) →
      ``CACHE_CONTROL_SHORT`` (60s): curated folds and steward catalog edits must
      surface promptly.
    - everything else (the rebuild-stable ``/api/docs/*`` reads) → the 24h
      ``CACHE_CONTROL``.

    Exact-match is checked first so ``/api/context`` can never be shadowed by a
    prefix; no catalog path is in ``REVALIDATE_ALWAYS_PATHS`` today, but the order
    keeps the intent explicit."""
    if path in REVALIDATE_ALWAYS_PATHS:
        return CACHE_CONTROL_REVALIDATE
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
