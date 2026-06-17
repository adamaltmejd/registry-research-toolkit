"""ETag + Cache-Control logic for the read endpoints.

See DESIGN.md → ETag / Cache-Control (etag.py + middleware.py). Pure,
FastAPI-free functions so the scheme is unit-testable in isolation; the
middleware (``middleware.py``) wires them onto every read response. The
scheme:

    ETag: "<reg_meta_version>-<steward_id>-<sha256(body)[:16]>"
    Cache-Control: <one of three per-route policies — see below>

``cache_control_for`` picks one of three policies per path, all paired with the
body-hash ETag so revalidation stays a cheap 304 when unchanged and a fresh 200
when the body moves:

- ``/api/context`` (the deployment-identity read the SPA vintage footer renders)
  carries ``no-cache`` — it must revalidate on every request because it visibly
  asserts a version/date, so any stale copy would lie after a deploy.
- ``/api/catalog/*`` reads carry a SHORT ``max-age=60`` window: freshly-curated
  concept-group folds (and other catalog edits) must surface promptly for a
  returning user whose browser holds the unversioned cached copy. The body-hash
  ETag already changes when the group set changes, but a 24h ``max-age`` lets the
  browser serve its cached copy for a day WITHOUT revalidating, so the new fold
  never appears. A short window forces revalidation soon (the ETag keeps it a
  304); we keep it ``public`` (NOT ``no-cache``) so the Cloudflare edge stays
  cacheable — ``CF-Cache-Status: HIT`` and the #220 probe survive, which
  ``no-cache`` would break.
- Every other read (docs/search) keeps the 24h ``max-age=86400`` window by SCOPE
  decision — #499 is scoped to ``/api/catalog/*``. ``/api/search`` shares the same
  fold-staleness (it embeds the #322 concept-group folds too), so it is NOT
  stale-immune and could be lowered to the short window in a follow-up.

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

# Short window for catalog reads: a freshly-curated concept-group fold (or any
# catalog edit) must surface promptly for a returning user whose browser holds
# the unversioned cached copy. The body-hash ETag already changes on a fold, but
# the 24h `CACHE_CONTROL` window lets the browser serve its stale copy for a day
# WITHOUT revalidating — so the fold never appears. 60s forces revalidation soon
# (the ETag keeps it a cheap 304). We keep it `public` (NOT `no-cache`) so the
# Cloudflare edge stays cacheable: `CF-Cache-Status: HIT` and the #220 probe
# survive, which `no-cache` would break.
CACHE_CONTROL_SHORT = "public, max-age=60, must-revalidate"

# Exact API paths that must revalidate every request because they visibly assert
# a version/date (a stale copy lies). Currently only the vintage-footer source.
REVALIDATE_ALWAYS_PATHS = frozenset({"/api/context"})

# API path prefixes that get the short catalog window. PREFIX (not exact) match:
# the catalog read surface is `/api/catalog`, `/api/catalog/{...}/variants`, the
# `{fqid:path}` suffixed sub-endpoints (states/predecessors/successors/dimensions
# /related/lineage/lineage_warnings), and the `/api/catalog/{fqid:path}` catch-all
# — all share this prefix, and no other read route does (context/search/docs sit
# elsewhere under /api).
SHORT_CACHE_PATH_PREFIXES = ("/api/catalog",)

# 16 hex chars of the body sha256 — enough to make per-URL ETags
# collision-safe in practice while keeping the header short.
_HASH_PREFIX_LEN = 16


def cache_control_for(path: str) -> str:
    """The ``Cache-Control`` policy for a read endpoint by its API path.

    Three tiers, checked in order:

    - ``REVALIDATE_ALWAYS_PATHS`` (exact match, currently ``/api/context``) →
      ``CACHE_CONTROL_REVALIDATE`` (``no-cache``): revalidate every request, they
      assert a deploy version/date.
    - ``SHORT_CACHE_PATH_PREFIXES`` (prefix match, the ``/api/catalog/*`` reads) →
      ``CACHE_CONTROL_SHORT`` (60s): curated folds must surface promptly.
    - everything else (docs/search) → the 24h ``CACHE_CONTROL``.

    Exact-match is checked first so ``/api/context`` can never be shadowed by a
    prefix; no catalog path is in ``REVALIDATE_ALWAYS_PATHS`` today, but the order
    keeps the intent explicit."""
    if path in REVALIDATE_ALWAYS_PATHS:
        return CACHE_CONTROL_REVALIDATE
    if path.startswith(SHORT_CACHE_PATH_PREFIXES):
        return CACHE_CONTROL_SHORT
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
