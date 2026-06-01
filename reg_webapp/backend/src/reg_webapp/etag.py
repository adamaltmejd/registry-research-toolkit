"""ETag + Cache-Control logic for the read endpoints (§9.4 / §9.5).

Pure, FastAPI-free functions so the scheme is unit-testable in isolation; the
middleware (``middleware.py``) wires them onto every read response. The §9.4
scheme:

    ETag: "<reg_meta_version>-<steward_id>-<sha256(body)[:16]>"
    Cache-Control: public, max-age=86400, must-revalidate

The body-hash component makes ``If-None-Match`` per-URL coherent (every URL —
including its ``?period`` / ``?variant`` query, already part of the URL — gets
its own ETag). The ``reg_meta_version`` + ``steward_id`` prefix isn't needed for
correctness (the hash disambiguates) but keeps ETags human-debuggable and
invalidates the whole keyspace when either axis changes (e.g. a DB rebuild on a
new reg_meta release).

``reg_meta_version`` is the INSTALLED package version (``reg_meta.__version__``,
the v1.x Model A release), NOT the DB ``schema_version`` manifest value (§9.5).
"""

from __future__ import annotations

import hashlib

CACHE_CONTROL = "public, max-age=86400, must-revalidate"

# §9.4: 16 hex chars of the body sha256 — enough to make per-URL ETags
# collision-safe in practice while keeping the header short.
_HASH_PREFIX_LEN = 16


def compute_etag(body: bytes, reg_meta_version: str, steward_id: str) -> str:
    """The §9.4 strong ETag for a response body: a quoted
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

    Uses the WEAK comparison function RFC 7232 §3.2 mandates for ``If-None-Match``
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
