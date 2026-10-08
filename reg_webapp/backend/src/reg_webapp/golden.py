"""Curated golden-boost for `/api/search` (#393 item 4 / #311).

A *golden pin* promotes a canonical result to the TOP (rank 1) of its search group
for an exact (folded) query, even when FTS would not surface it. If the pinned
entity is already on the result page it is deduped — removed from its FTS slot and
re-prepended at rank 1, not duplicated. This closes confirmed eval gaps where the
register a researcher should land on does not rank for a topical term
(``sysselsättning`` → ``scb/lisa``; ``diagnos`` → ``sos/par``).

Why it lives here (not in the route): golden-boost must operate on the reg_meta
typed search models (the ``reg_meta.queries.search`` output, #701) so BOTH the
route AND the eval runner (`scripts/run_search_eval.py`) apply the SAME function.
That is what makes the eval measure the route's TRUE behavior rather than an
approximation of it.

The pins are curated build input (``reg_meta_build/curation/search_pins.toml``)
stored in the catalog's ``search_pin`` table, keyed by ``fold_search(query)``. The
build guarantees that every stored pin resolves, so a lookup needs no validation.
"""

from __future__ import annotations

import json
from base64 import urlsafe_b64decode, urlsafe_b64encode
from binascii import Error as Base64Error
from hashlib import sha256
from typing import TYPE_CHECKING

from reg_meta.catalog import Catalog
from reg_meta.fqid import parse as parse_fqid
from reg_meta.queries import fold_search
from reg_meta.search import ClassificationSearchResult, RegisterSearchResult

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable

    from reg_meta.holdings import ReadScope
    from reg_meta.search import SearchResult


def _register_pin(conn: sqlite3.Connection, fqid: str) -> RegisterSearchResult:
    """Build the `RegisterSearchResult` model for a pinned register fqid (#701).
    A pin is order-prepended, not rank-sorted, so `rank=0.0`."""
    parsed = parse_fqid(fqid)
    row = conn.execute(
        "SELECT r.name AS register_name, r.purpose AS register_purpose "
        "FROM register r "
        "JOIN provider p ON p.provider_id = r.provider_id "
        "WHERE p.slug = ? AND r.slug = ?",
        (parsed.provider, parsed.register),
    ).fetchone()
    # Pass the parsed `Fqid` (not the raw string) — the field is `Fqid | None`; it
    # serializes back to the identical canonical string on the wire.
    return RegisterSearchResult(
        fqid=parsed,
        name=row["register_name"],
        purpose=row["register_purpose"],
        rank=0.0,
    )


def _classification_pin(
    conn: sqlite3.Connection, fqid: str
) -> ClassificationSearchResult:
    """Build the `ClassificationSearchResult` model for a pinned classification fqid
    (#701); `rank=0.0` (a pin is order-prepended, not rank-sorted)."""
    parsed = parse_fqid(fqid)
    row = conn.execute(
        "SELECT short_name, name AS classification_name "
        "FROM classification WHERE slug = ?",
        (parsed.classification,),
    ).fetchone()
    return ClassificationSearchResult(
        fqid=parsed,
        short_name=row["short_name"],
        name=row["classification_name"],
        rank=0.0,
    )


# The pin types the build stores; other groups never have pins.
_PIN_BUILDERS: dict[str, Callable[[sqlite3.Connection, str], SearchResult]] = {
    "register": _register_pin,
    "classification": _classification_pin,
}
_CURSOR_PREFIX = "golden."
_CURSOR_VERSION = 1
_CURSOR_DOMAIN = "reg-webapp-golden-cursor-v1"


class GoldenCursorError(ValueError):
    """An invalid or context-mismatched golden continuation cursor."""


def _cursor_context(query: str, group: str, fqids: tuple[str, ...]) -> str:
    value = json.dumps(
        {"query": fold_search(query), "group": group, "fqids": fqids},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return sha256(value.encode()).hexdigest()


def _cursor_signature(payload: dict[str, object]) -> str:
    value = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return sha256(f"{_CURSOR_DOMAIN}:{value}".encode()).hexdigest()


def decode_continuation(
    cursor: str | None,
    query: str,
    group: str,
    fqids: tuple[str, ...],
) -> tuple[str | None, int]:
    """Unwrap golden state, or treat a plain reg_meta cursor as pins-exhausted."""
    if cursor is None:
        return None, 0
    if not cursor.startswith(_CURSOR_PREFIX):
        return cursor, len(fqids)
    try:
        raw = cursor.removeprefix(_CURSOR_PREFIX)
        payload = json.loads(urlsafe_b64decode(raw + "=" * (-len(raw) % 4)))
        if not isinstance(payload, dict):
            raise TypeError
        signature = payload.pop("signature", None)
        if signature != _cursor_signature(payload):
            raise ValueError
        if payload.get("v") != _CURSOR_VERSION:
            raise GoldenCursorError("Golden search cursor version is unsupported.")
        if payload.get("context") != _cursor_context(query, group, fqids):
            raise GoldenCursorError(
                "Golden search cursor does not match this query and result group."
            )
        origin = payload.get("origin")
        offset = payload.get("offset")
        if not isinstance(origin, str) or not isinstance(offset, int):
            raise TypeError
        if offset < 0 or offset >= len(fqids):
            raise ValueError
        return origin, offset
    except GoldenCursorError:
        raise
    except (
        Base64Error,
        UnicodeDecodeError,
        json.JSONDecodeError,
        TypeError,
        ValueError,
    ) as exc:
        raise GoldenCursorError("Golden search cursor is invalid or tampered.") from exc


def encode_continuation(
    origin_cursor: str,
    query: str,
    group: str,
    fqids: tuple[str, ...],
    offset: int,
) -> str:
    """Wrap the origin cursor with the next bounded golden-pin position."""
    payload: dict[str, object] = {
        "v": _CURSOR_VERSION,
        "context": _cursor_context(query, group, fqids),
        "origin": origin_cursor,
        "offset": offset,
    }
    payload["signature"] = _cursor_signature(payload)
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return _CURSOR_PREFIX + urlsafe_b64encode(raw).decode().rstrip("=")


def pinned_fqids(conn: sqlite3.Connection, query: str, group: str) -> tuple[str, ...]:
    """Return curated identities that origin search must exclude on every page."""
    return tuple(
        entity
        for (entity,) in conn.execute(
            "SELECT entity FROM search_pin WHERE key = ? AND type = ? "
            "ORDER BY position",
            (fold_search(query), group),
        )
    )


def eligible_pinned_fqids(
    conn: sqlite3.Connection, query: str, group: str, *, scope: ReadScope
) -> tuple[str, ...]:
    """The pins for ``(query, group)`` that exist in ``scope``."""
    fqids = pinned_fqids(conn, query, group)
    if not fqids or scope == "reference":
        return fqids
    catalog = Catalog(conn, scope=scope)
    return tuple(fqid for fqid in fqids if catalog.exists(fqid))


def apply_golden_boost(
    conn: sqlite3.Connection,
    query: str,
    group: str,
    results: tuple[SearchResult, ...],
    *,
    fqids: tuple[str, ...] | None = None,
    start: int = 0,
    limit: int | None = None,
) -> list[SearchResult]:
    """Promote any curated pin for ``(query, group)`` to the TOP of ``results``.

    Each pin fqid (``fqids``, or the stored pins when ``None``) is resolved into its
    reg_meta result MODEL and PREPENDED (in pin order) to the front. Any existing
    entry in ``results`` whose ``fqid`` equals a pin fqid is REMOVED first, so the
    pinned entity is promoted to rank 1 rather than duplicated. Net effect on length:

    - pin already on the page → removed from its FTS slot, re-prepended at rank 1
      (promoted, no duplicate) → ``len`` unchanged.
    - pin not on the page → prepended → ``len`` +1, so the route can retain
      continuation for the displaced origin row without computing an exact total.

    The common case — no matching pin — returns ``results`` as a list unchanged.

    Operates on the reg_meta typed search models (#701) so the route and the eval
    runner share one behavior. Dedup compares the SERIALIZED fqid string (a result
    model's `fqid` is an `Fqid | None`; a pin's fqids are the canonical strings). A
    `ConceptGroupSearchResult` (foldable into the classification arm) carries no
    `fqid` field, so `getattr(..., None)` treats it as un-pinnable — never deduped.

    Callers exclude ``pinned_fqids(conn, query, group)`` from origin search on every
    page, so a pinned identity cannot reappear at its natural deep FTS position.
    """
    if group not in _PIN_BUILDERS:
        return list(results)
    selected_fqids = pinned_fqids(conn, query, group) if fqids is None else fqids
    if not selected_fqids:
        return list(results)
    build = _PIN_BUILDERS[group]
    stop = None if limit is None else start + limit
    pinned = [build(conn, fqid) for fqid in selected_fqids[start:stop]]
    pin_fqids = set(selected_fqids)
    # `getattr(..., None)`: a `ConceptGroupSearchResult` carries no `fqid` field, so
    # it can never match a pin (matches the old dict `.get("fqid")` semantics).
    kept = [r for r in results if _fqid_str(getattr(r, "fqid", None)) not in pin_fqids]
    return pinned + kept


def _fqid_str(fqid: object | None) -> str | None:
    """The canonical string form of a result model's `Fqid | None` field, for
    comparison against a pin's canonical fqid strings (None stays None)."""
    return str(fqid) if fqid is not None else None
