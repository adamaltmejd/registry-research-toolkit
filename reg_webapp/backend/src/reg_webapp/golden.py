"""Curated golden-boost for `/api/search` (#393 item 4 / #311).

A *golden pin* promotes a canonical result to the TOP (rank 1) of its search group
for an exact (normalized) query, even when FTS would not surface it. If the pinned
entity is already on the result page it is deduped — removed from its FTS slot and
re-prepended at rank 1, not duplicated. This closes confirmed eval gaps where the
register a researcher should land on does not rank for a topical term
(``sysselsättning`` → ``scb/lisa``; ``diagnos`` → ``sos/par``).

Why it lives here (not in the route): golden-boost must operate on the reg_meta
typed search models (the ``reg_meta.queries.search`` output, #701) so BOTH the
route AND the eval runner (`scripts/run_search_eval.py`) apply the SAME function.
That is what makes the eval measure the route's TRUE behavior rather than an
approximation of it.

The curated pins live in ``reg_webapp/backend/src/reg_webapp/search_golden.toml``
(INSIDE the importable package so it travels with the src tree the runtime Docker
stage copies — `search_eval.toml` stays at `reg_webapp/backend/`, read only by the
dev eval runner, never shipped). Steward-authored, grow as needed. The TOML is parsed
once at import and validated fail-fast (CLAUDE.md): a pin with an unknown ``group`` or
an unsupported group is a config error at LOAD, not a silent no-op. A MISSING file is
likewise a packaging bug, not a "no pins" state — `_load_pins` raises rather than
silently disabling the feature; to intentionally ship no pins, commit a TOML with no
``[[pin]]`` entries (parses to ``{}`` gracefully). fqid RESOLVABILITY needs a DB
connection, so it is checked at apply time — a typo'd fqid raises there rather than
silently dropping the pin.
"""

from __future__ import annotations

import json
import tomllib
import unicodedata
from base64 import urlsafe_b64decode, urlsafe_b64encode
from binascii import Error as Base64Error
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING

from reg_meta.fqid import FqidError, parse as parse_fqid
from reg_meta.search import ClassificationSearchResult, RegisterSearchResult

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable

    from reg_meta.search import SearchResult

# Packaged INSIDE reg_webapp (beside this module) so it ships with the src tree the
# runtime Docker stage copies (Dockerfile: `COPY .../reg_webapp/backend/src ...`); a
# sibling TOML at backend/ would be absent in the deployed image → silent no-op.
GOLDEN_PATH = Path(__file__).resolve().parent / "search_golden.toml"


def _normalize(query: str) -> str:
    """The pin lookup key normalization: ASCII-fold diacritics, then casefold +
    strip. Mirrors a forgiving omnibox match (case-insensitive, surrounding
    whitespace ignored, diacritics folded). The diacritic fold (NFKD decompose +
    drop combining marks) keeps the pin lookup consistent with the rest of
    `/api/search`, where FTS unicode61 folds å/ä→a, ö→o on both index and query
    side — so a diacriticless `sysselsattning` still hits the `sysselsättning` pin.
    Same ASCII-fold approach as `reg_meta.fqid.derive_variable_slug`. Pin keys are
    built via this too, so both sides fold identically."""
    folded = "".join(
        ch
        for ch in unicodedata.normalize("NFKD", query)
        if not unicodedata.combining(ch)
    )
    return folded.casefold().strip()


@dataclass(frozen=True)
class _Pin:
    query: str
    group: str
    fqids: tuple[str, ...]
    note: str | None


def _register_pin(conn: sqlite3.Connection, fqid: str) -> RegisterSearchResult:
    """Build the `RegisterSearchResult` model for a pinned register fqid (#701).
    Resolves the 2-seg fqid (`provider/register`) by slug. A pin is order-prepended,
    not rank-sorted, so `rank=0.0`."""
    parsed = parse_fqid(fqid)
    row = conn.execute(
        "SELECT r.name AS register_name, r.purpose AS register_purpose "
        "FROM register r "
        "JOIN provider p ON p.provider_id = r.provider_id "
        "WHERE p.slug = ? AND r.slug = ?",
        (parsed.provider, parsed.register),
    ).fetchone()
    if row is None:
        raise ValueError(f"golden pin fqid {fqid!r} does not resolve to a register")
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
    (#701). Resolves ``class/<slug>`` by slug; `rank=0.0` (a pin is order-prepended,
    not rank-sorted)."""
    parsed = parse_fqid(fqid)
    row = conn.execute(
        "SELECT short_name, name AS classification_name "
        "FROM classification WHERE slug = ?",
        (parsed.classification,),
    ).fetchone()
    if row is None:
        raise ValueError(
            f"golden pin fqid {fqid!r} does not resolve to a classification"
        )
    # Pass the parsed `Fqid` (see `_register_pin`): the field is `Fqid | None` and
    # serializes back to the identical canonical string.
    return ClassificationSearchResult(
        fqid=parsed,
        short_name=row["short_name"],
        name=row["classification_name"],
        rank=0.0,
    )


# Builders are the single source of truth for which groups golden-boost supports;
# _SUPPORTED_GROUPS is DERIVED from them so a group can never be declared supported
# (passing load-validation) without a builder to back it (which would KeyError at
# apply). register + classification both resolve cheaply by slug; variable/value
# pins are NOT implemented — a pin targeting them is a config error at load (fail
# fast) rather than a silent no-op.
_PIN_BUILDERS: dict[str, Callable[[sqlite3.Connection, str], SearchResult]] = {
    "register": _register_pin,
    "classification": _classification_pin,
}
_SUPPORTED_GROUPS = frozenset(_PIN_BUILDERS)


def _load_pins(path: Path) -> dict[tuple[str, str], _Pin]:
    """Parse + validate ``search_golden.toml`` into a ``(normalized_query, group)``
    lookup. Validates structure eagerly (fail fast): each pin needs a non-empty
    ``query``, a supported ``group``, and a non-empty ``fqids`` list whose entries
    parse as FQIDs of the group's kind. Resolvability against the DB is deferred to
    `apply_golden_boost` (no connection at import)."""
    # The config is committed AND packaged inside reg_webapp, so its absence is a
    # packaging bug — fail loud (CLAUDE.md fail-fast) rather than silently disabling
    # golden-boost. To ship NO pins, commit a TOML with no `[[pin]]` entries (it
    # parses to `{}` gracefully via the loop below).
    if not path.exists():
        raise FileNotFoundError(
            f"golden config missing at {path} (it is committed + packaged with "
            "reg_webapp — its absence is a packaging bug; to ship no pins, commit a "
            "TOML with no `[[pin]]` entries)"
        )
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    pins: dict[tuple[str, str], _Pin] = {}
    for i, p in enumerate(raw.get("pin", [])):
        query = p.get("query")
        group = p.get("group")
        fqids = p.get("fqids")
        if not query or not isinstance(query, str):
            raise ValueError(f"golden pin #{i}: missing/invalid `query`")
        if group not in _SUPPORTED_GROUPS:
            raise ValueError(
                f"golden pin #{i} ({query!r}): unsupported group {group!r} "
                f"(supported: {sorted(_SUPPORTED_GROUPS)})"
            )
        if not fqids or not isinstance(fqids, list):
            raise ValueError(f"golden pin #{i} ({query!r}): missing/empty `fqids`")
        if len(fqids) != len(set(fqids)):
            raise ValueError(f"golden pin #{i} ({query!r}): duplicate `fqids`")
        for fqid in fqids:
            _validate_pin_fqid(query, group, fqid)
        key = (_normalize(query), group)
        if key in pins:
            raise ValueError(f"golden pin #{i}: duplicate (query, group) {key!r}")
        pins[key] = _Pin(
            query=query,
            group=group,
            fqids=tuple(fqids),
            note=p.get("note"),
        )
    return pins


def _validate_pin_fqid(query: str, group: str, fqid: str) -> None:
    """A pin's fqid must parse as an FQID of the group's kind (a register pin needs
    a 2-seg register fqid; a classification pin needs a ``class/<slug>`` fqid).
    Structural validation only — resolvability is checked at apply time."""
    try:
        parsed = parse_fqid(fqid)
    except FqidError as exc:
        raise ValueError(
            f"golden pin ({query!r}): invalid fqid {fqid!r}: {exc}"
        ) from exc
    if str(parsed.kind) != group:
        raise ValueError(
            f"golden pin ({query!r}): fqid {fqid!r} is a {parsed.kind} FQID, "
            f"but the pin's group is {group!r}"
        )


_PINS = _load_pins(GOLDEN_PATH)
_CURSOR_PREFIX = "golden."
_CURSOR_VERSION = 1
_CURSOR_DOMAIN = "reg-webapp-golden-cursor-v1"


class GoldenCursorError(ValueError):
    """An invalid or context-mismatched golden continuation cursor."""


def _cursor_context(query: str, group: str, fqids: tuple[str, ...]) -> str:
    value = json.dumps(
        {"query": _normalize(query), "group": group, "fqids": fqids},
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


def pinned_fqids(query: str, group: str) -> tuple[str, ...]:
    """Return curated identities that origin search must exclude on every page."""
    pin = _PINS.get((_normalize(query), group))
    return () if pin is None else pin.fqids


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

    For a pin matching the normalized query + group, each pin fqid is resolved into
    its reg_meta result MODEL and PREPENDED (in pin order) to the front. Any existing
    entry in ``results`` whose ``fqid`` equals a pin fqid is REMOVED first, so the
    pinned entity is promoted to rank 1 rather than duplicated. Net effect on length:

    - pin already on the page → removed from its FTS slot, re-prepended at rank 1
      (promoted, no duplicate) → ``len`` unchanged.
    - pin not on the page → prepended → ``len`` +1, so the route can retain
      continuation for the displaced origin row without computing an exact total.

    The common case — no matching pin — returns ``results`` as a list unchanged (kept
    cheap: one dict lookup).

    Operates on the reg_meta typed search models (#701) so the route and the eval
    runner share one behavior. A pin whose fqid does not resolve raises (fail fast)
    rather than silently dropping. Dedup compares the SERIALIZED fqid string (a result
    model's `fqid` is an `Fqid | None`; a pin's fqids are the canonical strings). A
    `ConceptGroupSearchResult` (foldable into the classification arm) carries no
    `fqid` field, so `getattr(..., None)` treats it as un-pinnable — never deduped.

    Callers exclude ``pinned_fqids(query, group)`` from origin search on every page,
    so a pinned identity cannot reappear at its natural deep FTS position.
    """
    pin = _PINS.get((_normalize(query), group))
    selected_fqids = pin.fqids if fqids is None and pin is not None else fqids
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
