"""Curated golden-boost for `/api/search` (#393 item 4 / #311).

A *golden pin* promotes a canonical result to the TOP (rank 1) of its search group
for an exact (normalized) query, even when FTS would not surface it. If the pinned
entity is already on the result page it is deduped — removed from its FTS slot and
re-prepended at rank 1, not duplicated. This closes confirmed eval gaps where the
register a researcher should land on does not rank for a topical term
(``sysselsättning`` → ``scb/lisa``; ``diagnos`` → ``sos/par``).

Why it lives here (not in the route): golden-boost must operate on the RAW
reg_meta result dicts (the pre-shaping ``reg_meta.queries.search`` output) so BOTH
the route AND the eval runner (`scripts/run_search_eval.py`, which works on raw
dicts) apply the SAME function. That is what makes the eval measure the route's
TRUE behavior rather than an approximation of it.

The curated pins live in ``reg_webapp/backend/search_golden.toml`` (parallels
``search_eval.toml``); steward-authored, grow as needed. The TOML is parsed once
at import and validated fail-fast (CLAUDE.md): a pin with an unknown ``group`` or
an unsupported group is a config error at LOAD, not a silent no-op. fqid
RESOLVABILITY needs a DB connection, so it is checked at apply time — a typo'd
fqid raises there rather than silently dropping the pin.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from reg_meta.fqid import FqidError, parse as parse_fqid

if TYPE_CHECKING:
    import sqlite3

GOLDEN_PATH = Path(__file__).resolve().parents[2] / "search_golden.toml"


def _normalize(query: str) -> str:
    """The pin lookup key normalization: casefold + strip. Mirrors a forgiving
    omnibox match (case-insensitive, surrounding whitespace ignored)."""
    return query.casefold().strip()


@dataclass(frozen=True)
class _Pin:
    query: str
    group: str
    fqids: tuple[str, ...]
    note: str | None


def _register_raw_dict(conn: sqlite3.Connection, fqid: str) -> dict[str, Any]:
    """Build the RAW register result dict for a pinned register fqid, with the same
    keys ``routes.search._register_result`` reads (`fqid`, `register_name`,
    `register_purpose`). Resolves the 2-seg fqid (`provider/register`) by slug."""
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
    return {
        "type": "register",
        "fqid": fqid,
        "register_name": row["register_name"],
        "register_purpose": row["register_purpose"],
    }


def _classification_raw_dict(conn: sqlite3.Connection, fqid: str) -> dict[str, Any]:
    """Build the RAW classification result dict for a pinned classification fqid,
    with the same keys ``routes.search._classification_result`` reads (`fqid`,
    `short_name`, `classification_name`). Resolves ``class/<slug>`` by slug."""
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
    return {
        "type": "classification",
        "fqid": fqid,
        "short_name": row["short_name"],
        "classification_name": row["classification_name"],
    }


# Builders are the single source of truth for which groups golden-boost supports;
# _SUPPORTED_GROUPS is DERIVED from them so a group can never be declared supported
# (passing load-validation) without a builder to back it (which would KeyError at
# apply). register + classification both resolve cheaply by slug; variable/value
# pins are NOT implemented — a pin targeting them is a config error at load (fail
# fast) rather than a silent no-op.
_RAW_DICT_BUILDERS = {
    "register": _register_raw_dict,
    "classification": _classification_raw_dict,
}
_SUPPORTED_GROUPS = frozenset(_RAW_DICT_BUILDERS)


def _load_pins(path: Path) -> dict[tuple[str, str], _Pin]:
    """Parse + validate ``search_golden.toml`` into a ``(normalized_query, group)``
    lookup. Validates structure eagerly (fail fast): each pin needs a non-empty
    ``query``, a supported ``group``, and a non-empty ``fqids`` list whose entries
    parse as FQIDs of the group's kind. Resolvability against the DB is deferred to
    `apply_golden_boost` (no connection at import)."""
    if not path.exists():
        return {}
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


def apply_golden_boost(
    conn: sqlite3.Connection,
    query: str,
    group: str,
    results: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Promote any curated pin for ``(query, group)`` to the TOP of ``results``.

    For a pin matching the normalized query + group, each pin fqid is resolved into
    its raw result dict and PREPENDED (in pin order) to the front. Any existing entry
    in ``results`` whose ``fqid`` equals a pin fqid is REMOVED first, so the pinned
    entity is promoted to rank 1 rather than duplicated. Net effect on length:

    - pin already on the page → removed from its FTS slot, re-prepended at rank 1
      (promoted, no dup) → ``len`` unchanged → the route's ``total_count`` delta is 0
      (correct: it was already counted by FTS).
    - pin not on the page (the shipped non-matching case) → prepended → ``len`` +1 →
      ``total_count`` +1 (correct: a net-new injection).

    The common case — no matching pin — returns ``results`` unchanged (kept cheap:
    one dict lookup).

    Operates on the raw reg_meta result dicts so the route and the eval runner share
    one behavior. A pin whose fqid does not resolve raises (fail fast) rather than
    silently dropping.

    LIMITATION: only the result PAGE is visible here, not the full FTS match set. A
    pin that is itself an FTS match ranking BEYOND the page can't be deduped — it is
    re-counted (``total_count`` +1) and may reappear on a deep page. Golden pins are
    intended for canonical answers that rank poorly or not at all, so pinning a
    top-FTS-match is a degenerate config; the limitation does not bite the intended use.
    """
    pin = _PINS.get((_normalize(query), group))
    if pin is None:
        return results
    build = _RAW_DICT_BUILDERS[group]
    pinned = [build(conn, fqid) for fqid in pin.fqids]
    pin_fqids = set(pin.fqids)
    kept = [r for r in results if r.get("fqid") not in pin_fqids]
    return pinned + kept
