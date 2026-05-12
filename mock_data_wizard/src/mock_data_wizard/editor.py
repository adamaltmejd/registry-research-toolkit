"""Stateless editor API for ``mock_data_config.json``.

Pure functions over the on-disk config file plus the regmeta DB. No
in-memory session state; every mutation autosaves atomically and
returns a fresh snapshot. Concurrency is handled via SHA-256
``snapshot_version`` tokens — a stale ``expected_version`` raises
``StaleStateError`` without writing.

Module surface (see ``DESIGN.md`` § Editor API for the full contract):

- ``get_state``, ``init_if_missing``
- ``set_column_type``, ``set_group_register``, ``set_source_registers``,
  ``set_source_years``, ``set_column_options``
- ``put_panel``, ``remove_panel``
- Helpers: ``list_registers``, ``resolve_register``,
  ``detect_year_from_source_name``, ``detect_panel_member_kind``
- Re-exports: ``Panel``, ``PanelMember``, ``Register``,
  ``PanelMemberSuggestion``
- Errors: ``NotInitializedError``, ``ValidationError``, ``StaleStateError``
- Sentinel: ``UNCHANGED``
- Constants: ``VALID_COLUMN_TYPES``, ``VALID_OPTION_KEYS``,
  ``VALID_ID_SUBTYPES``, ``VALID_NUMERIC_SUBTYPES``, ``INLINE_HINT_KEYS``,
  ``GLOBAL_SUPPRESS_K``
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from collections.abc import Sequence
from typing import Any, Iterator, Literal

from .classify import (
    COLUMN_TYPES,
    RegmetaSignal,
    _classify,
    _regmeta_lookup,
    _validate_discover_payload,
    regmeta_implied_type,
)
from .config import (
    CONFIG_FILENAME,
    INLINE_HINT_KEYS,
    SCHEMA_VERSION,
    VALID_ID_SUBTYPES,
    VALID_NUMERIC_SUBTYPES,
    VALID_OPTION_KEYS,
    ColumnTypeOverride,
    MDWConfig,
    Panel,
    PanelMember,
    _parse_options,
    _parse_panel,
    _reject_duplicate_keys,
    panel_to_dict,
    parse_config,
)
from .panels import (
    PanelMemberHints,
    PanelMemberSuggestion,
    detect_panel_candidate,
    detect_panel_member_hints,
    detect_panel_member_kind,
    detect_year_from_source_name,
)
from .registers import Register, list_registers, resolve_register
from .summarize import SUPPRESS_K as GLOBAL_SUPPRESS_K
from ._util import lookup_with_prefix_fallback, strip_project_prefix

__all__ = [
    # API
    "get_state",
    "init_if_missing",
    "set_column_type",
    "set_group_register",
    "set_source_registers",
    "set_source_years",
    "set_column_options",
    "put_panel",
    "parse_panel_payload",
    "remove_panel",
    # Data models
    "StateSnapshot",
    "RegisterGroupView",
    "ColumnInfo",
    "EditorWarning",
    "Panel",
    "PanelMember",
    "PanelMemberHints",
    "PanelMemberSuggestion",
    "Register",
    # Helpers
    "list_registers",
    "resolve_register",
    "detect_year_from_source_name",
    "detect_panel_member_hints",
    "detect_panel_member_kind",
    # Errors
    "NotInitializedError",
    "ValidationError",
    "StaleStateError",
    # Sentinel
    "UNCHANGED",
    # Constants
    "VALID_COLUMN_TYPES",
    "VALID_OPTION_KEYS",
    "VALID_ID_SUBTYPES",
    "VALID_NUMERIC_SUBTYPES",
    "INLINE_HINT_KEYS",
    "GLOBAL_SUPPRESS_K",
]

# Re-exposed under the editor's preferred name.
VALID_COLUMN_TYPES = COLUMN_TYPES
DISCOVER_FILENAME_DEFAULT = "mock_data_discovery.json"

Confidence = Literal["high", "partial", "none"]
_CONFIDENCE_HIGH_FLOOR = 0.75


# -- Sentinel --------------------------------------------------------------


class _Unchanged:
    """Singleton marker used as a default for "leave this field alone"
    parameters. ``None`` means "clear"; ``UNCHANGED`` means "no change".
    Compared by identity."""

    _singleton: "_Unchanged | None" = None

    def __new__(cls):
        if cls._singleton is None:
            cls._singleton = super().__new__(cls)
        return cls._singleton

    def __repr__(self) -> str:
        return "UNCHANGED"

    def __bool__(self) -> bool:
        return False


UNCHANGED = _Unchanged()


# -- Errors ----------------------------------------------------------------


class NotInitializedError(Exception):
    """Raised by ``get_state`` when ``mock_data_config.json`` is absent."""


class ValidationError(ValueError):
    """Raised on invalid input (unknown type, unresolved register, missing
    source/column, etc.). Inherits from ``ValueError`` so structural
    config errors surfaced via ``parse_config`` and editor-side input
    validation share one error type."""


class StaleStateError(Exception):
    """Raised when ``expected_version`` doesn't match the current on-disk
    ``snapshot_version`` — another writer mutated the file. Clients
    re-fetch via ``get_state`` and retry."""


# -- Data models -----------------------------------------------------------


@dataclass(frozen=True)
class EditorWarning:
    """One non-fatal observation surfaced by ``get_state`` (e.g.
    ``discover_drift`` when the discover payload's hash differs from the
    stored ``discover_hash``)."""

    code: str
    message: str
    context: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ColumnInfo:
    """Per-column view rendered for the UI.

    ``provenance`` is derived from the top-level ``manual_columns``
    array — it is *not* a stored per-column field. The bundle's strict
    parser ignores ``manual_columns`` entirely.
    """

    name: str
    sql_type: str | None
    current_type: str
    hint: dict[str, Any] | None
    provenance: Literal["manual", "auto"]
    regmeta_signal: RegmetaSignal | None
    regmeta_implied_type: str | None


@dataclass(frozen=True)
class RegisterGroupView:
    """One register-group rendered for the UI (per principle 7).

    ``group_id`` is ``"reg-<register_id>"`` for assigned, ``"noreg-<source_name>"``
    for each unassigned source (per-source singletons). Stable across
    reads as long as ``register`` is stable on the underlying sources.
    """

    group_id: str
    register_id: int | None
    register_name: str | None
    confidence: Confidence
    sources: tuple[str, ...]
    columns_by_source: dict[str, tuple[ColumnInfo, ...]]
    schema_variants: int
    panel_candidate: Any  # PanelCandidate | None — looser to avoid cycle
    # Per-source seeds for the manual panel editor. Computed server-side
    # via ``detect_panel_member_hints`` so the client doesn't reimplement
    # date-token / time-key-column detection (the previous client code
    # used a naïve ``\d{4}`` regex that missed HT/VT/Q tags and embedded
    # YYYYMM tokens).
    member_hints: dict[str, PanelMemberHints]


@dataclass(frozen=True)
class StateSnapshot:
    """Immutable view of one config + its derived groups.

    ``snapshot_version`` is the SHA-256 of the on-disk config bytes at
    read time. Pass it back as ``expected_version`` on the next mutation;
    a mismatch means another writer touched the file and the mutation
    raises ``StaleStateError`` without writing.
    """

    config: MDWConfig
    groups: tuple[RegisterGroupView, ...]
    discover: dict[str, Any] | None
    warnings: tuple[EditorWarning, ...]
    snapshot_version: str


# -- File IO + concurrency ------------------------------------------------


def _config_path(project_dir: Path) -> Path:
    return Path(project_dir) / CONFIG_FILENAME


def _compute_snapshot_version(path: Path) -> str:
    """SHA-256 of the on-disk config bytes."""
    h = hashlib.sha256()
    with path.open("rb") as fp:
        for chunk in iter(lambda: fp.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _atomic_write(path: Path, payload: dict[str, Any]) -> None:
    """Atomic write: tmp file in the same directory, then ``os.replace``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2, ensure_ascii=False) + "\n"
    fd, tmp = tempfile.mkstemp(prefix=".mdw_config_", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fp:
            fp.write(text)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise


@contextmanager
def _config_lock(project_dir: Path) -> Iterator[None]:
    """Hold an exclusive cross-process lock on a sidecar file for the
    duration of a mutation. ``snapshot_version`` is a CAS token at read
    time only; without serialising the read+write window, two clients
    mutating from the same snapshot can both pass ``_verify_version``
    and the second ``os.replace`` silently clobbers the first. The lock
    closes that window.

    POSIX-only: ``fcntl`` is unavailable on Windows. Lazy-imported here
    so read paths (``get_state``) and the no-mutation branch of
    ``init_if_missing`` remain importable cross-platform; only mutators
    raise on Windows, with a clear message."""
    try:
        import fcntl
    except ModuleNotFoundError as exc:  # pragma: no cover - Windows path
        raise NotImplementedError(
            "mock_data_wizard.editor mutations require POSIX fcntl; "
            "Windows is not supported for the editor write path."
        ) from exc
    project_dir.mkdir(parents=True, exist_ok=True)
    lock_path = project_dir / ".mock_data_config.lock"
    # "a" avoids truncating the (always-empty) sidecar; close() releases the flock.
    with open(lock_path, "a") as fp:
        fcntl.flock(fp.fileno(), fcntl.LOCK_EX)
        yield


def _read_payload(project_dir: Path) -> tuple[dict[str, Any], str]:
    """Read + parse the JSON payload, return ``(payload, snapshot_version)``."""
    path = _config_path(project_dir)
    if not path.exists():
        raise NotInitializedError(
            f"{path} does not exist. Call init_if_missing() with a "
            f"discover payload first."
        )
    with path.open("r", encoding="utf-8") as fp:
        payload = json.load(fp, object_pairs_hook=_reject_duplicate_keys)
    if not isinstance(payload, dict):
        raise ValueError(f"{CONFIG_FILENAME}: top-level value must be an object")
    return payload, _compute_snapshot_version(path)


def _verify_version(project_dir: Path, expected_version: str) -> dict[str, Any]:
    """Read the payload and assert ``expected_version`` matches the current
    on-disk version. Returns the parsed payload."""
    payload, current = _read_payload(project_dir)
    if current != expected_version:
        raise StaleStateError(
            f"snapshot_version mismatch: expected {expected_version!r}, "
            f"on-disk {current!r}. Re-fetch via get_state() and retry."
        )
    return payload


# -- Discover hash --------------------------------------------------------


def _compute_discover_hash(payload: dict[str, Any]) -> str:
    """SHA-256 of ``(source_name, [(col_name, sql_type)])`` tuples,
    sorted on both axes for determinism. ``row_count``, ``nullable``,
    and ``source_detail`` are deliberately excluded — they shift across
    MONA runs without invalidating type overrides."""
    sources = payload.get("sources", [])
    items = []
    for src in sources:
        cols = sorted((c["name"], c.get("sql_type")) for c in src.get("columns", []))
        items.append((src["source_name"], cols))
    items.sort(key=lambda x: x[0])
    blob = json.dumps(items, ensure_ascii=False, sort_keys=False).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def _read_discover(discover_path: Path) -> dict[str, Any]:
    """Read + validate a discover payload."""
    with discover_path.open("r", encoding="utf-8") as fp:
        payload = json.load(fp)
    _validate_discover_payload(payload, str(discover_path))
    return payload


# -- Classification primitives -------------------------------------------


def _classify_columns(
    columns: list[dict[str, Any]],
    register: str | None,
    signals: dict[str, RegmetaSignal],
) -> dict[str, dict[str, Any]]:
    """Classify every column on a single source. Always returns one entry
    per column (dense classification, principle 8); columns that the
    classifier can't otherwise tag default to ``opaque``."""
    out: dict[str, dict[str, Any]] = {}
    for col in columns:
        name = col["name"]
        sql_type = col.get("sql_type")
        signal = lookup_with_prefix_fallback(signals, name)
        out[name] = {"type": _classify(name, sql_type, signal, register)}
    return out


@contextmanager
def _open_regmeta_conn(db_path: Path | None) -> Iterator[Any]:
    """Open a regmeta DB connection. Yields ``None`` when regmeta is
    unavailable or the DB can't be opened — callers handle that branch
    before using the connection. Closes on exit when one was opened."""
    import sqlite3

    try:
        from regmeta import open_db
        from regmeta.db import db_path_from_args
        from regmeta.errors import RegmetaError
    except ImportError:
        yield None
        return
    try:
        resolved = db_path_from_args(str(db_path) if db_path else None)
        conn = open_db(resolved)
    except (FileNotFoundError, OSError, sqlite3.OperationalError, RegmetaError):
        yield None
        return
    try:
        yield conn
    finally:
        conn.close()


def _resolve_signals_for_register(
    register_str: str,
    col_names: set[str],
    db_path: Path | None,
    *,
    relevant_years: set[int] | None = None,
) -> dict[str, RegmetaSignal]:
    """Open regmeta, resolve the register, query column signals.

    Returns ``{}`` when regmeta is unavailable, the register doesn't
    resolve, or no columns match. Editor calls this once per register
    (sources sharing a register batch into one DB call).

    ``relevant_years`` scopes the year-variance counts surfaced on the
    signal — see ``classify._regmeta_lookup``. ``None`` = no filter."""
    with _open_regmeta_conn(db_path) as conn:
        if conn is None:
            return {}
        from regmeta import resolve_register_ids
        from regmeta.errors import RegmetaError

        try:
            register_ids = resolve_register_ids(conn, register_str)
        except RegmetaError:
            return {}
        if not register_ids:
            return {}
        return _regmeta_lookup(
            conn, col_names, register_ids, relevant_years=relevant_years
        )


def _resolve_signals_for_groups(
    register_to_columns: dict[str, set[str]],
    db_path: Path | None,
    *,
    register_to_relevant_years: dict[str, set[int]] | None = None,
) -> dict[str, dict[str, RegmetaSignal]]:
    """Resolve signals for every register in one pass. Returns
    ``{register: {col_lower: RegmetaSignal}}``.

    ``register_to_relevant_years`` maps a register to the project's
    source years for that register's group. ``None`` (or a missing
    register entry) means "no year filter for this register"."""
    out: dict[str, dict[str, RegmetaSignal]] = {}
    for register_str, cols in register_to_columns.items():
        years = (
            register_to_relevant_years.get(register_str)
            if register_to_relevant_years is not None
            else None
        )
        out[register_str] = _resolve_signals_for_register(
            register_str, cols, db_path, relevant_years=years
        )
    return out


def _confidence_for_source(
    register: str | None,
    source_columns: list[dict[str, Any]],
    signals: dict[str, RegmetaSignal],
) -> Confidence:
    """How well does ``register`` cover this source's non-id columns?"""
    if register is None:
        return "none"
    from .classify import is_known_id

    nonid = [c["name"] for c in source_columns if not is_known_id(c["name"])]
    if not nonid:
        return "partial"
    matched = sum(
        1 for c in nonid if lookup_with_prefix_fallback(signals, c) is not None
    )
    rate = matched / len(nonid)
    if rate >= _CONFIDENCE_HIGH_FLOOR:
        return "high"
    if matched > 0:
        return "partial"
    return "none"


def _worst_confidence(a: Confidence, b: Confidence) -> Confidence:
    """A group is only as confident as its weakest source."""
    rank = {"high": 0, "partial": 1, "none": 2}
    return max(a, b, key=rank.__getitem__)


# Review-order tier: lowest rank surfaces first in `_build_groups`'s
# sorted output, so the cards needing attention land at the top.
_REVIEW_RANK: dict[Confidence, int] = {"none": 0, "partial": 1, "high": 2}


def _is_unmatched_categorical(col: ColumnInfo) -> bool:
    """Categorical with no regmeta evidence to back the classification.
    Mirrors ``columnIsUnmatchedCategorical`` in the frontend store; keep
    the two in lockstep — drift here changes both card ordering and the
    "unmatched" filter chip's counts."""
    if col.current_type != "categorical":
        return False
    sig = col.regmeta_signal
    if sig is None:
        return True
    return not sig.classification_short_name and not sig.has_value_codes


def _review_sort_key(group: RegisterGroupView) -> tuple[int, int, str]:
    """Order groups by review need: confidence tier (none → partial →
    high), then descending unmatched-categorical count, then
    register_name (or group_id) ascending for a stable tertiary key."""
    unmatched = sum(
        1
        for cols in group.columns_by_source.values()
        for c in cols
        if _is_unmatched_categorical(c)
    )
    return (
        _REVIEW_RANK[group.confidence],
        -unmatched,
        group.register_name or group.group_id,
    )


# -- Register auto-detection ----------------------------------------------


def _autodetect_register_per_source(
    discover: dict[str, Any], db_path: Path | None
) -> dict[str, str | None]:
    """Pick a register for each source via regmeta voting + filename rules.

    Returns ``{source_name: register_str | None}``. ``register_str`` is
    the registernamn (string) when resolved, ``None`` otherwise. The
    editor stores the chosen register in ``sources[name].register``;
    ``set_group_register`` is the only way to change it after init.
    """
    sources = discover.get("sources", [])
    with _open_regmeta_conn(db_path) as conn:
        if conn is None:
            return {src["source_name"]: None for src in sources}

        from .classify import is_known_id
        from .enrich import (
            _bulk_resolve_all_registers,
            _source_name_register_fallback,
            _vote_register,
        )

        out: dict[str, str | None] = {}
        all_names: set[str] = set()
        for src in sources:
            for col in src.get("columns", []):
                if not is_known_id(col["name"]):
                    all_names.add(col["name"])
        col_to_registers = (
            _bulk_resolve_all_registers(conn, all_names) if all_names else {}
        )

        chosen_ids: dict[str, int | None] = {}
        for src in sources:
            cols = src.get("columns", [])
            nonid = [c["name"] for c in cols if not is_known_id(c["name"])]
            vote = _vote_register(nonid, col_to_registers, src["source_name"])
            chosen_ids[src["source_name"]] = (
                vote.register_id
                if vote.register_id is not None
                else _source_name_register_fallback(src["source_name"])
            )

        # Resolve each unique register_id → registernamn in one query.
        ids_needed = sorted({i for i in chosen_ids.values() if i is not None})
        names: dict[int, str] = {}
        if ids_needed:
            placeholders = ",".join("?" for _ in ids_needed)
            rows = conn.execute(
                "SELECT register_id, registernamn FROM register "
                f"WHERE register_id IN ({placeholders})",
                ids_needed,
            ).fetchall()
            names = {r["register_id"]: r["registernamn"] for r in rows}
        for src in sources:
            rid = chosen_ids[src["source_name"]]
            out[src["source_name"]] = names.get(rid) if rid is not None else None
        return out


# -- Group view assembly --------------------------------------------------


def _resolve_register_id_name(
    register_str: str, db_path: Path | None
) -> tuple[int | None, str | None]:
    """Resolve a register string to ``(id, name)`` or ``(None, None)``."""
    reg = resolve_register(register_str, db_path=db_path)
    if reg is None:
        return (None, None)
    return (reg.id, reg.name)


def _build_groups(
    config: MDWConfig,
    discover: dict[str, Any] | None,
    signals_per_register: dict[str, dict[str, RegmetaSignal]],
    db_path: Path | None,
) -> tuple[RegisterGroupView, ...]:
    """Assemble ``RegisterGroupView`` tuple per principle 7.

    Sources with the same non-null ``register`` group under
    ``reg-<register_id>``; unassigned sources each form a singleton
    ``noreg-<source_name>``.
    """
    discover_sources_by_name: dict[str, dict[str, Any]] = {}
    if discover is not None:
        for src in discover.get("sources", []):
            discover_sources_by_name[src["source_name"]] = src

    # Union: every source that appears in `sources` block OR in
    # `column_types` is groupable. Sources with no metadata but with
    # classified columns still need to show up in the inspector.
    all_sources: list[str] = []
    seen: set[str] = set()
    for source_name in config.sources:
        if source_name not in seen:
            all_sources.append(source_name)
            seen.add(source_name)
    for source_name in config.column_types:
        if source_name not in seen:
            all_sources.append(source_name)
            seen.add(source_name)

    by_register: dict[str, list[str]] = {}
    unassigned: list[str] = []
    for source_name in all_sources:
        register = config.sources.get(source_name, {}).get("register")
        if register:
            by_register.setdefault(register, []).append(source_name)
        else:
            unassigned.append(source_name)

    manual_pairs = set(config.manual_columns)
    groups: list[RegisterGroupView] = []

    def _build_columns(
        source_name: str, signals: dict[str, RegmetaSignal]
    ) -> tuple[ColumnInfo, ...]:
        src = discover_sources_by_name.get(source_name)
        types_for_source = config.column_types.get(source_name, {})
        cols: list[ColumnInfo] = []
        if src is not None:
            for c in src.get("columns", []):
                name = c["name"]
                override = types_for_source.get(name)
                signal = lookup_with_prefix_fallback(signals, name)
                hint = _hint_dict_for(override) if override is not None else None
                cols.append(
                    ColumnInfo(
                        name=name,
                        sql_type=c.get("sql_type"),
                        current_type=override.type if override else "opaque",
                        hint=hint,
                        provenance=(
                            "manual" if (source_name, name) in manual_pairs else "auto"
                        ),
                        regmeta_signal=signal,
                        regmeta_implied_type=regmeta_implied_type(signal),
                    )
                )
        else:
            # Discover absent — synthesise from stored types only.
            for name, override in types_for_source.items():
                cols.append(
                    ColumnInfo(
                        name=name,
                        sql_type=None,
                        current_type=override.type,
                        hint=_hint_dict_for(override),
                        provenance=(
                            "manual" if (source_name, name) in manual_pairs else "auto"
                        ),
                        regmeta_signal=None,
                        regmeta_implied_type=None,
                    )
                )
        return tuple(cols)

    def _schema_variants(columns_by_source: dict[str, tuple[ColumnInfo, ...]]) -> int:
        if not columns_by_source:
            return 0
        seen: set[tuple[tuple[str, str | None], ...]] = set()
        for cols in columns_by_source.values():
            seen.add(tuple((c.name, c.sql_type) for c in cols))
        return len(seen)

    # Assigned-register groups
    for register_str, source_names in by_register.items():
        register_id, register_name = _resolve_register_id_name(register_str, db_path)
        signals = signals_per_register.get(register_str, {})
        columns_by_source: dict[str, tuple[ColumnInfo, ...]] = {}
        worst: Confidence = "high"
        for sn in source_names:
            columns_by_source[sn] = _build_columns(sn, signals)
            src = discover_sources_by_name.get(sn)
            if src is not None:
                worst = _worst_confidence(
                    worst,
                    _confidence_for_source(
                        register_str, src.get("columns", []), signals
                    ),
                )
        # If we have no register_id, the group's confidence is "none"
        # regardless — the user passed a name we couldn't resolve.
        if register_id is None:
            worst = "none"
        sources_by_name = {
            sn: discover_sources_by_name[sn]
            for sn in source_names
            if sn in discover_sources_by_name
        }
        cand = (
            detect_panel_candidate(
                source_names, sources_by_name, register_name=register_name
            )
            if sources_by_name
            else None
        )
        groups.append(
            RegisterGroupView(
                group_id=f"reg-{register_id}"
                if register_id is not None
                else f"reg-unresolved-{register_str}",
                register_id=register_id,
                register_name=register_name,
                confidence=worst,
                sources=tuple(source_names),
                columns_by_source=columns_by_source,
                schema_variants=_schema_variants(columns_by_source),
                panel_candidate=cand,
                member_hints=_member_hints_for(columns_by_source),
            )
        )

    # Unassigned sources — one singleton group per source. Built before
    # the final sort below so noreg groups participate in review-order
    # ranking alongside the register-assigned ones.
    for source_name in unassigned:
        columns_by_source = {source_name: _build_columns(source_name, {})}
        src = discover_sources_by_name.get(source_name)
        cand = (
            detect_panel_candidate([source_name], {source_name: src})
            if src is not None
            else None
        )
        groups.append(
            RegisterGroupView(
                group_id=f"noreg-{source_name}",
                register_id=None,
                register_name=None,
                confidence="none",
                sources=(source_name,),
                columns_by_source=columns_by_source,
                schema_variants=_schema_variants(columns_by_source),
                panel_candidate=cand,
                member_hints=_member_hints_for(columns_by_source),
            )
        )

    return tuple(sorted(groups, key=_review_sort_key))


def _member_hints_for(
    columns_by_source: dict[str, tuple[ColumnInfo, ...]],
) -> dict[str, PanelMemberHints]:
    """Per-source seeds for the manual panel editor."""
    return {
        sn: detect_panel_member_hints(sn, tuple(c.name for c in cols))
        for sn, cols in columns_by_source.items()
    }


def _hint_dict_for(override: ColumnTypeOverride) -> dict[str, Any] | None:
    """Project a ``ColumnTypeOverride`` into the editor's ``hint`` shape
    or None if no inline hint is set."""
    if not override.has_inline_hint():
        return None
    out: dict[str, Any] = {}
    if override.id_subtype is not None:
        out["id_subtype"] = override.id_subtype
    if override.numeric_subtype is not None:
        out["numeric_subtype"] = override.numeric_subtype
    if override.date_format is not None:
        out["date_format"] = override.date_format
    return out or None


# -- Snapshot assembly ---------------------------------------------------


def _build_snapshot(
    project_dir: Path,
    payload: dict[str, Any],
    discover: dict[str, Any] | None,
    snapshot_version: str,
    db_path: Path | None,
) -> StateSnapshot:
    try:
        config = parse_config(payload)
    except ValueError as exc:
        # Surface structural / version errors through the editor's own
        # error type so UI clients have one Exception to catch.
        raise ValidationError(str(exc)) from exc
    register_to_columns: dict[str, set[str]] = {}
    register_to_relevant_years: dict[str, set[int]] = {}
    if discover is not None:
        sources_by_name = {s["source_name"]: s for s in discover.get("sources", [])}
        for source_name, entry in config.sources.items():
            register = entry.get("register")
            if not register:
                continue
            src = sources_by_name.get(source_name)
            if src is None:
                continue
            register_to_columns.setdefault(register, set()).update(
                c["name"] for c in src.get("columns", [])
            )
            year = entry.get("year")
            if isinstance(year, int):
                register_to_relevant_years.setdefault(register, set()).add(year)
    else:
        # No discover: still need to expose register groupings using
        # whatever column_types we have.
        for source_name, entry in config.sources.items():
            register = entry.get("register")
            if not register:
                continue
            cols = config.column_types.get(source_name, {}).keys()
            register_to_columns.setdefault(register, set()).update(cols)
            year = entry.get("year")
            if isinstance(year, int):
                register_to_relevant_years.setdefault(register, set()).add(year)

    # Year filter only applies when every source in a register-group
    # carries a configured year: a partial set would silently drop
    # variants the user never opted out of.
    scope_for_signals: dict[str, set[int]] = {}
    for reg, years in register_to_relevant_years.items():
        cols = register_to_columns.get(reg)
        if not cols:
            continue
        sources_for_reg = [
            sn for sn, e in config.sources.items() if e.get("register") == reg
        ]
        if sources_for_reg and all(
            isinstance(config.sources[sn].get("year"), int) for sn in sources_for_reg
        ):
            scope_for_signals[reg] = years

    signals = _resolve_signals_for_groups(
        register_to_columns,
        db_path,
        register_to_relevant_years=scope_for_signals,
    )
    groups = _build_groups(config, discover, signals, db_path)

    warnings: list[EditorWarning] = []
    if discover is not None and config.discover_hash is not None:
        current_hash = _compute_discover_hash(discover)
        if current_hash != config.discover_hash:
            warnings.append(
                EditorWarning(
                    code="discover_drift",
                    message=(
                        "discover payload differs from the snapshot the config "
                        "was authored against; column or source set may have "
                        "changed since."
                    ),
                    context={
                        "stored_hash": config.discover_hash,
                        "current_hash": current_hash,
                    },
                )
            )

    return StateSnapshot(
        config=config,
        groups=groups,
        discover=discover,
        warnings=tuple(warnings),
        snapshot_version=snapshot_version,
    )


# -- Public API: read ----------------------------------------------------


def get_state(
    project_dir: Path,
    *,
    discover_path: Path | None = None,
    db_path: Path | None = None,
) -> StateSnapshot:
    """Read the config, compute a snapshot.

    Raises ``NotInitializedError`` when ``mock_data_config.json`` doesn't
    exist. When ``discover_path`` is None, defaults to
    ``project_dir/mock_data_discovery.json``; if that file is also
    absent, succeeds with ``discover=None`` and skips drift detection.
    """
    project_dir = Path(project_dir)
    payload, snapshot_version = _read_payload(project_dir)
    discover: dict[str, Any] | None
    if discover_path is None:
        discover = _load_local_discover(project_dir)
    else:
        discover = _read_discover(Path(discover_path))
    return _build_snapshot(project_dir, payload, discover, snapshot_version, db_path)


# -- Public API: init ----------------------------------------------------


def init_if_missing(
    project_dir: Path,
    discover_path: Path,
    *,
    db_path: Path | None = None,
    overwrite: bool = False,
) -> StateSnapshot:
    """Create an initial config from a discover payload.

    When the config already exists and ``overwrite`` is False, returns
    ``get_state(project_dir, discover_path=discover_path, db_path=db_path)``
    — idempotent. When ``overwrite`` is True, re-runs auto-detection
    and re-classifies every column from scratch (manual overrides are
    lost since the file is being rewritten).
    """
    project_dir = Path(project_dir)
    discover_path = Path(discover_path)
    target = _config_path(project_dir)
    # Common case: already initialized — read-only path, no lock needed.
    if target.exists() and not overwrite:
        return get_state(project_dir, discover_path=discover_path, db_path=db_path)
    with _config_lock(project_dir):
        # Re-check after acquiring: another writer may have just initialized.
        if target.exists() and not overwrite:
            return get_state(project_dir, discover_path=discover_path, db_path=db_path)
        discover = _read_discover(discover_path)
        if not discover["sources"]:
            raise ValidationError(
                f"{discover_path} has no sources -- nothing to configure."
            )

        register_per_source = _autodetect_register_per_source(discover, db_path)
        register_to_columns: dict[str, set[str]] = {}
        sources_by_name = {s["source_name"]: s for s in discover["sources"]}
        for source_name, register in register_per_source.items():
            if register is None:
                continue
            src = sources_by_name[source_name]
            register_to_columns.setdefault(register, set()).update(
                c["name"] for c in src.get("columns", [])
            )
        signals_per_register = _resolve_signals_for_groups(register_to_columns, db_path)

        column_types: dict[str, dict[str, dict[str, Any]]] = {}
        sources_out: dict[str, dict[str, Any]] = {}
        panels_out: list[dict[str, Any]] = []
        panel_sources_seen: set[str] = set()

        for src in discover["sources"]:
            source_name = src["source_name"]
            register = register_per_source.get(source_name)
            signals = signals_per_register.get(register, {}) if register else {}
            column_types[source_name] = _classify_columns(
                src.get("columns", []), register, signals
            )
            meta: dict[str, Any] = {}
            year = src.get("source_detail", {}).get("year")
            if year is None:
                year = detect_year_from_source_name(source_name)
            if year is not None:
                meta["year"] = int(year)
            if register is not None:
                meta["register"] = register
            if meta:
                sources_out[source_name] = meta

        # Auto-apply unambiguous panel candidates per register-group.
        by_register: dict[str | None, list[str]] = {}
        for source_name, register in register_per_source.items():
            by_register.setdefault(register, []).append(source_name)
        for register, source_names in by_register.items():
            if register is None:
                # Unassigned sources don't get cross-source panel candidates.
                continue
            cand = detect_panel_candidate(source_names, sources_by_name)
            if cand is None or cand.suggested_entity_key is None:
                continue
            if any(m["source"] in panel_sources_seen for m in cand.members):
                continue
            panel_id = cand.suggested_panel_id or f"reg-{register}-panel"
            panels_out.append(
                {
                    "panel_id": panel_id,
                    "entity_key": cand.suggested_entity_key,
                    "members": [dict(m) for m in cand.members],
                }
            )
            for m in cand.members:
                panel_sources_seen.add(m["source"])

        payload: dict[str, Any] = {
            "contract_version": SCHEMA_VERSION,
            "discover_hash": _compute_discover_hash(discover),
            "column_types": column_types,
        }
        if sources_out:
            payload["sources"] = sources_out
        if panels_out:
            payload["panels"] = panels_out
        _atomic_write(target, payload)
        snapshot_version = _compute_snapshot_version(target)
    return _build_snapshot(project_dir, payload, discover, snapshot_version, db_path)


# -- Public API: mutators ------------------------------------------------


def _validate_hint(new_type: str, hint: dict[str, Any] | None) -> dict[str, Any] | None:
    """Validate a hint dict against ``INLINE_HINT_KEYS[new_type]`` and
    return a fresh dict (or None). Raises ``ValidationError`` on
    unknown keys or invalid values."""
    if hint is None:
        return None
    if not isinstance(hint, dict):
        raise ValidationError(f"hint must be a dict or None, got {type(hint).__name__}")
    allowed = set(INLINE_HINT_KEYS[new_type])
    extra = set(hint) - allowed
    if extra:
        raise ValidationError(
            f"hint key(s) {sorted(extra)} not valid for type={new_type!r} "
            f"(allowed: {sorted(allowed)})"
        )
    out: dict[str, Any] = {}
    for k, v in hint.items():
        if k == "id_subtype" and v not in VALID_ID_SUBTYPES:
            raise ValidationError(
                f"hint.id_subtype={v!r}, expected one of {VALID_ID_SUBTYPES}"
            )
        if k == "numeric_subtype" and v not in VALID_NUMERIC_SUBTYPES:
            raise ValidationError(
                f"hint.numeric_subtype={v!r}, expected one of {VALID_NUMERIC_SUBTYPES}"
            )
        if k == "date_format" and not isinstance(v, str):
            raise ValidationError(
                f"hint.date_format must be a string, got {type(v).__name__}"
            )
        out[k] = v
    return out or None


def _existing_hint_keys_valid_for(
    type_entry: dict[str, Any], new_type: str
) -> dict[str, Any]:
    """Filter ``type_entry``'s inline hint keys to those still valid
    under ``new_type``. Returns a dict containing only the survivors;
    used for ``UNCHANGED`` semantics on type changes."""
    allowed = set(INLINE_HINT_KEYS[new_type])
    return {k: v for k, v in type_entry.items() if k != "type" and k in allowed}


def _load_local_discover(project_dir: Path) -> dict[str, Any] | None:
    """Read+validate ``mock_data_discovery.json`` next to the config, or
    return None when the file is absent or malformed."""
    candidate = project_dir / DISCOVER_FILENAME_DEFAULT
    try:
        return _read_discover(candidate)
    except (FileNotFoundError, ValueError):
        return None


def _discover_sources_index(
    project_dir: Path,
) -> dict[str, dict[str, Any]] | None:
    """Load discover once and index by ``source_name``. Returns ``None``
    when discover is absent or unreadable — callers fall back to
    ``payload["column_types"]`` keys."""
    discover = _load_local_discover(project_dir)
    if discover is None:
        return None
    return {s["source_name"]: s for s in discover.get("sources", [])}


def _assert_column_in_discover(
    sources_by_name: dict[str, dict[str, Any]] | None,
    payload: dict[str, Any],
    source_name: str,
    column_name: str,
) -> None:
    if sources_by_name is not None:
        src = sources_by_name.get(source_name)
        found = src is not None and any(
            c["name"] == column_name for c in src.get("columns", [])
        )
    else:
        found = column_name in payload.get("column_types", {}).get(source_name, {})
    if not found:
        raise ValidationError(
            f"({source_name!r}, {column_name!r}) not found in discover or "
            f"existing column_types — refusing to silently create entries."
        )


def _drop_column_options(
    column_options: dict[str, dict[str, Any]] | None,
    source_name: str,
    column_name: str,
) -> None:
    """Drop the ``column_options`` entry for one cell, pruning the empty
    source dict when it was the last column. Called whenever a cell's
    type changes — options can be type-specific."""
    if not column_options:
        return
    opts = column_options.get(source_name)
    if opts and column_name in opts:
        opts.pop(column_name, None)
        if not opts:
            column_options.pop(source_name, None)


def _assert_source_in_discover(
    sources_by_name: dict[str, dict[str, Any]] | None,
    payload: dict[str, Any],
    source_name: str,
) -> None:
    """Reject unknown ``source_name`` so a typo can't silently create a
    phantom source entry. Falls back to ``column_types``/``sources`` keys
    when the discover payload is absent."""
    if sources_by_name is not None:
        known = set(sources_by_name)
    else:
        known = set(payload.get("column_types", {}).keys()) | set(
            payload.get("sources", {}).keys()
        )
    if source_name not in known:
        raise ValidationError(
            f"source_name={source_name!r} not found in discover or existing "
            f"config — refusing to silently create entries."
        )


def set_column_type(
    project_dir: Path,
    source_names: Sequence[str],
    column_name: str,
    new_type: str,
    *,
    expected_version: str,
    hint: dict[str, Any] | None | _Unchanged = UNCHANGED,
    db_path: Path | None = None,
) -> StateSnapshot:
    """Set or override the type of ``column_name`` across one or more
    sources. See ``DESIGN.md`` § Editor API.

    Bulk semantics: every ``(sn, column_name)`` pair is validated against
    discover before any write; a single bad pair aborts the whole call
    with no on-disk changes. Per-source updates happen under one
    ``_config_lock`` and one ``_atomic_write``, so the snapshot version
    advances exactly once and clients can't observe a partial apply.

    ``hint`` semantics: ``UNCHANGED`` preserves any existing hint *if*
    it remains valid for ``new_type`` (silently dropped otherwise);
    ``None`` clears any hint; a dict sets it (validated). The hint is
    validated once and applied identically to every targeted source.
    """
    if new_type not in VALID_COLUMN_TYPES:
        raise ValidationError(
            f"new_type={new_type!r}, expected one of {VALID_COLUMN_TYPES}"
        )
    # `str` and `bytes` satisfy `Sequence[str]` structurally, so without
    # this guard `list("src")` would silently produce `['s', 'r', 'c']`.
    # Fail loudly instead of running per-character validation.
    if isinstance(source_names, (str, bytes)):
        raise ValidationError(
            f"source_names must be a sequence of source names, not a single "
            f"{type(source_names).__name__}; pass [source_name] for one source."
        )
    sources_list = list(source_names)
    if not sources_list:
        raise ValidationError("source_names must be non-empty")
    if len(set(sources_list)) != len(sources_list):
        raise ValidationError(f"source_names contains duplicates: {sources_list}")

    project_dir = Path(project_dir)
    with _config_lock(project_dir):
        payload = _verify_version(project_dir, expected_version)
        # Validate every pair before mutating anything; bulk writes are
        # all-or-nothing. Load discover once — _assert_column_in_discover
        # would otherwise reopen the file per source.
        sources_by_name = _discover_sources_index(project_dir)
        for sn in sources_list:
            _assert_column_in_discover(sources_by_name, payload, sn, column_name)

        # Validate the hint once: same input, same result for every source.
        if isinstance(hint, _Unchanged):
            shared_validated_hint: dict[str, Any] | None | _Unchanged = UNCHANGED
        else:
            shared_validated_hint = _validate_hint(new_type, hint)

        column_types = payload.setdefault("column_types", {})
        column_options = payload.get("column_options")
        manual = payload.get("manual_columns", [])
        dirty = False

        for sn in sources_list:
            cols = column_types.setdefault(sn, {})
            existing = cols.get(column_name, {})
            old_type = existing.get("type")
            if isinstance(shared_validated_hint, _Unchanged):
                new_entry: dict[str, Any] = {"type": new_type}
                new_entry.update(_existing_hint_keys_valid_for(existing, new_type))
            else:
                new_entry = {"type": new_type}
                if shared_validated_hint:
                    new_entry.update(shared_validated_hint)

            # Re-asserting same type+hint shouldn't promote auto→manual
            # (cancel-by-mistake on Save is common). Already-manual cells
            # stay manual — their pair is already in `manual`.
            if existing == new_entry:
                continue

            cols[column_name] = new_entry
            dirty = True

            if old_type is not None and old_type != new_type:
                _drop_column_options(column_options, sn, column_name)

            pair = [sn, column_name]
            if pair not in manual:
                manual.append(pair)

        if column_options is not None and not column_options:
            payload.pop("column_options", None)
        if manual:
            payload["manual_columns"] = manual

        if dirty:
            _atomic_write(_config_path(project_dir), payload)
    return get_state(project_dir, db_path=db_path)


def unset_column_manual_override(
    project_dir: Path,
    source_names: Sequence[str],
    column_name: str,
    *,
    expected_version: str,
    db_path: Path | None = None,
) -> StateSnapshot:
    """Drop manual-override markers for ``column_name`` across one or more
    sources and re-classify those cells from scratch. Pairs that aren't
    currently in ``manual_columns`` are silently skipped — the contract
    is "ensure not manual", not "fail when not manual".

    Bulk semantics mirror ``set_column_type``: every ``(sn, column_name)``
    pair is validated against discover before any write; the snapshot
    advances exactly once. ``column_options`` for the cell is dropped
    when the new auto type differs from the existing one (mirrors the
    set_group_register reclassify path)."""
    # `str` and `bytes` satisfy `Sequence[str]` structurally, so without
    # this guard `list("src")` would silently produce `['s', 'r', 'c']`.
    if isinstance(source_names, (str, bytes)):
        raise ValidationError(
            f"source_names must be a sequence of source names, not a single "
            f"{type(source_names).__name__}; pass [source_name] for one source."
        )
    sources_list = list(source_names)
    if not sources_list:
        raise ValidationError("source_names must be non-empty")
    if len(set(sources_list)) != len(sources_list):
        raise ValidationError(f"source_names contains duplicates: {sources_list}")

    project_dir = Path(project_dir)
    with _config_lock(project_dir):
        payload = _verify_version(project_dir, expected_version)

        # Load discover once: reused for validation AND reclassify below.
        # Re-reading would open a TOCTOU window where a source visible at
        # validation time vanishes before classification, leading to a
        # partial apply (some markers cleared, others left stale).
        sources_by_name = _discover_sources_index(project_dir)
        for sn in sources_list:
            _assert_column_in_discover(sources_by_name, payload, sn, column_name)

        manual_pairs = {tuple(p) for p in payload.get("manual_columns", [])}
        to_clear = [sn for sn in sources_list if (sn, column_name) in manual_pairs]
        if not to_clear:
            return get_state(project_dir, db_path=db_path)

        # Reclassify needs the discover schema; bail loudly if it's gone.
        if sources_by_name is None:
            raise ValidationError(
                f"unset_column_manual_override requires {DISCOVER_FILENAME_DEFAULT} "
                f"next to the config to re-classify the column."
            )

        sources_block = payload.setdefault("sources", {})
        column_types = payload.setdefault("column_types", {})
        column_options = payload.get("column_options")

        # Group by register so each register's signals batch into one DB hit.
        by_register: dict[str | None, list[str]] = {}
        for sn in to_clear:
            register_str = sources_block.get(sn, {}).get("register") or None
            by_register.setdefault(register_str, []).append(sn)

        for register_str, group_sources in by_register.items():
            signals = (
                _resolve_signals_for_register(register_str, {column_name}, db_path)
                if register_str is not None
                else {}
            )
            for sn in group_sources:
                # Validation above proved the pair exists in our discover
                # snapshot, so direct indexing is safe here.
                src = sources_by_name[sn]
                src_columns = [
                    c for c in src.get("columns", []) if c["name"] == column_name
                ]
                classified = _classify_columns(src_columns, register_str, signals)
                new_entry = classified[column_name]
                cols = column_types.setdefault(sn, {})
                old_type = cols.get(column_name, {}).get("type")
                cols[column_name] = new_entry
                if old_type is not None and old_type != new_entry["type"]:
                    _drop_column_options(column_options, sn, column_name)
                manual_pairs.discard((sn, column_name))

        if column_options is not None and not column_options:
            payload.pop("column_options", None)
        # Sort for deterministic JSON output (matches set_group_register).
        payload["manual_columns"] = [list(p) for p in sorted(manual_pairs)]

        _atomic_write(_config_path(project_dir), payload)
    return get_state(project_dir, db_path=db_path)


def set_source_registers(
    project_dir: Path,
    assignments: dict[str, str | None],
    *,
    expected_version: str,
    db_path: Path | None = None,
    reclassify_manual: bool = False,
) -> StateSnapshot:
    """Assign or clear ``register`` for one or more sources atomically.

    ``assignments`` maps source_name → register name (or ``None`` to
    clear). Every source must already exist in the config. Each non-null
    register is resolved against regmeta — an unknown name aborts the
    whole call before any write. Sources whose register actually changes
    are re-classified (auto-only by default; manuals included when
    ``reclassify_manual=True``). Type changes drop the affected cell's
    ``column_options`` entry (mirrors the historical
    ``set_group_register`` behaviour).

    When ``reclassify_manual=True`` and a source's register is
    unchanged, the source is still re-classified — the flag means
    "force reclassification on these sources", matching
    ``set_group_register``'s contract for the same flag. The flag's
    only observable effect is dropping manual overrides, so if no
    manual_columns intersect the requested sources and no register
    moves, the call is treated as a no-op (``snapshot_version`` stays
    stable).

    Atomic: validation runs in full before any mutation, and the snapshot
    advances exactly once."""
    if not isinstance(assignments, dict):
        raise ValidationError(
            f"assignments must be a dict, got {type(assignments).__name__}"
        )
    if not assignments:
        raise ValidationError("assignments must be non-empty")
    for sn, val in assignments.items():
        if not isinstance(sn, str):
            raise ValidationError(
                f"assignments keys must be strings, got {type(sn).__name__}"
            )
        if val is not None and not isinstance(val, str):
            raise ValidationError(
                f"assignments[{sn!r}] must be a string or None, "
                f"got {type(val).__name__}"
            )

    project_dir = Path(project_dir)
    with _config_lock(project_dir):
        payload = _verify_version(project_dir, expected_version)

        # Validate every source exists. Reject unknowns up front so a
        # typo can't half-apply a batch (the lock is held; rolling back
        # mid-batch would require duplicating the read).
        known_sources = set(payload.get("sources", {}).keys()) | set(
            payload.get("column_types", {}).keys()
        )
        for sn in assignments:
            if sn not in known_sources:
                raise ValidationError(
                    f"source_name={sn!r} not found in current snapshot."
                )

        # Resolve each non-null register against regmeta. Cache by input
        # value so a panel-wide batch (many sources sharing one register
        # name) is one DB hit per distinct name, not one per source.
        resolve_cache: dict[str, str] = {}
        resolved: dict[str, str | None] = {}
        for sn, val in assignments.items():
            if val is None:
                resolved[sn] = None
                continue
            if val in resolve_cache:
                resolved[sn] = resolve_cache[val]
                continue
            reg = resolve_register(val, db_path=db_path)
            if reg is None:
                raise ValidationError(f"register={val!r} did not resolve in regmeta.")
            resolve_cache[val] = reg.name
            resolved[sn] = reg.name

        sources_block = payload.setdefault("sources", {})

        # Per-source: did the register actually change? Normalise legacy
        # `""` entries to None so a "set to empty" assignment on a cleared
        # source isn't falsely reported as a change.
        register_changed: dict[str, bool] = {}
        for sn, new_register in resolved.items():
            current = sources_block.get(sn, {}).get("register") or None
            register_changed[sn] = current != new_register

        # ``reclassify_manual=True`` has no observable effect when no
        # manual overrides intersect the requested sources — the only
        # thing the flag changes is whether manuals get dropped. Treat
        # it as effectively False in that case so an idle re-submit
        # doesn't churn snapshot_version.
        manual_pairs_initial = {tuple(p) for p in payload.get("manual_columns", [])}
        effective_reclassify_manual = reclassify_manual and any(
            sn in assignments for (sn, _c) in manual_pairs_initial
        )

        # Reclassify a source if its register changes OR the caller asked
        # for forced reclassification (the same rule the wrapper relied
        # on for the old "reclassify-only" path on set_group_register).
        to_reclassify = [
            sn
            for sn in assignments
            if register_changed[sn] or effective_reclassify_manual
        ]
        any_change = any(register_changed.values()) or effective_reclassify_manual
        if not any_change:
            # Nothing would move: skip the write so the snapshot version
            # stays stable across no-op submits (matches set_column_type).
            return get_state(project_dir, db_path=db_path)

        # Discover is required whenever we reclassify; defer the check
        # to the reclassify branch so a pure no-op (handled above) does
        # not force a discover file to exist.
        if to_reclassify:
            discover_path = project_dir / DISCOVER_FILENAME_DEFAULT
            if not discover_path.exists():
                raise ValidationError(
                    f"set_source_registers requires {DISCOVER_FILENAME_DEFAULT} "
                    f"next to the config to re-classify columns."
                )
            discover = _read_discover(discover_path)
            sources_by_name = {s["source_name"]: s for s in discover.get("sources", [])}
        else:
            sources_by_name = {}

        # Write the new register on each source whose value changed.
        for sn, new_register in resolved.items():
            if not register_changed[sn]:
                continue
            entry = sources_block.setdefault(sn, {})
            if new_register is None:
                entry.pop("register", None)
            else:
                entry["register"] = new_register

        # Group reclassify sources by their (now-current) register so
        # signals are queried once per distinct register, not per source.
        by_register: dict[str | None, list[str]] = {}
        for sn in to_reclassify:
            by_register.setdefault(resolved[sn], []).append(sn)

        # Working copy of manual_pairs — `manual_pairs_initial` is the
        # pre-loop snapshot we used to compute effective_reclassify_manual.
        manual_pairs = set(manual_pairs_initial)
        column_types = payload.setdefault("column_types", {})
        column_options = payload.get("column_options", {})

        for register_str, group_sources in by_register.items():
            if register_str is not None:
                all_cols: set[str] = set()
                for sn in group_sources:
                    src = sources_by_name.get(sn)
                    if src is None:
                        continue
                    for c in src.get("columns", []):
                        all_cols.add(c["name"])
                signals = (
                    _resolve_signals_for_register(register_str, all_cols, db_path)
                    if all_cols
                    else {}
                )
            else:
                signals = {}

            for sn in group_sources:
                src = sources_by_name.get(sn)
                if src is None:
                    continue
                new_cols = _classify_columns(
                    src.get("columns", []), register_str, signals
                )
                existing_cols = column_types.get(sn, {})
                out_cols: dict[str, dict[str, Any]] = {}
                for col_name, classified in new_cols.items():
                    is_manual = (sn, col_name) in manual_pairs
                    if is_manual and not effective_reclassify_manual:
                        # Preserve manual override exactly.
                        if col_name in existing_cols:
                            out_cols[col_name] = existing_cols[col_name]
                        else:
                            out_cols[col_name] = classified
                        continue
                    if effective_reclassify_manual and is_manual:
                        manual_pairs.discard((sn, col_name))
                    old_type = existing_cols.get(col_name, {}).get("type")
                    new_type = classified["type"]
                    out_cols[col_name] = classified
                    if old_type is not None and old_type != new_type:
                        _drop_column_options(column_options, sn, col_name)
                column_types[sn] = out_cols

        if effective_reclassify_manual:
            # Sort for deterministic JSON output — manual_pairs is a set,
            # so insertion order isn't preserved and would otherwise
            # produce noisy diffs across runs.
            payload["manual_columns"] = [list(p) for p in sorted(manual_pairs)]
        if column_options:
            payload["column_options"] = column_options
        elif "column_options" in payload:
            # Empty dict left over from drops — keep schema clean.
            del payload["column_options"]

        _atomic_write(_config_path(project_dir), payload)
    return get_state(project_dir, db_path=db_path)


def set_group_register(
    project_dir: Path,
    group_id: str,
    register: str | None,
    *,
    expected_version: str,
    db_path: Path | None = None,
    reclassify_manual: bool = False,
) -> StateSnapshot:
    """Assign or clear a register for a group, then reclassify.

    Thin wrapper over ``set_source_registers``: resolves ``group_id``
    against the current snapshot and forwards an all-same assignment.
    See ``set_source_registers`` for the reclassify and column_options
    contract."""
    project_dir = Path(project_dir)
    # Resolve group_id without holding the lock — the primitive's
    # _verify_version catches any concurrent mutation. flock would
    # deadlock if we re-entered the lock from set_source_registers in
    # the same process (locks are per-fd).
    payload, _ = _read_payload(project_dir)
    try:
        config = parse_config(payload)
    except ValueError as exc:
        raise ValidationError(str(exc)) from exc
    affected_sources: list[str] = _sources_in_group(config, group_id, db_path)
    if not affected_sources:
        raise ValidationError(f"group_id={group_id!r} not found in current snapshot.")
    assignments: dict[str, str | None] = {sn: register for sn in affected_sources}
    return set_source_registers(
        project_dir,
        assignments,
        expected_version=expected_version,
        reclassify_manual=reclassify_manual,
        db_path=db_path,
    )


def _sources_in_group(
    config: MDWConfig, group_id: str, db_path: Path | None = None
) -> list[str]:
    """Reverse the group_id naming scheme to the underlying source list."""
    if group_id.startswith("noreg-"):
        sn = group_id[len("noreg-") :]
        entry = config.sources.get(sn)
        # A source with a register assigned does not belong to a noreg
        # group, even if the caller passed a stale group_id.
        if entry is not None and entry.get("register"):
            return []
        if entry is not None:
            return [sn]
        # No `sources` entry: surface the source if column_types carries
        # it (dense classification means it usually does).
        return [sn] if sn in config.column_types else []
    if group_id.startswith("reg-"):
        rest = group_id[len("reg-") :]
        if rest.startswith("unresolved-"):
            target = rest[len("unresolved-") :]
            return [
                sn
                for sn, entry in config.sources.items()
                if entry.get("register") == target
            ]
        # Numeric register_id: resolve each unique register string once,
        # then match. A project with N sources sharing M registers does
        # M lookups, not N.
        try:
            target_id = int(rest)
        except ValueError:
            return []
        unique_registers = {
            entry.get("register")
            for entry in config.sources.values()
            if entry.get("register")
        }
        register_ids: dict[str, int | None] = {}
        for register in unique_registers:
            reg = resolve_register(register, db_path=db_path)
            register_ids[register] = reg.id if reg is not None else None
        return [
            sn
            for sn, entry in config.sources.items()
            if register_ids.get(entry.get("register")) == target_id
        ]
    return []


def set_source_years(
    project_dir: Path,
    assignments: dict[str, int | None],
    *,
    expected_version: str,
    db_path: Path | None = None,
) -> StateSnapshot:
    """Bulk-update ``year`` on multiple sources atomically.

    ``assignments`` maps ``source_name`` to either:

    - ``int`` — set/replace the year for this source.
    - ``None`` — **delete** the year key entirely. Sends the row back
      to the "missing" state (the editor UI's warning resurfaces on
      next read). Note: legacy on-disk entries with ``"year": null``
      (configs that pre-date this change) are still readable by the
      bundle and parser; nothing emits them anymore.

    Every source must exist in the current snapshot; unknown sources
    abort the whole call before any on-disk write. A fully-no-op call
    leaves ``snapshot_version`` unchanged.
    """
    if not isinstance(assignments, dict):
        raise ValidationError(
            f"assignments must be a dict, got {type(assignments).__name__}"
        )
    if not assignments:
        raise ValidationError("assignments must be non-empty")
    for sn, val in assignments.items():
        if not isinstance(sn, str) or not sn:
            raise ValidationError(
                f"assignments keys must be non-empty strings, got {sn!r}"
            )
        if val is None:
            continue
        if isinstance(val, bool) or not isinstance(val, int):
            raise ValidationError(
                f"assignments[{sn!r}] must be int or None, got {type(val).__name__}"
            )

    project_dir = Path(project_dir)
    with _config_lock(project_dir):
        payload = _verify_version(project_dir, expected_version)
        discover_index = _discover_sources_index(project_dir)
        for sn in assignments:
            _assert_source_in_discover(discover_index, payload, sn)

        sources = payload.setdefault("sources", {})
        any_change = False
        for sn, year in assignments.items():
            entry = sources.setdefault(sn, {})
            had_year = "year" in entry
            old_year = entry.get("year") if had_year else None
            if year is None:
                if not had_year:
                    continue
                entry.pop("year", None)
                any_change = True
            else:
                if had_year and old_year == year:
                    continue
                entry["year"] = year
                any_change = True
        # Prune sources entries that ended up entirely empty (no `year`
        # and no `register`) so JSON output stays clean.
        for sn in list(sources):
            if not sources[sn]:
                sources.pop(sn)
        if not any_change:
            return get_state(project_dir, db_path=db_path)
        _atomic_write(_config_path(project_dir), payload)
    return get_state(project_dir, db_path=db_path)


def set_column_options(
    project_dir: Path,
    source_name: str,
    column_name: str,
    options: dict[str, Any] | None,
    *,
    expected_version: str,
    db_path: Path | None = None,
) -> StateSnapshot:
    """Set or clear ``column_options`` for one column."""
    project_dir = Path(project_dir)
    with _config_lock(project_dir):
        payload = _verify_version(project_dir, expected_version)
        _assert_column_in_discover(
            _discover_sources_index(project_dir), payload, source_name, column_name
        )
        column_options = payload.setdefault("column_options", {})
        if options is None:
            cols = column_options.get(source_name)
            if cols and column_name in cols:
                cols.pop(column_name)
                if not cols:
                    column_options.pop(source_name, None)
        else:
            try:
                validated = _parse_options(source_name, column_name, options)
            except ValueError as exc:
                raise ValidationError(str(exc)) from exc
            cols = column_options.setdefault(source_name, {})
            cols[column_name] = validated
        if not column_options:
            payload.pop("column_options", None)
        _atomic_write(_config_path(project_dir), payload)
    return get_state(project_dir, db_path=db_path)


def parse_panel_payload(raw: dict[str, Any]) -> Panel:
    """Parse a wire-shape ``{panel_id, panel_key, members}`` dict into a
    ``Panel``. Reuses ``config._parse_panel`` so the wire format and the
    on-disk JSON share one validator. Raises ``ValidationError`` on any
    structural problem."""
    try:
        return _parse_panel(raw, idx=0)
    except ValueError as exc:
        raise ValidationError(str(exc)) from exc


def put_panel(
    project_dir: Path,
    panel: Panel,
    *,
    expected_version: str,
    db_path: Path | None = None,
    previous_panel_id: str | None = None,
) -> StateSnapshot:
    """Add or replace a panel by ``panel_id``.

    ``previous_panel_id`` supports rename: when set and different from
    ``panel.panel_id``, the renamed-from entry is dropped first so it
    doesn't collide with the new entry on member-source overlap during
    ``parse_config`` validation.
    """
    project_dir = Path(project_dir)
    with _config_lock(project_dir):
        payload = _verify_version(project_dir, expected_version)
        panels = payload.setdefault("panels", [])
        if previous_panel_id is not None and previous_panel_id != panel.panel_id:
            panels[:] = [p for p in panels if p.get("panel_id") != previous_panel_id]
        serialized = panel_to_dict(panel)
        replaced = False
        for i, existing in enumerate(panels):
            if existing.get("panel_id") == panel.panel_id:
                panels[i] = serialized
                replaced = True
                break
        if not replaced:
            panels.append(serialized)
        # Validate the resulting payload via parse_config to catch source
        # collisions and other integrity errors.
        try:
            parse_config(payload)
        except ValueError as exc:
            raise ValidationError(str(exc)) from exc
        _atomic_write(_config_path(project_dir), payload)
    return get_state(project_dir, db_path=db_path)


def remove_panel(
    project_dir: Path,
    panel_id: str,
    *,
    expected_version: str,
    db_path: Path | None = None,
) -> StateSnapshot:
    """Remove a panel by id. No-op when the id is absent."""
    project_dir = Path(project_dir)
    with _config_lock(project_dir):
        payload = _verify_version(project_dir, expected_version)
        panels = payload.get("panels", [])
        payload["panels"] = [p for p in panels if p.get("panel_id") != panel_id]
        if not payload["panels"]:
            payload.pop("panels", None)
        _atomic_write(_config_path(project_dir), payload)
    return get_state(project_dir, db_path=db_path)


# -- Public API: read-only regmeta lookups -------------------------------


@dataclass(frozen=True)
class ColumnValue:
    """One code/label pair, used by ``get_column_values`` results."""

    code: str
    label: str | None


VarianceTier = Literal["1", "2", "3a", "3b"]


_NOTE_TIER_2 = (
    "Value code sets differ by period. Showing the union — pick a specific "
    "value-set below for year-correct codes."
)
_NOTE_TIER_3A = (
    "Some codes have different meanings in different years (e.g. municipal "
    "reorgs). Showing the most common value-set — pick another below for "
    "year-correct labels."
)


@dataclass(frozen=True)
class ValueSetGroup:
    value_set_id: int
    # Smallest cvid in the group; used purely as a deterministic
    # tie-breaker in chronological sort + tier-3a default selection,
    # not surfaced to the user.
    label_cvid: int
    cvid_count: int
    year_min: int | None
    year_max: int | None


@dataclass(frozen=True)
class ClassificationGroup:
    """One classification entry with its year window."""

    short_name: str
    year_min: int | None
    year_max: int | None


@dataclass(frozen=True)
class ColumnValuesResult:
    kind: Literal["classification", "values", "none"]
    title: str
    description: str | None
    codes: tuple[ColumnValue, ...]
    tier: VarianceTier | None = None
    note: str | None = None
    classifications: tuple[ClassificationGroup, ...] = ()
    picked_classification: str | None = None
    value_sets: tuple[ValueSetGroup, ...] = ()
    picked_value_set: int | None = None


def _fetch_distinct_classifications(
    conn: Any,
    matched_alias: str,
    register_ids: list[int],
    *,
    relevant_years: set[int] | None = None,
) -> tuple[ClassificationGroup, ...]:
    if not register_ids:
        return ()
    from regmeta.queries import extract_year

    ph = ",".join("?" for _ in register_ids)
    rows = conn.execute(
        "SELECT DISTINCT c.short_name, rv.registerversionnamn AS regver_name "
        "FROM variable_alias va "
        "JOIN variable_instance vi ON va.cvid = vi.cvid "
        "JOIN classification c ON vi.classification_id = c.id "
        "JOIN register_version rv ON vi.regver_id = rv.regver_id "
        f"WHERE LOWER(va.kolumnnamn) = LOWER(?) "
        f"  AND vi.register_id IN ({ph}) "
        "  AND c.short_name IS NOT NULL",
        [matched_alias, *register_ids],
    ).fetchall()
    # Aggregate (short_name → set of years seen), then min/max for the
    # picker tooltip. Yearless rows survive filtering (we can't disprove
    # their relevance) but they don't contribute to the year window.
    years_by_sn: dict[str, set[int]] = {}
    seen_yearless: set[str] = set()
    for r in rows:
        year = extract_year(r["regver_name"] or "")
        if (
            relevant_years is not None
            and year is not None
            and year not in relevant_years
        ):
            continue
        sn = r["short_name"]
        if year is None:
            seen_yearless.add(sn)
        else:
            years_by_sn.setdefault(sn, set()).add(year)
    all_short_names = set(years_by_sn) | seen_yearless
    return tuple(
        sorted(
            (
                ClassificationGroup(
                    short_name=sn,
                    year_min=min(years_by_sn[sn]) if sn in years_by_sn else None,
                    year_max=max(years_by_sn[sn]) if sn in years_by_sn else None,
                )
                for sn in all_short_names
            ),
            key=lambda g: (g.year_min is None, g.year_min or 0, g.short_name),
        )
    )


def _fetch_value_set_groups(
    conn: Any,
    matched_alias: str,
    register_ids: list[int],
    *,
    var_id: int | None = None,
) -> tuple[ValueSetGroup, ...]:
    """Resolve the value-set groups attached to ``matched_alias`` in
    ``register_ids``. When ``var_id`` is set, restrict to value-sets
    attached to that variable's instances — without the filter, an SCB
    column-name slot reused across var_ids (e.g. ``F12`` covering both a
    Skolform variable and a Distansutb survey question) leaks code lists
    from variables other than the one the popup is describing."""
    if not register_ids:
        return ()
    from regmeta.queries import extract_year

    ph = ",".join("?" for _ in register_ids)
    sql = (
        "SELECT DISTINCT vi.value_set_id, vi.cvid, rv.registerversionnamn "
        "FROM variable_alias va "
        "JOIN variable_instance vi ON va.cvid = vi.cvid "
        "JOIN register_version rv ON vi.regver_id = rv.regver_id "
        f"WHERE LOWER(va.kolumnnamn) = LOWER(?) "
        f"  AND vi.register_id IN ({ph}) "
        "  AND vi.value_set_id IS NOT NULL"
    )
    params: list[Any] = [matched_alias, *register_ids]
    if var_id is not None:
        sql += " AND vi.var_id = ?"
        params.append(var_id)
    rows = conn.execute(sql, params).fetchall()
    by_vs: dict[int, list[tuple[int, int | None]]] = {}
    for r in rows:
        vsid = int(r["value_set_id"])
        cvid = int(r["cvid"])
        year = extract_year(r["registerversionnamn"] or "")
        by_vs.setdefault(vsid, []).append((cvid, year))
    groups: list[ValueSetGroup] = []
    for vsid, items in by_vs.items():
        cvids = sorted(c for c, _ in items)
        years = sorted(y for _, y in items if y is not None)
        groups.append(
            ValueSetGroup(
                value_set_id=vsid,
                label_cvid=cvids[0],
                cvid_count=len(cvids),
                year_min=years[0] if years else None,
                year_max=years[-1] if years else None,
            )
        )
    return tuple(
        sorted(
            groups,
            key=lambda g: (g.year_min is None, g.year_min or 0, g.label_cvid),
        )
    )


def _fetch_value_set_codes(
    conn: Any, groups: tuple[ValueSetGroup, ...]
) -> list[tuple[int, str, str | None]]:
    if not groups:
        return []
    vsids = [g.value_set_id for g in groups]
    ph = ",".join("?" for _ in vsids)
    rows = conn.execute(
        f"SELECT DISTINCT vsm.value_set_id, vc.vardekod, vc.vardebenamning "
        f"FROM value_set_member vsm "
        f"JOIN value_code vc ON vsm.code_id = vc.code_id "
        f"WHERE vsm.value_set_id IN ({ph}) "
        f"ORDER BY vc.vardekod",
        vsids,
    ).fetchall()
    return [
        (int(r["value_set_id"]), str(r["vardekod"]), r["vardebenamning"]) for r in rows
    ]


def _filter_groups_by_relevant_years(
    groups: tuple[ValueSetGroup, ...],
    relevant_years: list[int] | None,
) -> tuple[tuple[ValueSetGroup, ...], str | None]:
    # Yearless groups (no parseable register-version year) survive the
    # filter: we can't disprove their relevance, and excluding them
    # would hide useful codes for projects sourced against a yearless
    # register version.
    if not relevant_years or not groups:
        return groups, None
    rel = set(relevant_years)
    kept = tuple(
        g
        for g in groups
        if g.year_min is None
        or g.year_max is None
        or any(g.year_min <= y <= g.year_max for y in rel)
    )
    if kept:
        return kept, None
    sample = ", ".join(str(y) for y in sorted(rel))
    return (
        groups,
        (
            f"regmeta has no value-set covering your project's year"
            f"{'s' if len(rel) > 1 else ''} ({sample}). Showing all "
            "available value-sets instead."
        ),
    )


def get_column_values(
    register: str | None,
    column: str,
    *,
    picked_classification: str | None = None,
    picked_value_set: int | None = None,
    picked_var_id: int | None = None,
    relevant_years: list[int] | None = None,
    db_path: Path | None = None,
) -> ColumnValuesResult:
    """Resolve value codes for one column under one register.

    ``picked_var_id`` scopes the value-set lookup to one variable. When
    multiple variables in the same register alias to ``column`` (SCB
    recycles column slots), passing the var_id chosen in the varinfo
    popup keeps the year tabs honest — without it, the panel mixes code
    lists from every variable that ever used the column name.

    Returns ``kind="none"`` rather than raising when regmeta is missing,
    the register doesn't resolve, or the column is unknown — degrades
    gracefully into an empty popover.
    """
    if not column or not column.strip():
        raise ValidationError("column must be a non-empty string")

    empty = ColumnValuesResult(kind="none", title=column, description=None, codes=())
    with _open_regmeta_conn(db_path) as conn:
        if conn is None:
            return empty
        try:
            from regmeta import resolve_register_ids
            from regmeta.errors import RegmetaError
            from regmeta.queries import get_classification_codes
        except ImportError:
            return empty

        register_ids: list[int] = []
        if register is not None and register.strip():
            try:
                register_ids = resolve_register_ids(conn, register)
            except RegmetaError:
                register_ids = []
        # Same year scope is applied to the signal's variance counts and
        # the picker's classification list — the popup must agree with
        # the inline badge in GroupCard.
        years_set: set[int] | None = set(relevant_years) if relevant_years else None
        signals = (
            _regmeta_lookup(conn, {column}, register_ids, relevant_years=years_set)
            if register_ids
            else {}
        )
        signal = lookup_with_prefix_fallback(signals, column)
        # Matched alias can differ from the column literal when regmeta
        # resolved via project-prefix stripping (e.g. "P1105_AStud" →
        # "AStud"); per-instance SQL must use the alias regmeta knows.
        matched_alias = _matched_alias_key(signals, column) or column

        classifications: tuple[ClassificationGroup, ...] = ()
        if signal is not None and signal.n_classifications > 1:
            classifications = _fetch_distinct_classifications(
                conn, matched_alias, register_ids, relevant_years=years_set
            )

        if signal is not None and signal.classification_short_name:
            chosen_sn = signal.classification_short_name
            classification_names = {g.short_name for g in classifications}
            if (
                picked_classification
                and classification_names
                and picked_classification in classification_names
            ):
                chosen_sn = picked_classification
            try:
                meta = get_classification_codes(conn, chosen_sn)
            except RegmetaError:
                meta = None
            if meta is not None:
                codes = _dedupe_codes(
                    (str(c.get("vardekod")), c.get("vardebenamning"))
                    for c in meta.get("codes", [])
                )
                if codes:
                    title = meta.get("short_name") or chosen_sn
                    description = meta.get("description") or meta.get("name")
                    tier, note = _tier_for_classification_path(
                        signal.n_value_sets,
                        signal.n_classifications,
                        chosen_sn,
                    )
                    return ColumnValuesResult(
                        kind="classification",
                        title=title,
                        description=description,
                        codes=codes,
                        tier=tier,
                        note=note,
                        classifications=classifications,
                        picked_classification=(
                            chosen_sn if signal.n_classifications > 1 else None
                        ),
                    )

        if signal is not None and signal.has_value_codes and register_ids:
            all_groups = _fetch_value_set_groups(
                conn, matched_alias, register_ids, var_id=picked_var_id
            )
            groups_used, filter_note = _filter_groups_by_relevant_years(
                all_groups, relevant_years
            )
            triples = _fetch_value_set_codes(conn, groups_used)
            union_codes = _dedupe_codes((c, lbl) for _, c, lbl in triples)
            if union_codes:
                tier, tier_note = _tier_for_values_path(
                    triples,
                    n_value_sets=len(groups_used),
                )
                chosen_vs = _resolve_picked_value_set(
                    tier, groups_used, picked_value_set
                )
                if chosen_vs is None:
                    codes = union_codes
                else:
                    codes = _dedupe_codes(
                        (c, lbl) for vsid, c, lbl in triples if vsid == chosen_vs
                    )
                    if not codes:
                        codes = union_codes
                        chosen_vs = None
                note = " ".join(n for n in (filter_note, tier_note) if n) or None
                return ColumnValuesResult(
                    kind="values",
                    title=column,
                    description=None,
                    codes=codes,
                    tier=tier,
                    note=note,
                    classifications=(),
                    picked_classification=None,
                    value_sets=groups_used,
                    picked_value_set=chosen_vs,
                )

        return empty


def _tier_for_classification_path(
    n_value_sets: int, n_classifications: int, picked: str
) -> tuple[VarianceTier, str | None]:
    # 3b dominates 2: the picker note is the actionable signal; a
    # "value-sets differ" note on top would just be noise.
    if n_classifications > 1:
        return ("3b", _note_tier_3b(picked))
    if n_value_sets > 1:
        return ("2", _NOTE_TIER_2)
    return ("1", None)


def _tier_for_values_path(
    triples: list[tuple[int, str, str | None]],
    n_value_sets: int,
) -> tuple[VarianceTier, str | None]:
    # Tier 3a's note is "pick another value-set below" — pointless with a
    # single set (label divergence inside the set isn't pickable). Drop
    # to tier 1 to avoid rendering the banner without a picker beneath.
    if n_value_sets <= 1:
        return ("1", None)
    # 3a (label collisions on the same code) dominates 2: under 3a the
    # deduped union arbitrarily picks one label per code, so the popup
    # loses meaning without the picker note.
    labels_per_code: dict[str, set[str]] = {}
    for _, code, label in triples:
        if label is None:
            continue
        labels_per_code.setdefault(code, set()).add(label)
    if any(len(s) > 1 for s in labels_per_code.values()):
        return ("3a", _NOTE_TIER_3A)
    return ("2", _NOTE_TIER_2)


def _note_tier_3b(picked: str) -> str:
    return (
        f"This column maps to different classifications across years. "
        f"Showing {picked} — pick another below for year-correct codes."
    )


def _resolve_picked_value_set(
    tier: VarianceTier,
    value_sets: tuple[ValueSetGroup, ...],
    picked: int | None,
) -> int | None:
    # Tier 3a default = most-common value-set (most cvids) so labels stay
    # self-consistent. Tier 2 default = union (None) since labels are
    # stable across sets. Bad picks degrade silently to the default.
    if not value_sets:
        return None
    valid_ids = {g.value_set_id for g in value_sets}
    if picked is not None and picked in valid_ids:
        return picked
    if tier == "3a":
        # Ties broken by lowest ``label_cvid`` (= oldest cvid in the
        # group), so two equally-large value-sets resolve to the same
        # winner across runs — keeps the popup deterministic when SCB
        # has parallel revisions with the same coverage.
        return max(value_sets, key=lambda g: (g.cvid_count, -g.label_cvid)).value_set_id
    return None


def _matched_alias_key(signals: dict[str, RegmetaSignal], column: str) -> str | None:
    lower = column.lower()
    if lower in signals:
        return lower
    stripped = strip_project_prefix(column).lower()
    if stripped in signals:
        return stripped
    return None


def _dedupe_codes(
    pairs: Iterator[tuple[str, str | None]],
) -> tuple[ColumnValue, ...]:
    seen: dict[str, ColumnValue] = {}
    for code, label in pairs:
        if code in seen:
            continue
        seen[code] = ColumnValue(code=code, label=label)
    return tuple(seen.values())


# -- Public API: column varinfo ------------------------------------------


@dataclass(frozen=True)
class VarinfoDescription:
    """One regmeta `variable` row, flattened to the fields the editor
    surfaces. The full audit-trail fields live behind the modal's
    expander; the primary fields are shown above the fold."""

    variabelnamn: str | None
    variabeldefinition: str | None
    variabelbeskrivning: str | None
    variabeloperationell_definition: str | None
    variabelreferenstid: str | None
    variabelhamtadfran: str | None
    variabelregister_kalla: str | None
    mattenhet: str | None
    var_id: int
    register_name: str | None


@dataclass(frozen=True)
class VarinfoAlternative:
    description: VarinfoDescription
    instances: int


VarinfoNoneReason = Literal["not_found", "unavailable", "no_register"]


@dataclass(frozen=True)
class ColumnVarinfoResult:
    """Resolved varinfo for one (column, register) pair.

    ``kind`` matches the wire envelope:
      - ``single``: exactly one variable aliases to this column.
      - ``divergent``: more than one variable aliases to this column under
        the same register (SCB has recycled the column name across var_ids).
        ``primary`` is the variable with the most ``variable_instance``
        rows; ``alternatives`` lists the rest in descending frequency.
      - ``none``: nothing to render. ``none_reason`` distinguishes the
        three cases the UI wants to surface differently:
          * ``"no_register"`` — column has no register pinned;
          * ``"unavailable"`` — regmeta DB / package not present;
          * ``"not_found"`` — column is unknown to regmeta (or the
            register itself doesn't resolve).
    """

    kind: Literal["single", "divergent", "none"]
    primary: VarinfoDescription | None = None
    primary_instances: int | None = None
    total_instances: int | None = None
    alternatives: tuple[VarinfoAlternative, ...] = ()
    none_reason: VarinfoNoneReason | None = None


def _varinfo_description_from_row(row: dict[str, Any]) -> VarinfoDescription:
    return VarinfoDescription(
        variabelnamn=row.get("variabelnamn"),
        variabeldefinition=row.get("variabeldefinition"),
        variabelbeskrivning=row.get("variabelbeskrivning"),
        variabeloperationell_definition=row.get("variabeloperationell_definition"),
        variabelreferenstid=row.get("variabelreferenstid"),
        variabelhamtadfran=row.get("variabelhamtadfran"),
        variabelregister_kalla=row.get("variabelregister_kalla"),
        mattenhet=row.get("mattenhet"),
        var_id=int(row["var_id"]),
        register_name=row.get("register_name"),
    )


def _variable_year_distance(
    variable: dict[str, Any], relevant_years: set[int]
) -> int | None:
    """Min distance from any instance's year to any year in
    ``relevant_years``. Returns None when neither side has a year — the
    caller treats that as "no year signal", same as a non-matching
    variable, so the popularity tie-breaker takes over."""
    instance_years = [
        y
        for inst in variable.get("instances") or ()
        if isinstance((y := inst.get("year")), int)
    ]
    if not instance_years or not relevant_years:
        return None
    return min(abs(iy - ry) for iy in instance_years for ry in relevant_years)


def _rank_variables(
    variables: list[dict[str, Any]], relevant_years: set[int] | None
) -> list[dict[str, Any]]:
    """Order variables — and, when ``relevant_years`` is set, drop those
    with no instance in those years.

    Filter rationale: an SCB column-name slot reused across decades (e.g.
    ``F12`` mapping to a 1990s Skolform variable AND a 2020 Distansutb
    survey item) shouldn't surface the wrong-era variant as an
    "alternative" — once the user has told us which year the source is
    from, off-year variables are not just lower-ranked, they're
    wrong. Within the kept set, sort by minimum year distance (closer
    wins), then popularity, then var_id for stable output. With no year
    context we collapse to the original popularity-only ranking — every
    variable is a plausible match."""
    if relevant_years:
        # Compute distance once per variable. Variables with parseable
        # years that don't overlap (dist > 0) are dropped; variables
        # with no parseable years (dist is None) are kept — "we can't
        # disprove relevance" mirrors `_filter_groups_by_relevant_years`.
        scored: list[tuple[int, int, int, dict[str, Any]]] = []
        for v in variables:
            dist = _variable_year_distance(v, relevant_years)
            if dist is not None and dist > 0:
                continue
            effective_dist = dist if dist is not None else 10_000
            scored.append(
                (
                    effective_dist,
                    -len(v.get("instances") or ()),
                    int(v["var_id"]),
                    v,
                )
            )
        scored.sort(key=lambda t: t[:3])
        return [t[3] for t in scored]
    return sorted(
        variables,
        key=lambda v: (-len(v.get("instances") or ()), int(v["var_id"])),
    )


def get_column_varinfo(
    register: str | None,
    column: str,
    *,
    relevant_years: list[int] | None = None,
    db_path: Path | None = None,
) -> ColumnVarinfoResult:
    """Resolve the regmeta variable description(s) for one column under
    one register.

    ``relevant_years`` (typically the source's configured year) is a
    strict filter: variables with parseable instance years that don't
    overlap the window are dropped entirely. Necessary because SCB
    reuses column-name slots across decades — a question-number like
    ``F12`` may map to a Skolform variable from 2004 and a Distansutb
    survey item from 2020 inside the same register, and once the user
    has set a year the off-year variant is not a viable "alternative",
    it's wrong. Variables with no parseable years survive the filter
    (mirrors ``_filter_groups_by_relevant_years``: we can't disprove
    their relevance). Without ``relevant_years``, falls back to pure
    popularity ranking and every variable is surfaced.

    Returns ``kind="none"`` rather than raising when regmeta is missing,
    the register doesn't resolve, or the column is unknown — degrades
    gracefully into an empty popover (same stance as
    ``get_column_values``).
    """
    if not column or not column.strip():
        raise ValidationError("column must be a non-empty string")

    if register is None or not register.strip():
        return ColumnVarinfoResult(kind="none", none_reason="no_register")
    unavailable = ColumnVarinfoResult(kind="none", none_reason="unavailable")
    not_found = ColumnVarinfoResult(kind="none", none_reason="not_found")

    year_set: set[int] | None = set(relevant_years) if relevant_years else None

    with _open_regmeta_conn(db_path) as conn:
        if conn is None:
            return unavailable
        try:
            from regmeta.errors import RegmetaError
            from regmeta.queries import get_varinfo
        except ImportError:
            return unavailable

        # MONA-prefixed columns (e.g. "P1105_Kon") aren't stored in regmeta
        # — mirror `get_column_values` and retry with the stripped form.
        candidates = [column]
        stripped = strip_project_prefix(column)
        if stripped and stripped != column:
            candidates.append(stripped)

        variables: list[dict[str, Any]] | None = None
        for candidate in candidates:
            try:
                variables = get_varinfo(conn, candidate, register=register)
                break
            except RegmetaError as exc:
                if exc.code != "not_found":
                    raise
                variables = None

        if not variables:
            return not_found

        ranked = _rank_variables(variables, year_set)
        if not ranked:
            # Filter dropped every match: regmeta knows the column under
            # this register, but only in years that don't overlap the
            # project's. Treat as not_found — the popup renders "not in
            # regmeta for these years" rather than a misleading
            # other-year description.
            return not_found
        primary_row = ranked[0]
        primary = _varinfo_description_from_row(primary_row)
        primary_n = len(primary_row.get("instances") or ())
        total = sum(len(v.get("instances") or ()) for v in ranked)

        if len(ranked) == 1:
            return ColumnVarinfoResult(
                kind="single",
                primary=primary,
                primary_instances=primary_n,
                total_instances=total,
            )

        alternatives = tuple(
            VarinfoAlternative(
                description=_varinfo_description_from_row(v),
                instances=len(v.get("instances") or ()),
            )
            for v in ranked[1:]
        )
        return ColumnVarinfoResult(
            kind="divergent",
            primary=primary,
            primary_instances=primary_n,
            total_instances=total,
            alternatives=alternatives,
        )


__all__ += [
    "ColumnValue",
    "ColumnValuesResult",
    "ClassificationGroup",
    "ValueSetGroup",
    "get_column_values",
    "ColumnVarinfoResult",
    "VarinfoAlternative",
    "VarinfoDescription",
    "VarinfoNoneReason",
    "get_column_varinfo",
]
