"""Stateless editor API for ``mock_data_config.json``.

Pure functions over the on-disk config file plus the regmeta DB. No
in-memory session state; every mutation autosaves atomically and
returns a fresh snapshot. Concurrency is handled via SHA-256
``snapshot_version`` tokens — a stale ``expected_version`` raises
``StaleStateError`` without writing.

Module surface (see ``DESIGN.md`` § Editor API for the full contract):

- ``get_state``, ``init_if_missing``
- ``set_column_type``, ``set_group_register``, ``set_source_metadata``,
  ``set_column_options``
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
from ._util import lookup_with_prefix_fallback

__all__ = [
    # API
    "get_state",
    "init_if_missing",
    "set_column_type",
    "set_group_register",
    "set_source_metadata",
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
    register_str: str, col_names: set[str], db_path: Path | None
) -> dict[str, RegmetaSignal]:
    """Open regmeta, resolve the register, query column signals.

    Returns ``{}`` when regmeta is unavailable, the register doesn't
    resolve, or no columns match. Editor calls this once per register
    (sources sharing a register batch into one DB call)."""
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
        return _regmeta_lookup(conn, col_names, register_ids)


def _resolve_signals_for_groups(
    register_to_columns: dict[str, set[str]], db_path: Path | None
) -> dict[str, dict[str, RegmetaSignal]]:
    """Resolve signals for every register in one pass. Returns
    ``{register: {col_lower: RegmetaSignal}}``."""
    out: dict[str, dict[str, RegmetaSignal]] = {}
    for register_str, cols in register_to_columns.items():
        out[register_str] = _resolve_signals_for_register(register_str, cols, db_path)
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

    # Unassigned sources — one singleton group per source.
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

    return tuple(groups)


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
    else:
        # No discover: still need to expose register groupings using
        # whatever column_types we have.
        for source_name, entry in config.sources.items():
            register = entry.get("register")
            if not register:
                continue
            cols = config.column_types.get(source_name, {}).keys()
            register_to_columns.setdefault(register, set()).update(cols)

    signals = _resolve_signals_for_groups(register_to_columns, db_path)
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


def set_group_register(
    project_dir: Path,
    group_id: str,
    register: str | None,
    *,
    expected_version: str,
    db_path: Path | None = None,
    reclassify_manual: bool = False,
) -> StateSnapshot:
    """Assign or clear a register for a group. Re-classifies the group's
    columns. Per session decision: when a column's type changes during
    reclassification, its ``column_options`` entry is dropped."""
    project_dir = Path(project_dir)
    with _config_lock(project_dir):
        payload = _verify_version(project_dir, expected_version)
        try:
            config = parse_config(payload)
        except ValueError as exc:
            raise ValidationError(str(exc)) from exc

        # Resolve group_id → list of source_names.
        affected_sources: list[str] = _sources_in_group(config, group_id, db_path)
        if not affected_sources:
            raise ValidationError(
                f"group_id={group_id!r} not found in current snapshot."
            )

        # Resolve the new register against regmeta when non-null.
        register_str: str | None
        if register is None:
            register_str = None
        else:
            reg = resolve_register(register, db_path=db_path)
            if reg is None:
                raise ValidationError(
                    f"register={register!r} did not resolve in regmeta."
                )
            register_str = reg.name

        # Discover payload (for column lists). Bail if absent —
        # re-classify needs the discover schema.
        discover_path = project_dir / DISCOVER_FILENAME_DEFAULT
        if not discover_path.exists():
            raise ValidationError(
                f"set_group_register requires {DISCOVER_FILENAME_DEFAULT} next to "
                f"the config to re-classify columns."
            )
        discover = _read_discover(discover_path)
        sources_by_name = {s["source_name"]: s for s in discover.get("sources", [])}

        # Update `register` on each affected source.
        sources_block = payload.setdefault("sources", {})
        for sn in affected_sources:
            entry = sources_block.setdefault(sn, {})
            if register_str is None:
                entry.pop("register", None)
            else:
                entry["register"] = register_str

        # Resolve regmeta signals once for the new register.
        if register_str is not None:
            all_cols: set[str] = set()
            for sn in affected_sources:
                src = sources_by_name.get(sn)
                if src is None:
                    continue
                for c in src.get("columns", []):
                    all_cols.add(c["name"])
            signals = _resolve_signals_for_register(register_str, all_cols, db_path)
        else:
            signals = {}

        manual_pairs = set(tuple(p) for p in payload.get("manual_columns", []))
        column_types = payload.setdefault("column_types", {})
        column_options = payload.get("column_options", {})

        for sn in affected_sources:
            src = sources_by_name.get(sn)
            if src is None:
                continue
            new_cols = _classify_columns(src.get("columns", []), register_str, signals)
            existing_cols = column_types.get(sn, {})
            out_cols: dict[str, dict[str, Any]] = {}
            for col_name, classified in new_cols.items():
                is_manual = (sn, col_name) in manual_pairs
                if is_manual and not reclassify_manual:
                    # Preserve manual override exactly.
                    if col_name in existing_cols:
                        out_cols[col_name] = existing_cols[col_name]
                    else:
                        out_cols[col_name] = classified
                    continue
                # Auto column (or manual being reclassified): write the
                # classifier output.
                if reclassify_manual and is_manual:
                    manual_pairs.discard((sn, col_name))
                old_type = existing_cols.get(col_name, {}).get("type")
                new_type = classified["type"]
                out_cols[col_name] = classified
                if old_type is not None and old_type != new_type:
                    _drop_column_options(column_options, sn, col_name)
            column_types[sn] = out_cols

        if reclassify_manual:
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


def set_source_metadata(
    project_dir: Path,
    source_name: str,
    *,
    expected_version: str,
    year: int | None | _Unchanged = UNCHANGED,
    db_path: Path | None = None,
) -> StateSnapshot:
    """Modify per-source metadata. Currently scoped to ``year``."""
    project_dir = Path(project_dir)
    with _config_lock(project_dir):
        payload = _verify_version(project_dir, expected_version)
        _assert_source_in_discover(
            _discover_sources_index(project_dir), payload, source_name
        )
        sources = payload.setdefault("sources", {})
        entry = sources.setdefault(source_name, {})
        if not isinstance(year, _Unchanged):
            if year is None:
                entry["year"] = None
            elif isinstance(year, int) and not isinstance(year, bool):
                entry["year"] = year
            else:
                raise ValidationError(
                    f"year must be int or None, got {type(year).__name__}"
                )
        if not entry:
            sources.pop(source_name, None)
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


# Year-variance notes surfaced inside the popup. Kept as module-level
# constants so tests can assert on the exact text without re-deriving it.
_NOTE_TIER_2 = (
    "Value code sets differ by period. Not all codes are valid for every year."
)
_NOTE_TIER_3A = (
    "Some codes have different meanings in different years (e.g. municipal "
    "reorgs). Use `regmeta get values <var>` for year-correct lookups."
)


@dataclass(frozen=True)
class ColumnValuesResult:
    """Year-correct value codes for one (register, column) pair.

    ``kind`` distinguishes the source the codes came from:
      - ``"classification"``: full code list of a SCB classification (e.g.
        SUN2020). Same codes regardless of register; the chosen register
        only narrows down which classification this column maps to.
      - ``"values"``: per-instance value-set codes aggregated across
        ``variable_instance`` rows for this register. Used when no
        classification is attached (register-local code lists like
        ``ALKod`` / ``Kon``).
      - ``"none"``: regmeta knows the column but has no codes — the
        caller usually shows "no value codes available".

    ``title`` is short user-facing text (classification short_name or
    variable name); ``description`` is optional long text shown below.

    ``tier`` flags the year-variance shape (issue #64). ``None`` only when
    ``kind="none"``. ``note`` is the user-facing variance message (None
    when there is nothing to warn about). ``classifications`` lists every
    distinct ``classification.short_name`` attached to this column under
    this register — populated when more than one exists so the popup can
    render a picker. ``picked_classification`` is the short_name actually
    rendered (only populated on the classification path when picker is
    relevant).
    """

    kind: Literal["classification", "values", "none"]
    title: str
    description: str | None
    codes: tuple[ColumnValue, ...]
    tier: VarianceTier | None = None
    note: str | None = None
    classifications: tuple[str, ...] = ()
    picked_classification: str | None = None


def _fetch_distinct_classifications(
    conn: Any, matched_alias: str, register_ids: list[int]
) -> tuple[str, ...]:
    """Distinct ``classification.short_name`` attached to ``matched_alias``
    under ``register_ids``. Ordered alphabetically so the picker is
    stable across opens. Empty tuple when no classifications are attached."""
    if not register_ids:
        return ()
    ph = ",".join("?" for _ in register_ids)
    rows = conn.execute(
        "SELECT DISTINCT c.short_name "
        "FROM variable_alias va "
        "JOIN variable_instance vi ON va.cvid = vi.cvid "
        "JOIN classification c ON vi.classification_id = c.id "
        f"WHERE LOWER(va.kolumnnamn) = LOWER(?) "
        f"  AND vi.register_id IN ({ph}) "
        "  AND c.short_name IS NOT NULL "
        "ORDER BY c.short_name",
        [matched_alias, *register_ids],
    ).fetchall()
    return tuple(r["short_name"] for r in rows)


def get_column_values(
    register: str | None,
    column: str,
    *,
    picked_classification: str | None = None,
    db_path: Path | None = None,
) -> ColumnValuesResult:
    """Resolve value codes for one column under one register.

    Strategy: classification beats per-instance codes when both are
    available — the classification carries the canonical list (often with
    hierarchy levels) while per-instance codes can be a register-local
    subset. Falls back to per-instance value codes when no classification
    is attached, and to ``kind="none"`` when regmeta has no codes either.

    ``picked_classification`` opts into rendering a non-default
    classification when the column maps to multiple. Ignored when not in
    the column's distinct classifications for this register (the popup
    falls back to the default winner rather than 404-ing).

    Returns ``kind="none"`` rather than raising when regmeta is missing,
    the register doesn't resolve, or the column is unknown — the UI
    surfaces an empty popover rather than an error envelope, matching
    the "regmeta degrades gracefully" stance elsewhere in the editor.
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
        signals = _regmeta_lookup(conn, {column}, register_ids) if register_ids else {}
        signal = lookup_with_prefix_fallback(signals, column)
        # The matched alias may differ from the literal column when the
        # signal was found via project-prefix stripping (e.g. column
        # "P1105_AStud" matched alias "AStud"). Use the matched key for
        # the per-instance SQL so we don't query for an alias regmeta
        # doesn't know.
        matched_alias = _matched_alias_key(signals, column) or column

        # Distinct classifications: populated when the picker is relevant
        # (i.e. > 1 across years). Fetched lazily — most columns have one
        # or zero classifications, no need to hit the DB twice.
        classifications: tuple[str, ...] = ()
        if signal is not None and signal.n_classifications > 1:
            classifications = _fetch_distinct_classifications(
                conn, matched_alias, register_ids
            )

        # Classification path: when a short_name is attached, fetch the
        # canonical code list. ``get_classification_codes`` raises on
        # not_found; fall through to the per-instance path below in that
        # case so a stale classification name doesn't blank the popover.
        if signal is not None and signal.classification_short_name:
            # Honor an explicit pick when it's a real candidate for this
            # column. Bad picks degrade silently to the default winner
            # rather than 404 — the UI is just showing year-variance and
            # a transient picker state shouldn't break the popup.
            chosen_sn = signal.classification_short_name
            if (
                picked_classification
                and classifications
                and picked_classification in classifications
            ):
                chosen_sn = picked_classification
            try:
                meta = get_classification_codes(conn, chosen_sn)
            except RegmetaError:
                meta = None
            if meta is not None:
                # Dedupe on vardekod — SCB classifications occasionally
                # carry multiple labels per code across versions; the
                # first occurrence wins for display purposes.
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

        # Per-instance values path: aggregate codes across variable
        # instances for this register. SCB occasionally widens or
        # narrows the code set across years; deduping by `vardekod`
        # surfaces a clean union without losing the per-year history
        # (the user can still hit `regmeta get values` for that).
        if signal is not None and signal.has_value_codes and register_ids:
            ph = ",".join("?" for _ in register_ids)
            rows = conn.execute(
                "SELECT DISTINCT vc.vardekod, vc.vardebenamning "
                "FROM variable_alias va "
                "JOIN variable_instance vi ON va.cvid = vi.cvid "
                "JOIN value_set_member vsm ON vi.value_set_id = vsm.value_set_id "
                "JOIN value_code vc ON vsm.code_id = vc.code_id "
                f"WHERE LOWER(va.kolumnnamn) = LOWER(?) "
                f"  AND vi.register_id IN ({ph}) "
                "ORDER BY vc.vardekod",
                [matched_alias, *register_ids],
            ).fetchall()
            raw_pairs = [(str(r["vardekod"]), r["vardebenamning"]) for r in rows]
            codes = _dedupe_codes(iter(raw_pairs))
            if codes:
                tier, note = _tier_for_values_path(
                    raw_pairs, signal.n_value_sets, signal.n_classifications
                )
                return ColumnValuesResult(
                    kind="values",
                    title=column,
                    description=None,
                    codes=codes,
                    tier=tier,
                    note=note,
                    classifications=classifications,
                    picked_classification=None,
                )

        return empty


def _tier_for_classification_path(
    n_value_sets: int, n_classifications: int, picked: str
) -> tuple[VarianceTier, str | None]:
    """Tier + note for the classification path.

    3b dominates 2: when a column maps to multiple classifications the
    picker note is the actionable signal; an extra "value-sets differ"
    note would just be noise on top of that.
    """
    if n_classifications > 1:
        return ("3b", _note_tier_3b(picked))
    if n_value_sets > 1:
        return ("2", _NOTE_TIER_2)
    return ("1", None)


def _tier_for_values_path(
    raw_pairs: list[tuple[str, str | None]],
    n_value_sets: int,
    n_classifications: int,
) -> tuple[VarianceTier, str | None]:
    """Tier + note for the per-instance values path.

    3a (label collisions on the same code) dominates 2 because the
    rendered popup actually loses meaning under 3a — the deduped list
    arbitrarily picks one label per code. 3b is conceptually possible
    here too (signal carries a classification short_name but
    ``get_classification_codes`` returned empty / not_found, so we fell
    through). Treating that as 3b would invite a picker that promises
    classification codes we couldn't fetch; flag the simpler tiers
    instead.
    """
    labels_per_code: dict[str, set[str]] = {}
    for code, label in raw_pairs:
        if label is None:
            continue
        labels_per_code.setdefault(code, set()).add(label)
    if any(len(s) > 1 for s in labels_per_code.values()):
        return ("3a", _NOTE_TIER_3A)
    if n_value_sets > 1 or n_classifications > 1:
        return ("2", _NOTE_TIER_2)
    return ("1", None)


def _note_tier_3b(picked: str) -> str:
    return (
        f"This column maps to different classifications across years. "
        f"Showing {picked} — pick another below for year-correct codes."
    )


def _matched_alias_key(signals: dict[str, RegmetaSignal], column: str) -> str | None:
    """Mirror ``lookup_with_prefix_fallback``'s key resolution and return
    the *key* it matched, not the value. Used so per-instance SQL queries
    the same alias regmeta resolved on, even after project-prefix
    stripping."""
    from ._util import strip_project_prefix

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
    """Dedupe ``(code, label)`` pairs by code, preserving first-seen
    order. SCB sometimes returns multiple labels per code (per-year
    relabels, partial classification merges); the table renderer keys on
    code, so we collapse here rather than crash there."""
    seen: dict[str, ColumnValue] = {}
    for code, label in pairs:
        if code in seen:
            continue
        seen[code] = ColumnValue(code=code, label=label)
    return tuple(seen.values())


__all__ += ["ColumnValue", "ColumnValuesResult", "get_column_values"]
