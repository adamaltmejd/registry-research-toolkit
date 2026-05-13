"""JSON-safe serialisation for the editor's data models.

Pure functions, no IO. Every nested dataclass exposed by
``editor.StateSnapshot`` has a ``_*_to_dict`` helper that produces a
JSON-safe dict — ``tuple`` → ``list``, frozen dataclasses → ``dict``,
inline subtype/format hints elided when None (mirrors the on-disk
config format).

This module exists so ``server.py`` can stream snapshots over HTTP
without depending on ``dataclasses.asdict``: that helper would silently
expose private fields and break on rename. Hand-written serialisers
keep the wire format coupled to an explicit contract (see
``tests/data/state_snapshot.golden.json``).
"""

from __future__ import annotations

from typing import Any

from .classify import RegmetaSignal
from .config import ColumnTypeOverride, MDWConfig, panel_to_dict
from .editor import (
    ColumnInfo,
    EditorWarning,
    RegisterGroupView,
    StateSnapshot,
)
from .panels import PanelCandidate, PanelMemberHints

__all__ = ["state_snapshot_to_dict"]


def state_snapshot_to_dict(snap: StateSnapshot) -> dict[str, Any]:
    """Serialise a ``StateSnapshot`` to a JSON-safe dict."""
    return {
        "config": _mdw_config_to_dict(snap.config),
        "groups": [_register_group_view_to_dict(g) for g in snap.groups],
        "discover": snap.discover,
        "warnings": [_editor_warning_to_dict(w) for w in snap.warnings],
        "snapshot_version": snap.snapshot_version,
    }


def _mdw_config_to_dict(config: MDWConfig) -> dict[str, Any]:
    return {
        "contract_version": config.contract_version,
        "discover_hash": config.discover_hash,
        "column_types": {
            source: {
                col: _column_type_override_to_dict(override)
                for col, override in cols.items()
            }
            for source, cols in config.column_types.items()
        },
        "column_options": {
            source: {col: dict(opts) for col, opts in cols.items()}
            for source, cols in config.column_options.items()
        },
        "sources": {name: dict(entry) for name, entry in config.sources.items()},
        "panels": [panel_to_dict(p) for p in config.panels],
        "manual_columns": [list(pair) for pair in config.manual_columns],
    }


def _column_type_override_to_dict(override: ColumnTypeOverride) -> dict[str, Any]:
    """``{"type": ...}`` plus only the inline hints that are non-null.

    Mirrors the on-disk JSON format and ``editor._hint_dict_for``.
    """
    out: dict[str, Any] = {"type": override.type}
    if override.id_subtype is not None:
        out["id_subtype"] = override.id_subtype
    if override.numeric_subtype is not None:
        out["numeric_subtype"] = override.numeric_subtype
    if override.date_format is not None:
        out["date_format"] = override.date_format
    return out


def _register_group_view_to_dict(group: RegisterGroupView) -> dict[str, Any]:
    return {
        "group_id": group.group_id,
        "register_id": group.register_id,
        "register_name": group.register_name,
        "confidence": group.confidence,
        "sources": list(group.sources),
        "columns_by_source": {
            source: [_column_info_to_dict(c) for c in cols]
            for source, cols in group.columns_by_source.items()
        },
        "schema_variant_groups": [list(g) for g in group.schema_variant_groups],
        "panel_candidate": (
            _panel_candidate_to_dict(group.panel_candidate)
            if group.panel_candidate is not None
            else None
        ),
        "member_hints": {
            sn: _panel_member_hints_to_dict(h) for sn, h in group.member_hints.items()
        },
    }


def _panel_member_hints_to_dict(hints: PanelMemberHints) -> dict[str, Any]:
    return {
        "year_from_name": hints.year_from_name,
        "time_key_column": hints.time_key_column,
    }


def _column_info_to_dict(info: ColumnInfo) -> dict[str, Any]:
    return {
        "name": info.name,
        "sql_type": info.sql_type,
        "current_type": info.current_type,
        "hint": dict(info.hint) if info.hint is not None else None,
        "provenance": info.provenance,
        "regmeta_signal": (
            _regmeta_signal_to_dict(info.regmeta_signal)
            if info.regmeta_signal is not None
            else None
        ),
        "regmeta_implied_type": info.regmeta_implied_type,
    }


def _regmeta_signal_to_dict(signal: RegmetaSignal) -> dict[str, Any]:
    return {
        "datatyp_kind": signal.datatyp_kind,
        "classification_short_name": signal.classification_short_name,
        "has_value_codes": signal.has_value_codes,
        "n_value_sets": signal.n_value_sets,
        "n_classifications": signal.n_classifications,
    }


def _panel_candidate_to_dict(candidate: PanelCandidate) -> dict[str, Any]:
    """``PanelCandidate.members`` is already a tuple of plain dicts, so a
    shallow copy is enough to break aliasing."""
    return {
        "members": [dict(m) for m in candidate.members],
        "suggested_panel_id": candidate.suggested_panel_id,
        "suggested_entity_key": candidate.suggested_entity_key,
    }


def _editor_warning_to_dict(warning: EditorWarning) -> dict[str, Any]:
    return {
        "code": warning.code,
        "message": warning.message,
        "context": dict(warning.context),
    }
