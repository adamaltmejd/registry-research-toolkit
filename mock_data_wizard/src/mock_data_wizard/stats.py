"""Parse and validate the stats JSON contract produced by the MONA extract bundle."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .classify import COLUMN_TYPES
from .extract import CONTRACT_VERSION  # producer owns the version

VALID_SOURCE_TYPES = frozenset({"file", "sql"})


@dataclass
class ColumnStats:
    column_name: str
    inferred_type: str
    nullable: bool
    null_count: int
    null_rate: float
    n_distinct: int
    stats: dict[str, Any] = field(default_factory=dict)


@dataclass
class SourceStats:
    source_name: str
    source_type: str
    source_detail: dict[str, Any]
    row_count: int
    columns: list[ColumnStats]


@dataclass
class SharedColumn:
    column_name: str
    sources: list[str]
    max_n_distinct: int


@dataclass
class PanelPeriod:
    # ``period`` is whatever the time_key carries: usually a year (int),
    # but may be a quarter/date string for sub-annual column-member panels.
    period: int | str
    source: str
    n_rows: int
    n_entity_ids: int


@dataclass
class PanelMemberRef:
    """Mirror of ``config.PanelMember``: polymorphic ``time_key`` (int for
    a literal period, str for a column name)."""

    source: str
    time_key: int | str


@dataclass
class Panel:
    panel_id: str
    entity_key: str
    members: list[PanelMemberRef]
    by_period: list[PanelPeriod]


@dataclass
class ProjectStats:
    contract_version: str
    generated_at: str
    sources: list[SourceStats]
    shared_columns: list[SharedColumn]
    panels: list[Panel] = field(default_factory=list)


class StatsValidationError(Exception):
    pass


def _require(obj: dict, key: str, context: str) -> Any:
    if key not in obj:
        raise StatsValidationError(f"Missing required field '{key}' in {context}")
    return obj[key]


def _parse_column(raw: dict, context: str) -> ColumnStats:
    name = _require(raw, "column_name", context)
    inferred = _require(raw, "inferred_type", context)
    if inferred not in COLUMN_TYPES:
        raise StatsValidationError(
            f"Invalid inferred_type '{inferred}' for column '{name}' in {context}. "
            f"Valid types: {sorted(COLUMN_TYPES)}"
        )
    return ColumnStats(
        column_name=name,
        inferred_type=inferred,
        nullable=raw.get("nullable", False),
        null_count=raw.get("null_count", 0),
        null_rate=raw.get("null_rate", 0.0),
        n_distinct=raw.get("n_distinct", 0),
        stats=raw.get("stats", {}),
    )


def _parse_source(raw: dict) -> SourceStats:
    name = _require(raw, "source_name", "sources[]")
    ctx = f"source '{name}'"
    source_type = _require(raw, "source_type", ctx)
    if source_type not in VALID_SOURCE_TYPES:
        raise StatsValidationError(
            f"Invalid source_type '{source_type}' for {ctx}. "
            f"Valid types: {sorted(VALID_SOURCE_TYPES)}"
        )
    columns_raw = _require(raw, "columns", ctx)
    if not columns_raw:
        raise StatsValidationError(f"Source '{name}' has no columns")
    detail = raw.get("source_detail", {})
    if not isinstance(detail, dict):
        raise StatsValidationError(
            f"source_detail must be an object in {ctx}, got {type(detail).__name__}"
        )
    return SourceStats(
        source_name=name,
        source_type=source_type,
        source_detail=detail,
        row_count=_require(raw, "row_count", ctx),
        columns=[_parse_column(c, ctx) for c in columns_raw],
    )


def _parse_shared(raw: dict) -> SharedColumn:
    return SharedColumn(
        column_name=_require(raw, "column_name", "shared_columns[]"),
        sources=_require(raw, "sources", "shared_columns[]"),
        max_n_distinct=_require(raw, "max_n_distinct", "shared_columns[]"),
    )


def _parse_panel(raw: dict) -> Panel:
    pid = _require(raw, "panel_id", "panels[]")
    ctx = f"panels[panel_id={pid!r}]"
    members_raw = _require(raw, "members", ctx)
    if not isinstance(members_raw, list) or not members_raw:
        raise StatsValidationError(f"panel {pid!r}: members must be a non-empty list")
    members: list[PanelMemberRef] = []
    for m in members_raw:
        m_src = _require(m, "source", f"{ctx}.members[]")
        if "time_key" not in m:
            raise StatsValidationError(
                f"panel {pid!r}: member {m_src!r} missing required 'time_key' "
                f"(int for literal period, str for column name)"
            )
        tk = m["time_key"]
        # bool is an int subclass; reject explicitly.
        if isinstance(tk, bool):
            raise StatsValidationError(
                f"panel {pid!r}: member {m_src!r} time_key must be int or "
                f"non-empty string, got bool"
            )
        if isinstance(tk, int):
            members.append(PanelMemberRef(source=m_src, time_key=tk))
        elif isinstance(tk, str):
            if not tk:
                raise StatsValidationError(
                    f"panel {pid!r}: member {m_src!r} time_key must be a "
                    f"non-empty string"
                )
            members.append(PanelMemberRef(source=m_src, time_key=tk))
        else:
            raise StatsValidationError(
                f"panel {pid!r}: member {m_src!r} time_key must be int or "
                f"non-empty string, got {type(tk).__name__}"
            )
    by_period_raw = _require(raw, "by_period", ctx)
    if not isinstance(by_period_raw, list):
        raise StatsValidationError(
            f"panel {pid!r}: by_period must be a list, got {type(by_period_raw).__name__}"
        )
    by_period: list[PanelPeriod] = []
    for p in by_period_raw:
        bp_ctx = f"{ctx}.by_period[]"
        period = _require(p, "period", bp_ctx)
        # period: int or non-empty str. Reject null/bool/empty so
        # downstream pool-keying invariants hold.
        if isinstance(period, bool) or period is None:
            raise StatsValidationError(
                f"panel {pid!r}: by_period.period must be int or string, "
                f"got {type(period).__name__}"
            )
        if isinstance(period, str) and not period:
            raise StatsValidationError(
                f"panel {pid!r}: by_period.period must be non-empty"
            )
        if not isinstance(period, (int, str)):
            raise StatsValidationError(
                f"panel {pid!r}: by_period.period must be int or string, "
                f"got {type(period).__name__}"
            )
        source = _require(p, "source", bp_ctx)
        if not isinstance(source, str) or not source:
            raise StatsValidationError(
                f"panel {pid!r}: by_period.source must be a non-empty string"
            )
        n_rows = _require(p, "n_rows", bp_ctx)
        if isinstance(n_rows, bool) or not isinstance(n_rows, int):
            raise StatsValidationError(
                f"panel {pid!r}: by_period.n_rows must be int, "
                f"got {type(n_rows).__name__}"
            )
        n_entity_ids = _require(p, "n_entity_ids", bp_ctx)
        if isinstance(n_entity_ids, bool) or not isinstance(n_entity_ids, int):
            raise StatsValidationError(
                f"panel {pid!r}: by_period.n_entity_ids must be int, "
                f"got {type(n_entity_ids).__name__}"
            )
        by_period.append(
            PanelPeriod(
                period=period,
                source=source,
                n_rows=n_rows,
                n_entity_ids=n_entity_ids,
            )
        )
    return Panel(
        panel_id=pid,
        entity_key=_require(raw, "entity_key", ctx),
        members=members,
        by_period=by_period,
    )


def parse_stats(path: Path) -> ProjectStats:
    """Parse and validate a stats JSON file into ProjectStats."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise StatsValidationError(f"Invalid JSON in {path}: {exc}") from exc

    version = _require(raw, "contract_version", "root")
    major = version.split(".")[0]
    if major != CONTRACT_VERSION.split(".")[0]:
        raise StatsValidationError(
            f"Unsupported contract major version '{version}' "
            f"(expected {CONTRACT_VERSION.split('.')[0]}.x.x). "
            f"Regenerate mock_data_stats.json with mock-data-wizard >= v0.3.0."
        )

    sources_raw = _require(raw, "sources", "root")
    if not sources_raw:
        raise StatsValidationError("No sources in stats JSON")

    return ProjectStats(
        contract_version=version,
        generated_at=raw.get("generated_at", ""),
        sources=[_parse_source(s) for s in sources_raw],
        shared_columns=[_parse_shared(s) for s in raw.get("shared_columns", [])],
        panels=[_parse_panel(p) for p in raw.get("panels", [])],
    )
