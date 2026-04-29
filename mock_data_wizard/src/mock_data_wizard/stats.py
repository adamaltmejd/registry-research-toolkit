"""Parse and validate the stats JSON contract produced by the R script."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

CONTRACT_VERSION = "2.0.0"

VALID_TYPES = frozenset({"numeric", "categorical", "high_cardinality", "date", "id"})
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
class ProjectStats:
    contract_version: str
    generated_at: str
    sources: list[SourceStats]
    shared_columns: list[SharedColumn]


class StatsValidationError(Exception):
    pass


def _require(obj: dict, key: str, context: str) -> Any:
    if key not in obj:
        raise StatsValidationError(f"Missing required field '{key}' in {context}")
    return obj[key]


def _parse_column(raw: dict, context: str) -> ColumnStats:
    name = _require(raw, "column_name", context)
    inferred = _require(raw, "inferred_type", context)
    if inferred not in VALID_TYPES:
        raise StatsValidationError(
            f"Invalid inferred_type '{inferred}' for column '{name}' in {context}. "
            f"Valid types: {sorted(VALID_TYPES)}"
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
            f"Regenerate stats.json with mock-data-wizard >= v0.3.0."
        )

    sources_raw = _require(raw, "sources", "root")
    if not sources_raw:
        raise StatsValidationError("No sources in stats JSON")

    return ProjectStats(
        contract_version=version,
        generated_at=raw.get("generated_at", ""),
        sources=[_parse_source(s) for s in sources_raw],
        shared_columns=[_parse_shared(s) for s in raw.get("shared_columns", [])],
    )
