"""Guard retained-unknown holdings against the accepted census and policy."""

from __future__ import annotations

import csv
import hashlib
import io
import tomllib
from typing import TYPE_CHECKING, Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    TypeAdapter,
    field_validator,
)
from reg_meta.source_evidence import canonical_sha256

if TYPE_CHECKING:
    from pathlib import Path


class UndatedHolding(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    table: str = Field(min_length=1)
    register_fqid: str = Field(alias="register")
    rows_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    reason: str = Field(min_length=1)
    edition: JsonValue = None
    partition: str | None = None

    @field_validator("partition")
    @classmethod
    def check_partition(cls, value: str | None) -> str | None:
        from reg_meta.fqid import validate_slug

        if value is not None:
            validate_slug(value, "partition")
        return value


def load_holdings_retention_policy(
    policy_path: Path,
    source_path: Path,
) -> tuple[tuple[UndatedHolding, ...], dict[str, list[dict[str, Any]]], str]:
    """Check exact source and ordered-row guards for inventory and warning emission."""
    policy = tomllib.loads(policy_path.read_text(encoding="utf-8"))
    source = source_path.read_bytes()
    source_sha256 = hashlib.sha256(source).hexdigest()
    if policy.get("source_sha256") != source_sha256:
        raise ValueError("Undated holdings source SHA-256 does not match policy")
    entries = TypeAdapter(tuple[UndatedHolding, ...]).validate_python(
        policy.get("retain_unknown", [])
    )
    if len({entry.table for entry in entries}) != len(entries):
        raise ValueError("Duplicate undated holdings table")
    rows_by_table: dict[str, list[dict[str, Any]]] = {}
    for line, cells in enumerate(csv.reader(io.StringIO(source.decode("utf-8"))), 1):
        if len(cells) >= 3:
            rows_by_table.setdefault(cells[2].strip(), []).append(
                {"line": line, "cells": cells}
            )
    excluded = {entry["table"] for entry in policy.get("exclude", [])}
    for entry in entries:
        rows = rows_by_table.get(entry.table)
        if not rows or canonical_sha256(rows) != entry.rows_sha256:
            raise ValueError(f"Undated holdings row guard failed: {entry.table}")
        if entry.table in excluded:
            raise ValueError(f"Retained holding cannot also be excluded: {entry.table}")
    return entries, rows_by_table, source_sha256
