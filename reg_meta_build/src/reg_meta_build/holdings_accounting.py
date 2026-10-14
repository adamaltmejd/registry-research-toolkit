"""Exact table and physical-column disposition accounting before compilation."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .holdings_census import census_rows
from .source_evidence import canonical_sha256
from .steward_spec import census_guard, scope_locator

if TYPE_CHECKING:
    from pathlib import Path

    from .inventory import DeliveryInventory
    from .steward_spec import StewardSpec, UndatedHolding

DISPOSITIONS = ("dated", "year_independent", "retained_unknown", "excluded", "lookup")
# A private candidate's `manifest.json` `roles`: each names one manifest member.
# The census and the storage export (types only) are the steward's raw inputs;
# the inventory is the generator's output, at a fixed path because every
# compiled `holding_table.source_ref` locates it.
CANDIDATE_ROLES = ("census", "storage_export", "inventory")
INVENTORY_ROLE_PATH = "policy/inventory.toml"


@dataclass(frozen=True)
class HoldingsAccounting:
    columns: dict[str, set[str]]
    counts: dict[str, dict[str, int]]
    sha256: str
    unknown: tuple[UndatedHolding, ...]


def account_holdings(
    root: Path, inventory: DeliveryInventory, *, spec: StewardSpec, steward: str
) -> HoldingsAccounting:
    """Require every census table and column to have exactly one disposition.

    The census is the candidate's `census` role; lookups, exclusions and
    retained-unknown tables are the spec's `scope.toml` dispositions, the rest
    the inventory's tables."""
    source_name = json.loads((root / "manifest.json").read_text(encoding="utf-8"))[
        "roles"
    ]["census"]
    source = (root / source_name).read_bytes()
    census_guard(spec.scope, source)
    raw: dict[str, set[str]] = {}
    for _category, _detail, table, columns in census_rows(root / source_name):
        raw.setdefault(table, set()).update(columns)
    scope = scope_locator(steward)
    claims: dict[str, tuple[str, set[str]]] = {}

    def claim(table: str, disposition: str, evidence: set[str]) -> None:
        if table not in raw:
            raise ValueError(
                f"Holdings accounting: {table!r} is absent from census ({sorted(evidence)})"
            )
        if table in claims:
            raise ValueError(
                f"Holdings accounting: duplicate disposition for {table!r}: {claims[table][0]} and {disposition}"
            )
        claims[table] = disposition, evidence | {f"{source_name}:table[{table!r}]"}

    for index, entry in enumerate(spec.scope.lookups):
        claim(entry.table, "lookup", {f"{scope}:lookup[{index}]"})
    for index, entry in enumerate(spec.scope.excludes):
        claim(entry.table, "excluded", {f"{scope}:exclude[{index}]"})
    for index, entry in enumerate(spec.scope.retain_unknown):
        claim(entry.table, "retained_unknown", {f"{scope}:retain_unknown[{index}]"})
    for index, table in enumerate(inventory.tables):
        locator = f"{INVENTORY_ROLE_PATH}:table[{index}]"
        claim(
            table.id,
            "year_independent" if table.period_scope == "year_independent" else "dated",
            {locator},
        )
        actual = {column.name for column in table.columns}
        if actual != raw[table.id]:
            raise ValueError(
                f"Holdings accounting: physical-column census mismatch at {locator} ({table.id!r}); missing={sorted(raw[table.id] - actual)!r}, extra={sorted(actual - raw[table.id])!r}"
            )
    if missing := sorted(raw.keys() - claims.keys()):
        raise ValueError(
            f"Holdings accounting: {len(missing)} census tables have no disposition: {missing!r}"
        )
    counts = {name: {"tables": 0, "columns": 0} for name in ("raw", *DISPOSITIONS)}
    projection = []
    for table in sorted(raw):
        disposition, refs = claims[table]
        counts["raw"]["tables"] += 1
        counts["raw"]["columns"] += len(raw[table])
        counts[disposition]["tables"] += 1
        counts[disposition]["columns"] += len(raw[table])
        for column in (None, *sorted(raw[table])):
            projection.append(
                {
                    "table": table,
                    "column": column,
                    "disposition": disposition,
                    "evidence": sorted(refs),
                }
            )
    # Only this canonical count summary and the projection digest enter SQLite.
    return HoldingsAccounting(
        raw,
        counts,
        canonical_sha256(projection),
        tuple(spec.scope.retain_unknown),
    )
