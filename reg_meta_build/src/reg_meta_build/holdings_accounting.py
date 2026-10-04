"""Exact table and physical-column disposition accounting before compilation."""

from __future__ import annotations

import hashlib
import json
import tomllib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from reg_meta.source_evidence import canonical_sha256

from .holdings_census import census_rows
from .swecov_policy import SourcePolicy

if TYPE_CHECKING:
    from pathlib import Path

    from reg_meta.inventory import DeliveryInventory

DISPOSITIONS = ("dated", "year_independent", "retained_unknown", "excluded", "lookup")
POLICY_NAMES = ("source_policy.toml", "inventory_overlay.toml", "holdings_policy.toml")


@dataclass(frozen=True)
class HoldingsAccounting:
    columns: dict[str, set[str]]
    counts: dict[str, dict[str, int]]
    sha256: str
    policy_sha256: str
    source_name: str
    unknown: tuple[Any, ...]


def account_holdings(root: Path, inventory: DeliveryInventory) -> HoldingsAccounting:
    """Require every census table and column to have exactly one disposition."""
    from .extend_db import load_holdings_retention_policy

    source_paths = sorted((root / "swecov").glob("SWECOV_variables_full_*.csv"))
    if len(source_paths) != 1:
        raise ValueError("Holdings accounting requires exactly one complete census CSV")
    source = source_paths[0]
    source_name = source.relative_to(root).as_posix()
    policies = {
        name: tomllib.loads((root / "policy" / name).read_text(encoding="utf-8"))
        for name in POLICY_NAMES
    }
    routing = SourcePolicy.model_validate(policies["source_policy.toml"])
    lookup_routes = {
        (route.category, route.detail): index
        for index, route in enumerate(routing.route)
        if route.status == "lookup"
    }
    raw: dict[str, set[str]] = {}
    lookups: dict[str, set[str]] = {}
    for category, detail, table, columns in census_rows(source):
        raw.setdefault(table, set()).update(columns)
        if (category, detail) in lookup_routes:
            index = lookup_routes[category, detail]
            lookups.setdefault(table, set()).add(
                f"policy/source_policy.toml:route[{index}]"
            )
    unknown, _, _ = load_holdings_retention_policy(
        root / "policy/holdings_policy.toml", source
    )
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

    for table, refs in lookups.items():
        claim(table, "lookup", refs)
    for name in ("inventory_overlay.toml", "holdings_policy.toml"):
        for index, entry in enumerate(policies[name].get("exclude", [])):
            # The generator removes lookup routes before applying the overlay;
            # annotations on those tables are inert, not a second disposition.
            if name == "inventory_overlay.toml" and entry["table"] in lookups:
                continue
            claim(entry["table"], "excluded", {f"policy/{name}:exclude[{index}]"})
    for index, entry in enumerate(unknown):
        claim(
            entry.table,
            "retained_unknown",
            {f"policy/holdings_policy.toml:retain_unknown[{index}]"},
        )
    for index, table in enumerate(inventory.tables):
        locator = f"policy/inventory.toml:table[{index}]"
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
    policy_digests = {
        f"policy/{name}": hashlib.sha256(
            (root / "policy" / name).read_bytes()
        ).hexdigest()
        for name in POLICY_NAMES
    }
    # Only this canonical count summary and the projection digest enter SQLite.
    json.dumps(counts, allow_nan=False)
    return HoldingsAccounting(
        raw,
        counts,
        canonical_sha256(projection),
        canonical_sha256(policy_digests),
        source_name,
        unknown,
    )
