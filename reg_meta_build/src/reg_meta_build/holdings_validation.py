"""Artifact invariants for compiled physical holdings and semantic identity."""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING

from reg_meta.fqid import validate_slug
from reg_meta.inventory import DeliveryInventory, InventoryTable, edition_bounds

from .artifact_identity import GENERATION_KEYS, STEWARD_GENERATION_KEYS, generation_id
from .holdings_accounting import DISPOSITIONS
from .holdings_compile import canonical_inventory

if TYPE_CHECKING:
    import sqlite3

HOLDING_RELATIONS = (
    "holding_table",
    "holding_period",
    "holding_column",
    "holding_mapping",
)


def validate_compiled_holdings(conn: sqlite3.Connection) -> None:
    """Validate facts through their authored models and the shared placement gate."""
    manifest = dict(conn.execute("SELECT key, value FROM import_manifest"))
    kind = manifest.get("catalog_artifact_kind")
    if kind == "diagnostic":
        return
    if kind not in {"catalog", "steward"}:
        raise ValueError(
            "Compiled artifact requires catalog_artifact_kind catalog or steward"
        )
    required = {*GENERATION_KEYS, "generation_id"}
    if kind == "steward":
        required |= {
            *STEWARD_GENERATION_KEYS,
            "base_db_sha256",
            "holdings_accounting_counts",
        }
    missing = sorted(required - manifest.keys())
    if missing:
        raise ValueError(f"Artifact manifest keys missing: {', '.join(missing)}")
    for key in required:
        if key.endswith(("sha256", "generation_id")):
            if not re.fullmatch(r"[0-9a-f]{64}", manifest[key]):
                raise ValueError(f"Artifact manifest {key} must be a SHA-256 digest")
        elif key.endswith("commit") and not re.fullmatch(
            r"[0-9a-f]{40}", manifest[key]
        ):
            raise ValueError(f"Artifact manifest {key} must be a full commit")
    if manifest["generation_id"] != generation_id(manifest):
        raise ValueError(
            "Artifact generation_id does not match canonical semantic inputs"
        )
    tables = list(conn.execute("SELECT * FROM holding_table ORDER BY table_id"))
    if kind == "catalog":
        if set(manifest) & {
            *STEWARD_GENERATION_KEYS,
            "base_db_sha256",
            "holdings_accounting_counts",
        }:
            raise ValueError("Catalog artifact has steward-only manifest keys")
        if any(
            conn.execute(f"SELECT COUNT(*) FROM {relation}").fetchone()[0]
            for relation in HOLDING_RELATIONS
        ):
            raise ValueError("Catalog artifact must have empty holding relations")
        return
    if not tables:
        raise ValueError("Steward artifact has no holding tables")
    held_counts = {
        name: {"tables": 0, "columns": 0}
        for name in ("dated", "year_independent", "retained_unknown")
    }
    inventory_tables = []
    stored_canonical = []
    for table in tables:
        scope = table["scope"]
        if table["partition"] is not None:
            validate_slug(table["partition"], "partition")
        if not re.fullmatch(
            r"policy/(?:inventory\.toml:table|holdings_policy\.toml:retain_unknown)\[[0-9]+\]",
            table["source_ref"],
        ):
            raise ValueError("Holding source_ref must locate an accepted policy record")
        rows = list(
            conn.execute(
                "SELECT * FROM holding_column WHERE table_id=? ORDER BY column_id",
                (table["table_id"],),
            )
        )
        if not rows:
            raise ValueError(
                f"Holding table {table['physical_id']!r} has no columns ({table['source_ref']})"
            )
        periods = tuple(
            tuple(row)
            for row in conn.execute(
                "SELECT lo, hi FROM holding_period WHERE table_id=? ORDER BY lo, hi",
                (table["table_id"],),
            )
        )
        columns = []
        for column in rows:
            mappings = []
            for mapping in conn.execute(
                "SELECT hm.*, v.register_id AS variable_register, rv.register_id AS variant_register, v.slug AS variable_slug, rv.slug AS variant_slug, r.slug AS register_slug, p.slug AS provider_slug FROM holding_mapping hm JOIN variable v USING(variable_id) JOIN register_variant rv ON rv.register_variant_id=hm.variant_id JOIN register r ON r.register_id=v.register_id JOIN provider p USING(provider_id) WHERE hm.column_id=? ORDER BY hm.variant_id, hm.variable_id, hm.representation_canonical",
                (column["column_id"],),
            ):
                if mapping["variable_register"] != mapping["variant_register"]:
                    raise ValueError(
                        f"Holding mapping ownership mismatch at column {column['column_id']}"
                    )
                prefix = f"{mapping['provider_slug']}/{mapping['register_slug']}"
                mappings.append(
                    {
                        "register_variant": f"{prefix}/{mapping['variant_slug']}",
                        "variable": f"{prefix}/{mapping['variable_slug']}",
                        "representation": mapping["representation_literal"],
                    }
                )
                stored_canonical.append(mapping["representation_canonical"])
            if scope == "unknown" and mappings:
                raise ValueError(
                    f"Unknown holding {table['physical_id']!r} has mappings"
                )
            if column["unmapped_reason"] is not None and mappings:
                raise ValueError(
                    f"Holding column {column['column_id']} has mappings and unmapped_reason"
                )
            columns.append(
                {
                    "name": column["name"],
                    "mapping": mappings,
                    "unmapped_reason": column["unmapped_reason"],
                }
            )
        if scope == "unknown":
            if (
                periods
                or not table["retain_unknown_reason"]
                or not table["retain_unknown_reason"].strip()
            ):
                raise ValueError(
                    f"Invalid retained unknown holding {table['physical_id']!r}"
                )
        else:
            physical = InventoryTable.model_validate(
                {
                    "id": table["physical_id"],
                    "edition": json.loads(table["edition_json"]),
                    "period_scope": scope,
                    "partition": table["partition"],
                    "column": columns,
                }
            )
            expected = edition_bounds(physical.edition) if scope == "intervals" else ()
            if periods != expected:
                raise ValueError(
                    f"Holding periods disagree with edition at {table['source_ref']} ({table['physical_id']!r})"
                )
            inventory_tables.append(physical)
        disposition = (
            "retained_unknown"
            if scope == "unknown"
            else "dated"
            if scope == "intervals"
            else "year_independent"
        )
        held_counts[disposition]["tables"] += 1
        held_counts[disposition]["columns"] += len(rows)
    if inventory_tables:
        inventory = DeliveryInventory(
            steward=manifest["steward"], version=1, table=tuple(inventory_tables)
        )
        canonical = canonical_inventory(conn, inventory)
        expected_canonical = [
            mapping.representation
            for table in canonical.tables
            for column in table.columns
            for mapping in column.mappings
        ]
        if expected_canonical != stored_canonical:
            raise ValueError(
                "Holding canonical representations disagree with resolver representative spelling"
            )
    counts = json.loads(manifest["holdings_accounting_counts"])
    if set(counts) != {"raw", *DISPOSITIONS} or any(
        set(value) != {"tables", "columns"}
        or any(type(number) is not int or number < 0 for number in value.values())
        for value in counts.values()
    ):
        raise ValueError("Malformed holdings_accounting_counts")
    for disposition, actual in held_counts.items():
        if counts[disposition] != actual:
            raise ValueError(f"Holding {disposition} counts disagree with manifest")
    for grain in ("tables", "columns"):
        if counts["raw"][grain] != sum(counts[name][grain] for name in DISPOSITIONS):
            raise ValueError(f"Holding raw {grain} accounting is not a disjoint total")
