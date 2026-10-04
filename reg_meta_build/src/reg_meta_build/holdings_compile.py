"""Compile physical holdings facts; semantic resolution stays in Catalog."""

from __future__ import annotations

import json
import sqlite3
import tomllib
from dataclasses import dataclass
from typing import TYPE_CHECKING

from reg_meta.catalog import Catalog, representative_columns
from reg_meta.db import register_py_lower
from reg_meta.inventory import (
    DeliveryInventory,
    edition_bounds,
    load_inventory,
    validate_inventory_placements,
)

from .holdings_accounting import HoldingsAccounting, account_holdings

if TYPE_CHECKING:
    from pathlib import Path


class HoldingsFoldError(ValueError):
    """Located rejected mappings, retained locally for separate source review."""

    def __init__(self, rejections: tuple[str, ...]) -> None:
        self.rejections = rejections
        super().__init__(
            f"Holdings fold gate rejected {len(rejections)} mappings:\n"
            + "\n".join(rejections)
        )


@dataclass(frozen=True)
class CompiledHoldings:
    tables: int
    columns: int
    mappings: int
    accounting: HoldingsAccounting


def canonical_inventory(
    conn: sqlite3.Connection, inventory: DeliveryInventory
) -> DeliveryInventory:
    """Canonicalize against the resolver's actual whole-history delivery universe."""
    conn.row_factory = sqlite3.Row
    register_py_lower(conn)
    catalog = Catalog(conn)
    variables = {
        f"{row['provider']}/{row['register_slug']}/{row['slug']}": row["variable_id"]
        for row in conn.execute(
            "SELECT v.variable_id, v.slug, p.slug AS provider, r.slug AS register_slug FROM variable v JOIN register r USING(register_id) JOIN provider p USING(provider_id)"
        )
    }
    variants = {
        f"{row['provider']}/{row['register_slug']}/{row['slug']}": row[
            "register_variant_id"
        ]
        for row in conn.execute(
            "SELECT rv.register_variant_id, rv.slug, p.slug AS provider, r.slug AS register_slug FROM register_variant rv JOIN register r USING(register_id) JOIN provider p USING(provider_id)"
        )
    }
    universes: dict[tuple[int, int], dict[str, str]] = {}
    rejections = []
    tables = []
    for table in inventory.tables:
        columns = []
        for column in table.columns:
            mappings = []
            seen = set()
            for index, mapping in enumerate(column.mappings):
                locator = f"policy/inventory.toml:table[{table.id!r}].column[{column.name!r}].mapping[{index}]"
                variable_id = variables.get(str(mapping.variable))
                variant_id = variants.get(mapping.register_variant)
                if variable_id is None or variant_id is None:
                    rejections.append(
                        f"{locator}: unresolved coordinate {mapping.register_variant} {mapping.variable}"
                    )
                    continue
                key = variable_id, variant_id
                if key not in universes:
                    # Call the authority directly: the consistency-checker's mirror
                    # omits per-column coding replacement/intersection rules.
                    rows = catalog._states_in_bounds(variable_id, variant_id, None)
                    emitted = catalog._expand_state_windows(
                        variable_id,
                        rows,
                        None,
                        with_codes=False,
                        with_code_summary=False,
                    )
                    delivered = {
                        state.delivery_column_name.lower()
                        for state in emitted
                        if state.delivery_column_name is not None
                    }
                    windows = catalog._variable_windows(variable_id).get(variant_id, [])
                    spelling = representative_columns(
                        (row["delivery_column_name"] for row in rows),
                        (window[0] for window in windows),
                    )
                    universes[key] = {
                        fold: name
                        for fold, name in spelling.items()
                        if fold in delivered
                    }
                canonical = universes[key].get(mapping.representation.lower())
                if canonical is None:
                    rejections.append(
                        f"{locator}: {mapping.register_variant} {mapping.variable} representation {mapping.representation!r} is not a resolver-emitted delivery column"
                    )
                    continue
                triple = variant_id, variable_id, canonical
                if triple in seen:
                    raise ValueError(
                        f"{locator}: duplicate canonical triple {mapping.register_variant} {mapping.variable} {canonical!r}"
                    )
                seen.add(triple)
                mappings.append(
                    mapping.model_copy(update={"representation": canonical})
                )
            columns.append(column.model_copy(update={"mappings": tuple(mappings)}))
        tables.append(table.model_copy(update={"columns": tuple(columns)}))
    if rejections:
        raise HoldingsFoldError(tuple(rejections))
    canonical = inventory.model_copy(update={"tables": tuple(tables)})
    validate_inventory_placements(canonical.tables)
    return canonical


def compile_holdings(
    conn: sqlite3.Connection, root: Path, *, steward: str
) -> CompiledHoldings:
    """Insert inventory and retained-unknown census facts after every gate passes."""
    inventory_path = root / "policy/inventory.toml"
    inventory = load_inventory(inventory_path)
    if inventory.steward != steward:
        raise ValueError("Accepted inventory steward does not match selected steward")
    accounting = account_holdings(root, inventory)
    canonical = canonical_inventory(conn, inventory)
    raw = tomllib.loads(inventory_path.read_text(encoding="utf-8"))
    authored = {table["id"]: (index, table) for index, table in enumerate(raw["table"])}
    logical = {table.id: table for table in canonical.tables}
    unknown = {
        entry.table: (index, entry) for index, entry in enumerate(accounting.unknown)
    }
    column_id = mappings_count = 0
    for table_id, physical_id in enumerate(sorted(set(logical) | set(unknown)), 1):
        if physical_id in logical:
            table = logical[physical_id]
            index, original = authored[physical_id]
            edition = original["edition"]
            scope, partition, reason = table.period_scope, table.partition, None
            source_ref = f"policy/inventory.toml:table[{index}]"
            periods = edition_bounds(table.edition) if scope == "intervals" else ()
            columns = table.columns
        else:
            index, entry = unknown[physical_id]
            edition, scope, partition, reason = (
                entry.edition,
                "unknown",
                entry.partition,
                entry.reason,
            )
            source_ref = f"policy/holdings_policy.toml:retain_unknown[{index}]"
            periods = ()
            from reg_meta.inventory import InventoryColumn

            columns = tuple(
                InventoryColumn(name=name)
                for name in sorted(accounting.columns[physical_id])
            )
        conn.execute(
            "INSERT INTO holding_table(table_id, physical_id, scope, edition_json, partition, retain_unknown_reason, source_ref) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                table_id,
                physical_id,
                scope,
                json.dumps(
                    edition, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                )
                if edition is not None
                else None,
                partition,
                reason,
                source_ref,
            ),
        )
        conn.executemany(
            "INSERT INTO holding_period(table_id, lo, hi) VALUES (?, ?, ?)",
            ((table_id, lo, hi) for lo, hi in periods),
        )
        literal_columns = (
            {column.name: column for column in inventory.tables[index].columns}
            if physical_id in logical
            else {}
        )
        for column in sorted(columns, key=lambda c: c.name):
            column_id += 1
            conn.execute(
                "INSERT INTO holding_column(column_id, table_id, name, unmapped_reason) VALUES (?, ?, ?, ?)",
                (column_id, table_id, column.name, column.unmapped_reason),
            )
            literals = (
                literal_columns[column.name].mappings if physical_id in logical else ()
            )
            for literal, mapping in zip(literals, column.mappings, strict=True):
                provider, register, variant = mapping.register_variant.split("/")
                variable = conn.execute(
                    "SELECT v.variable_id FROM variable v JOIN register r USING(register_id) JOIN provider p USING(provider_id) WHERE p.slug=? AND r.slug=? AND v.slug=?",
                    (provider, register, mapping.variable.variable),
                ).fetchone()
                variant_row = conn.execute(
                    "SELECT rv.register_variant_id FROM register_variant rv JOIN register r USING(register_id) JOIN provider p USING(provider_id) WHERE p.slug=? AND r.slug=? AND rv.slug=?",
                    (provider, register, variant),
                ).fetchone()
                conn.execute(
                    "INSERT INTO holding_mapping(column_id, variant_id, variable_id, representation_literal, representation_canonical) VALUES (?, ?, ?, ?, ?)",
                    (
                        column_id,
                        variant_row[0],
                        variable[0],
                        literal.representation,
                        mapping.representation,
                    ),
                )
                mappings_count += 1
    return CompiledHoldings(
        len(logical) + len(unknown), column_id, mappings_count, accounting
    )


def write_holdings_assessment_warnings(conn: sqlite3.Connection) -> int:
    """Keep the range/list build-assessment disposition without inventory witnesses."""
    from reg_meta.catalog import DataWarning
    from reg_meta.source_evidence import canonical_sha256

    from .data_warnings import write_data_warnings

    warnings = []
    for provider, register, count in conn.execute(
        "SELECT p.slug, r.slug, COUNT(DISTINCT ht.table_id) "
        "FROM holding_table ht JOIN holding_column hc USING(table_id) "
        "JOIN holding_mapping hm USING(column_id) JOIN variable v USING(variable_id) "
        "JOIN register r USING(register_id) JOIN provider p USING(provider_id) "
        "WHERE ht.scope='intervals' AND json_type(ht.edition_json) IN ('array', 'object') "
        "GROUP BY p.slug, r.slug ORDER BY p.slug, r.slug"
    ):
        payload = {
            "register_fqid": f"{provider}/{register}",
            "variable_fqid": None,
            "variant": None,
            "delivery_column_name": None,
            "valid_from": None,
            "valid_to": None,
            "code": "holding_temporally_unassessed",
            "severity": "warning",
            "summary": "Range/list holdings are not column-availability evidence",
            "detail": f"{count} range/list table(s) retain their physical periods. Semantic applicability is assessed by the resolver at query time.",
            "source_subject": f"{provider}/{register}",
            "fields": ["inventory.edition"],
            "refs": [],
            "withheld_output": ["build_column_window_coverage"],
            "acknowledged_by": "source_policy",
            "case_id": "holding_temporally_unassessed",
        }
        payload["diagnostic_detail_sha256"] = canonical_sha256(payload["detail"])
        warnings.append(
            DataWarning.model_validate_json(
                json.dumps({"warning_id": canonical_sha256(payload), **payload})
            )
        )
    write_data_warnings(conn, tuple(warnings))
    return len(warnings)
