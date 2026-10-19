"""Compile physical holdings facts against the derived `resolver_column` universe."""

from __future__ import annotations

import sqlite3
import tomllib
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .data_warnings import DataWarning, keep_warning, mapping_data_warnings
from .db import catalog_coordinate_ids, stored_data_warnings, write_data_warnings
from .holdings_accounting import (
    INVENTORY_ROLE_PATH,
    HoldingsAccounting,
    account_holdings,
)
from .inventory import (
    DeliveryInventory,
    InventoryColumn,
    edition_bounds,
    load_inventory,
    validate_inventory_placements,
)
from .source_evidence import canonical_json
from .steward_spec import scope_locator

if TYPE_CHECKING:
    from pathlib import Path

    from .steward_spec import StewardSpec


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
    conn: sqlite3.Connection,
    inventory: DeliveryInventory,
    *,
    coordinate_ids: tuple[dict[str, int], dict[str, int]] | None = None,
) -> DeliveryInventory:
    """Canonicalize against the resolver's whole-history delivery universe."""
    variables, variants = coordinate_ids or catalog_coordinate_ids(conn)
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
                variable_id = variables.get(mapping.variable)
                variant_id = variants.get(mapping.register_variant)
                if variable_id is None or variant_id is None:
                    rejections.append(
                        f"{locator}: unresolved coordinate {mapping.register_variant} {mapping.variable}"
                    )
                    continue
                key = variable_id, variant_id
                if key not in universes:
                    universes[key] = dict(
                        conn.execute(
                            "SELECT delivery_column_lower, delivery_column_name "
                            "FROM resolver_column "
                            "WHERE variable_id = ? AND register_variant_id = ?",
                            key,
                        )
                    )
                canonical = universes[key].get(mapping.representation.lower())
                if canonical is None:
                    rejections.append(
                        f"{locator}: {mapping.register_variant} {mapping.variable} representation {mapping.representation!r} is not a resolver-emitted delivery column"
                    )
                    continue
                triple = variant_id, variable_id, canonical
                if triple in seen:
                    rejections.append(
                        f"{locator}: duplicate canonical triple {mapping.register_variant} {mapping.variable} {canonical!r}"
                    )
                    continue
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


def _write_mapping_warnings(
    conn: sqlite3.Connection, canonical: DeliveryInventory
) -> None:
    """Add the steward's mapping warnings (#1684) to the base's, in the flavor only.

    `write_data_warnings` writes a whole, empty table pair, so the stored warnings
    and the steward's are merged and rewritten. Base rows keep their content; the
    flavored `validate_built_db` re-derives every row's identity."""
    steward = mapping_data_warnings(canonical.tables)
    if not steward:
        return
    merged: dict[str, DataWarning] = {}
    for warning in (*stored_data_warnings(conn), *steward):
        keep_warning(merged, warning)
    # `data_warning` references its text rows, so it is cleared first.
    conn.execute("DELETE FROM data_warning")
    conn.execute("DELETE FROM data_warning_text")
    write_data_warnings(conn, tuple(merged[k] for k in sorted(merged)))


def compile_holdings(
    conn: sqlite3.Connection,
    root: Path,
    *,
    steward: str,
    spec: StewardSpec,
    accounting: HoldingsAccounting | None = None,
) -> CompiledHoldings:
    """Compile facts, reusing accounting only from the same immutable input snapshot."""
    conn.row_factory = sqlite3.Row
    inventory_path = root / INVENTORY_ROLE_PATH
    inventory = load_inventory(inventory_path)
    if inventory.steward != steward:
        raise ValueError("Accepted inventory steward does not match selected steward")
    accounting = accounting or account_holdings(
        root, inventory, spec=spec, steward=steward
    )
    variables, variants = catalog_coordinate_ids(conn)
    canonical = canonical_inventory(
        conn, inventory, coordinate_ids=(variables, variants)
    )
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
            source_ref = f"{scope_locator(steward)}:retain_unknown[{index}]"
            periods = ()
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
                canonical_json(edition) if edition is not None else None,
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
                member = mapping.member
                lo, hi = (member and member.bounds()) or (None, None)
                window_lo, window_hi = (
                    edition_bounds(mapping.window)[0]
                    if mapping.window is not None
                    else (None, None)
                )
                conn.execute(
                    "INSERT INTO holding_mapping(column_id, variant_id, variable_id, representation_literal, representation_canonical, member_position, member_lo, member_hi, window_lo, window_hi) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        column_id,
                        variants[mapping.register_variant],
                        variables[mapping.variable],
                        literal.representation,
                        mapping.representation,
                        member and member.position,
                        lo,
                        hi,
                        window_lo,
                        window_hi,
                    ),
                )
                mappings_count += 1
    # A second pass: tables insert in physical_id order, so a projection may sort
    # before the parent its foreign key names. One statement sets both columns,
    # which the schema requires together.
    conn.executemany(
        "UPDATE holding_table SET projection_of = ?, projection = ? WHERE physical_id = ?",
        (
            (table.projection.of, table.projection.token, table.id)
            for table in canonical.tables
            if table.projection is not None
        ),
    )
    _write_mapping_warnings(conn, canonical)
    return CompiledHoldings(
        len(logical) + len(unknown), column_id, mappings_count, accounting
    )
