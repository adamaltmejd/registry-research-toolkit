"""Steward-side check of curated global data types against the steward's storage.

A steward's storage-schema export (table, column, SQL type; no row content) is
steward-derived, so the global build never reads it (#1562). The steward
generator's `inventory` stage runs this check instead: it compares every curated
global data type on a register the generated inventory maps against the storage
types of the physical columns the inventory maps to that entry's variable, and
reports disagreements as a worklist. Curation, not this check, decides what
changes.
"""

from __future__ import annotations

import csv
import io
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from reg_meta_build._curation import (
    data_type_class,
    fold_column,
    widen_data_type_classes,
)
from reg_meta_build.source_naming import accepted_variable_slugs

if TYPE_CHECKING:
    from pathlib import Path

    from reg_meta_build.curation_tree import CurationTree
    from reg_meta_build.inventory import DeliveryInventory

_HEADER = (
    "table_schema",
    "table_name",
    "table_type",
    "column_name",
    "data_type",
    "character_maximum_length",
    "numeric_precision",
    "numeric_scale",
    "ordinal_position",
)


class StorageSchemaError(ValueError):
    """The storage-schema export violates its contract."""


def read_storage_types(path: Path) -> dict[tuple[str, str], str]:
    """Read (table, folded column) → SQL type; only schema metadata is read.

    A missing or unreadable export is a `StorageSchemaError`: the check never
    skips (#1594)."""
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        reason = exc.strerror if isinstance(exc, OSError) else str(exc)
        raise StorageSchemaError(
            f"storage-schema export {path} is unreadable ({reason}); pass the "
            "steward's information_schema column export as --storage-csv"
        ) from exc
    return parse_storage_types(text, str(path))


def parse_storage_types(text: str, path: str) -> dict[tuple[str, str], str]:
    """`read_storage_types` over already-read export text; `path` names it in errors."""
    result: dict[tuple[str, str], str] = {}
    try:
        with io.StringIO(text, newline="") as handle:
            rows = csv.reader(handle, strict=True)
            if tuple(next(rows)) != _HEADER:
                raise StorageSchemaError(f"invalid storage-schema header in {path}")
            for row_number, row in enumerate(rows, start=2):
                if len(row) != len(_HEADER):
                    raise StorageSchemaError(
                        f"{path} row {row_number} has {len(row)} cells; expected {len(_HEADER)}"
                    )
                values = dict(zip(_HEADER, row, strict=True))
                if any(not values[key] for key in _HEADER[:5]):
                    raise StorageSchemaError(
                        f"{path} row {row_number} has an empty required text field"
                    )
                if values["table_type"] not in {"BASE TABLE", "VIEW"}:
                    raise StorageSchemaError(
                        f"{path} row {row_number} has invalid table_type {values['table_type']!r}"
                    )
                if any(
                    values[key] and re.fullmatch(r"-?[0-9]+", values[key]) is None
                    for key in _HEADER[5:]
                ):
                    raise StorageSchemaError(
                        f"{path} row {row_number} has a noninteger numeric field"
                    )
                key = values["table_name"], fold_column(values["column_name"])
                if key in result:
                    raise StorageSchemaError(
                        f"{path} row {row_number} repeats table and folded column {key!r}"
                    )
                result[key] = values["data_type"]
    except (UnicodeDecodeError, csv.Error, StopIteration) as exc:
        raise StorageSchemaError(
            f"invalid storage-schema export {path}: {exc}"
        ) from exc
    return dict(sorted(result.items()))


type FindingKind = Literal["disagrees", "narrower", "uncurated", "unmatched"]


@dataclass(frozen=True)
class StorageFinding:
    """One curated type and the storage evidence for the same register column.

    ``disagrees``: storage holds a type the curated value does not cover (or an
    SQL type outside the catalog vocabulary). ``narrower``: storage is strictly
    narrower than the curated type, which documented Datatyp can legitimately
    widen. ``uncurated``: a steward-holdings column with no curated type that
    storage could type. ``unmatched``: a steward holding or typed
    ``errata.delivered`` column whose inventory cells storage does not hold, so
    nothing checked it (the inventory maps it nowhere, or storage spells or
    places the mapped column differently). A documented (``scb-docs``) type is
    compared only where storage holds its column.
    """

    kind: FindingKind
    register: str
    location: str
    column: str
    curated: str | None
    storage: str


def storage_type_findings(
    tree: CurationTree,
    inventory: DeliveryInventory,
    storage: dict[tuple[str, str], str],
) -> tuple[StorageFinding, ...]:
    """Compare curated register data types with the storage of their own cells.

    An entry's cells are the physical columns the generated inventory maps to
    its variable on its variant under its column (#1666). A minted variable
    covers only its own versions, and the same literal can be another variable
    in other versions (#1647), so another version's table of that literal is not
    evidence. Storage is read under each cell's physical header, which the
    inventory carries for a renamed column, a pseudonymised key and every member
    of a wide pivot."""
    # (register, folded representation) -> [(variant, variable leaf, (table, header))]
    cells: dict[tuple[str, str], list[tuple[str, str, tuple[str, str]]]] = defaultdict(
        list
    )
    for table in inventory.tables:
        for column in table.columns:
            for mapping in column.mappings:
                register, variant = mapping.register_variant.rsplit("/", 1)
                cells[register, fold_column(mapping.representation)].append(
                    (
                        variant,
                        mapping.variable.rsplit("/", 1)[1],
                        (table.id, fold_column(column.name)),
                    )
                )
    mapped = {register for register, _ in cells}
    findings: list[StorageFinding] = []
    for register in sorted(tree.registers, key=lambda item: item.source_file):
        name = f"{register.register_info.provider}/{register.register_info.slug}"
        if name not in mapped:
            continue
        accepted = accepted_variable_slugs(tree, register)
        # SCB entries name the variant by slug. A non-SCB entry names its source
        # variant label instead (#1633), which only the build resolves, so its
        # variable identity alone scopes its cells.
        slugs = {variant.slug for variant in register.variant} or {"_default"}
        native = register.register_info.native_id
        # A mint's variable is named by its `<register>.<column>` source id,
        # whose slug every curating provider pins.
        mints = {
            fold_column(entry.column): accepted.get(
                f"{native}.{entry.column}", (None,)
            )[0]
            for entry in register.errata.column
        }
        # (location, variant, column, data_type, steward-held, minted)
        curated: list[tuple[str, str, str, str | None, bool, bool]] = [
            (
                f"{register.source_file}#/errata.column/{index}",
                entry.variant,
                entry.column,
                entry.data_type,
                entry.source == "steward-holdings",
                True,
            )
            for index, entry in enumerate(register.errata.column, 1)
        ]
        curated += [
            (
                f"{register.source_file}#/errata.delivered/{index}",
                entry.variant,
                entry.column,
                entry.data_type,
                # A typed delivered erratum is an additional physical column
                # typed from steward storage, so storage must hold it.
                True,
                False,
            )
            for index, entry in enumerate(register.errata.delivered, 1)
            if entry.data_type is not None
        ]
        for location, variant, column, data_type, holding, minted in curated:
            if data_type is None and not holding:
                continue
            mint = mints.get(fold_column(column))
            held = {
                (table, storage[table, header])
                for on, leaf, (table, header) in cells.get(
                    (name, fold_column(column)), ()
                )
                if (on == variant or variant not in slugs)
                # A delivered column's variable is the documented one, never
                # the register's mint of that column, which holds the other
                # versions (#1647).
                and (leaf == mint if minted else leaf != mint)
                and (table, header) in storage
            }
            kind: FindingKind | None
            if not held:
                # Only a steward-held column is expected in storage; a column
                # SCB documents need not be one SWECOV holds.
                if holding:
                    findings.append(
                        StorageFinding(
                            "unmatched", name, location, column, data_type, ""
                        )
                    )
                continue
            classes = {data_type_class(sql_type) for _, sql_type in held}
            if data_type is None:
                kind = "uncurated"
            elif (
                None in classes
                or widen_data_type_classes(
                    {data_type, *(item for item in classes if item is not None)}
                )
                != data_type
            ):
                kind = "disagrees"
            elif widen_data_type_classes(c for c in classes if c) != data_type:
                kind = "narrower"
            else:
                kind = None
            if kind is not None:
                text = ", ".join(f"{table}={sql}" for table, sql in sorted(held))
                findings.append(
                    StorageFinding(kind, name, location, column, data_type, text)
                )
    return tuple(findings)


def write_storage_findings(findings: tuple[StorageFinding, ...], path: Path) -> None:
    """Write the findings as a deterministic TSV worklist."""
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
        writer.writerow(
            ("kind", "register", "location", "column", "curated", "storage")
        )
        for item in findings:
            writer.writerow(
                (
                    item.kind,
                    item.register,
                    item.location,
                    item.column,
                    item.curated or "",
                    item.storage,
                )
            )
