"""Steward-side check of curated global data types against the steward's storage.

A steward's storage-schema export (table, column, SQL type; no row content) is
steward-derived, so the global build never reads it (#1562). The steward
generator's `inventory` stage runs this check instead: it compares every curated
global data type on a register a steward series names against the storage types
of the same folded column in that series' tables, and reports disagreements as a
worklist. Curation, not this check, decides what changes.
"""

from __future__ import annotations

import csv
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from reg_meta_build._curation import (
    data_type_class,
    fold_column,
    widen_data_type_classes,
)

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

    from reg_meta_build.curation_tree import RegisterCuration
    from reg_meta_build.steward_spec import StewardSpec

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
    """The storage-schema export or the spec's storage headers violate their contract."""


class StoragePolicyRegister(BaseModel):
    """One register's steward storage tables and physical header spellings.

    ``tables`` maps each member table to its curated columns stored under
    another header, where the two differ beyond the column fold (FoU's
    ``v0201`` is stored as ``V201``). A header applies only to the table it is
    keyed under.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    tables: dict[str, dict[str, str]] = Field(min_length=1)

    @field_validator("tables")
    @classmethod
    def _literal(cls, value: dict[str, dict[str, str]]) -> dict[str, dict[str, str]]:
        for table, headers in value.items():
            names = [table, *headers, *headers.values()]
            if any(not name or name != name.strip() for name in names):
                raise ValueError("tables and headers must be non-empty literal names")
            if len({fold_column(column) for column in headers}) != len(headers):
                raise ValueError(
                    f"storage columns of {table} must be unique after the column fold"
                )
        return value


def storage_policy_from_spec(spec: StewardSpec) -> dict[str, StoragePolicyRegister]:
    """`provider/slug` → its series' member tables, each with the
    `storage_header` spellings the decisions select for it.

    Each series' register is checked against the series' own tables."""
    tables: dict[str, dict[str, dict[str, str]]] = defaultdict(dict)
    for series in spec.series:
        if series.register_coord is None:
            continue
        members = tables[series.register_coord]
        for table in series.tables:
            members[table.table] = {}
        for (table, column), header in series.storage_headers().items():
            members[table][column] = header
    return {
        register: StoragePolicyRegister(tables=dict(sorted(members.items())))
        for register, members in sorted(tables.items())
    }


def read_storage_types(path: Path) -> dict[tuple[str, str], str]:
    """Read (table, folded column) → SQL type; only schema metadata is read.

    A missing or unreadable export is a `StorageSchemaError`: the check never
    skips (#1594)."""
    result: dict[tuple[str, str], str] = {}
    try:
        with path.open(encoding="utf-8", newline="") as handle:
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
    except OSError as exc:
        raise StorageSchemaError(
            f"storage-schema export {path} is unreadable ({exc.strerror}); pass the "
            "steward's information_schema column export as --storage-csv"
        ) from exc
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
    ``errata.delivered`` column the register's storage tables do not hold, so
    nothing checked it (a misspelt header, a missing ``storage_header`` decision
    or a table outside the prefixes). A documented (``scb-docs``) type is
    compared only where storage holds its column.
    """

    kind: FindingKind
    register: str
    location: str
    column: str
    curated: str | None
    storage: str


def _storage_text(tables: dict[str, str]) -> str:
    return ", ".join(f"{table}={tables[table]}" for table in sorted(tables))


def storage_type_findings(
    registers: Iterable[RegisterCuration],
    policy: dict[str, StoragePolicyRegister],
    storage: dict[tuple[str, str], str],
) -> tuple[StorageFinding, ...]:
    """Compare curated register data types with the prefix-selected storage."""
    findings: list[StorageFinding] = []
    for register in sorted(registers, key=lambda item: item.source_file):
        name = f"{register.register_info.provider}/{register.register_info.slug}"
        selected = policy.get(name)
        if selected is None:
            continue
        # simplify: member ids are matched as prefixes, so a storage table whose
        # name extends a member id is checked too, under the longest member it
        # extends; give the check an exact table set if storage ever holds such
        # a table outside the series.
        members = sorted(selected.tables, key=len, reverse=True)
        headers = {
            member: {
                fold_column(column): fold_column(header)
                for column, header in columns.items()
            }
            for member, columns in selected.tables.items()
        }
        # (member, storage column) -> {storage table: SQL type}
        by_column: dict[tuple[str, str], dict[str, str]] = {}
        for (table, column), sql_type in storage.items():
            member = next((m for m in members if table.startswith(m)), None)
            if member is not None:
                by_column.setdefault((member, column), {})[table] = sql_type
        curated: list[tuple[str, str, str | None, bool]] = [
            (
                f"{register.source_file}#/errata.column/{index}",
                entry.column,
                entry.data_type,
                entry.source == "steward-holdings",
            )
            for index, entry in enumerate(register.errata.column, 1)
        ]
        curated += [
            (
                f"{register.source_file}#/errata.delivered/{index}",
                entry.column,
                entry.data_type,
                # A typed delivered erratum is an additional physical column
                # typed from steward storage, so storage must hold it.
                True,
            )
            for index, entry in enumerate(register.errata.delivered, 1)
            if entry.data_type is not None
        ]
        for location, column, data_type, holding in curated:
            if data_type is None and not holding:
                continue
            folded = fold_column(column)
            # Each member table is read under its own header for the column.
            tables = {
                table: sql_type
                for member in members
                for table, sql_type in by_column.get(
                    (member, headers[member].get(folded, folded)), {}
                ).items()
            }
            kind: FindingKind | None
            if not tables:
                # Only a steward-held column is expected in storage; a column
                # SCB documents need not be one SWECOV holds.
                if holding:
                    findings.append(
                        StorageFinding(
                            "unmatched", name, location, column, data_type, ""
                        )
                    )
                continue
            classes = {data_type_class(kind) for kind in tables.values()}
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
                findings.append(
                    StorageFinding(
                        kind, name, location, column, data_type, _storage_text(tables)
                    )
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
