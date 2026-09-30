"""SWECOV storage-schema observations; no MONA row content is read."""

from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from reg_meta.source_evidence import (
    DeliveredCell,
    RecordLocator,
    SourceField,
    SourceRevision,
)

from reg_meta_build._curation import (
    data_type_class,
    fold_column,
    widen_data_type_classes,
)
from reg_meta_build.source_records import value_field
from reg_meta_build.source_reference_records import (
    CleanedSourceReferences,
    SourceColumnTypeDeclaration,
)
from reg_meta_build.sources.code_lists import read_selected_bytes

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path


SWECOV_COLUMN_TYPES_PATH = "catalog/swecov/derived/swecov_column_types.csv"
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
_NUMERIC_FIELDS = _HEADER[5:]


class SwecovColumnTypesError(ValueError):
    """Selected SWECOV schema CSV violates its structural contract."""


def storage_class(sql_type: str) -> str | None:
    """Map supported SQL storage types to the catalog's coarse vocabulary."""
    return data_type_class(sql_type)


def read_swecov_column_types(
    path: Path, revision: SourceRevision
) -> CleanedSourceReferences:
    """Read only schema metadata, preserving typed cells and source locators."""
    payload = read_selected_bytes(path, revision, error_type=SwecovColumnTypesError)
    try:
        rows = csv.reader(io.StringIO(payload.decode("utf-8"), newline=""), strict=True)
        if tuple(next(rows)) != _HEADER:
            raise SwecovColumnTypesError(
                f"invalid SWECOV column header in {revision.artifact_path}"
            )
        declarations = []
        seen: set[tuple[str, str]] = set()
        for row_number, row in enumerate(rows, start=2):
            if len(row) != len(_HEADER):
                raise SwecovColumnTypesError(
                    f"SWECOV row {row_number} has {len(row)} cells; expected {len(_HEADER)}"
                )
            values = dict(zip(_HEADER, row, strict=True))
            if any(not values[key] for key in _HEADER[:5]):
                raise SwecovColumnTypesError(
                    f"SWECOV row {row_number} has an empty required text field"
                )
            if values["table_type"] not in {"BASE TABLE", "VIEW"}:
                raise SwecovColumnTypesError(
                    f"SWECOV row {row_number} has invalid table_type {values['table_type']!r}"
                )
            if any(
                values[key] and re.fullmatch(r"-?[0-9]+", values[key]) is None
                for key in _NUMERIC_FIELDS
            ):
                raise SwecovColumnTypesError(
                    f"SWECOV row {row_number} has a noninteger numeric field"
                )
            table, column = values["table_name"], values["column_name"]
            key = table, fold_column(column)
            if key in seen:
                raise SwecovColumnTypesError(
                    f"SWECOV row {row_number} repeats table and folded column {key!r}"
                )
            seen.add(key)
            cells = tuple(
                DeliveredCell(
                    name=name,
                    present=True,
                    raw_value=raw,
                    interpreted_value=raw,
                    raw_type="str",
                    storage_type="csv",
                )
                for name, raw in zip(_HEADER, row, strict=True)
            )
            declarations.append(
                SourceColumnTypeDeclaration(
                    revision=revision,
                    locator=RecordLocator(
                        semantic_record_key=("swecov_column_type", table, key[1]),
                        physical_file=revision.artifact_path,
                        physical_table=table,
                        physical_record=f"row:{row_number}",
                        physical_cells=tuple(
                            f"row:{row_number}:cell:{index}"
                            for index in range(1, len(_HEADER) + 1)
                        ),
                    ),
                    delivered_cells=cells,
                    table_name=value_field(table, raw=table),
                    column_name=value_field(column, raw=column),
                    data_type=value_field(values["data_type"], raw=values["data_type"]),
                    declared_width=SourceField(status="unknown"),
                    nullable=SourceField(status="unknown"),
                )
            )
    except (UnicodeDecodeError, csv.Error, StopIteration) as exc:
        raise SwecovColumnTypesError(
            f"invalid SWECOV column CSV in {revision.artifact_path}: {exc}"
        ) from exc
    return CleanedSourceReferences(revision, tuple(declarations), ())


def index_swecov_column_types(
    declarations: Iterable[SourceColumnTypeDeclaration],
) -> dict[tuple[str, str], SourceColumnTypeDeclaration]:
    """Index exact table names and folded columns, independent of CSV row order."""
    result = {}
    for declaration in declarations:
        table = declaration.table_name.value
        column = declaration.column_name.value
        if not isinstance(table, str) or not isinstance(column, str):
            raise SwecovColumnTypesError(
                "SWECOV declaration lacks a table or column name"
            )
        key = table, fold_column(column)
        if key in result:
            raise SwecovColumnTypesError(
                f"duplicate SWECOV table and folded column {key!r}"
            )
        result[key] = declaration
    return dict(sorted(result.items()))


def steward_column_types(
    prefixes: tuple[str, ...],
    declarations: dict[tuple[str, str], SourceColumnTypeDeclaration],
) -> dict[tuple[str, str], SourceColumnTypeDeclaration]:
    """Select only a register's literal SWECOV table prefixes."""
    return {
        key: declaration
        for key, declaration in declarations.items()
        if prefixes and key[0].startswith(prefixes)
    }


@dataclass(frozen=True)
class StewardColumnStorage:
    classes: frozenset[str | None]
    provenance: str


def index_steward_column_storage(
    prefixes: tuple[str, ...],
    declarations: dict[tuple[str, str], SourceColumnTypeDeclaration],
) -> dict[str, StewardColumnStorage]:
    """Index every wave's class and table provenance by folded column."""
    by_column: dict[str, dict[str, str]] = {}
    for (table, column), declaration in steward_column_types(
        prefixes, declarations
    ).items():
        sql_type = declaration.data_type.value
        if not isinstance(sql_type, str):
            raise SwecovColumnTypesError("SWECOV declaration lacks a data type")
        by_column.setdefault(column, {})[table] = sql_type
    return {
        column: StewardColumnStorage(
            classes=frozenset(storage_class(kind) for kind in tables.values()),
            provenance=f"SWECOV storage {SWECOV_COLUMN_TYPES_PATH}: "
            + ", ".join(f"{table}={tables[table]}" for table in sorted(tables)),
        )
        for column, tables in sorted(by_column.items())
    }


def steward_column_storage_classes(
    column: str,
    prefixes: tuple[str, ...],
    declarations: dict[tuple[str, str], SourceColumnTypeDeclaration],
) -> tuple[set[str], str | None]:
    """Return matching storage classes and sorted per-table provenance."""
    folded = fold_column(column)
    selected = steward_column_types(prefixes, declarations)
    by_table = {
        table: declaration.data_type.value
        for (table, name), declaration in selected.items()
        if name == folded
    }
    if not by_table:
        return set(), None
    evidence = ", ".join(f"{table}={by_table[table]}" for table in sorted(by_table))
    classes = {
        kind
        for sql_type in by_table.values()
        if isinstance(sql_type, str)
        if (kind := storage_class(sql_type)) is not None
    }
    return classes, f"SWECOV storage {SWECOV_COLUMN_TYPES_PATH}: {evidence}"


def infer_steward_column_type(
    column: str,
    prefixes: tuple[str, ...],
    declarations: dict[tuple[str, str], SourceColumnTypeDeclaration],
) -> tuple[str | None, str | None]:
    """Widen all matching waves; return the sorted storage evidence as provenance."""
    classes, evidence = steward_column_storage_classes(column, prefixes, declarations)
    return widen_data_type_classes(classes), evidence
