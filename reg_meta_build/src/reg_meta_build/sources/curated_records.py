"""Source observations from the maintained thin-provider TOML layout.

The seven selected global TOMLs share this format. Reading a declaration neither
inherits parent bounds nor supplies a default variant, flag, coding or identity.
The independently pinned source bytes retain comments and TOML spelling; delivered
cells retain decoded TOML values and their types at exact table/key coordinates.
"""

from __future__ import annotations

import json
import re
import tomllib
from dataclasses import dataclass, field
from datetime import date
from types import MappingProxyType
from typing import TYPE_CHECKING, Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from reg_meta_build.normalization import normalize_text, normalize_token
from reg_meta_build.source_records import (
    DeliveredCell,
    NativeCoordinates,
    RecordLocator,
    ScopeInterval,
    SourceCoordinate,
    SourceEvidenceRow,
    SourceEvidenceTable,
    SourceField,
    SourceFieldCells,
    SourceFields,
    SourceParentObservation,
    SourceRecord,
    SourceRevision,
    SourceSubject,
    TemporalScope,
    value_field,
)
from reg_meta_build.sources.code_lists import read_selected_bytes

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

    from reg_meta_build.source_values import (
        SourceValue,
        SourceValueAssociation,
        SourceValueDescriptor,
    )


class CuratedSourceError(ValueError):
    """A selected authored input violates its actual file-format contract."""


class _Declaration(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


_Nonempty = Annotated[str, Field(min_length=1)]


class _Variant(_Declaration):
    key: _Nonempty
    name: _Nonempty
    description: str | None = None
    valid_from: str | None = None
    valid_to: str | None = None


class _Variable(_Declaration):
    name: _Nonempty
    column: _Nonempty
    definition: str | None = None
    description: str | None = None
    data_type: str | None = None
    measurement_unit: str | None = None
    is_identifier: bool | None = None
    is_sensitive: bool | None = None
    valid_from: str | None = None
    valid_to: str | None = None
    variants: list[str] | None = None
    classification: str | None = None
    value_set: str | None = None


class _Register(_Declaration):
    key: _Nonempty
    name: _Nonempty
    purpose: str | None = None
    valid_from: str | None = None
    valid_to: str | None = None
    variant: list[_Variant] = Field(default_factory=list)
    variable: list[_Variable] = Field(default_factory=list)


class _Document(_Declaration):
    registers: list[_Register] = Field(alias="register", min_length=1)


@dataclass(frozen=True)
class CleanedCuratedSource:
    """Declared facts and raw evidence, with source-local value occurrences only."""

    revision: SourceRevision
    records: tuple[SourceRecord, ...]
    tables: tuple[SourceEvidenceTable, ...]
    declared_value_lists: tuple[str, ...] = ()
    descriptors: Mapping[str, SourceValueDescriptor] = field(
        default_factory=lambda: MappingProxyType({})
    )
    values: Mapping[str, SourceValue] = field(
        default_factory=lambda: MappingProxyType({})
    )
    associations: tuple[SourceValueAssociation, ...] = ()


def _text(
    entry: Mapping[str, Any], key: str, *, token: bool = False, multiline: bool = False
) -> SourceField | None:
    if key not in entry:
        return None
    raw = entry[key]
    normalized = (
        normalize_token(raw) if token else normalize_text(raw, multiline=multiline)
    )
    if not normalized:
        return SourceField(status="unknown", raw_value=raw)
    return value_field(normalized, raw=raw)


def _flag(entry: Mapping[str, Any], key: str) -> SourceField | None:
    return value_field(entry[key], raw=entry[key]) if key in entry else None


def _cells(entry: Mapping[str, Any]) -> tuple[DeliveredCell, ...]:
    result = []
    for key, raw in entry.items():
        # Child tables have their own evidence rows and locators. An explicitly
        # empty table array still needs a cell to distinguish it from omission.
        if key in {"register", "variant", "variable"} and raw:
            continue
        raw_text = (
            raw
            if isinstance(raw, str)
            else json.dumps(raw, ensure_ascii=False, separators=(",", ":"))
        )
        result.append(
            DeliveredCell(
                name=key,
                present=True,
                raw_value=raw_text,
                interpreted_value=raw_text,
                raw_type=type(raw).__name__,
                storage_type="toml",
            )
        )
    return tuple(result)


def _locator(
    revision: SourceRevision,
    table: str,
    path: str,
    key: tuple[str, ...],
    cells: tuple[DeliveredCell, ...],
) -> RecordLocator:
    return RecordLocator(
        semantic_record_key=key,
        physical_file=revision.artifact_path,
        physical_table=table,
        physical_record=path,
        physical_cells=tuple(f"{path}.{cell.name}" for cell in cells),
    )


def _scope(entry: Mapping[str, Any]) -> TemporalScope:
    start, end = entry.get("valid_from"), entry.get("valid_to")
    if start is None and end is None:
        return TemporalScope(kind="not_applicable")
    if start is not None and end is not None:
        start, end = normalize_token(start), normalize_token(end)
        try:
            if not all(
                re.fullmatch(r"\d{4}-\d{2}-\d{2}", value) for value in (start, end)
            ):
                raise ValueError("not ISO dates")
            if date.fromisoformat(start) > date.fromisoformat(end):
                raise ValueError("reversed dates")
        except ValueError:
            return TemporalScope(
                kind="unknown",
                label="Invalid or conflicting declared valid_from/valid_to; original bounds retained",
            )
        return TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start=start, end=end),)
        )
    return TemporalScope(
        kind="unknown",
        label="Only one authored date bound; no inherited or inferred endpoint",
    )


def _coordinate(key: str, name: str | None = None) -> SourceCoordinate:
    native = normalize_token(key)
    if not native:
        raise CuratedSourceError("declared coordinate must not be whitespace-only")
    return SourceCoordinate(
        status="value", native_id=native, name=normalize_text(name) if name else None
    )


def _record(
    revision: SourceRevision,
    *,
    provider: str,
    register: Mapping[str, Any],
    entry: Mapping[str, Any],
    kind: str,
    path: str,
) -> SourceRecord:
    reg = _coordinate(register["key"], register["name"])
    key = (f"register:{reg.native_id}",)
    absent = SourceCoordinate(status="not_applicable")
    variable = member = absent
    variant = absent
    references = ()
    if kind == "variant":
        variant = _coordinate(entry["key"], entry["name"])
        key += (f"variant:{variant.native_id}",)
    elif kind == "variable":
        variable = _coordinate(entry["column"], entry["name"])
        member = _coordinate(entry["column"], entry["name"])
        references = tuple(
            _coordinate(reference) for reference in entry.get("variants", ())
        )
        key += (f"variable:{variable.native_id}",)
    cells = _cells(entry)
    table = "register" if kind == "register" else f"register.{kind}"
    fields = SourceFields(
        availability=value_field(True) if kind == "variable" else None,
        name=_text(entry, "name"),
        column_name=_text(entry, "column", token=True),
        definition=_text(entry, "definition", multiline=True),
        description=_text(entry, "description", multiline=True),
        purpose=_text(entry, "purpose", multiline=True),
        data_type=_text(entry, "data_type", token=True),
        measurement_unit=_text(entry, "measurement_unit"),
        sensitivity=_flag(entry, "is_sensitive"),
        identifier=_flag(entry, "is_identifier"),
        classification_declared=_text(entry, "classification", token=True),
        value_set_declared=_text(entry, "value_set", token=True),
        coverage_from=_text(entry, "valid_from", token=True),
        coverage_to=_text(entry, "valid_to", token=True),
    )
    return SourceRecord.create(
        revision=revision,
        locators=(_locator(revision, table, path, key, cells),),
        subject=SourceSubject(
            provider=provider,
            register=reg,
            variant=variant,
            variant_references=references,
            population=SourceCoordinate(status="unknown"),
            variable=variable,
            member=member,
            native=NativeCoordinates(),
        ),
        # This source declares coverage bounds, not annual delivery editions.
        edition_scope=TemporalScope(kind="not_applicable"),
        edition_period_scope=_scope(entry),
        fields=fields if kind == "variable" else SourceFields(),
        parent_facts=()
        if kind == "variable"
        else (
            SourceParentObservation(
                kind="register" if kind == "register" else "variant",
                coordinate=reg if kind == "register" else variant,
                register=reg,
                variant=variant if kind == "variant" else None,
                fields=fields,
                field_cells=tuple(
                    SourceFieldCells(
                        field=name,
                        positions=(
                            next(
                                index
                                for index, cell in enumerate(cells)
                                if cell.name
                                == {
                                    "coverage_from": "valid_from",
                                    "coverage_to": "valid_to",
                                }.get(name, name)
                            ),
                        ),
                    )
                    for name in SourceFields.model_fields
                    if getattr(fields, name) is not None
                ),
            ),
        ),
        delivered_cells=cells,
    )


def read_curated_source(
    path: Path, revision: SourceRevision, *, provider: str
) -> CleanedCuratedSource:
    """Read the selected global TOML layout without catalog interpretation."""
    payload = read_selected_bytes(path, revision, error_type=CuratedSourceError)
    try:
        raw = tomllib.loads(payload.decode("utf-8"))
        _Document.model_validate(raw)
    except (UnicodeDecodeError, tomllib.TOMLDecodeError, ValidationError) as exc:
        raise CuratedSourceError(
            f"invalid global curated TOML {revision.artifact_path}: {exc}"
        ) from exc
    records = []
    table_rows: dict[str, list[SourceEvidenceRow]] = {}
    references = []
    for reg_index, register in enumerate(raw["register"]):
        declarations = [("register", f"register[{reg_index}]", register)]
        for kind in ("variant", "variable"):
            declarations.extend(
                (kind, f"register[{reg_index}].{kind}[{index}]", entry)
                for index, entry in enumerate(register.get(kind, ()))
            )
        for kind, table_path, entry in declarations:
            record = _record(
                revision,
                provider=provider,
                register=register,
                entry=entry,
                kind=kind,
                path=table_path,
            )
            records.append(record)
            locator = record.locators[0]
            table_rows.setdefault(locator.physical_table, []).append(
                SourceEvidenceRow(
                    locator=locator, role="declaration", cells=record.delivered_cells
                )
            )
            if kind == "variable" and (name := entry.get("value_set")):
                references.append(normalize_token(name))
    return CleanedCuratedSource(
        revision=revision,
        records=tuple(records),
        tables=tuple(
            SourceEvidenceTable(
                source=revision.dataset,
                source_revision_id=revision.revision_id,
                name=name,
                rows=tuple(rows),
            )
            for name, rows in table_rows.items()
        ),
        declared_value_lists=tuple(dict.fromkeys(references)),
    )


__all__ = [
    "CleanedCuratedSource",
    "CuratedSourceError",
    "read_curated_source",
]
