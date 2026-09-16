"""Source-faithful variable records from parsed Socialstyrelsen workbooks.

This boundary performs only source-format cleaning. It does not invent a missing
deldatamängd, group same-name rows, choose a code list, or assign catalog identity.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from reg_meta_build.normalization import normalize_text, normalize_token
from reg_meta_build.source_records import (
    DeliveredCell,
    NativeCoordinates,
    RecordLocator,
    ScopeInterval,
    SourceCoordinate,
    SourceField,
    SourceFields,
    SourceRecord,
    SourceSubject,
    TemporalScope,
    value_field,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from reg_meta_build.source_records import FieldScalar, SourceRevision
    from reg_meta_build.sources.sos import (
        SosCellEvidence,
        SosRegister,
        SosVariable,
    )


_DATA_TYPES = {
    "datum": "date",
    "heltal": "integer",
    "sträng (text)": "text",
}


def _cell(variable: SosVariable, field_name: str) -> SosCellEvidence | None:
    evidence = variable.source_evidence
    if evidence is None:
        return None
    return next(
        (cell for cell in evidence.cells if cell.field_name == field_name),
        None,
    )


def _raw_scalar(cell: SosCellEvidence | None) -> FieldScalar | None:
    if cell is None or cell.raw_value is None:
        return None
    if isinstance(cell.raw_value, (bool, int, str)):
        return cell.raw_value
    return cell.display_value


def _text_field(
    value: str | None,
    cell: SosCellEvidence | None,
    *,
    token: bool = False,
    multiline: bool = False,
) -> SourceField | None:
    if cell is None:
        return None
    raw = _raw_scalar(cell)
    if value is None:
        return SourceField(status="unknown", raw_value=raw)
    normalized = (
        normalize_token(value) if token else normalize_text(value, multiline=multiline)
    )
    if not normalized:
        return SourceField(status="unknown", raw_value=raw)
    return value_field(normalized, raw=raw)


def _data_type_field(variable: SosVariable) -> SourceField | None:
    cell = _cell(variable, "data_type")
    field = _text_field(variable.data_type, cell, token=True)
    if field is None or field.status != "value" or not isinstance(field.value, str):
        return field
    normalized = _DATA_TYPES.get(field.value.casefold(), field.value)
    return SourceField(
        status="value",
        value=normalized,
        raw_value=field.raw_value,
    )


def _coverage_scope(variable: SosVariable) -> TemporalScope:
    from_cell = _cell(variable, "data_from")
    to_cell = _cell(variable, "data_to")
    if from_cell is None and to_cell is None:
        return TemporalScope(kind="not_applicable")

    def source_year(cell: SosCellEvidence | None) -> int | None:
        if cell is None or cell.display_value is None:
            return None
        if not re.fullmatch(r"[0-9]{4}", cell.display_value):
            return None
        return int(cell.display_value)

    start = source_year(from_cell)
    end = source_year(to_cell)
    if start is not None and end is not None and start <= end:
        return TemporalScope(
            kind="intervals",
            intervals=(
                ScopeInterval(
                    start=str(start),
                    end=str(end),
                ),
            ),
        )

    def shown(cell: SosCellEvidence | None) -> str:
        if cell is None:
            return "<not delivered>"
        return cell.display_value if cell.display_value is not None else "<blank>"

    return TemporalScope(
        kind="unknown",
        label=f"Data från={shown(from_cell)}; Data till={shown(to_cell)}",
    )


def _delivered_cells(variable: SosVariable) -> tuple[DeliveredCell, ...]:
    evidence = variable.source_evidence
    if evidence is None:
        return ()
    return tuple(
        DeliveredCell(
            name=cell.header,
            present=True,
            raw_value=("" if cell.raw_value is None else str(cell.raw_value)),
            interpreted_value=cell.display_value or "",
            raw_type=(
                type(cell.raw_value).__name__ if cell.raw_value is not None else "none"
            ),
            storage_type=cell.data_type,
            number_format=cell.number_format,
            hyperlink_target=cell.hyperlink_target,
            hyperlink_location=cell.hyperlink_location,
        )
        for cell in evidence.cells
    )


def clean_sos_variable(
    register: SosRegister,
    variable: SosVariable,
    revision: SourceRevision,
) -> SourceRecord:
    """Normalize one delivered variable row without resolving SOS semantics."""

    evidence = variable.source_evidence
    if evidence is None:
        raise ValueError("SOS source records require parser row evidence")

    register_name = (
        normalize_text(register.dataset_name) if register.dataset_name else None
    )
    variant_name = (
        normalize_token(variable.deldatamangd) if variable.deldatamangd else None
    )
    member_name = normalize_token(variable.name)
    semantic_key = (
        f"register:{register_name or '<unknown>'}",
        f"deldatamangd:{variant_name or '<unknown>'}",
        f"variable:{member_name}",
    )
    return SourceRecord.create(
        revision=revision,
        locators=(
            RecordLocator(
                semantic_record_key=semantic_key,
                physical_file=revision.artifact_path,
                physical_table=evidence.sheet_name,
                physical_record=f"row:{evidence.row_number}",
                physical_cells=tuple(
                    f"{evidence.sheet_name}!{cell.coordinate}"
                    for cell in evidence.cells
                ),
            ),
        ),
        subject=SourceSubject(
            provider="sos",
            register=(
                SourceCoordinate(status="value", name=register_name)
                if register_name
                else SourceCoordinate(status="unknown")
            ),
            variant=(
                SourceCoordinate(status="value", name=variant_name)
                if variant_name
                else SourceCoordinate(status="unknown")
            ),
            population=SourceCoordinate(status="unknown"),
            member=SourceCoordinate(status="value", name=member_name),
            native=NativeCoordinates(),
        ),
        edition_scope=_coverage_scope(variable),
        edition_period_scope=TemporalScope(kind="not_applicable"),
        fields=SourceFields(
            availability=value_field(True),
            column_name=_text_field(
                variable.name,
                _cell(variable, "name"),
                token=True,
            ),
            name=_text_field(variable.label, _cell(variable, "label")),
            description=_text_field(
                variable.description,
                _cell(variable, "description"),
                multiline=True,
            ),
            data_type=_data_type_field(variable),
            representation=_text_field(
                variable.value_set_text,
                _cell(variable, "value_set_text"),
                multiline=True,
            ),
            source_attribution=_text_field(
                variable.source_detail,
                _cell(variable, "source_detail"),
                multiline=True,
            ),
        ),
        delivered_cells=_delivered_cells(variable),
    )


def iter_sos_variable_records(
    register: SosRegister,
    revision: SourceRevision,
) -> Iterator[SourceRecord]:
    """Yield every physical SOS variable occurrence in workbook order."""

    for variable in register.variables:
        yield clean_sos_variable(register, variable, revision)


__all__ = ["clean_sos_variable", "iter_sos_variable_records"]
