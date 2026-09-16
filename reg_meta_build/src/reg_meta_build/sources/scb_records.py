"""Compact source records from raw SCB ``Registerinformation`` rows."""

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING

from reg_meta.errors import EXIT_CONFIG, RegMetaError

from reg_meta_build.normalization import normalize_text, normalize_token
from reg_meta_build.source_periods import SourcePeriodIssue, source_scopes
from reg_meta_build.source_records import (
    DeliveredCell,
    NativeCoordinates,
    RecordLocator,
    SourceCoordinate,
    SourceField,
    SourceFieldCells,
    SourceFields,
    SourceParentObservation,
    SourceRecord,
    SourceRevision,
    SourceSubject,
    value_field,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping, Sequence

    from reg_meta_build.input_snapshot import ScbSnapshotReader

LISA_REGISTER_ID = 34


@dataclass(frozen=True, slots=True)
class _RegisterinformationRow:
    """Compact interpretation shared by catalog import and source evidence."""

    register_id: int
    register_variant_id: int
    edition_id: int
    variable_id: int
    member_id: int
    register_name: str
    variant_name: str
    edition_name: str
    variable_name: str
    column_name: str
    variable_definition: str
    variable_description: str
    operational_definition: str
    source_attribution: str
    measurement_unit: str
    population_name: str
    population_definition: str
    population_comment: str
    population_date: str


def _scb_native_id(
    value: str,
    field: str,
    row_number: int,
    *,
    filename: str = "Registerinformation.csv",
) -> int:
    try:
        return int(value)
    except ValueError as exc:
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="scb_native_id_invalid",
            error_class="configuration",
            message=(
                f"Invalid SCB native ID in {filename} "
                f"at row {row_number}, field {field}: {value!r}."
            ),
            remediation="Re-export the file from mikrometadata.scb.se.",
        ) from exc


def _interpret_registerinformation_row(
    text: Callable[[str], str],
    row_number: int,
    *,
    register_id: int | None = None,
) -> _RegisterinformationRow:
    """Interpret the shared coordinates and stripped fields of one source row."""
    return _RegisterinformationRow(
        register_id=(
            register_id
            if register_id is not None
            else _scb_native_id(text("RegisterId"), "RegisterId", row_number)
        ),
        register_variant_id=_scb_native_id(text("RegVarID"), "RegVarID", row_number),
        edition_id=_scb_native_id(text("RegVerID"), "RegVerID", row_number),
        variable_id=_scb_native_id(text("VarId"), "VarId", row_number),
        member_id=_scb_native_id(text("CVID"), "CVID", row_number),
        register_name=text("Registernamn").strip(),
        variant_name=text("Registervariantnamn").strip(),
        edition_name=text("Registerversionnamn").strip(),
        variable_name=text("Variabelnamn").strip(),
        column_name=text("Kolumnnamn").strip(),
        variable_definition=text("Variabeldefinition").strip(),
        variable_description=text("Variabelbeskrivning").strip(),
        operational_definition=text("VariabelOperationell_definition").strip(),
        source_attribution=text("VariabelRegister_Källa").strip(),
        measurement_unit=text("Mattenhet").strip(),
        population_name=text("Populationnamn").strip(),
        population_definition=text("Populationdefinition").strip(),
        population_comment=text("Populationkommentar").strip(),
        population_date=text("Populationdatum").strip(),
    )


@dataclass(frozen=True)
class ScbInterpretationIssue:
    kind: SourcePeriodIssue
    physical_record: str
    detail: str
    record_id: str


@dataclass(frozen=True)
class ScbObservation:
    record: SourceRecord
    issue: ScbInterpretationIssue | None


def _cell_field(
    cell: tuple[bool, str | None, str], *, label: bool = False
) -> SourceField | None:
    present, raw, interpreted = cell
    if not present:
        return SourceField(status="unknown", raw_value=None)
    if not interpreted.strip():
        return SourceField(status="unknown", raw_value=raw)
    cleaned = normalize_text(interpreted) if label else normalize_token(interpreted)
    return value_field(cleaned, raw=raw)


def _delivered_cells(
    cells: Mapping[str, tuple[bool, str | None, str]],
) -> tuple[DeliveredCell, ...]:
    return tuple(
        DeliveredCell(
            name=name, present=cell[0], raw_value=cell[1], interpreted_value=cell[2]
        )
        for name, cell in cells.items()
    )


def _data_type_field(cell: tuple[bool, str | None, str]) -> SourceField | None:
    field = _cell_field(cell)
    if field is None or field.status != "value" or not isinstance(field.value, str):
        return field
    declared = field.value.casefold()
    if declared in {"tinyint", "smallint", "int", "integer", "bigint"}:
        kind = "integer"
    elif declared in {"char", "varchar", "nchar", "nvarchar", "text", "ntext"}:
        kind = "text"
    else:
        return field
    return SourceField(status="value", value=kind, raw_value=field.raw_value)


def _optional_text_field(
    value: str, raw: str, *, multiline: bool = False
) -> SourceField | None:
    if not value:
        return None
    return value_field(normalize_text(value, multiline=multiline), raw=raw)


def _data_length_field(cell: tuple[bool, str | None, str]) -> SourceField | None:
    field = _cell_field(cell)
    if (
        field is not None
        and isinstance(field.value, str)
        and re.fullmatch(r"\+?[0-9]+", field.value)
    ):
        return SourceField(
            status="value", value=str(int(field.value)), raw_value=field.raw_value
        )
    return field


_PARENT_COLUMNS = (
    ("register", (("name", "Registernamn"), ("purpose", "Registersyfte"))),
    (
        "variant",
        (
            ("name", "Registervariantnamn"),
            ("description", "Registervariantbeskrivning"),
        ),
    ),
    (
        "edition",
        (
            ("name", "Registerversionnamn"),
            ("description", "Registerversionbeskrivning"),
            ("measurement_information", "Registerversionmätinformation"),
            ("documentation_status", "Registerversion_DocStaus"),
            ("first_approved_at", "Registerversion_ForstaGodkannandeDatum"),
            ("last_approved_at", "Registerversion_SenastGodkandDatum"),
        ),
    ),
    (
        "population",
        (
            ("name", "Populationnamn"),
            ("population_definition", "Populationdefinition"),
            ("population_comment", "Populationkommentar"),
            ("population_date", "Populationdatum"),
        ),
    ),
    ("object_type", (("name", "Objekttypnamn"), ("definition", "Objekttypdefinition"))),
)
_PARENT_COLUMN_NAMES = tuple(
    column for _, fields in _PARENT_COLUMNS for _, column in fields
)
_PARENT_PARAGRAPHS = {
    "purpose",
    "description",
    "measurement_information",
    "population_definition",
    "population_comment",
    "definition",
}


@lru_cache(maxsize=8192)
def _parent_facts(
    register_id: int,
    variant_id: int,
    edition_id: int,
    positions: tuple[int, ...],
    cells: tuple[tuple[bool, str | None, str], ...],
) -> tuple[SourceParentObservation, ...]:
    """Share repeated parent claims while retaining every original row occurrence."""
    fields_by_kind = {}
    mappings = {}
    offset = 0
    for kind, names in _PARENT_COLUMNS:
        fields = {}
        field_cells = []
        for field, _ in names:
            present, raw, interpreted = cells[offset]
            fields[field] = (
                value_field(
                    normalize_text(interpreted, multiline=field in _PARENT_PARAGRAPHS),
                    raw=raw,
                )
                if present and interpreted.strip()
                else SourceField(status="unknown", raw_value=raw)
            )
            field_cells.append(
                SourceFieldCells(field=field, positions=(positions[offset],))
            )
            offset += 1
        fields_by_kind[kind] = SourceFields.model_validate(fields)
        mappings[kind] = tuple(field_cells)

    def coordinate(kind: str, native_id: int | None = None) -> SourceCoordinate:
        name = fields_by_kind[kind].name
        text = name.value if name is not None and isinstance(name.value, str) else None
        return (
            SourceCoordinate(status="value", native_id=native_id, name=text)
            if native_id is not None or text
            else SourceCoordinate(status="unknown")
        )

    register = coordinate("register", register_id)
    variant = coordinate("variant", variant_id)
    edition = coordinate("edition", edition_id)
    coordinates = {
        "register": register,
        "variant": variant,
        "edition": edition,
        "population": coordinate("population"),
        "object_type": coordinate("object_type"),
    }
    return tuple(
        SourceParentObservation(
            kind=kind,
            coordinate=coordinates[kind],
            register=register,
            variant=variant if kind != "register" else None,
            edition=edition
            if kind in {"edition", "population", "object_type"}
            else None,
            fields=fields_by_kind[kind],
            field_cells=mappings[kind],
        )
        for kind, _ in _PARENT_COLUMNS
    )


def clean_scb_row(
    header: Sequence[str],
    row_number: int,
    cells: Mapping[str, tuple[bool, str | None, str]],
    revision: SourceRevision,
) -> ScbObservation:
    """Normalize one validated SCB row without selecting facts or catalog identities."""

    def text(field: str) -> str:
        return cells[field][2]

    interpreted = _interpret_registerinformation_row(text, row_number)
    original_column = text("Kolumnnamn")
    edition_scope, edition_period_scope, issue_kind = source_scopes(
        interpreted.edition_name
    )
    semantic_key = (
        f"register:{interpreted.register_id}",
        f"variant:{interpreted.register_variant_id}",
        f"edition:{interpreted.edition_id}",
        f"variable:{interpreted.variable_id}",
        f"member:{interpreted.member_id}",
    )
    record = SourceRecord.create(
        revision=revision,
        locators=(
            RecordLocator(
                semantic_record_key=semantic_key,
                physical_file="Registerinformation.csv",
                physical_table="Registerinformation.csv",
                physical_record=f"row:{row_number}",
                physical_cells=tuple(
                    f"Registerinformation.csv:row:{row_number}:{field}"
                    for field in header
                ),
            ),
        ),
        subject=SourceSubject(
            provider="scb",
            register=SourceCoordinate(
                status="value",
                native_id=interpreted.register_id,
                name=normalize_text(interpreted.register_name) or None,
            ),
            variant=SourceCoordinate(
                status="value",
                native_id=interpreted.register_variant_id,
                name=normalize_text(interpreted.variant_name) or None,
            ),
            population=(
                SourceCoordinate(
                    status="value", name=normalize_text(interpreted.population_name)
                )
                if interpreted.population_name
                else SourceCoordinate(status="unknown")
            ),
            variable=SourceCoordinate(
                status="value",
                native_id=interpreted.variable_id,
                name=normalize_text(interpreted.variable_name) or None,
            ),
            member=SourceCoordinate(
                status="value",
                native_id=interpreted.member_id,
                name=normalize_text(interpreted.variable_name) or None,
            ),
            native=NativeCoordinates(
                register_id=interpreted.register_id,
                register_variant_id=interpreted.register_variant_id,
                edition_id=interpreted.edition_id,
                variable_id=interpreted.variable_id,
                member_id=interpreted.member_id,
            ),
        ),
        edition_scope=edition_scope,
        edition_period_scope=edition_period_scope,
        fields=SourceFields(
            availability=value_field(True),
            column_name=(
                value_field(
                    normalize_token(interpreted.column_name), raw=original_column
                )
                if interpreted.column_name
                else SourceField(status="unknown", raw_value=original_column)
            ),
            name=_optional_text_field(interpreted.variable_name, text("Variabelnamn")),
            definition=_optional_text_field(
                interpreted.variable_definition,
                text("Variabeldefinition"),
                multiline=True,
            ),
            description=_optional_text_field(
                interpreted.variable_description,
                text("Variabelbeskrivning"),
                multiline=True,
            ),
            operational_definition=_optional_text_field(
                interpreted.operational_definition,
                text("VariabelOperationell_definition"),
                multiline=True,
            ),
            reference_period=_cell_field(cells["VariabelReferenstid"], label=True),
            data_type=_data_type_field(cells["Datatyp"]),
            data_length=_data_length_field(cells["Datalängd"]),
            source_attribution=_optional_text_field(
                interpreted.source_attribution,
                text("VariabelRegister_Källa"),
                multiline=True,
            ),
            measurement_unit=_optional_text_field(
                interpreted.measurement_unit, text("Mattenhet")
            ),
        ),
        parent_facts=_parent_facts(
            interpreted.register_id,
            interpreted.register_variant_id,
            interpreted.edition_id,
            tuple(header.index(name) for name in _PARENT_COLUMN_NAMES),
            tuple(cells[name] for name in _PARENT_COLUMN_NAMES),
        ),
        original_period_text=text("Registerversionnamn"),
        context=(
            interpreted.register_name,
            interpreted.variant_name,
            interpreted.edition_name,
            text("Populationnamn"),
            text("Populationdefinition"),
            text("Populationkommentar"),
            text("Populationdatum"),
            text("Objekttypnamn"),
            text("Objekttypdefinition"),
        ),
        delivered_cells=_delivered_cells(cells),
    )
    issue = None
    if issue_kind is not None:
        issue = ScbInterpretationIssue(
            kind=issue_kind,
            physical_record=f"Registerinformation.csv:row:{row_number}",
            detail=(
                "SCB period is pooled and was not expanded into annual claims"
                if issue_kind == "pooled_period"
                else "SCB edition has no parseable period"
            ),
            record_id=record.record_id,
        )
    return ScbObservation(record=record, issue=issue)


def iter_scb_observations(
    snapshot: ScbSnapshotReader,
    revision: SourceRevision,
    *,
    register_id: int | None = None,
) -> Iterator[ScbObservation]:
    """Stream lossless normalized Registerinformation observations once.

    A register selection is applied after decoding its required native ID and
    before interpreting the rest of the row or constructing strict models.
    """
    from reg_meta_build.db import _open_scb_csv_prepared

    path = snapshot.root / "Registerinformation.csv"
    with _open_scb_csv_prepared(path, snapshot) as (header, rows):
        for row_number, cells in rows:
            row_register_id = _scb_native_id(
                cells["RegisterId"][2], "RegisterId", row_number
            )
            if register_id is not None and row_register_id != register_id:
                continue

            yield clean_scb_row(header, row_number, cells, revision)


__all__ = [
    "LISA_REGISTER_ID",
    "ScbInterpretationIssue",
    "ScbObservation",
    "clean_scb_row",
    "iter_scb_observations",
]
