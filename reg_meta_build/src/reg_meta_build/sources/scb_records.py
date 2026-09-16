"""Compact source records from raw SCB ``Registerinformation`` rows."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta.fqid import _YEAR

from reg_meta_build.edition_bounds import edition_claims
from reg_meta_build.source_records import (
    DeliveredCell,
    NativeCoordinates,
    RecordLocator,
    ScopeInterval,
    SourceCoordinate,
    SourceField,
    SourceFields,
    SourceRecord,
    SourceRevision,
    SourceSubject,
    TemporalScope,
    value_field,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping, Sequence

    from reg_meta_build.input_snapshot import ScbSnapshotReader

LISA_REGISTER_ID = 34
IssueKind = Literal["pooled_period", "unparseable_period"]
_YEAR_TOKEN_RE = re.compile(rf"(?<!\d)({_YEAR})(?!\d)")


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


def _scb_native_id(value: str, field: str, row_number: int) -> int:
    try:
        return int(value)
    except ValueError as exc:
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="scb_native_id_invalid",
            error_class="configuration",
            message=(
                "Invalid SCB native ID in Registerinformation.csv "
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
    kind: IssueKind
    physical_record: str
    detail: str
    record_id: str


@dataclass(frozen=True)
class ScbObservation:
    record: SourceRecord
    issue: ScbInterpretationIssue | None


def _cell_field(cell: tuple[bool, str | None, str]) -> SourceField | None:
    present, raw, interpreted = cell
    if not present:
        return SourceField(status="unknown", raw_value=None)
    if not interpreted.strip():
        return SourceField(status="unknown", raw_value=raw)
    return value_field(interpreted.strip(), raw=raw)


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


def _scopes(version_name: str) -> tuple[TemporalScope, TemporalScope, IssueKind | None]:
    claims = edition_claims(version_name)
    if len(claims) == 1 and len(set(_YEAR_TOKEN_RE.findall(version_name))) > 1:
        claims = ()
    if len(claims) == 1:
        year, low, high = claims[0]
        return (
            TemporalScope(
                kind="intervals",
                intervals=(ScopeInterval(start=str(year), end=str(year)),),
            ),
            TemporalScope(
                kind="intervals",
                intervals=(ScopeInterval(start=low, end=high),),
            ),
            None,
        )
    label = version_name or "<blank Registerversionnamn>"
    if claims:
        return (
            TemporalScope(kind="pooled", label=label),
            TemporalScope(kind="pooled", label=label),
            "pooled_period",
        )
    return (
        TemporalScope(kind="unknown", label=label),
        TemporalScope(kind="unknown", label=label),
        "unparseable_period",
    )


def _optional_text_field(value: str, raw: str) -> SourceField | None:
    if not value:
        return None
    return value_field(value, raw=raw)


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
    edition_scope, reference_scope, issue_kind = _scopes(interpreted.edition_name)
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
                name=interpreted.register_name or None,
            ),
            variant=SourceCoordinate(
                status="value",
                native_id=interpreted.register_variant_id,
                name=interpreted.variant_name or None,
            ),
            population=(
                SourceCoordinate(status="value", name=interpreted.population_name)
                if interpreted.population_name
                else SourceCoordinate(status="unknown")
            ),
            member=SourceCoordinate(
                status="value",
                native_id=interpreted.member_id,
                name=interpreted.variable_name or None,
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
        reference_period_scope=reference_scope,
        fields=SourceFields(
            availability=value_field(True),
            column_name=(
                value_field(interpreted.column_name, raw=original_column)
                if interpreted.column_name
                else SourceField(status="unknown", raw_value=original_column)
            ),
            name=_optional_text_field(interpreted.variable_name, text("Variabelnamn")),
            definition=_optional_text_field(
                interpreted.variable_definition, text("Variabeldefinition")
            ),
            description=_optional_text_field(
                interpreted.variable_description, text("Variabelbeskrivning")
            ),
            operational_definition=_optional_text_field(
                interpreted.operational_definition,
                text("VariabelOperationell_definition"),
            ),
            data_type=_data_type_field(cells["Datatyp"]),
            data_length=_cell_field(cells["Datalängd"]),
            source_attribution=_optional_text_field(
                interpreted.source_attribution,
                text("VariabelRegister_Källa"),
            ),
            measurement_unit=_optional_text_field(
                interpreted.measurement_unit, text("Mattenhet")
            ),
            population_definition=_optional_text_field(
                interpreted.population_definition,
                text("Populationdefinition"),
            ),
            population_comment=_optional_text_field(
                interpreted.population_comment, text("Populationkommentar")
            ),
            population_date=_optional_text_field(
                interpreted.population_date, text("Populationdatum")
            ),
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
        delivered_cells=tuple(
            DeliveredCell(
                name=name,
                present=cell[0],
                raw_value=cell[1],
                interpreted_value=cell[2],
            )
            for name, cell in cells.items()
        ),
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
