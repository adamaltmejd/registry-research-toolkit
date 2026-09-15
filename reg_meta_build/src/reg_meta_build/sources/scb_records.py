"""Compact source records from raw SCB ``Registerinformation`` rows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from reg_meta_build.db import _open_scb_csv
from reg_meta_build.input_snapshot import SnapshotError
from reg_meta_build.source_records import (
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
from reg_meta_build.sources.scb import register_edition_claims

if TYPE_CHECKING:
    from reg_meta_build.input_snapshot import ScbSnapshotReader

LISA_REGISTER_ID = 34
IssueKind = Literal["pooled_period", "unparseable_period"]


class ScbRecordError(SnapshotError):
    """A raw SCB row cannot be represented by the source-record contract."""


@dataclass(frozen=True)
class ScbInterpretationIssue:
    kind: IssueKind
    physical_record: str
    detail: str
    record_id: str


def _native_int(row: dict[str, str], field: str, row_number: int) -> int:
    try:
        return int(row[field])
    except ValueError as exc:
        raise ScbRecordError(
            f"invalid SCB native id at Registerinformation.csv row {row_number}, "
            f"field {field}: {row[field]!r}"
        ) from exc


def _scopes(
    register_id: int, version_name: str
) -> tuple[TemporalScope, TemporalScope, IssueKind | None]:
    claims = register_edition_claims(register_id, version_name)
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


def _optional_text_field(raw: str) -> SourceField | None:
    normalized = raw.strip()
    if not normalized:
        return None
    return value_field(normalized, raw=raw)


def read_scb_lisa_records(
    snapshot: ScbSnapshotReader, revision: SourceRevision
) -> tuple[tuple[SourceRecord, ...], tuple[ScbInterpretationIssue, ...]]:
    """Read only prepared Registerinformation and retain raw LISA occurrences."""
    records: list[SourceRecord] = []
    issues: list[ScbInterpretationIssue] = []
    record_ids: set[str] = set()
    path = snapshot.root / "Registerinformation.csv"
    with _open_scb_csv(path, snapshot) as (_, rows):
        for row_number, row in rows:
            register_id = _native_int(row, "RegisterId", row_number)
            if register_id != LISA_REGISTER_ID:
                continue
            register_variant_id = _native_int(row, "RegVarID", row_number)
            edition_id = _native_int(row, "RegVerID", row_number)
            variable_id = _native_int(row, "VarId", row_number)
            member_id = _native_int(row, "CVID", row_number)
            register_name = row["Registernamn"].strip()
            variant_name = row["Registervariantnamn"].strip()
            version_name = row["Registerversionnamn"].strip()
            variable_name = row["Variabelnamn"].strip()
            original_column = row["Kolumnnamn"]
            column = original_column.strip()
            edition_scope, reference_scope, issue_kind = _scopes(
                register_id, version_name
            )
            semantic_key = (
                f"register:{register_id}",
                f"variant:{register_variant_id}",
                f"edition:{edition_id}",
                f"variable:{variable_id}",
                f"member:{member_id}",
            )
            record = SourceRecord.create(
                revision=revision,
                locator=RecordLocator(
                    semantic_record_key=semantic_key,
                    physical_file="Registerinformation.csv",
                    physical_table="Registerinformation.csv",
                    physical_record=f"row:{row_number}",
                    physical_cells=tuple(
                        f"Registerinformation.csv:row:{row_number}:{field}"
                        for field in (
                            "RegisterId",
                            "RegVarID",
                            "RegVerID",
                            "VarId",
                            "CVID",
                            "Registerversionnamn",
                            "Kolumnnamn",
                            "Variabelnamn",
                            "Variabeldefinition",
                            "Variabelbeskrivning",
                            "VariabelOperationell_definition",
                            "Datatyp",
                            "Datalängd",
                        )
                    ),
                ),
                subject=SourceSubject(
                    provider="scb",
                    register=SourceCoordinate(
                        status="value",
                        native_id=register_id,
                        name=register_name or None,
                    ),
                    variant=SourceCoordinate(
                        status="value",
                        native_id=register_variant_id,
                        name=variant_name or None,
                    ),
                    population=(
                        SourceCoordinate(
                            status="value", name=row["Populationnamn"].strip()
                        )
                        if row["Populationnamn"].strip()
                        else SourceCoordinate(status="unknown")
                    ),
                    member=SourceCoordinate(
                        status="value",
                        native_id=member_id,
                        name=variable_name or None,
                    ),
                    native=NativeCoordinates(
                        register_id=register_id,
                        register_variant_id=register_variant_id,
                        edition_id=edition_id,
                        variable_id=variable_id,
                        member_id=member_id,
                    ),
                ),
                edition_scope=edition_scope,
                reference_period_scope=reference_scope,
                fields=SourceFields(
                    availability=value_field(True),
                    column_name=(
                        value_field(column, raw=original_column)
                        if column
                        else SourceField(status="unknown", raw_value=original_column)
                    ),
                    name=_optional_text_field(row["Variabelnamn"]),
                    definition=_optional_text_field(row["Variabeldefinition"]),
                    description=_optional_text_field(row["Variabelbeskrivning"]),
                    operational_definition=_optional_text_field(
                        row["VariabelOperationell_definition"]
                    ),
                    data_type=_optional_text_field(row["Datatyp"]),
                    data_length=_optional_text_field(row["Datalängd"]),
                    source_attribution=_optional_text_field(
                        row["VariabelRegister_Källa"]
                    ),
                    measurement_unit=_optional_text_field(row["Mattenhet"]),
                    population_definition=_optional_text_field(
                        row["Populationdefinition"]
                    ),
                    population_comment=_optional_text_field(row["Populationkommentar"]),
                    population_date=_optional_text_field(row["Populationdatum"]),
                ),
                original_period_text=row["Registerversionnamn"],
                context=(register_name, variant_name, version_name),
            )
            if record.record_id in record_ids:
                raise ScbRecordError(
                    "duplicate SCB source coordinates at "
                    f"Registerinformation.csv row {row_number}: {semantic_key!r}"
                )
            record_ids.add(record.record_id)
            records.append(record)
            if issue_kind is not None:
                issues.append(
                    ScbInterpretationIssue(
                        kind=issue_kind,
                        physical_record=f"Registerinformation.csv:row:{row_number}",
                        detail=(
                            "SCB period is pooled and was not expanded into annual claims"
                            if issue_kind == "pooled_period"
                            else "SCB edition has no parseable period"
                        ),
                        record_id=record.record_id,
                    )
                )
    return tuple(records), tuple(issues)


__all__ = [
    "LISA_REGISTER_ID",
    "ScbInterpretationIssue",
    "ScbRecordError",
    "read_scb_lisa_records",
]
