"""Identity and raw-cell contracts for normalized source observations."""

from __future__ import annotations

from reg_meta_build.source_records import (
    DeliveredCell,
    NativeCoordinates,
    RecordLocator,
    SourceCoordinate,
    SourceFields,
    SourceRecord,
    SourceRevision,
    SourceSubject,
    TemporalScope,
    value_field,
)


def _revision() -> SourceRevision:
    return SourceRevision.create(
        dataset="fixture",
        publisher="SCB",
        purpose="source-record identity test",
        upstream_revision="fixture",
        artifact_path="fixture.csv",
        artifact_size=0,
        artifact_sha256="0" * 64,
    )


def _record(*, row: int, data_length: str, context: str = "population") -> SourceRecord:
    return SourceRecord.create(
        revision=_revision(),
        locators=(
            RecordLocator(
                semantic_record_key=("member:2181",),
                physical_file="fixture.csv",
                physical_table="fixture.csv",
                physical_record=f"row:{row}",
                physical_cells=(f"fixture.csv:row:{row}:Datalängd",),
            ),
        ),
        subject=SourceSubject(
            provider="scb",
            register=SourceCoordinate(status="value", native_id=161, name="HAMN"),
            variant=SourceCoordinate(status="value", native_id=232),
            population=SourceCoordinate(status="value", name=context),
            member=SourceCoordinate(status="value", native_id=2181, name="Signal"),
            native=NativeCoordinates(
                register_id=161,
                register_variant_id=232,
                edition_id=204,
                variable_id=1880,
                member_id=2181,
            ),
        ),
        edition_scope=TemporalScope(kind="unknown", label="2003"),
        reference_period_scope=TemporalScope(kind="unknown", label="2003"),
        fields=SourceFields(
            column_name=value_field("Signal"),
            data_length=value_field(data_length),
        ),
        original_period_text="2003",
        context=(context,),
        delivered_cells=(
            DeliveredCell(
                name="Kolumnnamn",
                present=True,
                raw_value="Signal",
                interpreted_value="Signal",
            ),
            DeliveredCell(
                name="Datalängd",
                present=True,
                raw_value=data_length,
                interpreted_value=data_length,
            ),
        ),
    )


def test_observation_identity_includes_payload_and_context_but_not_row_position() -> (
    None
):
    first = _record(row=2, data_length="10")
    duplicate = _record(row=9, data_length="10")
    changed_payload = _record(row=2, data_length="11")
    changed_context = _record(row=2, data_length="10", context="another population")

    assert duplicate.record_id == first.record_id
    assert duplicate.locators[0].physical_record != first.locators[0].physical_record
    combined = first.with_additional_locators(duplicate.locators)
    assert combined.record_id == first.record_id
    assert [locator.physical_record for locator in combined.locators] == [
        "row:2",
        "row:9",
    ]
    assert changed_payload.record_id != first.record_id
    assert changed_context.record_id != first.record_id


def test_delivered_cells_distinguish_missing_from_supplied_empty() -> None:
    missing = DeliveredCell(
        name="Populationkommentar",
        present=False,
        interpreted_value="",
    )
    empty = DeliveredCell(
        name="Populationkommentar",
        present=True,
        raw_value="",
        interpreted_value="",
    )

    assert missing != empty
    assert missing.model_dump(mode="json", exclude_none=True) == {
        "name": "Populationkommentar",
        "present": False,
        "interpreted_value": "",
    }
    assert empty.model_dump(mode="json", exclude_none=True)["raw_value"] == ""
