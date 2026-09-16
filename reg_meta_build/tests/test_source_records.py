"""Identity and raw-cell contracts for normalized source observations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from _csv_fixtures import (
    HAMN_SIGNAL_TARGETS,
    hamn_signal_rows,
    write_scb_input,
    write_scb_snapshot,
)
from reg_meta_build.input_snapshot import open_scb_snapshot
from reg_meta_build.source_records import (
    CodeSetReference,
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
from reg_meta_build.sources.scb_records import iter_scb_observations

if TYPE_CHECKING:
    from pathlib import Path


def _revision(
    *, artifact_sha256: str = "0" * 64, artifact_path: str = "fixture.csv"
) -> SourceRevision:
    return SourceRevision.create(
        dataset="fixture",
        publisher="SCB",
        purpose="source-record identity test",
        upstream_revision="fixture",
        artifact_path=artifact_path,
        artifact_size=0,
        artifact_sha256=artifact_sha256,
    )


def _record(
    *,
    row: int,
    data_length: str,
    context: str = "population",
    revision: SourceRevision | None = None,
    physical_file: str = "fixture.csv",
    code_set_locator: str | None = None,
) -> SourceRecord:
    return SourceRecord.create(
        revision=revision or _revision(),
        locators=(
            RecordLocator(
                semantic_record_key=("member:2181",),
                physical_file=physical_file,
                physical_table=physical_file,
                physical_record=f"row:{row}",
                physical_cells=(f"{physical_file}:row:{row}:Datalängd",),
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
        code_set_references=(
            CodeSetReference(
                reference_id="signal-codes",
                content_sha256="a" * 64,
                physical_locator=code_set_locator,
            ),
        )
        if code_set_locator is not None
        else (),
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


def test_observation_identity_excludes_revision_and_physical_evidence() -> None:
    first = _record(
        row=2,
        data_length="10",
        revision=_revision(artifact_path="first.csv"),
        physical_file="first.csv",
        code_set_locator="first.xlsx!A1:B9",
    )
    relocated = _record(
        row=19,
        data_length="10",
        revision=_revision(
            artifact_sha256="1" * 64,
            artifact_path="reordered.csv",
        ),
        physical_file="reordered.csv",
        code_set_locator="reordered.xlsx!D4:E12",
    )

    assert relocated.source_revision_id != first.source_revision_id
    assert relocated.locators != first.locators
    assert relocated.code_set_references != first.code_set_references
    assert relocated.record_id == first.record_id


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


def test_hamn_fixture_yields_twenty_lossless_observations_for_ten_exact_members(
    tmp_path: Path,
) -> None:
    source = write_scb_input(
        tmp_path / "source", registerinformation_rows=hamn_signal_rows()
    )
    selection = write_scb_snapshot(tmp_path / "accepted", source)
    snapshot = open_scb_snapshot(selection)
    item = next(
        item
        for item in snapshot.manifest.files
        if item.name == "Registerinformation.csv"
    )
    assert item.raw_size is not None and item.raw_sha256 is not None
    revision = SourceRevision.create(
        dataset="scb-registerinformation",
        publisher="SCB",
        purpose="HAMN fixture",
        upstream_revision=snapshot.manifest.edition,
        artifact_path="Registerinformation.csv",
        artifact_size=item.raw_size,
        artifact_sha256=item.raw_sha256,
    )

    observations = tuple(iter_scb_observations(snapshot, revision, register_id=161))

    assert len(observations) == 20
    assert {
        (
            int(observation.record.original_period_text),
            observation.record.subject.native.edition_id,
            observation.record.subject.native.member_id,
        )
        for observation in observations
    } == {
        (year, regver_id, cvid) for year, regver_id, cvid, *_rest in HAMN_SIGNAL_TARGETS
    }
    for target in {
        (year, regver_id, cvid) for year, regver_id, cvid, *_rest in HAMN_SIGNAL_TARGETS
    }:
        members = [
            observation.record
            for observation in observations
            if (
                int(observation.record.original_period_text),
                observation.record.subject.native.edition_id,
                observation.record.subject.native.member_id,
            )
            == target
        ]
        assert {record.fields.data_length.value for record in members} == {"10", "11"}
        assert all(len(record.delivered_cells) == 36 for record in members)
        assert all(
            next(
                cell
                for cell in record.delivered_cells
                if cell.name == "Populationkommentar"
            ).raw_value
            == ""
            for record in members
        )
