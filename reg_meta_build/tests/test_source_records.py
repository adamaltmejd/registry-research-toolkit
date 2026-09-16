"""Identity and raw-cell contracts for normalized source observations."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import (
    HAMN_SIGNAL_TARGETS,
    REGISTERINFORMATION_HEADER,
    _var_row,
    hamn_signal_rows,
    scb_interpretation_rows,
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
from reg_meta_build.sources.scb_records import clean_scb_row, iter_scb_observations

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
    variable: SourceCoordinate | None = None,
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
            variable=variable or SourceCoordinate(status="unknown"),
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
        edition_period_scope=TemporalScope(kind="unknown", label="2003"),
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


def test_source_variable_coordinate_is_independent_and_checked_in_record_identity() -> (
    None
):
    unknown = _record(row=2, data_length="10")
    known = _record(
        row=2,
        data_length="10",
        variable=SourceCoordinate(status="value", native_id="source-variable"),
    )

    assert unknown.subject.variable.status == "unknown"
    assert known.subject.member == unknown.subject.member
    assert known.record_id != unknown.record_id
    changed = known.model_dump(mode="python")
    changed["subject"]["variable"]["native_id"] = "another-source-variable"
    with pytest.raises(ValueError, match="record identity"):
        SourceRecord.model_validate(changed)


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


def test_scb_observations_share_coordinates_and_stripped_fields_without_losing_cells(
    tmp_path: Path,
) -> None:
    source = write_scb_input(
        tmp_path / "source", registerinformation_rows=scb_interpretation_rows()
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
        purpose="shared interpretation fixture",
        upstream_revision=snapshot.manifest.edition,
        artifact_path="Registerinformation.csv",
        artifact_size=item.raw_size,
        artifact_sha256=item.raw_sha256,
    )

    observations = tuple(iter_scb_observations(snapshot, revision, register_id=34))

    assert len(observations) == 4
    first, duplicate, another_column, unparseable = observations
    assert first.record.subject.native == NativeCoordinates(
        register_id=34,
        register_variant_id=153,
        edition_id=204,
        variable_id=1880,
        member_id=9001,
    )
    assert first.record.subject.register_name.name == "LISA"
    assert first.record.subject.variant.name == "Individer"
    assert first.record.subject.population.name == "Population A"
    assert first.record.subject.variable == SourceCoordinate(
        status="value", native_id=1880, name="Signal variable"
    )
    assert first.record.subject.member.name == "Signal variable"
    assert first.record.subject.member.native_id == 9001
    assert another_column.record.subject.variable == first.record.subject.variable
    assert first.record.original_period_text == " 2001-2003 "
    assert first.record.fields.name == value_field(
        "Signal variable", raw=" Signal variable "
    )
    assert first.record.fields.definition == value_field(
        "Shared definition", raw=" Shared definition "
    )
    assert first.record.fields.description == value_field(
        "Shared description", raw=" Shared description "
    )
    assert first.record.fields.operational_definition == value_field("E22", raw=" E22 ")
    assert first.record.fields.source_attribution == value_field(
        "Source system", raw=" Source system "
    )
    assert first.record.fields.measurement_unit == value_field("count", raw=" count ")
    assert first.record.fields.data_type == value_field("numeric", raw=" numeric ")
    assert first.record.fields.data_length == value_field("0", raw="0")
    assert first.record.edition_scope.kind == "pooled"
    assert first.issue is not None and first.issue.kind == "pooled_period"

    first_comment = next(
        cell
        for cell in first.record.delivered_cells
        if cell.name == "Populationkommentar"
    )
    supplied_empty = next(
        cell
        for cell in unparseable.record.delivered_cells
        if cell.name == "Populationkommentar"
    )
    assert (
        first_comment.present,
        first_comment.raw_value,
        first_comment.interpreted_value,
    ) == (
        False,
        None,
        "",
    )
    assert (
        supplied_empty.present,
        supplied_empty.raw_value,
        supplied_empty.interpreted_value,
    ) == (True, "", "")
    assert duplicate.record.record_id == first.record.record_id
    assert another_column.record.record_id != first.record.record_id
    assert [
        observation.record.locators[0].physical_record
        for observation in observations[:3]
    ] == ["row:2", "row:3", "row:4"]
    assert unparseable.record.edition_scope.kind == "unknown"
    assert unparseable.issue is not None
    assert unparseable.issue.kind == "unparseable_period"


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


def _clean_row(*, reference_period: str | None = "", **changes: object) -> SourceRecord:
    header = REGISTERINFORMATION_HEADER.split("|")
    row = _var_row(colname="Example", cvid=1001, var_id=101, **changes).split("|")
    cells = {
        name: (True, value, value) for name, value in zip(header, row, strict=True)
    }
    cells["VariabelReferenstid"] = (
        reference_period is not None,
        reference_period,
        reference_period or "",
    )
    return clean_scb_row(header, 2, cells, _revision()).record


@pytest.mark.parametrize(
    ("declared", "expected"),
    [
        ("tinyint", "integer"),
        ("smallint", "integer"),
        (" int ", "integer"),
        ("integer", "integer"),
        ("BIGINT", "integer"),
        ("char", "text"),
        ("varchar", "text"),
        ("nchar", "text"),
        (" NVARCHAR ", "text"),
        ("text", "text"),
        ("ntext", "text"),
        ("numeric", "numeric"),
        ("decimal", "decimal"),
        ("float", "float"),
        ("date", "date"),
        ("unrecognized declaration", "unrecognized declaration"),
    ],
)
def test_scb_storage_types_normalize_without_discarding_declaration_or_width(
    declared: str, expected: str
) -> None:
    record = _clean_row(data_type=declared, data_length="12")

    assert record.fields.data_type == value_field(expected, raw=declared)
    assert record.fields.data_length == value_field("12")
    delivered = next(cell for cell in record.delivered_cells if cell.name == "Datatyp")
    assert delivered.raw_value == declared
    assert delivered.interpreted_value == declared


def test_scb_normalization_retains_distinct_observations_and_substantive_types() -> (
    None
):
    small, big, text = (
        _clean_row(data_type=kind) for kind in ("smallint", "bigint", "varchar")
    )

    assert small.fields.data_type.value == big.fields.data_type.value == "integer"
    assert text.fields.data_type.value == "text"
    assert len({record.record_id for record in (small, big, text)}) == 3


def test_cleaning_keeps_projection_register_range_pooled() -> None:
    record = _clean_row(
        register=("Befolkningsframskrivningar", 310, 10),
        versionname="2024-2070",
    )

    assert record.edition_scope == TemporalScope(kind="pooled", label="2024-2070")
    assert record.edition_period_scope == record.edition_scope
    assert record.original_period_text == "2024-2070"


def test_scb_text_cleaning_compares_clean_values_and_retains_original_evidence() -> (
    None
):
    first = _clean_row(varname=" A\u030ars\u00a0  inkomst ", data_length="+0004")
    second = _clean_row(varname="Års inkomst", data_length="4")

    assert first.fields.name.value == second.fields.name.value == "Års inkomst"
    assert first.subject.member.name == second.subject.member.name == "Års inkomst"
    assert first.fields.name.raw_value == " A\u030ars\u00a0  inkomst "
    assert first.fields.data_length.value == second.fields.data_length.value == "4"
    assert first.fields.data_length.raw_value == "+0004"
    assert first.record_id != second.record_id


def test_scb_paragraph_cleaning_retains_layout_and_real_text_differences() -> None:
    raw = "Rubrik\r\n\r\n  - Kön\u00a0 \r\nA  B\tC"
    first = _clean_row(vardesc=raw)
    second = _clean_row(vardesc="Rubrik\n\n  - Ålder\nA  B\tC")

    assert first.fields.description.value == "Rubrik\n\n  - Kön\nA  B\tC"
    assert first.fields.description.raw_value == raw
    assert first.fields.description.value != second.fields.description.value
    assert _clean_row(data_length="1,5").fields.data_length.value == "1,5"


def test_edition_date_does_not_replace_declared_variable_reference_period() -> None:
    declared = " Under  föregående\u00a0kalenderår "
    record = _clean_row(versionname="2025-12-31", reference_period=declared)

    assert record.edition_scope.intervals[0].start == "2025"
    assert record.edition_period_scope.intervals[0].start == "2025-12-31"
    assert record.fields.reference_period == value_field(
        "Under föregående kalenderår", raw=declared
    )
    assert "reference_period_scope" not in record.model_dump()


def test_declared_reference_period_preserves_missing_and_empty_evidence() -> None:
    missing = _clean_row(reference_period=None).fields.reference_period
    empty = _clean_row(reference_period="").fields.reference_period

    assert missing.status == empty.status == "unknown"
    assert missing.raw_value is None
    assert empty.raw_value == ""
