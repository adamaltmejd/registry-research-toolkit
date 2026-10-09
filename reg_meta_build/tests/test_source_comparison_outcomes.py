"""The availability comparison as a pure fold over hand-built source records.

`compare_availability_records` also reaches statuses no delivered source can produce:
both readers record every occurrence as available (`sources/lisa.py` and
`sources/scb_records.py` set `availability` to true), so a negative or unknown
availability, and with it `conflict`, exists only here. The bundle-driven claims are
the `inspect-source-records` CLI cases (`cases/cli/inspect-source-records/`).
"""

from __future__ import annotations

from reg_meta_build.source_evidence import RecordLocator, SourceField, SourceRevision
from reg_meta_build.source_inspection import (
    compare_availability_records,
)
from reg_meta_build.source_records import (
    NativeCoordinates,
    ScopeInterval,
    SourceCoordinate,
    SourceFields,
    SourceRecord,
    SourceSubject,
    TemporalScope,
    value_field,
)


def _record(
    revision: SourceRevision,
    *,
    key: str,
    column: str | None,
    year: int,
    availability: SourceField | None = None,
    variant_id: int = 153,
    variable_id: int = 1,
    member_id: int = 1,
    workbook_table: str | None = None,
) -> SourceRecord:
    return SourceRecord.create(
        revision=revision,
        locators=(
            RecordLocator(
                semantic_record_key=(key,),
                physical_file="fixture",
                physical_table="fixture",
                physical_record=key,
                physical_cells=(f"fixture:{key}",),
            ),
        ),
        subject=SourceSubject(
            provider="scb",
            register=SourceCoordinate(status="value", native_id=34, name="LISA"),
            variant=(
                SourceCoordinate(status="value", name=workbook_table)
                if workbook_table is not None
                else SourceCoordinate(status="value", native_id=variant_id)
            ),
            population=SourceCoordinate(status="unknown"),
            member=SourceCoordinate(
                status="value",
                native_id=None if workbook_table is not None else member_id,
                name=column if workbook_table is not None else None,
            ),
            native=(
                NativeCoordinates()
                if workbook_table is not None
                else NativeCoordinates(
                    register_id=34,
                    register_variant_id=variant_id,
                    edition_id=year,
                    variable_id=variable_id,
                    member_id=member_id,
                )
            ),
        ),
        edition_scope=TemporalScope(
            kind="intervals",
            intervals=(ScopeInterval(start=str(year), end=str(year)),),
        ),
        edition_period_scope=TemporalScope(kind="not_applicable"),
        fields=SourceFields(
            availability=availability or value_field(True),
            column_name=(
                value_field(column)
                if column is not None
                else SourceField(status="unknown", raw_value="")
            ),
        ),
    )


def test_compact_comparison_retains_witnesses_conflicts_ids_and_spelling() -> None:
    revision = SourceRevision.create(
        dataset="fixture",
        publisher="SCB",
        purpose="comparison",
        upstream_revision="1",
        artifact_path="fixture",
        artifact_size=0,
        artifact_sha256="0" * 64,
    )
    workbook = (
        _record(
            revision,
            key="workbook",
            column="AmPolTyp",
            year=2020,
            workbook_table="individual",
        ),
        _record(
            revision,
            key="negative",
            column="Negative",
            year=2020,
            availability=SourceField(status="negative"),
            workbook_table="individual",
        ),
        _record(
            revision,
            key="suffix",
            column="Thing_LISA",
            year=2020,
            workbook_table="individual",
        ),
        _record(
            revision,
            key="unknown-availability",
            column="Unknown",
            year=2020,
            availability=SourceField(status="unknown", raw_value="Okänt"),
            workbook_table="individual",
        ),
    )
    scb = (
        _record(
            revision,
            key="named-a",
            column="AmPolTyp",
            year=2020,
            variable_id=31619,
            member_id=10,
        ),
        _record(
            revision,
            key="named-b",
            column="AmPolTyp",
            year=2020,
            variant_id=1335,
            variable_id=999,
            member_id=11,
        ),
        _record(
            revision,
            key="positive",
            column="Negative",
            year=2020,
            member_id=12,
        ),
        _record(
            revision,
            key="distinct-suffix",
            column="Thing_MiDAS",
            year=2020,
            member_id=13,
        ),
        _record(
            revision,
            key="known-availability",
            column="Unknown",
            year=2020,
            member_id=14,
        ),
    )

    outcomes = compare_availability_records(workbook, scb)
    by_column = {
        outcome.column_name: outcome
        for outcome in outcomes
        if outcome.workbook_record_ids
    }

    assert by_column["AmPolTyp"].status == "agreement"
    assert by_column["AmPolTyp"].variable_ids == (31619,)
    assert len(by_column["AmPolTyp"].scb_record_ids) == 1
    assert by_column["AmPolTyp"].assumption_ids == (
        "lisa-individual-2010-2024-to-scb-variant-153",
    )
    assert any(
        outcome.status == "source_only_observation" and outcome.variable_ids == (999,)
        for outcome in outcomes
    )
    assert by_column["Negative"].status == "conflict"
    assert by_column["Unknown"].status == "unknown_applicability"
    assert by_column["Thing_LISA"].status == "unobserved_counterpart"
    assert not by_column["Thing_LISA"].scb_record_ids
    assert all(outcome.assumption_ids for outcome in outcomes)
