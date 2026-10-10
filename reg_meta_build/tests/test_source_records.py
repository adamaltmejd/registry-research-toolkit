"""Source-record identity and flag contracts no prepare reader reaches.

The readers' identity, cleaning and raw-cell claims are `cases/prepare/scb-records-*`.
What stays here crosses deliveries or is model validation: identity across two source
revisions, combining a repeated observation's locators (done by LISA inspection, not by
a prepare reader), and the JSON-contract checks a prepared record passes when reopened.
"""

from __future__ import annotations

import pytest
from reg_meta_build.source_evidence import (
    DeliveredCell,
    RecordLocator,
    SourceRevision,
)
from reg_meta_build.source_records import (
    NativeCoordinates,
    SourceCoordinate,
    SourceFields,
    SourceRecord,
    SourceSubject,
    TemporalScope,
    value_field,
)


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
    revision: SourceRevision | None = None,
    physical_file: str = "fixture.csv",
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
            population=SourceCoordinate(status="value", name="population"),
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
            data_length=value_field("10"),
        ),
        original_period_text="2003",
        context=("population",),
        delivered_cells=(
            DeliveredCell(
                name="Datalängd",
                present=True,
                raw_value="10",
                interpreted_value="10",
            ),
        ),
    )


def test_observation_identity_excludes_revision_and_physical_evidence() -> None:
    # Kept: a reader sees one delivery, so no case can present one observation
    # under two source revisions. Fails if the record id hashes the revision, a
    # locator.
    first = _record(
        row=2,
        revision=_revision(artifact_path="first.csv"),
        physical_file="first.csv",
    )
    relocated = _record(
        row=19,
        revision=_revision(artifact_sha256="1" * 64, artifact_path="reordered.csv"),
        physical_file="reordered.csv",
    )

    assert relocated.source_revision_id != first.source_revision_id
    assert relocated.locators != first.locators
    assert relocated.record_id == first.record_id


def test_repeated_observation_combines_its_locators_under_one_identity() -> None:
    # Kept: the prepare readers return each row's observation; combining a
    # repeated one is LISA inspection's fold (`source_inspection`), done through
    # this model method. Fails if combining changes the identity or drops or
    # reorders a locator.
    first = _record(row=2)
    duplicate = _record(row=9)

    combined = first.with_additional_locators(duplicate.locators)

    assert combined.record_id == first.record_id
    assert [locator.physical_record for locator in combined.locators] == [
        "row:2",
        "row:9",
    ]


def test_reopened_record_whose_variable_changed_fails_its_identity_check() -> None:
    # Kept: JSON-contract validation of a stored record, which a reader never
    # produces. Fails if the identity check stops covering the source variable.
    known = _record(
        row=2, variable=SourceCoordinate(status="value", native_id="source-variable")
    )
    changed = known.model_dump(mode="python")
    changed["subject"]["variable"]["native_id"] = "another-source-variable"

    with pytest.raises(ValueError, match="record identity"):
        SourceRecord.model_validate(changed)


@pytest.mark.parametrize("value", ["true", 1, 0])
@pytest.mark.parametrize("field", ["identifier", "conditional_sensitivity"])
def test_flag_declarations_reject_nonboolean_values(
    field: str, value: str | int
) -> None:
    # Kept: JSON-contract validation of `SourceFields`; the readers only ever
    # write booleans. Fails if a flag coerces a string or an integer to a boolean.
    with pytest.raises(ValueError, match=f"{field} must carry a boolean"):
        SourceFields.model_validate({field: value_field(value)})
