"""SCB summary and identifier declarations remain evidence before reconciliation."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import (
    IDENTIFIERARE_HEADER,
    PIPE,
    UNIKA_HEADER,
    write_scb_input,
    write_scb_snapshot,
)
from reg_meta.errors import RegMetaError
from reg_meta_build.input_snapshot import open_scb_snapshot
from reg_meta_build.source_records import SourceRevision
from reg_meta_build.sources.scb_auxiliary import (
    clean_identifier_row,
    clean_unika_row,
    iter_scb_auxiliary_records,
)

if TYPE_CHECKING:
    from pathlib import Path


def _revision() -> SourceRevision:
    return SourceRevision.create(
        dataset="scb-declarations",
        publisher="SCB",
        purpose="source declaration test",
        upstream_revision="fixture",
        artifact_path="summary.csv",
        artifact_size=0,
        artifact_sha256="0" * 64,
    )


def _cells(
    header: str, values: list[str | None]
) -> dict[str, tuple[bool, str | None, str]]:
    return {
        name: (value is not None, value, value or "")
        for name, value in zip(header.split(PIPE), values, strict=True)
    }


def _summary() -> dict[str, tuple[bool, str | None, str]]:
    return _cells(
        UNIKA_HEADER,
        [
            "  TESTREG  ",
            "Test register",
            "Individuals",
            "Individual data",
            "Name  with padding",
            "Column",
            "1990/91",
            "2023",
            "0",
            "1",
            "0",
        ],
    )


def test_summary_keeps_independent_flags_and_does_not_infer_occurrences() -> None:
    record = clean_unika_row(UNIKA_HEADER.split(PIPE), 2, _summary(), _revision())

    assert record.subject.register_name.name == "TESTREG"
    assert record.subject.register_name.native_id is None
    assert record.subject.variable.name == "Name with padding"
    assert record.subject.native.variable_id is None
    assert record.fields.sensitivity is not None
    assert record.fields.sensitivity.value is False
    assert record.fields.conditional_sensitivity is not None
    assert record.fields.conditional_sensitivity.value is True
    assert record.fields.identifier is not None
    assert record.fields.identifier.value is False
    assert record.fields.availability is None
    assert record.edition_scope.kind == "not_applicable"
    assert record.edition_period_scope.kind == "not_applicable"
    assert record.fields.coverage_from is not None
    assert record.fields.coverage_from.value == "1990/91"
    assert record.delivered_cells[0].raw_value == "  TESTREG  "
    assert record.locators[0].physical_cells[-1] == "row:2:Identitetsvariabel"


@pytest.mark.parametrize("value", [None, "", "Ja", "unknown", "2"])
def test_unknown_summary_flags_never_become_false(value: str | None) -> None:
    cells = _summary()
    cells["Identitetsvariabel"] = (value is not None, value, value or "")
    record = clean_unika_row(UNIKA_HEADER.split(PIPE), 2, cells, _revision())

    assert record.fields.identifier is not None
    assert record.fields.identifier.status == "unknown"
    assert record.fields.identifier.value is None
    assert record.fields.identifier.raw_value == value
    delivered = record.delivered_cells[-1]
    assert delivered.present is (value is not None)
    assert delivered.raw_value == value


def test_conflicting_summary_assertions_remain_separate_membership_evidence() -> None:
    cells = _summary()
    first = clean_unika_row(UNIKA_HEADER.split(PIPE), 2, cells, _revision())
    duplicate = clean_unika_row(UNIKA_HEADER.split(PIPE), 7, cells, _revision())
    cells["KansligVariabel"] = (True, "1", "1")
    conflict = clean_unika_row(UNIKA_HEADER.split(PIPE), 9, cells, _revision())

    assert duplicate.record_id == first.record_id
    assert duplicate.locators != first.locators
    assert conflict.record_id != first.record_id
    assert (
        conflict.locators[0].semantic_record_key
        == first.locators[0].semantic_record_key
    )
    assert conflict.fields.sensitivity is not None
    assert conflict.fields.sensitivity.value is True
    assert conflict.fields.conditional_sensitivity is not None
    assert conflict.fields.conditional_sensitivity.value is True


def test_identifier_declaration_has_source_wide_native_scope() -> None:
    cells = _cells(IDENTIFIERARE_HEADER, ["6266", "Workplace ID", "Definition"])
    record = clean_identifier_row(
        IDENTIFIERARE_HEADER.split(PIPE), 2, cells, _revision()
    )

    assert record.subject.variable.native_id == 6266
    assert record.subject.native.variable_id == 6266
    assert record.subject.native.register_id is None
    assert record.subject.register_name.status == "not_applicable"
    assert record.subject.variant.status == "not_applicable"
    assert record.fields.identifier is not None
    assert record.fields.identifier.value is True
    assert record.fields.availability is None
    assert record.fields.definition is not None
    assert record.fields.definition.value == "Definition"
    assert record.locators[0].semantic_record_key == ("identifier", "6266")


def test_bad_identifier_has_actionable_source_coordinate() -> None:
    cells = _cells(IDENTIFIERARE_HEADER, ["bad", "Workplace ID", "Definition"])
    with pytest.raises(RegMetaError) as error:
        clean_identifier_row(IDENTIFIERARE_HEADER.split(PIPE), 2, cells, _revision())
    assert "Identifierare.csv at row 2, field VarID" in error.value.message


def test_summary_stream_retains_order_and_physical_duplicates(tmp_path: Path) -> None:
    row = PIPE.join(value[2] for value in _summary().values())
    directory = write_scb_input(tmp_path / "source", unika_rows=[row, row])
    snapshot = open_scb_snapshot(write_scb_snapshot(tmp_path / "accepted", directory))
    records = list(
        iter_scb_auxiliary_records(
            snapshot, "UnikaRegisterOchVariabler.csv", _revision()
        )
    )

    assert len(records) == 2
    assert records[0].record_id == records[1].record_id
    assert [record.locators[0].physical_record for record in records] == [
        "row:2",
        "row:3",
    ]
    identifiers = list(
        iter_scb_auxiliary_records(snapshot, "Identifierare.csv", _revision())
    )
    assert identifiers
    with pytest.raises(ValueError, match="unsupported SCB declaration table"):
        list(iter_scb_auxiliary_records(snapshot, "Unknown.csv", _revision()))
