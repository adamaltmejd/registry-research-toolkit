"""Availability comparison outcomes between LISA workbook records and SCB observations."""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import (
    var_row,
    write_input_bundle,
    write_scb_input,
)
from _lisa_fixtures import LISA_LAYOUT, QUALIFICATION_CONTEXT, write_lisa_workbook
from openpyxl import load_workbook
from reg_meta.source_evidence import RecordLocator, SourceField, SourceRevision
from reg_meta_build.input_snapshot import (
    LisaWorkbookSelection,
    open_input_bundle,
)
from reg_meta_build.source_inspection import (
    compare_availability_records,
    inspect_bundle_source_records,
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

if TYPE_CHECKING:
    from pathlib import Path


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
    population_name: str | None = None,
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
            population=(
                SourceCoordinate(status="value", name=population_name)
                if population_name is not None
                else SourceCoordinate(status="unknown")
            ),
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


def test_focused_inspection_coalesces_equivalent_scb_rows_with_all_locators(
    tmp_path: Path,
) -> None:
    row = var_row(
        colname="AmPolTyp",
        cvid=1,
        var_id=31619,
        year="2020",
        regver_id=200,
        register=("LISA", 34, 153),
    )
    input_dir = tmp_path / "source"
    write_scb_input(input_dir, registerinformation_rows=[row, row])
    workbook = write_lisa_workbook(input_dir / "docs" / "lisa.xlsx")
    selection = write_input_bundle(
        tmp_path / "accepted",
        input_dir,
        lisa_workbook=LisaWorkbookSelection(
            path=workbook,
            upstream_revision="2024-2025",
            sha256=hashlib.sha256(workbook.read_bytes()).hexdigest(),
        ),
    )

    report = inspect_bundle_source_records(
        open_input_bundle(selection), code_commit="c" * 40, exact_column="AmPolTyp"
    )
    scb = [
        record
        for record in report.source_records
        if record.source == "scb-registerinformation"
    ]
    assert len(scb) == 1
    assert [locator.physical_record for locator in scb[0].locators] == [
        "row:2",
        "row:3",
    ]
    assert report.summary.scb_total_occurrences == 2
    assert report.summary.scb_retained_occurrences == 2


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


@pytest.mark.parametrize(
    (
        "scb_column",
        "scb_variant",
        "expected_status",
        "edition_present",
        "expected_witness",
    ),
    (
        ("X", 153, "missing_edition", False, False),
        ("Other", 153, "missing_edition", False, False),
        ("X", 152, "agreement", True, True),
        ("Other", 152, "unobserved_counterpart", True, False),
    ),
    ids=(
        "cross-variant-exact-does-not-establish-edition",
        "cross-variant-edition-is-missing",
        "scoped-exact-agrees",
        "scoped-edition-is-present",
    ),
)
def test_company_comparison_uses_only_its_finite_native_variant_assumption(
    scb_column: str,
    scb_variant: int,
    expected_status: str,
    edition_present: bool | None,
    expected_witness: bool,
) -> None:
    revision = SourceRevision.create(
        dataset="fixture",
        publisher="SCB",
        purpose="scoped comparison",
        upstream_revision="1",
        artifact_path="fixture",
        artifact_size=0,
        artifact_sha256="0" * 64,
    )
    workbook = _record(
        revision,
        key="company-workbook",
        column="X",
        year=2020,
        workbook_table="company",
        # The company table's declared population: workbook rows Företag!A5:A6.
        population_name="\n".join(
            LISA_LAYOUT["Företag"]["text_rows"][row] for row in ("5", "6")
        ),
    )
    scb_record = _record(
        revision,
        key="scb",
        column=scb_column,
        year=2020,
        variant_id=scb_variant,
        population_name=("individuals 15+" if scb_variant == 153 else "companies"),
    )

    outcomes = compare_availability_records((workbook,), (scb_record,))
    compared = next(outcome for outcome in outcomes if outcome.workbook_record_ids)

    assert compared.status == expected_status
    assert compared.scb_edition_present is edition_present
    assert bool(compared.scb_record_ids) is expected_witness
    assert compared.assumption_ids[0] == ("lisa-company-1990-2024-to-scb-variant-152")
    if scb_variant == 153 and scb_column == "X":
        assert compared.assumption_ids == ("lisa-company-1990-2024-to-scb-variant-152",)
        assert any(
            outcome.status == "source_only_observation"
            and outcome.scb_record_ids == (scb_record.record_id,)
            for outcome in outcomes
        )


@pytest.mark.parametrize(
    (
        "witness_variant",
        "known_assumed_edition",
        "expected_status",
        "expected_edition_present",
    ),
    (
        (153, False, "missing_edition", False),
        (153, True, "unobserved_counterpart", True),
        (152, False, "unknown_applicability", None),
        (152, True, "unknown_applicability", True),
    ),
    ids=(
        "cross-variant-without-assumed-edition",
        "cross-variant-with-assumed-edition",
        "same-variant-unparsed-without-known-edition",
        "same-variant-unparsed-with-known-edition",
    ),
)
def test_filtered_company_comparison_separates_witnesses_from_edition_presence(
    tmp_path: Path,
    witness_variant: int,
    known_assumed_edition: bool,
    expected_status: str,
    expected_edition_present: bool | None,
) -> None:
    input_dir = tmp_path / "source"
    witness_version = "okänd utgåva" if witness_variant == 152 else "2020"
    rows = [
        var_row(
            colname="X",
            cvid=1,
            var_id=9,
            year="2020",
            versionname=witness_version,
            regver_id=201,
            register=("LISA", 34, witness_variant),
        ),
        var_row(
            colname="x",
            cvid=2,
            var_id=10,
            year="2020",
            regver_id=202,
            register=("LISA", 34, 153),
        ),
        var_row(
            colname="",
            cvid=3,
            var_id=9,
            year="2021",
            versionname="okänd utgåva",
            regver_id=203,
            register=("LISA", 34, witness_variant),
        ),
    ]
    if known_assumed_edition:
        rows.append(
            var_row(
                colname="Other",
                cvid=4,
                var_id=99,
                year="2020",
                regver_id=204,
                register=("LISA", 34, 152),
            )
        )
    write_scb_input(input_dir, registerinformation_rows=rows)
    workbook = write_lisa_workbook(input_dir / "docs" / "lisa.xlsx")
    source = load_workbook(workbook)
    source["Företag"]["A9"] = "X"
    source["Företag"]["C9"] = 2020
    source.save(workbook)
    source.close()
    selection = write_input_bundle(
        tmp_path / "accepted",
        input_dir,
        lisa_workbook=LisaWorkbookSelection(
            path=workbook,
            upstream_revision="2024-2025",
            sha256=hashlib.sha256(workbook.read_bytes()).hexdigest(),
        ),
    )
    bundle = open_input_bundle(selection)

    full = inspect_bundle_source_records(bundle, code_commit="c" * 40)
    filtered = inspect_bundle_source_records(
        bundle, code_commit="c" * 40, exact_column="X"
    )

    comparison = next(
        outcome
        for outcome in filtered.comparison_outcomes
        if outcome.column_name == "X"
        and outcome.edition_scope.intervals
        == (ScopeInterval(start="2020", end="2020"),)
        and outcome.workbook_record_ids
    )
    assert comparison.status == expected_status
    assert comparison.scb_edition_present is expected_edition_present
    assert bool(comparison.scb_record_ids) is (witness_variant == 152)
    assert comparison.register_variant_ids == ((152,) if witness_variant == 152 else ())
    expected_assumption_ids = (
        (
            "lisa-company-1990-2024-to-scb-variant-152",
            "unestablished-workbook-to-scb-variant-scope",
        )
        if witness_variant == 152
        else ("lisa-company-1990-2024-to-scb-variant-152",)
    )
    assert comparison.assumption_ids == expected_assumption_ids
    assert filtered.target_preview is not None
    assert filtered.target_preview.expected_source_target_count == 0
    assert filtered.target_preview.target_record_ids == ()
    filtered_scb_by_member = {
        record.subject.native.member_id: record
        for record in filtered.source_records
        if record.source == "scb-registerinformation"
    }
    assert filtered_scb_by_member[2].record_id in (
        filtered.target_preview.casefold_collision_record_ids
    )
    assert filtered_scb_by_member[3].record_id not in (
        filtered.target_preview.target_record_ids
    )
    for report in (full, filtered):
        scb_by_member = {
            record.subject.native.member_id: record
            for record in report.source_records
            if record.source == "scb-registerinformation"
        }
        assert {1, 2, 3} <= set(scb_by_member)
        assert any(
            scb_by_member[3].record_id in outcome.scb_record_ids
            and outcome.status == "unknown_applicability"
            and outcome.assumption_ids
            == (
                "unestablished-workbook-to-scb-variant-scope",
                "same-native-variable-spelling-continuity",
            )
            for outcome in report.comparison_outcomes
        )


def test_filtered_report_retains_unparseable_period_as_incomplete(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "source"
    rows = [
        var_row(
            colname="AmPolTyp",
            cvid=1,
            var_id=31619,
            year="2020",
            versionname="okänd utgåva",
            regver_id=200,
            register=("LISA", 34, 153),
        )
    ]
    write_scb_input(input_dir, registerinformation_rows=rows)
    workbook = write_lisa_workbook(input_dir / "docs" / "lisa.xlsx")
    selection = write_input_bundle(
        tmp_path / "accepted",
        input_dir,
        lisa_workbook=LisaWorkbookSelection(
            path=workbook,
            upstream_revision="2024-2025",
            sha256=hashlib.sha256(workbook.read_bytes()).hexdigest(),
        ),
    )

    report = inspect_bundle_source_records(
        open_input_bundle(selection),
        code_commit="c" * 40,
        exact_column="AmPolTyp",
    )

    assert report.complete is False
    assert len(report.interpretation_issues) == 1
    assert report.interpretation_issues[0].kind == "unparseable_period"
    assert report.interpretation_issues[0].incomplete is True
    assert any(
        outcome.status == "unknown_applicability"
        and outcome.scb_edition_present is None
        and outcome.scb_record_ids
        for outcome in report.comparison_outcomes
    )


def test_column_filter_retains_source_only_exact_observations(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "source"
    rows = [
        var_row(
            colname="AmPolTyp",
            cvid=1,
            var_id=31619,
            year="2020",
            regver_id=200,
            register=("LISA", 34, 153),
        ),
        var_row(
            colname="AmPolTyp",
            cvid=2,
            var_id=31619,
            year="2025",
            regver_id=201,
            register=("LISA", 34, 153),
        ),
        var_row(
            colname="OnlyScb",
            cvid=3,
            var_id=40000,
            year="2026",
            regver_id=202,
            register=("LISA", 34, 153),
        ),
    ]
    write_scb_input(input_dir, registerinformation_rows=rows)
    workbook = write_lisa_workbook(input_dir / "docs" / "lisa.xlsx")
    selection = write_input_bundle(
        tmp_path / "accepted",
        input_dir,
        lisa_workbook=LisaWorkbookSelection(
            path=workbook,
            upstream_revision="2024-2025",
            sha256=hashlib.sha256(workbook.read_bytes()).hexdigest(),
        ),
    )
    bundle = open_input_bundle(selection)

    ampoltyp = inspect_bundle_source_records(
        bundle, code_commit="c" * 40, exact_column="AmPolTyp"
    )
    assert {record.subject.native.member_id for record in ampoltyp.source_records} >= {
        1,
        2,
    }
    assert any(
        outcome.status == "source_only_observation"
        and outcome.edition_scope.intervals
        == (ScopeInterval(start="2025", end="2025"),)
        and not outcome.workbook_record_ids
        for outcome in ampoltyp.comparison_outcomes
    )

    scb_only = inspect_bundle_source_records(
        bundle, code_commit="c" * 40, exact_column="OnlyScb"
    )
    assert scb_only.summary.workbook_selected_occurrences == 0
    assert set(scb_only.workbook_context) >= QUALIFICATION_CONTEXT
    assert all(
        scb_only.workbook_context.count(item) == 1 for item in QUALIFICATION_CONTEXT
    )
    assert [record.subject.native.member_id for record in scb_only.source_records] == [
        3
    ]
    assert all(
        item not in record.context
        for record in scb_only.source_records
        for item in QUALIFICATION_CONTEXT
    )
    assert len(scb_only.comparison_outcomes) == 1
    assert scb_only.comparison_outcomes[0].status == "source_only_observation"
    assert scb_only.comparison_outcomes[0].workbook_record_ids == ()
