"""Focused tests for LISA/SCB source records and diagnostic target inspection."""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import (
    var_row,
    write_input_bundle,
    write_scb_input,
)
from _lisa_fixtures import write_lisa_workbook
from openpyxl import load_workbook
from reg_meta.source_evidence import SourceField
from reg_meta_build.input_snapshot import (
    LisaWorkbookSelection,
    open_input_bundle,
)
from reg_meta_build.source_inspection import (
    inspect_bundle_source_records,
)
from reg_meta_build.source_records import (
    ScopeInterval,
)

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize(
    ("anchor_year", "blank_year", "expected_target_count"),
    (
        (2019, 2020, 1),
        (2020, 2021, 0),
    ),
    ids=("anchor-outside-workbook-target-inside", "target-outside-workbook"),
)
def test_filtered_preview_uses_finite_anchors_and_workbook_scoped_targets(
    tmp_path: Path,
    anchor_year: int,
    blank_year: int,
    expected_target_count: int,
) -> None:
    input_dir = tmp_path / "source"
    rows = [
        var_row(
            colname="X",
            cvid=1,
            var_id=9,
            year=str(anchor_year),
            regver_id=200,
            register=("LISA", 34, 152),
        ),
        var_row(
            colname="",
            cvid=2,
            var_id=9,
            year=str(blank_year),
            regver_id=201,
            register=("LISA", 34, 152),
        ),
    ]
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

    assert filtered.target_preview is not None
    scb_by_member = {
        record.subject.native.member_id: record
        for record in filtered.source_records
        if record.subject.native.member_id is not None
    }
    anchor = scb_by_member[1]
    blank = scb_by_member[2]
    assert anchor.record_id in filtered.target_preview.existing_named_record_ids
    assert filtered.target_preview.candidate_variable_ids == (9,)
    assert filtered.target_preview.candidate_variant_ids == (152,)
    assert filtered.target_preview.expected_source_target_count == expected_target_count
    assert (blank.record_id in filtered.target_preview.target_record_ids) is bool(
        expected_target_count
    )
    for report in (full, filtered):
        assert blank.record_id in {record.record_id for record in report.source_records}
        blank_outcome = next(
            outcome
            for outcome in report.comparison_outcomes
            if blank.record_id in outcome.scb_record_ids
            and outcome.status
            == (
                "unknown_spelling" if expected_target_count else "unknown_applicability"
            )
        )
        if not expected_target_count:
            assert (
                "ineligible under the declared target assumptions"
                in blank_outcome.detail
            )


@pytest.mark.parametrize(
    ("case", "expected_target_members"),
    (
        ("named-and-blank-parallel", ()),
        ("two-blank-parallel", ()),
        ("single-member-edition", (2,)),
    ),
)
def test_blank_targets_require_one_distinct_member_per_native_edition(
    tmp_path: Path,
    case: str,
    expected_target_members: tuple[int, ...],
) -> None:
    if case == "named-and-blank-parallel":
        row_specs = (("X", 1, "2020", 200), ("", 2, "2020", 200))
    elif case == "two-blank-parallel":
        row_specs = (
            ("X", 1, "2019", 199),
            ("", 2, "2020", 200),
            ("", 3, "2020", 200),
        )
    else:
        row_specs = (("X", 1, "2019", 199), ("", 2, "2020", 200))

    input_dir = tmp_path / "source"
    rows = [
        var_row(
            colname=column,
            cvid=member_id,
            var_id=9,
            year=year,
            regver_id=edition_id,
            register=("LISA", 34, 152),
        )
        for column, member_id, year, edition_id in row_specs
    ]
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

    report = inspect_bundle_source_records(
        open_input_bundle(selection), code_commit="c" * 40, exact_column="X"
    )

    assert report.target_preview is not None
    scb_by_member = {
        record.subject.native.member_id: record
        for record in report.source_records
        if record.source == "scb-registerinformation"
    }
    target_members = {
        member_id
        for member_id, record in scb_by_member.items()
        if record.record_id in report.target_preview.target_record_ids
    }
    assert target_members == set(expected_target_members)
    assert set(scb_by_member) == {member_id for _, member_id, _, _ in row_specs}

    if case == "named-and-blank-parallel":
        assert any(
            outcome.status == "agreement"
            and outcome.scb_record_ids == (scb_by_member[1].record_id,)
            for outcome in report.comparison_outcomes
        )
        assert any(
            outcome.status == "ambiguous_match"
            and outcome.scb_record_ids == (scb_by_member[2].record_id,)
            for outcome in report.comparison_outcomes
        )
    elif case == "two-blank-parallel":
        assert any(
            outcome.status == "ambiguous_match"
            and set(outcome.scb_record_ids)
            == {scb_by_member[2].record_id, scb_by_member[3].record_id}
            for outcome in report.comparison_outcomes
        )
    else:
        assert any(
            outcome.status == "unknown_spelling"
            and outcome.scb_record_ids == (scb_by_member[2].record_id,)
            for outcome in report.comparison_outcomes
        )


def test_competing_finite_native_spelling_makes_blank_target_ambiguous(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "source"
    rows = [
        var_row(
            colname=column,
            cvid=member_id,
            var_id=9,
            year=str(year),
            regver_id=200 + member_id,
            register=("LISA", 34, 152),
        )
        for column, year, member_id in (
            ("X", 2018, 1),
            ("Y", 2019, 2),
            ("", 2020, 3),
        )
    ]
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

    assert filtered.target_preview is not None
    assert filtered.target_preview.expected_source_target_count == 0
    assert filtered.target_preview.target_record_ids == ()
    scb_by_member = {
        record.subject.native.member_id: record
        for record in filtered.source_records
        if record.subject.native.member_id is not None
    }
    assert set(scb_by_member) == {1, 2, 3}
    competing = scb_by_member[2]
    blank = scb_by_member[3]
    for report in (full, filtered):
        ambiguous = next(
            outcome
            for outcome in report.comparison_outcomes
            if outcome.column_name == "X"
            and outcome.workbook_record_ids
            and outcome.status == "ambiguous_match"
        )
        assert set(ambiguous.scb_record_ids) == {
            competing.record_id,
            blank.record_id,
        }
        assert not any(
            outcome.status == "unknown_spelling"
            and blank.record_id in outcome.scb_record_ids
            for outcome in report.comparison_outcomes
        )


@pytest.mark.parametrize(
    ("version_name", "scope_kind", "issue_kind", "expected_complete"),
    (
        ("okänd utgåva", "unknown", "unparseable_period", False),
        ("2020-2021", "pooled", "pooled_period", True),
    ),
    ids=("unparseable", "pooled"),
)
def test_filtered_report_retains_unscoped_same_variable_witness_but_not_target(
    tmp_path: Path,
    version_name: str,
    scope_kind: str,
    issue_kind: str,
    expected_complete: bool,
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
            colname="",
            cvid=2,
            var_id=31619,
            year="2021",
            versionname=version_name,
            regver_id=201,
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

    full = inspect_bundle_source_records(bundle, code_commit="c" * 40)
    filtered = inspect_bundle_source_records(
        bundle, code_commit="c" * 40, exact_column="AmPolTyp"
    )

    assert full.complete is expected_complete
    assert filtered.complete is expected_complete
    assert filtered.target_preview is not None
    unscoped = next(
        record
        for record in filtered.source_records
        if record.subject.native.member_id == 2
    )
    assert unscoped.edition_scope.kind == scope_kind
    assert unscoped.record_id not in filtered.target_preview.target_record_ids
    assert filtered.target_preview.expected_source_target_count == 0
    assert any(
        issue.source_record_ids == (unscoped.record_id,) and issue.kind == issue_kind
        for issue in filtered.interpretation_issues
    )
    assert any(
        outcome.status == "unknown_applicability"
        and outcome.scb_record_ids == (unscoped.record_id,)
        and outcome.edition_scope.kind == scope_kind
        for outcome in filtered.comparison_outcomes
    )


@pytest.mark.parametrize(
    ("version_name", "issue_kind", "expected_complete"),
    (
        ("okänd utgåva", "unparseable_period", False),
        ("2018-2019", "pooled_period", True),
    ),
    ids=("unparseable", "pooled"),
)
def test_source_only_unknown_period_survives_full_and_filtered_inspection(
    tmp_path: Path,
    version_name: str,
    issue_kind: str,
    expected_complete: bool,
) -> None:
    input_dir = tmp_path / "source"
    rows = [
        var_row(
            colname="OnlyScb",
            cvid=1,
            var_id=9,
            year="2019",
            regver_id=200,
            register=("LISA", 34, 152),
        ),
        var_row(
            colname="",
            cvid=2,
            var_id=9,
            year="2020",
            versionname=version_name,
            regver_id=201,
            register=("LISA", 34, 152),
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

    full = inspect_bundle_source_records(bundle, code_commit="c" * 40)
    filtered = inspect_bundle_source_records(
        bundle, code_commit="c" * 40, exact_column="OnlyScb"
    )

    named = next(
        record
        for record in filtered.source_records
        if record.subject.native.member_id == 1
    )
    unknown = next(
        record
        for record in filtered.source_records
        if record.subject.native.member_id == 2
    )
    assert filtered.complete is expected_complete
    assert filtered.target_preview is not None
    assert filtered.target_preview.expected_source_target_count == 0
    assert filtered.target_preview.target_record_ids == ()
    assert filtered.target_preview.existing_named_record_ids == (named.record_id,)
    assert filtered.target_preview.assumption_ids == (
        "unestablished-workbook-to-scb-variant-scope",
    )
    for report in (full, filtered):
        assert report.complete is expected_complete
        assert {named.record_id, unknown.record_id} <= {
            record.record_id for record in report.source_records
        }
        assert any(
            outcome.status == "unknown_applicability"
            and outcome.workbook_record_ids == ()
            and outcome.scb_record_ids == (unknown.record_id,)
            and outcome.assumption_ids
            == (
                "unestablished-workbook-to-scb-variant-scope",
                "same-native-variable-spelling-continuity",
            )
            for outcome in report.comparison_outcomes
        )
        assert any(
            issue.kind == issue_kind and issue.source_record_ids == (unknown.record_id,)
            for issue in report.interpretation_issues
        )


def test_year_independent_declarations_without_counterparts_have_zero_target_preview(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(
        input_dir,
        registerinformation_rows=[
            var_row(
                colname="Other",
                cvid=1,
                var_id=1,
                year="2020",
                regver_id=200,
                register=("LISA", 34, 153),
            )
        ],
    )
    workbook = write_lisa_workbook(input_dir / "docs" / "lisa.xlsx")
    source = load_workbook(workbook)
    for sheet_name, row, values in (
        (
            "Individ",
            42,
            (
                "Inv_UtvGrEg5",
                "In- och utvandring efter grund för egen uppgift",
                None,
                "LISA",
                "Nej",
                "RTB",
            ),
        ),
        (
            "Individ årsoberoende",
            43,
            (
                "Inv_UtvGrEg5",
                "In- och utvandring efter grund för egen uppgift",
                "LISA",
                "RTB",
            ),
        ),
    ):
        sheet = source[sheet_name]
        for column, value in enumerate(values, start=1):
            sheet.cell(row, column, value)
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
        bundle, code_commit="c" * 40, exact_column="Inv_UtvGrEg5"
    )

    assert filtered.summary.workbook_selected_occurrences == 2
    assert filtered.summary.workbook_year_independent_occurrences == 2
    assert filtered.target_preview is not None
    assert filtered.target_preview.documented_edition_scope is None
    assert filtered.target_preview.expected_source_target_count == 0
    assert filtered.target_preview.target_record_ids == ()
    assert filtered.target_preview.assumption_ids == (
        "unestablished-workbook-to-scb-variant-scope",
    )
    assert {
        (record.locators[0].physical_table, record.locators[0].physical_record)
        for record in filtered.source_records
    } == {
        ("Individ", "row:42"),
        ("Individ årsoberoende", "row:43"),
    }
    for report in (full, filtered):
        matching = [
            outcome
            for outcome in report.comparison_outcomes
            if outcome.column_name == "Inv_UtvGrEg5"
        ]
        assert len(matching) == 2
        assert all(outcome.status == "unknown_applicability" for outcome in matching)
        assert all(
            outcome.assumption_ids == ("unestablished-workbook-to-scb-variant-scope",)
            for outcome in matching
        )


def test_ampoltyp_preview_keeps_nine_targets_and_a_separate_2024_gap(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "source"
    rows = [
        var_row(
            colname="" if 2011 <= year <= 2019 else "AmPolTyp",
            cvid=year,
            var_id=31619,
            year=str(year),
            regver_id=year,
            register=("LISA", 34, 1335 if year <= 2009 else 153),
        )
        for year in range(1990, 2024)
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

    assert report.target_preview is not None
    assert report.target_preview.expected_source_target_count == 9
    assert len(report.target_preview.target_record_ids) == 9
    assert len(report.target_preview.existing_named_record_ids) == 25
    assert report.target_preview.candidate_variable_ids == (31619,)
    assert report.target_preview.candidate_variant_ids == (153, 1335)
    assert report.target_preview.unobserved_documented_editions == (2024,)
    target_records = {
        record.record_id: record
        for record in report.source_records
        if record.record_id in report.target_preview.target_record_ids
    }
    assert {
        record.subject.native.member_id for record in target_records.values()
    } == set(range(2011, 2020))
    assert all(
        record.fields.column_name == SourceField(status="unknown", raw_value="")
        for record in target_records.values()
    )
    assert any(
        outcome.status == "missing_edition"
        and outcome.edition_scope.intervals
        == (ScopeInterval(start="2024", end="2024"),)
        and not outcome.scb_record_ids
        for outcome in report.comparison_outcomes
    )
