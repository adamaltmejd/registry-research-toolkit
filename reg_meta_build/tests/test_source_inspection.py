"""Focused tests for LISA/SCB source records and diagnostic target inspection."""

from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import (
    _var_row,
    sparsify_scb_values,
    write_input_bundle,
    write_scb_input,
    write_scb_snapshot,
)
from _lisa_fixtures import write_lisa_workbook
from openpyxl import load_workbook
from pydantic import ValidationError
from reg_meta.errors import RegMetaError
from reg_meta_build.input_snapshot import (
    LISA_DATASET_ID,
    LisaWorkbookSelection,
    ScbSnapshotReader,
    open_input_bundle,
    open_scb_snapshot,
)
from reg_meta_build.source_inspection import (
    SourceInspectionReport,
    compare_availability_records,
    inspect_bundle_source_records,
    report_semantic_sha256,
)
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
from reg_meta_build.sources.lisa import LisaWorkbookError, read_lisa_records
from reg_meta_build.sources.scb_records import read_scb_lisa_records

from reg_meta_build import cli as cli_module

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path
    from typing import Any


def _revision(path: Path, dataset: str = LISA_DATASET_ID) -> SourceRevision:
    payload = path.read_bytes()
    return SourceRevision.create(
        dataset=dataset,
        publisher="SCB",
        purpose="fixture source records",
        upstream_revision="fixture-2024-2025",
        artifact_path=path.name,
        artifact_size=len(payload),
        artifact_sha256=hashlib.sha256(payload).hexdigest(),
    )


def _field_text(record: SourceRecord, field: str) -> str | None:
    observation = getattr(record.fields, field)
    if observation is None or observation.status != "value":
        return None
    assert isinstance(observation.value, str)
    return observation.value


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
) -> SourceRecord:
    return SourceRecord.create(
        revision=revision,
        locator=RecordLocator(
            semantic_record_key=(key,),
            physical_file="fixture",
            physical_table="fixture",
            physical_record=key,
            physical_cells=(f"fixture:{key}",),
        ),
        subject=SourceSubject(
            provider="scb",
            register=SourceCoordinate(status="value", native_id=34, name="LISA"),
            variant=SourceCoordinate(status="value", native_id=variant_id),
            population=SourceCoordinate(status="unknown"),
            member=SourceCoordinate(status="value", native_id=member_id),
            native=NativeCoordinates(
                register_id=34,
                register_variant_id=variant_id,
                edition_id=year,
                variable_id=variable_id,
                member_id=member_id,
            ),
        ),
        edition_scope=TemporalScope(
            kind="intervals",
            intervals=(ScopeInterval(start=str(year), end=str(year)),),
        ),
        reference_period_scope=TemporalScope(kind="not_applicable"),
        fields=SourceFields(
            availability=availability or value_field(True),
            column_name=(
                value_field(column)
                if column is not None
                else SourceField(status="unknown", raw_value="")
            ),
        ),
    )


def test_lisa_reader_preserves_four_layouts_sections_periods_and_occurrences(
    tmp_path: Path,
) -> None:
    path = write_lisa_workbook(tmp_path / "lisa.xlsx")
    records = read_lisa_records(path, _revision(path))

    assert len(records) == 8
    assert {record.locator.physical_table for record in records} == {
        "Individ",
        "Individ årsoberoende",
        "Företag",
        "Arbetsställe",
    }
    person_numbers = [
        record for record in records if _field_text(record, "column_name") == "PersonNr"
    ]
    assert len(person_numbers) == 4
    assert len({record.record_id for record in person_numbers}) == 4
    assert {record.context[1] for record in person_numbers} == {
        "Födelseland",
        "Avlidna",
        "In- och Utvandring",
    }
    assert all(
        record.edition_scope.kind == "year_independent" for record in person_numbers
    )

    rekr = next(
        record for record in records if _field_text(record, "column_name") == "RekrBidr"
    )
    assert rekr.original_period_text == "2003-2008\n2013"
    assert [(item.start, item.end) for item in rekr.edition_scope.intervals] == [
        ("2003", "2008"),
        ("2013", "2013"),
    ]
    workplace = next(
        record
        for record in records
        if _field_text(record, "column_name") == "Ast_LoneSum"
    )
    assert [(item.start, item.end) for item in workplace.edition_scope.intervals] == [
        ("1990", "2021"),
        ("2024", "2024"),
    ]
    company = next(
        record
        for record in records
        if _field_text(record, "column_name") == "ForetagKod"
    )
    assert company.edition_scope.intervals == (ScopeInterval(start="2024", end="2024"),)
    assert company.subject.variant.name == "company"

    ampoltyp = next(
        record for record in records if _field_text(record, "column_name") == "AmPolTyp"
    )
    assert ampoltyp.locator.physical_record == "row:600"
    assert _field_text(ampoltyp, "description") == (
        "Typ av arbetsmarknadspolitisk åtgärd"
    )
    assert ampoltyp.fields.sensitivity == SourceField(
        status="value", value=False, raw_value="Nej"
    )
    assert ampoltyp.fields.availability == value_field(True)


@pytest.mark.parametrize(
    ("mutation", "expected"),
    (
        ("shift-header", r"Individ!A3:F3"),
        ("change-section", r"Individ årsoberoende!A30"),
        ("bad-period", r"Individ!C600"),
        ("extra-column", r"expected 6 columns, got 7"),
    ),
)
def test_lisa_reader_rejects_changed_actual_layout_with_coordinates(
    tmp_path: Path, mutation: str, expected: str
) -> None:
    path = write_lisa_workbook(tmp_path / "lisa.xlsx")
    workbook = load_workbook(path)
    if mutation == "shift-header":
        sheet = workbook["Individ"]
        for column in range(1, 7):
            sheet.cell(4, column, sheet.cell(3, column).value)
            sheet.cell(3, column).value = None
    elif mutation == "change-section":
        workbook["Individ årsoberoende"]["A30"] = "Ny tabell"
    elif mutation == "bad-period":
        workbook["Individ"]["C600"] = "1990 till 2024"
    else:
        workbook["Individ"]["G3"] = "Ny kolumn"
    workbook.save(path)
    workbook.close()

    with pytest.raises(LisaWorkbookError, match=expected):
        read_lisa_records(path, _revision(path))


def test_source_fields_distinguish_missing_unknown_negative_and_sensitivity() -> None:
    missing = SourceFields()
    unknown = SourceField(status="unknown", raw_value="")
    negative = SourceField(status="negative", raw_value="Nej")

    assert missing.column_name is None
    assert unknown.status == "unknown"
    assert negative.status == "negative"
    assert SourceFields(availability=negative).availability == negative
    sensitivity = SourceFields(sensitivity=value_field(False)).sensitivity
    assert sensitivity is not None
    assert sensitivity.status == "value"
    with pytest.raises(ValidationError, match="negative is supported only"):
        SourceFields(sensitivity=negative)


def test_prepare_cli_requires_complete_lisa_selection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def reject_prepare(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("incomplete LISA selection reached preparation")

    monkeypatch.setattr(cli_module, "prepare_input_bundle", reject_prepare)
    args = cli_module._build_parser().parse_args(
        [
            "prepare-input-bundle",
            "--input-dir",
            "inputs",
            "--scb-snapshot",
            "snapshot",
            "--scb-input-commit",
            "a" * 40,
            "--scb-manifest-sha256",
            "b" * 64,
            "--output-dir",
            "candidate",
            "--lisa-workbook",
            "lisa.xlsx",
        ]
    )

    with pytest.raises(RegMetaError) as exc_info:
        cli_module._cmd_prepare_input_bundle(args)
    assert exc_info.value.code == "lisa_source_selection_incomplete"


def test_inspection_cli_rejects_bundle_where_lisa_was_not_selected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    selection = write_input_bundle(tmp_path / "accepted", input_dir)
    monkeypatch.setattr(cli_module, "source_interpreter_commit", lambda: "c" * 40)

    exit_code = cli_module.run(
        [
            "inspect-source-records",
            "--input-bundle",
            str(selection.path),
            "--input-commit",
            selection.input_commit,
            "--input-manifest-sha256",
            selection.manifest_sha256,
        ]
    )

    assert exit_code != 0
    error = json.loads(capsys.readouterr().out)["error"]
    assert error["code"] == "source_record_inspection_invalid"
    assert "was not selected" in error["message"]
    assert "explicitly captures" in error["remediation"]


def test_raw_scb_reader_preserves_native_instances_fields_and_period_limits(
    tmp_path: Path,
) -> None:
    rows = [
        _var_row(
            colname="AmPolTyp",
            cvid=1,
            var_id=31619,
            year="2020",
            regver_id=200,
            register=("LISA", 34, 153),
            vardef="Definition 2020",
            varopdef="Operational 2020",
        ),
        _var_row(
            colname="",
            cvid=2,
            var_id=31619,
            year="2021",
            regver_id=201,
            register=("LISA", 34, 153),
            data_type="text",
            data_length="3",
        ),
        _var_row(
            colname="Pooled",
            cvid=3,
            var_id=3,
            year="2018",
            versionname="2018-2019",
            regver_id=202,
            register=("LISA", 34, 1335),
        ),
        _var_row(
            colname="UnknownPeriod",
            cvid=4,
            var_id=4,
            year="2020",
            versionname="okänd utgåva",
            regver_id=203,
            register=("LISA", 34, 153),
        ),
    ]
    scb_dir = write_scb_input(tmp_path / "source", registerinformation_rows=rows)
    selection = write_scb_snapshot(tmp_path / "accepted", scb_dir)
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
        purpose="fixture raw rows",
        upstream_revision=snapshot.manifest.edition,
        artifact_path="Registerinformation.csv",
        artifact_size=item.raw_size,
        artifact_sha256=item.raw_sha256,
    )

    records, issues = read_scb_lisa_records(snapshot, revision)

    assert len(records) == 4
    blank = next(record for record in records if record.subject.native.member_id == 2)
    assert blank.fields.column_name == SourceField(status="unknown", raw_value="")
    assert blank.fields.availability == value_field(True)
    assert blank.subject.native.variable_id == 31619
    assert blank.subject.native.register_variant_id == 153
    assert _field_text(blank, "data_type") == "text"
    assert _field_text(blank, "data_length") == "3"
    pooled = next(record for record in records if record.subject.native.member_id == 3)
    assert pooled.edition_scope == TemporalScope(kind="pooled", label="2018-2019")
    unknown = next(record for record in records if record.subject.native.member_id == 4)
    assert unknown.edition_scope.kind == "unknown"
    assert [(issue.kind, issue.record_id) for issue in issues] == [
        ("pooled_period", pooled.record_id),
        ("unparseable_period", unknown.record_id),
    ]


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
        _record(revision, key="workbook", column="AmPolTyp", year=2020),
        _record(
            revision,
            key="negative",
            column="Negative",
            year=2020,
            availability=SourceField(status="negative"),
        ),
        _record(revision, key="suffix", column="Thing_LISA", year=2020),
        _record(
            revision,
            key="unknown-availability",
            column="Unknown",
            year=2020,
            availability=SourceField(status="unknown", raw_value="Okänt"),
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
    by_column = {outcome.column_name: outcome for outcome in outcomes}

    assert by_column["AmPolTyp"].status == "ambiguous_match"
    assert by_column["AmPolTyp"].variable_ids == (999, 31619)
    assert len(by_column["AmPolTyp"].scb_record_ids) == 2
    assert by_column["Negative"].status == "conflict"
    assert by_column["Unknown"].status == "unknown_applicability"
    assert by_column["Thing_LISA"].status == "unobserved_counterpart"
    assert not by_column["Thing_LISA"].scb_record_ids


def test_filtered_report_retains_unparseable_period_as_incomplete(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "source"
    rows = [
        _var_row(
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


def test_pinned_bundle_cli_reports_deterministic_source_targets_without_cold_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    rows = [
        _var_row(
            colname="AmPolTyp",
            cvid=1,
            var_id=31619,
            year="2020",
            regver_id=200,
            register=("LISA", 34, 153),
            vardef="Kept definition",
        ),
        _var_row(
            colname="",
            cvid=2,
            var_id=31619,
            year="2021",
            regver_id=201,
            register=("LISA", 34, 153),
            data_type="text",
        ),
        _var_row(
            colname="ampoltyp",
            cvid=3,
            var_id=99,
            year="2022",
            regver_id=202,
            register=("LISA", 34, 1335),
        ),
        _var_row(
            colname="Other",
            cvid=4,
            var_id=4,
            year="2023",
            regver_id=203,
            register=("LISA", 34, 153),
        ),
    ]
    input_dir = tmp_path / "source"
    write_scb_input(input_dir, registerinformation_rows=rows)
    workbook = write_lisa_workbook(input_dir / "docs" / "lisa.xlsx")
    workbook_sha = hashlib.sha256(workbook.read_bytes()).hexdigest()
    selection = write_input_bundle(
        tmp_path / "accepted",
        input_dir,
        lisa_workbook=LisaWorkbookSelection(
            path=workbook,
            upstream_revision="2024-2025",
            sha256=workbook_sha,
        ),
    )
    bundle = open_input_bundle(selection)
    tracked_before = (bundle.root / "catalog-bundle.json").read_bytes()
    workbook_before = (
        bundle.root / "supplemental" / LISA_DATASET_ID / "source.xlsx"
    ).read_bytes()
    sparsify_scb_values(selection)

    opened: list[str] = []
    original_open_csv = ScbSnapshotReader.open_csv

    @contextmanager
    def guarded_open_csv(
        reader: ScbSnapshotReader, name: str
    ) -> Iterator[tuple[list[str | None], Iterator[list[str | None]]]]:
        opened.append(name)
        assert name == "Registerinformation.csv"
        with original_open_csv(reader, name) as value:
            yield value

    monkeypatch.setattr(ScbSnapshotReader, "open_csv", guarded_open_csv)
    monkeypatch.setattr(cli_module, "source_interpreter_commit", lambda: "c" * 40)
    outputs = [tmp_path / "report-a.json", tmp_path / "report-b.json"]
    reports: list[dict[str, Any]] = []
    for output in outputs:
        exit_code = cli_module.run(
            [
                "--output",
                str(output),
                "inspect-source-records",
                "--input-bundle",
                str(selection.path),
                "--input-commit",
                selection.input_commit,
                "--input-manifest-sha256",
                selection.manifest_sha256,
                "--column",
                "AmPolTyp",
            ]
        )
        assert exit_code == 0
        assert capsys.readouterr().out == ""
        reports.append(json.loads(output.read_text(encoding="utf-8")))

    assert reports[0] == reports[1]
    report = reports[0]
    assert report["diagnostic_only"] is True
    assert report["preview_level"] == "source_target_only"
    assert report["complete"] is True
    assert report["pins"] == {
        "bundle_id": "scb-mikrometadata",
        "bundle_manifest_sha256": selection.manifest_sha256,
        "code_commit": "c" * 40,
        "input_repository_commit": selection.input_commit,
        "interpretation_id": "scb-lisa-source-record-inspection-v1",
        "scb_snapshot_manifest_sha256": bundle.manifest.scb_manifest_sha256,
    }
    assert report["scope"]["selected_sources"] == [
        LISA_DATASET_ID,
        "scb-registerinformation",
    ]
    assert "SCB Vardemangder value streams" in report["scope"]["excluded_inputs"]
    assert {revision["artifact_sha256"] for revision in report["source_revisions"]} == {
        workbook_sha,
        bundle.snapshot.raw_sha256("Registerinformation.csv"),
    }
    assert report["target_preview"]["expected_source_target_count"] == 1
    assert report["target_preview"]["candidate_variable_ids"] == [31619]
    assert report["target_preview"]["candidate_variant_ids"] == [153]
    assert 2024 in report["target_preview"]["unobserved_documented_editions"]
    assert len(report["target_preview"]["target_record_ids"]) == 1
    assert report["summary"]["outcome_counts"]["unknown_spelling"] == 1
    assert report["summary"]["outcome_counts"]["ambiguous_match"] == 1
    assert report["summary"]["outcome_counts"]["unobserved_counterpart"] == 1
    assert report["summary"]["outcome_counts"]["missing_edition"] >= 1
    agreement = next(
        outcome
        for outcome in report["comparison_outcomes"]
        if outcome["status"] == "agreement"
    )
    assert len(agreement["workbook_record_ids"]) == 1
    assert len(agreement["scb_record_ids"]) == 1
    target_id = report["target_preview"]["target_record_ids"][0]
    target = next(
        record
        for record in report["source_records"]
        if record["record_id"] == target_id
    )
    assert target["subject"]["native"] == {
        "edition_id": 201,
        "member_id": 2,
        "register_id": 34,
        "register_variant_id": 153,
        "variable_id": 31619,
    }
    assert target["fields"]["column_name"] == {
        "raw_value": "",
        "status": "unknown",
    }
    assert target["fields"]["data_type"]["value"] == "text"
    assert (bundle.root / "catalog-bundle.json").read_bytes() == tracked_before
    assert (
        bundle.root / "supplemental" / LISA_DATASET_ID / "source.xlsx"
    ).read_bytes() == workbook_before
    semantic_payload = dict(report)
    semantic_sha256 = semantic_payload.pop("semantic_sha256")
    validated = SourceInspectionReport.model_validate_json(
        json.dumps(semantic_payload, ensure_ascii=False)
    )
    assert semantic_sha256 == report_semantic_sha256(validated)

    full_a = inspect_bundle_source_records(
        open_input_bundle(selection), code_commit="c" * 40
    )
    full_b = inspect_bundle_source_records(
        open_input_bundle(selection), code_commit="c" * 40
    )
    assert full_a == full_b
    assert full_a.target_preview is None
    assert full_a.summary.workbook_selected_occurrences == 8
    assert opened == ["Registerinformation.csv"] * 4
