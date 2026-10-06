"""CLI contracts of prepare-input-bundle LISA selection and inspect-source-records."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from typing import TYPE_CHECKING

from _csv_fixtures import (
    sparsify_scb_values,
    var_row,
    write_input_bundle,
    write_scb_input,
    write_scb_snapshot,
)
from _lisa_fixtures import write_lisa_workbook
from _source_inspection_fixtures import (
    InterpreterCheckout,
    record_path_opens,
    snapshot_record_files,
)
from reg_meta.errors import EXIT_CONFIG, EXIT_USAGE
from reg_meta_build.input_snapshot import (
    LISA_DATASET_ID,
    LisaWorkbookSelection,
    open_input_bundle,
)
from reg_meta_build.source_inspection import (
    SourceInspectionReport,
    inspect_bundle_source_records,
    report_semantic_sha256,
)

from reg_meta_build import cli as cli_module

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any

    import pytest


def test_prepare_cli_requires_complete_lisa_selection(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    candidate = tmp_path / "candidate"
    exit_code = cli_module.run(
        [
            "prepare-input-bundle",
            "--input-dir",
            str(tmp_path / "inputs"),
            "--scb-snapshot",
            str(tmp_path / "snapshot"),
            "--scb-input-commit",
            "a" * 40,
            "--scb-manifest-sha256",
            "b" * 64,
            "--output-dir",
            str(candidate),
            "--lisa-workbook",
            str(tmp_path / "lisa.xlsx"),
        ]
    )

    assert exit_code == EXIT_USAGE
    error = json.loads(capsys.readouterr().out)["error"]
    assert error["code"] == "lisa_source_selection_incomplete"
    assert not candidate.exists()


def test_prepare_cli_never_writes_output_over_selected_external_lisa(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    snapshot = write_scb_snapshot(tmp_path / "accepted", input_dir / "SCB")
    workbook = write_lisa_workbook(tmp_path / "official-source" / "lisa.xlsx")
    source_bytes = workbook.read_bytes()
    exit_code = cli_module.run(
        [
            "--output",
            str(workbook),
            "prepare-input-bundle",
            "--input-dir",
            str(input_dir),
            "--scb-snapshot",
            str(snapshot.path),
            "--scb-input-commit",
            snapshot.input_commit,
            "--scb-manifest-sha256",
            snapshot.manifest_sha256,
            "--output-dir",
            str(snapshot.path.parent / "candidate"),
            "--lisa-workbook",
            str(workbook),
            "--lisa-revision",
            "2024-2025",
            "--lisa-workbook-sha256",
            hashlib.sha256(source_bytes).hexdigest(),
        ]
    )

    assert exit_code == EXIT_CONFIG
    # The guard runs before the command: preparation never creates its candidate.
    assert not (snapshot.path.parent / "candidate").exists()
    error = json.loads(capsys.readouterr().out)["error"]
    assert error["code"] == "catalog_input_output_conflict"
    assert workbook.read_bytes() == source_bytes


def test_inspection_cli_rejects_bundle_where_lisa_was_not_selected(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    selection = write_input_bundle(tmp_path / "accepted", input_dir)
    checkout = InterpreterCheckout(tmp_path / "interpreter")

    result = checkout.run(
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

    assert result.returncode != 0
    error = json.loads(result.stdout)["error"]
    assert error["code"] == "source_record_inspection_invalid"
    assert "was not selected" in error["message"]
    assert "explicitly captures" in error["remediation"]


def test_pinned_bundle_cli_reports_deterministic_source_targets_without_cold_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rows = [
        var_row(
            colname="AmPolTyp",
            cvid=1,
            var_id=31619,
            year="2020",
            regver_id=200,
            register=("LISA", 34, 153),
            vardef="Kept definition",
        ),
        var_row(
            colname="",
            cvid=2,
            var_id=31619,
            year="2021",
            regver_id=201,
            register=("LISA", 34, 153),
            data_type="text",
        ),
        var_row(
            colname="ampoltyp",
            cvid=3,
            var_id=99,
            year="2022",
            regver_id=202,
            register=("LISA", 34, 1335),
        ),
        var_row(
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

    # Cold values are gone from disk: opening any value stream would fail the run.
    checkout = InterpreterCheckout(tmp_path / "interpreter")
    outputs = [tmp_path / "report-a.json", tmp_path / "report-b.json"]
    reports: list[dict[str, Any]] = []
    for output in outputs:
        result = checkout.run(
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
        assert result.returncode == 0, result.stderr
        assert result.stdout == ""
        reports.append(json.loads(output.read_text(encoding="utf-8")))

    assert reports[0] == reports[1]
    report = reports[0]
    assert report["schema_version"] == 3
    assert report["diagnostic_only"] is True
    assert report["preview_level"] == "source_target_only"
    assert report["complete"] is True
    assert report["pins"] == {
        "bundle_id": "scb-mikrometadata",
        "bundle_manifest_sha256": selection.manifest_sha256,
        "code_commit": checkout.commit,
        "input_repository_commit": selection.input_commit,
        "interpretation_id": "scb-lisa-source-record-inspection-v7",
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
    assert set(report["target_preview"]["assumption_ids"]) >= {
        "lisa-individual-1990-2009-to-scb-variant-1335",
        "lisa-individual-2010-2024-to-scb-variant-153",
        "same-native-variable-spelling-continuity",
    }
    assert 2024 in report["target_preview"]["unobserved_documented_editions"]
    assert len(report["target_preview"]["target_record_ids"]) == 1
    assert report["summary"]["outcome_counts"]["unknown_spelling"] == 1
    assert report["summary"]["outcome_counts"].get("unknown_applicability", 0) == 0
    assert report["summary"]["outcome_counts"]["unobserved_counterpart"] == 1
    assert report["summary"]["outcome_counts"]["missing_edition"] >= 1
    agreement = next(
        outcome
        for outcome in report["comparison_outcomes"]
        if outcome["status"] == "agreement"
    )
    assert len(agreement["workbook_record_ids"]) == 1
    assert len(agreement["scb_record_ids"]) == 1
    assert agreement["assumption_ids"] == [
        "lisa-individual-2010-2024-to-scb-variant-153"
    ]
    cross_variant_casefold = next(
        record
        for record in report["source_records"]
        if record["subject"]["native"].get("member_id") == 3
    )
    assert (
        cross_variant_casefold["record_id"]
        in report["target_preview"]["casefold_collision_record_ids"]
    )
    assert any(
        outcome["status"] == "missing_edition"
        and outcome["scb_edition_present"] is False
        and not outcome["scb_record_ids"]
        and {"start": "2022", "end": "2022"} in outcome["edition_scope"]["intervals"]
        for outcome in report["comparison_outcomes"]
    )
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

    bundles = [open_input_bundle(selection) for _ in range(3)]
    record_files = snapshot_record_files(bundle.snapshot, "Registerinformation.csv")
    opened = record_path_opens(monkeypatch)
    filtered = inspect_bundle_source_records(
        bundles[0], code_commit="c" * 40, exact_column="AmPolTyp"
    )
    full_a = inspect_bundle_source_records(bundles[1], code_commit="c" * 40)
    full_b = inspect_bundle_source_records(bundles[2], code_commit="c" * 40)
    assert filtered.target_preview is not None
    assert full_a == full_b
    assert full_a.target_preview is None
    assert full_a.summary.workbook_selected_occurrences == 9
    # One streaming pass per inspection, and never a value stream.
    assert Counter(path for path in opened if path in record_files) == dict.fromkeys(
        record_files, 3
    )
    assert not any("Vardemangder.csv" in path.parts for path in opened)


def test_source_interpreter_pin_rejects_a_loaded_dependency_from_another_checkout(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
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
    # reg_meta is loaded from a second clean repository, not the builder's checkout.
    checkout = InterpreterCheckout(tmp_path / "interpreter", split_reg_meta=True)

    result = checkout.run(
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

    assert result.returncode == EXIT_CONFIG
    error = json.loads(result.stdout)["error"]
    assert "must come from the same Git checkout" in error["message"]
