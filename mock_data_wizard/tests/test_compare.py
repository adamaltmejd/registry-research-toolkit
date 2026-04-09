"""Tests for the compare command."""

from __future__ import annotations

import json
from pathlib import Path

from mock_data_wizard.cli import main


def test_compare_columns(regmeta_db: Path, capsys):
    rc = main(
        [
            "compare",
            "--columns",
            "Kon,FakeColumn",
            "--register",
            "TESTREG",
            "--db",
            str(regmeta_db.parent),
            "--format",
            "json",
        ]
    )
    assert rc == 0
    data = json.loads(capsys.readouterr().out)
    files = data["files"]
    assert len(files) == 1
    f = files[0]
    assert f["register_status"] == "resolved"
    assert f["summary"]["matched"] >= 1
    matched_cols = {m["column"] for m in f["matched"]}
    assert "Kon" in matched_cols
    assert "FakeColumn" in f["extra_local"]


def test_compare_columns_requires_register(regmeta_db: Path):
    rc = main(
        [
            "compare",
            "--columns",
            "Kon",
            "--db",
            str(regmeta_db.parent),
        ]
    )
    assert rc == 1


def test_compare_manifest(regmeta_db: Path, tmp_path: Path, capsys):
    manifest = {
        "schema_version": "2",
        "generated_at": "2026-03-23T00:00:00Z",
        "seed": 42,
        "sample_pct": 1.0,
        "output_dir": str(tmp_path),
        "files": [
            {
                "file_name": "test.csv",
                "relative_path": "test.csv",
                "row_count": 100,
                "sha256": "abc",
                "columns": ["Kon", "UnknownCol"],
                "column_count": 2,
                "delimiter": ",",
                "encoding": "utf-8",
                "header_hash": "abc",
                "register_hint": 1,
                "year_hint": 2020,
            }
        ],
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))

    rc = main(
        [
            "compare",
            str(manifest_path),
            "--db",
            str(regmeta_db.parent),
            "--format",
            "json",
        ]
    )
    assert rc == 0
    data = json.loads(capsys.readouterr().out)
    f = data["files"][0]
    assert f["register_status"] == "resolved"
    assert f["register_name"] == "TESTREG"
    assert f["year_hint"] == 2020
    assert f["summary"]["matched"] >= 1
    assert "UnknownCol" in f["extra_local"]


def test_compare_files(regmeta_db: Path, tmp_path: Path, capsys):
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("Kon,Extra\n1,y\n")

    rc = main(
        [
            "compare",
            "--files",
            str(csv_path),
            "--register",
            "TESTREG",
            "--db",
            str(regmeta_db.parent),
            "--format",
            "json",
        ]
    )
    assert rc == 0
    f = json.loads(capsys.readouterr().out)["files"][0]
    assert f["register_status"] == "resolved"
    assert "Extra" in f["extra_local"]


def test_compare_bad_manifest_version(regmeta_db: Path, tmp_path: Path):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps({"schema_version": "1", "files": []}))

    rc = main(
        [
            "compare",
            str(manifest_path),
            "--db",
            str(regmeta_db.parent),
        ]
    )
    assert rc == 1


def test_compare_strips_project_prefix(regmeta_db: Path, capsys):
    """P1105_Kon should match Kon after prefix stripping."""
    rc = main(
        [
            "compare",
            "--columns",
            "P1105_Kon,P1105_FakeCol",
            "--register",
            "TESTREG",
            "--db",
            str(regmeta_db.parent),
            "--format",
            "json",
        ]
    )
    assert rc == 0
    data = json.loads(capsys.readouterr().out)
    f = data["files"][0]
    assert f["summary"]["matched"] >= 1
    matched_cols = {m["column"] for m in f["matched"]}
    assert "Kon" in matched_cols
    assert "FakeCol" in f["extra_local"]


def test_compare_table_output(regmeta_db: Path, capsys):
    rc = main(
        [
            "compare",
            "--columns",
            "Kon,FakeColumn",
            "--register",
            "TESTREG",
            "--db",
            str(regmeta_db.parent),
        ]
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "matched" in out
    assert "extra_local" in out
