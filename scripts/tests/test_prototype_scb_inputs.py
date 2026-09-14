"""Executable-contract tests for the SCB input-snapshot prototype."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from reg_meta_build.input_snapshot import SCB_CSV_FILES, SnapshotStats, prepare_snapshot

from conftest import load_scripts_module

if TYPE_CHECKING:
    import pytest

_MODULE = Path(__file__).resolve().parents[1] / "prototype_scb_inputs.py"
prototype_scb_inputs = load_scripts_module("prototype_scb_inputs")


def _run(*args: object) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(_MODULE), *(str(arg) for arg in args)],
        check=False,
        capture_output=True,
        text=True,
    )


def test_executable_help_is_nonempty() -> None:
    process = _run("--help")

    assert process.returncode == 0
    assert process.stdout.startswith("usage: prototype_scb_inputs.py")
    assert "measure-codecs" in process.stdout
    assert "prepare" in process.stdout
    assert process.stderr == ""


def test_executable_verifies_a_prepared_snapshot(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    source = source_dir / "Registerinformation.csv"
    source.write_bytes(b'unknown|blank\r\n001|""\r\n')
    archive = tmp_path / "retained.zip"
    archive.write_bytes(b"independently retained fixture")
    inventory = tmp_path / "inventory.json"
    inventory.write_text(
        json.dumps(
            {
                "bundle_id": "scb-cli-fixture",
                "edition": "2026-09-14",
                "source_dir": str(source_dir),
                "files": [
                    {
                        "name": name,
                        "required": name == "Registerinformation.csv",
                    }
                    for name in SCB_CSV_FILES
                ],
                "archives": [
                    {
                        "path": str(archive),
                        "locator": "offline/retained.zip",
                        "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
                        "members": ["Registerinformation.csv"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    snapshot = tmp_path / "snapshot"
    prepare_snapshot(inventory, snapshot, converter_commit="a" * 40)

    verify = _run("verify", snapshot)
    assert verify.returncode == 0, verify.stderr
    assert json.loads(verify.stdout) == {
        "bundle_id": "scb-cli-fixture",
        "edition": "2026-09-14",
        "files": 6,
        "records": 1,
        "status": "verified",
    }


def test_prepare_derives_provenance_from_the_executed_cli(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inventory = tmp_path / "inventory.json"
    output = tmp_path / "snapshot"
    calls: list[Path] = []

    def provenance(cli_path: Path) -> str:
        calls.append(cli_path)
        return "b" * 40

    monkeypatch.setattr(prototype_scb_inputs, "converter_source_commit", provenance)
    monkeypatch.setattr(
        prototype_scb_inputs,
        "prepare_snapshot",
        lambda *_args, **_kwargs: SnapshotStats(
            raw_bytes=0,
            normalized_bytes=0,
            records=0,
            elapsed_seconds=0,
            codec_sample={},
        ),
    )

    args = prototype_scb_inputs._parser().parse_args(
        ["prepare", str(inventory), str(output)]
    )
    assert prototype_scb_inputs._run(args)["records"] == 0
    assert calls == [_MODULE]


def test_prepare_rejects_caller_supplied_converter_checkout(tmp_path: Path) -> None:
    process = _run(
        "prepare",
        tmp_path / "inventory.json",
        tmp_path / "snapshot",
        "--builder-repo",
        tmp_path / "unrelated",
    )

    assert process.returncode == 2
    assert "unrecognized arguments: --builder-repo" in process.stderr
