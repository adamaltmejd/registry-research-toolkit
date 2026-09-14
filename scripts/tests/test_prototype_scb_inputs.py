"""Executable-contract tests for the SCB input-snapshot prototype."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

from reg_meta_build.input_snapshot import SCB_CSV_FILES

from conftest import load_scripts_module

_MODULE = Path(__file__).resolve().parents[1] / "prototype_scb_inputs.py"
prototype_scb_inputs = load_scripts_module("prototype_scb_inputs")


def _run(*args: object) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(_MODULE), *(str(arg) for arg in args)],
        check=False,
        capture_output=True,
        text=True,
    )


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True
    )


def test_executable_help_is_nonempty() -> None:
    process = _run("--help")

    assert process.returncode == 0
    assert process.stdout.startswith("usage: prototype_scb_inputs.py")
    assert "measure-codecs" in process.stdout
    assert "prepare" in process.stdout
    assert process.stderr == ""


def test_executable_prepares_and_verifies_a_snapshot(tmp_path: Path) -> None:
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
    builder = tmp_path / "builder"
    builder.mkdir()
    (builder / "tracked.txt").write_text("fixture\n", encoding="utf-8")
    _git(builder, "init", "-q")
    _git(builder, "config", "user.email", "test@example.invalid")
    _git(builder, "config", "user.name", "Test")
    _git(builder, "add", ".")
    _git(builder, "commit", "-q", "-m", "fixture")

    snapshot = tmp_path / "snapshot"
    prepare = _run("prepare", inventory, snapshot, "--builder-repo", builder)
    assert prepare.returncode == 0, prepare.stderr
    assert json.loads(prepare.stdout)["records"] == 1

    verify = _run("verify", snapshot)
    assert verify.returncode == 0, verify.stderr
    assert json.loads(verify.stdout) == {
        "bundle_id": "scb-cli-fixture",
        "edition": "2026-09-14",
        "files": 6,
        "records": 1,
        "status": "verified",
    }
