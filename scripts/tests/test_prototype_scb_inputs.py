"""Executable-contract tests for the SCB input-snapshot prototype."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

from reg_meta_build.input_snapshot import SCB_CSV_FILES, prepare_snapshot

_MODULE = Path(__file__).resolve().parents[1] / "prototype_scb_inputs.py"


def _run(*args: object) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(_MODULE), *(str(arg) for arg in args)],
        check=False,
        capture_output=True,
        text=True,
    )


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
