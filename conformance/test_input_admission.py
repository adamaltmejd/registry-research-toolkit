"""Explicit private-input admission fails at the pytest command boundary."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from reader_artifacts import build_reader_artifact

ROOT = Path(__file__).resolve().parents[1]


def collect(*options):
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "conformance/test_holdings_accounting.py",
            "--collect-only",
            "-q",
            *options,
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def test_private_input_requires_explicit_real_artifact_flags(tmp_path):
    result = collect(f"--holdings-input={tmp_path / 'missing'}")
    assert result.returncode == 4
    assert "--holdings-input requires --run-release and --artifact-dir" in result.stderr


def test_wrong_private_input_path_fails_before_collection(tmp_path):
    directory = tmp_path / "artifact"
    build_reader_artifact(directory, "reader", "steward")
    result = collect(
        "--run-release",
        f"--artifact-dir={directory}",
        f"--holdings-input={tmp_path / 'missing'}",
    )
    assert result.returncode == 4
    assert "Holdings input path or accepted manifest is invalid" in result.stderr
    assert "collected" not in result.stdout
