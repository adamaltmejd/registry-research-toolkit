"""Curation source fixtures → located lock refusal, before catalog publication."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

from reader_artifacts import REG_BUILD, REPO


def test_assignments_refuse_concurrent_folds_at_lock(tmp_path):
    # Regression: assignment resolution accepts concurrent different folds in one
    # table, leaving only artifact verify to reject the resulting segment. The
    # one-day overlap must refuse beside nonconcurrent and cross-table allowed twins.
    case = Path(__file__).parent / "cases/lock/concurrent-fold-assignment"
    expected = json.loads((case / "expected.json").read_text())
    curation = tmp_path / "curation"
    shutil.copytree(case / "curation", curation)
    statements = curation / "registers/demo/assignment/curation.toml"
    command = [
        REG_BUILD,
        "lock",
        "--inputs",
        case / "inputs",
        "--curation",
        curation,
    ]
    statements.write_text((case / "allowed.toml").read_text())
    allowed = subprocess.run(
        command, cwd=REPO, capture_output=True, text=True, check=False
    )
    assert allowed.returncode == 0, allowed.stderr

    statements.write_text((case / "refused.toml").read_text())
    refused = subprocess.run(
        command, cwd=REPO, capture_output=True, text=True, check=False
    )
    assert refused.returncode == expected["exit_code"]
    assert expected["located_error"] in refused.stderr
