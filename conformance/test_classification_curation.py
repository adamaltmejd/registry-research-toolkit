"""Classification curation refusals at the builder CLI boundary."""

from __future__ import annotations

import json
import shutil
import subprocess
from typing import TYPE_CHECKING

import pytest
from reader_artifacts import CASES, REG_BUILD, REPO

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize(
    "case",
    json.loads((CASES / "classification-curation/refusals.json").read_text()),
    ids=lambda case: case["name"],
)
def test_classification_curation_refusals(tmp_path: Path, case: dict) -> None:
    # Fails if inventory metadata stops entering binding premises, or missing version
    # and reader-list references escape located refusal, or a source binds a version.
    # Oracles are authored JSON;
    # existing order cases exercise successful binding, not builder lock invalidation.
    # Also fails if duplicate versions, dangling predecessors, cycles, conflicting
    # family facts or source-authored bindings
    # stop refusing at their authoring locations.
    source = tmp_path / "fixture"
    shutil.copytree(CASES / "fixtures/classification-pins", source)

    def run(command: str) -> subprocess.CompletedProcess[str]:
        args = [
            REG_BUILD,
            command,
            "--inputs",
            source / "inputs",
            "--curation",
            source / "curation",
        ]
        if command == "build":
            args.extend(["--out", tmp_path / "catalog.db", "--release"])
        return subprocess.run(
            args, cwd=REPO, capture_output=True, text=True, check=False
        )

    locked = run("lock")
    assert locked.returncode == 0, locked.stderr
    path = source / case["file"]
    path.write_text(path.read_text().replace(case["before"], case["after"], 1))
    refused = run(case["command"])
    assert refused.returncode == 1
    for expected in case["expected"]:
        assert expected in refused.stderr, refused.stderr
