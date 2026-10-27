"""Tier 3 admission of a selected artifact (`--artifact-dir`): `reg-build verify`.

Failure messages carry only the first line of the verifier's stderr.
"""

from __future__ import annotations

import subprocess

import pytest
from reader_artifacts import REG_BUILD, REPO


def test_selected_artifact_passes_reg_build_verify(artifact_dir, request):
    """`reg-build verify`, the one structural authority over a catalog file. Synthetic
    artifacts are verified when `reg-build build` writes them, so only a selected one
    is checked here."""
    if request.config.getoption("--artifact-dir") is None:
        pytest.skip("synthetic artifacts are verified when they are built")
    verified = subprocess.run(
        [REG_BUILD, "verify", artifact_dir / "reg_meta.db"],
        cwd=REPO,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        check=False,
    )
    first = (verified.stderr.splitlines() or [""])[0]
    assert verified.returncode == 0, f"reg-build verify failed: {first}"
