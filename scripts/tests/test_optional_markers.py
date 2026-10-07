"""The root conftest gates opt-in tests by marker, never by path or module name.

pytest's `item.keywords` also holds parent directory and module names, so a keyword
check skipped the whole suite from a checkout under a directory named `integration`
(e.g. `.claude/worktrees/integration`) while reporting green.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT_CONFTEST = Path(__file__).resolve().parents[2] / "conftest.py"

GATED_MODULE = """
import pytest

def test_unmarked():
    pass

@pytest.mark.{name}
def test_decorated():
    pass

@pytest.mark.parametrize("value", [pytest.param(1, marks=pytest.mark.{name})])
def test_param_marked(value):
    pass
"""

MODULE_MARKED = """
import pytest

pytestmark = pytest.mark.{name}

def test_module_marked():
    pass
"""


def _outcomes(tmp_path: Path, name: str, *args: str) -> dict[str, str]:
    """Run the root conftest over a suite nested in a directory named `name`."""
    shutil.copy(ROOT_CONFTEST, tmp_path / "conftest.py")
    (tmp_path / "pytest.ini").write_text(
        "[pytest]\nmarkers =\n    integration: gated\n    release: gated\n"
    )
    suite = tmp_path / name
    suite.mkdir()
    (suite / f"test_{name}.py").write_text(GATED_MODULE.format(name=name))
    (suite / "test_module_marked.py").write_text(MODULE_MARKED.format(name=name))
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-v", "-p", "no:cacheprovider", *args],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    outcomes = {}
    for line in result.stdout.splitlines():
        nodeid, _, outcome = line.partition(" ")
        if "::" in nodeid and outcome:
            outcomes[nodeid.rpartition("::")[2]] = outcome.split()[0]
    return outcomes


@pytest.mark.parametrize("name", ["integration", "release"])
def test_only_marked_tests_skip_under_marker_named_directory(
    tmp_path: Path, name: str
) -> None:
    assert _outcomes(tmp_path, name) == {
        "test_unmarked": "PASSED",
        "test_decorated": "SKIPPED",
        "test_param_marked[1]": "SKIPPED",
        "test_module_marked": "SKIPPED",
    }


@pytest.mark.parametrize("name", ["integration", "release"])
def test_opt_in_flag_runs_marked_tests(tmp_path: Path, name: str) -> None:
    assert set(_outcomes(tmp_path, name, f"--run-{name}").values()) == {"PASSED"}
