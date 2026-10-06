"""Enforce 800-line test modules. The frozen file allowlist only shrinks.

Only test_*.py and conftest.py under tests/ and conformance/ count. Source fixtures
and support scripts are not test modules. New untracked files are scanned as well.
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ALLOWLIST = {
    "reg_schema/tests/test_structural.py",
}


def test_test_modules_stay_under_800_lines(python_test_files):
    files = [
        path
        for path in python_test_files
        if path.name.startswith("test_") or path.name == "conftest.py"
    ]
    assert files, "No test modules scanned"
    observed = {}
    for path in files:
        lines = len(path.read_text().splitlines())
        if lines > 800:
            observed[path.relative_to(ROOT).as_posix()] = lines
    assert set(observed) == ALLOWLIST, (
        "New oversized files: "
        + repr({name: observed[name] for name in sorted(set(observed) - ALLOWLIST)})
        + "; remove stale allowlist entries: "
        + repr(sorted(ALLOWLIST - set(observed)))
    )
