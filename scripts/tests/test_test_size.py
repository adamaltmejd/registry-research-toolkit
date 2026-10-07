"""Enforce 800-line test modules; an oversized module is split by contract surface.

Only test_*.py and conftest.py under tests/ and conformance/ count. Source fixtures
and support scripts are not test modules. New untracked files are scanned as well.
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_test_modules_stay_under_800_lines(python_test_files):
    files = [
        path
        for path in python_test_files
        if path.name.startswith("test_") or path.name == "conftest.py"
    ]
    assert files, "No test modules scanned"
    oversized = {
        path.relative_to(ROOT).as_posix(): lines
        for path in files
        if (lines := len(path.read_text().splitlines())) > 800
    }
    assert not oversized, f"Split by contract surface: {oversized!r}"
