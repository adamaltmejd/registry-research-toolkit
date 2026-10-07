"""Enforce 800-line test modules; an oversized module is split by contract surface.

Python: only test_*.py and conftest.py under tests/ and conformance/ count. Source
fixtures and support scripts are not test modules. Frontend: every Vitest file
(`*.test.ts`, unit and browser projects) under reg_webapp/frontend/src counts; its
colocated `*-test-helpers.ts` modules do not. New untracked files are scanned as well.
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MAX_LINES = 800


def _oversized(files: list[Path]) -> dict[str, int]:
    return {
        path.relative_to(ROOT).as_posix(): lines
        for path in files
        if (lines := len(path.read_text().splitlines())) > MAX_LINES
    }


def test_test_modules_stay_under_800_lines(python_test_files):
    files = [
        path
        for path in python_test_files
        if path.name.startswith("test_") or path.name == "conftest.py"
    ]
    assert files, "No test modules scanned"
    oversized = _oversized(files)
    assert not oversized, f"Split by contract surface: {oversized!r}"


def test_frontend_test_files_stay_under_800_lines(frontend_test_files):
    assert frontend_test_files, "No frontend test files scanned"
    oversized = _oversized(frontend_test_files)
    assert not oversized, f"Split by contract surface: {oversized!r}"
