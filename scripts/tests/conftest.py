"""Shared fixtures for the `scripts/` unit tests.

Helpers that test files import live in `_scripts_support.py`, never here: a bare
`from conftest import ...` resolves to another suite's `conftest` when several test trees
share one pytest session (#1401).
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]


def _repo_files(pathspec: str) -> list[Path]:
    """Tracked and unignored files matching a git pathspec (`*` crosses `/`)."""
    names = subprocess.check_output(
        [
            "git",
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "--",
            pathspec,
        ],
        cwd=_ROOT,
        text=True,
    ).splitlines()
    return sorted({_ROOT / name for name in names if (_ROOT / name).is_file()})


@pytest.fixture(scope="session")
def python_test_files() -> list[Path]:
    """Discover tracked and unignored Python test/support files once per scan."""
    return [
        path
        for path in _repo_files("*.py")
        if {"tests", "conformance"} & set(path.relative_to(_ROOT).parts)
    ]


@pytest.fixture(scope="session")
def frontend_test_files() -> list[Path]:
    """Discover tracked and unignored Vitest files (unit and browser projects)."""
    return _repo_files("reg_webapp/frontend/src/*.test.ts")
