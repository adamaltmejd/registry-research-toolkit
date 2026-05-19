"""Shared fixtures for regmeta tests.

The fixture bodies live in `regmeta_build/tests/_shared_fixtures.py`;
this conftest just re-exports them so both `regmeta/tests/` and
`regmeta_build/tests/` see the same fixture DB without drifting.
"""

from __future__ import annotations

import sys
from pathlib import Path

# `_shared_fixtures` and `_csv_fixtures` are bare-name helpers in
# regmeta_build/tests/. Add that directory to sys.path so this query-side
# conftest can import them.
sys.path.insert(
    0, str(Path(__file__).resolve().parent.parent.parent / "regmeta_build" / "tests")
)

from _shared_fixtures import db_conn, db_path, fixture_db  # noqa: E402,F401
