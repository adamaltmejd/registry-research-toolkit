"""Load the shared dev/HTTP artifact builder without a conftest module collision."""

from __future__ import annotations

import importlib.util
from pathlib import Path

# Load the sibling builder script directly, without mutating sys.path (mirrors
# test_openapi_snapshot.py), so its bare-name imports don't leak.
_FIXTURE_DB_PATH = Path(__file__).resolve().parents[1] / "scripts" / "fixture_db.py"
_spec = importlib.util.spec_from_file_location(
    "reg_webapp_fixture_db", _FIXTURE_DB_PATH
)
assert _spec and _spec.loader
fixture_db = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fixture_db)
