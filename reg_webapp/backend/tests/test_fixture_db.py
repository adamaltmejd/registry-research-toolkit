"""The fixture-DB builder writes a pair the real backend boots and serves.

`scripts/fixture_db.py` is what the `catalog_db` / `docs_db` fixtures build AND
what `dev.sh --fixture-db` runs to serve a synthetic catalog where no released DB
is reachable. The fixtures cover the catalog DB in isolation; this covers the
`build_fixture_db_dir` entry point dev.sh actually calls — a REG_META_DB
directory that `create_app` boots against and serves rows from.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

from fastapi.testclient import TestClient
from reg_webapp.app import create_app

# Load the sibling script directly, without mutating sys.path (mirrors
# test_openapi_snapshot.py), so its bare-name imports don't leak.
_FIXTURE_DB_PATH = Path(__file__).resolve().parents[1] / "scripts" / "fixture_db.py"
_spec = importlib.util.spec_from_file_location(
    "reg_webapp_fixture_db_test", _FIXTURE_DB_PATH
)
assert _spec and _spec.loader
fixture_db = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fixture_db)


def test_built_dir_boots_and_serves_a_populated_catalog(tmp_path, monkeypatch):
    db_dir = fixture_db.build_fixture_db_dir(tmp_path / "fixture")
    monkeypatch.setenv("REG_META_DB", str(db_dir))

    # Entering the context runs the lifespan — the schema-compat gate, the catalog
    # open, and the optional docs-DB open — so a bad pair fails here, not later.
    with TestClient(create_app()) as client:
        assert client.get("/api/context").status_code == 200
        assert client.get("/api/catalog").json()["children"]
        assert client.get("/api/stats").json()["variables"] > 0
        assert client.get("/api/docs/doc/Kon.md").status_code == 200
