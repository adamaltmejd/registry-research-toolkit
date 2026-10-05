"""Artifact identity → application boot, with readable admission cases."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from reg_meta.errors import RegMetaError
from reg_webapp.app import create_app
from webapp_fixture_support import fixture_db

CASES = Path(__file__).parent / "cases/boot"


@pytest.mark.parametrize("case", sorted(CASES.iterdir()), ids=lambda p: p.name)
def test_artifact_boot(case, tmp_path, monkeypatch):
    request = json.loads((case / "request.json").read_text())
    expected = json.loads((case / "expected.json").read_text())
    path = fixture_db.build_reader_fixture_db(
        tmp_path / "catalog", kind=request["kind"]
    )
    with sqlite3.connect(path) as conn:
        conn.executemany(
            "INSERT OR REPLACE INTO import_manifest VALUES (?, ?)",
            request.get("manifest", {}).items(),
        )
    monkeypatch.setenv("REG_META_DB", str(path.parent))
    monkeypatch.setenv("REG_WEBAPP_STEWARD", request["configured_steward"])
    if request.get("poison_inventory"):
        stewards = tmp_path / "stewards"
        directory = stewards / "swecov"
        directory.mkdir(parents=True)
        source = Path(__file__).parents[2] / "stewards/swecov/steward.toml"
        (directory / "steward.toml").write_bytes(source.read_bytes())
        (directory / "inventory.toml").write_text(
            "this is deliberately invalid TOML [[[\n"
        )
        monkeypatch.setenv("REG_WEBAPP_STEWARDS_DIR", str(stewards))
    try:
        with TestClient(create_app()):
            pass
    except (RuntimeError, RegMetaError) as exc:
        result = {"ok": False, "type": type(exc).__name__}
        if "code" in expected:
            result["code"] = exc.code
        if "message_contains" in expected:
            assert expected["message_contains"] in str(exc)
            assert str(path) in str(exc)
            result["message_contains"] = expected["message_contains"]
    else:
        result = {"ok": True}
    assert result == expected
