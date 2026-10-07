"""``GET /api/context``: the steward period span is capped to the import vintage.

The catalog-kind and steward-kind context bodies are pinned by
``conformance/cases/http_context``.
"""

from __future__ import annotations

from fastapi.testclient import TestClient
from reg_webapp.app import create_app


def test_context_caps_physical_span_to_import_vintage(tmp_path, monkeypatch):
    from webapp_fixture_support import fixture_db

    path = fixture_db.build_reader_fixture_db(
        tmp_path / "steward",
        kind="steward",
        identity_overrides={"import_date": "2018-12-31T00:00:00Z"},
    )
    monkeypatch.setenv("REG_META_DB", str(path.parent))
    monkeypatch.setenv("REG_WEBAPP_STEWARD", "swecov")
    with TestClient(create_app()) as client:
        body = client.get("/api/context").json()
    assert body["steward"]["catalog_period_span"] == {"from": 2018, "to": 2018}
