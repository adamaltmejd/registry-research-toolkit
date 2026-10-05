"""``GET /api/context`` smoke against the manifest-only fixture DB."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from http_cases import CASES, assert_http_case
from reg_webapp.app import create_app
from reg_webapp.models import ContextResponse


@pytest.mark.parametrize(
    "case", sorted((CASES / "context").iterdir()), ids=lambda p: p.name
)
def test_context_contract(case, tmp_path, monkeypatch):
    assert_http_case(case, tmp_path, monkeypatch)


def test_context_returns_200_and_shape(
    compatible_db, fixture_schema_version, fixture_import_date
):
    # TestClient drives the lifespan, opening the fixture DB read-only.
    with TestClient(create_app()) as client:
        resp = client.get("/api/context")
    assert resp.status_code == 200

    body = resp.json()
    # The committed response_model shape — parses cleanly into the Pydantic model.
    ctx = ContextResponse.model_validate(body)

    assert ctx.steward.id == "global"
    assert ctx.steward.name
    assert ctx.steward.long_name

    # The fixture's schema_version differs from reg_meta.SCHEMA_VERSION in the
    # patch, so this proves /api/context surfaces the MANIFEST value.
    assert ctx.reg_meta.schema_version == fixture_schema_version
    assert ctx.reg_meta.import_date == fixture_import_date
    assert ctx.reg_meta.catalog_artifact_kind == "catalog"
    assert ctx.reg_meta.steward is None
    assert ctx.reg_meta.default_scope == "reference"
    assert len(ctx.reg_meta.generation_id) == 64
    assert "catalog_drift_warnings" not in body

    assert ctx.webapp.version
    assert ctx.webapp.reg_meta_version
    assert ctx.steward.catalog_period_span is None


def test_context_uses_compiled_physical_periods(steward_db):
    with TestClient(create_app()) as client:
        body = client.get("/api/context").json()
    assert body["reg_meta"]["catalog_artifact_kind"] == "steward"
    assert body["reg_meta"]["steward"] == "swecov"
    assert body["reg_meta"]["default_scope"] == "holdings"
    assert body["steward"]["catalog_period_span"] == {"from": 2018, "to": 2019}
    assert "catalog_drift_warnings" not in body


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
