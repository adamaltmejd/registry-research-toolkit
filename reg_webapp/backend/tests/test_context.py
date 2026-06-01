"""``GET /api/context`` smoke against the manifest-only fixture DB."""

from __future__ import annotations

from fastapi.testclient import TestClient
from reg_webapp.app import create_app
from reg_webapp.models import ContextResponse


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

    assert ctx.webapp.version
    assert ctx.webapp.reg_meta_version
