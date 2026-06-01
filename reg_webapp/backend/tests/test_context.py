"""``GET /api/context`` smoke against the manifest-only fixture DB."""

from __future__ import annotations

import reg_meta.db
from fastapi.testclient import TestClient
from reg_webapp.app import create_app
from reg_webapp.models import ContextResponse


def test_context_returns_200_and_shape(compatible_db, fixture_import_date):
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

    assert ctx.reg_meta.schema_version == reg_meta.db.SCHEMA_VERSION
    assert ctx.reg_meta.import_date == fixture_import_date

    assert ctx.webapp.version
    assert ctx.webapp.reg_meta_version
