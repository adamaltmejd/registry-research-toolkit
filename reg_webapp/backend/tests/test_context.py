"""``GET /api/context`` smoke against the manifest-only fixture DB."""

from __future__ import annotations

from fastapi.testclient import TestClient
from reg_webapp.app import create_app
from reg_webapp.catalog_index import CatalogIndex
from reg_webapp.models import ContextResponse
from reg_webapp.routes.context import _catalog_period_span


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
    assert ctx.steward.catalog_period_span is None


def test_catalog_period_span_clamps_to_vintage_year():
    index = CatalogIndex(
        bindings_by_variant={},
        period_range_by_register={"scb/lisa": ("1995", "2030")},
        drift_warnings=(),
    )

    span = _catalog_period_span(index, vintage_year=2026)

    assert span is not None
    assert span.from_ == 1995
    assert span.to == 2026
