"""``GET /api/stats`` against the slugged ``catalog_db`` fixture (#675/#726).

The headline catalog-size counts the landing page renders. Asserts 200 and
that the unfiltered ``global`` deployment returns reg_meta's slug-aware
``Catalog.catalog_sizes()`` value. Filtered-steward coverage then asserts that
the route counts through the steward index instead of the full DB.
"""

from __future__ import annotations

import pytest
import reg_meta.db
from _steward_helpers import write_steward
from fastapi.testclient import TestClient
from reg_meta.catalog import Catalog, CatalogSizes
from reg_webapp.app import create_app
from reg_webapp.etag import CACHE_CONTROL_SHORT


@pytest.fixture
def client(catalog_db):
    with TestClient(create_app()) as c:
        yield c


def test_stats_returns_global_catalog_sizes(client, catalog_db):
    resp = client.get("/api/stats")
    assert resp.status_code == 200
    assert resp.headers["cache-control"] == CACHE_CONTROL_SHORT

    stats = CatalogSizes.model_validate(resp.json())
    conn = reg_meta.db.open_db(catalog_db, check_schema=False)
    try:
        expected = Catalog(conn).catalog_sizes()
    finally:
        conn.close()
    assert stats == expected


def test_stats_uses_steward_index_for_filtered_catalog(
    catalog_db, tmp_path, monkeypatch
):
    stewards = tmp_path / "stewards"
    monkeypatch.setenv("REG_WEBAPP_STEWARDS_DIR", str(stewards))
    monkeypatch.setenv("REG_WEBAPP_STEWARD", "ifau")
    write_steward(
        stewards,
        "ifau",
        [
            {
                "name": "lisa",
                "register_variant": "scb/lisa/individer-15plus",
                "period": 2018,
                "bindings": [{"variable": "scb/lisa/kon", "type": "categorical"}],
            }
        ],
    )

    with TestClient(create_app()) as client:
        resp = client.get("/api/stats")

    assert resp.status_code == 200
    assert resp.headers["cache-control"] == CACHE_CONTROL_SHORT
    assert CatalogSizes.model_validate(resp.json()) == CatalogSizes(
        providers=1, registers=1, variables=1
    )
