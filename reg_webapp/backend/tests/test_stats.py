"""``GET /api/stats`` against the slugged ``catalog_db`` fixture (#675/#726).

The headline catalog-size counts the landing page renders. Asserts 200 and
that the unfiltered ``global`` deployment returns reg_meta's slug-aware
``Catalog.catalog_sizes()`` value. Scoped compiled-artifact counts are covered by the HTTP scope corpus.
"""

from __future__ import annotations

import pytest
import reg_meta.db
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
