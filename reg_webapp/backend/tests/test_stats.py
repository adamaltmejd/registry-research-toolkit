"""``GET /api/stats`` against the slugged ``catalog_db`` fixture (#675).

The headline catalog-size counts the landing page renders. Asserts 200 and
that the three integer fields are each positive (the fixture seeds several
providers, registers, and variables) — a smoke that the slug-aware counts
reach a populated DB through the per-request connection seam. The slug-aware
exclusion itself (counts match the browsable listings, not raw ``COUNT(*)``)
is covered by reg_meta's ``test_catalog_listing.py::TestCatalogSizes``.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from reg_meta.catalog import CatalogSizes
from reg_webapp.app import create_app


@pytest.fixture
def client(catalog_db):
    with TestClient(create_app()) as c:
        yield c


def test_stats_returns_200_and_positive_counts(client):
    resp = client.get("/api/stats")
    assert resp.status_code == 200

    # Parses cleanly into the committed response_model shape (three ints) —
    # reg_meta's `CatalogSizes`, computed slug-aware by `Catalog.catalog_sizes()`.
    stats = CatalogSizes.model_validate(resp.json())

    # The fixture seeds several providers, registers, and variables, so the
    # slug-aware counts are all positive.
    assert stats.providers > 0
    assert stats.registers > 0
    assert stats.variables > 0
