"""Coverage aggregates in the catalog listing payloads (#351).

Against the slugged ``catalog_db`` fixture: scb/lisa/kon (one open-ended state),
scb/rams (syss one open-ended state; inkjan/inkfeb stateless). Asserts the
additive `coverage` objects on the provider-children (register nodes) and
register-children (binding nodes), including the open-ended and stateless cases.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from reg_webapp.app import create_app


@pytest.fixture
def client(catalog_db):
    with TestClient(create_app()) as c:
        yield c


def test_provider_children_carry_register_coverage(client):
    body = client.get("/api/catalog/scb").json()
    by_fqid = {c["fqid"]: c for c in body["children"]}
    lisa = by_fqid["scb/lisa"]["coverage"]
    assert lisa["variable_count"] == 2  # kon + lonfink (merged monthly family, #319)
    assert lisa["open_ended"] is True  # kon state is open-ended
    assert lisa["coverage_to"] is None
    rams = by_fqid["scb/rams"]["coverage"]
    assert rams["variable_count"] == 3  # syss + inkjan + inkfeb (all slugged)


def test_register_children_carry_variable_coverage(client):
    body = client.get("/api/catalog/scb/lisa").json()
    kon = next(c for c in body["children"] if c.get("fqid") == "scb/lisa/kon")
    cov = kon["coverage"]
    assert cov["state_count"] == 1
    assert cov["coverage_from"] == "2018-01-01"
    assert cov["coverage_to"] is None  # open-ended
    assert cov["open_ended"] is True


def test_stateless_variable_coverage_is_zero(client):
    body = client.get("/api/catalog/scb/rams").json()
    by_fqid = {c.get("fqid"): c for c in body["children"]}
    for slug in ("inkjan", "inkfeb"):
        cov = by_fqid[f"scb/rams/{slug}"]["coverage"]
        assert cov["state_count"] == 0
        assert cov["coverage_from"] is None
        assert cov["coverage_to"] is None
        assert cov["open_ended"] is False


def test_variants_ref_child_has_no_coverage(client):
    # The variants-ref child is not a binding — it carries no coverage field.
    body = client.get("/api/catalog/scb/lisa").json()
    ref = next(c for c in body["children"] if c["kind"] == "variants-ref")
    assert "coverage" not in ref
