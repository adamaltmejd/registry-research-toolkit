"""Coverage aggregates in the catalog listing payloads (#351).

Against the slugged ``catalog_db`` fixture: scb/lisa/kon (one open-ended state),
scb/rams (inkjan/inkfeb stateless). Asserts the additive `coverage` objects on
the register-children (binding nodes), open-ended and stateless. The register
coverage on provider children is pinned by
``conformance/cases/http_catalog/provider-register-coverage``.
"""

from __future__ import annotations


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
