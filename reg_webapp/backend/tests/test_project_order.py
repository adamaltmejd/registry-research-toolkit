"""`POST /api/project/order` against the slugged ``catalog_db`` fixture (§9.5).

Covers the default v1 order-export CSV: the column header + shape, the
``text/csv`` content-type + ``Content-Disposition`` download header,
determinism (same spec → byte-identical CSV), the §9.5 period serialization
(int / range / ``_default``), and the ``display_name`` fallback from reg_meta's
``delivery_column_name`` when the binding leaves ``display_name`` unset.

The fixture's ``scb/lisa/kon`` binding has ``delivery_column_name = "Kon"`` at
variant ``individer-15plus`` / state ``2018-01-01..9999-12-31``.
"""

from __future__ import annotations

import csv
import io

import pytest
from fastapi.testclient import TestClient
from reg_webapp.app import create_app


@pytest.fixture
def client(catalog_db):
    with TestClient(create_app()) as c:
        yield c


def _spec(*, display_name: str | None = None, period: object = 2018) -> dict:
    binding: dict = {"variable": "scb/lisa/kon", "type": "categorical"}
    if display_name is not None:
        binding["display_name"] = display_name
    return {
        "schema_version": "2.0.0",
        "steward": "ifau",
        "reg_meta_version": "5.1.0",
        "name": "test",
        "sources": [
            {
                "name": "lisa-2018",
                "register_variant": "scb/lisa/individer-15plus",
                "period": period,
                "bindings": [binding],
            }
        ],
    }


def _rows(csv_text: str) -> list[list[str]]:
    return list(csv.reader(io.StringIO(csv_text)))


def test_content_type_and_disposition(client):
    resp = client.post("/api/project/order", json=_spec())
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/csv")
    assert "attachment" in resp.headers["content-disposition"]
    assert "order.csv" in resp.headers["content-disposition"]


def test_csv_header_and_row_shape(client):
    resp = client.post("/api/project/order", json=_spec(display_name="Sex"))
    rows = _rows(resp.text)
    assert rows[0] == [
        "provider",
        "register",
        "variant",
        "variable",
        "period",
        "display_name",
    ]
    assert rows[1] == [
        "scb",
        "lisa",
        "individer-15plus",
        "scb/lisa/kon",
        "2018",
        "Sex",
    ]


def test_display_name_fallback_from_reg_meta(client):
    """No explicit ``display_name`` → the renderer resolves the binding at the
    source's ``(variant, period)`` and uses ``delivery_column_name`` (``"Kon"``
    in the fixture)."""
    resp = client.post("/api/project/order", json=_spec())
    rows = _rows(resp.text)
    assert rows[1][-1] == "Kon"


def test_range_period_serializes_with_double_dot(client):
    resp = client.post(
        "/api/project/order", json=_spec(period={"from": 2018, "to": 2020})
    )
    rows = _rows(resp.text)
    assert rows[1][4] == "2018..2020"


def test_default_sentinel_period_serializes_literally(client):
    resp = client.post("/api/project/order", json=_spec(period="_default"))
    rows = _rows(resp.text)
    assert rows[1][4] == "_default"


def test_deterministic(client):
    """Same spec → byte-identical CSV (no sort, no timestamps)."""
    a = client.post("/api/project/order", json=_spec(display_name="Sex")).text
    b = client.post("/api/project/order", json=_spec(display_name="Sex")).text
    assert a == b


def test_unresolvable_binding_falls_back_to_fqid_leaf(client):
    """A binding whose FQID doesn't resolve still renders a row — the order is a
    manifest, so the display_name best-effort falls back to the bare FQID leaf
    rather than crashing (unresolved bindings are the validator's job to flag)."""
    spec = _spec()
    spec["sources"][0]["bindings"][0]["variable"] = "scb/lisa/nosuchvar"
    resp = client.post("/api/project/order", json=spec)
    assert resp.status_code == 200
    rows = _rows(resp.text)
    assert rows[1][-1] == "nosuchvar"
