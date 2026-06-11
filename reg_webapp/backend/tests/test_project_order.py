"""`POST /api/project/order` against the slugged ``catalog_db`` fixture.

See DESIGN.md → Project-write surface (routes/project.py + routes/bundle.py).
Covers the default v1 order-export CSV: the column header + shape, the
``text/csv`` content-type + ``Content-Disposition`` download header,
determinism (same spec → byte-identical CSV), the period serialization
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


def _spec(
    *,
    display_name: str | None = None,
    period: object = 2018,
    representation: str | None = None,
) -> dict:
    binding: dict = {"variable": "scb/lisa/kon", "type": "categorical"}
    if display_name is not None:
        binding["display_name"] = display_name
    if representation is not None:
        binding["representation"] = representation
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
        "representation",
        "period",
        "display_name",
    ]
    assert rows[1] == [
        "scb",
        "lisa",
        "individer-15plus",
        "scb/lisa/kon",
        "",
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
    assert rows[1][5] == "2018..2020"


def test_list_period_serializes_comma_joined(client):
    # #307: the interrupted-series wire grammar is comma-joined member wires.
    # The order goes through the full validate-then-render endpoint, so this
    # also pins that a list-period spec passes the structural gate end-to-end.
    resp = client.post(
        "/api/project/order",
        json=_spec(period=[{"from": 2018, "to": 2019}, {"from": 2021, "to": 2022}]),
    )
    rows = _rows(resp.text)
    assert rows[1][5] == "2018..2019,2021..2022"


def test_default_sentinel_period_serializes_literally(client):
    resp = client.post("/api/project/order", json=_spec(period="_default"))
    rows = _rows(resp.text)
    assert rows[1][5] == "_default"


def test_representation_column_survives_custom_display_name(client):
    # A binding with BOTH a representation and a custom display_name must still
    # carry the representation in its own column (else the provider can't tell
    # which delivery column was pinned).
    resp = client.post(
        "/api/project/order",
        json=_spec(display_name="Sex (detailed)", representation="kon_detalj"),
    )
    rows = _rows(resp.text)
    assert rows[0][4] == "representation"
    assert rows[1][4] == "kon_detalj"
    assert rows[1][-1] == "Sex (detailed)"


def test_deterministic(client):
    """Same spec → byte-identical CSV (no sort, no timestamps)."""
    a = client.post("/api/project/order", json=_spec(display_name="Sex")).text
    b = client.post("/api/project/order", json=_spec(display_name="Sex")).text
    assert a == b


def test_structurally_invalid_spec_is_422_not_bad_csv(client):
    """/order runs the structural gate before rendering: a Pydantic-valid but
    structurally-invalid spec (here a bad period token — a `str`, so the model
    accepts it, but the period grammar rejects it) is a 422, NOT a 200 CSV of a bad
    provider order (Codex P2)."""
    spec = _spec()
    spec["sources"][0]["period"] = "notaperiod"
    resp = client.post("/api/project/order", json=spec)
    assert resp.status_code == 422, f"bad period → {resp.status_code}"


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


@pytest.mark.parametrize(
    "evil",
    ['=HYPERLINK("http://evil","x")', "+1+1", "-2+3", "@SUM(A1:A9)", "\t=cmd"],
)
def test_csv_formula_injection_is_neutralized(client, evil):
    """The order CSV is handed to a data provider who opens it in a spreadsheet, so
    an attacker-controlled display_name beginning with a formula trigger (= + - @,
    leading tab/CR) must be neutralized — prefixed with a single quote so it's
    treated as text, not executed (the stdlib csv writer does NOT do this)."""
    resp = client.post("/api/project/order", json=_spec(display_name=evil))
    assert resp.status_code == 200
    cell = _rows(resp.text)[1][-1]  # the display_name column
    assert cell == "'" + evil, f"formula cell not neutralized: {cell!r}"


def test_concurrent_order_no_cross_thread_error(catalog_db):
    """/order opens a per-request reg_meta conn in the handler body — the same
    DB-backed write path the A5.2a/b-i cross-thread P1 lived on. Drive it from a
    ThreadPoolExecutor (TestClient's sequential default would mask a regression).
    Rate limit raised so the limiter doesn't 429 the burst."""
    from concurrent.futures import ThreadPoolExecutor

    with (
        TestClient(create_app(rate_limit_per_minute=100_000)) as c,
        ThreadPoolExecutor(max_workers=8) as pool,
    ):
        codes = list(
            pool.map(
                lambda _: (
                    c.post(
                        "/api/project/order", json=_spec(display_name="Sex")
                    ).status_code
                ),
                range(50),
            )
        )
    assert all(code == 200 for code in codes), f"cross-thread failures: {codes}"
