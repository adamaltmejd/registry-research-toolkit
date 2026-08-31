"""`POST /api/project/order` — the FastAPI adapter over reg_meta's shared order
materializer, against the slugged ``catalog_db`` fixture.

See DESIGN.md → Project-write surface (routes/project.py) and reg_meta/DESIGN.md
→ Order materializer and manifest. The endpoint is a THIN adapter
(REFACTOR_SPEC.md §12), so the materializer's own rules are pinned by
``reg_meta/tests/test_order.py``; what belongs HERE is the adapter contract: the
``order.json`` download shape, the "not an order" 422s (an invalid spec and a
fail-closed blocked order alike — never a partial 200), and the byte-identity
with the ``reg-meta order`` CLI that is §12's whole point.

These tests run the deployment with NO steward configured, and that one has no
``inventory.toml``, so it runs §12's global-deployment fallback
(``inventory=None``) — hence ``steward: "global"`` in the spec below. A NAMED
steward gets no such fallback (see the boot guard below). The fixture's ``scb/lisa/kon`` binding resolves to
``delivery_column_name = "Kon"`` at variant ``individer-15plus`` / state
``2018-01-01..9999-12-31``.
"""

from __future__ import annotations

import json

import pytest
from _steward_helpers import CLEAN_SOURCES, write_steward
from fastapi.testclient import TestClient
from reg_meta.order import OrderManifest
from reg_webapp.app import create_app


@pytest.fixture
def client(catalog_db):
    with TestClient(create_app()) as c:
        yield c


def _spec(*, steward: str = "global", period: object = 2018) -> dict:
    return {
        "schema_version": "2.0.0",
        "steward": steward,
        "reg_meta_version": "5.1.0",
        "name": "test",
        "sources": [
            {
                "name": "lisa-2018",
                "register_variant": "scb/lisa/individer-15plus",
                "period": period,
                "bindings": [{"variable": "scb/lisa/kon", "type": "categorical"}],
            }
        ],
    }


def test_manifest_download_shape(client):
    resp = client.post("/api/project/order", json=_spec())
    assert resp.status_code == 200, resp.text
    assert resp.headers["content-type"].startswith("application/json")
    assert "attachment" in resp.headers["content-disposition"]
    assert "order.json" in resp.headers["content-disposition"]


def test_body_is_the_manifests_own_canonical_serialization(client):
    """The 200 body is ``OrderManifest.to_json()`` VERBATIM — FastAPI never
    re-serializes it — so it round-trips through the reg_meta contract model and
    keeps that serialization's sorted keys + trailing newline."""
    resp = client.post("/api/project/order", json=_spec())
    manifest = OrderManifest.model_validate(json.loads(resp.text))
    assert resp.text == manifest.to_json()
    assert resp.text.endswith("\n")


def test_manifest_grounds_the_global_fallback_entry(client):
    """The deployment has no inventory, so §12's global fallback grounds the
    order: blank ``table``, the resolved canonical column, ``edition`` = the
    requested period. Pinned here because it is the boot wiring
    (``app.state.inventory is None``) that selects it, not the materializer."""
    resp = client.post("/api/project/order", json=_spec())
    manifest = OrderManifest.model_validate(json.loads(resp.text))
    assert manifest.provenance.mode == "global_fallback"
    assert manifest.provenance.steward == "global"
    (entry,) = manifest.entries
    assert entry.physical.table == ""
    assert entry.physical.column == "Kon"
    assert entry.physical.edition == "2018"


def test_named_steward_without_an_inventory_fails_at_boot(
    catalog_db, tmp_path, monkeypatch
):
    """The other side of the fallback above: an absent inventory means "global
    fallback" ONLY for the deployment with no steward configured. A NAMED
    steward booting into it would block every one of its own projects on
    ``steward_mismatch`` (the fallback demands ``steward == "global"``) from a
    server that reported itself healthy at startup — a deployment error
    deferred to, and paid by, each researcher in turn. So it fails at boot,
    naming the file to author (fail fast, like ``load_steward``'s own checks)."""
    stewards = tmp_path / "stewards"
    write_steward(stewards, "ifau", CLEAN_SOURCES, inventory=False)
    monkeypatch.setenv("REG_WEBAPP_STEWARDS_DIR", str(stewards))
    monkeypatch.setenv("REG_WEBAPP_STEWARD", "ifau")

    with (
        pytest.raises(FileNotFoundError, match="inventory.toml"),
        TestClient(create_app()),
    ):
        pass


def test_deterministic(client):
    """Same spec → byte-identical manifest (no timestamps, stable order)."""
    a = client.post("/api/project/order", json=_spec()).text
    b = client.post("/api/project/order", json=_spec()).text
    assert a == b


def test_byte_identical_to_the_cli_adapter(client, catalog_db, tmp_path, capsys):
    """§12's contract: the FastAPI adapter and ``reg-meta order`` are thin
    adapters over ONE materializer, so the same (project, inventory, DB) inputs
    produce byte-identical ``order.json`` on both surfaces."""
    from reg_meta.cli import run

    project_path = tmp_path / "project_data.json"
    project_path.write_text(json.dumps(_spec()), encoding="utf-8")

    exit_code = run(["order", str(project_path), "--db", str(catalog_db.parent)])
    assert exit_code == 0
    cli_manifest = capsys.readouterr().out

    web_manifest = client.post("/api/project/order", json=_spec()).text
    assert cli_manifest == web_manifest


def test_blocked_order_is_422_not_a_200_manifest(client):
    """A fail-closed blocked order is NOT an order: a 422 naming every finding,
    never a 200 with a partial manifest. Here the project's steward provenance
    does not match the deployment's (§12 blocks retargeting)."""
    resp = client.post("/api/project/order", json=_spec(steward="swecov"))
    assert resp.status_code == 422
    body = resp.json()
    assert "steward_mismatch" in body["detail"]
    assert "order blocked" in body["detail"]


def test_blocked_order_carries_the_findings_as_data(client):
    """The 422 body carries the materializer's OWN findings — code, message and
    the source/variable/period coordinates — as an array, not one flattened
    string. This is the contract the SPA renders per finding and any other
    client acts on; a blob would lose the structure at the boundary."""
    spec = _spec()
    # An FQID the catalog does not admit: grammatically fine (so the gate passes
    # it) and unresolvable (so the materializer fail-closes on THAT binding).
    spec["sources"][0]["bindings"].append(
        {"variable": "scb/lisa/ghostvar", "type": "numeric"}
    )
    resp = client.post("/api/project/order", json=spec)
    assert resp.status_code == 422

    findings = resp.json()["findings"]
    assert findings, "a blocked order must report at least one finding"
    for finding in findings:
        assert set(finding) == {"code", "message", "source", "variable", "period"}
    # The coordinates are DATA — the SPA locates the offending card by them.
    (blocking,) = [f for f in findings if f["variable"] == "scb/lisa/ghostvar"]
    assert blocking["code"] == "variable_unresolved"
    assert blocking["source"] == "lisa-2018"
    assert blocking["message"]


def test_blocked_findings_match_the_materializers_own(client, catalog_db):
    """Not a re-modeled echo: the array IS ``OrderResult.findings``, so the
    adapter cannot drift from the materializer it adapts."""
    from reg_meta.order import materialize_order, project_from_raw
    from reg_webapp.project_validation import per_request_conn

    spec = _spec(steward="swecov")
    with per_request_conn(catalog_db) as conn:
        expected = materialize_order(project_from_raw(spec), None, conn)

    findings = client.post("/api/project/order", json=spec).json()["findings"]
    assert findings == [f.model_dump(mode="json") for f in expected.findings]


def test_invalid_spec_422_carries_no_findings(client):
    """The gate's 422 is the SAME shape with an EMPTY findings array — nothing
    found the project unorderable, it was never ordered."""
    spec = _spec()
    spec["sources"][0]["period"] = "notaperiod"
    body = client.post("/api/project/order", json=spec).json()
    assert body["findings"] == []
    assert body["detail"]


def test_empty_project_is_blocked_not_a_header_only_manifest(client):
    spec = _spec()
    spec["sources"] = []
    resp = client.post("/api/project/order", json=spec)
    assert resp.status_code == 422
    body = resp.json()
    assert "project_empty" in body["detail"]
    assert [f["code"] for f in body["findings"]] == ["project_empty"]


def test_structurally_invalid_spec_is_422(client):
    """The shared gate (`order.project_from_raw`) runs before materialization: a
    Pydantic-valid but structurally invalid spec (a bad period token — a `str`,
    so the model accepts it) is a 422, not a manifest of a bad provider order."""
    spec = _spec()
    spec["sources"][0]["period"] = "notaperiod"
    resp = client.post("/api/project/order", json=spec)
    assert resp.status_code == 422, f"bad period → {resp.status_code}"


def test_unknown_root_field_is_422_at_structural_gate(client):
    spec = _spec()
    spec["reg_monabundle"] = {"binding_options": {}}
    resp = client.post("/api/project/order", json=spec)
    assert resp.status_code == 422
    assert "unexpected_field@/reg_monabundle" in resp.json()["detail"]


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
                lambda _: c.post("/api/project/order", json=_spec()).status_code,
                range(50),
            )
        )
    assert all(code == 200 for code in codes), f"cross-thread failures: {codes}"
