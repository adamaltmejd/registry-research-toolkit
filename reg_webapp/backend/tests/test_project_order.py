"""`POST /api/project/order` — the FastAPI adapter over reg_meta's shared order
materializer, against the slugged ``catalog_db`` fixture.

See DESIGN.md → Project-write surface (routes/project.py) and reg_meta/DESIGN.md → Order
materializer and manifest. The endpoint is a THIN adapter: its manifest bytes, the
CLI byte identity and the located refusal findings are pinned by ``conformance/``
(``cases/validate`` order steps, ``test_validate.py``, ``test_artifact.py``). What
stays here is the ``order.json`` download shape and the "not an order" 422s (an
invalid spec and a fail-closed blocked order alike — never a partial 200), with the
CLI's agreement on those refusals.

The fixture is a catalog artifact, so the materializer uses global fallback
and requires ``steward: "global"`` in the spec below. Steward orderability
requires a steward artifact with compiled holdings. The fixture's ``scb/lisa/kon`` binding resolves to
``delivery_column_name = "Kon"`` at variant ``individer-15plus`` / state
``2018-01-01..9999-12-31``.
"""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient
from reg_webapp.app import create_app


def _spec(*, steward: str = "global", period: object = 2018) -> dict:
    return {
        "schema_version": "3.0.0",
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


def test_blocked_order_is_422_not_a_200_manifest_and_reads_the_same_on_the_cli(
    client, catalog_db, tmp_path, capsys
):
    """A fail-closed blocked order is NOT an order: a 422 naming every finding,
    never a 200 with a partial manifest. Here the project's steward provenance
    does not match the deployment's (retargeting is blocked).

    The byte-identical-adapters rule covers this path too, so ``reg-meta
    order``'s error envelope carries the SAME single line — both render
    ``order.blocked_message``. The WORDING is the materializer's, pinned once in
    ``reg_meta/tests/test_order_findings.py``; what belongs here is the flattening rule
    (code + message, one line) and the coordinates surviving as data beside it."""
    from reg_meta.cli import run
    from reg_meta.errors import EXIT_NO_MATCH

    spec = _spec(steward="swecov")
    resp = client.post("/api/project/order", json=spec)
    assert resp.status_code == 422
    body = resp.json()

    (finding,) = body["findings"]
    assert finding["code"] == "steward_mismatch"
    # Whole-project finding: no coordinate to name, and none invented.
    assert (finding["source"], finding["variable"], finding["period"]) == (
        None,
        None,
        None,
    )
    # One line: this string is read inside a JSON envelope and inside the SPA's
    # banner, where an embedded newline is an escape sequence / collapsed space.
    assert "\n" not in body["detail"]

    project_path = tmp_path / "project_data.json"
    project_path.write_text(json.dumps(spec), encoding="utf-8")
    exit_code = run(["order", str(project_path), "--db", str(catalog_db.parent)])
    assert exit_code == EXIT_NO_MATCH
    assert json.loads(capsys.readouterr().out)["error"]["message"] == body["detail"]


@pytest.mark.parametrize("version", ["99.0.0", "not-a-version"])
def test_unsupported_version_is_rejected_by_every_consumer(
    client, catalog_db, tmp_path, capsys, version
):
    """The equal-surfaces rule, applied to the supported-version decision: ONE
    project written for another schema contract, every consumer that reads a raw
    project, one answer — and no manifest anywhere. The CLI's error message and
    the adapter's 422 detail are the same words because both render reg_meta's
    single decision (``order.schema_version_issue``). The SUPPORTED version's
    agreement across these surfaces is ``conformance/test_validate.py``."""
    from reg_meta.cli import run
    from reg_meta.errors import EXIT_CONFIG

    spec = _spec()
    spec["schema_version"] = version

    validated = client.post("/api/project/validate", json=spec).json()
    assert validated["ok"] is False
    assert [i["code"] for i in validated["issues"]] == ["unsupported_schema_version"]

    ordered = client.post("/api/project/order", json=spec)
    assert ordered.status_code == 422
    body = ordered.json()
    assert body["findings"] == []
    assert "entries" not in body
    assert version in body["detail"]

    project_path = tmp_path / "project_data.json"
    project_path.write_text(json.dumps(spec), encoding="utf-8")
    exit_code = run(["order", str(project_path), "--db", str(catalog_db.parent)])
    assert exit_code == EXIT_CONFIG
    cli_error = json.loads(capsys.readouterr().out)["error"]
    assert cli_error["code"] == "unsupported_schema_version"
    assert cli_error["message"] == body["detail"]


def test_empty_project_is_blocked_not_a_header_only_manifest(client):
    spec = _spec()
    spec["sources"] = []
    resp = client.post("/api/project/order", json=spec)
    assert resp.status_code == 422
    body = resp.json()
    assert "project_empty" in body["detail"]
    assert [f["code"] for f in body["findings"]] == ["project_empty"]


@pytest.mark.parametrize("period", ["notaperiod", "_default"])
def test_structurally_invalid_spec_is_422(client, period):
    """The shared gate (`order.project_from_raw`) runs before materialization: a
    Pydantic-valid but structurally invalid spec (a bad period token — a `str`,
    so the model accepts it) is a 422, not a manifest of a bad provider order.
    The retired whole-history `_default` sentinel is now one of those tokens."""
    spec = _spec(period=period)
    resp = client.post("/api/project/order", json=spec)
    assert resp.status_code == 422, f"bad period → {resp.status_code}"
    assert "entries" not in resp.json()


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
