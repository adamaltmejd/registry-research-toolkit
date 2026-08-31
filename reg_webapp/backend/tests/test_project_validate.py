"""`POST /api/project/validate` against the slugged ``catalog_db`` fixture.

See DESIGN.md → Project-write surface (routes/project.py).
Covers the status discipline that defines this endpoint:

- a clean spec → 200 ``ok=true`` ``issues=[]``;
- an unresolvable FQID → 200 ``ok=false`` + the semantic issue (NOT 4xx —
  a validation failure is a successful validation RESPONSE);
- an extra/typo key on any CLOSED object (ProjectData/Source/Binding/Panel/member)
  → 200 with the structural ``unexpected_field`` issue (NOT 500);
- malformed JSON / duplicate keys / non-object body → 4xx (a malformed REQUEST);
- the two-layer concatenation (structural ⧺ semantic);
- a concurrency smoke test (the cross-thread sqlite P1 the sequential TestClient
  default MASKS — see ``test_catalog_browse``).

The fixture resolves ``scb/lisa/individer-15plus`` (variant) with binding
``scb/lisa/kon`` (state ``2018-01-01..9999-12-31``, value set) and the
classification ``class/sun2020``.
"""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor

import pytest
from _steward_helpers import IFAU_INVENTORY
from fastapi.testclient import TestClient
from reg_webapp.app import create_app


@pytest.fixture
def client(catalog_db):
    with TestClient(create_app()) as c:
        yield c


@pytest.fixture
def unthrottled_client(catalog_db):
    """A client whose app has the rate limit raised out of the way, so the
    cross-thread CONCURRENCY smoke test can fire >30 requests/min from one IP
    without the limiter (correctly) 429ing them. The limiter is still in the
    stack — its own behavior is covered in ``test_write_limits.py``."""
    with TestClient(create_app(rate_limit_per_minute=100_000)) as c:
        yield c


def _clean_spec() -> dict:
    return {
        "schema_version": "2.0.0",
        "steward": "ifau",
        "reg_meta_version": "5.1.0",
        "name": "test",
        "sources": [
            {
                "name": "lisa-2018",
                "register_variant": "scb/lisa/individer-15plus",
                "period": 2018,
                "bindings": [
                    {
                        "variable": "scb/lisa/kon",
                        "type": "categorical",
                        "value_set": "class/sun2020",
                    }
                ],
            }
        ],
    }


def test_clean_spec_is_ok(client):
    resp = client.post("/api/project/validate", json=_clean_spec())
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["issues"] == []


def test_openapi_documents_closed_canonical_project_root(client):
    openapi = client.get("/openapi.json").json()
    request_schema = openapi["paths"]["/api/project/validate"]["post"]["requestBody"][
        "content"
    ]["application/json"]["schema"]
    assert request_schema == {"$ref": "#/components/schemas/ProjectData"}
    schema = openapi["components"]["schemas"]["ProjectData"]
    assert schema["additionalProperties"] is False
    assert set(schema["properties"]) == {
        "schema_version",
        "steward",
        "reg_meta_version",
        "name",
        "sources",
        "panels",
        "window",
    }
    member_items = openapi["components"]["schemas"]["Panel"]["properties"]["members"][
        "items"
    ]
    assert {"type": "string"} in member_items["anyOf"]
    assert {"$ref": "#/components/schemas/PanelMember"} in member_items["anyOf"]


def test_unresolvable_fqid_is_200_not_4xx(client):
    """A binding FQID reg_meta doesn't admit → 200 with ``ok=false`` + the semantic
    ``fqid_unresolved`` issue. This is the load-bearing status discipline: a
    validation FAILURE is a successful validation RESPONSE (200), not a 4xx."""
    spec = _clean_spec()
    spec["sources"][0]["bindings"][0]["variable"] = "scb/lisa/nosuchvariable"
    resp = client.post("/api/project/validate", json=spec)
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is False
    codes = {(i["code"], i["level"]) for i in body["issues"]}
    assert ("fqid_unresolved", "error") in codes


def test_variable_replaced_successor_fqid_serializes(client):
    spec = _clean_spec()
    spec["sources"][0]["period"] = 2020
    resp = client.post("/api/project/validate", json=spec)
    assert resp.status_code == 200
    body = resp.json()
    issue = next(i for i in body["issues"] if i["code"] == "variable_replaced")
    assert issue["level"] == "info"
    assert issue["successor_fqid"] == "scb/rams/syss"
    assert body["ok"] is True


def test_extra_key_is_unexpected_field_not_500(client):
    """A typo'd key on a CLOSED object (Binding, extra=forbid) now surfaces as the
    canonical structural code ``unexpected_field`` (reg_schema owns it) — the
    extra-key case no longer routes through the webapp's ``invalid_field`` (which
    remains only as a rare defensive model-construction catch). A 200 ISSUE, NEVER
    a 500."""
    spec = _clean_spec()
    spec["sources"][0]["bindings"][0]["typoo_field"] = "oops"
    resp = client.post("/api/project/validate", json=spec)
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is False
    codes = {i["code"] for i in body["issues"]}
    assert "unexpected_field" in codes, codes


def test_calendar_invalid_period_is_structural_200_not_500(client):
    """#239 regression: a calendar-impossible day (`2019-02-29`, non-leap) is now
    rejected at the STRUCTURAL layer (`invalid_period`), so the retired #238
    semantic guard can't recur as an uncaught `date.fromisoformat` 500. A 200
    ISSUE, never a 500. The semantic layer is genuinely SKIPPED on structural
    failure (`_validate_blocking` only runs it `if structural.ok`): assert no
    semantic period codes (`period_outside_state_validity`,
    `range_period_partially_covered`) appear alongside the structural finding."""
    spec = _clean_spec()
    spec["sources"][0]["period"] = "2019-02-29"
    resp = client.post("/api/project/validate", json=spec)
    assert resp.status_code == 200, resp.status_code
    body = resp.json()
    assert body["ok"] is False
    codes = {i["code"] for i in body["issues"]}
    assert "invalid_period" in codes, codes
    # Semantic was skipped: none of its period codes leak through.
    assert "period_outside_state_validity" not in codes, codes
    assert "range_period_partially_covered" not in codes, codes


def test_non_leap_feb_range_to_endpoint_is_200_not_500(client):
    """#239 follow-up: a VALID range whose `to` is a non-leap `YYYY-02` month
    token (`{"from": 2019, "to": "2019-02"}`) passes structural validation, then
    the semantic gap math runs real `date` arithmetic on the SYNTHESIZED upper
    bound — which reg_meta over-counts to `2019-02-29` (not a real date). Without
    the month-end snap this 500s; assert a clean 200 (`scb/lisa/kon` covers all of
    2019, so the range is fully covered: no period issue)."""
    spec = _clean_spec()
    spec["sources"][0]["period"] = {"from": 2019, "to": "2019-02"}
    resp = client.post("/api/project/validate", json=spec)
    assert resp.status_code == 200, resp.status_code
    body = resp.json()
    assert body["ok"] is True, body["issues"]
    codes = {i["code"] for i in body["issues"]}
    assert "range_period_partially_covered" not in codes, codes


def test_model_issue_empty_loc_is_whole_document_pointer():
    """A model-level (empty-``loc``) residual ValidationError must map to the RFC
    6901 whole-document pointer ``""`` — NOT ``"/"`` (a property keyed by the empty
    string, unresolvable). A5.3's SPA resolves these pointers, so the contract is
    exact. Defensive path (structural owns the common cases), unit-tested directly."""
    from pydantic_core import ValidationError
    from reg_webapp.routes.project import _model_issue

    exc = ValidationError.from_exception_data(
        "ProjectData", [{"type": "missing", "loc": (), "input": {}}]
    )
    issue = _model_issue("residual model error", exc)
    assert issue.path == ""
    assert issue.code == "invalid_field"


def test_nested_extra_key_is_issue_not_500(client):
    """A typo'd nested ``Source`` field is a clean 200 issue, not a traceback."""
    spec = _clean_spec()
    spec["sources"][0]["register_varient"] = "typo"  # nested Source, extra=forbid
    resp = client.post("/api/project/validate", json=spec)
    assert resp.status_code == 200
    assert resp.json()["ok"] is False


def test_malformed_json_is_4xx(client):
    resp = client.post(
        "/api/project/validate",
        content=b"{not valid json",
        headers={"content-type": "application/json"},
    )
    assert resp.status_code == 400


def test_duplicate_keys_is_4xx(client):
    """A hand-edited spec with a duplicated JSON key is a malformed REQUEST (the
    last-wins default would validate the wrong value), so 4xx, not a 200."""
    resp = client.post(
        "/api/project/validate",
        content=b'{"name": "a", "name": "b"}',
        headers={"content-type": "application/json"},
    )
    assert resp.status_code == 400


def test_non_object_body_is_4xx(client):
    resp = client.post("/api/project/validate", json=[1, 2, 3])
    assert resp.status_code == 400


def test_deeply_nested_json_is_400_not_500(client):
    """A deeply-nested JSON body (well-formed, small, under the 1 MB cap) makes
    json.loads raise RecursionError — a RuntimeError, NOT a ValueError/
    JSONDecodeError. It must map to a malformed REQUEST (400), not escape as a 500
    (a write-side input crash on attacker-controlled input — see DESIGN.md →
    input-validation gates (security boundary))."""
    body = b"[" * 50_000 + b"]" * 50_000  # ~100 KB, depth 50k
    resp = client.post(
        "/api/project/validate",
        content=body,
        headers={"content-type": "application/json"},
    )
    assert resp.status_code == 400, f"deep-nested → {resp.status_code} (want 400)"


def test_invalid_utf8_body_is_400_not_500(client):
    """Invalid UTF-8 bytes make json.loads raise UnicodeDecodeError — a ValueError
    subclass, so the shared reader's `except ValueError` maps it to 400, not a 500
    (a write-side input crash). Pinned because the coverage is non-obvious (the
    except clauses don't NAME UnicodeDecodeError)."""
    resp = client.post(
        "/api/project/validate",
        content=b'{"x":"\xff"}',
        headers={"content-type": "application/json"},
    )
    assert resp.status_code == 400, f"invalid-utf8 → {resp.status_code} (want 400)"


def test_two_layer_concatenation(client):
    """The response issue list concatenates the two layers (no merge): structural
    runs first, and the reg_meta-backed semantic layer runs only when structural
    passes. Feed a structurally CLEAN spec whose binding is unresolvable so the
    semantic layer fires, and assert the semantic code lands in the one 200 list
    with no spurious structural code."""
    spec = _clean_spec()
    spec["sources"][0]["bindings"][0]["variable"] = "scb/lisa/nosuchvariable"
    resp = client.post("/api/project/validate", json=spec)
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is False
    codes = {i["code"] for i in body["issues"]}
    assert "fqid_unresolved" in codes, codes
    # Structural passed, so no structural code is concatenated alongside it.
    assert "unexpected_field" not in codes, codes


def test_unknown_root_fields_are_200_unexpected_field_issues(client):
    """Raw ingress preserves every invalid root key through structural diagnostics."""
    spec = _clean_spec()
    spec["z_scalar"] = 7
    spec["a/object~key"] = {"nested": True}
    resp = client.post("/api/project/validate", json=spec)
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is False
    issues = [issue for issue in body["issues"] if issue["code"] == "unexpected_field"]
    assert [issue["path"] for issue in issues] == ["/a~1object~0key", "/z_scalar"]


def test_concurrent_validate_no_cross_thread_error(unthrottled_client):
    """The A5.2a/b-i cross-thread sqlite P1: a generator-dependency-opened
    connection used cross-thread → ``sqlite3.ProgrammingError`` (reproduced 72/80
    on #168). The semantic step opens the connection in the sync handler body
    (one thread), so concurrent validates must all 200. TestClient's sequential
    default MASKS this, so drive it through a thread pool (against the
    rate-limit-raised client so the limiter doesn't 429 the burst)."""
    spec = _clean_spec()
    with ThreadPoolExecutor(max_workers=8) as pool:
        codes = list(
            pool.map(
                lambda _: (
                    unthrottled_client.post(
                        "/api/project/validate", json=spec
                    ).status_code
                ),
                range(50),
            )
        )
    failures = [c for c in codes if c != 200]
    assert not failures, f"cross-thread failures under concurrency: {failures}"


# ── #227: fqid_outside_steward_catalog through a FILTERED steward client ────
# A filtered steward deployment (catalog admits only `scb/lisa/kon`) surfaces
# `fqid_outside_steward_catalog` (warning) for a researcher spec referencing a
# resolvable-but-unadmitted FQID — the steward filter now wired into /validate.
# The default `client` fixture boots the `global` steward (no index), so this
# needs its own env-seam client (mirrors test_steward_index.py's seam).

_IFAU_TOML = """\
id = "ifau"
name = "IFAU"
long_name = "Institute for Evaluation of Labour Market and Education Policy"
hostname = "ifau.example.org"
"""


@pytest.fixture
def filtered_client(catalog_db, tmp_path, monkeypatch):
    """A client whose app boots the `ifau` steward with a catalog admitting ONLY
    `scb/lisa/kon`, so a researcher FQID outside it (`scb/rams/syss`) trips the
    steward filter."""
    base = tmp_path / "stewards" / "ifau"
    base.mkdir(parents=True)
    (base / "steward.toml").write_text(_IFAU_TOML, encoding="utf-8")
    (base / "steward.project_data.json").write_text(
        json.dumps(
            {
                "schema_version": "2.0.0",
                "steward": "ifau",
                "reg_meta_version": "5.1.0",
                "name": "ifau-catalog",
                "sources": [
                    {
                        "name": "lisa",
                        "register_variant": "scb/lisa/individer-15plus",
                        "period": 2018,
                        "bindings": [
                            {
                                "variable": "scb/lisa/kon",
                                "type": "categorical",
                                "value_set": "class/sun2020",
                            }
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    # A named steward must ship a delivery inventory or the deployment refuses
    # to boot (stewards.load_delivery_inventory) — irrelevant to validation,
    # required to get a client at all.
    (base / "inventory.toml").write_text(IFAU_INVENTORY, encoding="utf-8")
    monkeypatch.setenv("REG_WEBAPP_STEWARDS_DIR", str(tmp_path / "stewards"))
    monkeypatch.setenv("REG_WEBAPP_STEWARD", "ifau")
    with TestClient(create_app()) as c:
        yield c


def test_fqid_outside_steward_catalog_via_filtered_client(filtered_client):
    spec = {
        "schema_version": "2.0.0",
        "steward": "ifau",
        "reg_meta_version": "5.1.0",
        "name": "test",
        "sources": [
            {
                "name": "rams",
                "register_variant": "scb/rams/standard",
                "period": 2019,
                "bindings": [{"variable": "scb/rams/syss", "type": "numeric"}],
            }
        ],
    }
    resp = filtered_client.post("/api/project/validate", json=spec)
    assert resp.status_code == 200
    body = resp.json()
    # The FQID resolves reg_meta-wide but is outside the steward's catalog → a
    # non-blocking warning, so ok stays True.
    assert body["ok"] is True
    outside = [i for i in body["issues"] if i["code"] == "fqid_outside_steward_catalog"]
    assert len(outside) == 1
    assert outside[0]["level"] == "warning"
