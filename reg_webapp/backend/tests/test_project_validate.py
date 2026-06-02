"""`POST /api/project/validate` against the slugged ``catalog_db`` fixture (§9.5).

Covers the §6.8.0 status discipline that defines this endpoint:

- a clean spec → 200 ``ok=true`` ``issues=[]``;
- an unresolvable FQID → 200 ``ok=false`` + the §6.8.3 semantic issue (NOT 4xx —
  a validation failure is a successful validation RESPONSE);
- an extra/typo key → 200 with an ``invalid_field`` issue (NOT 500 — the
  ``ProjectData`` model's ``extra=forbid`` raise is caught and turned into an
  issue);
- malformed JSON / duplicate keys / non-object body → 4xx (a malformed REQUEST);
- the three-layer concatenation (structural ⧺ block ⧺ semantic);
- a concurrency smoke test (the cross-thread sqlite P1 the sequential TestClient
  default MASKS — see ``test_catalog_browse``).

The fixture resolves ``scb/lisa/individer-15plus`` (variant) with binding
``scb/lisa/kon`` (state ``2018-01-01..9999-12-31``, value set) and the
classification ``class/sun2020``.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient
from reg_webapp.app import create_app


@pytest.fixture
def client(catalog_db):
    with TestClient(create_app()) as c:
        yield c


@pytest.fixture
def unthrottled_client(catalog_db):
    """A client whose app has the §9.4 rate limit raised out of the way, so the
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


def test_unresolvable_fqid_is_200_not_4xx(client):
    """A binding FQID reg_meta doesn't admit → 200 with ``ok=false`` + the §6.8.3
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


def test_validate_catches_orphan_column_options(client):
    """Divergence reconciliation: an orphan ``reg_monabundle.column_options`` key
    (a binding FQID not bound in any source) that /api/bundle 422s on is now ALSO
    flagged by /validate (code ``column_options_orphan_fqid``) — so a /validate-
    clean spec is buildable for that class."""
    spec = _clean_spec()
    spec["reg_monabundle"] = {
        "column_options": {"scb/lisa/notbound": {"suppress_k": 10}}
    }
    resp = client.post("/api/project/validate", json=spec)
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is False
    assert "column_options_orphan_fqid" in {i["code"] for i in body["issues"]}


def test_malformed_column_options_value_is_issue_not_500(client):
    """A non-dict per-FQID column_options value (an int) is malformed — the block
    validator flags it (invalid_block). /validate ACCUMULATES issues (it doesn't
    fail-fast like /bundle's raise), so the cross-block check must skip the non-dict
    value defensively, not `"suppress_k" not in <int>` → TypeError → 500."""
    spec = _clean_spec()
    spec["reg_monabundle"] = {"column_options": {"scb/lisa/kon": 1}}  # bound, non-dict
    resp = client.post("/api/project/validate", json=spec)
    assert resp.status_code == 200, f"malformed column_options → {resp.status_code}"
    body = resp.json()
    assert body["ok"] is False
    assert "invalid_block" in {i["code"] for i in body["issues"]}


def test_top_level_extra_key_is_issue_not_500(client):
    """``ProjectData`` itself is ``extra=ignore`` (it tolerates namespaced
    blocks), so a stray TOP-LEVEL key does not raise — but a typo'd nested
    ``Source`` field (``extra=forbid``) does. Assert the nested-typo path is a
    clean 200 issue rather than a 500 traceback out of the handler."""
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
    (a §16 write-side input crash on attacker-controlled input)."""
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
    (a §16 write-side input crash). Pinned because the coverage is non-obvious (the
    except clauses don't NAME UnicodeDecodeError)."""
    resp = client.post(
        "/api/project/validate",
        content=b'{"x":"\xff"}',
        headers={"content-type": "application/json"},
    )
    assert resp.status_code == 400, f"invalid-utf8 → {resp.status_code} (want 400)"


def test_three_layer_concatenation(client):
    """The response issue list concatenates the three §6.8.0 layers (no merge).
    Feed a spec that trips structural (bad period token) AND semantic
    (unresolvable value_set) and assert codes from BOTH layers appear in the one
    200 list."""
    spec = _clean_spec()
    # Structural: a malformed period token → an emitted structural issue.
    spec["sources"][0]["period"] = "not-a-period!"
    # Semantic would also fire, but structural failure short-circuits the model
    # build; the block layer is independent, so add a bad block too to prove
    # block issues ride alongside structural ones.
    spec["reg_monabundle"] = {"column_options": {"bogus": {"suppress_k": 5}}}
    resp = client.post("/api/project/validate", json=spec)
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is False
    codes = {i["code"] for i in body["issues"]}
    # A structural issue (the bad period) and the block issue both present.
    assert any(c != "invalid_block" for c in codes), codes
    assert "invalid_block" in codes, codes


def test_block_layer_runs_even_when_structural_passes(client):
    """A structurally-clean spec with a broken ``reg_monabundle`` block → the
    block layer fires its ``invalid_block`` issue (the §6.8.2 layer is composed
    in regardless of the structural outcome)."""
    spec = _clean_spec()
    spec["reg_monabundle"] = {"unknown_key": 1}
    resp = client.post("/api/project/validate", json=spec)
    assert resp.status_code == 200
    codes = {i["code"] for i in resp.json()["issues"]}
    assert "invalid_block" in codes


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
