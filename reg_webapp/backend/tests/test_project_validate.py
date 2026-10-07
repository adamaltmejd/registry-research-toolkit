"""`POST /api/project/validate` against the slugged ``catalog_db`` fixture.

See DESIGN.md → Project-write surface (routes/project.py). The endpoint's
response contract (clean, unresolved, clipped, malformed) is pinned by the
readable ``conformance/cases/validate`` corpus; what stays here:

- unknown root keys surface as ``unexpected_field`` issues in sorted,
  RFC 6901-escaped path order;
- a concurrency smoke test (the cross-thread sqlite P1 the sequential TestClient
  default MASKS).

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
def unthrottled_client(catalog_db):
    """A client whose app has the rate limit raised out of the way, so the
    cross-thread CONCURRENCY smoke test can fire >30 requests/min from one IP
    without the limiter (correctly) 429ing them. The limiter is still in the
    stack — its own behavior is covered in ``test_write_limits.py``."""
    with TestClient(create_app(rate_limit_per_minute=100_000)) as c:
        yield c


def _clean_spec() -> dict:
    return {
        "schema_version": "3.0.0",
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
