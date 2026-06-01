"""`POST /api/bundle` — MONA bundle build (§9.5, A5.2b-ii).

Covers the verified 3-call reuse chain (validate_project_data →
project_data_to_loadedspec → build_bundle): a valid project → 200
``application/octet-stream`` non-empty bytes; determinism (pure function of
input — the §16 bundle-determinism property); a build-gate failure (bad input)
→ 422; and a concurrency smoke test (each build uses its own
``TemporaryDirectory``, so concurrent builds must not collide).

The bundle build is DB-FREE (it amalgamates the runtime + embeds the JSON), so
this does not need the ``catalog_db`` fixture — but the app still boots against
a manifest DB (``compatible_db``) because the lifespan opens reg_meta.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient
from reg_webapp.app import create_app


@pytest.fixture
def client(compatible_db):
    with TestClient(create_app()) as c:
        yield c


@pytest.fixture
def unthrottled_client(compatible_db):
    """Rate-limit raised so the concurrency smoke test isn't 429'd (see
    ``test_project_validate``)."""
    with TestClient(create_app(rate_limit_per_minute=100_000)) as c:
        yield c


def _valid_spec() -> dict:
    """A structurally + build-gate valid project. NB: the bundle build is reg_meta
    -free, so the binding FQIDs need only be well-formed (no DB resolution) — the
    build gate is the §6.8.1 structural + §6.8.2 block + step-4 capability checks,
    not the §6.8.3 semantic layer. ``display_name`` is REQUIRED here: the step-4
    capability gate (``project_data_to_loadedspec``) rejects a binding without one
    because reg_meta default-resolution doesn't run in the reg_meta-free bundle
    path (it lands in §15 step 6) — so a bundle spec must carry explicit names."""
    return {
        "schema_version": "2.0.0",
        "steward": "ifau",
        "reg_meta_version": "5.1.0",
        "name": "bundle-test",
        "sources": [
            {
                "name": "lisa-2018",
                "register_variant": "scb/lisa/individer-15plus",
                "period": 2018,
                "bindings": [
                    {
                        "variable": "scb/lisa/kon",
                        "type": "categorical",
                        "display_name": "Kon",
                    },
                ],
            }
        ],
    }


def test_valid_project_returns_octet_stream_bytes(client):
    resp = client.post("/api/bundle", json=_valid_spec())
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/octet-stream")
    assert "attachment" in resp.headers["content-disposition"]
    assert len(resp.content) > 0
    # The amalgamated bundle is a Python file embedding the spec — a couple of
    # cheap shape assertions that it's the real artifact, not an empty/JSON body.
    text = resp.content.decode("utf-8")
    assert "def configure" in text


def test_bundle_is_deterministic(client):
    """Pure function of input: same spec → byte-identical bundle (§16)."""
    a = client.post("/api/bundle", json=_valid_spec()).content
    b = client.post("/api/bundle", json=_valid_spec()).content
    assert a == b


def test_bad_input_is_422(client):
    """A spec that passes the Pydantic MODEL (so it isn't a framework 422) but
    FAILS the build gate is bad INPUT → 422. A ``reg_monabundle.column_options``
    key referencing no binding FQID is an orphan-key build-gate failure
    (``validate_project_data`` raises ``ValueError``)."""
    spec = _valid_spec()
    spec["reg_monabundle"] = {
        "column_options": {"scb/lisa/notbound": {"suppress_k": 5}}
    }
    resp = client.post("/api/bundle", json=spec)
    assert resp.status_code == 422


def test_structurally_malformed_body_is_422(client):
    """The body is typed ``ProjectData``, so a structurally malformed body is a
    framework 422 before the handler runs (the ``/order`` + ``/bundle`` typed-body
    path, distinct from ``/validate``'s raw-dict diagnostic)."""
    resp = client.post("/api/bundle", json={"not": "a project"})
    assert resp.status_code == 422


def test_concurrent_bundle_builds_do_not_collide(unthrottled_client):
    """Each build uses its own ``TemporaryDirectory``; concurrent builds must all
    succeed (no shared-path collision) and — being a pure function of input —
    return identical bytes."""
    spec = _valid_spec()
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(
            pool.map(
                lambda _: unthrottled_client.post("/api/bundle", json=spec),
                range(40),
            )
        )
    codes = [r.status_code for r in results]
    assert all(c == 200 for c in codes), f"concurrent build failures: {codes}"
    bodies = {r.content for r in results}
    assert len(bodies) == 1, "deterministic build produced differing bytes"


def test_tempfile_cleanup(client):
    """The handler builds into a ``TemporaryDirectory`` context that removes the
    file on exit — assert no ``reg_webapp_bundle_*`` tempdir leaks after a build
    (the prefix the handler uses)."""
    import tempfile
    from pathlib import Path

    resp = client.post("/api/bundle", json=_valid_spec())
    assert resp.status_code == 200
    leaked = list(Path(tempfile.gettempdir()).glob("reg_webapp_bundle_*"))
    assert not leaked, f"leaked bundle tempdirs: {leaked}"
