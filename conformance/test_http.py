"""Readable HTTP response and validation cases through the deployed app."""

from __future__ import annotations

from http_cases import CASES, assert_http_case

SURFACES = ("http_scope",)


def pytest_generate_tests(metafunc):
    cases = [
        p.parent
        for surface in SURFACES
        for p in (CASES / surface).glob("*/request.json")
    ]
    if metafunc.config.getoption("--server-cmd") is not None:
        # The `api` corpus targets the new API, served only out of process. A
        # `startup_error` case starts the template itself and expects a refusal.
        cases += [p.parent for p in (CASES / "api").glob("*/request.json")]
    metafunc.parametrize(
        "case", sorted(cases), ids=lambda p: f"{p.parent.name}/{p.name}"
    )


def test_http_contract(case, tmp_path, monkeypatch, http_servers):
    # Only the `api` corpus targets the Rust server; the other surfaces' routes
    # are still FastAPI's, answered in process.
    servers = http_servers if case.parent.name == "api" else None
    assert_http_case(case, tmp_path, monkeypatch, servers)
