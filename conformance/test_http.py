"""Readable HTTP response and validation cases through the deployed app."""

from __future__ import annotations

import json

from http_cases import CASES, assert_http_case

SURFACES = ("http_catalog", "http_context", "http_scope", "http_search", "validate")


def pytest_generate_tests(metafunc):
    cases = [
        p.parent
        for surface in SURFACES
        for p in (CASES / surface).glob("*/request.json")
    ]
    if metafunc.config.getoption("--server-cmd") is not None:
        # The `api` corpus targets the new API, served only out of process. A
        # `startup_error` case is a server that never listens, not a socket case.
        cases += [
            p.parent
            for p in (CASES / "api").glob("*/request.json")
            if "startup_error"
            not in json.loads(p.with_name("expected.json").read_text())
        ]
    metafunc.parametrize(
        "case", sorted(cases), ids=lambda p: f"{p.parent.name}/{p.name}"
    )


def test_http_contract(case, tmp_path, monkeypatch, http_servers):
    assert_http_case(case, tmp_path, monkeypatch, http_servers)
