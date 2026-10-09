"""Readable HTTP response cases (the `api` corpus) against the Rust server."""

from __future__ import annotations

from http_cases import CASES, assert_http_case


def pytest_generate_tests(metafunc):
    # The corpus targets the Rust server, served only out of process. A
    # `startup_error` case starts the template itself and expects a refusal.
    cases = []
    if metafunc.config.getoption("--server-cmd") is not None:
        cases = [p.parent for p in (CASES / "api").glob("*/request.json")]
    metafunc.parametrize(
        "case", sorted(cases), ids=lambda p: f"{p.parent.name}/{p.name}"
    )


def test_http_contract(case, tmp_path, http_servers):
    assert_http_case(case, tmp_path, http_servers)
