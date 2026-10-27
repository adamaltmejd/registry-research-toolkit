"""Readable HTTP response cases (the `api` corpus) against the Rust server."""

from __future__ import annotations

import pytest
from http_cases import CASES, assert_http_case, is_parked


def pytest_generate_tests(metafunc):
    # A `startup_error` case starts the template itself and expects a refusal.
    cases = [p.parent for p in (CASES / "api").glob("*/request.json")]
    metafunc.parametrize(
        "case", sorted(cases), ids=lambda p: f"{p.parent.name}/{p.name}"
    )


def test_http_contract(case, tmp_path, http_servers):
    if is_parked(case):
        pytest.skip("parked: conformance/PARKED.toml")
    assert_http_case(case, tmp_path, http_servers)
