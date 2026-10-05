"""Compiled artifact → scoped search HTTP conformance."""

from __future__ import annotations

import pytest
from http_cases import CASES, assert_http_case


@pytest.mark.parametrize(
    "case", sorted((CASES / "search").iterdir()), ids=lambda p: p.name
)
def test_search_contract(case, tmp_path, monkeypatch):
    assert_http_case(case, tmp_path, monkeypatch)
