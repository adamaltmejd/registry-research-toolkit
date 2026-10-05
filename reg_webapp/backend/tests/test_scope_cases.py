"""Browse overrides and project scope rejection at the HTTP boundary."""

from __future__ import annotations

import pytest
from http_cases import CASES, assert_http_case


@pytest.mark.parametrize(
    "case", sorted((CASES / "scope").iterdir()), ids=lambda p: p.name
)
def test_scope_contract(case, tmp_path, monkeypatch):
    assert_http_case(case, tmp_path, monkeypatch)
