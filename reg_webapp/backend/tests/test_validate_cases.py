"""Project → reference semantics and exact compiled source-variant membership."""

from __future__ import annotations

import pytest
from http_cases import CASES, assert_http_case


@pytest.mark.parametrize(
    "case", sorted((CASES / "validate").iterdir()), ids=lambda p: p.name
)
def test_validation_contract(case, tmp_path, monkeypatch):
    assert_http_case(case, tmp_path, monkeypatch)
