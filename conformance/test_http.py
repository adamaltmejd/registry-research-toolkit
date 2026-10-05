"""Readable HTTP response and validation cases through the deployed app."""

from __future__ import annotations

import pytest
from http_cases import CASES, assert_http_case


@pytest.mark.parametrize(
    "case",
    sorted(
        p.parent
        for surface in (
            "http_catalog",
            "http_context",
            "http_scope",
            "http_search",
            "validate",
        )
        for p in (CASES / surface).glob("*/request.json")
    ),
    ids=lambda p: f"{p.parent.name}/{p.name}",
)
def test_http_contract(case, tmp_path, monkeypatch):
    assert_http_case(case, tmp_path, monkeypatch)
