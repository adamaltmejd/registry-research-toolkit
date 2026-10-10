"""Source-built regressions for sampled public contracts, with authored order oracles."""

from __future__ import annotations

import json

import pytest
from artifact_requests import assert_sampled_agreement, server_client
from reader_artifacts import CASES, FIXTURE_IMPORT_DATE, build_reader_artifact


def _without_nulls(value):
    """The oracle spells an absent coordinate by omission."""
    if isinstance(value, dict):
        return {k: _without_nulls(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [_without_nulls(item) for item in value]
    return value


@pytest.mark.parametrize(
    "case", sorted((CASES / "artifact_sample").iterdir()), ids=lambda p: p.name
)
def test_sampled_order_and_search_contracts(case, tmp_path, request):
    spec = json.loads((case / "request.json").read_text())
    expected = json.loads((case / "expected.json").read_text())
    fixture = spec["fixture"]
    if fixture.startswith("artifact_sample/"):
        fixture = CASES / fixture
    path = build_reader_artifact(
        tmp_path / "artifact",
        fixture,
        spec["artifact"],
        identity_overrides={"import_date": FIXTURE_IMPORT_DATE},
    )
    server = server_client(request, path.parent)
    if absent := expected.get("absent_from_first_search_page"):
        response = server.get(
            "/api/search", params={"q": "Value", "type": "variable", "limit": 100}
        )
        assert response.status_code == 200
        page = response.json()["data"]
        assert page["next_cursor"] is not None
        assert all(hit.get("fqid") != absent for hit in page["items"])
    output = _without_nulls(assert_sampled_agreement(path.parent, server))
    assert {key: output[key] for key in ("entries", "clips")} == {
        key: expected[key] for key in ("entries", "clips")
    }
