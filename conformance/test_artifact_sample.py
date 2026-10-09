"""Source-built regressions for sampled public contracts, with authored order oracles."""

from __future__ import annotations

import json

import pytest
from artifact_requests import assert_sampled_agreement, sample_project, server_client
from reader_artifacts import CASES, FIXTURE_IMPORT_DATE, build_reader_artifact
from reg_meta.cli import run
from reg_meta.db import open_db
from reg_meta.order import materialize_order, project_from_raw


@pytest.mark.parametrize(
    "case", sorted((CASES / "artifact_sample").iterdir()), ids=lambda p: p.name
)
def test_sampled_order_and_search_contracts(
    case, tmp_path, monkeypatch, capsys, request
):
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
    with open_db(path) as conn:
        result = materialize_order(project_from_raw(sample_project(conn)), conn)
    assert result.manifest is not None
    output = result.manifest.model_dump(mode="json", exclude_none=True)
    assert {key: output[key] for key in ("entries", "clips")} == {
        key: expected[key] for key in ("entries", "clips")
    }
    server = server_client(request, path.parent)
    if absent := expected.get("absent_from_first_search_page"):
        response = server.get(
            "/api/search", params={"q": "Value", "type": "variable", "limit": 100}
        )
        assert response.status_code == 200
        page = response.json()["data"]
        assert page["next_cursor"] is not None
        assert all(hit.get("fqid") != absent for hit in page["items"])
        assert (
            run(
                [
                    "--db",
                    str(path.parent),
                    "--format",
                    "json",
                    "search",
                    "--query",
                    "Value",
                    "--type",
                    "variable",
                    "--no-fold",
                    "--limit",
                    "100",
                ]
            )
            == 0
        )
        page = json.loads(capsys.readouterr().out)
        assert page["has_more"]
        assert all(hit.get("fqid") != absent for hit in page["results"])
    assert_sampled_agreement(path.parent, server, tmp_path, capsys)
