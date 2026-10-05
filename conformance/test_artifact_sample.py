"""Source-built regressions for sampled public contracts, with authored order oracles."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from artifact_requests import sample_project
from fastapi.testclient import TestClient
from reader_artifacts import CASES, FIXTURE_IMPORT_DATE, build_reader_artifact
from reg_meta.cli import run
from reg_meta.db import open_db
from reg_meta.order import materialize_order, project_from_raw
from reg_webapp.app import create_app
from test_artifact import assert_sampled_agreement


@pytest.mark.parametrize(
    "case", sorted((CASES / "artifact_sample").iterdir()), ids=lambda p: p.name
)
def test_sampled_order_and_search_contracts(case, tmp_path, monkeypatch, capsys):
    request = json.loads((case / "request.json").read_text())
    expected = json.loads((case / "expected.json").read_text())
    fixture = request["fixture"]
    if fixture.startswith("artifact_sample/"):
        fixture = CASES / fixture
    path = build_reader_artifact(
        tmp_path / "artifact",
        fixture,
        request["artifact"],
        identity_overrides={"import_date": FIXTURE_IMPORT_DATE},
    )
    with open_db(path) as conn:
        result = materialize_order(project_from_raw(sample_project(conn)), conn)
    assert result.manifest is not None
    output = result.manifest.model_dump(mode="json", exclude_none=True)
    assert {key: output[key] for key in ("entries", "clips")} == {
        key: expected[key] for key in ("entries", "clips")
    }
    monkeypatch.setenv("REG_META_DB", str(path.parent))
    monkeypatch.setenv(
        "REG_WEBAPP_STEWARD", "global" if request["artifact"] == "catalog" else "swecov"
    )
    monkeypatch.setenv(
        "REG_WEBAPP_STEWARDS_DIR",
        str(Path(__file__).resolve().parents[1] / "reg_webapp/stewards"),
    )
    with TestClient(create_app(rate_limit_per_minute=1000)) as client:
        if absent := expected.get("absent_from_first_search_page"):
            response = client.get(
                "/api/search", params={"q": "Value", "type": "variable", "limit": 100}
            )
            assert response.status_code == 200
            group = response.json()["groups"][0]
            assert group["has_more"]
            assert all(hit.get("fqid") != absent for hit in group["results"])
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
        assert_sampled_agreement(path.parent, client, tmp_path, capsys)
