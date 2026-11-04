"""The MONA order file over HTTP, against hand-written case data (crates/DESIGN.md,
"From a logical order to physical files" and "MONA order file").

Each case under `cases/materialize/` holds a `request.json` (the project and a note
naming the regression it catches) and either `manifest.json`, the order file without
its generation ids and project hash, or `expected.json`, the findings that block it.
Every case runs on one steward artifact (the `steward` fixture in conftest.py): the
holdings fixture with the documented slot family, a long Part B header and a first-half
Parent amount (`prepare_materialize`). Write the expectation from the
project and the fixture source before serving it; a disagreement is a server bug or a
wrong expectation, never a golden to save. The MONA runner's refusal suite reads some
of these goldens as orders (`reg_mona_runner/tests`).
"""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING

import pytest
from holdings_artifacts import HOLDINGS_CASE
from http_cases import artifact_env
from reader_artifacts import CASES, cached_reader_artifact

if TYPE_CHECKING:
    from pathlib import Path

MATERIALIZE = CASES / "materialize"
HASH = re.compile(r"[0-9a-f]{64}")
ROUTE = "/api/project/materialize"


@pytest.mark.parametrize(
    "case",
    sorted(p for p in MATERIALIZE.iterdir() if (p / "request.json").exists()),
    ids=lambda p: p.name,
)
def test_order_file_or_located_finding(case: Path, steward) -> None:
    # Each case's note names the regression it catches. Also fails if the order file
    # stops naming the served artifact pair and project, or a blocked order stops
    # refusing with every located finding and no partial manifest.
    request = json.loads((case / "request.json").read_text())
    answer = steward.post(ROUTE, json=request["project"])
    oracle = case / "manifest.json"
    if oracle.exists():
        assert answer.status_code == 200, answer.text
        data = answer.json()["data"]
        provenance = data["provenance"]
        assert (
            provenance["catalog"].pop("generation")
            == answer.json()["meta"]["generation"]
        )
        for value in [
            provenance["holdings"].pop("generation"),
            provenance.pop("base_generation"),
            provenance["project"].pop("project_hash"),
        ]:
            assert HASH.fullmatch(value)
        assert data == json.loads(oracle.read_text())
        # Fails if steward `order` drops the holdings stage's warnings (a
        # `mapping_caveat`) that the order file carries. `order` names them; the
        # order file carries the bare finding.
        logical = steward.post("/api/project/order", json=request["project"])
        assert logical.status_code == 200, logical.text
        names = {"variable_name", "table_name", "successor_name"}
        assert [
            {key: value for key, value in finding.items() if key not in names}
            for finding in logical.json()["data"]["findings"]
        ] == data["findings"]
    else:
        assert answer.status_code == 422, answer.text
        error = answer.json()["error"]
        assert error["code"] == "order_blocked"
        assert "data" not in answer.json()
        findings = [
            {key: value for key, value in finding.items() if value is not None}
            for finding in error["fields"]["findings"]
        ]
        expected = json.loads((case / "expected.json").read_text())
        assert {"findings": findings} == expected
        # Also fails if steward `validate` stops running the holdings stage and passes
        # a project (an unheld variable, a contested cell) that materialize blocks
        # with the same located findings (#2194).
        validation = steward.post("/api/project/validate", json=request["project"])
        assert validation.status_code == 200, validation.text
        data = validation.json()["data"]
        assert not data["ok"]
        issues = [
            {key: value for key, value in issue.items() if value is not None}
            for issue in data["issues"]
        ]
        assert {"findings": issues} == expected
        # Fails if steward `order` stops running the holdings stage and returns a
        # logical order the SPA reads as "Ready to order" for a project materialize
        # blocks (an unheld variable is `mapping_missing`).
        logical = steward.post("/api/project/order", json=request["project"])
        assert logical.status_code == 422, logical.text
        error = logical.json()["error"]
        assert error["code"] == "order_blocked"
        findings = [
            {key: value for key, value in finding.items() if value is not None}
            for finding in error["fields"]["findings"]
        ]
        assert {"findings": findings} == expected


def test_table_row_key_shows_the_years_held(steward) -> None:
    # Fails if a steward table's row key keeps the reference years (#2217): the base
    # documents demo/keyed/id in Visits over 2017..2022; Visits places it in Id over
    # 2017 and IdAlt over 2018..2020 (materialize/fixture/keyed.toml).
    shown = steward.get("/api/catalog/demo/keyed/visits")
    assert shown.status_code == 200, shown.text
    held_years = {"start": "2017-01-01", "end": "2021-01-01"}
    data = shown.json()["data"]
    assert data["spans"] == [held_years]
    assert [(k["fqid"], k["period"]) for k in data["row_keys"]] == [
        ("demo/keyed/id", held_years)
    ]


def test_unnamed_project_is_a_logical_draft_but_no_order_file(steward) -> None:
    # Fails if a whitespace-only name reaches the order file's provenance, or if
    # refusing it also refuses the logical order of the same draft.
    project = json.loads((MATERIALIZE / "key-only/request.json").read_text())["project"]
    project["name"] = " \t "
    refused = steward.post(ROUTE, json=project)
    assert refused.status_code == 422, refused.text
    assert refused.json()["error"]["code"] == "project_unnamed"
    assert steward.post("/api/project/order", json=project).status_code == 200


def test_reference_only_server_has_no_order_file(http_servers) -> None:
    # Fails if a server without holdings invents physical files from catalog
    # availability (the dropped v3 global fallback) instead of refusing.
    project = json.loads((MATERIALIZE / "key-only/request.json").read_text())["project"]
    base = cached_reader_artifact(HOLDINGS_CASE / "base")
    refused = http_servers.client(artifact_env(base)).post(ROUTE, json=project)
    assert refused.status_code == 400, refused.text
    assert refused.json()["error"]["code"] == "holdings_unavailable"
