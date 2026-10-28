"""Logical orders and located findings over HTTP, against independently authored
case data (conformance/README.md).

A case that orders commits its `order.json`: the logical order's `data` without
`catalog.generation_id` and `project.project_hash`. A blocked case has none, and
its `expected.json` findings are the oracle.

Recipe for a new case (a content decision, reviewed in the diff; never rewritten
to make a run pass): first write down, from the project and the fixture source
alone, the entries, clips, row keys and findings the order must hold, or the
findings that block it. Then serve the case's artifact
(`cached_case_artifact({"fixture": request["fixture"]})`) with `reg-meta serve`,
`POST /api/project/order` with `request["project"]` and compare. A disagreement
is a server bug or a wrong expectation, never a golden to save.
"""

from __future__ import annotations

import json
import re
import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from http_cases import CASES, artifact_env, cached_case_artifact

if TYPE_CHECKING:
    from pathlib import Path

HASH = re.compile(r"[0-9a-f]{64}")


def generation(db_path: Path) -> str:
    with closing(sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)) as db:
        (value,) = db.execute(
            "SELECT value FROM meta WHERE key = 'generation_id'"
        ).fetchone()
    return value


@pytest.mark.parametrize(
    "case", sorted((CASES / "order").iterdir()), ids=lambda p: p.name
)
def test_logical_order_or_located_finding(case: Path, http_servers) -> None:
    # Fails when the logical order drifts from the written-down `order.json`
    # (entries, clips, row keys, findings, their order or messages), when it does
    # not name the served generation, when a blocked order stops refusing with
    # these located findings, or when `validate` disagrees with `order`.
    # The classification ambiguity pair fails if a pin filters unresolved-version
    # windows away before the blocking ambiguity check.
    request = json.loads((case / "request.json").read_text())
    path = cached_case_artifact({"fixture": request["fixture"]})
    client = http_servers.client(artifact_env(path))
    answer = client.post("/api/project/order", json=request["project"])
    validation = client.post("/api/project/validate", json=request["project"])
    assert validation.status_code == 200
    validation = validation.json()["data"]
    oracle = case / "order.json"
    if oracle.exists():
        assert answer.status_code == 200
        data = answer.json()["data"]
        assert data["catalog"].pop("generation_id") == generation(path)
        assert HASH.fullmatch(data["project"].pop("project_hash"))
        assert data == json.loads(oracle.read_text())
        assert validation["ok"]
        assert not [i for i in validation["issues"] if i["level"] == "error"]
    else:
        assert answer.status_code == 422
        error = answer.json()["error"]
        assert error["code"] == "order_blocked"
        # The oracle spells an absent coordinate by omission.
        findings = [
            {key: value for key, value in finding.items() if value is not None}
            for finding in error["fields"]["findings"]
        ]
        expected = json.loads((case / "expected.json").read_text())
        assert {"findings": findings} == expected
        assert not validation["ok"]
        blocking = [f for f in error["fields"]["findings"] if f["level"] == "error"]
        assert [i for i in validation["issues"] if i["level"] == "error"] == blocking
