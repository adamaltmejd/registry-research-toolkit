"""Project validation: the CLI and HTTP adapters emit byte-identical findings.

Every `/api/project/validate` step of the readable `cases/validate` corpus is
also run through `reg-meta validate` against the same artifact. The HTTP oracle
(`test_http.py`) pins the findings; this pins that both adapters of the shared
`reg_meta.semantic.validate_project` serialize them to the same bytes, and that
the CLI's exit code follows the findings' `ok`.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from fastapi.testclient import TestClient
from http_cases import CASES, case_artifact
from reg_meta.cli import run
from reg_meta.errors import EXIT_NO_MATCH, EXIT_SUCCESS
from reg_webapp.app import create_app

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize(
    "case",
    sorted(p.parent for p in (CASES / "validate").glob("*/request.json")),
    ids=lambda p: p.name,
)
def test_cli_and_http_validation_bytes_agree(
    case: Path, tmp_path: Path, monkeypatch, capsys
) -> None:
    request = json.loads((case / "request.json").read_text())
    path = case_artifact(request, tmp_path, monkeypatch)
    steps = [s for s in request["requests"] if s["path"] == "/api/project/validate"]
    assert steps
    project = tmp_path / "project.json"
    with TestClient(create_app(rate_limit_per_minute=1000)) as client:
        for step in steps:
            response = client.post(step["path"], json=step["body"])
            assert response.status_code == 200
            project.write_text(json.dumps(step["body"]))
            code = run(["--db", str(path.parent), "validate", str(project)])
            assert capsys.readouterr().out == response.text
            assert code == (EXIT_SUCCESS if response.json()["ok"] else EXIT_NO_MATCH)
