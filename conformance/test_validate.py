"""Project adapters: the CLI and HTTP doors give the same answer for the same bytes.

Every `/api/project/validate` and `/api/project/order` step of the readable
`cases/validate` corpus is also run through `reg-meta validate` /
`reg-meta order` against the same artifact. The HTTP oracle (`test_http.py`)
pins the answers; this pins that both adapters of the shared `reg_meta` code
give them identically:

- 200: the CLI's stdout is the HTTP body byte for byte (`validation_json` or
  `OrderManifest.to_json`); `validate` exits 0 when `ok` and 17 otherwise,
  `order` exits 0.
- 400 (a malformed project document, refused by the shared
  `order.parse_project`): exit 10 with `error.code` `project_unreadable` and
  `error.message` equal to the HTTP `detail`.
"""

from __future__ import annotations

import json
import re
from contextlib import contextmanager
from typing import TYPE_CHECKING

import pytest
from fastapi.testclient import TestClient
from http_cases import CASES, case_artifact, raw_body, request_body
from reg_meta.cli import run
from reg_meta.db import open_db
from reg_meta.errors import EXIT_CONFIG, EXIT_NO_MATCH, EXIT_SUCCESS
from reg_meta.semantic import validate_project
from reg_webapp.app import create_app

if TYPE_CHECKING:
    from pathlib import Path

_COMMANDS = {"/api/project/validate": "validate", "/api/project/order": "order"}


@pytest.mark.parametrize(
    "case",
    sorted(p.parent for p in (CASES / "validate").glob("*/request.json")),
    ids=lambda p: p.name,
)
def test_cli_and_http_project_adapters_agree(
    case: Path, tmp_path: Path, monkeypatch, capsys
) -> None:
    request = json.loads((case / "request.json").read_text())
    path = case_artifact(request, monkeypatch)
    steps = [s for s in request["requests"] if s["path"] in _COMMANDS]
    assert steps
    project = tmp_path / "project.json"
    with TestClient(create_app(rate_limit_per_minute=1000)) as client:
        for step in steps:
            response = client.post(step["path"], **request_body(step))
            raw = raw_body(step)
            project.write_bytes(
                raw if raw is not None else json.dumps(step["body"]).encode()
            )
            command = _COMMANDS[step["path"]]
            code = run(["--db", str(path.parent), command, str(project)])
            out = capsys.readouterr().out
            if response.status_code == 200:
                assert out == response.text
                ok = command == "order" or response.json()["ok"]
                assert code == (EXIT_SUCCESS if ok else EXIT_NO_MATCH)
                continue
            assert response.status_code == 400
            error = json.loads(out)["error"]
            assert error["message"] == response.json()["detail"]
            assert (code, error["code"]) == (EXIT_CONFIG, "project_unreadable")


# Code membership: value-set members, their codes, classification codes and the
# conformance report's nonconforming codes.
_CODE_LIST_TABLES = frozenset(
    {
        "value_set_member",
        "value_code",
        "classification_code",
        "classification_conformance_code",
    }
)


def test_validation_reads_state_metadata_never_code_lists(monkeypatch):
    """Project validation answers identity and state-window questions, so it
    never reads code membership (reg_meta/DESIGN.md → Project semantic
    validation): a geography variable's shared code list would otherwise be
    loaded per state to answer a question about one period. The statements are
    captured from the public door on the test's own connection, over every
    case of the readable semantic fixture (which carries value sets and a
    classification)."""
    cases = [
        json.loads(p.read_text())
        for p in sorted((CASES / "validate").glob("*/request.json"))
    ]
    cases = [c for c in cases if c.get("fixture") == "semantic"]
    path = case_artifact(cases[0], monkeypatch)
    statements: list[str] = []

    @contextmanager
    def traced():
        conn = open_db(path)
        conn.set_trace_callback(statements.append)
        try:
            yield conn
        finally:
            conn.close()

    for case in cases:
        for step in case["requests"]:
            if step["path"] == "/api/project/validate" and "body" in step:
                validate_project(step["body"], traced)
    tables = {
        table
        for sql in statements
        for table in re.findall(r"\b(?:FROM|JOIN)\s+(\w+)", sql, re.IGNORECASE)
    }
    assert "variable_state" in tables
    assert not tables & _CODE_LIST_TABLES, sorted(tables & _CODE_LIST_TABLES)


def test_cli_validate_fails_fast_on_a_missing_catalog(tmp_path, capsys):
    """The catalog is required before the project is read, so a missing catalog
    is reported as such (exit 10, `db_not_found`) even for a project the CLI
    could not have validated anyway."""
    code = run(
        ["--db", str(tmp_path / "absent"), "validate", str(tmp_path / "nope.json")]
    )
    error = json.loads(capsys.readouterr().out)["error"]
    assert (code, error["code"]) == (EXIT_CONFIG, "db_not_found")
