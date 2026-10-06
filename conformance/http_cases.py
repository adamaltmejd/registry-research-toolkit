"""Readable request/expected cases at the HTTP boundary."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

from fastapi.testclient import TestClient
from reg_webapp.app import create_app

import reg_webapp

CASES = Path(__file__).parent / "cases"


def select_json(value, path):
    """JSON-pointer projection; '*' collects a list in response order."""
    parts = path.strip("/").split("/") if path != "/" else []

    def walk(current, remaining):
        if not remaining:
            return current
        head, *tail = remaining
        if head == "*":
            return [walk(item, tail) for item in current]
        return walk(
            current[int(head)] if isinstance(current, list) else current[head], tail
        )

    return walk(value, parts)


def case_artifact(request, tmp_path, monkeypatch):
    """Build a case's readable source and point the app at it; return its path."""
    from reader_artifacts import FIXTURE_IMPORT_DATE, build_reader_artifact

    kind = request.get("kind", "steward")
    fixture = request.get("fixture", "compiled")
    source = fixture if fixture.startswith("reader") else CASES / "fixtures" / fixture
    path = build_reader_artifact(
        tmp_path / "artifact",
        source,
        kind,
        identity_overrides={"import_date": FIXTURE_IMPORT_DATE},
    )
    monkeypatch.setenv("REG_META_DB", str(path.parent))
    monkeypatch.setenv(
        "REG_WEBAPP_STEWARD", "swecov" if kind == "steward" else "global"
    )
    return path


def assert_http_case(case, tmp_path, monkeypatch):
    request = json.loads((case / "request.json").read_text())
    expected = json.loads((case / "expected.json").read_text())
    kind = request.get("kind", "steward")
    path = case_artifact(request, tmp_path, monkeypatch)
    if "golden_config" in request:
        # Pins are loaded at import, so exercise packaged files in a fresh runtime.
        runtime = tmp_path / "runtime"
        package = runtime / "reg_webapp"
        shutil.copytree(
            Path(reg_webapp.__file__).parent,
            package,
            ignore=shutil.ignore_patterns("__pycache__"),
        )
        shutil.copyfile(case / request["golden_config"], package / "search_golden.toml")
        completed = subprocess.run(
            [
                sys.executable,
                "-c",
                "import json, sys; from http_cases import run_http_requests; "
                "print(json.dumps(run_http_requests(json.load(sys.stdin))))",
            ],
            input=json.dumps(request["requests"]),
            text=True,
            capture_output=True,
            check=False,
            env={
                "REG_META_DB": str(path.parent),
                "REG_WEBAPP_STEWARD": "swecov" if kind == "steward" else "global",
                "PYTHONPATH": os.pathsep.join((str(runtime), str(CASES.parent))),
                "REG_WEBAPP_STEWARDS_DIR": str(
                    CASES.parents[1] / "reg_webapp/stewards"
                ),
            },
        )
        assert completed.returncode == 0, completed.stderr
        responses = json.loads(completed.stdout)
    else:
        responses = run_http_requests(request["requests"])
    for response, oracle in zip(responses, expected, strict=True):
        assert response["status"] == oracle["status"]
        if "location" in oracle:
            assert response["location"] == oracle["location"]
        projection = {
            pointer: select_json(response["body"], pointer)
            for pointer in oracle.get("json", {})
        }
        assert projection == oracle.get("json", {}), (
            projection,
            oracle.get("json", {}),
        )


def request_body(step):
    """A step's body: `body` is sent as JSON; `content` is a raw string sent
    verbatim (as application/json), for bytes a JSON value cannot spell, such as
    a duplicate key."""
    if "content" in step:
        return {
            "content": step["content"].encode(),
            "headers": {"content-type": "application/json"},
        }
    return {"json": step.get("body")}


def run_http_requests(steps):
    """Exercise app responses, including fail-fast packaged configuration errors."""
    responses = []
    with TestClient(
        create_app(rate_limit_per_minute=1000), raise_server_exceptions=False
    ) as client:
        for step in steps:
            params = dict(step.get("query", {}))
            if "cursor_from" in step:
                idx, pointer = step["cursor_from"]
                params["cursor"] = select_json(responses[idx]["body"], pointer)
                assert params["cursor"] is not None
            response = client.request(
                step.get("method", "GET"),
                step["path"],
                params=params,
                **request_body(step),
                follow_redirects=False,
            )
            body = (
                response.json()
                if response.headers.get("content-type", "").startswith(
                    "application/json"
                )
                else response.text
                if response.content
                else None
            )
            responses.append(
                {
                    "status": response.status_code,
                    "location": response.headers.get("location"),
                    "body": body,
                }
            )
    return responses
