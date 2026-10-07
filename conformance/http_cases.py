"""Readable request/expected cases at the HTTP boundary.

Cases run in-process through `TestClient` by default, or with `--server-cmd`
over a real socket against one server process per cached artifact
(`ServerPool`). Both clients are `httpx2.Client`s, so one request path serves
both transports.
"""

from __future__ import annotations

import json
import os
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import time
from contextlib import suppress
from pathlib import Path

import httpx2
from fastapi.testclient import TestClient
from reg_webapp.app import create_app

import reg_webapp

CASES = Path(__file__).parent / "cases"
READY_PATH = "/openapi.json"
SERVER_READY_SECONDS = 120


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


def artifact_env(path, kind):
    """The environment the app needs to serve the artifact at `path`."""
    return {
        "REG_META_DB": str(path.parent),
        "REG_WEBAPP_STEWARD": "swecov" if kind == "steward" else "global",
        "REG_WEBAPP_STEWARDS_DIR": str(CASES.parents[1] / "reg_webapp/stewards"),
    }


def case_artifact(request, monkeypatch):
    """Point the app at a case's cached, read-only artifact; return its path."""
    from reader_artifacts import FIXTURE_IMPORT_DATE, cached_reader_artifact

    kind = request.get("kind", "steward")
    fixture = request.get("fixture", "compiled")
    source = fixture if fixture.startswith("reader") else CASES / "fixtures" / fixture
    path = cached_reader_artifact(
        source,
        kind,
        identity_overrides={"import_date": FIXTURE_IMPORT_DATE},
    )
    for name, value in artifact_env(path, kind).items():
        monkeypatch.setenv(name, value)
    return path


def assert_http_case(case, tmp_path, monkeypatch, servers=None):
    """Run a case in-process, or against `servers` (a `ServerPool`) when given."""
    request = json.loads((case / "request.json").read_text())
    expected = json.loads((case / "expected.json").read_text())
    kind = request.get("kind", "steward")
    path = case_artifact(request, monkeypatch)
    if "golden_config" in request:
        # Pins are loaded at import, so exercise packaged files in a fresh runtime,
        # in-process even with --server-cmd. No raw bytes cross the pipe.
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
                "print(json.dumps([r | {'content': None} "
                "for r in run_http_requests(json.load(sys.stdin))]))",
            ],
            input=json.dumps(request["requests"]),
            text=True,
            capture_output=True,
            check=False,
            env=artifact_env(path, kind)
            | {"PYTHONPATH": os.pathsep.join((str(runtime), str(CASES.parent)))},
        )
        assert completed.returncode == 0, completed.stderr
        responses = json.loads(completed.stdout)
    elif servers is not None:
        client = servers.client(artifact_env(path, kind))
        responses = run_http_requests(request["requests"], client)
    else:
        responses = run_http_requests(request["requests"])
    for response, oracle in zip(responses, expected, strict=True):
        assert response["status"] == oracle["status"]
        if "location" in oracle:
            assert response["location"] == oracle["location"]
        if "media_type" in oracle:
            assert response["media_type"] == oracle["media_type"]
        for name, value in oracle.get("headers", {}).items():
            assert response["headers"].get(name) == value, name
        if "bytes" in oracle:
            assert response["content"] == (case / oracle["bytes"]).read_bytes()
        projection = {
            pointer: select_json(response["body"], pointer)
            for pointer in oracle.get("json", {})
        }
        assert projection == oracle.get("json", {}), (
            projection,
            oracle.get("json", {}),
        )


def raw_body(step):
    """A step's raw request bytes, or None when it sends `body` as JSON.

    `content` is a string sent verbatim, encoded with the step's `encoding`
    (default UTF-8), for documents a JSON value cannot spell: a duplicate key,
    a byte-order mark, another encoding. `nested_arrays: N` is a root object
    whose one value nests N arrays deep, too large to spell literally."""
    if "nested_arrays" in step:
        depth = step["nested_arrays"]
        return b'{"nested": ' + b"[" * depth + b"]" * depth + b"}"
    if "content" in step:
        return step["content"].encode(step.get("encoding", "utf-8"))
    return None


def request_body(step):
    """The `httpx2.Client.request` keyword arguments for a step's body."""
    raw = raw_body(step)
    if raw is not None:
        return {"content": raw, "headers": {"content-type": "application/json"}}
    return {"json": step.get("body")}


def run_http_requests(steps, client=None):
    """Send the steps through `client`, or the app in this process (which
    exercises fail-fast packaged configuration errors)."""
    if client is None:
        with TestClient(
            create_app(rate_limit_per_minute=1000), raise_server_exceptions=False
        ) as app_client:
            return run_http_requests(steps, app_client)
    responses = []
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
        content_type = response.headers.get("content-type", "")
        body = (
            response.json()
            if content_type.startswith("application/json")
            else response.text
            if response.content
            else None
        )
        responses.append(
            {
                "status": response.status_code,
                "location": response.headers.get("location"),
                "media_type": content_type.split(";")[0].strip(),
                "headers": dict(response.headers),  # names lower-cased
                "content": response.content,
                "body": body,
            }
        )
    return responses


class ServerPool:
    """One server process per artifact, started from a `--server-cmd` template.

    The template is split like a shell command line, and `{db}` (the artifact
    directory) and `{port}` are substituted in each word. The process also gets
    `artifact_env`, runs in its own process group (so stopping it also stops
    what `uv run` launched) and logs to `log_dir`."""

    def __init__(self, template, log_dir):
        self._template = shlex.split(template)
        self._log_dir = log_dir
        self._servers = {}

    def client(self, env):
        db = env["REG_META_DB"]
        if db not in self._servers:
            # A failed start is remembered, so later cases on the artifact fail
            # at once instead of waiting out the readiness deadline again.
            try:
                self._servers[db] = self._start(env)
            except RuntimeError as exc:
                self._servers[db] = exc
        if isinstance(self._servers[db], RuntimeError):
            raise self._servers[db]
        return self._servers[db][1]

    def close(self):
        while self._servers:
            _, server = self._servers.popitem()
            if not isinstance(server, RuntimeError):
                server[1].close()
                _stop(server[0])

    def _start(self, env):
        log = self._log_dir / f"server-{len(self._servers)}.log"
        deadline = time.monotonic() + SERVER_READY_SECONDS
        # A server that exits before answering may have lost its port to another
        # process between `_free_port` and its bind: retry on a fresh port.
        for _attempt in range(3):
            port = str(_free_port())
            argv = [
                word.replace("{db}", env["REG_META_DB"]).replace("{port}", port)
                for word in self._template
            ]
            with log.open("ab") as output:
                process = subprocess.Popen(
                    argv,
                    cwd=CASES.parents[1],
                    env=os.environ | env,
                    stdin=subprocess.DEVNULL,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
            # A connection per request: uvicorn closes one after an unhandled
            # exception's 500, and the next step must not inherit that reset.
            client = httpx2.Client(
                base_url=f"http://127.0.0.1:{port}",
                timeout=60,
                limits=httpx2.Limits(max_keepalive_connections=0),
            )
            if _ready(process, client, deadline):
                return process, client
            client.close()
            _stop(process)
            if time.monotonic() >= deadline:
                break
        raise RuntimeError(
            f"server never answered {READY_PATH}: {shlex.join(argv)}\n"
            f"{log.read_text(errors='replace')[-4000:]}"
        )


def _ready(process, client, deadline):
    """Poll `READY_PATH` until the server answers (True), or it exits or the
    deadline passes (False)."""
    while time.monotonic() < deadline:
        try:
            client.get(READY_PATH, timeout=1)
        except httpx2.TransportError:
            pass
        else:
            # simplify: a foreign listener that took the port before our bind
            # could answer before our process exits; hand the server a bound
            # socket instead if that ever bites.
            return process.poll() is None
        # Waiting on the process paces the poll and notices an early exit.
        with suppress(subprocess.TimeoutExpired):
            process.wait(timeout=0.05)
            return False
    return False


def _stop(process):
    with suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        process.wait()


def _free_port():
    """A loopback port the kernel just reported free."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]
