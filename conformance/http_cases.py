"""Readable request/expected cases at the HTTP boundary.

Cases run with `--server-cmd` over a real socket against one server process per
cached artifact (`ServerPool`).
"""

from __future__ import annotations

import json
import os
import shlex
import signal
import socket
import sqlite3
import subprocess
import time
import tomllib
from contextlib import closing, suppress
from pathlib import Path

import httpx2
from reg_meta.errors import RegMetaError

CASES = Path(__file__).parent / "cases"
READY_PATH = "/openapi.json"
SERVER_READY_SECONDS = 120
EXIT_STATUS = {
    code["code"]: code.get("exit")
    for code in tomllib.loads((CASES.parent / "api/errors.toml").read_text())["code"]
}


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


def fixture_source(request):
    """A case's fixture: a source directory under `cases/fixtures`, else a name
    `reader_artifacts` resolves (`reader`, `reader/<case>`, a builder holdings case)."""
    fixture = request.get("fixture", "compiled")
    local = CASES / "fixtures" / fixture
    return local if local.is_dir() else fixture


def docs_source(request):
    """The readable docs source a case's `docs` key names, or None (no docs DB)."""
    return CASES / "fixtures" / request["docs"] if "docs" in request else None


def cached_case_artifact(request, case=None, identity=None):
    """A case's cached, read-only artifact. A `search_pins` key names a pins file
    in the `case` directory that the build stores; fixtures have no pins
    otherwise. A `docs` key names a docs source whose database is built beside
    the catalog. `identity` overrides apply over the fixed import date (an
    `artifacts` entry's other generation)."""
    from reader_artifacts import FIXTURE_IMPORT_DATE, cached_reader_artifact

    return cached_reader_artifact(
        fixture_source(request),
        request.get("kind", "steward"),
        identity_overrides={"import_date": FIXTURE_IMPORT_DATE, **(identity or {})},
        search_pins=case / request["search_pins"] if "search_pins" in request else None,
        docs=docs_source(request),
    )


def assert_http_case(case, tmp_path, servers):
    """Run a case against `servers` (a `ServerPool`).

    An expected `build_error` (`code`, `message`) is the artifact build's refusal;
    the case sends no requests."""
    request = json.loads((case / "request.json").read_text())
    expected = json.loads((case / "expected.json").read_text())
    if "startup_error" in expected:
        assert_startup_refusal(request, expected["startup_error"], servers, tmp_path)
        return
    if "build_error" in expected:
        try:
            cached_case_artifact(request, case)
        except RegMetaError as exc:
            assert {"code": exc.code, "message": exc.message} == expected["build_error"]
            return
        raise AssertionError("the artifact build was expected to fail")
    responses = run_http_requests(
        request["requests"], case_clients(request, case, servers)
    )
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


def case_clients(request, case, servers):
    """The `servers` clients of a case: its artifact's under `None`, each
    `artifacts` entry's under its name. An entry overrides the identity
    (`identity`), the docs source (`docs`) or both."""
    kind = request.get("kind", "steward")
    clients = {
        name: servers.client(
            artifact_env(
                cached_case_artifact(
                    {**request, **({"docs": spec["docs"]} if "docs" in spec else {})},
                    case,
                    spec.get("identity"),
                ),
                kind,
            )
        )
        for name, spec in request.get("artifacts", {}).items()
    }
    clients[None] = servers.client(
        artifact_env(cached_case_artifact(request, case), kind)
    )
    return clients


def assert_startup_refusal(request, refusal, servers, tmp_path):
    """The server refuses the case's artifact: it prints the error document on
    stderr and exits with the code's `exit` status in `api/errors.toml`. The
    `manifest` and `doc_meta` overrides are written to the private copies."""
    from reader_artifacts import FIXTURE_IMPORT_DATE, build_reader_artifact

    path = build_reader_artifact(
        tmp_path / "catalog",
        fixture_source(request),
        request.get("kind", "steward"),
        identity_overrides={"import_date": FIXTURE_IMPORT_DATE},
        docs=docs_source(request),
    )
    overrides = [("reg_meta.db", "import_manifest", request.get("manifest", {}))]
    if "doc_meta" in request:
        overrides.append(("reg_meta_docs.db", "doc_meta", request["doc_meta"]))
    for filename, table, values in overrides:
        with closing(sqlite3.connect(path.parent / filename)) as conn, conn:
            conn.executemany(
                f"INSERT OR REPLACE INTO {table} VALUES (?, ?)", values.items()
            )
    completed = subprocess.run(
        servers.argv(path.parent, request["serve"]["catalog"], _free_port()),
        cwd=CASES.parents[1],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    document = json.loads(completed.stderr.strip().splitlines()[-1])
    assert document["code"] == refusal["code"], completed.stderr
    assert completed.returncode == EXIT_STATUS[refusal["code"]]


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


def run_http_requests(steps, clients):
    """Send the steps through `clients` (the case artifact's client under `None`,
    each `artifacts` entry's under its name)."""
    responses = []
    for step in steps:
        client = clients[step.get("artifact")]
        params = dict(step.get("query", {}))
        if "cursor_from" in step:
            idx, pointer = step["cursor_from"]
            params["cursor"] = select_json(responses[idx]["body"], pointer)
            assert params["cursor"] is not None
        # An identifier only an earlier response carries (a storage id).
        for name, (idx, pointer) in step.get("query_from", {}).items():
            params[name] = select_json(responses[idx]["body"], pointer)
        body = request_body(step)
        if "etag_from" in step:
            # A conditional read that revalidates an earlier step's response.
            body["headers"] = body.get("headers", {}) | {
                "if-none-match": responses[step["etag_from"]]["headers"]["etag"]
            }
        response = client.request(
            step.get("method", "GET"),
            step["path"],
            params=params,
            **body,
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
    directory), `{catalog}` (`global` or the artifact's steward) and `{port}` are
    substituted in each word. The process also gets
    `artifact_env`, runs in its own process group (so stopping it also stops
    what `uv run` launched) and logs to `log_dir`."""

    def __init__(self, template, log_dir):
        self.template = shlex.split(template)
        self.log_dir = log_dir
        self.servers = {}

    def argv(self, db, catalog, port):
        return [
            word.replace("{db}", str(db))
            .replace("{catalog}", catalog)
            .replace("{port}", str(port))
            for word in self.template
        ]

    def client(self, env):
        db = env["REG_META_DB"]
        if db not in self.servers:
            # A failed start is remembered, so later cases on the artifact fail
            # at once instead of waiting out the readiness deadline again.
            try:
                self.servers[db] = self.start(env)
            except RuntimeError as exc:
                self.servers[db] = exc
        if isinstance(self.servers[db], RuntimeError):
            raise self.servers[db]
        return self.servers[db][1]

    def close(self):
        while self.servers:
            _, server = self.servers.popitem()
            if not isinstance(server, RuntimeError):
                server[1].close()
                _stop(server[0])

    def start(self, env):
        log = self.log_dir / f"server-{len(self.servers)}.log"
        deadline = time.monotonic() + SERVER_READY_SECONDS
        # A server that exits before answering may have lost its port to another
        # process between `_free_port` and its bind: retry on a fresh port.
        for _attempt in range(3):
            port = _free_port()
            argv = self.argv(env["REG_META_DB"], env["REG_WEBAPP_STEWARD"], port)
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
