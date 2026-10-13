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
from reg_meta_build.errors import RegMetaError

CASES = Path(__file__).parent / "cases"
READY_PATH = "/api/context"
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
        if "members" in oracle:
            assert_members(responses, oracle["members"])


def assert_members(responses, members):
    """A tie-order-free oracle across steps: `pointer` projected from each of
    `steps` in turn and concatenated holds no repeats and exactly the `equals`
    set."""
    rows = [
        row
        for index in members["steps"]
        for row in select_json(responses[index]["body"], members["pointer"])
    ]
    assert len(rows) == len(set(rows)), rows
    assert set(rows) == set(members["equals"]), rows


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
    `manifest` and `doc_meta` overrides are written to the private copies;
    `serve.db: "absent"` serves an empty directory instead of an artifact."""
    from reader_artifacts import FIXTURE_IMPORT_DATE, build_reader_artifact

    if request["serve"].get("db") == "absent":
        (tmp_path / "absent").mkdir()
        _assert_refused(
            servers, tmp_path / "absent", request["serve"]["catalog"], refusal
        )
        return
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
    _assert_refused(servers, path.parent, request["serve"]["catalog"], refusal)


def _assert_refused(servers, db, catalog, refusal):
    completed = subprocess.run(
        servers.argv(db, catalog, _free_port()),
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
        if "if_none_match" in step:
            # A conditional read with a literal validator: a stale or foreign tag.
            body["headers"] = body.get("headers", {}) | {
                "if-none-match": step["if_none_match"]
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
        expected = _artifact_identity(Path(env["REG_META_DB"]) / "reg_meta.db")
        observed = None
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
            ready, observed = _ready(process, client, deadline, expected, observed)
            if ready:
                return process, client
            client.close()
            _stop(process)
            if time.monotonic() >= deadline:
                break
        raise RuntimeError(
            f"server never answered {READY_PATH} as artifact {expected}"
            f" (last answer: {observed}): {shlex.join(argv)}\n"
            f"{log.read_text(errors='replace')[-4000:]}"
        )


def _artifact_identity(catalog_db):
    """The `(generation_id, import_date)` an artifact's server reports in
    `READY_PATH`. Both: `generation_id` does not cover the import date, and an
    `artifacts` entry's other generation differs only there."""
    with closing(sqlite3.connect(f"{catalog_db.as_uri()}?mode=ro", uri=True)) as conn:
        manifest = dict(
            conn.execute(
                "SELECT key, value FROM import_manifest"
                " WHERE key IN ('generation_id', 'import_date')"
            )
        )
    return manifest.get("generation_id"), manifest.get("import_date")


def _ready(process, client, deadline, expected, observed):
    """Poll `READY_PATH` until our server answers as the `expected` artifact
    (True), or it exits or the deadline passes (False); also the last identity
    an answer reported.

    The identity check is what keeps a case on its own artifact (#1529):
    `_free_port` only narrows the window in which another process (another xdist
    worker's server) can hold our port and answer this poll. A foreign answer is
    not accepted; our server then fails its bind and exits, and `start` retries
    on a fresh port. simplify: two artifacts that differ only in their docs
    database report the same identity; check the docs generation too if a
    docs-only twin ever collides."""
    while time.monotonic() < deadline:
        try:
            # `context` counts the catalog: slower than a static route on a
            # real-seed artifact's cold first read.
            response = client.get(READY_PATH, timeout=10)
            body = response.json()
            observed = (body["meta"]["generation"], body["data"]["import_date"])
        except httpx2.TransportError, ValueError, KeyError, TypeError:
            pass
        else:
            if observed == expected and process.poll() is None:
                return True, observed
        # Waiting on the process paces the poll and notices an early exit.
        with suppress(subprocess.TimeoutExpired):
            process.wait(timeout=0.05)
            return False, observed
    return False, observed


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
    """A loopback port held back for the server about to bind it.

    A port merely probed free can be drawn again by another xdist worker before
    our server binds it (#1529). A connection closed from the accepting side
    leaves the port in TIME_WAIT (about a minute on Linux), which the kernel
    skips when it picks a port for `bind(0)` but lets an `SO_REUSEADDR` bind
    take, as tokio's does. This only makes a collision rarer: a server that
    starts slower than the TIME_WAIT can still lose its port. `_ready`'s
    identity check is what keeps a foreign server out."""
    with socket.socket() as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        port = listener.getsockname()[1]
        with socket.create_connection(("127.0.0.1", port)):
            accepted, _ = listener.accept()
            # The accepting side closes first, so the TIME_WAIT is on `port`.
            accepted.close()
    return port
