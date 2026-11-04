"""Readable request/expected cases at the HTTP boundary.

Cases run with `--server-cmd` over a real socket against one server process per
cached artifact (`ServerPool`).
"""

from __future__ import annotations

import os
import re
import shlex
import signal
import subprocess
import time
from contextlib import suppress

import httpx2
from reader_artifacts import REPO, cached_reader_artifact

READY_PATH = "/openapi.json"
# The line `reg-meta serve` prints on stderr once bound.
LISTENING = re.compile(rb"^listening on \S+:(\d+)\n", re.MULTILINE)
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


def artifact_env(path):
    """The environment the app needs to serve the artifact at `path`."""
    return {"REG_META_DB": str(path.parent)}


def fixture_source(request):
    """A case's fixture: a directory name under `cases/fixtures` (default `base`)."""
    return request.get("fixture", "base")


def cached_case_artifact(request):
    """A case's cached, read-only artifact, built by `reg-build` from its fixture."""
    return cached_reader_artifact(fixture_source(request))


def case_clients(request, servers):
    """The `servers` clients of a case: its artifact's under `None`, each
    `artifacts` entry's under its name. An entry may name its own `fixture`."""
    clients = {
        name: servers.client(artifact_env(cached_case_artifact({**request, **spec})))
        for name, spec in request.get("artifacts", {}).items()
    }
    clients[None] = servers.client(artifact_env(cached_case_artifact(request)))
    return clients


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
    directory) and `{port}` (0: the server picks a port and prints it) are
    substituted in each word. The process also gets `artifact_env`, runs in its own process group (so stopping it also stops
    what `uv run` launched) and logs to `log_dir`."""

    def __init__(self, template, log_dir):
        self.template = shlex.split(template)
        self.log_dir = log_dir
        self.servers = {}

    def argv(self, db, port):
        return [
            word.replace("{db}", str(db)).replace("{port}", str(port))
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
        argv = self.argv(env["REG_META_DB"], 0)
        with log.open("wb") as output:
            process = subprocess.Popen(
                argv,
                cwd=REPO,
                env=os.environ | env,
                stdin=subprocess.DEVNULL,
                stdout=output,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        client = _ready(process, log, time.monotonic() + SERVER_READY_SECONDS)
        if client is None:
            _stop(process)
            raise RuntimeError(
                f"server never answered {READY_PATH}: {shlex.join(argv)}\n"
                f"{log.read_text(errors='replace')[-4000:]}"
            )
        return process, client


def _ready(process, log, deadline):
    """A client of the server once it answers `READY_PATH`, or None when the process
    exits or the deadline passes first.

    The server binds `--port 0` and names the bound address in `LISTENING` on its
    log, so the client reaches this server and no other worker's. `READY_PATH` is
    static, so polling it stacks no work on a server still on its cold first read."""
    client = None
    while time.monotonic() < deadline:
        if client is None and (match := LISTENING.search(log.read_bytes())):
            client = httpx2.Client(
                base_url=f"http://127.0.0.1:{int(match[1])}", timeout=60
            )
        if client is not None:
            try:
                client.get(READY_PATH, timeout=1)
            except httpx2.TransportError:
                pass
            else:
                # An exited server's released port can be answered by another.
                if process.poll() is None:
                    return client
        # Waiting on the process paces the poll and notices an early exit.
        with suppress(subprocess.TimeoutExpired):
            process.wait(timeout=0.05)
            break
    if client is not None:
        client.close()
    return None


def _stop(process):
    with suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        process.wait()
