"""The `--server-cmd` server: one process per cached artifact (`ServerPool`), reached
over a real socket by the pytest surfaces not yet ported to `crates/reg-conformance`.
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
from reader_artifacts import REPO

READY_PATH = "/openapi.json"
# The line `reg-meta serve` prints on stderr once bound.
LISTENING = re.compile(rb"^listening on \S+:(\d+)\n", re.MULTILINE)
SERVER_READY_SECONDS = 120


def artifact_env(path):
    """The environment the app needs to serve the artifact at `path`."""
    return {"REG_META_DB": str(path.parent)}


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
