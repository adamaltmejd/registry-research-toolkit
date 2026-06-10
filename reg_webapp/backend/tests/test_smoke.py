"""Unit tests for the per-deploy smoke gate (reg_webapp.smoke).

Drives ``run_smoke`` against a stdlib ``http.server`` stub so the golden-check
logic is exercised without Docker or the real app — deterministic and offline.
The smoke module is stdlib-only (urllib) for exactly this reason: it ships in
the ``--no-dev`` runtime venv where httpx/TestClient are absent.
"""

from __future__ import annotations

import contextlib
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import TYPE_CHECKING

import pytest
from reg_webapp.smoke import SmokeError, run_smoke

if TYPE_CHECKING:
    from collections.abc import Iterator

# A healthy provider node the catalog walk descends into.
_GOOD_CONTEXT = {
    "steward": {"id": "global", "name": "Global"},
    "reg_meta": {"schema_version": "5.2.0", "import_date": "2026-06-10"},
    "webapp": {"version": "0.1.0"},
}
_GOOD_ROOT = {
    "kind": "root",
    "children": [
        {"kind": "provider", "fqid": "scb", "name": "SCB"},
        {"kind": "classification-root"},
    ],
}
_GOOD_PROVIDER = {"kind": "provider", "fqid": "scb", "name": "SCB"}


def _make_handler(routes: dict[str, tuple[int, object]]):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args: object) -> None:  # silence test noise
            pass

        def do_GET(self) -> None:  # noqa: N802 (BaseHTTPRequestHandler API)
            if self.path not in routes:
                self.send_response(404)
                self.end_headers()
                return
            status, body = routes[self.path]
            payload = json.dumps(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(payload)

    return Handler


@contextlib.contextmanager
def _serve(routes: dict[str, tuple[int, object]]) -> Iterator[str]:
    """Run a stub HTTP server for the body of the `with`, yielding its base URL."""
    server = HTTPServer(("127.0.0.1", 0), _make_handler(routes))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        thread.join()


@pytest.fixture
def healthy_server() -> Iterator[str]:
    routes = {
        "/api/context": (200, _GOOD_CONTEXT),
        "/api/catalog": (200, _GOOD_ROOT),
        "/api/catalog/scb": (200, _GOOD_PROVIDER),
    }
    with _serve(routes) as base:
        yield base


def test_run_smoke_passes_on_healthy_server(healthy_server: str) -> None:
    # No raise == pass.
    run_smoke(healthy_server, ready_deadline_s=5.0, timeout_s=2.0)


def test_run_smoke_fails_on_empty_catalog() -> None:
    routes = {
        "/api/context": (200, _GOOD_CONTEXT),
        "/api/catalog": (200, {"kind": "root", "children": []}),
    }
    with _serve(routes) as base, pytest.raises(SmokeError, match="zero providers"):
        run_smoke(base, ready_deadline_s=5.0, timeout_s=2.0)


def test_run_smoke_fails_on_bad_context_shape() -> None:
    routes = {"/api/context": (200, {"steward": {"id": "global"}})}  # missing keys
    with _serve(routes) as base, pytest.raises(SmokeError, match="missing key"):
        run_smoke(base, ready_deadline_s=5.0, timeout_s=2.0)


def test_run_smoke_fails_on_500_context() -> None:
    # A reachable-but-failing server (boot-time 500) must FAIL the smoke (exit 1
    # equivalent), NOT be retried for the full deadline → exit 2. The readiness
    # wait stops on the reachable non-200, and _check_context reports the code.
    routes = {"/api/context": (500, {"detail": "boot failed"})}
    start = time.monotonic()
    with _serve(routes) as base, pytest.raises(SmokeError, match="returned 500"):
        run_smoke(base, ready_deadline_s=30.0, timeout_s=2.0)
    # Bailed on the reachable 500, not after burning the 30s deadline.
    assert time.monotonic() - start < 5.0


def test_run_smoke_times_out_when_unreachable() -> None:
    # Nothing listening on this port → readiness wait gives up → TimeoutError.
    with pytest.raises(TimeoutError):
        run_smoke(
            "http://127.0.0.1:1",
            ready_deadline_s=1.0,
            timeout_s=0.5,
        )


def test_run_smoke_fails_fast_when_server_pid_dead() -> None:
    # A long deadline + a dead server PID must fail FAST (uvicorn aborted on
    # boot): the readiness wait detects the dead process instead of waiting out
    # the deadline. PID 2**31-1 is never a live process.
    dead_pid = 2**31 - 1
    start = time.monotonic()
    with pytest.raises(TimeoutError, match="exited before"):
        run_smoke(
            "http://127.0.0.1:1",
            ready_deadline_s=30.0,
            timeout_s=0.5,
            server_pid=dead_pid,
        )
    # Well under the 30s deadline — it bailed on the dead PID, not the timeout.
    assert time.monotonic() - start < 5.0
