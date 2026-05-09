"""End-to-end tests for ``mock_data_wizard.server``.

The server is a thin adapter; its job is route dispatch + envelope
shape + StaleState round-trip. The behaviour underneath each endpoint
already has dedicated tests in ``test_editor.py``, so these tests focus
on the HTTP boundary.

Strategy. Spin up a real ``ThreadingHTTPServer`` on port 0 in a daemon
thread, hit it with ``urllib.request``. Regmeta lookups are stubbed via
the same monkeypatching pattern used by ``test_editor.py``: signals are
empty by default, individual tests override.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from mock_data_wizard import editor, server
from mock_data_wizard.server import ServerConfig, build_server


@pytest.fixture(autouse=True)
def _no_regmeta(monkeypatch):
    """Regmeta DB unavailable by default; matches test_editor.py."""
    monkeypatch.setattr(
        editor,
        "_autodetect_register_per_source",
        lambda discover, db_path: {
            src["source_name"]: None for src in discover.get("sources", [])
        },
    )
    monkeypatch.setattr(editor, "_resolve_signals_for_register", lambda *a, **kw: {})
    monkeypatch.setattr(editor, "resolve_register", lambda name, db_path=None: None)


def _write_discover(path: Path, sources):
    target = path / "mock_data_discovery.json"
    target.write_text(
        json.dumps({"contract_version": "discover-1.0.0", "sources": sources}),
        encoding="utf-8",
    )
    return target


@pytest.fixture
def initialized_project(tmp_path: Path) -> Path:
    """A tmp project with an initialised config + 1 source / 2 columns."""
    discover_path = _write_discover(
        tmp_path,
        [
            {
                "source_name": "src",
                "columns": [
                    {"name": "LopNr", "sql_type": "BIGINT"},
                    {"name": "Mystery", "sql_type": "VARCHAR"},
                ],
            }
        ],
    )
    editor.init_if_missing(tmp_path, discover_path)
    return tmp_path


@pytest.fixture
def running_server(initialized_project: Path):
    """Start a real ThreadingHTTPServer on an ephemeral port."""
    config = ServerConfig(project_dir=initialized_project, host="127.0.0.1", port=0)
    httpd = build_server(config)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    host, port = httpd.server_address[:2]
    base_url = f"http://{host}:{port}"
    try:
        yield base_url
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=2)


# -- Helpers --------------------------------------------------------------


def _fetch(method: str, url: str, body: dict[str, Any] | None = None):
    """Return ``(status, parsed_json)`` for one request. Raises on
    non-JSON responses; callers can catch it."""
    data = json.dumps(body).encode("utf-8") if body is not None else None
    headers = {"Content-Type": "application/json"} if data else {}
    req = Request(url, data=data, method=method, headers=headers)
    try:
        with urlopen(req) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


# -- Endpoint contract ----------------------------------------------------


def test_get_state_returns_snapshot(running_server: str):
    status, body = _fetch("GET", f"{running_server}/api/state")
    assert status == 200
    assert set(body.keys()) == {
        "config",
        "groups",
        "discover",
        "warnings",
        "snapshot_version",
    }
    assert body["config"]["contract_version"] == "mdw-config-3.0.0"


def test_get_state_404_when_not_initialized(tmp_path: Path):
    """Project_dir without mock_data_config.json → 404 not_initialized."""
    config = ServerConfig(project_dir=tmp_path, host="127.0.0.1", port=0)
    httpd = build_server(config)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    host, port = httpd.server_address[:2]
    try:
        status, body = _fetch("GET", f"http://{host}:{port}/api/state")
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=2)
    assert status == 404
    assert body["error"]["code"] == "not_initialized"


def test_init_creates_config_then_idempotent(tmp_path: Path):
    """POST /api/init bootstraps from discover; second call is a no-op."""
    _write_discover(
        tmp_path,
        [
            {
                "source_name": "src",
                "columns": [{"name": "LopNr", "sql_type": "BIGINT"}],
            }
        ],
    )
    config = ServerConfig(project_dir=tmp_path, host="127.0.0.1", port=0)
    httpd = build_server(config)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    host, port = httpd.server_address[:2]
    base = f"http://{host}:{port}"
    try:
        status, body = _fetch("POST", f"{base}/api/init", {})
        assert status == 200
        assert body["config"]["contract_version"] == "mdw-config-3.0.0"
        assert (tmp_path / "mock_data_config.json").exists()

        # Idempotent: second call returns the same snapshot.
        status2, body2 = _fetch("POST", f"{base}/api/init", {})
        assert status2 == 200
        assert body2["snapshot_version"] == body["snapshot_version"]
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=2)


def test_init_404_when_no_discover(tmp_path: Path):
    """POST /api/init with no mock_data_discovery.json → 404 not_initialized."""
    config = ServerConfig(project_dir=tmp_path, host="127.0.0.1", port=0)
    httpd = build_server(config)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    host, port = httpd.server_address[:2]
    try:
        status, body = _fetch("POST", f"http://{host}:{port}/api/init", {})
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=2)
    assert status == 404
    assert body["error"]["code"] == "not_initialized"
    assert "mock_data_discovery.json" in body["error"]["message"]


def test_set_column_type_round_trip(running_server: str):
    _, snapshot = _fetch("GET", f"{running_server}/api/state")
    version = snapshot["snapshot_version"]

    status, body = _fetch(
        "POST",
        f"{running_server}/api/column-type",
        {
            "source": "src",
            "column": "Mystery",
            "type": "categorical",
            "expected_version": version,
        },
    )
    assert status == 200
    cols = body["config"]["column_types"]["src"]
    assert cols["Mystery"]["type"] == "categorical"
    assert ["src", "Mystery"] in body["config"]["manual_columns"]


def test_set_column_type_validation_error(running_server: str):
    _, snapshot = _fetch("GET", f"{running_server}/api/state")
    status, body = _fetch(
        "POST",
        f"{running_server}/api/column-type",
        {
            "source": "src",
            "column": "Mystery",
            "type": "not_a_type",
            "expected_version": snapshot["snapshot_version"],
        },
    )
    assert status == 400
    assert body["error"]["code"] == "validation"


def test_set_column_type_missing_field(running_server: str):
    """Missing required field → 400 validation envelope."""
    status, body = _fetch(
        "POST",
        f"{running_server}/api/column-type",
        {"source": "src", "column": "x"},  # no type, no expected_version
    )
    assert status == 400
    assert body["error"]["code"] == "validation"


def test_stale_state_returns_409_with_fresh_state(running_server: str):
    """A stale ``expected_version`` triggers a 409 carrying the fresh
    snapshot in ``context.fresh_state`` so the client can re-apply
    without an extra GET."""
    _, snapshot = _fetch("GET", f"{running_server}/api/state")
    real_version = snapshot["snapshot_version"]

    status, body = _fetch(
        "POST",
        f"{running_server}/api/column-type",
        {
            "source": "src",
            "column": "Mystery",
            "type": "categorical",
            "expected_version": "stale-token",
        },
    )
    assert status == 409
    assert body["error"]["code"] == "stale_state"
    assert "fresh_state" in body["error"]["context"]
    assert body["error"]["context"]["fresh_state"]["snapshot_version"] == real_version


def test_set_group_register_validation(running_server: str):
    _, snapshot = _fetch("GET", f"{running_server}/api/state")
    status, body = _fetch(
        "POST",
        f"{running_server}/api/group-register",
        {
            "group_id": "noreg-src",
            "register": "totally-not-a-register",
            "expected_version": snapshot["snapshot_version"],
        },
    )
    # `resolve_register` is stubbed to return None; the editor raises
    # ValidationError for unresolved registers.
    assert status == 400
    assert body["error"]["code"] == "validation"


def test_set_group_register_requires_register_key(running_server: str):
    """A missing `register` key must fail validation rather than silently
    clearing the group's register. Regression: `body.get("register")`
    used to default to None, turning typos into destructive writes."""
    _, snapshot = _fetch("GET", f"{running_server}/api/state")
    status, body = _fetch(
        "POST",
        f"{running_server}/api/group-register",
        {
            "group_id": "noreg-src",
            "expected_version": snapshot["snapshot_version"],
        },
    )
    assert status == 400
    assert body["error"]["code"] == "validation"
    assert "register" in body["error"]["message"]


def test_set_group_register_explicit_null_is_accepted(
    running_server: str, monkeypatch: pytest.MonkeyPatch
):
    """Explicit JSON null is the documented way to clear; ensure it
    reaches the editor (even though the editor here is a no-op stub)."""
    called: dict[str, Any] = {}

    def fake_set(project_dir, group_id, register, **kwargs):
        called["register"] = register
        from mock_data_wizard.editor import get_state

        return get_state(project_dir)

    monkeypatch.setattr(server.editor, "set_group_register", fake_set)
    _, snapshot = _fetch("GET", f"{running_server}/api/state")
    status, _body = _fetch(
        "POST",
        f"{running_server}/api/group-register",
        {
            "group_id": "noreg-src",
            "register": None,
            "expected_version": snapshot["snapshot_version"],
        },
    )
    assert status == 200
    assert called["register"] is None


def test_list_registers_returns_payload(
    running_server: str, monkeypatch: pytest.MonkeyPatch
):
    """The endpoint mirrors ``editor.list_registers`` shape regardless
    of whether a regmeta DB is present locally; mock it to a known list
    so the test passes deterministically across machines."""
    from mock_data_wizard.registers import Register

    monkeypatch.setattr(
        editor,
        "list_registers",
        lambda **kwargs: [Register(id=1, name="LISA"), Register(id=2, name="RTB")],
    )
    status, body = _fetch("GET", f"{running_server}/api/registers")
    assert status == 200
    assert body == {"registers": [{"id": 1, "name": "LISA"}, {"id": 2, "name": "RTB"}]}


def test_unknown_route_returns_404_envelope(running_server: str):
    status, body = _fetch("GET", f"{running_server}/api/does-not-exist")
    assert status == 404
    assert body["error"]["code"] == "not_found"


def test_invalid_json_body(running_server: str):
    """Non-JSON POST body must return a 400 envelope, not crash."""
    req = Request(
        f"{running_server}/api/column-type",
        data=b"not-json",
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(req) as resp:
            status, body = resp.status, json.loads(resp.read())
    except HTTPError as exc:
        status, body = exc.code, json.loads(exc.read())
    assert status == 400
    assert body["error"]["code"] == "invalid_json"


def test_method_not_allowed_on_static(running_server: str):
    """POST to a non-API path should yield a clean 405, not crash."""
    status, body = _fetch("POST", f"{running_server}/", {"x": 1})
    assert status == 405
    assert body["error"]["code"] == "method_not_allowed"


def test_method_not_allowed_on_known_api_path(running_server: str):
    """DELETE on /api/state must return an enveloped 405 (not the
    stdlib HTML 501) — the SPA's parseEnvelope expects JSON."""
    status, body = _fetch("DELETE", f"{running_server}/api/state")
    assert status == 405
    assert body["error"]["code"] == "method_not_allowed"


def test_unhandled_exception_returns_envelope(
    running_server: str, monkeypatch: pytest.MonkeyPatch
):
    """An unexpected exception in an editor call must surface as a
    JSON 500 envelope, not BaseHTTPRequestHandler's HTML default —
    otherwise the SPA's parseEnvelope crashes on the first byte."""

    def boom(*args, **kwargs):
        raise RuntimeError("simulated corruption")

    monkeypatch.setattr(server.editor, "get_state", boom)
    status, body = _fetch("GET", f"{running_server}/api/state")
    assert status == 500
    assert body["error"]["code"] == "internal"


# -- Cache-control headers ------------------------------------------------


def _fetch_headers(url: str) -> tuple[int, dict[str, str]]:
    req = Request(url, method="GET")
    try:
        with urlopen(req) as resp:
            resp.read()
            return resp.status, {k.lower(): v for k, v in resp.headers.items()}
    except HTTPError as exc:
        exc.read()
        return exc.code, {k.lower(): v for k, v in exc.headers.items()}


def test_index_html_no_cache(running_server: str):
    """The SPA shell must never be `immutable` — users would get stuck
    on stale shells across deploys."""
    status, headers = _fetch_headers(f"{running_server}/")
    assert status == 200
    assert "no-cache" in headers["cache-control"].lower()


def test_traversal_into_index_uses_no_cache(running_server: str):
    """Regression: cache-control was decided from the raw URL prefix,
    so `/assets/../index.html` resolved to the SPA shell but inherited
    `immutable` caching. The fix bases the decision on the resolved
    target's path under the static root."""
    status, headers = _fetch_headers(f"{running_server}/assets/../index.html")
    assert status == 200
    assert "no-cache" in headers["cache-control"].lower(), (
        f"expected no-cache for resolved-to-index, got {headers.get('cache-control')!r}"
    )


# -- Loopback gate --------------------------------------------------------


def test_build_server_refuses_non_loopback(tmp_path: Path):
    config = ServerConfig(
        project_dir=tmp_path, host="0.0.0.0", port=0, unsafe_host=False
    )
    with pytest.raises(ValueError, match="loopback"):
        build_server(config)


def test_is_loopback_host_true_for_loopback():
    assert server.is_loopback_host("127.0.0.1")
    assert server.is_loopback_host("localhost")
    assert server.is_loopback_host("::1")


def test_is_loopback_host_false_for_external():
    assert not server.is_loopback_host("0.0.0.0")
    assert not server.is_loopback_host("8.8.8.8")


def test_is_ipv6_host_distinguishes_literals():
    assert server.is_ipv6_host("::1")
    assert server.is_ipv6_host("fe80::1")
    assert not server.is_ipv6_host("127.0.0.1")
    assert not server.is_ipv6_host("localhost")


def test_build_server_binds_ipv6_loopback(tmp_path: Path):
    """Regression for the IPv6 bug: stdlib ThreadingHTTPServer defaults
    to AF_INET, so passing ``::1`` would crash at getaddrinfo time
    without the AF_INET6 subclass."""
    config = ServerConfig(project_dir=tmp_path, host="::1", port=0)
    httpd = build_server(config)
    try:
        host, port = httpd.server_address[:2]
        assert host == "::1"
        assert port > 0
    finally:
        httpd.server_close()


def test_resolve_bind_family_prefers_ipv4_when_dual_stack(
    monkeypatch: pytest.MonkeyPatch,
):
    """`localhost` typically resolves to both 127.0.0.1 and ::1; we
    prefer AF_INET to match stdlib HTTPServer's default and what most
    local clients connect to."""
    import socket as _socket

    def fake_getaddrinfo(host, port, **kwargs):
        return [
            (_socket.AF_INET, _socket.SOCK_STREAM, 0, "", ("127.0.0.1", 0)),
            (_socket.AF_INET6, _socket.SOCK_STREAM, 0, "", ("::1", 0, 0, 0)),
        ]

    monkeypatch.setattr(server.socket, "getaddrinfo", fake_getaddrinfo)
    assert server._resolve_bind_family("localhost") == _socket.AF_INET


def test_resolve_bind_family_picks_ipv6_when_no_ipv4(
    monkeypatch: pytest.MonkeyPatch,
):
    """Regression: a hostname that resolves only to IPv6 (e.g.
    `ip6-localhost`, or `localhost` on IPv6-only setups) used to take
    the AF_INET path because the colon heuristic only inspected the
    string. The fix consults `getaddrinfo` for non-literal hosts."""
    import socket as _socket

    def fake_getaddrinfo(host, port, **kwargs):
        return [(_socket.AF_INET6, _socket.SOCK_STREAM, 0, "", ("::1", 0, 0, 0))]

    monkeypatch.setattr(server.socket, "getaddrinfo", fake_getaddrinfo)
    assert server._resolve_bind_family("ip6-localhost") == _socket.AF_INET6


def test_resolve_bind_family_falls_back_to_ipv4_on_resolve_failure(
    monkeypatch: pytest.MonkeyPatch,
):
    def fake_getaddrinfo(*args, **kwargs):
        raise OSError("dns down")

    import socket as _socket

    monkeypatch.setattr(server.socket, "getaddrinfo", fake_getaddrinfo)
    assert server._resolve_bind_family("anything") == _socket.AF_INET


def test_payload_too_large_returns_413(running_server: str):
    """Oversized Content-Length must be rejected with a 413 envelope
    before the server reads anything off the wire — otherwise a bogus
    header could pin a worker thread or balloon memory."""
    huge = str(server._MAX_REQUEST_BYTES + 1)
    # urllib would set its own Content-Length from a real `data=` body;
    # to advertise an oversized length without sending the bytes, drop
    # to a raw socket.
    import socket as _socket
    from urllib.parse import urlparse as _urlparse

    parsed = _urlparse(running_server)
    sock = _socket.create_connection((parsed.hostname, parsed.port), timeout=5)
    try:
        request = (
            f"POST /api/column-type HTTP/1.1\r\n"
            f"Host: {parsed.hostname}:{parsed.port}\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {huge}\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        ).encode("ascii")
        sock.sendall(request)
        # Read until close. We never write the giant body — server should
        # 413 before reading rfile.
        chunks: list[bytes] = []
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            chunks.append(chunk)
        raw = b"".join(chunks)
    finally:
        sock.close()
    head, _, body = raw.partition(b"\r\n\r\n")
    status_line = head.split(b"\r\n", 1)[0].decode("ascii")
    assert "413" in status_line, status_line
    envelope = json.loads(body)
    assert envelope["error"]["code"] == "payload_too_large"
