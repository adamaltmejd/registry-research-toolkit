"""Local HTTP adapter over ``mock_data_wizard.editor``.

A thin shim: parse JSON requests, call into the editor API, serialise
the resulting ``StateSnapshot`` via ``_serialize.state_snapshot_to_dict``.
No new runtime deps — stdlib ``http.server`` only.

Concurrency. ``ThreadingHTTPServer`` runs one thread per request because
the SPA fires several requests in parallel on first load (the four
``GET`` endpoints). Mutations are still serialised at the editor layer
by ``_config_lock`` (fcntl on a sidecar file), so the server doesn't
need its own lock.

Stale-state protocol. ``editor.set_*`` raises ``StaleStateError`` when
``expected_version`` doesn't match the on-disk SHA. The wrapper catches
it, fetches a fresh snapshot, and returns 409 with
``context.fresh_state`` so the client can re-apply without an extra
round-trip. If that fetch itself fails (rare — config deleted
mid-flight), 409 still fires but ``fresh_state`` is omitted.

Safety. ``serve()`` only binds non-loopback hosts when ``unsafe_host``
is True; the CLI surfaces the same gate. There is no auth — local-only
binding is the only line of defence, mirroring the editor's "stateless,
local" stance (DESIGN.md § Editor API).
"""

from __future__ import annotations

import importlib.resources
import json
import logging
import socket
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

from . import editor
from ._serialize import state_snapshot_to_dict

__all__ = [
    "ServerConfig",
    "build_server",
    "serve",
    "is_loopback_host",
    "is_ipv6_host",
]


_LOG = logging.getLogger("mock_data_wizard.server")

_STATIC_DIR = Path(str(importlib.resources.files("mock_data_wizard") / "static"))

# JSON request bodies are tiny (a column-type mutation is well under 1 KiB).
# Cap at 1 MiB so a bogus or malicious Content-Length can't pin a worker
# thread or balloon memory. Loopback-only mitigates the threat, but it's
# cheap insurance.
_MAX_REQUEST_BYTES = 1 * 1024 * 1024


class _BadJSON(Exception):
    """Internal: maps to a 400 ``invalid_json`` envelope."""


class _PayloadTooLarge(Exception):
    """Internal: maps to a 413 ``payload_too_large`` envelope."""


_MIME_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".mjs": "application/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".svg": "image/svg+xml",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".webp": "image/webp",
    ".ico": "image/x-icon",
    ".woff": "font/woff",
    ".woff2": "font/woff2",
    ".map": "application/json; charset=utf-8",
}


# -- Config + entry points -----------------------------------------------


@dataclass(frozen=True)
class ServerConfig:
    """Runtime parameters for one server instance."""

    project_dir: Path
    host: str = "127.0.0.1"
    port: int = 8765
    db_path: Path | None = None
    unsafe_host: bool = False
    static_dir: Path = _STATIC_DIR


def is_loopback_host(host: str) -> bool:
    """True for `127.0.0.1`, `::1`, `localhost`, and the IPv4 loopback
    range. Anything we can't classify is treated as non-loopback (fail
    closed)."""
    candidate = host.strip().lower()
    if candidate in {"localhost", "::1"}:
        return True
    try:
        info = socket.getaddrinfo(candidate, None)
    except OSError:
        return False
    for _, _, _, _, sockaddr in info:
        addr = sockaddr[0]
        if addr == "::1":
            return True
        if addr.startswith("127."):
            return True
    return False


def is_ipv6_host(host: str) -> bool:
    """True when ``host`` is an IPv6 literal. The ``:`` discriminator is
    sufficient because hostnames and IPv4 literals never contain one.
    Used for URL formatting (bracketing), not for picking the bind
    family — see ``_resolve_bind_family``."""
    return ":" in host


def _resolve_bind_family(host: str) -> int:
    """Pick ``AF_INET`` vs ``AF_INET6`` for binding ``host``.

    A literal IPv6 address always needs ``AF_INET6``. For hostnames we
    resolve with ``getaddrinfo`` and prefer ``AF_INET`` when it's
    available — that matches stdlib ``HTTPServer``'s default and is
    what most local clients connect to. We only flip to ``AF_INET6``
    when the host resolves exclusively to IPv6 (e.g. ``ip6-localhost``,
    or ``localhost`` on IPv6-only setups). On resolution failure we
    fall back to ``AF_INET`` and let the bind error surface.
    """
    if ":" in host:
        return socket.AF_INET6
    try:
        infos = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except OSError:
        return socket.AF_INET
    families = {info[0] for info in infos}
    if socket.AF_INET in families:
        return socket.AF_INET
    if socket.AF_INET6 in families:
        return socket.AF_INET6
    return socket.AF_INET


class _ThreadingHTTPServer6(ThreadingHTTPServer):
    """IPv6 variant. Stdlib's ``ThreadingHTTPServer`` defaults to
    ``AF_INET``, so binding ``::1`` fails at ``getaddrinfo`` time. This
    subclass flips the family for IPv6 hosts."""

    address_family = socket.AF_INET6


def build_server(config: ServerConfig) -> ThreadingHTTPServer:
    """Construct a ``ThreadingHTTPServer`` bound to ``config.host:config.port``.

    Refuses non-loopback hosts unless ``config.unsafe_host`` is True. The
    caller invokes ``.serve_forever()`` and ``.shutdown()`` on the
    returned server.
    """
    if not config.unsafe_host and not is_loopback_host(config.host):
        raise ValueError(
            f"refusing to bind {config.host!r} — pass unsafe_host=True "
            f"if you really want to expose the editor API non-loopback. "
            f"There is no auth; this is for trusted networks only."
        )

    handler_cls = _make_handler(config)
    server_cls = (
        _ThreadingHTTPServer6
        if _resolve_bind_family(config.host) == socket.AF_INET6
        else ThreadingHTTPServer
    )
    return server_cls((config.host, config.port), handler_cls)


def serve(config: ServerConfig) -> None:
    """Convenience: build + serve_forever. Blocks the calling thread."""
    server = build_server(config)
    try:
        server.serve_forever()
    finally:
        server.server_close()


# -- Request handling ----------------------------------------------------


def _make_handler(config: ServerConfig) -> type[BaseHTTPRequestHandler]:
    """Curry ``config`` into a handler class.

    A class (not an instance) is required by ``HTTPServer``; closing
    over ``config`` keeps the handler stateless and avoids a global.
    """

    api_routes: dict[tuple[str, str], Callable[[dict[str, Any]], tuple[int, dict]]] = {
        ("GET", "/api/state"): lambda body: _api_get_state(config),
        ("POST", "/api/init"): lambda body: _api_init(config, body),
        ("POST", "/api/column-type"): lambda body: _api_set_column_type(config, body),
        ("POST", "/api/unset-column-manual"): lambda body: _api_unset_column_manual(
            config, body
        ),
        ("POST", "/api/group-register"): lambda body: _api_set_group_register(
            config, body
        ),
        ("POST", "/api/source-registers"): lambda body: _api_set_source_registers(
            config, body
        ),
        ("POST", "/api/source-years"): lambda body: _api_set_source_years(config, body),
        ("GET", "/api/registers"): lambda body: _api_list_registers(config),
        ("POST", "/api/column-values"): lambda body: _api_get_column_values(
            config, body
        ),
        ("POST", "/api/column-varinfo"): lambda body: _api_get_column_varinfo(
            config, body
        ),
        ("POST", "/api/panel"): lambda body: _api_put_panel(config, body),
        ("POST", "/api/remove-panel"): lambda body: _api_remove_panel(config, body),
    }
    # Paths that exist for at least one method — used to distinguish
    # 405 (path exists, wrong verb) from 404 (path does not exist).
    api_paths: frozenset[str] = frozenset(p for _, p in api_routes)

    class Handler(BaseHTTPRequestHandler):
        # http.server defaults to HTTP/1.0; advertising 1.1 lets the
        # browser keep connections alive between asset requests.
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt: str, *args: Any) -> None:
            _LOG.info("%s - %s", self.address_string(), fmt % args)

        def do_GET(self) -> None:  # noqa: N802 - stdlib API
            self._dispatch("GET", api_routes)

        def do_POST(self) -> None:  # noqa: N802
            self._dispatch("POST", api_routes)

        # Other verbs land here. BaseHTTPRequestHandler would otherwise
        # 501 with an HTML body that breaks the SPA's parseEnvelope().
        def do_PUT(self) -> None:  # noqa: N802
            self._dispatch("PUT", api_routes)

        def do_DELETE(self) -> None:  # noqa: N802
            self._dispatch("DELETE", api_routes)

        def do_PATCH(self) -> None:  # noqa: N802
            self._dispatch("PATCH", api_routes)

        def do_OPTIONS(self) -> None:  # noqa: N802
            self._dispatch("OPTIONS", api_routes)

        def do_HEAD(self) -> None:  # noqa: N802
            self._dispatch("HEAD", api_routes)

        def _dispatch(
            self,
            method: str,
            routes: dict[tuple[str, str], Callable[[dict[str, Any]], tuple[int, dict]]],
        ) -> None:
            path = urlparse(self.path).path
            if path.startswith("/api/"):
                self._dispatch_api(method, path, routes)
                return
            if method != "GET":
                self._send_error_envelope(
                    HTTPStatus.METHOD_NOT_ALLOWED,
                    "method_not_allowed",
                    f"{method} not allowed on {path}",
                )
                return
            self._serve_static(path)

        def _dispatch_api(
            self,
            method: str,
            path: str,
            routes: dict[tuple[str, str], Callable[[dict[str, Any]], tuple[int, dict]]],
        ) -> None:
            handler = routes.get((method, path))
            if handler is None:
                if path in api_paths:
                    self._send_error_envelope(
                        HTTPStatus.METHOD_NOT_ALLOWED,
                        "method_not_allowed",
                        f"{method} not allowed on {path}",
                    )
                    return
                self._send_error_envelope(
                    HTTPStatus.NOT_FOUND, "not_found", f"no route for {method} {path}"
                )
                return
            try:
                body = self._read_json_body() if method == "POST" else {}
            except _PayloadTooLarge as exc:
                self._send_error_envelope(
                    HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                    "payload_too_large",
                    str(exc),
                )
                return
            except _BadJSON as exc:
                self._send_error_envelope(
                    HTTPStatus.BAD_REQUEST, "invalid_json", str(exc)
                )
                return

            try:
                status, payload = handler(body)
            except editor.NotInitializedError as exc:
                self._send_error_envelope(
                    HTTPStatus.NOT_FOUND,
                    "not_initialized",
                    str(exc),
                )
                return
            except editor.ValidationError as exc:
                self._send_error_envelope(
                    HTTPStatus.BAD_REQUEST,
                    "validation",
                    str(exc),
                )
                return
            except editor.StaleStateError as exc:
                context: dict[str, Any] = {}
                try:
                    fresh = editor.get_state(config.project_dir, db_path=config.db_path)
                    context["fresh_state"] = state_snapshot_to_dict(fresh)
                except editor.NotInitializedError:
                    # Config vanished mid-flight; client must re-init.
                    pass
                self._send_error_envelope(
                    HTTPStatus.CONFLICT, "stale_state", str(exc), context=context
                )
                return
            except Exception:
                # Anything else is a bug or a corrupt-state condition.
                # The SPA's parseEnvelope expects JSON, so we must not
                # let BaseHTTPRequestHandler emit its default HTML 500.
                _LOG.exception("unhandled error in %s %s", method, path)
                self._send_error_envelope(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    "internal",
                    "internal server error; check the server log",
                )
                return

            self._send_json(status, payload)

        # -- Static SPA --------------------------------------------------

        def _serve_static(self, path: str) -> None:
            rel = "index.html" if path in ("", "/") else path.lstrip("/")
            try:
                target = (config.static_dir / rel).resolve(strict=True)
            except (OSError, ValueError):
                # Anything we can't resolve falls back to index.html.
                # SPA routing means /foo can be a client-side route;
                # serving the shell is correct.
                target = (config.static_dir / "index.html").resolve()

            try:
                root = config.static_dir.resolve()
            except OSError:
                self._send_error_envelope(
                    HTTPStatus.NOT_FOUND, "not_found", "static dir missing"
                )
                return

            if root not in target.parents and target != root:
                # Path-traversal guard: the resolved target must live
                # under the static root.
                self._send_error_envelope(
                    HTTPStatus.NOT_FOUND, "not_found", "outside static root"
                )
                return

            if not target.is_file():
                if (root / "index.html").is_file():
                    target = root / "index.html"
                else:
                    self._send_error_envelope(
                        HTTPStatus.NOT_FOUND,
                        "not_found",
                        (
                            "bundled SPA assets are missing — reinstall "
                            "the package, or for a source checkout run "
                            "`bun run build` in mock_data_wizard/web/."
                        ),
                    )
                    return

            data = target.read_bytes()
            mime = _MIME_TYPES.get(target.suffix.lower(), "application/octet-stream")

            # Decide caching from the *resolved* path, not the request URL:
            # `/assets/../index.html` resolves to the SPA shell but its raw
            # URL prefix would imply long-lived caching, which would stick
            # users on a stale shell. Hashed-asset caching is opt-in and
            # restricted to files actually living under root/assets/.
            relative = target.relative_to(root) if target.is_relative_to(root) else None
            is_hashed_asset = (
                relative is not None
                and relative.parts
                and relative.parts[0] == "assets"
                and target.name != "index.html"
            )
            cache_control = (
                "public, max-age=31536000, immutable" if is_hashed_asset else "no-cache"
            )
            self._send_bytes(HTTPStatus.OK, mime, data, cache_control=cache_control)

        # -- Body / response helpers -------------------------------------

        def _read_json_body(self) -> dict[str, Any]:
            length_header = self.headers.get("Content-Length")
            if length_header is None:
                raise _BadJSON("missing Content-Length")
            try:
                length = int(length_header)
            except ValueError as exc:
                raise _BadJSON("invalid Content-Length") from exc
            if length < 0:
                raise _BadJSON("invalid Content-Length")
            if length > _MAX_REQUEST_BYTES:
                raise _PayloadTooLarge(
                    f"request body too large ({length} > {_MAX_REQUEST_BYTES} bytes)"
                )
            if length == 0:
                return {}
            raw = self.rfile.read(length)
            try:
                value = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise _BadJSON(f"invalid JSON body: {exc}") from exc
            if not isinstance(value, dict):
                raise _BadJSON("request body must be a JSON object")
            return value

        def _send_json(self, status: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self._send_bytes(
                status,
                "application/json; charset=utf-8",
                body,
                cache_control="no-cache",
            )

        def _send_error_envelope(
            self,
            status: int,
            code: str,
            message: str,
            *,
            context: dict[str, Any] | None = None,
        ) -> None:
            envelope: dict[str, Any] = {"error": {"code": code, "message": message}}
            if context:
                envelope["error"]["context"] = context
            self._send_json(status, envelope)

        def _send_bytes(
            self,
            status: int,
            content_type: str,
            data: bytes,
            *,
            cache_control: str = "no-cache",
        ) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", cache_control)
            self.end_headers()
            self.wfile.write(data)

    return Handler


# -- API handlers --------------------------------------------------------


def _api_get_state(config: ServerConfig) -> tuple[int, dict[str, Any]]:
    snap = editor.get_state(config.project_dir, db_path=config.db_path)
    return HTTPStatus.OK, state_snapshot_to_dict(snap)


def _api_init(config: ServerConfig, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    """Idempotent: bootstraps from discover when the config is missing,
    otherwise returns the current snapshot.

    Discover is only required for first-time bootstrap. Once the project
    is initialised, ``POST /api/init`` is a safe no-op even if the
    discover file has been removed — the contract stays idempotent for
    callers that don't track local file state."""
    # Lock the contract: the only way to clobber an existing config is
    # to delete the file on disk. If a future client sends ``{"force":
    # true}`` or similar, fail loudly rather than silently ignoring.
    if body:
        raise editor.ValidationError(
            f"POST /api/init does not accept a body; got keys "
            f"{sorted(body)}. Overwrite mode is intentionally not "
            f"exposed — delete the config file to re-initialise."
        )
    config_path = config.project_dir / editor.CONFIG_FILENAME
    discover_path = config.project_dir / editor.DISCOVER_FILENAME_DEFAULT
    if config_path.exists():
        # Already initialised — read-only no-op. get_state's discover
        # fallback handles the case where discover.json is absent.
        snap = editor.get_state(config.project_dir, db_path=config.db_path)
    else:
        if not discover_path.exists():
            raise editor.NotInitializedError(
                f"{discover_path} not found. Run the discover step on MONA "
                f"first, then place {editor.DISCOVER_FILENAME_DEFAULT} next "
                f"to your project before initialising."
            )
        snap = editor.init_if_missing(
            config.project_dir, discover_path, db_path=config.db_path
        )
    return HTTPStatus.OK, state_snapshot_to_dict(snap)


def _api_set_column_type(
    config: ServerConfig, body: dict[str, Any]
) -> tuple[int, dict[str, Any]]:
    sources = _required_str_list(body, "sources")
    column = _required_str(body, "column")
    new_type = _required_str(body, "type")
    expected_version = _required_str(body, "expected_version")

    # ``hint`` follows editor's three-state convention: missing key
    # means UNCHANGED, JSON null clears, dict sets.
    hint_arg: Any
    if "hint" not in body:
        hint_arg = editor.UNCHANGED
    else:
        hint_value = body["hint"]
        if hint_value is None or isinstance(hint_value, dict):
            hint_arg = hint_value
        else:
            raise editor.ValidationError(
                f"hint must be an object or null, got {type(hint_value).__name__}"
            )

    snap = editor.set_column_type(
        config.project_dir,
        sources,
        column,
        new_type,
        expected_version=expected_version,
        hint=hint_arg,
        db_path=config.db_path,
    )
    return HTTPStatus.OK, state_snapshot_to_dict(snap)


def _api_unset_column_manual(
    config: ServerConfig, body: dict[str, Any]
) -> tuple[int, dict[str, Any]]:
    sources = _required_str_list(body, "sources")
    column = _required_str(body, "column")
    expected_version = _required_str(body, "expected_version")
    snap = editor.unset_column_manual_override(
        config.project_dir,
        sources,
        column,
        expected_version=expected_version,
        db_path=config.db_path,
    )
    return HTTPStatus.OK, state_snapshot_to_dict(snap)


def _api_set_group_register(
    config: ServerConfig, body: dict[str, Any]
) -> tuple[int, dict[str, Any]]:
    group_id = _required_str(body, "group_id")
    expected_version = _required_str(body, "expected_version")
    # `register` must be present; only an explicit JSON null clears it.
    # A missing key would otherwise let a stale frontend or typo trigger
    # a destructive reclassification write.
    if "register" not in body:
        raise editor.ValidationError(
            "missing required field 'register' (use null to clear)"
        )
    register_value = body["register"]
    if register_value is not None and not isinstance(register_value, str):
        raise editor.ValidationError(
            f"register must be a string or null, got {type(register_value).__name__}"
        )
    reclassify_manual = body.get("reclassify_manual", False)
    if not isinstance(reclassify_manual, bool):
        raise editor.ValidationError("reclassify_manual must be boolean")

    snap = editor.set_group_register(
        config.project_dir,
        group_id,
        register_value,
        expected_version=expected_version,
        reclassify_manual=reclassify_manual,
        db_path=config.db_path,
    )
    return HTTPStatus.OK, state_snapshot_to_dict(snap)


def _api_set_source_registers(
    config: ServerConfig, body: dict[str, Any]
) -> tuple[int, dict[str, Any]]:
    """Per-source register assignment. ``assignments`` maps source_name
    → register name (or JSON null to clear). The key must be present so
    a stale frontend or typo can't trigger a destructive write."""
    expected_version = _required_str(body, "expected_version")
    if "assignments" not in body:
        raise editor.ValidationError("missing required field 'assignments'")
    raw = body["assignments"]
    if not isinstance(raw, dict):
        raise editor.ValidationError(
            f"assignments must be an object, got {type(raw).__name__}"
        )
    if not raw:
        raise editor.ValidationError("assignments must be non-empty")
    assignments: dict[str, str | None] = {}
    for sn, val in raw.items():
        if not isinstance(sn, str):
            raise editor.ValidationError(
                f"assignments keys must be strings, got {type(sn).__name__}"
            )
        if val is not None and not isinstance(val, str):
            raise editor.ValidationError(
                f"assignments[{sn!r}] must be a string or null, "
                f"got {type(val).__name__}"
            )
        assignments[sn] = val
    reclassify_manual = body.get("reclassify_manual", False)
    if not isinstance(reclassify_manual, bool):
        raise editor.ValidationError("reclassify_manual must be boolean")

    snap = editor.set_source_registers(
        config.project_dir,
        assignments,
        expected_version=expected_version,
        reclassify_manual=reclassify_manual,
        db_path=config.db_path,
    )
    return HTTPStatus.OK, state_snapshot_to_dict(snap)


def _api_set_source_years(
    config: ServerConfig, body: dict[str, Any]
) -> tuple[int, dict[str, Any]]:
    """Bulk-set per-source ``year`` across multiple sources atomically.

    Body shape: ``{assignments: {source_name: year|null, ...},
    expected_version}``. Each year is an integer (set the year) or null
    (delete the ``year`` key — sends the row back to "missing"). Every
    listed source must exist in the current config; an unknown source
    aborts the whole call before any on-disk write. A fully no-op call
    leaves ``snapshot_version`` unchanged."""
    expected_version = _required_str(body, "expected_version")
    raw = body.get("assignments")
    if not isinstance(raw, dict):
        raise editor.ValidationError(
            f"assignments must be an object, got {type(raw).__name__}"
        )
    assignments: dict[str, int | None] = {}
    for sn, val in raw.items():
        if not isinstance(sn, str) or not sn:
            raise editor.ValidationError(
                f"assignments keys must be non-empty strings, got {sn!r}"
            )
        if val is None:
            assignments[sn] = None
        elif isinstance(val, bool) or not isinstance(val, int):
            raise editor.ValidationError(
                f"assignments[{sn!r}] must be int or null, got {type(val).__name__}"
            )
        else:
            assignments[sn] = val
    snap = editor.set_source_years(
        config.project_dir,
        assignments,
        expected_version=expected_version,
        db_path=config.db_path,
    )
    return HTTPStatus.OK, state_snapshot_to_dict(snap)


def _api_list_registers(config: ServerConfig) -> tuple[int, dict[str, Any]]:
    registers = editor.list_registers(db_path=config.db_path)
    return HTTPStatus.OK, {
        "registers": [{"id": r.id, "name": r.name} for r in registers]
    }


def _api_put_panel(
    config: ServerConfig, body: dict[str, Any]
) -> tuple[int, dict[str, Any]]:
    """Add or replace one panel, identified by ``panel_id``.

    Member shape mirrors the on-disk JSON: ``{source, period?, time_key?}``,
    with exactly one of ``period`` / ``time_key`` per member. Structural
    validation is delegated to ``editor.parse_panel_payload`` so the wire
    format and the on-disk JSON share one validator. Optional
    ``previous_panel_id`` carries the renamed-from id so rename can drop
    the old entry atomically (prevents source-overlap collision).
    """
    expected_version = _required_str(body, "expected_version")
    previous_panel_id = body.get("previous_panel_id")
    if previous_panel_id is not None and not isinstance(previous_panel_id, str):
        raise editor.ValidationError(
            f"previous_panel_id must be a string or null, "
            f"got {type(previous_panel_id).__name__}"
        )
    panel_body = {
        k: v
        for k, v in body.items()
        if k not in {"expected_version", "previous_panel_id"}
    }
    panel = editor.parse_panel_payload(panel_body)
    snap = editor.put_panel(
        config.project_dir,
        panel,
        expected_version=expected_version,
        db_path=config.db_path,
        previous_panel_id=previous_panel_id,
    )
    return HTTPStatus.OK, state_snapshot_to_dict(snap)


def _api_remove_panel(
    config: ServerConfig, body: dict[str, Any]
) -> tuple[int, dict[str, Any]]:
    panel_id = _required_str(body, "panel_id")
    expected_version = _required_str(body, "expected_version")
    snap = editor.remove_panel(
        config.project_dir,
        panel_id,
        expected_version=expected_version,
        db_path=config.db_path,
    )
    return HTTPStatus.OK, state_snapshot_to_dict(snap)


def _api_get_column_values(
    config: ServerConfig, body: dict[str, Any]
) -> tuple[int, dict[str, Any]]:
    column = _required_str(body, "column")
    register_value = _required_nullable_str(body, "register")
    picked = body.get("picked_classification")
    if picked is not None and not isinstance(picked, str):
        raise editor.ValidationError(
            "picked_classification must be a string or null, "
            f"got {type(picked).__name__}"
        )
    picked_vs_raw = body.get("picked_value_set")
    # bool is a subclass of int in Python — reject it explicitly so a
    # client bug doesn't accidentally pick value_set_id 0 / 1.
    if picked_vs_raw is not None and (
        isinstance(picked_vs_raw, bool) or not isinstance(picked_vs_raw, int)
    ):
        raise editor.ValidationError(
            "picked_value_set must be an integer or null, "
            f"got {type(picked_vs_raw).__name__}"
        )
    picked_var_id_raw = body.get("picked_var_id")
    if picked_var_id_raw is not None and (
        isinstance(picked_var_id_raw, bool) or not isinstance(picked_var_id_raw, int)
    ):
        raise editor.ValidationError(
            "picked_var_id must be an integer or null, "
            f"got {type(picked_var_id_raw).__name__}"
        )
    relevant_years_raw = body.get("relevant_years")
    relevant_years: list[int] | None = None
    if relevant_years_raw is not None:
        if not isinstance(relevant_years_raw, list):
            raise editor.ValidationError(
                "relevant_years must be a list of integers or null, "
                f"got {type(relevant_years_raw).__name__}"
            )
        for y in relevant_years_raw:
            if isinstance(y, bool) or not isinstance(y, int):
                raise editor.ValidationError(
                    f"relevant_years entries must be integers, got {type(y).__name__}"
                )
        relevant_years = list(relevant_years_raw)
    result = editor.get_column_values(
        register_value,
        column,
        picked_classification=picked,
        picked_value_set=picked_vs_raw,
        picked_var_id=picked_var_id_raw,
        relevant_years=relevant_years,
        db_path=config.db_path,
    )
    return HTTPStatus.OK, {
        "kind": result.kind,
        "title": result.title,
        "description": result.description,
        "codes": [{"code": c.code, "label": c.label} for c in result.codes],
        "tier": result.tier,
        "note": result.note,
        "classifications": [
            {
                "short_name": c.short_name,
                "year_min": c.year_min,
                "year_max": c.year_max,
            }
            for c in result.classifications
        ],
        "picked_classification": result.picked_classification,
        "value_sets": [
            {
                "value_set_id": g.value_set_id,
                "year_min": g.year_min,
                "year_max": g.year_max,
            }
            for g in result.value_sets
        ],
        "picked_value_set": result.picked_value_set,
    }


def _api_get_column_varinfo(
    config: ServerConfig, body: dict[str, Any]
) -> tuple[int, dict[str, Any]]:
    column = _required_str(body, "column")
    register_value = _required_nullable_str(body, "register")
    relevant_years_raw = body.get("relevant_years")
    relevant_years: list[int] | None = None
    if relevant_years_raw is not None:
        if not isinstance(relevant_years_raw, list):
            raise editor.ValidationError(
                "relevant_years must be a list of integers or null, "
                f"got {type(relevant_years_raw).__name__}"
            )
        for y in relevant_years_raw:
            if isinstance(y, bool) or not isinstance(y, int):
                raise editor.ValidationError(
                    f"relevant_years entries must be integers, got {type(y).__name__}"
                )
        relevant_years = list(relevant_years_raw)
    result = editor.get_column_varinfo(
        register_value,
        column,
        relevant_years=relevant_years,
        db_path=config.db_path,
    )
    return HTTPStatus.OK, _serialize_varinfo(result)


def _serialize_varinfo(result: editor.ColumnVarinfoResult) -> dict[str, Any]:
    if result.kind == "none":
        return {"kind": "none", "reason": result.none_reason or "not_found"}
    if result.primary is None:
        # Defensive: kind="single"/"divergent" without a primary would be
        # a bug in get_column_varinfo. Surface as "none" rather than
        # emitting a malformed envelope.
        return {"kind": "none", "reason": "not_found"}
    payload: dict[str, Any] = {
        "kind": result.kind,
        "primary": _serialize_varinfo_description(result.primary),
        "primary_share": {
            "instances": result.primary_instances,
            "total": result.total_instances,
        },
    }
    if result.kind == "divergent":
        payload["alternatives"] = [
            {
                "description": _serialize_varinfo_description(alt.description),
                "instances": alt.instances,
            }
            for alt in result.alternatives
        ]
    return payload


def _serialize_varinfo_description(desc: editor.VarinfoDescription) -> dict[str, Any]:
    return {
        "variabelnamn": desc.variabelnamn,
        "variabeldefinition": desc.variabeldefinition,
        "variabelbeskrivning": desc.variabelbeskrivning,
        "variabeloperationell_definition": desc.variabeloperationell_definition,
        "variabelreferenstid": desc.variabelreferenstid,
        "variabelhamtadfran": desc.variabelhamtadfran,
        "variabelregister_kalla": desc.variabelregister_kalla,
        "mattenhet": desc.mattenhet,
        "var_id": desc.var_id,
        "register_name": desc.register_name,
    }


def _required_nullable_str(body: dict[str, Any], key: str) -> str | None:
    if key not in body:
        raise editor.ValidationError(
            f"missing required field {key!r} (use null when unassigned)"
        )
    value = body[key]
    if value is not None and not isinstance(value, str):
        raise editor.ValidationError(
            f"{key} must be a string or null, got {type(value).__name__}"
        )
    return value


def _required_str(body: dict[str, Any], key: str) -> str:
    if key not in body:
        raise editor.ValidationError(f"missing required field {key!r}")
    value = body[key]
    if not isinstance(value, str):
        raise editor.ValidationError(
            f"field {key!r} must be a string, got {type(value).__name__}"
        )
    return value


def _required_str_list(body: dict[str, Any], key: str) -> list[str]:
    if key not in body:
        raise editor.ValidationError(f"missing required field {key!r}")
    value = body[key]
    if not isinstance(value, list):
        raise editor.ValidationError(
            f"field {key!r} must be an array, got {type(value).__name__}"
        )
    if not value:
        raise editor.ValidationError(f"field {key!r} must be non-empty")
    for i, entry in enumerate(value):
        if not isinstance(entry, str):
            raise editor.ValidationError(
                f"field {key!r}[{i}] must be a string, got {type(entry).__name__}"
            )
    return value
