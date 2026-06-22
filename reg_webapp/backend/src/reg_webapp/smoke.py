"""Per-deploy smoke gate (REFACTOR_SPEC.md → Remaining test coverage).

Probes a RUNNING reg_webapp over loopback: a golden ``/api/context`` shape check
plus a shallow ``/api/catalog`` walk (root → first provider node). Run by the
container entrypoint BEFORE traffic is admitted; a failure must halt the deploy
with a non-zero exit so a container that booted against a broken DB bake / empty
catalog never serves.

Stdlib-only (``urllib``) on purpose: the runtime venv is installed with
``--no-dev``, so ``httpx`` / Starlette's ``TestClient`` are absent. Probing the
real uvicorn process over loopback also exercises the actual serving path
(lifespan, DB open, middleware), not just an in-process app object.

Exit codes are stable: ``0`` pass, ``1`` smoke assertion failed, ``2`` the
server never became reachable within the deadline.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any

EXIT_OK = 0
EXIT_SMOKE_FAILED = 1
EXIT_UNREACHABLE = 2


class SmokeError(Exception):
    """A golden-check assertion failed (distinct from a transport error)."""


def _get(url: str, timeout: float) -> tuple[int, Any]:
    """GET ``url`` and return ``(status, parsed_json_or_None)``.

    Returns a non-2xx status WITHOUT raising: ``urlopen`` raises ``HTTPError``
    (a ``URLError`` subclass) for any non-2xx, so we catch it FIRST and surface
    its ``.code`` as the status. This is what lets the readiness wait stop
    retrying a reachable-but-failing server (a boot-time 500) and the golden
    checks report the real status. Genuine transport failures (connection
    refused, DNS, timeout) still raise ``URLError`` and propagate to the caller.

    The body is ``json.loads``'d when present (genuinely ``Any``); callers narrow
    it with ``isinstance`` before use. An error body that isn't JSON yields
    ``None`` (the status already tells the checks it failed).
    """
    req = urllib.request.Request(url, method="GET")  # noqa: S310 (loopback only)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        try:
            body = json.loads(exc.read().decode("utf-8"))
        except ValueError, OSError:
            body = None
        return exc.code, body


def _server_alive(server_pid: int | None) -> bool:
    """True if ``server_pid`` is still running (or no PID was supplied to watch)."""
    if server_pid is None:
        return True
    try:
        os.kill(server_pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists but not ours to signal — treat as alive
    return True


def _wait_until_ready(
    base_url: str,
    deadline_s: float,
    poll_s: float = 0.5,
    server_pid: int | None = None,
) -> None:
    """Poll ``/api/context`` until it answers 200 or the deadline elapses.

    The lifespan opens the baked DB at startup; until that completes the socket
    may not yet accept, so transport errors during warmup are expected and
    retried. A non-200 once reachable is a real failure and is NOT retried away
    (it surfaces in the golden check). If ``server_pid`` is supplied and that
    process dies (e.g. uvicorn aborts on a lifespan/boot failure), the wait
    fails fast instead of burning the whole deadline.
    """
    end = time.monotonic() + deadline_s
    last_err: Exception | None = None
    while time.monotonic() < end:
        if not _server_alive(server_pid):
            raise TimeoutError(
                f"server process {server_pid} exited before {base_url} became ready"
                + (f" (last error: {last_err})" if last_err else "")
            )
        try:
            status, _ = _get(f"{base_url}/api/context", timeout=poll_s * 2)
        except (urllib.error.URLError, ConnectionError, OSError) as exc:
            last_err = exc
            time.sleep(poll_s)
            continue
        if status == 200:
            return
        # Reachable but unhealthy — let the golden check report the detail.
        return
    raise TimeoutError(
        f"server at {base_url} not reachable within {deadline_s:.0f}s"
        + (f" (last error: {last_err})" if last_err else "")
    )


def _check_context(base_url: str, timeout: float) -> None:
    status, body = _get(f"{base_url}/api/context", timeout=timeout)
    if status != 200:
        raise SmokeError(f"/api/context returned {status}, expected 200")
    if not isinstance(body, dict):
        raise SmokeError("/api/context body is not a JSON object")
    # Golden keys the lifespan validates at boot — their presence proves the DB
    # manifest read + steward load succeeded.
    for key in ("steward", "reg_meta", "webapp"):
        if key not in body:
            raise SmokeError(f"/api/context missing key {key!r}")
    steward = body["steward"]
    if not isinstance(steward, dict) or not steward.get("id"):
        raise SmokeError("/api/context steward.id is missing or empty")
    reg_meta_info = body["reg_meta"]
    if not isinstance(reg_meta_info, dict) or not reg_meta_info.get("schema_version"):
        raise SmokeError("/api/context reg_meta.schema_version is missing or empty")


def _check_catalog_walk(base_url: str, timeout: float) -> None:
    """Shallow walk: root must list providers; descend into the first one."""
    status, root = _get(f"{base_url}/api/catalog", timeout=timeout)
    if status != 200:
        raise SmokeError(f"/api/catalog returned {status}, expected 200")
    if not isinstance(root, dict) or root.get("kind") != "root":
        raise SmokeError("/api/catalog root is not a 'root' node")
    children = root.get("children") or []
    providers = [c for c in children if c.get("kind") == "provider"]
    if not providers:
        raise SmokeError("/api/catalog root lists zero providers (empty catalog?)")

    # Descend one level into the first provider to prove a real catalog read
    # (the root list is cheap; resolving a node hits the full reg_meta schema).
    fqid = providers[0].get("fqid")
    if not fqid:
        raise SmokeError("first provider node has no fqid")
    status, node = _get(f"{base_url}/api/catalog/{fqid}", timeout=timeout)
    if status != 200:
        raise SmokeError(f"/api/catalog/{fqid} returned {status}, expected 200")
    if not isinstance(node, dict) or node.get("kind") != "provider":
        raise SmokeError(f"/api/catalog/{fqid} did not resolve to a provider node")


def run_smoke(
    base_url: str,
    *,
    ready_deadline_s: float,
    timeout_s: float,
    server_pid: int | None = None,
) -> None:
    """Wait for readiness, then run the golden checks. Raises on any failure."""
    _wait_until_ready(base_url, ready_deadline_s, server_pid=server_pid)
    _check_context(base_url, timeout_s)
    _check_catalog_walk(base_url, timeout_s)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="reg_webapp.smoke", description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--ready-deadline", type=float, default=60.0)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument(
        "--server-pid",
        type=int,
        default=None,
        help="PID of the server being probed; the readiness wait fails fast if "
        "this process exits (e.g. uvicorn aborting on a boot failure).",
    )
    args = parser.parse_args(argv)

    try:
        run_smoke(
            args.base_url,
            ready_deadline_s=args.ready_deadline,
            timeout_s=args.timeout,
            server_pid=args.server_pid,
        )
    except TimeoutError as exc:
        sys.stderr.write(f"smoke: UNREACHABLE: {exc}\n")
        return EXIT_UNREACHABLE
    except SmokeError as exc:
        sys.stderr.write(f"smoke: FAILED: {exc}\n")
        return EXIT_SMOKE_FAILED
    except (urllib.error.URLError, ConnectionError, OSError) as exc:
        sys.stderr.write(f"smoke: FAILED: transport error during check: {exc}\n")
        return EXIT_SMOKE_FAILED
    sys.stdout.write("smoke: OK (/api/context + /api/catalog walk)\n")
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
