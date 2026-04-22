"""Self-update logic for the mock-data-wizard package.

mock-data-wizard is distributed as a pure Python package with no database
assets, so the update flow is much simpler than regmeta's: check PyPI for
the latest version and shell out to `uv tool upgrade`. The background
UpdateChecker mirrors regmeta's so CLI invocations never block on a network
round-trip.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

from . import __version__

PYPI_JSON_URL = "https://pypi.org/pypi/mock-data-wizard/json"

_UPDATE_CHECK_INTERVAL = 7 * 24 * 3600  # 1 week
_BG_JOIN_TIMEOUT = 3
_HTTP_TIMEOUT = 10

EXIT_CONFIG = 10
EXIT_NETWORK = 25


def _state_dir() -> Path:
    """User state directory for update-check cache.

    Resolution: ``$MOCK_DATA_WIZARD_STATE`` > ``$XDG_STATE_HOME/mock-data-wizard``
    > platform default. Uses STATE rather than DATA because the cache is
    regenerable and not worth backing up.
    """
    if env := os.environ.get("MOCK_DATA_WIZARD_STATE"):
        return Path(env).expanduser()
    if xdg := os.environ.get("XDG_STATE_HOME"):
        return Path(xdg) / "mock-data-wizard"
    if sys.platform == "win32":
        return (
            Path(os.environ.get("LOCALAPPDATA", "~/AppData/Local")).expanduser()
            / "mock-data-wizard"
        )
    return Path.home() / ".local" / "state" / "mock-data-wizard"


def _check_cache_path() -> Path:
    return _state_dir() / ".update_check"


def parse_version(v: str) -> tuple[int, ...]:
    """Parse a PEP 440-ish version into a comparable tuple.

    Supports ``X.Y.Z``, ``X.Y.ZaN`` (alpha), and ``X.Y.Z.devN`` (dev).
    Unparseable strings sort lowest so they always trigger an update.
    """
    v = v.lstrip("v")
    m = re.match(r"^(\d+)\.(\d+)\.(\d+)(?:\.(dev)(\d+)|(a)(\d+))?$", v)
    if not m:
        return (0, 0, 0, -99, 0)
    major, minor, patch = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if m.group(4):  # .devN
        return (major, minor, patch, -2, int(m.group(5)))
    if m.group(6):  # aN
        return (major, minor, patch, -1, int(m.group(7)))
    return (major, minor, patch, 0, 0)


def fetch_pypi_latest_version(*, timeout: float = _HTTP_TIMEOUT) -> str:
    """Return the latest installable mock-data-wizard version from PyPI."""
    req = urllib.request.Request(
        PYPI_JSON_URL,
        headers={"Accept": "application/json", "User-Agent": "mock-data-wizard"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = json.loads(resp.read())
    version = data.get("info", {}).get("version")
    if not version:
        raise RuntimeError(f"PyPI response missing info.version at {PYPI_JSON_URL}")
    return version


class UpdateChecker:
    """Non-blocking PyPI version check running on a background daemon thread.

    The cache file is only written when the result is consumed via
    ``get_newer_version()``, so a timed-out or failed check never poisons
    the cache and will be retried on the next invocation.
    """

    def __init__(self, *, http_timeout: float = _HTTP_TIMEOUT) -> None:
        self._result: str | None = None
        self._checked = False
        self._from_cache = False
        self._http_timeout = http_timeout
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        cache_path = _check_cache_path()
        now = time.time()

        if cache_path.exists():
            try:
                cache = json.loads(cache_path.read_text())
                if now - cache.get("timestamp", 0) < _UPDATE_CHECK_INTERVAL:
                    cached = cache.get("latest_version")
                    self._result = (
                        cached
                        if cached and parse_version(cached) > parse_version(__version__)
                        else None
                    )
                    self._checked = True
                    self._from_cache = True
                    return
            except (json.JSONDecodeError, OSError):
                pass

        try:
            latest = fetch_pypi_latest_version(timeout=self._http_timeout)
            self._result = (
                latest if parse_version(latest) > parse_version(__version__) else None
            )
            self._checked = True
        except Exception:
            pass

    @property
    def completed(self) -> bool:
        """Whether the check completed (vs timed out or errored)."""
        return self._checked

    def get_newer_version(self, *, timeout: float = _BG_JOIN_TIMEOUT) -> str | None:
        """Wait briefly for the check; return newer version or None."""
        self._thread.join(timeout=timeout)
        if not self._checked:
            return None
        if not self._from_cache:
            try:
                path = _check_cache_path()
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(
                    json.dumps(
                        {
                            "timestamp": time.time(),
                            "latest_version": self._result or __version__,
                        }
                    )
                )
            except OSError:
                pass
        return self._result


def _clear_check_cache() -> None:
    _check_cache_path().unlink(missing_ok=True)


def run_update() -> int:
    """Upgrade mock-data-wizard via `uv tool upgrade`. Returns exit code.

    No-op when already on the latest version. Shells out to ``uv`` rather
    than touching ``pip`` directly so the install stays managed by the
    same tool that put it there.
    """
    current = __version__
    try:
        latest = fetch_pypi_latest_version()
    except urllib.error.URLError as exc:
        sys.stderr.write(
            f"Error: failed to resolve latest version from PyPI: {exc.reason}\n"
            "Check your internet connection and retry.\n"
        )
        return EXIT_NETWORK
    except Exception as exc:
        sys.stderr.write(f"Error: PyPI lookup failed: {exc}\n")
        return EXIT_NETWORK

    current_parsed = parse_version(current)
    latest_parsed = parse_version(latest)
    if current_parsed >= latest_parsed:
        if current_parsed > latest_parsed:
            sys.stderr.write(
                f"Package v{current} is ahead of latest release v{latest}.\n"
            )
        sys.stderr.write(f"Already up to date (v{current}).\n")
        _clear_check_cache()
        return 0

    sys.stderr.write(f"Upgrading package: v{current} → v{latest}\n")
    try:
        proc = subprocess.run(
            ["uv", "tool", "upgrade", "mock-data-wizard"],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        sys.stderr.write(
            "Error: uv is not installed or not on PATH.\n"
            "Install uv (https://docs.astral.sh/uv/) and retry.\n"
        )
        return EXIT_CONFIG
    if proc.returncode != 0:
        sys.stderr.write(
            f"Error: uv tool upgrade failed: {proc.stderr.strip()}\n"
            "Check that mock-data-wizard was installed with "
            "`uv tool install mock-data-wizard`.\n"
        )
        return EXIT_CONFIG

    # uv exits 0 with "Nothing to upgrade" when the target version isn't yet
    # on the resolved index — avoid claiming a false success in that case.
    output = (proc.stdout or "") + (proc.stderr or "")
    if "Nothing to upgrade" in output:
        sys.stderr.write(
            f"  uv reported nothing to upgrade. v{latest} may not be on the "
            "configured index yet — try again shortly.\n"
        )
    else:
        sys.stderr.write(f"  Package upgraded to v{latest}.\n")

    _clear_check_cache()
    return 0
