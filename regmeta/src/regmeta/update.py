"""Self-update logic for regmeta package and database."""

from __future__ import annotations

import json
import re
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

from . import __version__
from .db import DB_FILENAME, default_db_dir
from .download import (
    DB_SOURCE_FILE,
    version_from_tag,
    download_db,
    resolve_latest_release,
)
from .errors import EXIT_CONFIG, RegmetaError

_UPDATE_CHECK_INTERVAL = 7 * 24 * 3600  # 1 week in seconds
_UPDATE_CHECK_TIMEOUT = 3  # max seconds to wait for the background thread


def _parse_version(v: str) -> tuple[int, ...]:
    """Parse a PEP 440-ish version into a comparable tuple.

    Supports ``X.Y.Z``, ``X.Y.ZaN`` (alpha), and ``X.Y.Z.devN`` (dev).
    Ordering: ``0.4.0.dev1 < 0.4.0a1 < 0.4.0 < 0.5.0``.
    Unparseable strings sort lowest so they always trigger an update.
    """
    v = v.lstrip("v")
    m = re.match(
        r"^(\d+)\.(\d+)\.(\d+)"
        r"(?:\.(dev)(\d+)|(a)(\d+))?$",
        v,
    )
    if not m:
        return (0, 0, 0, -99, 0)
    major, minor, patch = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if m.group(4):  # .devN
        return (major, minor, patch, -2, int(m.group(5)))
    if m.group(6):  # aN
        return (major, minor, patch, -1, int(m.group(7)))
    return (major, minor, patch, 0, 0)  # final


def _check_cache_path() -> Path:
    return default_db_dir() / ".update_check"


# ---------------------------------------------------------------------------
# Background update checker (launched at CLI startup, collected at exit)
# ---------------------------------------------------------------------------


class UpdateChecker:
    """Non-blocking package version checker running in a background daemon thread.

    The cache file is only written when the result is consumed via
    ``get_newer_version()``, so a timed-out or failed check never poisons
    the cache and will be retried on the next invocation.
    """

    def __init__(self) -> None:
        self._result: str | None = None
        self._checked = False
        self._from_cache = False
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        cache_path = _check_cache_path()
        now = time.time()

        # Use cached result if still fresh
        if cache_path.exists():
            try:
                cache = json.loads(cache_path.read_text())
                if now - cache.get("timestamp", 0) < _UPDATE_CHECK_INTERVAL:
                    cached_ver = cache.get("latest_version")
                    self._result = (
                        cached_ver
                        if cached_ver
                        and _parse_version(cached_ver) > _parse_version(__version__)
                        else None
                    )
                    self._checked = True
                    self._from_cache = True
                    return
            except (json.JSONDecodeError, OSError):
                pass

        # Stale or missing cache — hit the network
        try:
            _tag, version, _db_tag = resolve_latest_release(
                timeout=_UPDATE_CHECK_TIMEOUT
            )
            self._result = (
                version
                if _parse_version(version) > _parse_version(__version__)
                else None
            )
            self._checked = True
        except Exception:
            pass

    def get_newer_version(self) -> str | None:
        """Wait briefly for the check to finish; return newer version or None."""
        self._thread.join(timeout=_UPDATE_CHECK_TIMEOUT)
        if not self._checked:
            return None  # timed out or errored — retry next time
        # Persist cache only when consuming a fresh (non-cached) result
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


# ---------------------------------------------------------------------------
# Explicit update command
# ---------------------------------------------------------------------------


def _read_db_source_tag(db_dir: Path) -> str | None:
    """Read the release tag the local database was downloaded from."""
    try:
        return json.loads((db_dir / DB_SOURCE_FILE).read_text()).get("tag")
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None


def _clear_check_cache() -> None:
    path = _check_cache_path()
    path.unlink(missing_ok=True)


def run_update(
    db_dir: Path | None = None,
    *,
    tag: str = "latest",
    force: bool = False,
    yes: bool = False,
) -> dict[str, Any]:
    """Update regmeta package and database to the latest release.

    Skips the package upgrade if already on the latest version.
    Walks recent releases to find the most recent one with a DB asset
    and skips the download if already on that tag (unless *force* is
    True or the database does not exist).
    """
    if db_dir is None:
        db_dir = default_db_dir()

    # Resolve the target release
    if tag == "latest":
        _release_tag, latest_ver, db_tag = resolve_latest_release(timeout=10)
    else:
        latest_ver = version_from_tag(tag)
        db_tag = tag  # assume explicit tag has a db

    result: dict[str, Any] = {}

    # --- Package upgrade ---
    current = __version__
    current_parsed = _parse_version(current)
    latest_parsed = _parse_version(latest_ver)
    if current_parsed >= latest_parsed:
        if current_parsed > latest_parsed:
            sys.stderr.write(
                f"Package v{current} is ahead of latest release v{latest_ver}.\n"
            )
        result["package"] = "up_to_date"
    else:
        sys.stderr.write(f"Upgrading package: v{current} → v{latest_ver}\n")
        try:
            proc = subprocess.run(
                ["uv", "tool", "upgrade", "regmeta"],
                capture_output=True,
                text=True,
            )
        except FileNotFoundError:
            raise RegmetaError(
                exit_code=EXIT_CONFIG,
                code="uv_not_found",
                error_class="configuration",
                message="uv is not installed or not on PATH.",
                remediation="Install uv (https://docs.astral.sh/uv/) and retry.",
            )
        if proc.returncode != 0:
            raise RegmetaError(
                exit_code=EXIT_CONFIG,
                code="upgrade_failed",
                error_class="configuration",
                message=f"uv tool upgrade failed: {proc.stderr.strip()}",
                remediation=(
                    "Check that regmeta was installed with `uv tool install regmeta`."
                ),
            )
        sys.stderr.write(f"  Package upgraded to v{latest_ver}.\n")
        result["package"] = {"old_version": current, "new_version": latest_ver}

    # --- Database download ---
    db_path = db_dir / DB_FILENAME
    local_tag = _read_db_source_tag(db_dir)
    need_db = not db_path.exists() or force or (db_tag and local_tag != db_tag)
    if need_db and db_tag:
        sys.stderr.write("Updating database...\n")
        db_result = download_db(
            db_dir=db_dir, tag=db_tag, force=db_path.exists(), yes=yes
        )
        result["database"] = db_result
    elif not db_tag:
        result["database"] = "no_db_in_releases"
    else:
        result["database"] = "up_to_date"

    if result["package"] == "up_to_date" and result["database"] == "up_to_date":
        sys.stderr.write(f"Already up to date (v{current}).\n")

    _clear_check_cache()
    return result
