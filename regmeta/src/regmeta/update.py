"""Self-update logic for regmeta package and database."""

from __future__ import annotations

import json
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from . import __version__
from .db import DB_FILENAME, default_db_dir
from .download import DB_ASSET_NAME, GITHUB_REPO, RELEASES_API_URL, download_db
from .errors import EXIT_CONFIG, EXIT_NETWORK, RegmetaError

UPDATE_CHECK_INTERVAL = 7 * 24 * 3600  # 1 week in seconds
_UPDATE_CHECK_TIMEOUT = 3  # max seconds to wait for the background thread

# Written by `download_db` so `maintain update` can tell which release the
# local database came from.
DB_SOURCE_FILE = ".db_source"


def _check_cache_path() -> Path:
    return default_db_dir() / ".update_check"


def _resolve_latest_release(
    *, timeout: float = _UPDATE_CHECK_TIMEOUT
) -> tuple[str, str, bool]:
    """Return (raw_tag, version, has_db_asset) from the latest GitHub release.

    *raw_tag* is the literal tag string (e.g. ``"v0.5.0"``).
    *version* strips a leading ``v`` for comparison with ``__version__``.
    *has_db_asset* is True when the release includes ``regmeta.db.zst``.
    """
    req = urllib.request.Request(
        RELEASES_API_URL + "?per_page=1",
        headers={"Accept": "application/vnd.github+json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            releases = json.loads(resp.read())
            if not releases:
                raise RegmetaError(
                    exit_code=EXIT_NETWORK,
                    code="no_releases",
                    error_class="network",
                    message="No releases found.",
                    remediation=f"Check https://github.com/{GITHUB_REPO}/releases",
                )
            release = releases[0]
            tag = release["tag_name"]
            has_db = any(a["name"] == DB_ASSET_NAME for a in release.get("assets", []))
            return tag, tag.lstrip("v"), has_db
    except (urllib.error.URLError, KeyError, json.JSONDecodeError) as exc:
        raise RegmetaError(
            exit_code=EXIT_NETWORK,
            code="release_lookup_failed",
            error_class="network",
            message=f"Failed to check for updates: {exc}",
            remediation="Check your internet connection.",
        ) from exc


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
                if now - cache.get("timestamp", 0) < UPDATE_CHECK_INTERVAL:
                    cached_ver = cache.get("latest_version")
                    self._result = (
                        cached_ver if cached_ver and cached_ver != __version__ else None
                    )
                    self._checked = True
                    self._from_cache = True
                    return
            except (json.JSONDecodeError, OSError):
                pass

        # Stale or missing cache — hit the network
        try:
            _tag, version, _has_db = _resolve_latest_release()
            self._result = version if version != __version__ else None
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


def _read_db_source_tag() -> str | None:
    """Read the release tag the local database was downloaded from."""
    path = default_db_dir() / DB_SOURCE_FILE
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text()).get("tag")
    except (json.JSONDecodeError, OSError):
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
    Skips the database download if already on the latest release tag
    (unless *force* is True or the database does not exist).
    """
    if db_dir is None:
        db_dir = default_db_dir()

    # Resolve the target release
    if tag == "latest":
        release_tag, latest_ver, has_db = _resolve_latest_release(timeout=10)
    else:
        release_tag = tag
        latest_ver = tag.lstrip("v")
        has_db = True  # assume explicit tag has a db

    result: dict[str, Any] = {}

    # --- Package upgrade ---
    current = __version__
    if latest_ver == current:
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
    local_tag = _read_db_source_tag()
    need_db = not db_path.exists() or force or (has_db and local_tag != release_tag)
    if need_db and has_db:
        sys.stderr.write("Updating database...\n")
        db_result = download_db(
            db_dir=db_dir, tag=release_tag, force=db_path.exists(), yes=yes
        )
        result["database"] = db_result
    elif not has_db:
        result["database"] = "no_db_in_release"
    else:
        result["database"] = "up_to_date"

    if result["package"] == "up_to_date" and result["database"] == "up_to_date":
        sys.stderr.write(f"Already up to date (v{current}).\n")

    _clear_check_cache()
    return result
