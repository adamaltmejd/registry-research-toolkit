"""Self-update logic for regmeta package, main database, and doc DB."""

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
from .doc_db import DOC_DB_FILENAME, DOCS_SOURCE_FILE
from .download import (
    DB_SOURCE_FILE,
    download_db,
    download_docs_db,
    fetch_pypi_latest_version,
    resolve_latest_release,
    version_from_tag,
)
from .errors import EXIT_CONFIG, RegmetaError

_UPDATE_CHECK_INTERVAL = 7 * 24 * 3600  # 1 week in seconds
_BG_JOIN_TIMEOUT = 3  # max seconds to wait when collecting background check
_HTTP_TIMEOUT = 10  # per-socket timeout for the GitHub API call


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


def _update_available_path() -> Path:
    return default_db_dir() / ".update_available"


def read_pending_update() -> str | None:
    """Read the persistent update-available flag. Returns version or None."""
    try:
        return _update_available_path().read_text().strip() or None
    except OSError:
        return None


def _set_pending_update(version: str) -> None:
    path = _update_available_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(version)
    except OSError:
        pass


def _clear_pending_update() -> None:
    _update_available_path().unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Background update checker (launched at CLI startup, collected at exit)
# ---------------------------------------------------------------------------


class UpdateChecker:
    """Non-blocking package version checker running in a background daemon thread.

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

        # Stale or missing cache — hit the network. We ask PyPI (not GitHub
        # Releases) because PyPI is what `uv tool upgrade` actually installs
        # from; a GitHub tag can exist before PyPI has the matching wheel
        # while the publish workflow waits on environment approval.
        try:
            pypi_ver = fetch_pypi_latest_version(timeout=self._http_timeout)
            self._result = (
                pypi_ver
                if _parse_version(pypi_ver) > _parse_version(__version__)
                else None
            )
            self._checked = True
        except Exception:
            pass

    @property
    def completed(self) -> bool:
        """Whether the check completed (vs timed out or errored)."""
        return self._checked

    def get_newer_version(self, *, timeout: float = _BG_JOIN_TIMEOUT) -> str | None:
        """Wait briefly for the check to finish; return newer version or None."""
        self._thread.join(timeout=timeout)
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
        # Persist/clear the update-available flag so subsequent runs can
        # remind the user even if the background check times out.
        if self._result:
            _set_pending_update(self._result)
        else:
            _clear_pending_update()
        return self._result


# ---------------------------------------------------------------------------
# Explicit update command
# ---------------------------------------------------------------------------


def _read_source_tag(path: Path) -> str | None:
    """Read the release tag an artifact was downloaded from."""
    try:
        return json.loads(path.read_text()).get("tag")
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
    """Update regmeta package, main database, and doc DB to the latest release.

    Skips the package upgrade if already on the latest version. Each asset
    is fetched independently: the walker returns the most recent release
    carrying that asset, so a doc-less package release still serves the
    previous doc DB. Already-current assets are skipped unless *force*.
    """
    if db_dir is None:
        db_dir = default_db_dir()

    # Package version target comes from PyPI (what `uv tool upgrade` can
    # actually install); asset tags come from GitHub Releases (where the
    # artifacts are hosted). The two can diverge briefly when a release is
    # tagged before the gated PyPI publish runs — treating PyPI as the
    # upgrade target avoids a false-success loop where uv reports "Nothing
    # to upgrade" but we claim the upgrade happened.
    if tag == "latest":
        resolution = resolve_latest_release(timeout=10)
        db_tag = resolution.db_tag
        docs_tag = resolution.docs_tag
        try:
            latest_ver = fetch_pypi_latest_version(timeout=10)
        except RegmetaError:
            # PyPI unreachable — fall back to the GitHub tag so an offline
            # operator can still refresh assets. The post-upgrade version
            # check below catches the no-op case either way.
            latest_ver = resolution.version
    else:
        latest_ver = version_from_tag(tag)
        db_tag = tag  # assume explicit tag has both assets
        docs_tag = tag

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
        # uv exits 0 with "Nothing to upgrade" when the target version isn't
        # yet on the resolved index — don't print a false "Package upgraded"
        # line in that case.
        uv_output = (proc.stdout or "") + (proc.stderr or "")
        if "Nothing to upgrade" in uv_output:
            sys.stderr.write(
                f"  uv reported nothing to upgrade. v{latest_ver} may not be "
                f"on the configured index yet — try again shortly.\n"
            )
            result["package"] = "no_upgrade"
        else:
            sys.stderr.write(f"  Package upgraded to v{latest_ver}.\n")
            result["package"] = {"old_version": current, "new_version": latest_ver}

    # --- Main database ---
    db_path = db_dir / DB_FILENAME
    local_db_tag = _read_source_tag(db_dir / DB_SOURCE_FILE)
    need_db = not db_path.exists() or force or (db_tag and local_db_tag != db_tag)
    if need_db and db_tag:
        sys.stderr.write("Updating main database...\n")
        db_result = download_db(
            db_dir=db_dir, tag=db_tag, force=db_path.exists(), yes=yes
        )
        result["database"] = db_result
    elif need_db and not db_tag:
        # Walker found nothing AND the user has no usable local copy (or
        # --force was set). Returning success here would leave the install
        # broken — every query command would then fail with db_not_found.
        reason = "No recent release includes a main-DB asset required for this update."
        if force and db_path.exists():
            reason += " (--force requires a fresh asset; none was found.)"
        raise RegmetaError(
            exit_code=EXIT_CONFIG,
            code="no_db_in_release",
            error_class="configuration",
            message=reason,
            remediation=(
                "Build from CSV with `regmeta maintain build-db`, "
                "or check https://github.com/adamaltmejd/registry-research-toolkit/releases"
            ),
        )
    elif not db_tag:
        result["database"] = "no_db_in_release"
    else:
        result["database"] = "up_to_date"

    # --- Doc DB ---
    docs_path = db_dir / DOC_DB_FILENAME
    local_docs_tag = _read_source_tag(db_dir / DOCS_SOURCE_FILE)
    need_docs = (
        not docs_path.exists() or force or (docs_tag and local_docs_tag != docs_tag)
    )
    if need_docs and docs_tag:
        sys.stderr.write("Updating doc DB...\n")
        docs_result = download_docs_db(
            db_dir=db_dir, tag=docs_tag, force=docs_path.exists()
        )
        result["docs"] = docs_result
    elif need_docs and not docs_tag:
        # Symmetric with the main-DB case: fail fast rather than leave the
        # user with a broken install. Query commands require the doc DB.
        reason = "No recent release includes a doc-DB asset required for this update."
        if force and docs_path.exists():
            reason += " (--force requires a fresh asset; none was found.)"
        raise RegmetaError(
            exit_code=EXIT_CONFIG,
            code="no_docs_in_release",
            error_class="configuration",
            message=reason,
            remediation=(
                "Build from markdown with `regmeta maintain build-docs`, "
                "or check https://github.com/adamaltmejd/registry-research-toolkit/releases"
            ),
        )
    elif not docs_tag:
        result["docs"] = "no_docs_in_release"
    else:
        result["docs"] = "up_to_date"

    if (
        result["package"] == "up_to_date"
        and result["database"] == "up_to_date"
        and result["docs"] == "up_to_date"
    ):
        sys.stderr.write(f"Already up to date (v{current}).\n")

    _clear_check_cache()
    _clear_pending_update()
    return result
