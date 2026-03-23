"""Download pre-built regmeta database from GitHub Releases."""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import zstandard

from .db import DB_FILENAME, default_db_dir
from .errors import EXIT_CONFIG, EXIT_NETWORK, RegmetaError

GITHUB_REPO = "adamaltmejd/registry-research-toolkit"
DB_ASSET_NAME = "regmeta.db.zst"
RELEASES_API_URL = f"https://api.github.com/repos/{GITHUB_REPO}/releases"
DOWNLOAD_URL = (
    f"https://github.com/{GITHUB_REPO}/releases/download/{{tag}}/{DB_ASSET_NAME}"
)


def _resolve_latest_tag() -> str:
    """Find the most recent release tag (includes pre-releases)."""
    req = urllib.request.Request(
        RELEASES_API_URL + "?per_page=1",
        headers={"Accept": "application/vnd.github+json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            releases = json.loads(resp.read())
            if not releases:
                raise RegmetaError(
                    exit_code=EXIT_NETWORK,
                    code="no_releases",
                    error_class="network",
                    message="No releases found.",
                    remediation=f"Check https://github.com/{GITHUB_REPO}/releases",
                )
            return releases[0]["tag_name"]
    except (urllib.error.URLError, KeyError, json.JSONDecodeError) as exc:
        raise RegmetaError(
            exit_code=EXIT_NETWORK,
            code="release_lookup_failed",
            error_class="network",
            message=f"Failed to resolve latest release: {exc}",
            remediation="Check your internet connection, or specify --tag explicitly.",
        ) from exc


def _fmt_size(n: int) -> str:
    if n >= 1024 * 1024 * 1024:
        return f"{n / (1024**3):.1f} GB"
    return f"{n / (1024**2):.0f} MB"


def _progress(downloaded: int, total: int) -> None:
    if total <= 0:
        sys.stderr.write(f"\r  {_fmt_size(downloaded)} downloaded")
    else:
        pct = downloaded / total * 100
        bar_w = 30
        filled = int(bar_w * downloaded / total)
        bar = "\u2588" * filled + "\u2591" * (bar_w - filled)
        sys.stderr.write(
            f"\r  [{bar}] {pct:5.1f}%  {_fmt_size(downloaded)} / {_fmt_size(total)}"
        )
    sys.stderr.flush()


def _download_file(url: str, dest: Path) -> None:
    req = urllib.request.Request(url, headers={"User-Agent": "regmeta"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            total = int(resp.headers.get("Content-Length", 0))
            downloaded = 0
            with dest.open("wb") as f:
                while chunk := resp.read(65536):
                    f.write(chunk)
                    downloaded += len(chunk)
                    _progress(downloaded, total)
            sys.stderr.write("\n")
            sys.stderr.flush()
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            raise RegmetaError(
                exit_code=EXIT_CONFIG,
                code="release_not_found",
                error_class="configuration",
                message=f"Release asset not found: {url}",
                remediation="Check the --tag value. Available releases: "
                f"https://github.com/{GITHUB_REPO}/releases",
            ) from exc
        raise RegmetaError(
            exit_code=EXIT_NETWORK,
            code="download_failed",
            error_class="network",
            message=f"HTTP {exc.code}: {exc.reason}",
            remediation="Check your internet connection and try again.",
        ) from exc
    except urllib.error.URLError as exc:
        raise RegmetaError(
            exit_code=EXIT_NETWORK,
            code="download_failed",
            error_class="network",
            message=str(exc.reason),
            remediation="Check your internet connection and try again.",
        ) from exc


def _decompress(src: Path, dest: Path) -> None:
    sys.stderr.write("  Decompressing...\r")
    sys.stderr.flush()
    dctx = zstandard.ZstdDecompressor()
    with src.open("rb") as ifh, dest.open("wb") as ofh:
        dctx.copy_stream(ifh, ofh)
    sys.stderr.write("  Decompressing... done.\n")
    sys.stderr.flush()


def download_db(
    db_dir: Path | None = None,
    *,
    tag: str = "latest",
    force: bool = False,
    yes: bool = False,
) -> dict[str, Any]:
    """Download pre-built database from GitHub Releases.

    Returns dict with db_path, tag, and size_bytes.
    """
    if db_dir is None:
        db_dir = default_db_dir()
    final_path = db_dir / DB_FILENAME

    if final_path.exists() and not force:
        raise RegmetaError(
            exit_code=EXIT_CONFIG,
            code="db_exists",
            error_class="configuration",
            message=f"Database already exists: {final_path}",
            remediation="Use --force to overwrite.",
        )

    # Resolve tag
    resolved_tag = _resolve_latest_tag() if tag == "latest" else tag
    url = DOWNLOAD_URL.format(tag=resolved_tag)

    # Confirmation
    if not yes:
        sys.stderr.write(
            f"This will download ~400 MB and decompress to ~1.6 GB.\n"
            f"  Tag:         {resolved_tag}\n"
            f"  Destination: {final_path}\n"
            f"Continue? [y/N] "
        )
        sys.stderr.flush()
        answer = input().strip().lower()
        if answer not in ("y", "yes"):
            sys.stderr.write("Aborted.\n")
            return {"aborted": True}

    db_dir.mkdir(parents=True, exist_ok=True)
    tmp_zst = final_path.with_suffix(".db.zst.tmp")
    tmp_db = final_path.with_suffix(".db.tmp")

    try:
        sys.stderr.write(f"Downloading {resolved_tag}...\n")
        _download_file(url, tmp_zst)
        _decompress(tmp_zst, tmp_db)
        tmp_zst.unlink()

        if final_path.exists():
            final_path.unlink()
        tmp_db.rename(final_path)

        size = final_path.stat().st_size
        sys.stderr.write(f"Database ready: {final_path} ({_fmt_size(size)})\n")
        return {
            "db_path": str(final_path),
            "tag": resolved_tag,
            "size_bytes": size,
        }
    except Exception:
        for tmp in (tmp_zst, tmp_db):
            if tmp.exists():
                tmp.unlink()
        raise
