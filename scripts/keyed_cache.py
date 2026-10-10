"""Content digests and a keyed on-disk store, shared by the repository's caches.

Three caches use it: `scripts/real_seed_cache.py` (real-seed prepare and build
outputs), `conformance/differential/cache.py` (the G1 derived copies) and
`conformance/reader_artifacts.py` (the synthetic fixture artifacts). It is stdlib
only and lives with the tooling, so `uv run --no-project` scripts import it directly;
the test-side caches put `scripts/` on `sys.path` to reach it. Tooling never imports
test code, so the shared pieces live here and not in a test module.

An entry is a directory named by its key. It is built in a private staging directory
beside it and renamed into place whole (`staged`), so a reader never sees a partial
entry and concurrent builders of one key keep the first rename.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import sys
import tempfile
import time
import tomllib
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

NATIVE_PROJECT = "crates/reg-core-py"


def _native_sources() -> tuple[str, ...]:
    """`NATIVE_PROJECT`'s uv `cache-keys` as repo-relative paths, in their order.

    Each entry must be a `file` key naming one path or a whole directory (`<dir>/**`);
    any other key or glob is refused rather than dropped, since a dropped source would
    go unkeyed.
    """
    repo = Path(__file__).resolve().parents[1]
    project = repo / NATIVE_PROJECT
    keys = tomllib.loads((project / "pyproject.toml").read_text())["tool"]["uv"][
        "cache-keys"
    ]
    sources = []
    for key in keys:
        base = (
            key["file"].removesuffix("**").removesuffix("/")
            if isinstance(key, dict) and key.keys() == {"file"}
            else None
        )
        if base is None or any(char in base for char in "*?[{"):
            raise RuntimeError(
                f"{NATIVE_PROJECT}/pyproject.toml cache-key {key!r} is not a path or "
                "`<dir>/**`; extend keyed_cache._native_sources to key it"
            )
        path = Path(os.path.normpath(project / base))
        sources.append(path.relative_to(repo).as_posix())
    return tuple(sources)


# The sources `reg-core-py` is built from, read from its uv `cache-keys`: `uv run`
# rebuilds the extension when they change, and so must every cache of builder output.
NATIVE_SOURCES = _native_sources()
# What builder output depends on: the builder, the locked dependencies and the
# native extension. Repo-relative, in key order.
BUILDER_SOURCES = ("reg_meta_build/src", "uv.lock", *NATIVE_SOURCES)
STAGING_PREFIX = ".staging-"
# A staging directory older than this belongs to a build that died.
STAGING_RETENTION_SECONDS = 6 * 3600


def cache_home(name: str, env: str) -> Path:
    """`$<env>`, else `$XDG_CACHE_HOME/<name>`, else `~/.cache/<name>`."""
    if configured := os.environ.get(env):
        return Path(configured).expanduser().resolve()
    base = os.environ.get("XDG_CACHE_HOME") or str(Path.home() / ".cache")
    return Path(base).expanduser().resolve() / name


def owned_root(root: Path, marker: str) -> Path:
    """Create `root` with its `marker` file, or adopt one that already has it.

    Eviction deletes directories under the root, so never adopt an existing
    non-empty directory that is not already this cache (a mistyped env override).
    """
    if root.is_dir() and any(root.iterdir()) and not (root / marker).exists():
        raise RuntimeError(f"{root} is not this cache (no {marker}); pick an empty dir")
    root.mkdir(parents=True, exist_ok=True)
    (root / marker).touch()
    return root


def tree_digest(root: Path, files: Iterable[Path] | None = None) -> str:
    """Content digest of a file or directory, independent of where it lives.

    `files`, the files under `root` to hash, replaces the default walk of every file
    on disk but byte-code.
    """
    if files is None:
        files = (
            [root]
            if root.is_file()
            else (
                path
                for path in root.rglob("*")
                if path.is_file()
                and "__pycache__" not in path.parts
                and path.suffix != ".pyc"
            )
        )
    digest = hashlib.sha256()
    for path in sorted(files):
        digest.update(path.relative_to(root).as_posix().encode() + b"\0")
        with path.open("rb") as handle:
            digest.update(hashlib.file_digest(handle, "sha256").digest())
    return digest.hexdigest()


def file_sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


@contextmanager
def staged(entry: Path) -> Iterator[Path]:
    """Yield a private staging directory beside `entry`; rename it to `entry` when
    the block completes.

    If the block raises, nothing is published. If another builder of the same key
    renamed first, its entry is kept and this copy is discarded.
    """
    entry.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=STAGING_PREFIX, dir=entry.parent))
    try:
        yield staging
        try:
            staging.rename(entry)
        except OSError:
            if not entry.is_dir():
                raise
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def evict(
    home: Path, current: Path, keep: int, label: str, *, min_idle: float = 0
) -> None:
    """Delete all but the `keep` most recently used entries under `home` (`current`
    always stays), and the staging directories of builds that died.

    An entry's mtime is its last use: callers `os.utime` an entry on every hit. An
    entry used within the last `min_idle` seconds also stays, so a path another
    session is still reading is not deleted under it.
    """
    mtimes = {}
    for p in home.iterdir():
        # A concurrent builder can rename its staging directory away meanwhile.
        with suppress(FileNotFoundError):
            mtimes[p] = p.stat().st_mtime
    keys = sorted(
        (p for p in mtimes if not p.name.startswith(STAGING_PREFIX)),
        key=mtimes.__getitem__,
        reverse=True,
    )
    now = time.time()
    cutoff = now - STAGING_RETENTION_SECONDS
    stale = [p for p in keys[keep:] if p != current and mtimes[p] < now - min_idle] + [
        p for p in mtimes if p.name.startswith(STAGING_PREFIX) and mtimes[p] < cutoff
    ]
    for path in stale:
        sys.stderr.write(f"{label}: removing {path}\n")
        shutil.rmtree(path, ignore_errors=True)
