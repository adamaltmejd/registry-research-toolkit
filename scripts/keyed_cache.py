"""Content digests and a keyed on-disk store, shared by the repository's caches.

Three caches use it: `scripts/real_seed_cache.py` (real-seed prepare and build
outputs), `conformance/differential/cache.py` (the G1 derived copies) and
`reg_meta/tests/reader_artifacts.py` (the synthetic fixture artifacts). It is stdlib
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
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

# The sources `reg-core-py` is built from (its uv `cache-keys`): `uv run` rebuilds the
# extension when they change, and so must every cache of builder output.
NATIVE_SOURCES = ("crates/reg-core", "crates/reg-core-py", "Cargo.toml", "Cargo.lock")
# What builder output depends on: the builder, the reader code it imports, the locked
# dependencies and the native extension. Repo-relative, in key order.
BUILDER_SOURCES = ("reg_meta_build/src", "reg_meta/src", "uv.lock", *NATIVE_SOURCES)
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


def tree_digest(root: Path) -> str:
    """Content digest of a file or directory, independent of where it lives."""
    files = (
        [root]
        if root.is_file()
        else sorted(
            path
            for path in root.rglob("*")
            if path.is_file()
            and "__pycache__" not in path.parts
            and path.suffix != ".pyc"
        )
    )
    digest = hashlib.sha256()
    for path in files:
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


def evict(home: Path, current: Path, keep: int, label: str) -> None:
    """Delete all but the `keep` most recently used entries under `home` (`current`
    always stays), and the staging directories of builds that died.

    An entry's mtime is its last use: callers `os.utime` an entry on every hit.
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
    cutoff = time.time() - STAGING_RETENTION_SECONDS
    stale = [p for p in keys[keep:] if p != current] + [
        p for p in mtimes if p.name.startswith(STAGING_PREFIX) and mtimes[p] < cutoff
    ]
    for path in stale:
        sys.stderr.write(f"{label}: removing {path}\n")
        shutil.rmtree(path, ignore_errors=True)
