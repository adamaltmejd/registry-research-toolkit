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
import json
import os
import runpy
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

_REPO = Path(__file__).resolve().parents[1]
# The sources `reg-core-py` is built from, read from its uv `cache-keys`: `uv run`
# rebuilds the extension when they change, and so must every cache of builder output.
# The builder's source-record pin reads the same list; the file is loaded by path, as
# this module runs without the project environment. Documentation under them
# (`native_doc`) is never keyed.
_NATIVE = runpy.run_path(
    str(_REPO / "reg_meta_build/src/reg_meta_build/native_sources.py")
)
NATIVE_SOURCES = _NATIVE["native_sources"](_REPO)


def native_doc(path: str) -> bool:
    """Whether the repo-relative file `path` is documentation under `NATIVE_SOURCES`."""
    return _NATIVE["native_doc"](path, NATIVE_SOURCES)


# What builder output depends on, besides the locked runtime dependencies
# (`uv_lock_sha256`): the builder and the native extension. Repo-relative, in key
# order.
BUILDER_SOURCES = ("reg_meta_build/src", *NATIVE_SOURCES)
# The `uv.lock` package whose runtime closure every cache of builder output keys.
BUILDER_PACKAGE = "reg-meta-build"


def uv_runtime_closure(lock: dict) -> list[dict]:
    """The `uv.lock` packages the builder loads: the transitive `dependencies` of
    `BUILDER_PACKAGE`, with an edge's `extra` followed into the target's
    `optional-dependencies`. Never `dev-dependencies`: pytest, ruff and the other dev
    tools are installed beside the builder but never imported by it.

    Generous where the lock is ambiguous: an edge reaches every locked version of its
    target and ignores its `marker`, so a spurious miss is possible, a stale hit not.
    """
    tables: dict[str, list[dict]] = {}
    for package in lock["package"]:
        tables.setdefault(package["name"], []).append(package)
    reached: set[tuple[str, str | None]] = set()
    pending: list[tuple[str, str | None]] = [(BUILDER_PACKAGE, None)]
    while pending:
        name, extra = pending.pop()
        if (name, extra) in reached:
            continue
        reached.add((name, extra))
        if name not in tables:
            raise RuntimeError(f"uv.lock has no package {name!r}")
        for package in tables[name]:
            edges = (
                package.get("dependencies", [])
                if extra is None
                else package.get("optional-dependencies", {}).get(extra, [])
            )
            for edge in edges:
                pending.append((edge["name"], None))
                pending.extend((edge["name"], e) for e in edge.get("extra", ()))
    names = {name for name, _ in reached}
    return [p for p in lock["package"] if p["name"] in names]


def uv_lock_sha256(text: str) -> str:
    """The `uv.lock` text as every cache of builder output keys it: only the
    builder's runtime closure (`uv_runtime_closure`), and the workspace members
    without their own versions, which a release bumps and nothing built reads.

    Each closure package is held by name, version, `source` and artifact hashes
    (`sdist`, `wheels`), so a re-published artifact moves the key; its
    `dependencies` and `metadata` are left out, since the closure's own pins already
    fix what loads. A dev-only pin moves no key.
    """
    lock = tomllib.loads(text)
    members = set(lock["manifest"]["members"])
    held = ("name", "version", "source", "sdist", "wheels")
    packages = [
        {
            field: package[field]
            for field in held
            if field in package
            and not (field == "version" and package["name"] in members)
        }
        for package in uv_runtime_closure(lock)
    ]
    packages.sort(key=lambda p: json.dumps(p, sort_keys=True))
    return hashlib.sha256(
        json.dumps(
            {"package": packages}, sort_keys=True, separators=(",", ":")
        ).encode()
    ).hexdigest()


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
