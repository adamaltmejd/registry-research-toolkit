"""Conformance fixtures, built by `reg-build` from readable sources.

A fixture is a directory `conformance/cases/fixtures/<name>/` with `inputs/` (frame
JSONL and whatever else its readers read) and `curation/` (`inputs.toml` first). It is
built with `target/debug/reg-build build`, which `scripts/gate.py rust` has compiled;
the harness never runs cargo. `reg-build build` verifies what it writes.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Generator

CASES = Path(__file__).resolve().parent / "cases"
REPO = CASES.parents[1]
# `REG_BUILD_BIN` points the harness at another build of the same binary: artifact
# conformance (integration.yml) runs `reg-build verify` on a real catalog with the
# release build.
REG_BUILD = Path(os.environ.get("REG_BUILD_BIN") or REPO / "target/debug/reg-build")

FIXTURE_CACHE_ENV = "REG_FIXTURE_CACHE"
# A generation idle this long is pruned when a new one is created. Every lookup
# marks its generation as used, so a returned path stays valid this long after
# its last lookup.
FIXTURE_CACHE_RETENTION_SECONDS = 6 * 3600


def tree_digest(root: Path) -> str:
    """Content digest of a file or directory, independent of where it lives: every
    file's relative path and bytes, byte-code excluded."""
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


@contextmanager
def staged(entry: Path) -> Generator[Path]:
    """Yield a private staging directory beside `entry`; rename it to `entry` when
    the block completes, so a reader never sees a partial entry.

    If the block raises, nothing is published. If another builder of the same key
    renamed first, its entry is kept and this copy is discarded.
    """
    entry.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".staging-", dir=entry.parent))
    try:
        yield staging
        try:
            staging.rename(entry)
        except OSError:
            if not entry.is_dir():
                raise
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def fixture_source(fixture: str | Path) -> Path:
    """The directory a fixture name (under `cases/fixtures`) or path builds from."""
    return fixture if isinstance(fixture, Path) else CASES / "fixtures" / fixture


def fixture_cache_dir() -> Path:
    """Artifact cache shared by worktrees and pytest-xdist workers.

    The temp directory is writable inside agent sandboxes and cleared on reboot.
    """
    configured = os.environ.get(FIXTURE_CACHE_ENV)
    if configured:
        return Path(configured).expanduser()
    return Path(tempfile.gettempdir()) / "registry-research-toolkit-fixtures"


def build_inputs_digest(binary: Path = REG_BUILD) -> str:
    """Digest of the one fixture-independent build input: the `reg-build` binary."""
    if not binary.is_file():
        raise FileNotFoundError(
            f"{binary} is not built: run `cargo build -p reg-build`"
        )
    return tree_digest(binary)


def artifact_key(fixture: str | Path) -> str:
    """Cache key of one fixture: the file paths and bytes of its tree."""
    return tree_digest(fixture_source(fixture))


def generation_dir(build_inputs: str) -> Path:
    """The cache directory of the generation keyed `build_inputs`, marked as in use."""
    generations = fixture_cache_dir() / "generations"
    generation = generations / build_inputs
    if not generation.is_dir():
        generation.mkdir(parents=True, exist_ok=True)
        # simplify: every rebuilt binary starts a generation (~1 MB per fixture), so
        # ones idle for 6 hours are pruned; switch to size-based eviction if that
        # still grows. Pruning only ever touches `generations/`, never the cache's
        # parent.
        cutoff = time.time() - FIXTURE_CACHE_RETENTION_SECONDS
        for stale in generations.iterdir():
            if stale.is_dir() and stale.stat().st_mtime < cutoff:
                shutil.rmtree(stale, ignore_errors=True)
    os.utime(generation)
    return generation


def cached_reader_artifact(fixture: str | Path) -> Path:
    """Return the cached, read-only `reg_meta.db` of a fixture, building it on first use.

    Entries are immutable: a caller that mutates an artifact copies it. A miss builds into a private staging directory and renames it
    into place, so concurrent builders of the same key never expose a partial entry;
    the loser discards its copy.
    """
    entry = generation_dir(build_inputs_digest()) / artifact_key(fixture)
    path = entry / "reg_meta.db"
    if path.exists():
        return path
    source = fixture_source(fixture)
    if not (source / "curation/inputs.toml").is_file():
        raise FileNotFoundError(
            f"{source} is not a reg-build fixture (curation/inputs.toml)"
        )
    with staged(entry) as staging:
        built = subprocess.run(
            [
                REG_BUILD,
                "build",
                "--inputs",
                source / "inputs",
                "--curation",
                source / "curation",
                "--out",
                staging / "reg_meta.db",
                "--report",
                staging / "report.json",
            ],
            cwd=REPO,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            check=False,
        )
        if built.returncode:
            raise RuntimeError(f"reg-build build {source.name}: {built.stderr.strip()}")
        (staging / "reg_meta.db").chmod(0o444)
    return path
