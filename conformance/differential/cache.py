"""The shared G1 cache: pinned release artifacts, their candidate copies, and the
baseline environment.

Layout under the cache root (``$REG_META_G1_CACHE``, else
``$XDG_CACHE_HOME/reg-meta-g1``, else ``~/.cache/reg-meta-g1``)::

    artifacts/<tag>/global/reg_meta.db       + reg_meta_docs.db
    artifacts/<tag>/swecov/reg_meta.db       + reg_meta_docs.db -> ../global/...
    derived/<key>/global/reg_meta.db         + reg_meta_docs.db (indexed copy)
    derived/<key>/swecov/reg_meta.db         + reg_meta_docs.db -> ../global/...
    baseline/<commit>/src/                   detached worktree of the commit + .venv

Each asset is streamed once: hashed against its pinned SHA-256 while it is
decompressed, so no ``.zst`` is kept. A stamp beside the database records the
verified asset digest and the file's size and mtime; a later run refetches when they
no longer match (readers open the files immutable, so they never change).

The baseline commit is the pinned release's reader: one detached worktree with its own
locked environment (``uv sync``, where maturin builds ``reg-core-py``). It reads the
release originals. A derived (candidate) copy is the checkout's ``reg-meta-build
derive`` of an original; the checkout reads it. Its directory's ``<key>`` hashes the
pinned asset digests and the derive source tree, so checkouts with different
builders keep separate copies instead of re-deriving over one another's, and a
finished set is renamed into place whole. The most recently used ``KEEP`` keys stay.
Every other artifact or baseline that is not pinned is deleted, so the cache never
holds more than the pinned set. Callers hold ``locked`` for a whole run, which
serializes runs from concurrent worktrees (they share the report directory and the
CPU budget).
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.request
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

REPO = "adamaltmejd/registry-research-toolkit"
DOWNLOAD_URL = f"https://github.com/{REPO}/releases/download/{{tag}}/{{asset}}"
DB_FILENAME = "reg_meta.db"
DOC_DB_FILENAME = "reg_meta_docs.db"
STAMP_SUFFIX = ".pin.json"
CHUNK = 1 << 20
# Derived keys kept: two branches alternating G1 runs re-derive neither (a key is
# ~2.6 GB of the tight disk).
KEEP = 2
STAGING_PREFIX = ".staging-"
# A staging directory older than this belongs to a build that died (a derive takes
# minutes).
STAGING_RETENTION_SECONDS = 6 * 3600
REPO_ROOT = Path(__file__).resolve().parents[2]
# What a derived copy depends on: the builder, the reader code derive moved, the
# locked dependencies, and the Rust sources of `reg-core-py` (its uv `cache-keys`).
DERIVE_SOURCES = (
    "reg_meta_build/src",
    "reg_meta/src",
    "uv.lock",
    "crates/reg-core",
    "crates/reg-core-py",
    "Cargo.toml",
    "Cargo.lock",
)


@dataclass(frozen=True)
class Asset:
    name: str
    sha256: str


@dataclass(frozen=True)
class Pins:
    baseline_commit: str
    tag: str
    catalogs: dict[str, Asset]
    docs: Asset

    @classmethod
    def from_config(cls, config: dict) -> Pins:
        release = config["release"]
        return cls(
            baseline_commit=config["baseline"]["commit"],
            tag=release["tag"],
            catalogs={
                a["catalog"]: Asset(a["name"], a["sha256"]) for a in release["asset"]
            },
            docs=Asset(release["docs"]["name"], release["docs"]["sha256"]),
        )


def cache_root() -> Path:
    if env := os.environ.get("REG_META_G1_CACHE"):
        return Path(env).expanduser().resolve()
    base = os.environ.get("XDG_CACHE_HOME") or str(Path.home() / ".cache")
    return Path(base).expanduser().resolve() / "reg-meta-g1"


def _tag_dir(tag: str) -> str:
    return tag.replace("/", "-")


@contextmanager
def locked(root: Path) -> Iterator[None]:
    # Runs prune `artifacts/`, `baseline/` and `report/` under the root, so never
    # adopt an existing non-empty directory that is not already a G1 cache.
    if root.is_dir() and any(root.iterdir()) and not (root / ".lock").exists():
        raise RuntimeError(f"{root} is not a G1 cache (no .lock); pick an empty dir")
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".lock").open("w") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _stamp_path(path: Path) -> Path:
    return path.with_name(path.name + STAMP_SUFFIX)


def _stamp_ok(path: Path, asset: Asset) -> bool:
    stamp = _stamp_path(path)
    if not path.is_file() or not stamp.is_file():
        return False
    recorded = json.loads(stamp.read_text())
    st = path.stat()
    return (
        recorded.get("asset_sha256") == asset.sha256
        and recorded.get("size") == st.st_size
        and recorded.get("mtime_ns") == st.st_mtime_ns
    )


def _download(tag: str, asset: Asset, dest: Path) -> None:
    """Stream, hash and decompress one asset; publish ``dest`` only if it verifies."""
    import zstandard

    url = DOWNLOAD_URL.format(tag=tag, asset=asset.name)
    sys.stderr.write(f"g1: fetching {url}\n")
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".tmp")
    asset_digest = hashlib.sha256()
    try:
        with (
            urllib.request.urlopen(url, timeout=60) as response,
            tmp.open("wb") as out,
        ):
            decompressor = zstandard.ZstdDecompressor().decompressobj()
            while chunk := response.read(CHUNK):
                asset_digest.update(chunk)
                data = decompressor.decompress(chunk)
                out.write(data)
        if asset_digest.hexdigest() != asset.sha256:
            raise RuntimeError(
                f"{asset.name} from {tag} has SHA-256 {asset_digest.hexdigest()}, "
                f"pinned {asset.sha256}"
            )
        tmp.replace(dest)
    finally:
        tmp.unlink(missing_ok=True)
    _write_stamp(dest, asset)


def _write_stamp(dest: Path, asset: Asset) -> None:
    st = dest.stat()
    _stamp_path(dest).write_text(
        json.dumps(
            {
                "asset": asset.name,
                "asset_sha256": asset.sha256,
                "size": st.st_size,
                "mtime_ns": st.st_mtime_ns,
            },
            indent=2,
        )
        + "\n"
    )


def _prune(parent: Path, keep: set[str]) -> None:
    if not parent.is_dir():
        return
    for child in parent.iterdir():
        if child.name not in keep:
            sys.stderr.write(f"g1: removing unpinned {child}\n")
            if child.is_dir() and not child.is_symlink():
                shutil.rmtree(child)
            else:
                child.unlink()


def ensure_artifacts(pins: Pins) -> dict[str, Path]:
    """Return ``{catalog: directory}`` for the pinned artifacts, fetching as needed."""
    root = cache_root()
    _prune(root / "artifacts", {_tag_dir(pins.tag)})
    base = root / "artifacts" / _tag_dir(pins.tag)
    _prune(base, set(pins.catalogs))
    dirs: dict[str, Path] = {}
    docs: Path | None = None
    for catalog, asset in sorted(pins.catalogs.items()):
        directory = base / catalog
        # Candidate copies live under `derived/`, keyed by source tree.
        _prune(
            directory,
            {
                name + suffix
                for name in (DB_FILENAME, DOC_DB_FILENAME)
                for suffix in ("", STAMP_SUFFIX)
            },
        )
        db = directory / DB_FILENAME
        if not _stamp_ok(db, asset):
            _download(pins.tag, asset, db)
        if docs is None:
            docs = directory / DOC_DB_FILENAME
            if not _stamp_ok(docs, pins.docs):
                _download(pins.tag, pins.docs, docs)
        else:
            link = directory / DOC_DB_FILENAME
            target = Path("..") / docs.parent.name / DOC_DB_FILENAME
            if not link.is_symlink() or link.readlink() != target:
                link.unlink(missing_ok=True)
                link.symlink_to(target)
        dirs[catalog] = directory
    return dirs


def _repo(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=REPO_ROOT, capture_output=True, text=True, check=True
    ).stdout.strip()


def _file_sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def derive_source() -> str:
    """The checkout's derive inputs (``DERIVE_SOURCES``) as committed tree ids.

    Derive stamps the builder commit into each copy and refuses a tree with tracked
    changes, so G1 runs on a committed tree.
    """
    if _repo("status", "--porcelain", "--untracked-files=no"):
        raise RuntimeError("G1 derives with the committed builder; commit first")
    return _repo("rev-parse", *(f"HEAD:{path}" for path in DERIVE_SOURCES))


def ensure_derived(pins: Pins, dirs: dict[str, Path], source: str) -> dict[str, Path]:
    """Return ``{catalog: derived/<key>/<catalog>}``, each holding the checkout's
    derived copy of the catalog beside the candidate docs copy.

    ``key`` hashes the pinned assets and ``source`` (``derive_source``), so each
    source tree owns its directory and never writes another's. A missing key is
    built in a private staging directory and renamed into place, so no reader sees a
    partial copy and concurrent builders of one key keep the first rename. Each
    copy's stamp records its provenance: the ``builder_commit`` and SHA-256. The
    ``KEEP`` most recently used keys stay; older ones are deleted (disk is tight).
    """
    home = cache_root() / "derived"
    home.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha256(
        json.dumps(
            {
                "catalogs": {c: a.sha256 for c, a in sorted(pins.catalogs.items())},
                "docs": pins.docs.sha256,
                "source": source,
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()
    entry = home / key
    if not entry.is_dir():
        staging = Path(tempfile.mkdtemp(prefix=STAGING_PREFIX, dir=home))
        try:
            _derive(dirs, staging)
            try:
                staging.rename(entry)
            except OSError:
                # Another builder of this key renamed first; keep its copy.
                if not entry.is_dir():
                    raise
        finally:
            shutil.rmtree(staging, ignore_errors=True)
    os.utime(entry)
    _evict(home, entry)
    return {catalog: entry / catalog for catalog in dirs}


def _derive(dirs: dict[str, Path], staging: Path) -> None:
    """Derive every catalog into ``staging/<catalog>``, then the docs copy.

    Derive parallelizes only its resolver phase, so deriving the catalogs together
    overlaps their serial index and validation phases (the G1 budget).
    """
    started = time.monotonic()
    procs: dict[str, tuple[Path, subprocess.Popen]] = {}
    for catalog, directory in sorted(dirs.items()):
        out = staging / catalog / DB_FILENAME
        out.parent.mkdir()
        sys.stderr.write(f"g1: deriving the candidate copy of {catalog}\n")
        procs[catalog] = (
            out,
            subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "reg_meta_build.cli",
                    "derive",
                    "--base",
                    str(directory / DB_FILENAME),
                    "--out",
                    str(out),
                ],
                stdout=subprocess.PIPE,
                text=True,
                # A perturbation run's PYTHONPATH would derive from the perturbed tree
                # and cache it under the committed tree's key.
                env=isolated_env(),
            ),
        )
    failures = []
    for catalog, (out, proc) in procs.items():
        # Derive prints one JSON envelope, so reading the pipes in turn cannot block.
        stdout = proc.communicate()[0]
        if proc.returncode:
            failures.append(f"derive {catalog} failed: {stdout}")
            continue
        conn = sqlite3.connect(f"file:{out}?mode=ro&immutable=1", uri=True)
        try:
            (commit,) = conn.execute(
                "SELECT value FROM import_manifest WHERE key = 'builder_commit'"
            ).fetchone()
        finally:
            conn.close()
        _write_provenance(out, builder_commit=commit)
    if failures:
        raise RuntimeError("\n".join(failures))
    seconds = time.monotonic() - started
    sys.stderr.write(f"g1: derived the candidate copies in {seconds:.1f} s\n")
    _index_docs(dirs, staging)


def _index_docs(dirs: dict[str, Path], staging: Path) -> None:
    """The candidate docs copy: the checkout's docs build index step
    (``index_docs``) run over a copy of the pinned docs database, kept beside the
    first catalog and linked from the others'. The step takes well under a second,
    so it needs no derive path of its own.
    """
    sys.stderr.write("g1: indexing the candidate docs copy\n")
    first, *rest = sorted(dirs)
    out = staging / first / DOC_DB_FILENAME
    shutil.copyfile(dirs[first] / DOC_DB_FILENAME, out)
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sqlite3, sys; from reg_meta_build.doc_db import index_docs; "
            "conn = sqlite3.connect(sys.argv[1]); index_docs(conn); conn.close()",
            str(out),
        ],
        # As derive: never index with a perturbed tree under the committed key.
        env=isolated_env(),
        check=True,
    )
    _write_provenance(out)
    for catalog in rest:
        (staging / catalog / DOC_DB_FILENAME).symlink_to(
            Path("..") / first / DOC_DB_FILENAME
        )


def _write_provenance(path: Path, **provenance: str) -> None:
    _stamp_path(path).write_text(
        json.dumps({**provenance, "output_sha256": _file_sha256(path)}, indent=2) + "\n"
    )


def _evict(home: Path, current: Path) -> None:
    """Delete all but the ``KEEP`` most recently used keys (``current`` always
    stays), and the staging directories of builds that died."""
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
    stale = [p for p in keys[KEEP:] if p != current] + [
        p for p in mtimes if p.name.startswith(STAGING_PREFIX) and mtimes[p] < cutoff
    ]
    for path in stale:
        sys.stderr.write(f"g1: removing {path}\n")
        shutil.rmtree(path, ignore_errors=True)


def ensure_server() -> Path:
    """Build the checkout's ``reg-meta`` and return the binary: optimized, as served,
    which also keeps the served cases inside the G1 budget."""
    subprocess.run(
        ["cargo", "build", "--quiet", "--locked", "--release", "-p", "reg-meta"],
        cwd=REPO_ROOT,
        check=True,
    )
    target = REPO_ROOT / os.environ.get("CARGO_TARGET_DIR", "target")
    return target / "release" / "reg-meta"


def isolated_env() -> dict[str, str]:
    """An environment with no path or venv leaks from the caller, for the baseline
    arm and every derive."""
    env = {
        k: v
        for k, v in os.environ.items()
        if k not in {"PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV", "PYTHONSTARTUP"}
        and not k.startswith("UV_")
    }
    env["PYTHONNOUSERSITE"] = "1"
    return env


def baseline_python(tree: Path) -> Path:
    """The baseline interpreter in the tree ``ensure_baseline`` returns."""
    return tree / ".venv" / "bin" / "python"


def ensure_baseline(pins: Pins) -> Path:
    """Return the baseline tree: a detached worktree of the pinned commit with the
    commit's locked environment in ``.venv`` (``uv sync --frozen --no-dev``; maturin
    builds ``reg-core-py`` from the commit's crates). It holds the baseline reader and
    webapp and the webapp's steward branding.
    """
    root = cache_root()
    commit = pins.baseline_commit
    _prune(root / "baseline", {commit})
    home = root / "baseline" / commit
    tree = home / "src"
    # Outside worktree cleanup can delete the tree and leave the marker.
    if (home / "installed").is_file() and tree.is_dir():
        return tree
    shutil.rmtree(home, ignore_errors=True)
    home.mkdir(parents=True)
    sys.stderr.write(f"g1: installing the baseline at {commit}\n")
    # Forget the worktrees whose directories were deleted (repo-wide: git drops every
    # registration whose directory is missing), so the add below cannot collide.
    _repo("worktree", "prune")
    # The repo's post-checkout hook provisions a dev environment; this tree needs none.
    _repo(
        "-c",
        "core.hooksPath=/dev/null",
        "worktree",
        "add",
        "--detach",
        str(tree),
        commit,
    )
    subprocess.run(
        ["uv", "sync", "--quiet", "--frozen", "--no-dev"],
        cwd=tree,
        env=isolated_env(),
        check=True,
    )
    (home / "installed").write_text(f"{commit}\n")
    return tree
