"""The shared G1 cache: pinned release artifacts, their derived copies and the
baseline reader environment.

Layout under the cache root (``$REG_META_G1_CACHE``, else
``$XDG_CACHE_HOME/reg-meta-g1``, else ``~/.cache/reg-meta-g1``)::

    artifacts/<tag>/global/reg_meta.db       + reg_meta_docs.db
    artifacts/<tag>/global/derived/          reg_meta.db + reg_meta_docs.db -> ..
    artifacts/<tag>/swecov/reg_meta.db       + reg_meta_docs.db -> ../global/...
    artifacts/<tag>/swecov/derived/          reg_meta.db + reg_meta_docs.db -> ..
    baseline/<commit>/venv/                  the baseline reader's own environment

Each asset is streamed once: hashed against its pinned SHA-256 while it is
decompressed, so no ``.zst`` is kept. A stamp beside the database records the
verified asset digest and the file's size and mtime; a later run refetches when they
no longer match (readers open the files immutable, so they never change). A derived
copy is ``reg-meta-build derive`` of its catalog, stamped the same way with a key over
the base's asset digest and the builder source tree, and remade when either changes. Anything not pinned is deleted, so the cache never holds more than the
pinned set. Callers hold ``locked`` for a whole run, which serializes runs from
concurrent worktrees (they share the report directory and the CPU budget).
"""

from __future__ import annotations

import fcntl
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
from contextlib import contextmanager
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
# What uv export needs to read the commit's workspace lock, plus the two packages.
BASELINE_PATHS = (
    "pyproject.toml",
    "uv.lock",
    "reg_meta",
    "reg_schema",
    "reg_meta_build/pyproject.toml",
    "reg_webapp/backend/pyproject.toml",
)
CHUNK = 1 << 20
REPO_ROOT = Path(__file__).resolve().parents[2]
# What a derived copy depends on: the builder, the reader code derive moved, and the
# locked dependencies.
DERIVE_SOURCES = ("reg_meta_build/src", "reg_meta/src", "uv.lock")


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


def ensure_derived(pins: Pins, dirs: dict[str, Path]) -> dict[str, Path]:
    """Return ``{catalog: directory}`` of the derived copies, deriving as needed.

    Derive stamps the builder commit into each copy, so G1 runs on a committed tree.
    """
    if _repo("status", "--porcelain", "--untracked-files=no", "--", *DERIVE_SOURCES):
        raise RuntimeError(
            f"G1 derives from committed sources; commit {', '.join(DERIVE_SOURCES)}"
        )
    source = _repo("rev-parse", *(f"HEAD:{path}" for path in DERIVE_SOURCES))
    derived: dict[str, Path] = {}
    for catalog, directory in sorted(dirs.items()):
        out = directory / "derived" / DB_FILENAME
        key = Asset(
            "derive",
            hashlib.sha256(
                f"{pins.catalogs[catalog].sha256}\n{source}".encode()
            ).hexdigest(),
        )
        if not _stamp_ok(out, key):
            sys.stderr.write(f"g1: deriving {catalog}\n")
            _stamp_path(out).unlink(missing_ok=True)
            subprocess.run(
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
                stdout=subprocess.DEVNULL,
                check=True,
            )
            # The atomic publish keeps the replaced copy aside; disk is tight.
            out.with_name(out.name + ".prev").unlink(missing_ok=True)
            _write_stamp(out, key)
        docs = out.parent / DOC_DB_FILENAME
        if not docs.is_symlink():
            docs.symlink_to(Path("..") / DOC_DB_FILENAME)
        derived[catalog] = out.parent
    return derived


def isolated_env() -> dict[str, str]:
    """The baseline arm's environment: no path or venv leaks from the caller."""
    env = {
        k: v
        for k, v in os.environ.items()
        if k not in {"PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV", "PYTHONSTARTUP"}
        and not k.startswith("UV_")
    }
    env["PYTHONNOUSERSITE"] = "1"
    return env


def ensure_baseline(pins: Pins) -> Path:
    """Return the baseline interpreter: the pinned commit's reader in its own venv.

    The venv gets the commit's locked third-party dependencies (``uv export`` from the
    commit's ``uv.lock``) and non-editable ``reg_schema`` + ``reg_meta`` built from a
    ``git archive`` of the commit, which is deleted after the install.
    """
    root = cache_root()
    commit = pins.baseline_commit
    _prune(root / "baseline", {commit})
    home = root / "baseline" / commit
    python = home / "venv" / "bin" / "python"
    if (home / "installed").is_file():
        return python
    shutil.rmtree(home, ignore_errors=True)
    home.mkdir(parents=True)
    sys.stderr.write(f"g1: installing the baseline reader at {commit}\n")
    archive = subprocess.run(
        ["git", "archive", "--format=tar", commit, *BASELINE_PATHS],
        # Pathspecs are relative to cwd, so archive from the repo root.
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        check=True,
    ).stdout
    env = isolated_env()
    with tempfile.TemporaryDirectory(dir=home) as tmp:
        src = Path(tmp)
        with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
            tar.extractall(src, filter="data")
        requirements = src / "requirements.txt"
        uv_pip = ["uv", "pip", "install", "--quiet", "--python", str(python)]
        for cmd in (
            [
                "uv",
                "export",
                "--quiet",
                "--frozen",
                "--no-dev",
                "--package",
                "reg-meta",
                "--no-emit-workspace",
                "--no-header",
                "--output-file",
                str(requirements),
            ],
            ["uv", "venv", "--quiet", "--python", "3.14", str(home / "venv")],
            [*uv_pip, "--require-hashes", "-r", str(requirements)],
            [*uv_pip, "--no-deps", str(src / "reg_schema"), str(src / "reg_meta")],
        ):
            subprocess.run(cmd, cwd=src, env=env, check=True)
    (home / "installed").write_text(commit + "\n")
    return python
