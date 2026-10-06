"""Shared fixtures for SCB snapshot, bundle and build-lock tests: inventories, Git and
a copied builder checkout that runs the prototype CLI as a real subprocess."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

from _prepared_fixtures import accept_prepared
from reg_meta_build.input_snapshot import SCB_CSV_FILES

import reg_meta_build


def scb_inventory(
    tmp_path: Path,
    source_dir: Path,
    *,
    extra: tuple[str, ...] = (),
) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    archive = tmp_path / "retained-delivery.zip"
    archive.write_bytes(b"independently retained archive fixture")
    actual = {path.name for path in source_dir.glob("*.csv")}
    names = (*SCB_CSV_FILES, *extra)
    inventory = tmp_path / "inventory.json"
    inventory.write_text(
        json.dumps(
            {
                "bundle_id": "scb-mikrometadata",
                "edition": "fixture-2026-09-14",
                "source_dir": str(source_dir),
                "files": [{"name": name, "required": name in actual} for name in names],
                "archives": [
                    {
                        "path": str(archive),
                        "locator": "offline/scb-fixture.zip",
                        "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
                        "members": sorted(actual.intersection(names)),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return inventory


def git(repo: Path, *args: str) -> str:
    process = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return process.stdout.strip()


def copied_builder_checkout(tmp_path: Path) -> Path:
    """Commit a copy of the installed builder package, prototype CLI and lockfile.

    Provenance commands resolve the builder checkout from the running module's file,
    so they run in a subprocess whose PYTHONPATH points at this copy (see
    ``run_prototype``). ``ignored/`` is gitignored for untracked-source cases.
    """
    checkout = tmp_path / "builder-checkout"
    assert reg_meta_build.__file__ is not None
    package = Path(reg_meta_build.__file__).parent
    shutil.copytree(
        package,
        checkout / "reg_meta_build/src/reg_meta_build",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    source_root = package.resolve().parents[2]
    for name in ("scripts/prototype_scb_inputs.py", "uv.lock"):
        target = checkout / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_root / name, target)
    (checkout / ".gitignore").write_text("__pycache__/\nignored/\n", encoding="utf-8")
    accept_prepared(checkout / ".gitignore")
    git(checkout, "add", "-A")
    git(checkout, "commit", "-q", "-m", "Pin builder checkout")
    return checkout


def run_prototype(
    checkout: Path,
    *args: object,
    script: Path | None = None,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run the prototype SCB input CLI against the copied checkout's package.

    Success prints JSON on stdout; a SnapshotError exits 2 with ``error: ...`` on
    stderr.
    """
    script = script or checkout / "scripts/prototype_scb_inputs.py"
    return subprocess.run(
        [sys.executable, str(script), *(str(arg) for arg in args)],
        env={
            **os.environ,
            **(env or {}),
            "PYTHONPATH": str(checkout / "reg_meta_build/src"),
            "PYTHONDONTWRITEBYTECODE": "1",
        },
        capture_output=True,
        text=True,
        check=False,
    )
