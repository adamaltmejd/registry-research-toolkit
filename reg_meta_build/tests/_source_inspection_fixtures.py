"""Shared helpers for source-record inspection tests.

`interpreter_checkout` gives `inspect-source-records` the clean tracked implementation
its interpreter pin demands, without depending on the state of the developer's
checkout: it copies the imported `reg_meta` and `reg_meta_build` packages (whichever
copies this process imported) into a fresh Git repository with the monorepo layout,
commits them, and runs the real CLI from there in a subprocess.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import reg_meta
import reg_meta_build

if TYPE_CHECKING:
    from reg_meta_build.source_records import SourceRecord

_GIT_IDENTITY = (
    "-c",
    "user.name=Test",
    "-c",
    "user.email=test@example.invalid",
    "-c",
    "commit.gpgsign=false",
    "-c",
    "core.autocrlf=false",
)


def field_text(record: SourceRecord, field: str) -> str | None:
    observation = getattr(record.fields, field)
    if observation is None or observation.status != "value":
        return None
    assert isinstance(observation.value, str)
    return observation.value


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *_GIT_IDENTITY, "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _copy_package(package: object, destination: Path) -> None:
    source = Path(package.__file__).resolve().parent  # type: ignore[attr-defined]
    shutil.copytree(
        source, destination, ignore=shutil.ignore_patterns("__pycache__", "*.pyc")
    )


def _commit_tree(repo: Path) -> str:
    _git(repo, "init", "-q")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "interpreter")
    return _git(repo, "rev-parse", "HEAD")


class InterpreterCheckout:
    """A committed copy of the interpreter sources and a CLI runner bound to it."""

    def __init__(self, root: Path, *, split_reg_meta: bool = False) -> None:
        self.root = root
        build_src = root / "reg_meta_build" / "src"
        _copy_package(reg_meta_build, build_src / "reg_meta_build")
        shutil.copyfile(
            Path(__file__).resolve().parents[2] / "uv.lock", root / "uv.lock"
        )
        # A split checkout puts the loaded reg_meta dependency in a second repository.
        meta_root = root.parent / f"{root.name}-reg-meta" if split_reg_meta else root
        meta_src = meta_root / "reg_meta" / "src"
        _copy_package(reg_meta, meta_src / "reg_meta")
        self.commit = _commit_tree(root)
        if split_reg_meta:
            _commit_tree(meta_root)
        self.pythonpath = (build_src, meta_src)

    def run(self, args: list[str]) -> subprocess.CompletedProcess[str]:
        env = dict(os.environ)
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        env["PYTHONPATH"] = os.pathsep.join(
            [*(str(path) for path in self.pythonpath), env.get("PYTHONPATH", "")]
        )
        return subprocess.run(
            [sys.executable, "-m", "reg_meta_build", *args],
            check=False,
            capture_output=True,
            text=True,
            env=env,
        )


__all__ = [
    "InterpreterCheckout",
    "field_text",
]
