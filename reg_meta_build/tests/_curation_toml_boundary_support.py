"""Shared builders for the doc-curation boundary cases.

``builder_copy`` / ``run_build_docs`` run the ``reg-meta-build build-docs`` CLI in a
subprocess against a copy of the imported builder package. The curated doc files
(``doc_sources.toml``, ``related_documents.toml``) and the related-document binary
root (``input_data/SCB/docs``) resolve relative to the package file, so the cases
write synthetic files into the copy; nothing under the real checkout is read.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING

from _snapshot_fixtures import copied_builder_checkout

if TYPE_CHECKING:
    from pathlib import Path


def builder_copy(tmp_path: Path) -> Path:
    """The package root (``<copy>/reg_meta_build``) of a copied builder checkout.

    The copy carries no ``doc_sources.toml``, ``related_documents.toml`` or
    ``input_data/`` until a case writes them.
    """
    return copied_builder_checkout(tmp_path) / "reg_meta_build"


def reset_doc_curation(package_root: Path) -> None:
    """Remove every doc curation input a previous case wrote into the copy."""
    for name in ("doc_sources.toml", "related_documents.toml"):
        (package_root / name).unlink(missing_ok=True)
    shutil.rmtree(package_root / "input_data", ignore_errors=True)


@dataclass(frozen=True)
class CliRun:
    exit_code: int
    payload: dict
    stderr: str


def run_build_docs(package_root: Path, docs_dir: Path, db_dir: Path) -> CliRun:
    """Run ``python -m reg_meta_build build-docs`` against the copied package."""
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "reg_meta_build",
            "build-docs",
            "--docs-dir",
            str(docs_dir),
            "--db",
            str(db_dir),
        ],
        env={
            **os.environ,
            "PYTHONPATH": str(package_root / "src"),
            "PYTHONDONTWRITEBYTECODE": "1",
        },
        capture_output=True,
        text=True,
        check=False,
    )
    return CliRun(
        completed.returncode,
        json.loads(completed.stdout) if completed.stdout.strip() else {},
        completed.stderr,
    )


def write_register_doc(docs_dir: Path, register: str = "testreg") -> None:
    """One synthetic register doc page, enough for ``build-docs`` to index."""
    register_dir = docs_dir / register
    register_dir.mkdir(parents=True)
    (register_dir / "Doc.md").write_text(
        "---\nvariable: Test\ndisplay_name: Test\ntags:\n  - type/variable\n---\n\n"
        "Body.\n",
        encoding="utf-8",
    )
