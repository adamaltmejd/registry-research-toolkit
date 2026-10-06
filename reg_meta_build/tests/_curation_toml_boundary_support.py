"""Shared builders for the curation-TOML and doc-DB boundary cases.

Two shapes, both driven from readable synthetic sources:

- ``build_scb_catalog`` writes SCB delivery CSV rows (plus optional authored
  Försäkringskassan TOML), runs them through ``prepare_catalog_sources`` and
  ``build_catalog`` in diagnostic mode with a synthetic curation tree, and returns
  the built database and report directory.
- ``builder_copy`` / ``run_build_docs`` run the ``reg-meta-build build-docs`` CLI in a
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

from _csv_fixtures import write_input_bundle, write_scb_input
from _prepared_fixtures import accept_prepared
from _snapshot_fixtures import copied_builder_checkout
from reg_meta_build.pipeline import build_catalog
from reg_meta_build.prepared_catalog import prepare_catalog_sources

if TYPE_CHECKING:
    from pathlib import Path

SCB_SAMPLE_REGISTER = (
    '[register]\nprovider = "scb"\nslug = "sample"\nnative_id = "1"\n'
    '[[variant]]\nnative_id = "1.10"\nslug = "people"\n'
)


@dataclass(frozen=True)
class BuiltCatalog:
    db: Path
    report: Path
    result: dict


def build_scb_catalog(
    tmp_path: Path,
    *,
    curation: dict[str, str],
    registerinformation_rows: list[str],
    unika_rows: list[str] | None = None,
    vardemangder_rows: list[str] | None = None,
    valid_dates_rows: list[str] | None = None,
    fk_toml: str | None = None,
) -> BuiltCatalog:
    """Prepare and build a diagnostic catalog from synthetic sources.

    Only the SCB files whose rows are given are written. ``curation`` maps
    curation-tree relative paths to TOML or JSON text. ``fk_toml`` is an authored
    ``Forsakringskassan/fk.toml``.
    """
    source = tmp_path / "source"
    optional = {
        "unika": unika_rows,
        "vardemangder": vardemangder_rows,
        "valid_dates": valid_dates_rows,
    }
    write_scb_input(
        source,
        registerinformation_rows=registerinformation_rows,
        unika_rows=unika_rows or [],
        vardemangder_rows=vardemangder_rows or [],
        valid_dates_rows=valid_dates_rows,
        include=(
            "registerinformation",
            *(name for name, rows in optional.items() if rows is not None),
        ),
    )
    if fk_toml is not None:
        (source / "Forsakringskassan").mkdir()
        (source / "Forsakringskassan" / "fk.toml").write_text(fk_toml, encoding="utf-8")
    bundle = write_input_bundle(tmp_path / "inputs", source)
    prepared = tmp_path / "prepared" / "catalog"
    manifest = prepare_catalog_sources(bundle, prepared)
    commit = accept_prepared(prepared)
    root = tmp_path / "curation"
    (root / "classifications").mkdir(parents=True)
    for relative, text in curation.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    db = tmp_path / "db" / "reg_meta.db"
    report = tmp_path / "report"
    result = build_catalog(
        prepared,
        commit,
        manifest.sha256,
        db,
        report,
        curation_dir=root,
        diagnostic=True,
    )
    return BuiltCatalog(db, report, result)


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
