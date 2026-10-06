"""Curated related documents at the ``reg-meta-build build-docs`` boundary.

``related_documents.toml`` and the binary root ``input_data/SCB/docs`` resolve
relative to the builder package file and ``build_doc_db`` takes no parameter for
them, so each case writes a synthetic map and synthetic PDF bytes into a copied
builder package and runs the CLI against that copy in a subprocess (the copy is
of whichever package the test process imported). The observable results are the
CLI exit code and JSON, its stderr warnings, and the built ``reg_meta_docs.db``.
The entries mirror the deleted ``test_doc_db.py`` cases' ``RelatedDocument``
literals.
"""

from __future__ import annotations

import sqlite3
from hashlib import sha256
from typing import TYPE_CHECKING

import pytest
from _curation_toml_boundary_support import (
    builder_copy,
    reset_doc_curation,
    run_build_docs,
    write_register_doc,
)
from reg_meta.errors import EXIT_CONFIG

if TYPE_CHECKING:
    from pathlib import Path

    from _curation_toml_boundary_support import CliRun

PDF = b"%PDF-1.4\nfixture\n"


@pytest.fixture(scope="module")
def package_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return builder_copy(tmp_path_factory.mktemp("related-documents"))


@pytest.fixture
def package(package_root: Path) -> Path:
    reset_doc_curation(package_root)
    return package_root


def _map(
    package: Path,
    register: str,
    filename: str,
    *,
    content: bytes = PDF,
    required: bool = True,
) -> None:
    (package / "related_documents.toml").write_text(
        f"[[register.{register}.document]]\n"
        'title = "Related test document"\n'
        f'filename = "{filename}"\n'
        'source_url = "https://mikrometadata.scb.se/"\n'
        'license = "CC BY 4.0"\n'
        'fetched = "2026-06-23"\n'
        f'sha256 = "{sha256(content).hexdigest()}"\n'
        f"byte_size = {len(content)}\n" + ("" if required else "required = false\n"),
        encoding="utf-8",
    )


def _binary(package: Path, register: str, filename: str, content: bytes) -> None:
    directory = package / "input_data" / "SCB" / "docs" / register
    directory.mkdir(parents=True, exist_ok=True)
    (directory / filename).write_bytes(content)


def _build(package: Path, tmp_path: Path) -> CliRun:
    write_register_doc(tmp_path / "docs")
    return run_build_docs(package, tmp_path / "docs", tmp_path / "db")


def _related(run: CliRun) -> tuple[list[tuple], str]:
    with sqlite3.connect(run.payload["db_path"]) as conn:
        rows = conn.execute(
            "SELECT register, title, filename, source_url, license, fetched, "
            "sha256, byte_size, content FROM related_document"
        ).fetchall()
        (count,) = conn.execute(
            "SELECT value FROM doc_meta WHERE key = 'related_document_count'"
        ).fetchone()
    return rows, count


def test_mapped_binary_is_stored_with_its_pins(package: Path, tmp_path: Path) -> None:
    _map(package, "testreg", "related.pdf")
    _binary(package, "testreg", "related.pdf", PDF)
    run = _build(package, tmp_path)
    assert run.exit_code == 0
    assert run.stderr == ""
    assert _related(run) == (
        [
            (
                "testreg",
                "Related test document",
                "related.pdf",
                "https://mikrometadata.scb.se/",
                "CC BY 4.0",
                "2026-06-23",
                sha256(PDF).hexdigest(),
                len(PDF),
                PDF,
            )
        ],
        "1",
    )


def test_missing_and_unmapped_binaries_warn_and_store_nothing(
    package: Path, tmp_path: Path
) -> None:
    _map(package, "testreg", "missing.pdf")
    _binary(package, "testreg", "unmapped.pdf", b"%PDF-1.4\nunmapped\n")
    run = _build(package, tmp_path)
    assert run.exit_code == 0
    assert "testreg/unmapped.pdf" in run.stderr
    assert "testreg/missing.pdf" in run.stderr
    assert _related(run) == ([], "0")


def test_binary_that_differs_from_its_pins_fails_the_build(
    package: Path, tmp_path: Path
) -> None:
    _map(package, "testreg", "related.pdf")
    _binary(package, "testreg", "related.pdf", b"%PDF-1.4\nchanged\n")
    run = _build(package, tmp_path)
    assert run.exit_code == EXIT_CONFIG
    assert run.payload["error"]["code"] == "related_documents_binary_mismatch"
    assert "testreg/related.pdf" in run.payload["error"]["message"]


def test_absent_binary_root_warns_and_skips_mapped_documents(
    package: Path, tmp_path: Path
) -> None:
    _map(package, "testreg", "missing.pdf")
    run = _build(package, tmp_path)
    assert run.exit_code == 0
    assert "input_data/SCB/docs is missing" in run.stderr
    assert "testreg/missing.pdf" in run.stderr
    assert _related(run) == ([], "0")


def test_optional_document_for_an_inactive_register_is_skipped_silently(
    package: Path, tmp_path: Path
) -> None:
    _map(package, "future", "future.pdf", required=False)
    (package / "input_data" / "SCB" / "docs").mkdir(parents=True)
    run = _build(package, tmp_path)
    assert run.exit_code == 0
    assert run.stderr == ""
    assert _related(run) == ([], "0")
