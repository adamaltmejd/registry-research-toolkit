from __future__ import annotations

import logging
import sqlite3
from hashlib import sha256
from typing import TYPE_CHECKING

import pytest
import reg_meta_build.doc_db as doc_db
from reg_meta.errors import RegMetaError
from reg_meta_build.doc_db import RelatedDocument, build_doc_db, parse_frontmatter

if TYPE_CHECKING:
    from pathlib import Path


def _write_doc(docs_dir: Path, register: str = "testreg") -> None:
    reg_dir = docs_dir / register
    reg_dir.mkdir(parents=True)
    (reg_dir / "Doc.md").write_text(
        "---\nvariable: Test\ndisplay_name: Test\ntags:\n  - type/variable\n---\n\nBody.\n",
        encoding="utf-8",
    )


def _related_doc(
    register: str,
    filename: str,
    *,
    content: bytes = b"%PDF-1.4\nfixture\n",
    required: bool = True,
) -> RelatedDocument:
    return RelatedDocument(
        register=register,
        title="Related test document",
        filename=filename,
        source_url="https://mikrometadata.scb.se/",
        license="CC BY 4.0",
        fetched="2026-06-23",
        sha256=sha256(content).hexdigest(),
        byte_size=len(content),
        required=required,
    )


def test_parse_frontmatter_folded_block_scalar() -> None:
    """A ``>-`` folded scalar joins continuation lines with single spaces."""
    text = (
        "---\n"
        "variable: KU2AstSNI2002B\n"
        "display_name: >-\n"
        "  Summa inkomst föranledd av\n"
        "  förtidspension/sjukbidrag/sjukersättning/aktivitetsersättning\n"
        "tags:\n"
        "  - type/variable\n"
        'source: "lisa-bakgrundsfakta-1990-2017"\n'
        "---\n\nBody.\n"
    )
    meta, body = parse_frontmatter(text)
    assert meta["display_name"] == (
        "Summa inkomst föranledd av "
        "förtidspension/sjukbidrag/sjukersättning/aktivitetsersättning"
    )
    # Keys after the block scalar and existing behavior must still parse.
    assert meta["variable"] == "KU2AstSNI2002B"
    assert meta["tags"] == ["type/variable"]
    assert meta["source"] == "lisa-bakgrundsfakta-1990-2017"
    assert body == "Body.\n"


def test_parse_frontmatter_literal_block_scalar() -> None:
    """A ``|-`` literal scalar joins continuation lines with newlines."""
    text = "---\nvariable: Test\nnotes: |-\n  line one\n  line two\n---\n\nBody.\n"
    meta, _ = parse_frontmatter(text)
    assert meta["notes"] == "line one\nline two"
    assert meta["variable"] == "Test"


def test_parse_frontmatter_plain_multiline_flow_scalar() -> None:
    """A plain (no ``>``/``|``) wrapped value folds its continuation lines.

    Regression: panache emits a bare indented continuation for a long value with
    no quote-forcing characters; the parser must fold it, not truncate to the
    first physical line.
    """
    text = (
        "---\n"
        "variable: Test\n"
        "display_name: Ett långt namn som är\n"
        "  väldigt långt och bryts över rader\n"
        "tags:\n"
        "  - type/variable\n"
        "---\n\nBody.\n"
    )
    meta, body = parse_frontmatter(text)
    assert (
        meta["display_name"]
        == "Ett långt namn som är väldigt långt och bryts över rader"
    )
    # Keys after the plain-flow scalar and existing behavior must still parse.
    assert meta["variable"] == "Test"
    assert meta["tags"] == ["type/variable"]
    assert body == "Body.\n"


def test_parse_frontmatter_committed_lisa_shape() -> None:
    """Golden-style: a real committed lisa ``>-`` frontmatter parses fully.

    Mirrors reg_meta_build/docs/lisa/KU2AstSNI2007G.md so a change in the
    generator's block-scalar output surfaces here.
    """
    text = (
        "---\n"
        "variable: KU2AstSNI2007G\n"
        "display_name: >-\n"
        "  Näringsgrenstillhörighet enligt SNI2007 (arbetsställe, näst största förvärvskälla),\n"
        "  grov nivå\n"
        "tags:\n"
        "  - topic/identifier\n"
        "  - type/variable\n"
        'source: "lisa-bakgrundsfakta-1990-2017"\n'
        "---\n\nBody.\n"
    )
    meta, _ = parse_frontmatter(text)
    assert meta["display_name"] == (
        "Näringsgrenstillhörighet enligt SNI2007 (arbetsställe, näst största "
        "förvärvskälla), grov nivå"
    )
    assert meta["variable"] == "KU2AstSNI2007G"
    assert meta["tags"] == ["topic/identifier", "type/variable"]
    assert meta["source"] == "lisa-bakgrundsfakta-1990-2017"


def test_parse_frontmatter_same_line_scalars_and_list_unchanged() -> None:
    """Same-line quoted/unquoted scalars and simple lists parse as before."""
    text = (
        "---\n"
        "variable: Test\n"
        'display_name: "Quoted name"\n'
        "tags:\n"
        "  - topic/identifier\n"
        "  - type/variable\n"
        "---\n\nBody.\n"
    )
    meta, body = parse_frontmatter(text)
    assert meta["variable"] == "Test"
    assert meta["display_name"] == "Quoted name"
    assert meta["tags"] == ["topic/identifier", "type/variable"]
    assert body == "Body.\n"


def test_build_doc_db_stores_related_document_binary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    docs_dir = tmp_path / "docs"
    _write_doc(docs_dir)
    related_root = tmp_path / "related"
    related_dir = related_root / "testreg"
    related_dir.mkdir(parents=True)
    content = b"%PDF-1.4\nfixture\n"
    (related_dir / "related.pdf").write_bytes(content)

    monkeypatch.setattr(
        doc_db,
        "load_related_documents",
        lambda: {"testreg": [_related_doc("testreg", "related.pdf", content=content)]},
    )
    monkeypatch.setattr(
        doc_db, "repo_related_document_binaries_dir", lambda: related_root
    )

    db_path = build_doc_db(docs_dir, tmp_path / "db")

    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT register, title, filename, source_url, license, fetched, "
            "sha256, byte_size, content FROM related_document"
        ).fetchone()
        assert row is not None
        assert row["register"] == "testreg"
        assert row["filename"] == "related.pdf"
        assert row["sha256"] == sha256(content).hexdigest()
        assert row["byte_size"] == len(content)
        assert row["content"] == content
        assert (
            conn.execute(
                "SELECT value FROM doc_meta WHERE key = 'related_document_count'"
            ).fetchone()[0]
            == "1"
        )
    finally:
        conn.close()


def test_build_doc_db_warns_for_missing_and_unmapped_related_documents(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    docs_dir = tmp_path / "docs"
    _write_doc(docs_dir)
    related_root = tmp_path / "related"
    related_dir = related_root / "testreg"
    related_dir.mkdir(parents=True)
    (related_dir / "unmapped.pdf").write_bytes(b"%PDF-1.4\nunmapped\n")

    monkeypatch.setattr(
        doc_db,
        "load_related_documents",
        lambda: {"testreg": [_related_doc("testreg", "missing.pdf")]},
    )
    monkeypatch.setattr(
        doc_db, "repo_related_document_binaries_dir", lambda: related_root
    )
    caplog.set_level(logging.WARNING, logger="reg_meta_build.doc_db")

    db_path = build_doc_db(docs_dir, tmp_path / "db")

    messages = [record.getMessage() for record in caplog.records]
    assert any("unmapped.pdf" in message for message in messages)
    assert any("missing.pdf" in message for message in messages)
    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM related_document").fetchone()[0] == 0
        assert (
            conn.execute(
                "SELECT value FROM doc_meta WHERE key = 'related_document_count'"
            ).fetchone()[0]
            == "0"
        )
    finally:
        conn.close()


def test_build_doc_db_rejects_related_document_binary_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    docs_dir = tmp_path / "docs"
    _write_doc(docs_dir)
    related_root = tmp_path / "related"
    related_dir = related_root / "testreg"
    related_dir.mkdir(parents=True)
    (related_dir / "related.pdf").write_bytes(b"%PDF-1.4\nchanged\n")

    monkeypatch.setattr(
        doc_db,
        "load_related_documents",
        lambda: {"testreg": [_related_doc("testreg", "related.pdf")]},
    )
    monkeypatch.setattr(
        doc_db, "repo_related_document_binaries_dir", lambda: related_root
    )

    with pytest.raises(RegMetaError) as exc_info:
        build_doc_db(docs_dir, tmp_path / "db")
    assert exc_info.value.code == "related_documents_binary_mismatch"
    assert "related.pdf" in exc_info.value.message


def test_build_doc_db_warns_when_related_document_root_is_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    docs_dir = tmp_path / "docs"
    _write_doc(docs_dir, register="testreg")

    monkeypatch.setattr(
        doc_db,
        "load_related_documents",
        lambda: {"testreg": [_related_doc("testreg", "missing.pdf")]},
    )
    monkeypatch.setattr(doc_db, "repo_related_document_binaries_dir", lambda: None)
    caplog.set_level(logging.WARNING, logger="reg_meta_build.doc_db")

    db_path = build_doc_db(docs_dir, tmp_path / "db")

    messages = [record.getMessage() for record in caplog.records]
    assert any("testreg/missing.pdf" in message for message in messages)
    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM related_document").fetchone()[0] == 0
        assert (
            conn.execute(
                "SELECT value FROM doc_meta WHERE key = 'related_document_count'"
            ).fetchone()[0]
            == "0"
        )
    finally:
        conn.close()


def test_build_doc_db_skips_future_related_document_register(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    docs_dir = tmp_path / "docs"
    _write_doc(docs_dir, register="testreg")
    related_root = tmp_path / "related"
    related_root.mkdir()

    monkeypatch.setattr(
        doc_db,
        "load_related_documents",
        lambda: {"future": [_related_doc("future", "future.pdf", required=False)]},
    )
    monkeypatch.setattr(
        doc_db, "repo_related_document_binaries_dir", lambda: related_root
    )
    caplog.set_level(logging.WARNING, logger="reg_meta_build.doc_db")

    db_path = build_doc_db(docs_dir, tmp_path / "db")

    assert not caplog.records
    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM related_document").fetchone()[0] == 0
    finally:
        conn.close()
