"""Tests for the docs-DB query helpers."""

from __future__ import annotations

import sqlite3

from reg_meta.doc_db import DOC_SCHEMA_VERSION
from reg_meta.doc_queries import (
    related_document_content,
    related_documents_for_register,
)
from reg_meta_build.doc_db import DOC_DDL


def _doc_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(DOC_DDL)
    conn.execute(
        "INSERT INTO doc_meta (key, value) VALUES ('schema_version', ?)",
        (DOC_SCHEMA_VERSION,),
    )
    conn.execute(
        "INSERT INTO related_document ("
        "register, title, filename, source_url, license, fetched, "
        "sha256, byte_size, content"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "aes",
            "AES manual",
            "aes_manual.pdf",
            "https://example.test/aes_manual.pdf",
            "CC BY 4.0",
            "2026-06-29",
            "a" * 64,
            9,
            b"%PDF-test",
        ),
    )
    conn.execute(
        "INSERT INTO related_document ("
        "register, title, filename, source_url, license, fetched, "
        "sha256, byte_size, content"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "aes",
            "Questionnaire",
            "questionnaire.pdf",
            "https://example.test/questionnaire.pdf",
            "CC BY 4.0",
            "2026-06-29",
            "b" * 64,
            5,
            b"quest",
        ),
    )
    conn.commit()
    return conn


def test_related_documents_for_register_returns_ordered_metadata() -> None:
    conn = _doc_conn()

    docs = related_documents_for_register(conn, "aes")

    assert [doc.filename for doc in docs] == ["aes_manual.pdf", "questionnaire.pdf"]
    assert docs[0].model_dump() == {
        "title": "AES manual",
        "filename": "aes_manual.pdf",
        "source_url": "https://example.test/aes_manual.pdf",
        "license": "CC BY 4.0",
        "fetched": "2026-06-29",
        "sha256": "a" * 64,
        "byte_size": 9,
    }
    assert related_documents_for_register(conn, "lisa") == ()


def test_related_document_content_fetches_exact_register_filename() -> None:
    conn = _doc_conn()

    doc = related_document_content(conn, "aes", "aes_manual.pdf")

    assert doc is not None
    assert doc.content == b"%PDF-test"
    assert doc.filename == "aes_manual.pdf"
    assert related_document_content(conn, "aes", "../aes_manual.pdf") is None
    assert related_document_content(conn, "lisa", "aes_manual.pdf") is None
