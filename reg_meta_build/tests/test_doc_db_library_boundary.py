"""`build_doc_db` as a library reads only the inputs its caller passes.

Related-document curation and binaries are explicit arguments; only the
`build-docs` CLI supplies the repository's `related_documents.toml` and the
untracked `input_data/SCB/docs` seed, so library callers (test fixtures
included) stay hermetic.
"""

from __future__ import annotations

import logging
import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING

from reg_meta_build.doc_db import build_doc_db

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def test_library_build_reads_no_repository_related_documents(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    docs = tmp_path / "docs" / "testreg"
    docs.mkdir(parents=True)
    (docs / "Var.md").write_text("---\nvariable: Var\n---\n\nBody.\n", encoding="utf-8")

    with caplog.at_level(logging.WARNING):
        db_path = build_doc_db(tmp_path / "docs", tmp_path / "db")

    with closing(sqlite3.connect(db_path)) as conn:
        counts = dict(conn.execute("SELECT key, value FROM doc_meta"))
        stored = conn.execute("SELECT COUNT(*) FROM related_document").fetchone()[0]
    assert (counts["doc_count"], counts["related_document_count"], stored) == (
        "1",
        "0",
        0,
    )
    assert not [r for r in caplog.records if "related" in r.getMessage().lower()]
