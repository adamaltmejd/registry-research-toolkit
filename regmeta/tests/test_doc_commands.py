"""Tests for doc commands: search, get, list, build-docs."""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path

import pytest

from regmeta.cli import run
from regmeta.doc_db import build_doc_db


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_DOCS = {
    "testreg": {
        "Kommun.md": """\
---
variable: Kommun
display_name: "Bostadskommun"
tags:
  - type/variable
  - topic/demographic
source: "test-bakgrundsfakta"
---

**Bostadskommun Kommun**

Anges med en fyrställig kod.

Med bostadskommun avses den kommun där personen var folkbokförd.
""",
        "SjukPP.md": """\
---
variable: SjukPP
display_name: "Sjukpenning"
tags:
  - type/variable
  - topic/income
  - topic/social-insurance
source: "test-bakgrundsfakta"
---

**Sjukpenning SjukPP**

Sjukpenning, skattepliktig (ej arbetsgivarinträde).

Variabeln avser ersättning från Försäkringskassan.
""",
        "_overview.md": """\
---
display_name: "TESTREG — Översikt"
tags:
  - type/overview
  - topic/testreg
source: "test-bakgrundsfakta"
---

# TESTREG

Detta är ett testregister för dokumentationstester.
""",
        "_methodology-employment.md": """\
---
display_name: "Sysselsättningsmetodik"
tags:
  - type/methodology
  - topic/employment
source: "test-bakgrundsfakta"
---

# Sysselsättningsmetodik

Förvärvsarbetande avgränsas med hjälp av kontrolluppgifter.
""",
    }
}


@pytest.fixture(scope="session")
def doc_db_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Build a doc index from synthetic markdown fixtures."""
    docs_dir = tmp_path_factory.mktemp("docs")
    db_dir = tmp_path_factory.mktemp("doc_db")

    for register, files in SAMPLE_DOCS.items():
        reg_dir = docs_dir / register
        reg_dir.mkdir()
        for filename, content in files.items():
            (reg_dir / filename).write_text(content, encoding="utf-8")

    build_doc_db(docs_dir, db_dir)
    return db_dir


@pytest.fixture()
def doc_db_path(doc_db_dir: Path) -> str:
    """Return --db arg pointing to the doc fixture directory."""
    return str(doc_db_dir)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run_json(argv: list[str], *, verbose: bool = False) -> tuple[dict, int]:
    """Run CLI and parse JSON output."""
    if "--format" not in argv:
        argv = ["--format", "json", *argv]
    if verbose and "--verbose" not in argv:
        argv = ["--verbose", *argv]

    old_stdout = sys.stdout
    old_stderr = sys.stderr
    sys.stdout = buf = io.StringIO()
    sys.stderr = io.StringIO()  # suppress build-docs output
    try:
        exit_code = run(argv)
    finally:
        sys.stdout = old_stdout
        sys.stderr = old_stderr
    output = buf.getvalue()
    if output.strip():
        return json.loads(output), exit_code
    return {}, exit_code


def _run_text(argv: list[str]) -> tuple[str, int]:
    """Run CLI and capture text output."""
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    sys.stdout = buf = io.StringIO()
    sys.stderr = io.StringIO()
    try:
        exit_code = run(argv)
    finally:
        sys.stdout = old_stdout
        sys.stderr = old_stderr
    return buf.getvalue(), exit_code


# ---------------------------------------------------------------------------
# doc search
# ---------------------------------------------------------------------------


class TestDocSearch:
    def test_search_finds_variable(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "doc", "search", "kommun"])
        assert code == 0
        results = data["results"]
        assert len(results) >= 1
        names = [r["variable"] for r in results]
        assert "Kommun" in names

    def test_search_finds_by_content(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "doc", "search", "sjukpenning"])
        assert code == 0
        assert data["total_count"] >= 1
        assert any(r["variable"] == "SjukPP" for r in data["results"])

    def test_search_finds_non_variable(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "doc", "search", "testregister"])
        assert code == 0
        assert any(r["filename"] == "_overview.md" for r in data["results"])

    def test_search_filter_by_type(self, doc_db_path: str):
        data, code = _run_json(
            ["--db", doc_db_path, "doc", "search", "testregister", "--type", "overview"]
        )
        assert code == 0
        for r in data["results"]:
            assert "type/overview" in r["tags"]

    def test_search_filter_by_topic(self, doc_db_path: str):
        data, code = _run_json(
            ["--db", doc_db_path, "doc", "search", "sjukpenning", "--topic", "social-insurance"]
        )
        assert code == 0
        for r in data["results"]:
            assert "topic/social-insurance" in r["tags"]

    def test_search_has_snippet(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "doc", "search", "kommun"])
        assert code == 0
        for r in data["results"]:
            assert "snippet" in r

    def test_search_no_results(self, doc_db_path: str):
        data, code = _run_json(
            ["--db", doc_db_path, "doc", "search", "xyznonexistent"]
        )
        assert code == 0
        assert data["total_count"] == 0


# ---------------------------------------------------------------------------
# doc get
# ---------------------------------------------------------------------------


class TestDocGet:
    def test_get_by_variable(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "doc", "get", "Kommun"])
        assert code == 0
        assert data["variable"] == "Kommun"
        assert data["display_name"] == "Bostadskommun"
        assert "fyrställig kod" in data["body"]
        assert data["filename"] == "Kommun.md"

    def test_get_by_filename(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "doc", "get", "_overview"])
        assert code == 0
        assert data["display_name"] == "TESTREG — Översikt"
        assert data["variable"] is None

    def test_get_by_filename_with_extension(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "doc", "get", "_overview.md"])
        assert code == 0
        assert data["display_name"] == "TESTREG — Översikt"

    def test_get_not_found(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "doc", "get", "NonExistent"])
        assert code == 16  # EXIT_NOT_FOUND

    def test_get_has_tags(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "doc", "get", "SjukPP"])
        assert code == 0
        assert "type/variable" in data["tags"]
        assert "topic/social-insurance" in data["tags"]

    def test_get_includes_file_path(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "doc", "get", "Kommun"])
        assert code == 0
        assert data["file_path"] is not None
        assert "testreg/Kommun.md" in data["file_path"]

    def test_get_text_output(self, doc_db_path: str):
        text, code = _run_text(["--db", doc_db_path, "doc", "get", "Kommun"])
        assert code == 0
        assert "fyrställig kod" in text
        assert "file:" in text


# ---------------------------------------------------------------------------
# doc list
# ---------------------------------------------------------------------------


class TestDocList:
    def test_list_summary(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "doc", "list"])
        assert code == 0
        assert data["total_count"] == 4
        assert "testreg" in data["registers"]
        assert data["registers"]["testreg"] == 4

    def test_list_summary_has_types(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "doc", "list"])
        assert code == 0
        assert "type/variable" in data["types"]
        assert data["types"]["type/variable"] == 2

    def test_list_summary_has_topics(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "doc", "list"])
        assert code == 0
        assert "topic/demographic" in data["topics"]

    def test_list_filter_by_topic(self, doc_db_path: str):
        data, code = _run_json(
            ["--db", doc_db_path, "doc", "list", "--topic", "demographic"]
        )
        assert code == 0
        assert data["total_count"] == 1
        assert data["results"][0]["variable"] == "Kommun"

    def test_list_filter_by_type(self, doc_db_path: str):
        data, code = _run_json(
            ["--db", doc_db_path, "doc", "list", "--type", "methodology"]
        )
        assert code == 0
        assert data["total_count"] == 1
        assert data["results"][0]["filename"] == "_methodology-employment.md"

    def test_list_filter_by_register(self, doc_db_path: str):
        data, code = _run_json(
            ["--db", doc_db_path, "doc", "list", "--register", "testreg"]
        )
        assert code == 0
        assert data["total_count"] == 4

    def test_list_includes_docs_dir(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "doc", "list"])
        assert code == 0
        assert data["docs_dir"] is not None


# ---------------------------------------------------------------------------
# build-docs
# ---------------------------------------------------------------------------


class TestBuildDocs:
    def test_build_docs(self, tmp_path: Path):
        docs_dir = tmp_path / "docs" / "myreg"
        docs_dir.mkdir(parents=True)
        (docs_dir / "Var1.md").write_text(
            "---\nvariable: Var1\ndisplay_name: Test\ntags:\n  - type/variable\n---\n\nBody text.\n",
            encoding="utf-8",
        )
        db_dir = tmp_path / "db"
        db_dir.mkdir()

        text, code = _run_text(
            ["--db", str(db_dir), "maintain", "build-docs", "--docs-dir", str(tmp_path / "docs")]
        )
        assert code == 0
        assert (db_dir / "regmeta_docs.db").exists()
