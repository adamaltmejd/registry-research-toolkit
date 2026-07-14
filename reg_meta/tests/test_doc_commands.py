"""Tests for doc commands: search, get, list, build-docs."""

from __future__ import annotations

import io
import json
import sys
from argparse import Namespace
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest
from reg_meta.cli import run
from reg_meta_build.doc_db import build_doc_db

if TYPE_CHECKING:
    from pathlib import Path

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
        # A mapped source carrying a stray `.md` — the build strips it before
        # the curated-map lookup, so this resolves to the LISA bakgrundsfakta
        # URL/title and stores a de-`.md`'d `source` (#372).
        "MappedVar.md": """\
---
variable: MappedVar
display_name: "Mappad variabel"
tags:
  - type/variable
source: "lisa-bakgrundsfakta-1990-2017.md"
---

**Mappad variabel MappedVar**

Denna variabel har en kurerad källänk.
""",
        # An unmapped source: both source_url and source_title stay NULL (#372).
        "UnmappedVar.md": """\
---
variable: UnmappedVar
display_name: "Omappad variabel"
tags:
  - type/variable
source: "some-uncurated-source"
---

**Omappad variabel UnmappedVar**

Denna variabel saknar kurerad källänk.
""",
    }
}

# Curated values from reg_meta_build/doc_sources.toml (#372), for assertions.
_LISA_BAKGRUNDSFAKTA_URL = (
    "https://www.scb.se/contentassets/0521204f13e649299dec73f091e691e0/"
    "lisa-bakgrundsfakta-1990-2017.pdf"
)
_LISA_BAKGRUNDSFAKTA_TITLE = "LISA bakgrundsfakta 1990-2017"


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
        data, code = _run_json(["--db", doc_db_path, "docs", "search", "kommun"])
        assert code == 0
        results = data["results"]
        assert len(results) >= 1
        names = [r["variable"] for r in results]
        assert "Kommun" in names

    def test_search_finds_by_content(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "docs", "search", "sjukpenning"])
        assert code == 0
        assert data["total_count"] >= 1
        assert any(r["variable"] == "SjukPP" for r in data["results"])

    def test_search_finds_non_variable(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "docs", "search", "testregister"])
        assert code == 0
        assert any(r["filename"] == "_overview.md" for r in data["results"])

    def test_search_filter_by_type(self, doc_db_path: str):
        data, code = _run_json(
            [
                "--db",
                doc_db_path,
                "docs",
                "search",
                "testregister",
                "--type",
                "overview",
            ]
        )
        assert code == 0
        for r in data["results"]:
            assert "type/overview" in r["tags"]

    def test_search_filter_by_topic(self, doc_db_path: str):
        data, code = _run_json(
            [
                "--db",
                doc_db_path,
                "docs",
                "search",
                "sjukpenning",
                "--topic",
                "social-insurance",
            ]
        )
        assert code == 0
        for r in data["results"]:
            assert "topic/social-insurance" in r["tags"]

    def test_search_has_snippet(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "docs", "search", "kommun"])
        assert code == 0
        for r in data["results"]:
            assert "snippet" in r

    def test_search_no_results(self, doc_db_path: str):
        data, code = _run_json(
            ["--db", doc_db_path, "docs", "search", "xyznonexistent"]
        )
        assert code == 0
        assert data["total_count"] == 0


# ---------------------------------------------------------------------------
# doc get
# ---------------------------------------------------------------------------


class TestDocGet:
    def test_get_by_variable(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "docs", "get", "Kommun"])
        assert code == 0
        assert data["variable"] == "Kommun"
        assert data["display_name"] == "Bostadskommun"
        assert "fyrställig kod" in data["body"]
        assert data["filename"] == "Kommun.md"

    def test_get_by_filename(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "docs", "get", "_overview"])
        assert code == 0
        assert data["display_name"] == "TESTREG — Översikt"
        assert data["variable"] is None

    def test_get_by_filename_with_extension(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "docs", "get", "_overview.md"])
        assert code == 0
        assert data["display_name"] == "TESTREG — Översikt"

    def test_get_not_found(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "docs", "get", "NonExistent"])
        assert code == 16  # EXIT_NOT_FOUND

    def test_get_has_tags(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "docs", "get", "SjukPP"])
        assert code == 0
        assert "type/variable" in data["tags"]
        assert "topic/social-insurance" in data["tags"]

    def test_get_omits_file_path(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "docs", "get", "Kommun"])
        assert code == 0
        assert "file_path" not in data

    def test_get_text_output(self, doc_db_path: str):
        text, code = _run_text(["--db", doc_db_path, "docs", "get", "Kommun"])
        assert code == 0
        assert "fyrställig kod" in text
        assert "file:" not in text


# ---------------------------------------------------------------------------
# source_url / source_title (#372)
# ---------------------------------------------------------------------------


class TestDocSourceUrl:
    """The build-time curated source→PDF map (#372) flows through search/get."""

    def test_get_mapped_strips_md_and_resolves(self, doc_db_path: str):
        # The fixture's source `lisa-bakgrundsfakta-1990-2017.md` resolves: the
        # build strips the trailing `.md` for both the stored source and the
        # curated-map lookup.
        data, code = _run_json(["--db", doc_db_path, "docs", "get", "MappedVar"])
        assert code == 0
        assert data["source"] == "lisa-bakgrundsfakta-1990-2017"  # `.md` stripped
        assert data["source_url"] == _LISA_BAKGRUNDSFAKTA_URL
        assert data["source_title"] == _LISA_BAKGRUNDSFAKTA_TITLE

    def test_get_unmapped_has_null_url_and_title(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "docs", "get", "UnmappedVar"])
        assert code == 0
        assert data["source"] == "some-uncurated-source"
        assert data["source_url"] is None
        assert data["source_title"] is None

    def test_search_carries_source_url_and_title(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "docs", "search", "kurerad"])
        assert code == 0
        hit = next(r for r in data["results"] if r["variable"] == "MappedVar")
        assert hit["source_url"] == _LISA_BAKGRUNDSFAKTA_URL
        assert hit["source_title"] == _LISA_BAKGRUNDSFAKTA_TITLE

    def test_search_unmapped_has_null_url_and_title(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "docs", "search", "omappad"])
        assert code == 0
        hit = next(r for r in data["results"] if r["variable"] == "UnmappedVar")
        assert hit["source_url"] is None
        assert hit["source_title"] is None


# ---------------------------------------------------------------------------
# doc list
# ---------------------------------------------------------------------------


class TestDocList:
    def test_list_summary(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "docs", "list"])
        assert code == 0
        assert data["total_count"] == 6
        assert "testreg" in data["registers"]
        assert data["registers"]["testreg"] == 6

    def test_list_summary_has_types(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "docs", "list"])
        assert code == 0
        assert "type/variable" in data["types"]
        assert data["types"]["type/variable"] == 4

    def test_list_summary_has_topics(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "docs", "list"])
        assert code == 0
        assert "topic/demographic" in data["topics"]

    def test_list_filter_by_topic(self, doc_db_path: str):
        data, code = _run_json(
            ["--db", doc_db_path, "docs", "list", "--topic", "demographic"]
        )
        assert code == 0
        assert data["total_count"] == 1
        assert data["results"][0]["variable"] == "Kommun"

    def test_list_filter_by_type(self, doc_db_path: str):
        data, code = _run_json(
            ["--db", doc_db_path, "docs", "list", "--type", "methodology"]
        )
        assert code == 0
        assert data["total_count"] == 1
        assert data["results"][0]["filename"] == "_methodology-employment.md"

    def test_list_filter_by_register(self, doc_db_path: str):
        data, code = _run_json(
            ["--db", doc_db_path, "docs", "list", "--register", "testreg"]
        )
        assert code == 0
        assert data["total_count"] == 6

    def test_list_omits_docs_dir(self, doc_db_path: str):
        data, code = _run_json(["--db", doc_db_path, "docs", "list"])
        assert code == 0
        assert "docs_dir" not in data


# ---------------------------------------------------------------------------
# build-docs
# ---------------------------------------------------------------------------


class TestDocDbRequired:
    """Query commands refuse to run without a doc DB installed."""

    def test_search_without_docs_raises(self, tmp_path: Path):
        # Build a main DB in an empty dir — no doc DB present.
        from _csv_fixtures import write_scb_input
        from reg_meta_build.db import build_db

        write_scb_input(tmp_path / "input", include=("registerinformation",))
        db_dir = tmp_path / "db"
        db_dir.mkdir()
        build_db(
            input_dir=tmp_path / "input",
            db_dir=db_dir,
            skip_classifications=True,
            skip_slugs=True,
        )

        data, code = _run_json(
            ["--db", str(db_dir), "search", "--query", "testvariabel"],
            verbose=True,
        )
        # Doc DB is required; code is EXIT_CONFIG (10) with the structured
        # doc_db_not_found error the CLI surfaces on missing artifacts.
        assert code == 10
        assert data["error"]["code"] == "doc_db_not_found"

    def test_get_without_docs_raises(self, tmp_path: Path):
        from _csv_fixtures import write_scb_input
        from reg_meta_build.db import build_db

        write_scb_input(tmp_path / "input", include=("registerinformation",))
        db_dir = tmp_path / "db"
        db_dir.mkdir()
        build_db(
            input_dir=tmp_path / "input",
            db_dir=db_dir,
            skip_classifications=True,
            skip_slugs=True,
        )

        data, code = _run_json(
            ["--db", str(db_dir), "get", "register", "1"],
            verbose=True,
        )
        assert code == 10
        assert data["error"]["code"] == "doc_db_not_found"


class TestBuildDocs:
    def test_build_docs(self, tmp_path: Path):
        """`reg-meta-build build-docs` produces a usable doc DB. Lives here
        for the moment because the CLI smoke tests for build commands grew
        up alongside the query-side doc tests; could move to
        reg_meta_build/tests/ later."""
        from reg_meta_build.cli import run as build_run

        docs_dir = tmp_path / "docs" / "myreg"
        docs_dir.mkdir(parents=True)
        (docs_dir / "Var1.md").write_text(
            "---\nvariable: Var1\ndisplay_name: Test\ntags:\n  - type/variable\n---\n\nBody text.\n",
            encoding="utf-8",
        )
        db_dir = tmp_path / "db"
        db_dir.mkdir()

        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = io.StringIO()
        sys.stderr = io.StringIO()
        try:
            code = build_run(
                [
                    "--db",
                    str(db_dir),
                    "build-docs",
                    "--docs-dir",
                    str(tmp_path / "docs"),
                ]
            )
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr
        assert code == 0
        assert (db_dir / "reg_meta_docs.db").exists()


# ---------------------------------------------------------------------------
# Search integration (doc results in reg-meta search)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def combined_db_dir(tmp_path_factory: pytest.TempPathFactory, doc_db_dir: Path) -> str:
    """Create a DB dir with both reg_meta.db and reg_meta_docs.db."""
    import shutil

    from reg_meta_build.db import build_db

    combined = tmp_path_factory.mktemp("combined")

    # Build a minimal metadata DB
    input_dir = tmp_path_factory.mktemp("input_combined")
    from _csv_fixtures import write_scb_input

    write_scb_input(input_dir, include=("registerinformation",))
    build_db(
        input_dir=input_dir, db_dir=combined, skip_classifications=True, skip_slugs=True
    )

    # Copy the doc DB alongside it
    shutil.copy(doc_db_dir / "reg_meta_docs.db", combined / "reg_meta_docs.db")

    return str(combined)


class TestSearchIntegration:
    def test_docs_preserve_catalog_order_when_advancing_cursor(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Raw rank conflicts must not reorder the cursor-bearing catalog rows."""
        import reg_meta.cli as cli

        class _Connection:
            def close(self) -> None:
                pass

        class _Row:
            def __init__(self, fqid: str, rank: float) -> None:
                self._data = {"type": "variable", "fqid": fqid, "rank": rank}

            def model_dump(self, **_kwargs: object) -> dict[str, object]:
                return self._data.copy()

        pages = {
            None: SimpleNamespace(
                # Public order is A, B even though B has the better raw rank.
                results=(_Row("scb/reg/a", 10.0), _Row("scb/reg/b", 0.0)),
                has_more=True,
                page_cursor="start",
                cursors_after=("after-a", "after-b"),
            ),
            "after-a": SimpleNamespace(
                results=(_Row("scb/reg/b", 0.0), _Row("scb/reg/c", 20.0)),
                has_more=False,
                page_cursor="after-a",
                cursors_after=("after-b", "after-c"),
            ),
        }
        monkeypatch.setattr(cli, "db_path_from_args", lambda _db: "unused")
        monkeypatch.setattr(cli, "open_db", lambda _db: _Connection())
        monkeypatch.setattr(cli, "get_db_info", lambda _conn: {})
        monkeypatch.setattr(
            cli, "search", lambda _conn, _query, **kwargs: pages[kwargs["cursor"]]
        )
        monkeypatch.setattr(
            cli,
            "_search_docs",
            lambda _query, db_arg=None: [
                {"type": "doc", "display_name": "Guide", "rank": -100.0}
            ],
        )

        args = Namespace(
            db=None,
            query="needle",
            field="all",
            type="all",
            register=None,
            years=None,
            limit=2,
            cursor=None,
            no_fold=False,
        )
        first, code = cli._cmd_search(args)
        assert code == 0
        assert [row.get("fqid") for row in first["data"]["results"]] == [
            None,
            "scb/reg/a",
        ]
        assert first["data"]["next_cursor"] == "after-a"

        args.cursor = first["data"]["next_cursor"]
        second, code = cli._cmd_search(args)
        assert code == 0
        catalog_fqids = [
            row["fqid"]
            for page in (first, second)
            for row in page["data"]["results"]
            if row["type"] != "doc"
        ]
        assert catalog_fqids == ["scb/reg/a", "scb/reg/b", "scb/reg/c"]
        assert len(catalog_fqids) == len(set(catalog_fqids))
        assert not second["data"]["has_more"]

    def test_search_includes_doc_results(self, combined_db_dir: str):
        """Doc results must appear in default search."""
        data, code = _run_json(
            ["--db", combined_db_dir, "search", "--query", "kommun", "--field", "all"],
            verbose=True,
        )
        assert code == 0
        types = {r["type"] for r in data["data"]["results"]}
        assert "doc" in types, "Doc results should appear in default search"

    def test_search_doc_hint_when_truncated(self, combined_db_dir: str):
        """When doc results are cut off by limit, doc_hint should be in JSON data."""
        # First verify docs exist for this query
        docs_data, _ = _run_json(
            ["--db", combined_db_dir, "docs", "search", "kommun"],
        )
        doc_count = docs_data.get("total_count", 0)
        if doc_count <= 1:
            pytest.skip("Not enough doc results to exercise truncation")

        # Search with a one-row page so additional doc results are truncated.
        data, code = _run_json(
            [
                "--db",
                combined_db_dir,
                "search",
                "--query",
                "kommun",
                "--limit",
                "1",
            ],
        )
        assert code == 0
        assert "not shown" in data.get("doc_hint", "")

    def test_docs_do_not_displace_catalog_continuation(
        self, combined_db_dir: str
    ) -> None:
        first, code = _run_json(
            [
                "--db",
                combined_db_dir,
                "search",
                "--query",
                "kommun",
                "--limit",
                "1",
            ]
        )
        assert code == 0
        if not first["has_more"]:
            pytest.skip("Fixture query has no displaced catalog continuation")
        assert first["next_cursor"] is not None

        second, code = _run_json(
            [
                "--db",
                combined_db_dir,
                "search",
                "--query",
                "kommun",
                "--limit",
                "1",
                "--cursor",
                first["next_cursor"],
            ]
        )
        assert code == 0
        assert second["results"]
        assert second["results"] != first["results"]
        assert not second["has_more"] or second["next_cursor"] is not None

    def test_search_exact_variable_name_ranked_high(self, combined_db_dir: str):
        """Exact variable name match in docs should rank near the top."""
        data, code = _run_json(
            ["--db", combined_db_dir, "search", "--query", "Kommun", "--field", "all"],
            verbose=True,
        )
        assert code == 0
        results = data["data"]["results"]
        doc_results = [r for r in results if r["type"] == "doc"]
        assert len(doc_results) >= 1

        # The exact match on variable name "Kommun" should be in the first 5 results
        top5_types = [r["type"] for r in results[:5]]
        assert "doc" in top5_types, (
            f"Doc result for exact variable name match should be in top 5, "
            f"got types: {top5_types}"
        )
