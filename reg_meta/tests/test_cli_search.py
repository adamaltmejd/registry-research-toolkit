"""CLI `search`: result types, filters, pagination and code matching."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from cli_test_support import build_cli_source, run_json as _run_json
from reg_meta.cli import run

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def test_code_matching_needs_a_digit_and_three_characters(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Codes ZQX9 and Q71 have labels unrelated to their code text."""
    db = build_cli_source(tmp_path, "cli-code-shape")

    def codes(query: str) -> list[str]:
        assert run(["--db", db, "--format", "json", "search", "--query", query]) == 0
        results = json.loads(capsys.readouterr().out)["results"]
        return [row["code"] for row in results if row["type"] == "code"]

    assert codes("ZQX9") == ["ZQX9"]
    assert codes("Q71") == ["Q71"]
    assert codes(" ZQX9 ") == ["ZQX9"]
    assert codes("ZQX") == []
    assert codes("Q7") == []
    assert codes(" Q7 ") == []


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------


class TestSearch:
    def test_search_variable(self, db_path: str):
        data, code = _run_json(["--db", db_path, "search", "--query", "testvariabel"])
        assert code == 0
        assert len(data["data"]["results"]) >= 1
        result = data["data"]["results"][0]
        assert result["type"] == "variable"
        # The typed `variable` row (#701) carries the navigable fqid/name, not the
        # internal var_id (that's the CLI-only datacolumn/varname arms' identity).
        assert result["fqid"] == "scb/testreg/testcol"
        assert result["name"] == "TestVar"

    def test_search_register(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "search", "--query", "Testning", "--type", "register"]
        )
        assert code == 0
        assert len(data["data"]["results"]) >= 1
        # The typed `register` row (#701) carries the navigable fqid, not register_id.
        assert data["data"]["results"][0]["fqid"] == "scb/testreg"

    def test_search_type_filter(self, db_path: str):
        data, _ = _run_json(
            ["--db", db_path, "search", "--query", "Kön", "--type", "variable"]
        )
        variable_types = {"variable", "varname", "datacolumn", "value"}
        for r in data["data"]["results"]:
            assert r["type"] in variable_types

    def test_search_register_filter(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "search", "--query", "Kön", "--register", "TESTREG"]
        )
        assert code == 0
        # The typed rows (#701) carry no internal register_id; the register-scope
        # guarantee is now read off the `register` display-name field, present on
        # every register-bearing arm (variable/varname/datacolumn/code-owner).
        for r in data["data"]["results"]:
            assert r["register"] == "TESTREG"

    def test_search_register_filter_no_match(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "search", "--query", "Kön", "--register", "NONEXISTENT"]
        )
        assert code == 0
        assert len(data["data"]["results"]) == 0

    def test_search_pagination(self, db_path: str):
        first, _ = _run_json(
            [
                "--db",
                db_path,
                "search",
                "--query",
                "Kön",
                "--limit",
                "1",
            ]
        )
        assert len(first["data"]["results"]) <= 1
        if first["data"]["has_more"]:
            second, _ = _run_json(
                [
                    "--db",
                    db_path,
                    "search",
                    "--query",
                    "Kön",
                    "--limit",
                    "1",
                    "--cursor",
                    first["data"]["next_cursor"],
                ]
            )
            assert second["data"]["results"] != first["data"]["results"]

    def test_search_swedish_chars(self, db_path: str):
        data, _ = _run_json(["--db", db_path, "search", "--query", "svenska"])
        assert len(data["data"]["results"]) >= 1

    def test_search_value_code(self, db_path: str):
        """Search for a value label returns `code`-type hits (#352): label FTS over
        value_code_fts, each annotated with its owning variable(s) via
        code_variable_map."""
        data, code = _run_json(["--db", db_path, "search", "--query", "Man"])
        assert code == 0
        code_results = [r for r in data["data"]["results"] if r["type"] == "code"]
        assert len(code_results) >= 1
        man = next(r for r in code_results if r["label"] == "Man")
        assert man["code"] == "1"
        # The hit names its owning variable(s) (variable_id-grained map), each
        # FQID-addressable; the full owner count is also reported.
        assert man["variables"], "code hit should carry owning variables"
        assert man["variables"][0]["fqid"]
        assert man["variable_count"] >= 1

    def test_search_years_filter(self, db_path: str):
        """--years filters to results with versions in the given range."""
        # Kön exists at 2020-2022; filtering to 2020 should still find it
        data, code = _run_json(
            ["--db", db_path, "search", "--query", "Kön", "--years", "2020"]
        )
        assert code == 0
        assert len(data["data"]["results"]) >= 1

    def test_search_years_excludes_outside_range(self, db_path: str):
        """--years filters out results with no versions in range."""
        data, code = _run_json(
            ["--db", db_path, "search", "--query", "Kön", "--years", "2050"]
        )
        assert code == 0
        assert len(data["data"]["results"]) == 0

    def test_search_years_range(self, db_path: str):
        """--years accepts a range like 2020-2021."""
        data, code = _run_json(
            ["--db", db_path, "search", "--query", "Kön", "--years", "2020-2021"]
        )
        assert code == 0
        assert len(data["data"]["results"]) >= 1

    def test_search_years_register_type(self, db_path: str):
        """--years works with --type register."""
        data, code = _run_json(
            [
                "--db",
                db_path,
                "search",
                "--query",
                "Testning",
                "--type",
                "register",
                "--years",
                "2020",
            ]
        )
        assert code == 0
        assert len(data["data"]["results"]) >= 1

    def test_search_years_register_type_no_match(self, db_path: str):
        data, code = _run_json(
            [
                "--db",
                db_path,
                "search",
                "--query",
                "Testning",
                "--type",
                "register",
                "--years",
                "1900",
            ]
        )
        assert code == 0
        assert len(data["data"]["results"]) == 0
