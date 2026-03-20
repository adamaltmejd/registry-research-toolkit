"""Tests for query commands (Phase 2)."""

from __future__ import annotations

import json


from regmeta.cli import run


def _run_json(argv: list[str]) -> tuple[dict, int]:
    """Run a CLI command and parse the JSON output."""
    import io
    import sys

    old_stdout = sys.stdout
    sys.stdout = buf = io.StringIO()
    try:
        exit_code = run(argv)
    finally:
        sys.stdout = old_stdout
    output = buf.getvalue()
    if output.strip():
        return json.loads(output), exit_code
    return {}, exit_code


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------


class TestSearch:
    def test_search_variable(self, db_path: str):
        data, code = _run_json(["--db", db_path, "search", "--query", "testvariabel"])
        assert code == 0
        assert data["data"]["total_count"] >= 1
        result = data["data"]["results"][0]
        assert result["type"] == "variable"
        assert result["var_id"] == "100"

    def test_search_register(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "search", "--query", "Testning", "--type", "register"]
        )
        assert code == 0
        assert data["data"]["total_count"] >= 1
        assert data["data"]["results"][0]["register_id"] == "1"

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
        for r in data["data"]["results"]:
            assert r["register_id"] == "1"

    def test_search_register_filter_no_match(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "search", "--query", "Kön", "--register", "NONEXISTENT"]
        )
        assert code == 0
        assert data["data"]["total_count"] == 0

    def test_search_pagination(self, db_path: str):
        data_all, _ = _run_json(["--db", db_path, "search", "--query", "Kön"])
        data_page, _ = _run_json(
            [
                "--db",
                db_path,
                "search",
                "--query",
                "Kön",
                "--limit",
                "1",
                "--offset",
                "0",
            ]
        )
        assert len(data_page["data"]["results"]) <= 1
        assert data_page["data"]["total_count"] == data_all["data"]["total_count"]

    def test_search_swedish_chars(self, db_path: str):
        data, _ = _run_json(["--db", db_path, "search", "--query", "svenska"])
        assert data["data"]["total_count"] >= 1


# ---------------------------------------------------------------------------
# Get register
# ---------------------------------------------------------------------------


class TestGetRegister:
    def test_by_id(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "register", "1"])
        assert code == 0
        assert data["data"]["registernamn"] == "TESTREG"
        assert len(data["data"]["variants"]) == 1

    def test_by_name(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "register", "TESTREG"])
        assert code == 0
        assert data["data"]["register_id"] == "1"

    def test_fuzzy_match(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "register", "TEST"])
        assert code == 0
        # "TEST" matches "TESTREG" by substring
        if "registers" in data["data"]:
            ids = [r["register_id"] for r in data["data"]["registers"]]
            assert "1" in ids
        else:
            assert data["data"]["register_id"] == "1"

    def test_not_found(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "register", "ZZZNONEXIST"])
        assert code == 16
        assert data["error"]["code"] == "not_found"


# ---------------------------------------------------------------------------
# Get schema
# ---------------------------------------------------------------------------


class TestGetSchema:
    def test_by_regvar_id(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "schema", "10"])
        assert code == 0
        variants = data["data"]["variants"]
        assert len(variants) == 1
        assert variants[0]["regvar_id"] == "10"
        assert len(variants[0]["versions"]) == 3  # 2020, 2021, 2022

    def test_by_register(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "get", "schema", "--register", "TESTREG"]
        )
        assert code == 0
        assert len(data["data"]["variants"]) == 1

    def test_years_single(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "get", "schema", "10", "--years", "2020"]
        )
        assert code == 0
        versions = data["data"]["variants"][0]["versions"]
        assert len(versions) == 1
        assert versions[0]["year"] == 2020

    def test_years_range(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "get", "schema", "10", "--years", "2020-2021"]
        )
        assert code == 0
        versions = data["data"]["variants"][0]["versions"]
        years = [v["year"] for v in versions]
        assert set(years) == {2020, 2021}

    def test_years_open_end(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "get", "schema", "10", "--years", "2022-"]
        )
        assert code == 0
        versions = data["data"]["variants"][0]["versions"]
        assert all(v["year"] >= 2022 for v in versions)

    def test_columns_include_aliases(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "get", "schema", "10", "--years", "2020"]
        )
        columns = data["data"]["variants"][0]["versions"][0]["columns"]
        # Find the TestVar column — it should show aliases
        testvar_cols = [c for c in columns if c["var_id"] == "100"]
        assert len(testvar_cols) == 1
        assert (
            "TestCol" in testvar_cols[0]["aliases"]
            or "TestKolumn" in testvar_cols[0]["aliases"]
        )

    def test_not_found(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "schema", "99999"])
        assert code == 16

    def test_no_args(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "schema"])
        assert code == 2


# ---------------------------------------------------------------------------
# Get varinfo
# ---------------------------------------------------------------------------


class TestGetVarinfo:
    def test_by_name(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "get", "varinfo", "Kön", "--register", "TESTREG"]
        )
        assert code == 0
        assert data["data"]["variabelnamn"] == "Kön"
        assert data["data"]["register_id"] == "1"
        assert len(data["data"]["instances"]) == 3  # CVIDs 1001, 1003, 1004

    def test_by_var_id(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "get", "varinfo", "100", "--register", "1"]
        )
        assert code == 0
        assert data["data"]["variabelnamn"] == "TestVar"

    def test_cross_register(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "varinfo", "44"])
        assert code == 0
        # var_id 44 exists in both registers
        assert "variables" in data["data"]
        assert len(data["data"]["variables"]) == 2

    def test_instance_details(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "get", "varinfo", "Kön", "--register", "TESTREG"]
        )
        inst = data["data"]["instances"][0]
        assert "cvid" in inst
        assert "year" in inst
        assert "aliases" in inst
        assert "value_set_count" in inst

    def test_value_set_count(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "get", "varinfo", "Kön", "--register", "TESTREG"]
        )
        # CVID 1001 has 2 value items (Man, Kvinna)
        cvid_1001 = [i for i in data["data"]["instances"] if i["cvid"] == "1001"]
        assert len(cvid_1001) == 1
        assert cvid_1001[0]["value_set_count"] == 2

    def test_not_found(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "varinfo", "NONEXISTENT"])
        assert code == 16


# ---------------------------------------------------------------------------
# Get values
# ---------------------------------------------------------------------------


class TestGetValues:
    def test_values(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "values", "1001"])
        assert code == 0
        assert len(data["data"]) == 2
        codes = {v["vardekod"] for v in data["data"]}
        assert codes == {"1", "2"}

    def test_valid_at_within_range(self, db_path: str):
        """Item 5001 is valid 2000-2010, 5002 always valid → both returned."""
        data, code = _run_json(
            ["--db", db_path, "get", "values", "1001", "--valid-at", "2005-06-15"]
        )
        assert code == 0
        codes = {v["vardekod"] for v in data["data"]}
        assert codes == {"1", "2"}

    def test_valid_at_outside_range(self, db_path: str):
        """Item 5001 expired after 2010 → only 5002 (always valid) returned."""
        data, code = _run_json(
            ["--db", db_path, "get", "values", "1001", "--valid-at", "2020-01-01"]
        )
        assert code == 0
        codes = {v["vardekod"] for v in data["data"]}
        assert codes == {"2"}

    def test_valid_at_bad_format(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "get", "values", "1001", "--valid-at", "2020/01/01"]
        )
        assert code == 2
        assert data["error"]["code"] == "bad_date"

    def test_not_found(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "values", "99999"])
        assert code == 16


# ---------------------------------------------------------------------------
# Get datacolumns
# ---------------------------------------------------------------------------


class TestGetDatacolumns:
    def test_by_name(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "datacolumns", "Kön"])
        assert code == 0
        col_names = {r["kolumnnamn"] for r in data["data"]}
        assert "Kon" in col_names or "KON" in col_names

    def test_register_filter(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "get", "datacolumns", "Kön", "--register", "TESTREG"]
        )
        assert code == 0
        assert all(r["register_id"] == "1" for r in data["data"])

    def test_alias_anomaly(self, db_path: str):
        """TestVar should show both TestCol and TestKolumn aliases."""
        data, code = _run_json(
            ["--db", db_path, "get", "datacolumns", "TestVar", "--register", "TESTREG"]
        )
        assert code == 0
        col_names = {r["kolumnnamn"] for r in data["data"]}
        assert "TestCol" in col_names
        assert "TestKolumn" in col_names

    def test_not_found(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "datacolumns", "NONEXISTENT"])
        assert code == 16


# ---------------------------------------------------------------------------
# Get coded-variables
# ---------------------------------------------------------------------------


class TestGetCodedVariables:
    def test_returns_results(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "coded-variables"])
        assert code == 0
        assert len(data["data"]) >= 1
        first = data["data"][0]
        assert "variable_name" in first
        assert "n_distinct_codes" in first
        assert "n_registers" in first

    def test_min_codes_filter(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "get", "coded-variables", "--min-codes", "3"]
        )
        assert code == 0
        assert all(r["n_distinct_codes"] >= 3 for r in data["data"])

    def test_min_registers_filter(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "get", "coded-variables", "--min-registers", "2"]
        )
        assert code == 0
        assert all(r["n_registers"] >= 2 for r in data["data"])

    def test_kon_present(self, db_path: str):
        """Kön has value items in our fixtures → should appear."""
        data, code = _run_json(["--db", db_path, "get", "coded-variables"])
        names = {r["variable_name"] for r in data["data"]}
        assert "Kön" in names


# ---------------------------------------------------------------------------
# Resolve
# ---------------------------------------------------------------------------


class TestResolve:
    def test_exact_match(self, db_path: str):
        data, code = _run_json(["--db", db_path, "resolve", "--columns", "Kon"])
        assert code == 0
        col = data["data"]["columns"][0]
        assert col["status"] == "matched"
        assert len(col["matches"]) >= 1

    def test_register_filter(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "resolve", "--columns", "Kon", "--register", "TESTREG"]
        )
        assert code == 0
        col = data["data"]["columns"][0]
        assert col["status"] == "matched"
        assert all(m["register_id"] == "1" for m in col["matches"])

    def test_cross_register(self, db_path: str):
        data, code = _run_json(["--db", db_path, "resolve", "--columns", "Kon"])
        col = data["data"]["columns"][0]
        reg_ids = {m["register_id"] for m in col["matches"]}
        # "Kon" is in reg 1, "KON" is in reg 2 — case-insensitive should match both
        assert "1" in reg_ids

    def test_case_insensitive(self, db_path: str):
        data, _ = _run_json(["--db", db_path, "resolve", "--columns", "kon"])
        col = data["data"]["columns"][0]
        assert col["status"] == "matched"

    def test_no_match(self, db_path: str):
        data, code = _run_json(["--db", db_path, "resolve", "--columns", "ZZZNOPE"])
        assert code == 0
        col = data["data"]["columns"][0]
        assert col["status"] == "no_match"
        assert col["matches"] == []

    def test_require_match_fails(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "resolve", "--columns", "ZZZNOPE", "--require-match"]
        )
        assert code == 17

    def test_batch(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "resolve", "--columns", "Kon,TestCol,ZZZNOPE"]
        )
        assert code == 0
        columns = data["data"]["columns"]
        assert len(columns) == 3
        assert columns[0]["status"] == "matched"
        assert columns[1]["status"] == "matched"
        assert columns[2]["status"] == "no_match"

    def test_alias_anomaly(self, db_path: str):
        """Both TestCol and TestKolumn should resolve to var 100."""
        data1, _ = _run_json(
            [
                "--db",
                db_path,
                "resolve",
                "--columns",
                "TestCol",
                "--register",
                "TESTREG",
            ]
        )
        data2, _ = _run_json(
            [
                "--db",
                db_path,
                "resolve",
                "--columns",
                "TestKolumn",
                "--register",
                "TESTREG",
            ]
        )
        assert data1["data"]["columns"][0]["matches"][0]["var_id"] == "100"
        assert data2["data"]["columns"][0]["matches"][0]["var_id"] == "100"

    def test_no_confidence_or_reasons(self, db_path: str):
        """Resolve v2 should not include confidence or match_reasons."""
        data, _ = _run_json(["--db", db_path, "resolve", "--columns", "Kon"])
        match = data["data"]["columns"][0]["matches"][0]
        assert "confidence" not in match
        assert "match_reasons" not in match

    def test_no_ambiguous_status(self, db_path: str):
        """Resolve v2 should not return 'ambiguous' status."""
        data, _ = _run_json(["--db", db_path, "resolve", "--columns", "Kon"])
        assert data["data"]["columns"][0]["status"] in ("matched", "no_match")


# ---------------------------------------------------------------------------
# Envelope and error model
# ---------------------------------------------------------------------------


class TestEnvelope:
    def test_contract_version(self, db_path: str):
        data, _ = _run_json(["--db", db_path, "search", "--query", "test"])
        assert data["contract_version"] == "2.0.0"

    def test_envelope_fields(self, db_path: str):
        data, _ = _run_json(["--db", db_path, "search", "--query", "test"])
        assert "generated_at" in data
        assert "request" in data
        assert "database" in data
        assert "data" in data
        assert "run" in data
        assert "duration_ms" in data["run"]

    def test_table_format_works(self, db_path: str):
        import io
        import sys

        old_stdout = sys.stdout
        sys.stdout = buf = io.StringIO()
        try:
            code = run(
                [
                    "--db",
                    db_path,
                    "--format",
                    "table",
                    "search",
                    "--query",
                    "testvariabel",
                ]
            )
        finally:
            sys.stdout = old_stdout
        output = buf.getvalue()
        assert code == 0
        assert "TestVar" in output
        assert "---" in output

    def test_no_command(self):
        _, code = _run_json([])
        assert code == 2
