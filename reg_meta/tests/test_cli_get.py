"""CLI `get register|varinfo|datacolumns|coded-variables|lineage|availability`."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING

from cli_test_support import run_json as _run_json
from reg_meta.cli import run

if TYPE_CHECKING:
    import pytest

# ---------------------------------------------------------------------------
# Get register
# ---------------------------------------------------------------------------


class TestGetRegister:
    def test_by_id(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "register", "1"])
        assert code == 0
        assert data["data"]["name"] == "TESTREG"
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
# Get varinfo
# ---------------------------------------------------------------------------


class TestGetVarinfo:
    def test_by_var_id(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "get", "varinfo", "100", "--register", "1"]
        )
        assert code == 0
        assert data["data"]["name"] == "TestVar"

    def test_cross_register(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "varinfo", "44"])
        assert code == 0
        # var_id 44 exists in both registers
        assert "variables" in data["data"]
        assert len(data["data"]["variables"]) == 2

    def test_correction_provenance_is_printed(
        self, db_path: str, capsys: pytest.CaptureFixture[str]
    ) -> None:
        provenance = (
            "errata:scoped-attributions\n"
            '[{"class":"omitted-column-in-version",'
            '"evidence":"The steward holds this delivery, which SCB omits.",'
            '"source_editions":["Höstterminen 2020"]}]'
        )
        with sqlite3.connect(Path(db_path) / "reg_meta.db") as conn:
            conn.execute(
                "UPDATE variable_state SET provenance = ? "
                "WHERE state_id = (SELECT MIN(vs.state_id) FROM variable_state vs "
                "JOIN variable v ON v.variable_id = vs.variable_id "
                "WHERE v.register_id = 1 AND v.provider_key = '44')",
                (provenance,),
            )

        code = run(
            [
                "--format",
                "list",
                "--db",
                db_path,
                "get",
                "varinfo",
                "Kön",
                "--register",
                "TESTREG",
            ]
        )

        assert code == 0
        output = capsys.readouterr().out
        assert "provenance" in output
        assert provenance in output
        assert '"source_editions":["Höstterminen 2020"]' in output

    def test_value_set_count(self, db_path: str):
        data, _code = _run_json(
            ["--db", db_path, "get", "varinfo", "Kön", "--register", "TESTREG"]
        )
        # The Kön value-set (Man, Kvinna) carries 2 codes; at least one state
        # surfaces that count.
        with_codes = [i for i in data["data"]["instances"] if i["value_set_count"] == 2]
        assert with_codes


# ---------------------------------------------------------------------------
# Get datacolumns
# ---------------------------------------------------------------------------


class TestGetDatacolumns:
    def test_register_filter(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "get", "datacolumns", "Kön", "--register", "TESTREG"]
        )
        assert code == 0
        assert all(r["register_id"] == "1" for r in data["data"])

    def test_full_alias_history_survives_reparent(self):
        """A2.7: `get_datacolumns` reads the re-parented `variable_alias` (full
        history), NOT `variable_state.delivery_column_name` (latest era only).
        Seed two delivery-column eras for one variable (a rename, no shape
        change → one coalesced state carrying only the latest column) and assert
        BOTH columns surface. This fails under the rejected
        `variable_state.delivery_column_name` alternative."""
        from _slugged_db import build_slugged_db
        from reg_meta.queries import get_datacolumns

        # Default variable: register 1 (lisa), variant 10, slug 'kon'. Its state
        # + one alias ('Kon') are seeded by build_slugged_db; add an older
        # delivery column for the SAME variable to simulate a rename history.
        conn = build_slugged_db()
        conn.execute(
            "INSERT INTO variable_alias "
            "(variable_id, register_variant_id, delivery_column_name) "
            "SELECT variable_id, 10, 'Kon_OLD' FROM variable WHERE slug = 'kon'"
        )
        conn.commit()
        cols = {r["delivery_column_name"] for r in get_datacolumns(conn, "Kön")}
        assert cols == {"Kon", "Kon_OLD"}


# ---------------------------------------------------------------------------
# Get coded-variables
# ---------------------------------------------------------------------------


class TestGetCodedVariables:
    def test_min_codes_filter(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "get", "coded-variables", "--min-codes", "3"]
        )
        assert code == 0
        assert all(r["n_distinct_codes"] >= 3 for r in data["data"])


# ---------------------------------------------------------------------------
# Get lineage
# ---------------------------------------------------------------------------


class TestGetLineage:
    def test_basic_lineage(self, db_path: str):
        """Kön appears in TESTREG (source) and OTHERREG (consumer)."""
        data, code = _run_json(["--db", db_path, "get", "lineage", "Kön"])
        assert code == 0
        assert data["data"]["variable_name"] == "Kön"
        regs = data["data"]["registers"]
        assert len(regs) == 2
        roles = {r["register_name"]: r["role"] for r in regs}
        # TESTREG has no provenance → unknown (no source fields set)
        # OTHERREG has source_register_text=TESTREG → consumer
        assert roles["OTHERREG"] == "consumer"

    def test_source_resolution(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "lineage", "Kön"])
        assert code == 0
        otherreg = next(
            r for r in data["data"]["registers"] if r["register_name"] == "OTHERREG"
        )
        # TESTREG should resolve to register_id "1"
        assert otherreg["source_register_id"] == "1"
        assert otherreg["source_register_text"] == "TESTREG"

    def test_no_provenance_is_unknown(self, db_path: str):
        """UniqueVar has no provenance fields → role = unknown."""
        data, code = _run_json(["--db", db_path, "get", "lineage", "UniqueVar"])
        assert code == 0
        regs = data["data"]["registers"]
        assert len(regs) == 1
        assert regs[0]["role"] == "unknown"

    def test_register_filter(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "get", "lineage", "Kön", "--register", "OTHERREG"]
        )
        assert code == 0
        regs = data["data"]["registers"]
        assert len(regs) == 1
        assert regs[0]["register_name"] == "OTHERREG"

    def test_not_found(self, db_path: str):
        _data, code = _run_json(["--db", db_path, "get", "lineage", "NONEXISTENT"])
        assert code == 16

    def test_year_range(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "lineage", "Kön"])
        assert code == 0
        testreg = next(
            r for r in data["data"]["registers"] if r["register_name"] == "TESTREG"
        )
        # A2.6: year range spans the variable_state validity windows; the count
        # is of states now (the 2020+2021 cvids coalesced into one 2020-2021
        # state, plus the 2022 state → 2 states spanning 2020..2022).
        assert testreg["year_range"] == [2020, 2022]
        assert testreg["instance_count"] == 2


# ---------------------------------------------------------------------------
# Get availability
# ---------------------------------------------------------------------------


class TestGetAvailability:
    def test_variable_availability(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "availability", "Kön"])
        assert code == 0
        d = data["data"]
        assert d["target_type"] == "variable"
        assert d["variable_name"] == "Kön"
        assert d["min_year"] <= d["max_year"]
        assert len(d["years"]) >= 1
        assert len(d["registers"]) >= 1

    def test_variable_availability_with_register(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "get", "availability", "Kön", "--register", "TESTREG"]
        )
        assert code == 0
        d = data["data"]
        assert d["target_type"] == "variable"
        # Should only have TESTREG
        assert all(r["register_id"] == "1" for r in d["registers"])

    def test_not_found(self, db_path: str):
        _, code = _run_json(["--db", db_path, "get", "availability", "NONEXISTENT"])
        assert code == 16
