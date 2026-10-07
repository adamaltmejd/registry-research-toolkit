"""CLI `get schema`: editions, year filters, column filters and display modes."""

from __future__ import annotations

import json
import sqlite3
from typing import TYPE_CHECKING

from cli_test_support import build_cli_source, run_json as _run_json
from reg_meta.cli import run

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def test_open_year_bounds_keep_every_window_on_their_side(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = build_cli_source(tmp_path, "cli-periods")
    argv = ["--db", db, "--format", "json", "get", "schema", "--register", "Terms"]

    def windows(years: str) -> list[list[str]]:
        assert run([*argv, "--years", years]) == 0
        output = json.loads(capsys.readouterr().out)
        return [
            [version["valid_from"], version["valid_to"]]
            for variant in output["variants"]
            for version in variant["versions"]
        ]

    assert windows("-2019") == [
        ["2015-01-01", "2015-06-30"],
        ["2015-07-01", "2015-12-31"],
        ["2018-04-01", "2018-06-30"],
        ["2019-03-01", "2019-03-31"],
    ]
    assert windows("2019-") == [
        ["2019-03-01", "2019-03-31"],
        ["2020-01-01", "2020-12-31"],
        ["2021-01-01", "9999-12-31"],
    ]


# ---------------------------------------------------------------------------
# Get schema
# ---------------------------------------------------------------------------


class TestGetSchema:
    def test_by_register_variant_id(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "schema", "10"])
        assert code == 0
        variants = data["data"]["variants"]
        assert len(variants) == 1
        assert variants[0]["register_variant_id"] == "10"
        assert len(variants[0]["versions"]) == 3  # 2020, 2021, 2022

    def test_columns_include_aliases(self, db_path: str):
        data, _code = _run_json(
            ["--db", db_path, "get", "schema", "10", "--years", "2020"]
        )
        columns = data["data"]["variants"][0]["versions"][0]["columns"]
        # Find the TestVar column — it should show aliases
        testvar_cols = [c for c in columns if c["var_id"] == 100]
        assert len(testvar_cols) == 1
        assert (
            "TestCol" in testvar_cols[0]["aliases"]
            or "TestKolumn" in testvar_cols[0]["aliases"]
        )

    def test_not_found(self, db_path: str):
        _data, code = _run_json(["--db", db_path, "get", "schema", "99999"])
        assert code == 16

    def test_no_args(self, db_path: str):
        _data, code = _run_json(["--db", db_path, "get", "schema"])
        assert code == 2

    def test_columns_like_filter(self, db_path: str):
        """--columns-like filters columns by alias/variable name regex."""
        data, code = _run_json(
            ["--db", db_path, "get", "schema", "10", "--columns-like", "Kön|Test"]
        )
        assert code == 0
        for ver in data["data"]["variants"][0]["versions"]:
            for col in ver["columns"]:
                name = col.get("variable_name", "")
                aliases = col.get("aliases", "")
                assert (
                    "Kön" in name
                    or "Test" in name
                    or "Kön" in aliases
                    or "Test" in aliases
                )

    def test_columns_like_no_match(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "get", "schema", "10", "--columns-like", "ZZZZZ"]
        )
        assert code == 0
        for ver in data["data"]["variants"][0]["versions"]:
            assert ver["columns"] == []

    def test_source_present_for_imported_variable(self, db_path: str):
        """OTHERREG's Kön is imported from TESTREG — source column should show it."""
        data, code = _run_json(
            ["--db", db_path, "get", "schema", "--register", "OTHERREG"]
        )
        assert code == 0
        # A2.6: a var_id's column can land in any validity-window edition; search
        # across all of them rather than pinning versions[0].
        kon = [
            c
            for v in data["data"]["variants"]
            for ver in v["versions"]
            for c in ver["columns"]
            if c["var_id"] == 44
        ]
        assert kon
        assert all(c["source"] == "TESTREG" for c in kon)

    def test_source_empty_for_own_variable(self, db_path: str):
        """TESTREG's own variables have no source."""
        data, code = _run_json(
            [
                "--db",
                db_path,
                "get",
                "schema",
                "--register",
                "TESTREG",
                "--years",
                "2020",
            ]
        )
        assert code == 0
        kon = [
            c
            for v in data["data"]["variants"]
            for ver in v["versions"]
            for c in ver["columns"]
            if c["var_id"] == 44
        ]
        assert kon
        assert all(c["source"] == "" for c in kon)


# ---------------------------------------------------------------------------
# Get diff
# ---------------------------------------------------------------------------


class TestGetDiff:
    def test_basic_diff(self, db_path: str):
        """Diff between 2020 and 2022: ÅÄÖVar added in 2022, TestVar removed after 2020."""
        data, code = _run_json(
            [
                "--db",
                db_path,
                "get",
                "diff",
                "--register",
                "TESTREG",
                "--from",
                "2020",
                "--to",
                "2022",
            ]
        )
        assert code == 0
        assert data["data"]["register_name"] == "TESTREG"
        assert data["data"]["from_year"] == 2020
        assert data["data"]["to_year"] == 2022
        variants = data["data"]["variants"]
        assert len(variants) >= 1
        v = variants[0]
        added_names = {a["variable_name"] for a in v["added"]}
        removed_names = {r["variable_name"] for r in v["removed"]}
        assert "ÅÄÖVar" in added_names
        assert "TestVar" in removed_names

    def test_variable_filter_by_alias(self, db_path: str):
        """Kon is a column alias for Kön — resolved_variables shows the mapping."""
        data, code = _run_json(
            [
                "--db",
                db_path,
                "get",
                "diff",
                "--register",
                "TESTREG",
                "--from",
                "2020",
                "--to",
                "2022",
                "--variable",
                "Kon",
            ]
        )
        assert code == 0
        assert data["data"]["variants"] == []
        assert "Kön" in data["data"]["unchanged"]
        resolved = data["data"]["resolved_variables"]
        assert any(
            r["input"] == "Kon" and r["variable_name"] == "Kön" for r in resolved
        )

    def test_multiple_variables(self, db_path: str):
        """Multiple --variable values filter for all specified variables."""
        data, code = _run_json(
            [
                "--db",
                db_path,
                "get",
                "diff",
                "--register",
                "TESTREG",
                "--from",
                "2020",
                "--to",
                "2022",
                "--variable",
                "Kön",
                "TestVar",
            ]
        )
        assert code == 0
        # TestVar removed in 2022 → should appear in variants
        v = data["data"]["variants"][0]
        removed_names = {r["variable_name"] for r in v["removed"]}
        assert "TestVar" in removed_names
        # Kön unchanged everywhere
        assert "Kön" in data["data"]["unchanged"]
        # Both inputs resolved
        inputs = {r["input"] for r in data["data"]["resolved_variables"]}
        assert inputs == {"Kön", "TestVar"}

    def test_variant_filter(self, db_path: str):
        data, code = _run_json(
            [
                "--db",
                db_path,
                "get",
                "diff",
                "--register",
                "TESTREG",
                "--from",
                "2020",
                "--to",
                "2022",
                "--variant",
                "10",
            ]
        )
        assert code == 0
        assert len(data["data"]["variants"]) >= 1
        assert data["data"]["variants"][0]["register_variant_id"] == "10"

    def test_year_with_no_covering_state_is_empty(self, db_path: str):
        """A2.6: schema-at-year now uses `variable_state` validity overlap (no
        register_version closest-≤-year fallback). 2023 is covered by no state,
        so the diff against it finds no columns and 404s like any empty diff —
        the diff is year-keyed and carries from_year/to_year, not version rows.
        """
        data, code = _run_json(
            [
                "--db",
                db_path,
                "get",
                "diff",
                "--register",
                "TESTREG",
                "--from",
                "2020",
                "--to",
                "2023",
            ]
        )
        # No state covers 2023 → no versions found → not_found (exit 16).
        assert code == 16
        # A real in-range diff (2020→2022) succeeds and is year-keyed.
        data, code = _run_json(
            [
                "--db",
                db_path,
                "get",
                "diff",
                "--register",
                "TESTREG",
                "--from",
                "2020",
                "--to",
                "2022",
            ]
        )
        assert code == 0
        v = data["data"]["variants"][0]
        assert v["from_year"] == 2020
        assert v["to_year"] == 2022

    def test_from_gte_to_error(self, db_path: str):
        data, code = _run_json(
            [
                "--db",
                db_path,
                "get",
                "diff",
                "--register",
                "TESTREG",
                "--from",
                "2022",
                "--to",
                "2020",
            ]
        )
        assert code == 2
        assert data["error"]["code"] == "usage_error"

    def test_register_not_found(self, db_path: str):
        _data, code = _run_json(
            [
                "--db",
                db_path,
                "get",
                "diff",
                "--register",
                "NONEXIST",
                "--from",
                "2020",
                "--to",
                "2022",
            ]
        )
        assert code == 16


def _folded_window_db():
    """In-memory DB with ONE delivery window (2007) holding four columns:
    two ordinary variables (`value_set_version_label=''`) plus a folded
    multi-vintage variable delivering two states (`sni92` + `sni2007`) in the
    SAME window. Exercises that get_schema groups by delivery window, not by
    the per-column vintage label.

    Variable layout under variant 10, all valid 2007-01-01..2007-12-31:
      - kon  (var 44, label '')        ordinary
      - alder(var 45, label '')        ordinary
      - sni  (var 46, label 'sni92')   folded vintage A
      - sni  (var 46, label 'sni2007') folded vintage B
    """

    from reg_meta_build.db import DDL, seed_providers

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(DDL)
    seed_providers(conn)
    conn.execute(
        "INSERT INTO register (register_id, provider_id, slug, name) "
        "VALUES (1, 1, 'r1', 'R1')"
    )
    conn.execute(
        "INSERT INTO register_variant (register_variant_id, register_id, slug, name) "
        "VALUES (10, 1, 'v1', 'V1')"
    )
    for var_id, name, slug in (
        ("44", "Kön", "kon"),
        ("45", "Ålder", "alder"),
        ("46", "Näringsgren SNI", "sni"),
    ):
        conn.execute(
            "INSERT INTO variable (register_id, provider_key, name, slug) "
            "VALUES (1, ?, ?, ?)",
            (var_id, name, slug),
        )
    # window: two ordinary columns + folded variable's two vintage states
    states = (
        ("44", "Kon", ""),
        ("45", "Alder", ""),
        ("46", "Sni", "sni92"),
        ("46", "Sni", "sni2007"),
    )
    for var_id, col, label in states:
        vid = conn.execute(
            "SELECT variable_id FROM variable "
            "WHERE register_id = 1 AND provider_key = ?",
            (var_id,),
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO variable_state (variable_id, register_variant_id, valid_from, "
            "valid_to, data_type, delivery_column_name, value_set_version_label) "
            "VALUES (?, 10, '2007-01-01', '2007-12-31', 'int', ?, ?)",
            (vid, col, label),
        )
    conn.commit()
    return conn


class TestGetSchemaFoldedWindowNotSharded:
    """A2.6 P2 regression: get_schema groups editions by DELIVERY WINDOW only.

    A folded multi-vintage variable (see reg_meta_build/DESIGN.md → Build-time triage (SCB)) carries two states in one window with
    distinct `value_set_version_label`s while ordinary columns carry ''. Keying
    the edition by the label sharded one delivered schema into partial pseudo-
    versions (the '' group missing the folded var, each vintage group missing
    the ordinary columns). The fix keys by (valid_from, valid_to) so one edition
    holds every column delivered in the window; the label is per-column.
    """

    def test_single_version_holds_all_columns_with_per_column_labels(self):
        from reg_meta.queries import get_schema

        conn = _folded_window_db()
        out = get_schema(conn, register_variant_id="10")
        versions = [ver for var in out["variants"] for ver in var["versions"]]
        # Pre-fix this window sharded into 3 pseudo-versions ('', sni92, sni2007).
        assert len(versions) == 1, (
            f"window must NOT be sharded; got {len(versions)} versions"
        )

        ver = versions[0]
        assert (ver["valid_from"], ver["valid_to"]) == ("2007-01-01", "2007-12-31")
        # The single window's columns include BOTH ordinary columns AND both
        # folded-variable vintage states.
        assert "value_set_version_label" not in ver, (
            "version-level label removed; it is per-column now"
        )
        labels_by_alias: dict[str, str] = {
            col["aliases"]: col["value_set_version_label"] for col in ver["columns"]
        }
        # Four columns; the two SNI states share the alias but differ by label.
        assert len(ver["columns"]) == 4
        ordinary = {
            col["aliases"]
            for col in ver["columns"]
            if col["value_set_version_label"] == ""
        }
        assert ordinary == {"Kon", "Alder"}
        sni_labels = {
            col["value_set_version_label"]
            for col in ver["columns"]
            if col["aliases"] == "Sni"
        }
        assert sni_labels == {"sni92", "sni2007"}
        assert labels_by_alias["Kon"] == ""
