"""Tests for query commands (Phase 2)."""

from __future__ import annotations

import json
import sys

import pytest
from reg_meta.cli import run


def _run_json(argv: list[str], *, verbose: bool = True) -> tuple[dict, int]:
    """Run a CLI command and parse the JSON output.

    Forces --format json. Default verbose=True so tests get the full envelope.
    """
    import io
    import sys

    if "--format" not in argv:
        argv = ["--format", "json", *argv]
    if verbose and "--verbose" not in argv and "-v" not in argv:
        argv = ["--verbose", *argv]

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
        assert result["var_id"] == 100

    def test_search_register(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "search", "--query", "Testning", "--type", "register"]
        )
        assert code == 0
        assert data["data"]["total_count"] >= 1
        assert data["data"]["results"][0]["register_id"] == 1

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
            assert r["register_id"] == 1

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
        assert data["data"]["total_count"] >= 1

    def test_search_years_excludes_outside_range(self, db_path: str):
        """--years filters out results with no versions in range."""
        data, code = _run_json(
            ["--db", db_path, "search", "--query", "Kön", "--years", "2050"]
        )
        assert code == 0
        assert data["data"]["total_count"] == 0

    def test_search_years_range(self, db_path: str):
        """--years accepts a range like 2020-2021."""
        data, code = _run_json(
            ["--db", db_path, "search", "--query", "Kön", "--years", "2020-2021"]
        )
        assert code == 0
        assert data["data"]["total_count"] >= 1

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
        assert data["data"]["total_count"] >= 1

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
        assert data["data"]["total_count"] == 0


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
        assert data["data"]["register_id"] == 1

    def test_fuzzy_match(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "register", "TEST"])
        assert code == 0
        # "TEST" matches "TESTREG" by substring
        if "registers" in data["data"]:
            ids = [r["register_id"] for r in data["data"]["registers"]]
            assert 1 in ids
        else:
            assert data["data"]["register_id"] == 1

    def test_not_found(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "register", "ZZZNONEXIST"])
        assert code == 16
        assert data["error"]["code"] == "not_found"


# ---------------------------------------------------------------------------
# Get schema
# ---------------------------------------------------------------------------


class TestGetSchema:
    def test_by_register_variant_id(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "schema", "10"])
        assert code == 0
        variants = data["data"]["variants"]
        assert len(variants) == 1
        assert variants[0]["register_variant_id"] == 10
        assert len(variants[0]["versions"]) == 3  # 2020, 2021, 2022

    def test_by_register(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "get", "schema", "--register", "TESTREG"]
        )
        assert code == 0
        assert len(data["data"]["variants"]) == 1

    def test_years_single(self, db_path: str):
        # A2.6: "editions" are `variable_state` validity windows now; their
        # `year` is the window's opening year (`valid_from`). The 2020 filter
        # keeps every window opening in 2020 (the fixture has two).
        data, code = _run_json(
            ["--db", db_path, "get", "schema", "10", "--years", "2020"]
        )
        assert code == 0
        versions = data["data"]["variants"][0]["versions"]
        assert versions  # at least one window opens in 2020
        assert all(v["year"] == 2020 for v in versions)

    def test_years_range(self, db_path: str):
        # The window-opening year is what the range filters on (a window's
        # opening year falls inside 2020-2021 here).
        data, code = _run_json(
            ["--db", db_path, "get", "schema", "10", "--years", "2020-2021"]
        )
        assert code == 0
        versions = data["data"]["variants"][0]["versions"]
        years = {v["year"] for v in versions}
        assert years
        assert all(2020 <= y <= 2021 for y in years)

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
        testvar_cols = [c for c in columns if c["var_id"] == 100]
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

    def test_summary_mode(self, db_path: str):
        """--summary returns condensed variant-level info (via JSON data)."""
        data, code = _run_json(["--db", db_path, "get", "schema", "10", "--summary"])
        assert code == 0
        # JSON output still has full data; summary only affects table display
        variants = data["data"]["variants"]
        assert len(variants) >= 1
        assert len(variants[0]["versions"]) >= 1

    def test_flat_mode(self, db_path: str):
        """--flat is a display mode; JSON data is unchanged."""
        data, code = _run_json(["--db", db_path, "get", "schema", "10", "--flat"])
        assert code == 0
        variants = data["data"]["variants"]
        assert len(variants) >= 1

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
# Get varinfo
# ---------------------------------------------------------------------------


class TestGetVarinfo:
    def test_by_name(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "get", "varinfo", "Kön", "--register", "TESTREG"]
        )
        assert code == 0
        assert data["data"]["name"] == "Kön"
        assert data["data"]["register_id"] == 1
        # A2.6: "instances" are `variable_state` rows now (coalesced per-delivery
        # shape), not per-cvid rows — TESTREG Kön has two states.
        assert len(data["data"]["instances"]) == 2

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

    def test_instance_details(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "get", "varinfo", "Kön", "--register", "TESTREG"]
        )
        inst = data["data"]["instances"][0]
        # A2.6: per-state keys (state_id + validity window) replace cvid +
        # register_version name.
        assert "state_id" in inst
        assert "valid_from" in inst
        assert "year" in inst
        assert "aliases" in inst
        assert "value_set_count" in inst

    def test_value_set_count(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "get", "varinfo", "Kön", "--register", "TESTREG"]
        )
        # The Kön value-set (Man, Kvinna) carries 2 codes; at least one state
        # surfaces that count.
        with_codes = [i for i in data["data"]["instances"] if i["value_set_count"] == 2]
        assert with_codes

    def test_not_found(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "varinfo", "NONEXISTENT"])
        assert code == 16


# ---------------------------------------------------------------------------
# Split-sibling isolation (A2.2 split → A2.6 query)
# ---------------------------------------------------------------------------


class TestSplitSiblingIsolation:
    """A2.2 puts several variables under one `(register_id, provider_key)` —
    split siblings share a `var_id` but have distinct `variable_id`s, slugs,
    names, and their OWN `variable_state` + value sets. Once a `get_*` command
    has matched ONE sibling (by its unique name/alias), it must select states by
    that row's `variable_id`, not by the shared `provider_key`. Filtering on
    `provider_key` leaks the OTHER sibling's states/codes — these tests fail on
    that bug (they see 2 states / both value sets where 1 sibling has 1 each).
    """

    @staticmethod
    def _split_db():
        from _slugged_db import (
            add_state,
            add_value_set,
            add_variable,
            build_slugged_db,
        )

        # Default fixture: register 1 (lisa), variant 10, variable var_id 44
        # name "Kön" slug "kon". Replace it with two explicit siblings so the
        # scenario is unambiguous: drop the default variable layer, add A + B.
        conn = build_slugged_db(variable=None)
        # Sibling A — provider_key 44, distinct slug + name + own column/codes.
        add_variable(
            conn, register_id=1, var_id=44, name="SSYK 3-pos", slug="ssyk-3pos"
        )
        add_value_set(conn, value_set_id=1, codes=[("A1", "A-one"), ("A2", "A-two")])
        add_state(
            conn,
            register_id=1,
            variable_slug="ssyk-3pos",
            register_variant_id=10,
            valid_from="2018-01-01",
            delivery_column_name="Ssyk3",
            value_set_id=1,
        )
        # Sibling B — same provider_key 44, its own slug/name/column/codes.
        add_variable(
            conn, register_id=1, var_id=44, name="SSYK 5-pos", slug="ssyk-5pos"
        )
        add_value_set(conn, value_set_id=2, codes=[("B1", "B-one"), ("B2", "B-two")])
        add_state(
            conn,
            register_id=1,
            variable_slug="ssyk-5pos",
            register_variant_id=10,
            valid_from="2018-01-01",
            delivery_column_name="Ssyk5",
            value_set_id=2,
        )
        conn.commit()
        return conn

    def test_varinfo_returns_only_matched_sibling_states(self):
        from reg_meta.queries import get_varinfo

        conn = self._split_db()
        # Match sibling A by its unique name → exactly one variable, and its
        # instances must be ONLY A's single state (column Ssyk3), not B's.
        result = get_varinfo(conn, "SSYK 3-pos", register="lisa")
        assert len(result) == 1
        a = result[0]
        assert a["name"] == "SSYK 3-pos"
        cols = sorted(c for inst in a["instances"] for c in inst["aliases"])
        assert cols == ["Ssyk3"]
        assert len(a["instances"]) == 1

        # And sibling B in isolation sees only Ssyk5.
        b = get_varinfo(conn, "SSYK 5-pos", register="lisa")[0]
        b_cols = sorted(c for inst in b["instances"] for c in inst["aliases"])
        assert b_cols == ["Ssyk5"]

    def test_values_returns_only_matched_sibling_codes(self):
        from reg_meta.queries import get_values_by_variable

        conn = self._split_db()
        # Match sibling A by name → only A's value set (A1/A2), never B's.
        result = get_values_by_variable(conn, "SSYK 3-pos", register="lisa")
        codes = {v["code"] for inst in result["instances"] for v in inst["values"]}
        assert codes == {"A1", "A2"}

        b = get_values_by_variable(conn, "SSYK 5-pos", register="lisa")
        b_codes = {v["code"] for inst in b["instances"] for v in inst["values"]}
        assert b_codes == {"B1", "B2"}

    def test_classifications_isolate_per_sibling(self):
        """A2.7 resolves the A2.6 limitation: `classifications_for_variable`
        re-sources off `variable_state.classification_id` keyed by `variable_id`,
        so each split sibling returns ONLY its own classification. Pre-A2.7 (off
        `variable_instance`, keyed by the shared `var_id`) both siblings would
        return BOTH classifications."""
        from reg_meta.queries import classifications_for_variable

        conn = self._split_db()
        # Two distinct classifications; tag sibling A's state with one, B's with
        # the other. (`build_slugged_db` already seeded one classification, so
        # don't hard-code ids — capture the new rows' lastrowid.)
        cls_a = conn.execute(
            "INSERT INTO classification (short_name, name, slug) "
            "VALUES ('SSYK2012', 'Std för yrkesklassificering', 'ssyk2012')"
        ).lastrowid
        cls_b = conn.execute(
            "INSERT INTO classification (short_name, name, slug) "
            "VALUES ('SSYK96', 'Std för yrkesklassificering 96', 'ssyk96')"
        ).lastrowid
        a_vid, b_vid = (
            conn.execute(
                "SELECT variable_id FROM variable WHERE register_id = 1 AND slug = ?",
                (slug,),
            ).fetchone()[0]
            for slug in ("ssyk-3pos", "ssyk-5pos")
        )
        conn.execute(
            "UPDATE variable_state SET classification_id = ? WHERE variable_id = ?",
            (cls_a, a_vid),
        )
        conn.execute(
            "UPDATE variable_state SET classification_id = ? WHERE variable_id = ?",
            (cls_b, b_vid),
        )
        conn.commit()

        a_cls = classifications_for_variable(conn, a_vid)
        assert [c["short_name"] for c in a_cls] == ["SSYK2012"]
        b_cls = classifications_for_variable(conn, b_vid)
        assert [c["short_name"] for c in b_cls] == ["SSYK96"]

    def test_values_by_numeric_var_id_attributes_split_siblings(self):
        """A2.7 (Codex P2 #149): a numeric var_id that maps to >1 split sibling
        (same provider_key 44, distinct variable_id/slug) no longer merges them
        anonymously — every instance carries its owning variable's slug, and the
        codes stay sibling-exact (no cross-contamination)."""
        from reg_meta.queries import get_values_by_variable

        conn = self._split_db()
        result = get_values_by_variable(conn, "44", register="lisa")
        by_slug = {
            inst["variable_slug"]: {v["code"] for v in inst["values"]}
            for inst in result["instances"]
        }
        assert by_slug == {"ssyk-3pos": {"A1", "A2"}, "ssyk-5pos": {"B1", "B2"}}


# ---------------------------------------------------------------------------
# Get values
# ---------------------------------------------------------------------------


class TestGetValues:
    def test_values_by_var_id(self, db_path: str):
        """A2.7: a numeric target resolves as a var_id (was a CVID). var_id 44
        is Kön; with no --year it returns the multi-state view, whose states
        carry the {1=Man, 2=Kvinna} value set."""
        data, code = _run_json(["--db", db_path, "get", "values", "44"])
        assert code == 0
        payload = data["data"]
        assert payload["variable_name"] == "Kön"
        codes = {v["code"] for i in payload["instances"] for v in i.get("values", [])}
        assert codes == {"1", "2"}

    def test_not_found(self, db_path: str):
        # 99999 is neither a known var_id nor a variable name → not_found.
        data, code = _run_json(["--db", db_path, "get", "values", "99999"])
        assert code == 16

    def test_by_variable_year_resolves_to_single_cvid(self, db_path: str):
        """variable + year → flat value list (cvid shape)."""
        data, code = _run_json(
            [
                "--db",
                db_path,
                "get",
                "values",
                "Kön",
                "--register",
                "TESTREG",
                "--year",
                "2020",
            ]
        )
        assert code == 0
        assert isinstance(data["data"], list)
        codes = {v["code"] for v in data["data"]}
        assert codes == {"1", "2"}

    def test_by_variable_multi_year(self, db_path: str):
        """variable (no year) → multi-instance year × codes view."""
        data, code = _run_json(
            ["--db", db_path, "get", "values", "Kön", "--register", "TESTREG"]
        )
        assert code == 0
        payload = data["data"]
        assert payload["variable_name"] == "Kön"
        instances = payload["instances"]
        # A2.6: "instances" are `variable_state` rows. The coalescer merged the
        # 2020 + 2021 cvids into one 2020-01-01..2021-12-31 state (window-opening
        # year 2020); the 2022 cvid is its own state. So years are {2020, 2022}.
        years = {i["year"] for i in instances}
        assert years == {2020, 2022}
        # The Man/Kvinna value set surfaces on at least one state.
        coded = [i for i in instances if i["values"]]
        assert coded
        assert any({v["code"] for v in i["values"]} == {"1", "2"} for i in coded)

    def test_by_variable_year_collapses_across_registers(self, db_path: str):
        """variable + year across multiple registers collapses if codes match.

        var_id=44 ("Kön") exists in both TESTREG (cvid 1003) and OTHERREG
        (cvid 2001) for 2021. Both carry the same {1=Man, 2=Kvinna} codes,
        so the multi-register case should collapse to one flat list — the
        answer is unambiguous even if the provenance isn't.
        """
        data, code = _run_json(
            ["--db", db_path, "get", "values", "Kön", "--year", "2021"]
        )
        assert code == 0
        assert isinstance(data["data"], list)
        codes = {v["code"] for v in data["data"]}
        assert codes == {"1", "2"}

    def test_by_variable_unknown(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "values", "NONEXISTENT_VAR"])
        assert code == 16

    def test_by_variable_year_no_match(self, db_path: str):
        data, code = _run_json(
            [
                "--db",
                db_path,
                "get",
                "values",
                "Kön",
                "--register",
                "TESTREG",
                "--year",
                "1999",
            ]
        )
        assert code == 16

    def test_numeric_target_resolves_as_var_id(self, db_path: str):
        """A2.7: the by-CVID path is gone — a numeric target now resolves as a
        var_id (the variable's provider_key), so --year/--register apply. var_id
        44 is Kön; with --year 2020 it yields the year-correct flat code list."""
        data, code = _run_json(
            [
                "--db",
                db_path,
                "get",
                "values",
                "44",
                "--register",
                "TESTREG",
                "--year",
                "2020",
            ]
        )
        assert code == 0
        assert isinstance(data["data"], list)
        codes = {v["code"] for v in data["data"]}
        assert codes == {"1", "2"}

    def test_groups_payload_disagreement(self):
        """When (variable, year) hits multiple distinct value sets, the
        handler buckets them by code-set so callers don't drown in repeats.
        """
        from reg_meta.cli import _group_instances_by_codes

        # A2.6: instances are variable_state-shaped (state_id + validity window).
        instances = [
            {
                "state_id": 1,
                "variable_slug": "kon",
                "register_id": 100,
                "register_name": "RegA",
                "register_variant_id": 10,
                "variant_name": "A1",
                "valid_from": "2017-01-01",
                "valid_to": "2017-12-31",
                "year": 2017,
                "values": [
                    {"code": "1", "label": "Man"},
                    {"code": "2", "label": "Kvinna"},
                ],
            },
            {
                "state_id": 2,
                "variable_slug": "kon",
                "register_id": 101,
                "register_name": "RegB",
                "register_variant_id": 11,
                "variant_name": "B1",
                "valid_from": "2017-01-01",
                "valid_to": "2017-12-31",
                "year": 2017,
                "values": [
                    {"code": "1", "label": "Man"},
                    {"code": "2", "label": "Kvinna"},
                ],
            },
            {
                "state_id": 3,
                "variable_slug": "kon-barn",
                "register_id": 102,
                "register_name": "RegC",
                "register_variant_id": 12,
                "variant_name": "C1",
                "valid_from": "2017-01-01",
                "valid_to": "2017-12-31",
                "year": 2017,
                "values": [
                    {"code": "1", "label": "Pojke"},
                    {"code": "2", "label": "Flicka"},
                ],
            },
        ]
        out = _group_instances_by_codes(
            instances, input_value="Kön", variable_name="Kön", year=2017
        )
        assert out["value_set_count"] == 2
        assert out["instance_count"] == 3
        assert out["register_count"] == 3
        # Largest group (Man/Kvinna, 2 instances) comes first.
        assert out["groups"][0]["instance_count"] == 2
        assert out["groups"][0]["register_count"] == 2
        assert out["groups"][0]["registers"] == ["RegA", "RegB"]
        # Per-group variable attribution (A2.7, Codex P2 #149): the two
        # Man/Kvinna instances share one owning variable; RegC's differing codes
        # belong to another — each group names its own slug(s).
        assert out["groups"][0]["variable_slugs"] == ["kon"]
        assert out["groups"][1]["instance_count"] == 1
        assert out["groups"][1]["registers"] == ["RegC"]
        assert out["groups"][1]["variable_slugs"] == ["kon-barn"]
        labels = {v["label"] for v in out["groups"][1]["values"]}
        assert labels == {"Pojke", "Flicka"}

    def test_groups_text_rendering(self, tmp_path):
        """`_write_groups_payload` renders a header summary, per-group code
        listing, and a registers footer (with overflow hint when truncated).
        """
        from reg_meta.cli import _write_groups_payload

        payload = {
            "input": "Kön",
            "variable_name": "Kön",
            "year": 2017,
            "value_set_count": 2,
            "instance_count": 14,
            "register_count": 13,
            "groups": [
                {
                    "values": [
                        {"code": "1", "label": "Man"},
                        {"code": "2", "label": "Kvinna"},
                    ],
                    "instance_count": 12,
                    "register_count": 12,
                    "registers": [f"Reg{i:02d}" for i in range(12)],
                },
                {
                    "values": [
                        {"code": "1", "label": "Pojke"},
                        {"code": "2", "label": "Flicka"},
                    ],
                    "instance_count": 2,
                    "register_count": 1,
                    "registers": ["RegC"],
                },
            ],
        }
        out = tmp_path / "groups.txt"
        _write_groups_payload(payload, str(out))
        text = out.read_text()
        # Header summary
        assert "Variable 'Kön'" in text
        assert "year 2017" in text
        assert "2 distinct value set(s)" in text
        # Both groups rendered
        assert "[Group 1]" in text and "[Group 2]" in text
        assert "Man" in text and "Pojke" in text
        # Register list with overflow hint (12 registers, cap=10)
        assert "+2 more" in text

    def test_ambiguous_alias_errors(self, tmp_path):
        """Column aliases shared across unrelated variables (e.g. "Rad")
        must error rather than silently merging value sets under one name.
        """
        import sqlite3

        from reg_meta.queries import get_values_by_variable
        from reg_meta_build.db import DDL, seed_providers

        db = tmp_path / "amb.db"
        conn = sqlite3.connect(str(db))
        conn.row_factory = sqlite3.Row
        conn.executescript(DDL)
        seed_providers(conn)
        conn.execute(
            "INSERT INTO register (register_id, provider_id, name) VALUES (1, 1, 'R1')"
        )
        conn.execute(
            "INSERT INTO register (register_id, provider_id, name) VALUES (2, 1, 'R2')"
        )
        conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id, name) "
            "VALUES (10, 1, 'V1')"
        )
        conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id, name) "
            "VALUES (11, 2, 'V2')"
        )
        # A2.7: the ambiguous-alias check fires in the alias→variable fallback
        # (before any state lookup); `variable_alias` is variable_id-keyed.
        v1 = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name) VALUES (1, '50', 'AppleVar')"
        ).lastrowid
        v2 = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name) VALUES (2, '51', 'BananaVar')"
        ).lastrowid
        # Same alias 'Rad' used for two unrelated variables.
        conn.execute(
            "INSERT INTO variable_alias (variable_id, register_variant_id, delivery_column_name) "
            "VALUES (?, 10, 'Rad')",
            (v1,),
        )
        conn.execute(
            "INSERT INTO variable_alias (variable_id, register_variant_id, delivery_column_name) "
            "VALUES (?, 11, 'Rad')",
            (v2,),
        )
        conn.commit()

        import pytest
        from reg_meta.errors import RegMetaError

        with pytest.raises(RegMetaError) as exc:
            get_values_by_variable(conn, "Rad")
        assert exc.value.code == "ambiguous_alias"
        assert exc.value.exit_code == 2
        # Message names both variables (or shows count + sample).
        assert "AppleVar" in exc.value.message
        assert "BananaVar" in exc.value.message
        conn.close()

    def test_groups_cli_end_to_end(self, tmp_path):
        """Full CLI path: `get values <var> --year Y` across registers with
        disagreeing code labels emits a `groups`-shaped payload.
        """
        import sqlite3

        from reg_meta.db import SCHEMA_VERSION
        from reg_meta_build.db import DDL

        db_dir = tmp_path / "db"
        db_dir.mkdir()
        db = db_dir / "reg_meta.db"
        conn = sqlite3.connect(str(db))
        conn.row_factory = sqlite3.Row
        conn.executescript(DDL)
        conn.execute(
            "INSERT INTO import_manifest VALUES ('schema_version', ?)",
            (SCHEMA_VERSION,),
        )
        # Two registers, same variable name + var_id, same year, different code labels.
        from reg_meta_build.db import seed_providers

        seed_providers(conn)
        conn.execute(
            "INSERT INTO register (register_id, provider_id, name) "
            "VALUES (1, 1, 'RegAdult')"
        )
        conn.execute(
            "INSERT INTO register (register_id, provider_id, name) "
            "VALUES (2, 1, 'RegChild')"
        )
        conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id, name) "
            "VALUES (10, 1, 'Adults')"
        )
        conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id, name) "
            "VALUES (11, 2, 'Children')"
        )
        v1 = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name, slug) "
            "VALUES (1, '44', 'Kön', 'kon')"
        ).lastrowid
        v2 = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name, slug) "
            "VALUES (2, '44', 'Kön', 'kon')"
        ).lastrowid
        # Two distinct value sets. member_hash must be 32 bytes.
        conn.execute(
            "INSERT INTO value_set (value_set_id, member_hash) VALUES (1, ?)",
            (b"\xaa" * 32,),
        )
        conn.execute(
            "INSERT INTO value_set (value_set_id, member_hash) VALUES (2, ?)",
            (b"\xbb" * 32,),
        )
        conn.execute(
            "INSERT INTO value_code (code_id, code, label) VALUES (1, '1', 'Man')"
        )
        conn.execute(
            "INSERT INTO value_code (code_id, code, label) VALUES (2, '2', 'Kvinna')"
        )
        conn.execute(
            "INSERT INTO value_code (code_id, code, label) VALUES (3, '1', 'Pojke')"
        )
        conn.execute(
            "INSERT INTO value_code (code_id, code, label) VALUES (4, '2', 'Flicka')"
        )
        conn.execute("INSERT INTO value_set_member VALUES (1, 1)")
        conn.execute("INSERT INTO value_set_member VALUES (1, 2)")
        conn.execute("INSERT INTO value_set_member VALUES (2, 3)")
        conn.execute("INSERT INTO value_set_member VALUES (2, 4)")
        # A2.6: get_values_by_variable reads variable_state; one 2020 state per
        # variable, each carrying its own value set.
        conn.execute(
            "INSERT INTO variable_state (variable_id, register_variant_id, valid_from, "
            "valid_to, data_type, value_set_id) "
            "VALUES (?, 10, '2020-01-01', '2020-12-31', 'int', 1)",
            (v1,),
        )
        conn.execute(
            "INSERT INTO variable_state (variable_id, register_variant_id, valid_from, "
            "valid_to, data_type, value_set_id) "
            "VALUES (?, 11, '2020-01-01', '2020-12-31', 'int', 2)",
            (v2,),
        )
        # docs DB stub — query commands require it present.
        from reg_meta_build.doc_db import build_doc_db

        docs_src = tmp_path / "docs"
        (docs_src / "stub").mkdir(parents=True)
        (docs_src / "stub" / "Stub.md").write_text(
            "---\nvariable: Stub\ndisplay_name: Stub\ntags:\n  - type/variable\n---\n\nx\n",
            encoding="utf-8",
        )
        build_doc_db(docs_src, db_dir)
        conn.commit()
        conn.close()

        data, code = _run_json(
            ["--db", str(db_dir), "get", "values", "Kön", "--year", "2020"]
        )
        assert code == 0
        payload = data["data"]
        assert "groups" in payload
        assert payload["value_set_count"] == 2
        assert payload["instance_count"] == 2
        assert payload["register_count"] == 2
        # Two groups, each with one instance from one register.
        labels = {
            tuple(sorted(v["label"] for v in g["values"])) for g in payload["groups"]
        }
        assert labels == {("Kvinna", "Man"), ("Flicka", "Pojke")}


# ---------------------------------------------------------------------------
# Get datacolumns
# ---------------------------------------------------------------------------


class TestGetDatacolumns:
    def test_by_name(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "datacolumns", "Kön"])
        assert code == 0
        col_names = {r["delivery_column_name"] for r in data["data"]}
        assert "Kon" in col_names or "KON" in col_names

    def test_register_filter(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "get", "datacolumns", "Kön", "--register", "TESTREG"]
        )
        assert code == 0
        assert all(r["register_id"] == 1 for r in data["data"])

    def test_alias_anomaly(self, db_path: str):
        """TestVar should show both TestCol and TestKolumn aliases."""
        data, code = _run_json(
            ["--db", db_path, "get", "datacolumns", "TestVar", "--register", "TESTREG"]
        )
        assert code == 0
        col_names = {r["delivery_column_name"] for r in data["data"]}
        assert "TestCol" in col_names
        assert "TestKolumn" in col_names

    def test_not_found(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "datacolumns", "NONEXISTENT"])
        assert code == 16

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
        assert all(m["register_id"] == 1 for m in col["matches"])

    def test_cross_register(self, db_path: str):
        data, code = _run_json(["--db", db_path, "resolve", "--columns", "Kon"])
        col = data["data"]["columns"][0]
        reg_ids = {m["register_id"] for m in col["matches"]}
        # "Kon" is in reg 1, "KON" is in reg 2 — case-insensitive should match both
        assert 1 in reg_ids

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
        assert data1["data"]["columns"][0]["matches"][0]["var_id"] == 100
        assert data2["data"]["columns"][0]["matches"][0]["var_id"] == 100

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

    def test_variable_filter_unchanged(self, db_path: str):
        """Kön is unchanged 2020→2022; should appear in unchanged list."""
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
            ]
        )
        assert code == 0
        assert data["data"]["variants"] == []
        assert "Kön" in data["data"]["unchanged"]
        # resolved_variables shows the mapping
        resolved = data["data"]["resolved_variables"]
        assert any(
            r["variable_name"] == "Kön" and r["input"] == "Kön" for r in resolved
        )

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
        assert data["data"]["variants"][0]["register_variant_id"] == 10

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
        data, code = _run_json(
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

    def test_no_versions_in_range(self, db_path: str):
        data, code = _run_json(
            [
                "--db",
                db_path,
                "get",
                "diff",
                "--register",
                "TESTREG",
                "--from",
                "1990",
                "--to",
                "1995",
            ]
        )
        assert code == 16


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
        otherreg = [
            r for r in data["data"]["registers"] if r["register_name"] == "OTHERREG"
        ][0]
        # TESTREG should resolve to register_id "1"
        assert otherreg["source_register_id"] == 1
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
        data, code = _run_json(["--db", db_path, "get", "lineage", "NONEXISTENT"])
        assert code == 16

    def test_provenance_coverage(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "lineage", "Kön"])
        assert code == 0
        cov = data["data"]["provenance_coverage"]
        assert cov["total"] == cov["with_source"] + cov["without_source"]
        assert cov["total"] > 0

    def test_year_range(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "lineage", "Kön"])
        assert code == 0
        testreg = [
            r for r in data["data"]["registers"] if r["register_name"] == "TESTREG"
        ][0]
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
        assert all(r["register_id"] == 1 for r in d["registers"])

    def test_register_availability(self, db_path: str):
        data, code = _run_json(["--db", db_path, "get", "availability", "TESTREG"])
        assert code == 0
        d = data["data"]
        assert d["target_type"] == "register"
        assert d["register_name"] == "TESTREG"
        assert d["min_year"] <= d["max_year"]
        assert len(d["variants"]) >= 1

    def test_not_found(self, db_path: str):
        _, code = _run_json(["--db", db_path, "get", "availability", "NONEXISTENT"])
        assert code == 16


# ---------------------------------------------------------------------------
# Envelope and error model
# ---------------------------------------------------------------------------


class TestOutputFormats:
    def test_json_envelope(self, db_path: str):
        data, _ = _run_json(["--db", db_path, "search", "--query", "test"])
        assert data["contract_version"] == "3.0.0"
        assert "generated_at" in data
        assert "request" in data
        assert "database" in data
        assert "data" in data
        assert "duration_ms" in data["run"]

    def test_default_format_is_human_readable(self, db_path: str):
        """Default output (no --format) should be human-readable, not JSON."""
        import io
        import sys

        old_stdout = sys.stdout
        sys.stdout = buf = io.StringIO()
        try:
            code = run(["--db", db_path, "search", "--query", "testvariabel"])
        finally:
            sys.stdout = old_stdout
        output = buf.getvalue()
        assert code == 0
        assert "TestVar" in output
        assert not output.lstrip().startswith("{")

    def test_list_format(self, db_path: str):
        import io
        import sys

        old_stdout = sys.stdout
        sys.stdout = buf = io.StringIO()
        try:
            code = run(
                ["--db", db_path, "--format", "list", "get", "register", "TESTREG"]
            )
        finally:
            sys.stdout = old_stdout
        output = buf.getvalue()
        assert code == 0
        assert "register_id" in output
        assert "TESTREG" in output
        assert "---" not in output  # no table separator

    def test_json_verbose_has_envelope(self, db_path: str):
        data, _ = _run_json(["--db", db_path, "search", "--query", "Kön"])
        assert data["contract_version"] == "3.0.0"
        assert "run" in data

    def test_json_no_verbose_is_data_only(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "search", "--query", "Kön"], verbose=False
        )
        assert code == 0
        assert "contract_version" not in data
        assert "run" not in data
        assert "total_count" in data

    def test_repeated_flag_errors(self, db_path: str):
        """Repeated optional flags should error, not silently overwrite."""
        _, code = _run_json(
            ["--db", db_path, "--db", db_path, "search", "--query", "test"]
        )
        assert code == 2

    def test_table_auto_switches_to_list_when_wide(self, db_path: str):
        """When terminal is very narrow, table should auto-switch to list (no separator)."""
        import io
        import unittest.mock

        # Patch terminal width to something very small
        with unittest.mock.patch("reg_meta.cli_common.terminal_width", return_value=30):
            old_stdout = __import__("sys").stdout
            __import__("sys").stdout = buf = io.StringIO()
            try:
                code = run(["--db", db_path, "get", "register", "TESTREG"])
            finally:
                __import__("sys").stdout = old_stdout
        output = buf.getvalue()
        assert code == 0
        # List format: no separator line, has key-value pairs
        assert "---" not in output
        assert "register_id" in output

    def test_row_truncation(self, db_path: str):
        """Results exceeding MAX_DISPLAY_ROWS should emit truncation hint on stderr."""
        import io

        import reg_meta.cli_common

        old_max = reg_meta.cli_common.MAX_DISPLAY_ROWS
        try:
            reg_meta.cli_common.MAX_DISPLAY_ROWS = 1
            old_stdout, old_stderr = sys.stdout, sys.stderr
            sys.stdout = io.StringIO()
            sys.stderr = err_buf = io.StringIO()
            try:
                code = run(["--db", db_path, "get", "schema", "--register", "TESTREG"])
            finally:
                sys.stdout, sys.stderr = old_stdout, old_stderr
            assert code == 0
            assert "truncated" in err_buf.getvalue()
        finally:
            reg_meta.cli_common.MAX_DISPLAY_ROWS = old_max

    def test_diff_output_file_has_all_sections(self, db_path: str, tmp_path):
        """--output with get diff must include all sections, not just the last."""
        out = tmp_path / "diff.txt"
        code = run(
            [
                "--db",
                db_path,
                "--output",
                str(out),
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
        content = out.read_text(encoding="utf-8")
        # Multi-section: resolved variables header + diff table + unchanged footer
        assert "Kön" in content
        assert "TestVar" in content
        assert "Unchanged" in content

    def test_no_command(self):
        _, code = _run_json([])
        assert code == 2


# ---------------------------------------------------------------------------
# A2.6 year-filter overlap (regression: was filtering by valid_from year only)
# ---------------------------------------------------------------------------


def _overlap_db():
    """In-memory DB with three `variable_state` window shapes on one variant, so
    the requested-year FILTER sites (search / get_schema / get_values) can be
    exercised against multi-year, open-ended, and yearless-fallback windows.

    Variable `kon` (var_id 44) carries three states under variant 10:
      - multi-year     2010-01-01 .. 2012-12-31  (value_set 1)
      - open-ended     2015-01-01 .. 9999-12-31  (value_set 1)
      - yearless       0001-01-01 .. 9999-12-31  (value_set 1)
    The three distinct (valid_from, valid_to) windows are separate editions in
    get_schema (which groups by delivery window). The labels below are just
    per-window markers for the assertions."""
    import sqlite3

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
    vid = conn.execute(
        "INSERT INTO variable (register_id, provider_key, name, slug) "
        "VALUES (1, '44', 'Kön', 'kon')"
    ).lastrowid
    conn.execute(
        "INSERT INTO value_set (value_set_id, member_hash) VALUES (1, ?)",
        (b"\xaa" * 32,),
    )
    conn.execute("INSERT INTO value_code (code_id, code, label) VALUES (1, '1', 'Man')")
    conn.execute(
        "INSERT INTO value_code (code_id, code, label) VALUES (2, '2', 'Kvinna')"
    )
    conn.execute("INSERT INTO value_set_member VALUES (1, 1)")
    conn.execute("INSERT INTO value_set_member VALUES (1, 2)")
    for valid_from, valid_to, label in (
        ("2010-01-01", "2012-12-31", "multi"),
        ("2015-01-01", "9999-12-31", "open"),
        ("0001-01-01", "9999-12-31", "yearless"),
    ):
        conn.execute(
            "INSERT INTO variable_state (variable_id, register_variant_id, valid_from, "
            "valid_to, data_type, delivery_column_name, value_set_id, "
            "value_set_version_label) VALUES (?, 10, ?, ?, 'int', 'Kon', 1, ?)",
            (vid, valid_from, valid_to, label),
        )
    conn.commit()
    return conn


class TestStateOverlapHelpers:
    """Unit tests for the overlap predicates (the bug was START-year-only)."""

    def test_covers_year_spans_full_window(self):
        from reg_meta.queries import _state_covers_year

        # Multi-year window must match its MID and END years, not only the start.
        assert _state_covers_year("2010-01-01", "2012-12-31", 2010)
        assert _state_covers_year("2010-01-01", "2012-12-31", 2011)  # mid
        assert _state_covers_year("2010-01-01", "2012-12-31", 2012)  # end
        assert not _state_covers_year("2010-01-01", "2012-12-31", 2013)
        assert not _state_covers_year("2010-01-01", "2012-12-31", 2009)

    def test_covers_year_open_ended_matches_far_future(self):
        from reg_meta.queries import _state_covers_year

        # Open-ended (9999) must match years well past the opening year.
        assert _state_covers_year("2015-01-01", "9999-12-31", 2015)
        assert _state_covers_year("2015-01-01", "9999-12-31", 2099)
        assert not _state_covers_year("2015-01-01", "9999-12-31", 2014)

    def test_covers_year_yearless_matches_anything(self):
        from reg_meta.queries import _state_covers_year

        # Yearless-fallback (0001..9999) matches any reasonable calendar year.
        assert _state_covers_year("0001-01-01", "9999-12-31", 1850)
        assert _state_covers_year("0001-01-01", "9999-12-31", 2024)

    def test_overlaps_years_range_and_open_bounds(self):
        from reg_meta.queries import _state_overlaps_years

        # Multi-year window vs requested ranges.
        assert _state_overlaps_years("2010-01-01", "2012-12-31", 2011, 2011)  # mid
        assert _state_overlaps_years("2010-01-01", "2012-12-31", 2012, 2015)  # end edge
        assert not _state_overlaps_years("2010-01-01", "2012-12-31", 2013, 2014)
        # Open-ended window vs a far-future single year and open-high request.
        assert _state_overlaps_years("2015-01-01", "9999-12-31", 2099, 2099)
        assert _state_overlaps_years("2015-01-01", "9999-12-31", 2099, None)
        # Open-low request (lo=None → 0) matches a yearless window.
        assert _state_overlaps_years("0001-01-01", "9999-12-31", None, 1990)


class TestIsCodeShaped:
    """#352: a query is code-shaped (→ also matches by value_code.code) iff it has
    a digit AND length >= 3. Plain text (no digit) or too-short queries do label
    FTS only."""

    def test_code_shaped_queries(self):
        from reg_meta.queries import _is_code_shaped

        assert _is_code_shaped("F32")  # ICD-10
        assert _is_code_shaped("0180")  # numeric kommun code
        assert _is_code_shaped("47.11")  # SNI with separator
        # Leading/trailing whitespace is stripped before the length test.
        assert _is_code_shaped("  F32 ")

    def test_non_code_shaped_queries(self):
        from reg_meta.queries import _is_code_shaped

        assert not _is_code_shaped("Ja")  # no digit, too short
        assert not _is_code_shaped("Nej")  # no digit (len 3 but text-only)
        assert not _is_code_shaped("F3")  # has digit but len 2
        assert not _is_code_shaped("12")  # digits but len 2
        assert not _is_code_shaped("")  # empty
        assert not _is_code_shaped("inkomst")  # plain word, no digit


class TestGetValuesYearOverlap:
    """get_values_by_variable filters states by cover-the-year overlap."""

    def test_multi_year_state_matches_mid_and_end(self):
        from reg_meta.queries import get_values_by_variable

        conn = _overlap_db()
        # The multi-year state (2010-2012) must answer a MID-year (2011) and an
        # END-year (2012) query — not only its opening 2010. Without the fix the
        # int(valid_from[:4]) == year check drops it for 2011/2012.
        for y in (2010, 2011, 2012):
            out = get_values_by_variable(conn, "Kön", register="R1", year=y)
            labels = {v["label"] for inst in out["instances"] for v in inst["values"]}
            assert labels == {"Man", "Kvinna"}, f"year {y} should hit a state"

    def test_open_ended_state_matches_far_future(self):
        from reg_meta.queries import get_values_by_variable

        conn = _overlap_db()
        # 2099 is covered by both the open-ended and the yearless windows.
        out = get_values_by_variable(conn, "Kön", register="R1", year=2099)
        assert out["instances"], "open-ended state must match a year past its start"
        labels = {v["label"] for inst in out["instances"] for v in inst["values"]}
        assert labels == {"Man", "Kvinna"}

    def test_yearless_window_matches_arbitrary_year(self):
        from reg_meta.queries import get_values_by_variable

        conn = _overlap_db()
        # 1850 is covered only by the yearless-fallback window (0001..9999).
        out = get_values_by_variable(conn, "Kön", register="R1", year=1850)
        years = {inst["valid_from"] for inst in out["instances"]}
        assert "0001-01-01" in years

    def test_nonoverlapping_year_excluded(self):
        from reg_meta.errors import RegMetaError
        from reg_meta.queries import get_values_by_variable

        conn = _overlap_db()
        # 2013 falls in the gap between the multi-year (..2012) and open-ended
        # (2015..) windows, but the yearless window (0001..9999) still covers it,
        # so 2013 IS matched. 0 is below the yearless window's 0001 start and the
        # multi-year/open windows — nothing covers it.
        out = get_values_by_variable(conn, "Kön", register="R1", year=2013)
        assert {inst["valid_from"] for inst in out["instances"]} == {"0001-01-01"}
        try:
            zero = get_values_by_variable(conn, "Kön", register="R1", year=0)
        except RegMetaError:
            zero = {"instances": []}
        assert zero["instances"] == []


class TestGetSchemaYearOverlap:
    """get_schema filters editions by validity-window overlap."""

    def test_multi_year_edition_survives_mid_and_end_filter(self):
        from reg_meta.queries import get_schema

        conn = _overlap_db()
        # The 'multi' edition opens 2010 but spans through 2012; filtering for a
        # MID (2011) or END (2012) year must keep it. Pre-fix (year == start)
        # dropped it for 2011/2012.
        for y in ("2011", "2012"):
            out = get_schema(conn, register_variant_id="10", years=y)
            # Window identity is (valid_from, valid_to) now; the multi-year
            # edition opens 2010-01-01.
            windows = {
                ver["valid_from"] for var in out["variants"] for ver in var["versions"]
            }
            assert "2010-01-01" in windows, f"multi-year edition missing for years={y}"

    def test_open_and_yearless_editions_match_far_future(self):
        from reg_meta.queries import get_schema

        conn = _overlap_db()
        out = get_schema(conn, register_variant_id="10", years="2099")
        windows = {
            ver["valid_from"] for var in out["variants"] for ver in var["versions"]
        }
        # Open-ended (2015..) + yearless (0001..) cover 2099; the multi-year
        # (2010..2012) does not.
        assert "2015-01-01" in windows
        assert "0001-01-01" in windows
        assert "2010-01-01" not in windows

    def test_nonoverlapping_year_drops_bounded_editions(self):
        from reg_meta.queries import get_schema

        conn = _overlap_db()
        # 2013: only the yearless window covers it (gap year for multi/open).
        out = get_schema(conn, register_variant_id="10", years="2013")
        windows = {
            ver["valid_from"] for var in out["variants"] for ver in var["versions"]
        }
        assert windows == {"0001-01-01"}


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
    import sqlite3

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


class TestSearchYearOverlap:
    """_filter_search_by_years uses window overlap, not the opening year."""

    def test_var_pair_kept_for_mid_year_of_multi_year_state(self):
        from reg_meta.queries import search

        conn = _overlap_db()
        # 2011 is the MID year of the multi-year state; the variable must survive.
        out = search(conn, "Kön", field="varname", years="2011")
        assert out["total_count"] >= 1

    def test_var_pair_kept_for_far_future_open_ended(self):
        from reg_meta.queries import search

        conn = _overlap_db()
        # 2099 only overlaps the open-ended (2015..9999) and yearless windows.
        # Pre-fix `_years_in_range` capped both at their opening year, so 2099
        # matched nothing and the variable was wrongly dropped.
        out = search(conn, "Kön", field="varname", years="2099")
        assert out["total_count"] >= 1

    def test_var_pair_kept_for_gap_year_covered_only_by_yearless(self):
        from reg_meta.queries import search

        conn = _overlap_db()
        # 2013 falls in the gap between the multi-year (..2012) and open-ended
        # (2015..) windows but is covered by the yearless window (0001..9999).
        # Pre-fix the yearless window enumerated as just [1], so 2013 matched no
        # state and the variable was dropped.
        out = search(conn, "Kön", field="varname", years="2013")
        assert out["total_count"] >= 1

    def test_var_pair_dropped_when_no_window_covers_year(self):
        from reg_meta.queries import search

        conn = _overlap_db()
        # Year 0 is below every window (yearless starts at 0001) → no match.
        out = search(conn, "Kön", field="varname", years="0-0")
        assert out["total_count"] == 0


# ---------------------------------------------------------------------------
# Schema version gate (A2.7: 4.x → 5.0.0 break)
# ---------------------------------------------------------------------------


class TestSchemaCompat:
    """A2.7 bumped SCHEMA_VERSION to 5.0.0 (major break). A v4.x DB — which
    still carries `variable_instance` + a cvid-keyed `variable_alias` and no
    `variable_state.classification_id` — must be rejected with an actionable
    'rebuild' error via the major-version gate (4 != 5)."""

    @staticmethod
    def _db_with_manifest_version(tmp_path, version: str):
        import sqlite3

        from reg_meta_build.db import DDL

        db = tmp_path / "reg_meta.db"
        conn = sqlite3.connect(str(db))
        conn.executescript(DDL)
        conn.execute(
            "INSERT INTO import_manifest VALUES ('schema_version', ?)", (version,)
        )
        conn.commit()
        conn.close()
        return db

    def test_v4_db_rejected(self, tmp_path):
        from reg_meta.db import open_db
        from reg_meta.errors import EXIT_CONFIG, RegMetaError

        db = self._db_with_manifest_version(tmp_path, "4.9.0")
        with pytest.raises(RegMetaError) as exc:
            open_db(db, check_schema=True)
        assert exc.value.code == "schema_incompatible"
        assert exc.value.exit_code == EXIT_CONFIG
        assert "update" in exc.value.remediation.lower()

    def test_current_version_accepted(self, tmp_path):
        from reg_meta.db import SCHEMA_VERSION, open_db

        db = self._db_with_manifest_version(tmp_path, SCHEMA_VERSION)
        conn = open_db(db, check_schema=True)  # must not raise
        conn.close()


class TestPeriodTokenRendering:
    """#321: the `period` column in `get schema` / `get varinfo` table output
    renders each state's validity window at the COARSEST exact period token
    (via `period_token_for_bounds`), never rounding a sub-annual window down to
    a bare year. These drive `_write_payload` directly with hand-built payloads
    in the exact JSON shape the queries layer emits, so they're self-contained
    (no DB build) and pin the rendering, not the data path.

    `fmt="list"` is forced so each `period` value renders full-text as
    `period  <token>` — no table column truncation to fight."""

    @staticmethod
    def _render_schema(tmp_path, valid_from: str, valid_to: str) -> str:
        from reg_meta.cli import _write_payload

        out = tmp_path / "schema.txt"
        payload = {
            "data": {
                "variants": [
                    {
                        "register_variant_id": 10,
                        "versions": [
                            {
                                "valid_from": valid_from,
                                "valid_to": valid_to,
                                "year": int(valid_from[:4]),
                                "columns": [
                                    {
                                        "var_id": "44",
                                        "variable_name": "Kön",
                                        "data_type": "int",
                                        "aliases": "Kon",
                                        "source": "",
                                        "value_set_version_label": "",
                                        "concept_group": "",
                                    }
                                ],
                            }
                        ],
                    }
                ]
            }
        }
        _write_payload(("get", "schema"), payload, str(out), fmt="list")
        return out.read_text(encoding="utf-8")

    @staticmethod
    def _render_varinfo(tmp_path, valid_from: str, valid_to: str) -> str:
        from reg_meta.cli import _write_payload

        out = tmp_path / "varinfo.txt"
        payload = {
            "data": {
                "register_id": "1",
                "var_id": "44",
                "name": "Kön",
                "instances": [
                    {
                        "variant_name": "individer",
                        "valid_from": valid_from,
                        "valid_to": valid_to,
                        "data_type": "int",
                        "aliases": ["Kon"],
                        "value_set_count": 2,
                    }
                ],
            }
        }
        _write_payload(("get", "varinfo"), payload, str(out), fmt="list")
        return out.read_text(encoding="utf-8")

    # ── sub-annual windows render as their coarsest token (NOT a bare year) ──

    def test_schema_spring_term(self, tmp_path):
        text = self._render_schema(tmp_path, "2015-01-01", "2015-06-30")
        assert "period" in text
        assert "VT2015" in text
        assert "2015-01-01..2015-06-30" not in text  # not the raw range

    def test_schema_autumn_term(self, tmp_path):
        text = self._render_schema(tmp_path, "2015-07-01", "2015-12-31")
        assert "HT2015" in text

    def test_schema_quarter(self, tmp_path):
        text = self._render_schema(tmp_path, "2018-04-01", "2018-06-30")
        assert "2018-Q2" in text

    def test_schema_single_month(self, tmp_path):
        text = self._render_schema(tmp_path, "2019-03-01", "2019-03-31")
        assert "2019-03" in text
        assert "2019-03-01" not in text  # not the raw range

    def test_varinfo_spring_term(self, tmp_path):
        text = self._render_varinfo(tmp_path, "2015-01-01", "2015-06-30")
        assert "VT2015" in text

    def test_varinfo_autumn_term(self, tmp_path):
        text = self._render_varinfo(tmp_path, "2015-07-01", "2015-12-31")
        assert "HT2015" in text

    def test_varinfo_quarter(self, tmp_path):
        text = self._render_varinfo(tmp_path, "2018-04-01", "2018-06-30")
        assert "2018-Q2" in text

    def test_varinfo_single_month(self, tmp_path):
        text = self._render_varinfo(tmp_path, "2019-03-01", "2019-03-31")
        assert "2019-03" in text
        assert "2019-03-01" not in text  # not the raw range

    # ── full-year window stays the bare year (year-by-default preserved) ──

    def test_schema_full_year_is_bare_year(self, tmp_path):
        text = self._render_schema(tmp_path, "2020-01-01", "2020-12-31")
        assert "2020" in text
        assert "2020-01-01" not in text  # not the raw ISO bound

    def test_varinfo_full_year_is_bare_year(self, tmp_path):
        text = self._render_varinfo(tmp_path, "2020-01-01", "2020-12-31")
        assert "2020" in text
        assert "2020-01-01" not in text

    # ── open-ended window stays the raw range (sentinel, not collapsed) ──

    def test_schema_open_ended_is_raw_range(self, tmp_path):
        # `9999-12-31` = OPEN_ENDED_VALID_TO; the years differ so it falls
        # outside the grammar and must render as the explicit range — NOT a
        # bare year, and without crashing.
        text = self._render_schema(tmp_path, "2021-01-01", "9999-12-31")
        assert "2021-01-01..9999-12-31" in text

    def test_varinfo_open_ended_is_raw_range(self, tmp_path):
        text = self._render_varinfo(tmp_path, "2021-01-01", "9999-12-31")
        assert "2021-01-01..9999-12-31" in text
