"""CLI `get values`: code lists, multi-state views and value-set groups."""

from __future__ import annotations

import json
import sqlite3

import pytest
from cli_test_support import build_cli_source, run_json as _run_json
from reg_meta.cli import run


@pytest.fixture(scope="module")
def groups_db(tmp_path_factory: pytest.TempPathFactory) -> str:
    return build_cli_source(tmp_path_factory.mktemp("groups"), "cli-values-groups")


def _year_groups(db: str, capsys: pytest.CaptureFixture[str]) -> dict:
    argv = ["--db", db, "--format", "json", "get", "values", "Sex", "--year", "2017"]
    assert run(argv) == 0
    return json.loads(capsys.readouterr().out)


def test_year_groups_disagreeing_value_sets_largest_first(
    groups_db: str, capsys: pytest.CaptureFixture[str]
) -> None:
    payload = _year_groups(groups_db, capsys)
    assert (
        payload["value_set_count"],
        payload["instance_count"],
        payload["register_count"],
    ) == (3, 15, 14)
    adults = [f"Reg{i:02d}" for i in range(1, 13)] + ["RegD"]
    assert [
        (
            [(value["code"], value["label"]) for value in group["values"]],
            group["instance_count"],
            group["register_count"],
            group["registers"],
            group["variable_slugs"],
        )
        for group in payload["groups"]
    ] == [
        ([("1", "Man"), ("2", "Woman")], 13, 13, adults, ["sex"]),
        ([("1", "Boy"), ("2", "Girl")], 1, 1, ["RegC"], ["sex-child"]),
        ([("F", "Female"), ("M", "Male")], 1, 1, ["RegD"], ["sex"]),
    ]


def test_year_groups_keep_per_column_owner_coordinates(
    groups_db: str, capsys: pytest.CaptureFixture[str]
) -> None:
    payload = _year_groups(groups_db, capsys)
    owners = [
        instance
        for group in payload["groups"]
        for instance in group["instances"]
        if instance["register_name"] == "RegD"
    ]
    assert [
        (owner["delivery_column_name"], owner["value_set_version_label"])
        for owner in owners
    ] == [("SexA", "native-a"), ("SexQ", "native-q")]
    assert owners[0]["state_id"] == owners[1]["state_id"]


# ---------------------------------------------------------------------------
# Get values
# ---------------------------------------------------------------------------


class TestGetValues:
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

    def test_by_variable_year_no_match(self, db_path: str):
        _data, code = _run_json(
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

    def test_ambiguous_alias_errors(self, tmp_path):
        """Column aliases shared across unrelated variables (e.g. "Rad")
        must error rather than silently merging value sets under one name.
        """
        import sqlite3

        from reg_meta.db import register_py_lower
        from reg_meta.queries import get_values_by_variable
        from reg_meta_build.db import DDL, seed_providers

        db = tmp_path / "amb.db"
        conn = sqlite3.connect(str(db))
        conn.row_factory = sqlite3.Row
        # Production runs `get_values_by_variable` on an `open_db` conn, which
        # registers the `py_lower` UDF its delivery_column_name fallback needs.
        register_py_lower(conn)
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

    from reg_meta.db import register_py_lower
    from reg_meta_build.db import DDL, seed_providers

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    register_py_lower(conn)  # `search`'s LIKE arms fold with it; `open_db` registers it
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
        # The yearless window alone would also answer 2099; pin the open one.
        assert "2015-01-01" in {inst["valid_from"] for inst in out["instances"]}

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


class TestSearchYearOverlap:
    """`search --years` uses window overlap, not the opening year."""

    def test_var_pair_kept_for_mid_year_of_multi_year_state(self):
        from reg_meta.queries import search

        conn = _overlap_db()
        # 2011 is the MID year of the multi-year state; the variable must survive.
        out = search(conn, "Kön", field="varname", years="2011")
        assert len(out.results) >= 1

    def test_var_pair_kept_for_far_future_open_ended(self):
        from reg_meta.queries import search

        conn = _overlap_db()
        # 2099 only overlaps the open-ended (2015..9999) and yearless windows.
        # Pre-fix `_years_in_range` capped both at their opening year, so 2099
        # matched nothing and the variable was wrongly dropped.
        out = search(conn, "Kön", field="varname", years="2099")
        assert len(out.results) >= 1

    def test_var_pair_kept_for_gap_year_covered_only_by_yearless(self):
        from reg_meta.queries import search

        conn = _overlap_db()
        # 2013 falls in the gap between the multi-year (..2012) and open-ended
        # (2015..) windows but is covered by the yearless window (0001..9999).
        # Pre-fix the yearless window enumerated as just [1], so 2013 matched no
        # state and the variable was dropped.
        out = search(conn, "Kön", field="varname", years="2013")
        assert len(out.results) >= 1

    def test_var_pair_dropped_when_no_window_covers_year(self):
        from reg_meta.queries import search

        conn = _overlap_db()
        # Year 0 is below every window (yearless starts at 0001) → no match.
        out = search(conn, "Kön", field="varname", years="0-0")
        assert len(out.results) == 0
