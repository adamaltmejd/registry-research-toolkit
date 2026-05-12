"""Tests for ``mock_data_wizard.panels``.

Covers year detection, date-token parsing, panel-id/key suggestion, and
the multi-source panel-shape detector.
"""

from __future__ import annotations

import pytest

from mock_data_wizard.panels import (
    PanelCandidate,
    PanelMemberSuggestion,
    _build_panel_members,
    _DateToken,
    _find_time_key_in_source,
    _longest_common_prefix,
    _match_date_token,
    _resolve_period_month,
    _shared_id_column,
    detect_panel_candidate,
    detect_panel_member_kind,
    detect_year_from_source_name,
    suggest_panel_id,
)


# -- detect_year_from_source_name -----------------------------------------


@pytest.mark.parametrize(
    "name, expected",
    [
        ("Individ_2018", 2018),
        ("Individ_2018.csv", 2018),
        ("Foo_201907_Def", 2019),
        ("Kursprov_HT2011", 2011),
        ("dbo.scb_rams_2024", 2024),
        ("no_year_here", None),
        ("", None),
        # HT/VT term codes — Swedish academic-term shorthand. The earliest
        # match in scan order wins (HT20_VT21 → 2020, autumn term).
        ("Distansutb_grund_HT20_VT21", 2020),
        ("Distansutb_VT21", 2021),
        ("Distansutb_VT21.csv", 2021),
        # 4-digit year wins over a later term code; an earlier term code
        # wins over a later 4-digit year.
        ("rams_2024_HT25", 2024),
        ("HT19_followup_2024", 2019),
        # ``HT2020`` is unambiguous — the 4-digit pattern wins (term
        # pattern's negative lookahead blocks ``HT20`` here).
        ("survey_HT2020", 2020),
        # Pre-2000 term codes use the 1970+ century window.
        ("legacy_HT85", 1985),
    ],
)
def test_detect_year_from_source_name(name: str, expected: int | None):
    assert detect_year_from_source_name(name) == expected


# -- _match_date_token ----------------------------------------------------


def test_match_date_token_trailing_year():
    t = _match_date_token("Individ_2018")
    assert t == _DateToken(stem="Individ_", year=2018, month=None, suffix="")


def test_match_date_token_strips_csv_extension():
    t = _match_date_token("Individ_2018.csv")
    assert t is not None
    assert t.year == 2018
    assert t.suffix == ""


def test_match_date_token_year_month_embedded():
    t = _match_date_token("Arb_AGIIndivid201907_Def")
    assert t is not None
    assert (t.year, t.month) == (2019, 7)
    assert t.suffix == "_Def"


def test_match_date_token_yyyymmdd_returns_none():
    """A trailing YYYYMMDD timestamp must not parse as year=YYYY+month=MM
    (the trailing day digits would force the lookahead to fail)."""
    assert _match_date_token("foo_20241231") is None


def test_match_date_token_falls_back_to_raw_for_dotted_sql_table():
    """`dbo.scb_rams_2018` must not be over-shortened by extension stripping."""
    t = _match_date_token("dbo.scb_rams_2018")
    assert t is not None
    assert t.year == 2018


# -- _resolve_period_month ------------------------------------------------


def test_resolve_period_month_explicit_month_wins():
    t = _DateToken(stem="foo_", year=2019, month=7, suffix="")
    assert _resolve_period_month(t) == 7


def test_resolve_period_month_intra_year_tag():
    t = _DateToken(stem="Kursprov_HT_", year=2011, month=None, suffix="")
    assert _resolve_period_month(t) == 8  # HT → August


def test_resolve_period_month_no_signal():
    t = _DateToken(stem="Individ_", year=2018, month=None, suffix="")
    assert _resolve_period_month(t) is None


def test_date_token_shape_key_strips_intra_year_tag():
    t1 = _DateToken(stem="Kursprov_gymn_HT_", year=2011, month=None, suffix="")
    t2 = _DateToken(stem="Kursprov_gymn_VT_", year=2012, month=None, suffix="")
    assert t1.shape_key == t2.shape_key


# -- _longest_common_prefix -----------------------------------------------


def test_longest_common_prefix_basic():
    assert _longest_common_prefix(["foo_bar", "foo_baz"]) == "foo_ba"
    assert _longest_common_prefix(["foo", "foobar"]) == "foo"
    assert _longest_common_prefix([]) == ""
    assert _longest_common_prefix(["only_one"]) == "only_one"


# -- suggest_panel_id ------------------------------------------------------


def test_suggest_panel_id_year_only_panel():
    assert (
        suggest_panel_id(["Individ_2018", "Individ_2019", "Individ_2020"]) == "Individ"
    )


def test_suggest_panel_id_with_constant_suffix():
    assert (
        suggest_panel_id(["Arb_AGIIndivid201907_Def", "Arb_AGIIndivid202302_Def"])
        == "Arb_AGIIndivid_Def"
    )


def test_suggest_panel_id_falls_back_to_register_name():
    assert suggest_panel_id([], register_name="LISA") == "LISA"


def test_suggest_panel_id_falls_back_to_fallback_arg():
    assert suggest_panel_id([], fallback="grp-123") == "grp-123"


# -- _shared_id_column ----------------------------------------------------


def test_shared_id_column_single_shared_id():
    sources = [
        {"columns": [{"name": "LopNr"}, {"name": "Kommun"}]},
        {"columns": [{"name": "LopNr"}, {"name": "Other"}]},
    ]
    assert _shared_id_column(sources) == "LopNr"


def test_shared_id_column_prefers_personnr_variant():
    sources = [
        {"columns": [{"name": "LopNr"}, {"name": "LopNr_PersonNr"}]},
        {"columns": [{"name": "LopNr"}, {"name": "LopNr_PersonNr"}]},
    ]
    assert _shared_id_column(sources) == "LopNr_PersonNr"


def test_shared_id_column_no_shared_returns_none():
    sources = [
        {"columns": [{"name": "LopNr"}]},
        {"columns": [{"name": "PersonNr"}]},
    ]
    assert _shared_id_column(sources) is None


def test_shared_id_column_empty_columns():
    assert _shared_id_column([]) is None


# -- _build_panel_members -------------------------------------------------


def test_build_panel_members_year_as_period():
    members = _build_panel_members(
        ["Individ_2018", "Individ_2019"],
        years={"Individ_2018": 2018, "Individ_2019": 2019},
        months={"Individ_2018": None, "Individ_2019": None},
    )
    assert members == [
        {"source": "Individ_2018", "time_key": 2018},
        {"source": "Individ_2019", "time_key": 2019},
    ]


def test_build_panel_members_year_month_encoding():
    members = _build_panel_members(
        ["VT2012", "HT2012"],
        years={"VT2012": 2012, "HT2012": 2012},
        months={"VT2012": 1, "HT2012": 8},
    )
    # VT2012 = 201201 < HT2012 = 201208
    assert members == [
        {"source": "VT2012", "time_key": 201201},
        {"source": "HT2012", "time_key": 201208},
    ]


def test_build_panel_members_alphabetic_rank_fallback():
    """Two same-year sources without month info collapse to year*100+rank."""
    members = _build_panel_members(
        ["a_2018", "b_2018"],
        years={"a_2018": 2018, "b_2018": 2018},
        months={"a_2018": None, "b_2018": None},
    )
    # multiplier = 100; ranks 1, 2
    assert members == [
        {"source": "a_2018", "time_key": 201801},
        {"source": "b_2018", "time_key": 201802},
    ]


# -- _find_time_key_in_source ---------------------------------------------


@pytest.mark.parametrize(
    "columns, expected",
    [
        ([{"name": "AR"}, {"name": "x"}], "AR"),
        ([{"name": "Ar"}, {"name": "x"}], "Ar"),
        ([{"name": "INDATUM"}], "INDATUM"),
        ([{"name": "year"}], "year"),
        ([{"name": "period"}], "period"),
        ([{"name": "x"}, {"name": "y"}], None),
        ([], None),
    ],
)
def test_find_time_key_in_source(columns, expected):
    assert _find_time_key_in_source(columns) == expected


# -- detect_panel_member_kind ---------------------------------------------


def test_detect_panel_member_kind_file_member():
    s = detect_panel_member_kind("Individ_2018", ("LopNr", "Kommun"))
    assert s == PanelMemberSuggestion(
        kind="file", time_key=2018, suggested_entity_key="LopNr"
    )


def test_detect_panel_member_kind_column_member():
    s = detect_panel_member_kind("Population", ("LopNr", "AR"))
    assert s == PanelMemberSuggestion(
        kind="column", time_key="AR", suggested_entity_key="LopNr"
    )


def test_detect_panel_member_kind_file_takes_precedence():
    """When both shapes apply, file-member wins."""
    s = detect_panel_member_kind("Individ_2018", ("LopNr", "AR"))
    assert s.kind == "file"
    assert s.time_key == 2018


def test_detect_panel_member_kind_no_signal():
    s = detect_panel_member_kind("custom_table", ("col1", "col2"))
    assert s == PanelMemberSuggestion(kind=None, suggested_entity_key=None)


def test_detect_panel_member_kind_year_month_period():
    s = detect_panel_member_kind("Foo201907", ("LopNr",))
    assert s.kind == "file"
    assert s.time_key == 201907


# -- detect_panel_candidate (multi-source) --------------------------------


def test_detect_panel_candidate_multi_source_year_only():
    sources_by_name = {
        "Individ_2018": {"columns": [{"name": "LopNr"}]},
        "Individ_2019": {"columns": [{"name": "LopNr"}]},
    }
    cand = detect_panel_candidate(["Individ_2018", "Individ_2019"], sources_by_name)
    assert isinstance(cand, PanelCandidate)
    assert cand.suggested_entity_key == "LopNr"
    assert cand.suggested_panel_id == "Individ"
    assert [m["time_key"] for m in cand.members] == [2018, 2019]


def test_detect_panel_candidate_singleton_with_time_key():
    sources_by_name = {"Population": {"columns": [{"name": "LopNr"}, {"name": "AR"}]}}
    cand = detect_panel_candidate(["Population"], sources_by_name)
    assert isinstance(cand, PanelCandidate)
    assert cand.members == ({"source": "Population", "time_key": "AR"},)
    assert cand.suggested_panel_id == "Population"
    assert cand.suggested_entity_key == "LopNr"


def test_detect_panel_candidate_singleton_no_time_key_returns_none():
    sources_by_name = {"x": {"columns": [{"name": "Foo"}]}}
    assert detect_panel_candidate(["x"], sources_by_name) is None


def test_detect_panel_candidate_mismatched_shapes_returns_none():
    """`foo_2019_def` and `foo_2020_xyz` differ in suffix → not a panel."""
    sources_by_name = {
        "foo_2019_def": {"columns": [{"name": "LopNr"}]},
        "foo_2020_xyz": {"columns": [{"name": "LopNr"}]},
    }
    assert (
        detect_panel_candidate(["foo_2019_def", "foo_2020_xyz"], sources_by_name)
        is None
    )


def test_detect_panel_candidate_mixed_granularity_returns_none():
    """Year-only + year+month must not merge into one panel."""
    sources_by_name = {
        "Foo_2018": {"columns": [{"name": "LopNr"}]},
        "Foo201907": {"columns": [{"name": "LopNr"}]},
    }
    assert detect_panel_candidate(["Foo_2018", "Foo201907"], sources_by_name) is None


def test_detect_panel_candidate_no_date_token_returns_none():
    sources_by_name = {
        "x": {"columns": [{"name": "LopNr"}]},
        "y": {"columns": [{"name": "LopNr"}]},
    }
    assert detect_panel_candidate(["x", "y"], sources_by_name) is None
