"""Tests for config.py -- mdw_step2_config.json schema and lookups."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from mock_data_wizard.config import (
    ColumnTypeOverride,
    MDWConfig,
    load_config,
    parse_config,
)


# -- parse_config: schema validation -------------------------------------


def test_parse_config_minimal_valid():
    cfg = parse_config({"contract_version": "mdw-config-1.0.0"})
    assert cfg.contract_version == "mdw-config-1.0.0"
    assert cfg.column_types == {}
    assert cfg.column_options == {}


def test_parse_config_rejects_missing_contract_version():
    with pytest.raises(ValueError, match="missing required key 'contract_version'"):
        parse_config({})


def test_parse_config_rejects_unsupported_contract_version():
    with pytest.raises(ValueError, match="unsupported contract_version"):
        parse_config({"contract_version": "mdw-config-9.9.9"})


def test_parse_config_rejects_unknown_top_level_key():
    """A typo like 'column_type' must fail fast, not silently no-op."""
    with pytest.raises(ValueError, match="unknown top-level key"):
        parse_config({"contract_version": "mdw-config-1.0.0", "column_type": {}})


def test_parse_config_rejects_unknown_type():
    payload = {
        "contract_version": "mdw-config-1.0.0",
        "column_types": {"*": {"col": {"type": "blob"}}},
    }
    with pytest.raises(ValueError, match="expected one of"):
        parse_config(payload)


def test_parse_config_rejects_inline_hint_on_wrong_type():
    payload = {
        "contract_version": "mdw-config-1.0.0",
        # date_format is not valid for type=numeric
        "column_types": {"*": {"col": {"type": "numeric", "date_format": "%Y-%m-%d"}}},
    }
    with pytest.raises(ValueError, match="not valid for type"):
        parse_config(payload)


def test_parse_config_accepts_inline_subtypes():
    payload = {
        "contract_version": "mdw-config-1.0.0",
        "column_types": {
            "table_*": {
                "id_col": {"type": "id", "id_subtype": "string"},
                "n_col": {"type": "numeric", "numeric_subtype": "integer"},
                "d_col": {"type": "date", "date_format": "%Y%m%d"},
            }
        },
    }
    cfg = parse_config(payload)
    overrides = cfg.column_types["table_*"]
    assert overrides["id_col"].id_subtype == "string"
    assert overrides["id_col"].has_inline_hint()
    assert overrides["n_col"].numeric_subtype == "integer"
    assert overrides["d_col"].date_format == "%Y%m%d"


def test_parse_config_id_without_inline_hint_is_not_inline():
    cfg = parse_config(
        {
            "contract_version": "mdw-config-1.0.0",
            "column_types": {"*": {"col": {"type": "id"}}},
        }
    )
    assert cfg.column_types["*"]["col"].has_inline_hint() is False


def test_parse_config_rejects_invalid_subtype_value():
    with pytest.raises(ValueError, match="id_subtype="):
        parse_config(
            {
                "contract_version": "mdw-config-1.0.0",
                "column_types": {"*": {"col": {"type": "id", "id_subtype": "blob"}}},
            }
        )


# -- duplicate-key detection (object_pairs_hook) -------------------------


def test_load_config_rejects_duplicate_keys(tmp_path: Path):
    # json.loads silently keeps the second value on duplicate keys; the
    # object_pairs_hook should raise instead.
    raw = (
        '{"contract_version": "mdw-config-1.0.0", "column_types": {"t": {"col": {"type": "id"},'
        ' "col": {"type": "numeric"}}}}'
    )
    (tmp_path / "mdw_step2_config.json").write_text(raw, encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate key 'col'"):
        load_config(tmp_path)


# -- lookup behaviour ----------------------------------------------------


def test_lookup_type_matches_glob_and_column():
    cfg = parse_config(
        {
            "contract_version": "mdw-config-1.0.0",
            "column_types": {
                "Individ_*": {"Distriktskod": {"type": "opaque"}},
                "Pop_*": {"Salary": {"type": "numeric"}},
            },
        }
    )
    assert cfg.lookup_type("Individ_2018", "Distriktskod") == ColumnTypeOverride(
        type="opaque"
    )
    assert cfg.lookup_type("Pop_2024", "Salary") == ColumnTypeOverride(type="numeric")
    # Glob matches but column doesn't:
    assert cfg.lookup_type("Individ_2018", "Other") is None
    # Column matches but glob doesn't:
    assert cfg.lookup_type("Otherthing", "Distriktskod") is None


def test_lookup_type_last_glob_wins():
    cfg = parse_config(
        {
            "contract_version": "mdw-config-1.0.0",
            "column_types": {
                "*": {"col": {"type": "id"}},
                "Specific_*": {"col": {"type": "numeric"}},
            },
        }
    )
    # Last-match: list broad globs first, specific overrides below.
    # Symmetric with lookup_options' merge precedence.
    assert cfg.lookup_type("Specific_table", "col").type == "numeric"
    # A name only the broad glob matches still gets the broad rule.
    assert cfg.lookup_type("Other_table", "col").type == "id"


def test_lookup_options_merges_matching_globs():
    cfg = parse_config(
        {
            "contract_version": "mdw-config-1.0.0",
            "column_options": {
                "*": {"col": {"suppress_k": 10}},
                "Specific_*": {"col": {"suppress_k": 20}},
            },
        }
    )
    # Later-glob wins on key conflict (specific overrides general).
    assert cfg.lookup_options("Specific_x", "col") == {"suppress_k": 20}
    # Only the broad glob matches -> its value carries through.
    assert cfg.lookup_options("Other_x", "col") == {"suppress_k": 10}


# -- column_options validation -------------------------------------------


def test_parse_config_rejects_unknown_option_key():
    """A typo like 'supress_k' must fail fast, not silently no-op."""
    with pytest.raises(ValueError, match="unknown option 'supress_k'"):
        parse_config(
            {
                "contract_version": "mdw-config-1.0.0",
                "column_options": {"*": {"col": {"supress_k": 10}}},
            }
        )


def test_parse_config_rejects_suppress_k_below_floor():
    """suppress_k=0 would disable the disclosure-control gate."""
    with pytest.raises(ValueError, match="below the global minimum"):
        parse_config(
            {
                "contract_version": "mdw-config-1.0.0",
                "column_options": {"*": {"col": {"suppress_k": 0}}},
            }
        )


def test_parse_config_rejects_negative_suppress_k():
    with pytest.raises(ValueError, match="below the global minimum"):
        parse_config(
            {
                "contract_version": "mdw-config-1.0.0",
                "column_options": {"*": {"col": {"suppress_k": -5}}},
            }
        )


def test_parse_config_rejects_non_int_suppress_k():
    with pytest.raises(ValueError, match="suppress_k must be an int"):
        parse_config(
            {
                "contract_version": "mdw-config-1.0.0",
                "column_options": {"*": {"col": {"suppress_k": "10"}}},
            }
        )


def test_parse_config_rejects_bool_suppress_k():
    """``bool`` is an ``int`` subclass in Python; reject it explicitly."""
    with pytest.raises(ValueError, match="suppress_k must be an int"):
        parse_config(
            {
                "contract_version": "mdw-config-1.0.0",
                "column_options": {"*": {"col": {"suppress_k": True}}},
            }
        )


def test_parse_config_accepts_suppress_k_at_floor():
    cfg = parse_config(
        {
            "contract_version": "mdw-config-1.0.0",
            "column_options": {"*": {"col": {"suppress_k": 10}}},
        }
    )
    assert cfg.lookup_options("any_table", "col") == {"suppress_k": 10}


def test_parse_config_accepts_suppress_k_above_floor():
    cfg = parse_config(
        {
            "contract_version": "mdw-config-1.0.0",
            "column_options": {"*": {"col": {"suppress_k": 100}}},
        }
    )
    assert cfg.lookup_options("any_table", "col") == {"suppress_k": 100}


def test_parse_config_rejects_non_dict_options_value():
    with pytest.raises(ValueError, match="must be an object"):
        parse_config(
            {
                "contract_version": "mdw-config-1.0.0",
                "column_options": {"*": {"col": "not-a-dict"}},
            }
        )


# -- load_config: file-system integration --------------------------------


def test_load_config_returns_none_when_missing(tmp_path: Path):
    assert load_config(tmp_path) is None


def test_load_config_round_trips_through_disk(tmp_path: Path):
    payload = {
        "contract_version": "mdw-config-1.0.0",
        "column_types": {"Pop_*": {"Salary": {"type": "numeric"}}},
    }
    (tmp_path / "mdw_step2_config.json").write_text(
        json.dumps(payload), encoding="utf-8"
    )
    cfg = load_config(tmp_path)
    assert isinstance(cfg, MDWConfig)
    assert cfg.lookup_type("Pop_2024", "Salary").type == "numeric"


# -- per-source metadata (#24) ------------------------------------------


def test_parse_config_sources_year_round_trips():
    cfg = parse_config(
        {
            "contract_version": "mdw-config-1.0.0",
            "sources": {"lisa_2018": {"year": 2018}, "rtb_2019": {"year": 2019}},
        }
    )
    assert cfg.source_year("lisa_2018") == (True, 2018)
    assert cfg.source_year("rtb_2019") == (True, 2019)
    # No entry -> caller falls back.
    assert cfg.source_year("unknown") == (False, None)


def test_parse_config_sources_year_null_means_no_year():
    cfg = parse_config(
        {
            "contract_version": "mdw-config-1.0.0",
            "sources": {"weird_name_2024": {"year": None}},
        }
    )
    # Explicit null is configured-but-no-year (suppresses regex fallback).
    assert cfg.source_year("weird_name_2024") == (True, None)


def test_parse_config_rejects_unknown_source_key():
    with pytest.raises(ValueError, match="unknown key 'yr'"):
        parse_config(
            {
                "contract_version": "mdw-config-1.0.0",
                "sources": {"a": {"yr": 2018}},
            }
        )


def test_parse_config_rejects_string_year():
    with pytest.raises(ValueError, match="year must be an int"):
        parse_config(
            {
                "contract_version": "mdw-config-1.0.0",
                "sources": {"a": {"year": "2018"}},
            }
        )


def test_parse_config_rejects_bool_year():
    with pytest.raises(ValueError, match="year must be an int"):
        parse_config(
            {
                "contract_version": "mdw-config-1.0.0",
                "sources": {"a": {"year": True}},
            }
        )


def test_parse_config_rejects_non_dict_source_entry():
    with pytest.raises(ValueError, match="must be an object"):
        parse_config(
            {
                "contract_version": "mdw-config-1.0.0",
                "sources": {"a": 2018},
            }
        )


# -- panels (#23) --------------------------------------------------------


def test_parse_config_merged_table_panel():
    cfg = parse_config(
        {
            "contract_version": "mdw-config-1.0.0",
            "panels": [
                {
                    "panel_id": "swecov",
                    "layout": "merged_table",
                    "source": "SWECOV_SOS_SV",
                    "panel_key": "P1105_LopNr_PersonNr",
                    "time_key": "AR",
                }
            ],
        }
    )
    assert len(cfg.panels) == 1
    p = cfg.panels[0]
    assert p.panel_id == "swecov"
    assert p.layout == "merged_table"
    assert p.source == "SWECOV_SOS_SV"
    assert p.time_key == "AR"
    assert p.members == ()


def test_parse_config_separate_files_panel():
    cfg = parse_config(
        {
            "contract_version": "mdw-config-1.0.0",
            "panels": [
                {
                    "panel_id": "lisa",
                    "layout": "separate_files",
                    "panel_key": "LopNr",
                    "members": [
                        {"source": "lisa_2018.csv", "period": 2018},
                        {"source": "lisa_2019.csv", "period": 2019},
                    ],
                }
            ],
        }
    )
    p = cfg.panels[0]
    assert p.layout == "separate_files"
    assert [m.period for m in p.members] == [2018, 2019]
    assert p.source is None and p.time_key is None


def test_parse_config_rejects_duplicate_panel_id():
    with pytest.raises(ValueError, match="duplicate panel_id"):
        parse_config(
            {
                "contract_version": "mdw-config-1.0.0",
                "panels": [
                    {
                        "panel_id": "p",
                        "layout": "merged_table",
                        "source": "a",
                        "panel_key": "id",
                        "time_key": "t",
                    },
                    {
                        "panel_id": "p",
                        "layout": "merged_table",
                        "source": "b",
                        "panel_key": "id",
                        "time_key": "t",
                    },
                ],
            }
        )


def test_parse_config_rejects_unknown_panel_layout():
    with pytest.raises(ValueError, match="layout="):
        parse_config(
            {
                "contract_version": "mdw-config-1.0.0",
                "panels": [
                    {
                        "panel_id": "p",
                        "layout": "weird",
                        "panel_key": "id",
                    }
                ],
            }
        )


def test_parse_config_rejects_merged_panel_without_time_key():
    with pytest.raises(ValueError, match="non-empty string 'time_key'"):
        parse_config(
            {
                "contract_version": "mdw-config-1.0.0",
                "panels": [
                    {
                        "panel_id": "p",
                        "layout": "merged_table",
                        "panel_key": "id",
                        "source": "x",
                    }
                ],
            }
        )


def test_parse_config_rejects_separate_panel_without_members():
    with pytest.raises(ValueError, match="non-empty 'members'"):
        parse_config(
            {
                "contract_version": "mdw-config-1.0.0",
                "panels": [
                    {
                        "panel_id": "p",
                        "layout": "separate_files",
                        "panel_key": "id",
                    }
                ],
            }
        )


def test_parse_config_rejects_separate_panel_with_source_field():
    """A separate_files panel must not declare top-level 'source' --
    that's a merged_table-only field. Catching it stops misconfigured
    panels from silently behaving like merged_table."""
    with pytest.raises(ValueError, match="must not declare 'source'"):
        parse_config(
            {
                "contract_version": "mdw-config-1.0.0",
                "panels": [
                    {
                        "panel_id": "p",
                        "layout": "separate_files",
                        "panel_key": "id",
                        "source": "x",
                        "members": [{"source": "x", "period": 2018}],
                    }
                ],
            }
        )


def test_parse_config_rejects_two_merged_panels_on_same_source():
    """Two merged_table panels on one source would silently lose all but
    the last in the extract-side merged_panel_by_source map."""
    with pytest.raises(ValueError, match="both reference source 'x'"):
        parse_config(
            {
                "contract_version": "mdw-config-1.0.0",
                "panels": [
                    {
                        "panel_id": "a",
                        "layout": "merged_table",
                        "source": "x",
                        "panel_key": "id1",
                        "time_key": "t",
                    },
                    {
                        "panel_id": "b",
                        "layout": "merged_table",
                        "source": "x",
                        "panel_key": "id2",
                        "time_key": "t",
                    },
                ],
            }
        )


def test_parse_config_rejects_two_separate_panels_sharing_member_source():
    """Two separate_files panels listing the same source as a member
    would silently collide in generate.py's panel_by_source map (last
    write wins). Reject at parse time."""
    with pytest.raises(ValueError, match="both reference source 'shared.csv'"):
        parse_config(
            {
                "contract_version": "mdw-config-1.0.0",
                "panels": [
                    {
                        "panel_id": "a",
                        "layout": "separate_files",
                        "panel_key": "LopNr",
                        "members": [{"source": "shared.csv", "period": 2018}],
                    },
                    {
                        "panel_id": "b",
                        "layout": "separate_files",
                        "panel_key": "OrgNr",
                        "members": [{"source": "shared.csv", "period": 2018}],
                    },
                ],
            }
        )


def test_parse_config_rejects_merged_and_separate_sharing_source():
    """A merged_table source and a separate_files member referencing
    the same source would collide in panel_by_source (last write wins)."""
    with pytest.raises(ValueError, match="both reference source 'x.csv'"):
        parse_config(
            {
                "contract_version": "mdw-config-1.0.0",
                "panels": [
                    {
                        "panel_id": "merged",
                        "layout": "merged_table",
                        "source": "x.csv",
                        "panel_key": "LopNr",
                        "time_key": "ar",
                    },
                    {
                        "panel_id": "split",
                        "layout": "separate_files",
                        "panel_key": "OrgNr",
                        "members": [{"source": "x.csv", "period": 2018}],
                    },
                ],
            }
        )


def test_parse_config_rejects_two_panels_with_same_panel_key():
    """Two panels declaring the same panel_key would each build their
    own pool and clobber each other's entry in panel_pool_for_col --
    the second overwrites the first, leaving spine consumers and
    non-panel sources reading mismatched ids."""
    with pytest.raises(ValueError, match="panel_key='LopNr'"):
        parse_config(
            {
                "contract_version": "mdw-config-1.0.0",
                "panels": [
                    {
                        "panel_id": "a",
                        "layout": "merged_table",
                        "source": "x",
                        "panel_key": "LopNr",
                        "time_key": "t",
                    },
                    {
                        "panel_id": "b",
                        "layout": "separate_files",
                        "panel_key": "LopNr",
                        "members": [{"source": "y", "period": 2018}],
                    },
                ],
            }
        )


def test_parse_config_rejects_duplicate_panel_period():
    with pytest.raises(ValueError, match="duplicate period 2018"):
        parse_config(
            {
                "contract_version": "mdw-config-1.0.0",
                "panels": [
                    {
                        "panel_id": "p",
                        "layout": "separate_files",
                        "panel_key": "id",
                        "members": [
                            {"source": "a", "period": 2018},
                            {"source": "b", "period": 2018},
                        ],
                    }
                ],
            }
        )
