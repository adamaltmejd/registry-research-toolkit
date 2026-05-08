"""Tests for config.py -- mock_data_config.json schema and lookups."""

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
    cfg = parse_config({"contract_version": "mdw-config-3.0.0"})
    assert cfg.contract_version == "mdw-config-3.0.0"
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
        parse_config({"contract_version": "mdw-config-3.0.0", "column_type": {}})


def test_parse_config_rejects_unknown_type():
    payload = {
        "contract_version": "mdw-config-3.0.0",
        "column_types": {"*": {"col": {"type": "blob"}}},
    }
    with pytest.raises(ValueError, match="expected one of"):
        parse_config(payload)


def test_parse_config_rejects_inline_hint_on_wrong_type():
    payload = {
        "contract_version": "mdw-config-3.0.0",
        # date_format is not valid for type=numeric
        "column_types": {"*": {"col": {"type": "numeric", "date_format": "%Y-%m-%d"}}},
    }
    with pytest.raises(ValueError, match="not valid for type"):
        parse_config(payload)


def test_parse_config_accepts_inline_subtypes():
    payload = {
        "contract_version": "mdw-config-3.0.0",
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
            "contract_version": "mdw-config-3.0.0",
            "column_types": {"*": {"col": {"type": "id"}}},
        }
    )
    assert cfg.column_types["*"]["col"].has_inline_hint() is False


def test_parse_config_rejects_invalid_subtype_value():
    with pytest.raises(ValueError, match="id_subtype="):
        parse_config(
            {
                "contract_version": "mdw-config-3.0.0",
                "column_types": {"*": {"col": {"type": "id", "id_subtype": "blob"}}},
            }
        )


# -- duplicate-key detection (object_pairs_hook) -------------------------


def test_load_config_rejects_duplicate_keys(tmp_path: Path):
    # json.loads silently keeps the second value on duplicate keys; the
    # object_pairs_hook should raise instead.
    raw = (
        '{"contract_version": "mdw-config-3.0.0", "column_types": {"t": {"col": {"type": "id"},'
        ' "col": {"type": "numeric"}}}}'
    )
    (tmp_path / "mock_data_config.json").write_text(raw, encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate key 'col'"):
        load_config(tmp_path)


# -- lookup behaviour ----------------------------------------------------


def test_lookup_type_exact_source_match():
    """3.0.0 dropped fnmatchcase globs; lookups are exact-name dict access."""
    cfg = parse_config(
        {
            "contract_version": "mdw-config-3.0.0",
            "column_types": {
                "Individ_2018": {"Distriktskod": {"type": "opaque"}},
                "Pop_2024": {"Salary": {"type": "numeric"}},
            },
        }
    )
    assert cfg.lookup_type("Individ_2018", "Distriktskod") == ColumnTypeOverride(
        type="opaque"
    )
    assert cfg.lookup_type("Pop_2024", "Salary") == ColumnTypeOverride(type="numeric")
    # Source matches but column doesn't:
    assert cfg.lookup_type("Individ_2018", "Other") is None
    # No glob expansion: a glob-shaped key only matches itself literally.
    assert cfg.lookup_type("Individ_2019", "Distriktskod") is None


def test_lookup_options_exact_source_match():
    cfg = parse_config(
        {
            "contract_version": "mdw-config-3.0.0",
            "column_options": {"Specific_x": {"col": {"suppress_k": 20}}},
        }
    )
    assert cfg.lookup_options("Specific_x", "col") == {"suppress_k": 20}
    # No fallback to broad keys; an unmatched source returns empty.
    assert cfg.lookup_options("Other_x", "col") == {}


def test_lookup_options_returns_copy():
    """Caller must not be able to mutate the stored dict."""
    cfg = parse_config(
        {
            "contract_version": "mdw-config-3.0.0",
            "column_options": {"src": {"col": {"suppress_k": 20}}},
        }
    )
    out = cfg.lookup_options("src", "col")
    out["suppress_k"] = 999
    assert cfg.lookup_options("src", "col") == {"suppress_k": 20}


# -- column_options validation -------------------------------------------


def test_parse_config_rejects_unknown_option_key():
    """A typo like 'supress_k' must fail fast, not silently no-op."""
    with pytest.raises(ValueError, match="unknown option 'supress_k'"):
        parse_config(
            {
                "contract_version": "mdw-config-3.0.0",
                "column_options": {"*": {"col": {"supress_k": 10}}},
            }
        )


def test_parse_config_rejects_suppress_k_below_floor():
    """suppress_k=0 would disable the disclosure-control gate."""
    with pytest.raises(ValueError, match="below the global minimum"):
        parse_config(
            {
                "contract_version": "mdw-config-3.0.0",
                "column_options": {"*": {"col": {"suppress_k": 0}}},
            }
        )


def test_parse_config_rejects_negative_suppress_k():
    with pytest.raises(ValueError, match="below the global minimum"):
        parse_config(
            {
                "contract_version": "mdw-config-3.0.0",
                "column_options": {"*": {"col": {"suppress_k": -5}}},
            }
        )


def test_parse_config_rejects_non_int_suppress_k():
    with pytest.raises(ValueError, match="suppress_k must be an int"):
        parse_config(
            {
                "contract_version": "mdw-config-3.0.0",
                "column_options": {"*": {"col": {"suppress_k": "10"}}},
            }
        )


def test_parse_config_rejects_bool_suppress_k():
    """``bool`` is an ``int`` subclass in Python; reject it explicitly."""
    with pytest.raises(ValueError, match="suppress_k must be an int"):
        parse_config(
            {
                "contract_version": "mdw-config-3.0.0",
                "column_options": {"*": {"col": {"suppress_k": True}}},
            }
        )


def test_parse_config_accepts_suppress_k_at_floor():
    cfg = parse_config(
        {
            "contract_version": "mdw-config-3.0.0",
            "column_options": {"my_src": {"col": {"suppress_k": 10}}},
        }
    )
    assert cfg.lookup_options("my_src", "col") == {"suppress_k": 10}


def test_parse_config_accepts_suppress_k_above_floor():
    cfg = parse_config(
        {
            "contract_version": "mdw-config-3.0.0",
            "column_options": {"my_src": {"col": {"suppress_k": 100}}},
        }
    )
    assert cfg.lookup_options("my_src", "col") == {"suppress_k": 100}


def test_parse_config_rejects_non_dict_options_value():
    with pytest.raises(ValueError, match="must be an object"):
        parse_config(
            {
                "contract_version": "mdw-config-3.0.0",
                "column_options": {"*": {"col": "not-a-dict"}},
            }
        )


# -- load_config: file-system integration --------------------------------


def test_load_config_returns_none_when_missing(tmp_path: Path):
    assert load_config(tmp_path) is None


def test_load_config_round_trips_through_disk(tmp_path: Path):
    payload = {
        "contract_version": "mdw-config-3.0.0",
        "column_types": {"Pop_2024": {"Salary": {"type": "numeric"}}},
    }
    (tmp_path / "mock_data_config.json").write_text(
        json.dumps(payload), encoding="utf-8"
    )
    cfg = load_config(tmp_path)
    assert isinstance(cfg, MDWConfig)
    assert cfg.lookup_type("Pop_2024", "Salary").type == "numeric"


# -- 3.0.0 schema additions ----------------------------------------------


def test_parse_config_pre_3_0_0_rejected_with_regenerate_hint():
    """Pre-3.0.0 contract versions raise with an actionable hint to
    regenerate via the editor — no migration code lives in this build."""
    with pytest.raises(ValueError, match="Regenerate.*editor"):
        parse_config({"contract_version": "mdw-config-2.0.0"})
    with pytest.raises(ValueError, match="Regenerate.*editor"):
        parse_config({"contract_version": "mdw-config-1.0.0"})


def test_parse_config_register_in_sources_round_trips():
    cfg = parse_config(
        {
            "contract_version": "mdw-config-3.0.0",
            "sources": {
                "lisa_2018": {"year": 2018, "register": "LISA"},
                "rtb_2019": {"year": 2019, "register": None},
                "custom": {"register": "RAMS"},
            },
        }
    )
    assert cfg.sources["lisa_2018"]["register"] == "LISA"
    assert cfg.sources["rtb_2019"]["register"] is None
    assert cfg.sources["custom"]["register"] == "RAMS"


def test_parse_config_rejects_non_string_register():
    with pytest.raises(ValueError, match="register must be a string"):
        parse_config(
            {
                "contract_version": "mdw-config-3.0.0",
                "sources": {"a": {"register": 34}},
            }
        )


def test_parse_config_manual_columns_round_trips():
    cfg = parse_config(
        {
            "contract_version": "mdw-config-3.0.0",
            "manual_columns": [
                ["lisa_2018", "Distriktskod"],
                ["rtb_2019", "AterAnv"],
            ],
        }
    )
    assert cfg.manual_columns == (
        ("lisa_2018", "Distriktskod"),
        ("rtb_2019", "AterAnv"),
    )


def test_parse_config_manual_columns_default_empty():
    cfg = parse_config({"contract_version": "mdw-config-3.0.0"})
    assert cfg.manual_columns == ()


def test_parse_config_rejects_manual_column_pair_wrong_length():
    with pytest.raises(ValueError, match="2-element list"):
        parse_config(
            {
                "contract_version": "mdw-config-3.0.0",
                "manual_columns": [["only_source"]],
            }
        )


def test_parse_config_rejects_manual_column_non_string_entry():
    with pytest.raises(ValueError, match="must be a non-empty string"):
        parse_config(
            {
                "contract_version": "mdw-config-3.0.0",
                "manual_columns": [["src", 42]],
            }
        )


def test_parse_config_rejects_duplicate_manual_columns():
    with pytest.raises(ValueError, match="duplicate entry"):
        parse_config(
            {
                "contract_version": "mdw-config-3.0.0",
                "manual_columns": [["a", "b"], ["a", "b"]],
            }
        )


def test_parse_config_discover_hash_round_trips():
    cfg = parse_config(
        {
            "contract_version": "mdw-config-3.0.0",
            "discover_hash": "abc123",
        }
    )
    assert cfg.discover_hash == "abc123"


def test_parse_config_rejects_non_string_discover_hash():
    with pytest.raises(ValueError, match="discover_hash must be a string"):
        parse_config({"contract_version": "mdw-config-3.0.0", "discover_hash": 42})


# -- per-source metadata (#24) ------------------------------------------


def test_parse_config_sources_year_round_trips():
    cfg = parse_config(
        {
            "contract_version": "mdw-config-3.0.0",
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
            "contract_version": "mdw-config-3.0.0",
            "sources": {"weird_name_2024": {"year": None}},
        }
    )
    # Explicit null is configured-but-no-year (suppresses regex fallback).
    assert cfg.source_year("weird_name_2024") == (True, None)


def test_parse_config_rejects_unknown_source_key():
    with pytest.raises(ValueError, match="unknown key 'yr'"):
        parse_config(
            {
                "contract_version": "mdw-config-3.0.0",
                "sources": {"a": {"yr": 2018}},
            }
        )


def test_parse_config_rejects_string_year():
    with pytest.raises(ValueError, match="year must be an int"):
        parse_config(
            {
                "contract_version": "mdw-config-3.0.0",
                "sources": {"a": {"year": "2018"}},
            }
        )


def test_parse_config_rejects_bool_year():
    with pytest.raises(ValueError, match="year must be an int"):
        parse_config(
            {
                "contract_version": "mdw-config-3.0.0",
                "sources": {"a": {"year": True}},
            }
        )


def test_parse_config_rejects_non_dict_source_entry():
    with pytest.raises(ValueError, match="must be an object"):
        parse_config(
            {
                "contract_version": "mdw-config-3.0.0",
                "sources": {"a": 2018},
            }
        )


# -- panels --------------------------------------------------------------


def test_parse_config_panel_with_period_members():
    cfg = parse_config(
        {
            "contract_version": "mdw-config-3.0.0",
            "panels": [
                {
                    "panel_id": "lisa",
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
    assert p.panel_id == "lisa"
    assert p.panel_key == "LopNr"
    assert [(m.source, m.period, m.time_key) for m in p.members] == [
        ("lisa_2018.csv", 2018, None),
        ("lisa_2019.csv", 2019, None),
    ]


def test_parse_config_panel_with_time_key_member():
    cfg = parse_config(
        {
            "contract_version": "mdw-config-3.0.0",
            "panels": [
                {
                    "panel_id": "swecov",
                    "panel_key": "P1105_LopNr_PersonNr",
                    "members": [
                        {"source": "SWECOV_SOS_SV", "time_key": "AR"},
                    ],
                }
            ],
        }
    )
    p = cfg.panels[0]
    assert len(p.members) == 1
    m = p.members[0]
    assert m.source == "SWECOV_SOS_SV"
    assert m.time_key == "AR"
    assert m.period is None


def test_parse_config_panel_mixes_period_and_time_key_members():
    """A panel can intermix file-members and column-members — e.g.
    historical years in one merged file, the latest year in a fresh
    delivery."""
    cfg = parse_config(
        {
            "contract_version": "mdw-config-3.0.0",
            "panels": [
                {
                    "panel_id": "tax",
                    "panel_key": "LopNr",
                    "members": [
                        {"source": "tax_history.csv", "time_key": "AR"},
                        {"source": "tax_2024.csv", "period": 2024},
                    ],
                }
            ],
        }
    )
    p = cfg.panels[0]
    assert [(m.source, m.period, m.time_key) for m in p.members] == [
        ("tax_history.csv", None, "AR"),
        ("tax_2024.csv", 2024, None),
    ]


def test_parse_config_rejects_member_without_period_or_time_key():
    with pytest.raises(ValueError, match="exactly one of 'period' or 'time_key'"):
        parse_config(
            {
                "contract_version": "mdw-config-3.0.0",
                "panels": [
                    {
                        "panel_id": "p",
                        "panel_key": "id",
                        "members": [{"source": "x"}],
                    }
                ],
            }
        )


def test_parse_config_rejects_member_with_both_period_and_time_key():
    with pytest.raises(ValueError, match="exactly one of 'period' or 'time_key'"):
        parse_config(
            {
                "contract_version": "mdw-config-3.0.0",
                "panels": [
                    {
                        "panel_id": "p",
                        "panel_key": "id",
                        "members": [{"source": "x", "period": 2018, "time_key": "AR"}],
                    }
                ],
            }
        )


def test_parse_config_rejects_duplicate_panel_id():
    with pytest.raises(ValueError, match="duplicate panel_id"):
        parse_config(
            {
                "contract_version": "mdw-config-3.0.0",
                "panels": [
                    {
                        "panel_id": "p",
                        "panel_key": "id1",
                        "members": [{"source": "a", "time_key": "AR"}],
                    },
                    {
                        "panel_id": "p",
                        "panel_key": "id2",
                        "members": [{"source": "b", "time_key": "AR"}],
                    },
                ],
            }
        )


def test_parse_config_rejects_panel_without_members():
    with pytest.raises(ValueError, match="non-empty list"):
        parse_config(
            {
                "contract_version": "mdw-config-3.0.0",
                "panels": [{"panel_id": "p", "panel_key": "id", "members": []}],
            }
        )


def test_parse_config_rejects_two_panels_sharing_a_source():
    """A source can only belong to one panel; otherwise generate.py's
    flat panel_by_source map silently drops one."""
    with pytest.raises(ValueError, match="both reference source 'shared.csv'"):
        parse_config(
            {
                "contract_version": "mdw-config-3.0.0",
                "panels": [
                    {
                        "panel_id": "a",
                        "panel_key": "LopNr",
                        "members": [{"source": "shared.csv", "period": 2018}],
                    },
                    {
                        "panel_id": "b",
                        "panel_key": "OrgNr",
                        "members": [{"source": "shared.csv", "period": 2019}],
                    },
                ],
            }
        )


def test_parse_config_allows_two_panels_with_same_panel_key():
    """SCB registers routinely share a person-id panel_key across
    many distinct panels. parse_config accepts it; generate.py builds
    one shared pool per panel_key."""
    cfg = parse_config(
        {
            "contract_version": "mdw-config-3.0.0",
            "panels": [
                {
                    "panel_id": "lisa",
                    "panel_key": "P1105_LopNr_PersonNr",
                    "members": [{"source": "x", "time_key": "AR"}],
                },
                {
                    "panel_id": "rtb",
                    "panel_key": "P1105_LopNr_PersonNr",
                    "members": [{"source": "y", "period": 2018}],
                },
            ],
        }
    )
    keys = [p.panel_key for p in cfg.panels]
    assert keys == ["P1105_LopNr_PersonNr", "P1105_LopNr_PersonNr"]


def test_parse_config_rejects_duplicate_period_in_one_panel():
    """Period uniqueness is enforced across file-members. (Column
    members produce periods at runtime; their uniqueness is validated
    in extract.)"""
    with pytest.raises(ValueError, match="duplicate period 2018"):
        parse_config(
            {
                "contract_version": "mdw-config-3.0.0",
                "panels": [
                    {
                        "panel_id": "p",
                        "panel_key": "id",
                        "members": [
                            {"source": "a", "period": 2018},
                            {"source": "b", "period": 2018},
                        ],
                    }
                ],
            }
        )


def test_parse_config_rejects_duplicate_source_in_one_panel():
    with pytest.raises(ValueError, match="duplicate source 'x'"):
        parse_config(
            {
                "contract_version": "mdw-config-3.0.0",
                "panels": [
                    {
                        "panel_id": "p",
                        "panel_key": "id",
                        "members": [
                            {"source": "x", "period": 2018},
                            {"source": "x", "period": 2019},
                        ],
                    }
                ],
            }
        )
