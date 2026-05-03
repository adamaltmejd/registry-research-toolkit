"""Tests for config.py -- mdw_config.json schema and lookups."""

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
    cfg = parse_config({"version": 1})
    assert cfg.version == 1
    assert cfg.column_types == {}
    assert cfg.column_options == {}


def test_parse_config_rejects_missing_version():
    with pytest.raises(ValueError, match="missing required key 'version'"):
        parse_config({})


def test_parse_config_rejects_unsupported_version():
    with pytest.raises(ValueError, match="unsupported version"):
        parse_config({"version": 99})


def test_parse_config_rejects_unknown_top_level_key():
    """A typo like 'column_type' must fail fast, not silently no-op."""
    with pytest.raises(ValueError, match="unknown top-level key"):
        parse_config({"version": 1, "column_type": {}})


def test_parse_config_rejects_unknown_type():
    payload = {
        "version": 1,
        "column_types": {"*": {"col": {"type": "blob"}}},
    }
    with pytest.raises(ValueError, match="expected one of"):
        parse_config(payload)


def test_parse_config_rejects_inline_hint_on_wrong_type():
    payload = {
        "version": 1,
        # date_format is not valid for type=numeric
        "column_types": {"*": {"col": {"type": "numeric", "date_format": "%Y-%m-%d"}}},
    }
    with pytest.raises(ValueError, match="not valid for type"):
        parse_config(payload)


def test_parse_config_accepts_inline_subtypes():
    payload = {
        "version": 1,
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
    cfg = parse_config({"version": 1, "column_types": {"*": {"col": {"type": "id"}}}})
    assert cfg.column_types["*"]["col"].has_inline_hint() is False


def test_parse_config_rejects_invalid_subtype_value():
    with pytest.raises(ValueError, match="id_subtype="):
        parse_config(
            {
                "version": 1,
                "column_types": {"*": {"col": {"type": "id", "id_subtype": "blob"}}},
            }
        )


# -- duplicate-key detection (object_pairs_hook) -------------------------


def test_load_config_rejects_duplicate_keys(tmp_path: Path):
    # json.loads silently keeps the second value on duplicate keys; the
    # object_pairs_hook should raise instead.
    raw = (
        '{"version": 1, "column_types": {"t": {"col": {"type": "id"},'
        ' "col": {"type": "numeric"}}}}'
    )
    (tmp_path / "mdw_config.json").write_text(raw, encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate key 'col'"):
        load_config(tmp_path)


# -- lookup behaviour ----------------------------------------------------


def test_lookup_type_matches_glob_and_column():
    cfg = parse_config(
        {
            "version": 1,
            "column_types": {
                "Individ_*": {"Distriktskod": {"type": "high_cardinality"}},
                "Pop_*": {"Salary": {"type": "numeric"}},
            },
        }
    )
    assert cfg.lookup_type("Individ_2018", "Distriktskod") == ColumnTypeOverride(
        type="high_cardinality"
    )
    assert cfg.lookup_type("Pop_2024", "Salary") == ColumnTypeOverride(type="numeric")
    # Glob matches but column doesn't:
    assert cfg.lookup_type("Individ_2018", "Other") is None
    # Column matches but glob doesn't:
    assert cfg.lookup_type("Otherthing", "Distriktskod") is None


def test_lookup_type_last_glob_wins():
    cfg = parse_config(
        {
            "version": 1,
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
            "version": 1,
            "column_options": {
                "*": {"col": {"suppress_k": 5}},
                "Specific_*": {"col": {"suppress_k": 20, "extra": "y"}},
            },
        }
    )
    # Later-glob wins on key conflict (specific overrides general).
    merged = cfg.lookup_options("Specific_x", "col")
    assert merged == {"suppress_k": 20, "extra": "y"}


# -- load_config: file-system integration --------------------------------


def test_load_config_returns_none_when_missing(tmp_path: Path):
    assert load_config(tmp_path) is None


def test_load_config_round_trips_through_disk(tmp_path: Path):
    payload = {
        "version": 1,
        "column_types": {"Pop_*": {"Salary": {"type": "numeric"}}},
    }
    (tmp_path / "mdw_config.json").write_text(json.dumps(payload), encoding="utf-8")
    cfg = load_config(tmp_path)
    assert isinstance(cfg, MDWConfig)
    assert cfg.lookup_type("Pop_2024", "Salary").type == "numeric"
