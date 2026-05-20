"""Tests for mock_data_wizard.spec (project_data.json loader + LoadedSpec)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from mock_data_wizard.spec import (
    PROJECT_DATA_FILENAME,
    ColumnTypeOverride,
    LoadedSpec,
    _validate_reg_monabundle_block,
    load_project_data,
    parse_project_data,
)

from tests.conftest import make_project_data

if TYPE_CHECKING:
    from pathlib import Path

# -- ColumnTypeOverride ---------------------------------------------------


def test_column_type_override_inline_hint_detection():
    assert ColumnTypeOverride(type="id", id_subtype="integer").has_inline_hint()
    assert ColumnTypeOverride(
        type="numeric", numeric_subtype="double"
    ).has_inline_hint()
    assert ColumnTypeOverride(type="date", date_format="%Y-%m-%d").has_inline_hint()
    assert not ColumnTypeOverride(type="id").has_inline_hint()
    assert not ColumnTypeOverride(type="categorical").has_inline_hint()
    assert not ColumnTypeOverride(type="opaque").has_inline_hint()


# -- LoadedSpec lookup surface --------------------------------------------


def _basic_payload() -> dict:
    return make_project_data(
        sources=[
            {
                "name": "lisa_2018.csv",
                "columns": [
                    {"display_name": "LopNr", "type": "id", "id_subtype": "integer"},
                    {"display_name": "Kon", "type": "categorical"},
                ],
            }
        ]
    )


def test_loaded_spec_lookup_type_returns_override_with_inline_hint():
    spec = parse_project_data(_basic_payload())
    ov = spec.lookup_type("lisa_2018.csv", "LopNr")
    assert ov is not None
    assert ov.type == "id"
    assert ov.id_subtype == "integer"
    assert ov.has_inline_hint()


def test_loaded_spec_lookup_type_unknown_returns_none():
    spec = parse_project_data(_basic_payload())
    assert spec.lookup_type("lisa_2018.csv", "Missing") is None
    assert spec.lookup_type("other.csv", "LopNr") is None


def test_loaded_spec_column_types_for_source_returns_cached_mutable_dict():
    spec = parse_project_data(_basic_payload())
    first = spec.column_types_for_source("lisa_2018.csv")
    # Mutate the returned dict — sources._probe_and_promote_opaque does
    # this in place; lookup_type must see the mutation on next access.
    first["LopNr"] = ColumnTypeOverride(type="numeric", numeric_subtype="integer")
    assert spec.lookup_type("lisa_2018.csv", "LopNr").type == "numeric"
    # Same dict object every call (cache is sticky).
    assert spec.column_types_for_source("lisa_2018.csv") is first


def test_loaded_spec_lookup_options_resolves_through_binding_fqid():
    payload = make_project_data(
        sources=[
            {
                "name": "x.csv",
                "register_version": "scb/test/_default/2020",
                "columns": [
                    {
                        "name": "scb/test/_default/2020/lopnr",
                        "display_name": "LopNr",
                        "type": "id",
                        "id_subtype": "integer",
                    },
                ],
            }
        ],
        reg_monabundle={
            "column_options": {
                "scb/test/_default/2020/lopnr": {"suppress_k": 25},
            }
        },
    )
    spec = parse_project_data(payload)
    assert spec.lookup_options("x.csv", "LopNr") == {"suppress_k": 25}
    # Returns a fresh copy so callers can't mutate the underlying spec.
    spec.lookup_options("x.csv", "LopNr")["suppress_k"] = 999
    assert spec.lookup_options("x.csv", "LopNr") == {"suppress_k": 25}


def test_loaded_spec_lookup_options_unknown_returns_empty_dict():
    spec = parse_project_data(_basic_payload())
    assert spec.lookup_options("lisa_2018.csv", "LopNr") == {}


def test_loaded_spec_source_year_always_returns_none():
    # MDWConfig had per-source year overrides; the project_data.json
    # schema dropped them in step 4. Callers fall back to the
    # source-name regex.
    spec = parse_project_data(_basic_payload())
    assert spec.source_year("lisa_2018.csv") is None
    assert spec.source_year("anything.csv") is None


def test_loaded_spec_panels_passthrough():
    payload = make_project_data(
        sources=[
            {
                "name": "a.csv",
                "columns": [
                    {"display_name": "LopNr", "type": "id", "id_subtype": "integer"}
                ],
            },
            {
                "name": "b.csv",
                "columns": [
                    {"display_name": "LopNr", "type": "id", "id_subtype": "integer"}
                ],
            },
        ],
        panels=[
            {
                "panel_id": "P1",
                "entity_key": "LopNr",
                "members": [
                    {"source": "a.csv", "time_key": 2018},
                    {"source": "b.csv", "time_key": 2019},
                ],
            }
        ],
    )
    spec = parse_project_data(payload)
    assert len(spec.panels) == 1
    p = spec.panels[0]
    assert p.panel_id == "P1"
    assert p.entity_key == "LopNr"
    assert p.members[0].source == "a.csv"
    assert p.members[0].time_key == 2018


# -- Step 4 boundary: rejections --------------------------------------------


def test_composite_entity_key_rejected_with_step_10b_message():
    payload = make_project_data(
        sources=[
            {
                "name": "a.csv",
                "columns": [
                    {"display_name": "LopNr", "type": "id", "id_subtype": "integer"},
                    {
                        "display_name": "Year",
                        "type": "numeric",
                        "numeric_subtype": "integer",
                    },
                ],
            }
        ],
        panels=[
            {
                "panel_id": "P1",
                "entity_key": ["LopNr", "Year"],
                "members": [{"source": "a.csv", "time_key": 2018}],
            }
        ],
    )
    with pytest.raises(ValueError, match="step 10b"):
        parse_project_data(payload)


def test_composite_time_key_rejected_with_step_10b_message():
    payload = make_project_data(
        sources=[
            {
                "name": "a.csv",
                "columns": [
                    {"display_name": "LopNr", "type": "id", "id_subtype": "integer"},
                    {
                        "display_name": "Year",
                        "type": "numeric",
                        "numeric_subtype": "integer",
                    },
                    {
                        "display_name": "Quarter",
                        "type": "numeric",
                        "numeric_subtype": "integer",
                    },
                ],
            }
        ],
        panels=[
            {
                "panel_id": "P1",
                "entity_key": "LopNr",
                "members": [
                    {"source": "a.csv", "time_key": ["Year", "Quarter"]},
                ],
            }
        ],
    )
    with pytest.raises(ValueError, match="step 10b"):
        parse_project_data(payload)


def test_literal_period_time_key_rejected_with_step_10b_message():
    payload = make_project_data(
        sources=[
            {
                "name": "a.csv",
                "columns": [
                    {"display_name": "LopNr", "type": "id", "id_subtype": "integer"},
                ],
            }
        ],
        panels=[
            {
                "panel_id": "P1",
                "entity_key": "LopNr",
                "members": [{"source": "a.csv", "time_key": {"period": "2018-01"}}],
            }
        ],
    )
    with pytest.raises(ValueError, match="step 10b"):
        parse_project_data(payload)


def test_panel_level_time_key_rejected_with_step_10b_message():
    payload = make_project_data(
        sources=[
            {
                "name": "a.csv",
                "columns": [
                    {"display_name": "LopNr", "type": "id", "id_subtype": "integer"},
                    {
                        "display_name": "Ar",
                        "type": "numeric",
                        "numeric_subtype": "integer",
                    },
                ],
            }
        ],
        panels=[
            {
                "panel_id": "P1",
                "entity_key": "LopNr",
                "time_key": "Ar",
                "members": [{"source": "a.csv", "time_key": "Ar"}],
            }
        ],
    )
    with pytest.raises(ValueError, match="step 10b"):
        parse_project_data(payload)


def test_member_level_entity_key_override_rejected():
    payload = make_project_data(
        sources=[
            {
                "name": "a.csv",
                "columns": [
                    {"display_name": "LopNr", "type": "id", "id_subtype": "integer"},
                    {"display_name": "Pnr", "type": "id", "id_subtype": "integer"},
                ],
            }
        ],
        panels=[
            {
                "panel_id": "P1",
                "entity_key": "LopNr",
                "members": [
                    {"source": "a.csv", "entity_key": "Pnr", "time_key": 2018},
                ],
            }
        ],
    )
    with pytest.raises(ValueError, match="step 10b"):
        parse_project_data(payload)


def test_display_name_required_at_load():
    payload = make_project_data(
        sources=[
            {
                "name": "a.csv",
                "columns": [
                    {
                        "name": "scb/test/_default/2020/lopnr",
                        "type": "id",
                        "id_subtype": "integer",
                    },
                ],
            }
        ],
    )
    with pytest.raises(ValueError, match="display_name"):
        parse_project_data(payload)


def test_datetime_column_type_rejected_with_actionable_message():
    """reg_schema accepts type='datetime' but mdw has no end-to-end
    datetime path (sql_emit / summarize / generate). Reject at load
    with a pointer to the workaround instead of surfacing the late
    ValueError from sql_emit."""
    payload = make_project_data(
        sources=[
            {
                "name": "a.csv",
                "columns": [
                    {
                        "display_name": "Timestamp",
                        "type": "datetime",
                        "datetime_format": "%Y-%m-%dT%H:%M:%S",
                    },
                ],
            }
        ],
    )
    with pytest.raises(ValueError, match="datetime.*not supported"):
        parse_project_data(payload)


# -- reg_monabundle namespaced block --------------------------------------


def test_reg_monabundle_block_accepts_well_formed_options():
    _validate_reg_monabundle_block(
        {"column_options": {"scb/test/_default/2020/lopnr": {"suppress_k": 25}}}
    )


def test_reg_monabundle_block_rejects_unknown_keys():
    with pytest.raises(ValueError, match="unknown key"):
        _validate_reg_monabundle_block({"unknown": {}})


def test_reg_monabundle_block_rejects_non_fqid_key():
    with pytest.raises(ValueError, match="binding FQID"):
        _validate_reg_monabundle_block(
            {"column_options": {"LopNr": {"suppress_k": 25}}}
        )


@pytest.mark.parametrize(
    "bad_key",
    [
        # Whitespace inside a segment — would silently no-op at runtime.
        "scb/test/_default/2020/lop nr",
        # Empty segment.
        "scb/test//2020/lopnr",
        # Wrong segment count (4).
        "scb/test/_default/2020",
        # Wrong segment count (6).
        "scb/test/_default/2020/lopnr/extra",
        # Classification FQID, not a binding.
        "class/sun/v1/lopnr/extra",
        # Disallowed character (period).
        "scb/test/_default/2020/lop.nr",
    ],
)
def test_reg_monabundle_block_rejects_malformed_fqid_variants(bad_key):
    """The column_options key check mirrors reg_schema's binding-FQID
    rule (5 segments, non-class provider, [A-Za-z0-9_-]+ per segment).
    A loose count('/') == 4 used to pass whitespace / empty segments /
    class-prefixed strings; this test pins the tighter check."""
    with pytest.raises(ValueError, match="binding FQID"):
        _validate_reg_monabundle_block(
            {"column_options": {bad_key: {"suppress_k": 25}}}
        )


def test_reg_monabundle_block_rejects_unknown_option():
    with pytest.raises(ValueError, match="unknown option"):
        _validate_reg_monabundle_block(
            {
                "column_options": {
                    "scb/test/_default/2020/lopnr": {"unknown_opt": 1},
                }
            }
        )


def test_reg_monabundle_block_rejects_suppress_k_below_floor():
    with pytest.raises(ValueError, match="below the global minimum"):
        _validate_reg_monabundle_block(
            {
                "column_options": {
                    "scb/test/_default/2020/lopnr": {"suppress_k": 1},
                }
            }
        )


def test_reg_monabundle_block_rejects_bool_suppress_k():
    with pytest.raises(ValueError, match="must be an int"):
        _validate_reg_monabundle_block(
            {
                "column_options": {
                    "scb/test/_default/2020/lopnr": {"suppress_k": True},
                }
            }
        )


# -- load_project_data (disk path) ----------------------------------------


def test_load_project_data_returns_none_when_file_absent(tmp_path: Path):
    assert load_project_data(tmp_path) is None


def test_load_project_data_round_trips_disk(tmp_path: Path):
    payload = _basic_payload()
    (tmp_path / PROJECT_DATA_FILENAME).write_text(json.dumps(payload), encoding="utf-8")
    spec = load_project_data(tmp_path)
    assert spec is not None
    assert spec.lookup_type("lisa_2018.csv", "LopNr").type == "id"


def test_load_project_data_rejects_duplicate_keys(tmp_path: Path):
    # Hand-write JSON with duplicate top-level key — json.load with the
    # default hook silently keeps the last; the spec loader must raise.
    bad_json = (
        '{"schema_version": "1.0.0", "schema_version": "2.0.0", '
        '"steward": "global", "reg_meta_version": "test", '
        '"name": "x", "sources": [], "panels": []}'
    )
    (tmp_path / PROJECT_DATA_FILENAME).write_text(bad_json, encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate key"):
        load_project_data(tmp_path)


def test_load_project_data_surfaces_structural_validation_errors(tmp_path: Path):
    # Drop required top-level field — structural validator raises.
    payload = _basic_payload()
    del payload["steward"]
    (tmp_path / PROJECT_DATA_FILENAME).write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="structural validation"):
        load_project_data(tmp_path)


def test_load_project_data_rejects_non_object_root(tmp_path: Path):
    (tmp_path / PROJECT_DATA_FILENAME).write_text("[]", encoding="utf-8")
    with pytest.raises(ValueError, match="top-level value must be an object"):
        load_project_data(tmp_path)


# -- LoadedSpec direct construction over a ProjectData --------------------


def test_loaded_spec_handles_source_with_no_matching_name():
    # Spec adapter must not crash on column_types_for_source for an
    # unknown source — the auto-promotion path needs an empty dict
    # rather than a KeyError.
    spec = parse_project_data(_basic_payload())
    assert isinstance(spec, LoadedSpec)
    assert spec.column_types_for_source("unknown_source") == {}
