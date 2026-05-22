"""Tests for ``reg_monabundle.runtime.spec`` (project_data.json loader + LoadedSpec)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from _project_data_fixtures import make_project_data
from reg_monabundle.runtime.spec import (
    PROJECT_DATA_FILENAME,
    ColumnTypeOverride,
    LoadedSpec,
    load_project_data,
    parse_project_data,
)

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
                        "name": "scb/test/_default/2020/kon",
                        "display_name": "Kon",
                        "type": "categorical",
                    },
                ],
            }
        ],
        reg_monabundle={
            "column_options": {
                "scb/test/_default/2020/kon": {"suppress_k": 25},
            }
        },
    )
    spec = parse_project_data(payload)
    assert spec.lookup_options("x.csv", "Kon") == {"suppress_k": 25}
    # Returns a fresh copy so callers can't mutate the underlying spec.
    spec.lookup_options("x.csv", "Kon")["suppress_k"] = 999
    assert spec.lookup_options("x.csv", "Kon") == {"suppress_k": 25}


def test_loaded_spec_lookup_options_unknown_returns_empty_dict():
    spec = parse_project_data(_basic_payload())
    assert spec.lookup_options("lisa_2018.csv", "LopNr") == {}


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
#
# Per-rule validator coverage lives in
# reg_monabundle/tests/test_validate_block.py alongside the validator
# (§15 step 5 phase 1 — owner-validates-its-block).
# The tests that remain here exercise the cross-block referential checks
# (``_validate_column_options_against_columns``) that still need the
# resolved column dataclasses and so stay in ``reg_monabundle.runtime.spec``.


def test_parse_project_data_invokes_namespaced_block_validator():
    """Smoke test that ``parse_project_data`` still routes the
    ``reg_monabundle`` block through ``reg_monabundle.validate_block`` after
    the §15 step 5 phase 1 relocation. One representative failure mode is
    enough — the validator's own test suite owns the per-rule coverage."""
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
        reg_monabundle={"unknown": {}},
    )
    with pytest.raises(ValueError, match="unknown key"):
        parse_project_data(payload)


def test_column_options_rejects_orphan_fqid_not_matching_any_column():
    """A well-formed FQID that doesn't match any column.name in sources
    silently no-ops at lookup time without this check. Pin the
    referential-integrity guard so a typo surfaces at load."""
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
            # FQID is well-formed but no column declares this name.
            "column_options": {
                "scb/test/_default/2020/typo_here": {"suppress_k": 25},
            }
        },
    )
    with pytest.raises(ValueError, match="don't match any column FQID"):
        parse_project_data(payload)


def test_column_options_accepts_matching_fqid():
    """Sanity: the referential check doesn't reject a key that does
    match a declared column."""
    payload = make_project_data(
        sources=[
            {
                "name": "x.csv",
                "register_version": "scb/test/_default/2020",
                "columns": [
                    {
                        "name": "scb/test/_default/2020/kon",
                        "display_name": "Kon",
                        "type": "categorical",
                    },
                ],
            }
        ],
        reg_monabundle={
            "column_options": {
                "scb/test/_default/2020/kon": {"suppress_k": 25},
            }
        },
    )
    spec = parse_project_data(payload)
    assert spec.lookup_options("x.csv", "Kon") == {"suppress_k": 25}


# -- column_options type compatibility ------------------------------------


@pytest.mark.parametrize(
    "col, suffix",
    [
        ({"type": "id", "id_subtype": "integer"}, "lopnr"),
        ({"type": "numeric", "numeric_subtype": "integer"}, "ar"),
        ({"type": "date", "date_format": "%Y-%m-%d"}, "datum"),
        ({"type": "opaque"}, "namn"),
    ],
)
def test_column_options_rejects_suppress_k_on_non_categorical(col, suffix):
    """``suppress_k`` only feeds the categorical frequency cutoff in
    summarize_column; the id/numeric/date/opaque branches ignore it,
    so accepting it there is a silent no-op. Reject at load and
    point at the future panels[*].suppress_k for panel-level k."""
    fqid = f"scb/test/_default/2020/{suffix}"
    payload = make_project_data(
        sources=[
            {
                "name": "x.csv",
                "register_version": "scb/test/_default/2020",
                "columns": [
                    {"name": fqid, "display_name": suffix.upper(), **col},
                ],
            }
        ],
        reg_monabundle={"column_options": {fqid: {"suppress_k": 25}}},
    )
    with pytest.raises(ValueError, match="only honored on categorical"):
        parse_project_data(payload)


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
