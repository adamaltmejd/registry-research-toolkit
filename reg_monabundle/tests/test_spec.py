"""Tests for ``reg_monabundle.runtime.spec`` (the bundle-runtime deserializer).

Covers ``loadedspec_from_dict`` (dict -> ``LoadedSpec``), the ``LoadedSpec``
lookup surface, the step-4 runtime capability rejections, and the
``load_project_data`` sidecar path. **No structural validation runs at
this layer** (§9.6) — the build-time validation gate lives in
``reg_monabundle.build.spec_loader`` and is exercised in
``test_spec_loader.py``.
"""

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
    loadedspec_from_dict,
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
                "bindings": [
                    {"display_name": "LopNr", "type": "id", "id_subtype": "integer"},
                    {"display_name": "Kon", "type": "categorical"},
                ],
            }
        ]
    )


def test_loaded_spec_lookup_type_returns_override_with_inline_hint():
    spec = loadedspec_from_dict(_basic_payload())
    ov = spec.lookup_type("lisa_2018.csv", "LopNr")
    assert ov is not None
    assert ov.type == "id"
    assert ov.id_subtype == "integer"
    assert ov.has_inline_hint()


def test_loaded_spec_lookup_type_unknown_returns_none():
    spec = loadedspec_from_dict(_basic_payload())
    assert spec.lookup_type("lisa_2018.csv", "Missing") is None
    assert spec.lookup_type("other.csv", "LopNr") is None


def test_loaded_spec_column_types_for_source_returns_cached_mutable_dict():
    spec = loadedspec_from_dict(_basic_payload())
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
                "register_variant": "scb/test/_default",
                "bindings": [
                    {
                        "variable": "scb/test/kon",
                        "display_name": "Kon",
                        "type": "categorical",
                    },
                ],
            }
        ],
        reg_monabundle={
            "column_options": {
                "scb/test/kon": {"suppress_k": 25},
            }
        },
    )
    spec = loadedspec_from_dict(payload)
    assert spec.lookup_options("x.csv", "Kon") == {"suppress_k": 25}
    # Returns a fresh copy so callers can't mutate the underlying spec.
    spec.lookup_options("x.csv", "Kon")["suppress_k"] = 999
    assert spec.lookup_options("x.csv", "Kon") == {"suppress_k": 25}


def test_loaded_spec_lookup_options_unknown_returns_empty_dict():
    spec = loadedspec_from_dict(_basic_payload())
    assert spec.lookup_options("lisa_2018.csv", "LopNr") == {}


def test_loadedspec_from_dict_enforces_suppress_k_floor_at_load():
    # §6.8.2: the reg_monabundle namespaced-block validator (validate_block —
    # option keys + suppress_k floor) is pure-stdlib and runs at bundle LOAD
    # time on MONA too, not only at the build-time gate. loadedspec_from_dict
    # must reject a below-floor suppress_k (review P2 on #157).
    payload = make_project_data(
        sources=[
            {
                "name": "x.csv",
                "bindings": [
                    {
                        "variable": "scb/test/kon",
                        "display_name": "Kon",
                        "type": "categorical",
                    }
                ],
            }
        ],
        reg_monabundle={"column_options": {"scb/test/kon": {"suppress_k": 1}}},
    )
    with pytest.raises(ValueError, match="suppress_k"):
        loadedspec_from_dict(payload)


def test_loaded_spec_panels_passthrough():
    payload = make_project_data(
        sources=[
            {
                "name": "a.csv",
                "bindings": [
                    {"display_name": "LopNr", "type": "id", "id_subtype": "integer"}
                ],
            },
            {
                "name": "b.csv",
                "bindings": [
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
    spec = loadedspec_from_dict(payload)
    assert len(spec.panels) == 1
    p = spec.panels[0]
    assert p.panel_id == "P1"
    assert p.entity_key == "LopNr"
    assert p.members[0].source == "a.csv"
    assert p.members[0].time_key == 2018


# -- Step 4 boundary: runtime capability rejections -------------------------
#
# These are *runtime* rejections (shapes the on-MONA extract/generate
# pipeline can't execute), NOT structural validation. They fire from
# ``loadedspec_from_dict`` regardless of whether the input was
# validated, so they live here next to the deserializer.


def test_composite_entity_key_rejected_with_step_10b_message():
    payload = make_project_data(
        sources=[
            {
                "name": "a.csv",
                "bindings": [
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
        loadedspec_from_dict(payload)


def test_composite_time_key_rejected_with_step_10b_message():
    payload = make_project_data(
        sources=[
            {
                "name": "a.csv",
                "bindings": [
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
        loadedspec_from_dict(payload)


def test_literal_period_time_key_rejected_with_step_10b_message():
    payload = make_project_data(
        sources=[
            {
                "name": "a.csv",
                "bindings": [
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
        loadedspec_from_dict(payload)


def test_panel_level_time_key_rejected_with_step_10b_message():
    payload = make_project_data(
        sources=[
            {
                "name": "a.csv",
                "bindings": [
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
        loadedspec_from_dict(payload)


def test_bare_string_panel_member_without_time_key_raises_clean_error():
    # §6.4 bare-string member shorthand passes reg_schema's structural
    # validator (effective-key *presence* is reg_meta-backed, §6.8.1), so it
    # reaches _build_panel_member. The step-4 runtime must reject it with the
    # actionable "missing time_key" ValueError, not an AttributeError on a str
    # (Codex P2 on PR #155). The runtime builds from the raw JSON dict, so
    # members can still be bare strings here.
    payload = make_project_data(
        sources=[
            {
                "name": "a.csv",
                "bindings": [
                    {"display_name": "LopNr", "type": "id", "id_subtype": "integer"},
                ],
            }
        ],
        panels=[{"panel_id": "P1", "entity_key": "LopNr", "members": ["a.csv"]}],
    )
    with pytest.raises(ValueError, match="missing time_key"):
        loadedspec_from_dict(payload)


def test_member_level_entity_key_override_rejected():
    payload = make_project_data(
        sources=[
            {
                "name": "a.csv",
                "bindings": [
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
        loadedspec_from_dict(payload)


def test_display_name_required_at_load():
    payload = make_project_data(
        sources=[
            {
                "name": "a.csv",
                "bindings": [
                    {
                        "variable": "scb/test/lopnr",
                        "type": "id",
                        "id_subtype": "integer",
                    },
                ],
            }
        ],
    )
    with pytest.raises(ValueError, match="display_name"):
        loadedspec_from_dict(payload)


def test_datetime_column_type_rejected_with_actionable_message():
    """reg_schema accepts type='datetime' but mdw has no end-to-end
    datetime path (sql_emit / summarize / generate). Reject at deserialize
    with a pointer to the workaround instead of surfacing the late
    ValueError from sql_emit."""
    payload = make_project_data(
        sources=[
            {
                "name": "a.csv",
                "bindings": [
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
        loadedspec_from_dict(payload)


# -- Missing required keys: contextual ValueError, not bare KeyError --------
#
# The MONA sidecar project_data.json is a researcher hand-edit surface
# (§9.6). A missing required key in any _build_* deserializer must fail
# with an actionable, path-naming ValueError rather than a bare KeyError
# from a subscript deep in deserialization (A3.4 review P3).


def _payload_with_panel() -> dict:
    return make_project_data(
        sources=[
            {
                "name": "a.csv",
                "bindings": [
                    {"display_name": "LopNr", "type": "id", "id_subtype": "integer"}
                ],
            }
        ],
        panels=[
            {
                "panel_id": "P1",
                "entity_key": "LopNr",
                "members": [{"source": "a.csv", "time_key": 2018}],
            }
        ],
    )


def _del(payload: dict, *path) -> dict:
    """Delete a nested key (returning the payload) to forge a hand-edit typo."""
    target = payload
    for key in path[:-1]:
        target = target[key]
    del target[path[-1]]
    return payload


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (
            _del(_basic_payload(), "sources", 0, "name"),
            r"sources\[0\] is missing required key 'name'",
        ),
        (
            _del(_basic_payload(), "sources", 0, "bindings", 0, "type"),
            r"sources\['lisa_2018.csv'\].bindings\[0\] is missing required key 'type'",
        ),
        (
            _del(_basic_payload(), "sources", 0, "bindings", 0, "variable"),
            r"bindings\[0\] is missing required key 'variable'",
        ),
        (
            _del(_payload_with_panel(), "panels", 0, "panel_id"),
            r"panels\[0\] is missing required key 'panel_id'",
        ),
        (
            _del(_payload_with_panel(), "panels", 0, "members", 0, "source"),
            r"panels\['P1'\].members\[0\] is missing required key 'source'",
        ),
    ],
    ids=[
        "source_name",
        "binding_type",
        "binding_variable",
        "panel_id",
        "member_source",
    ],
)
def test_missing_required_key_raises_contextual_value_error(
    payload: dict, expected: str
):
    with pytest.raises(ValueError, match=expected):
        loadedspec_from_dict(payload)


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


def test_load_project_data_does_not_structurally_validate(tmp_path: Path):
    # §9.6: the sidecar file is trusted input on MONA — load_project_data
    # deserializes but does NOT structurally re-validate. A spec missing a
    # required top-level field (``steward``) that the build-time gate would
    # reject loads fine here (the runtime reads only sources/panels/
    # reg_monabundle). Structural validation is exercised in
    # test_spec_loader.py against validate_project_data.
    payload = _basic_payload()
    del payload["steward"]
    (tmp_path / PROJECT_DATA_FILENAME).write_text(json.dumps(payload), encoding="utf-8")
    spec = load_project_data(tmp_path)
    assert spec is not None
    assert spec.lookup_type("lisa_2018.csv", "LopNr").type == "id"


def test_load_project_data_surfaces_deserialization_errors(tmp_path: Path):
    # A binding missing its required ``variable`` key is a dataclass-
    # deserialization failure. §9.6: the runtime errors with a stdlib
    # exception on a broken embedded spec; it does not produce a structured
    # validation report. The MONA sidecar is a hand-edit surface, so the
    # missing key surfaces as a contextual ValueError naming the offending
    # path — not a bare KeyError from a subscript deep in deserialization.
    payload = {
        "schema_version": "2.0.0",
        "steward": "global",
        "reg_meta_version": "test",
        "name": "x",
        "sources": [
            {
                "name": "a.csv",
                "register_variant": "scb/test/_default",
                "period": 2018,
                "bindings": [{"display_name": "LopNr", "type": "id"}],
            }
        ],
        "panels": [],
    }
    (tmp_path / PROJECT_DATA_FILENAME).write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(
        ValueError, match=r"sources\['a.csv'\].bindings\[0\] is missing required key"
    ):
        load_project_data(tmp_path)


def test_load_project_data_rejects_non_object_root(tmp_path: Path):
    (tmp_path / PROJECT_DATA_FILENAME).write_text("[]", encoding="utf-8")
    with pytest.raises(ValueError, match="top-level value must be an object"):
        load_project_data(tmp_path)


# -- LoadedSpec direct construction --------------------------------------


def test_loaded_spec_handles_source_with_no_matching_name():
    # Spec adapter must not crash on column_types_for_source for an
    # unknown source — the auto-promotion path needs an empty dict
    # rather than a KeyError.
    spec = loadedspec_from_dict(_basic_payload())
    assert isinstance(spec, LoadedSpec)
    assert spec.column_types_for_source("unknown_source") == {}
