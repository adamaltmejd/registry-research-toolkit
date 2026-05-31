"""Tests for ``reg_monabundle.build.spec_loader`` (the build-time gate).

``spec_loader`` is the Pydantic side of the §9.6 boundary — it runs the
full ``reg_schema`` structural validator, the ``reg_monabundle``
namespaced-block validator, and the cross-block referential checks, then
converts a validated Pydantic ``ProjectData`` into the stdlib
``LoadedSpec`` the bundle runtime consumes.

The structural / block / column_options validation tests live here (not
in ``test_spec.py``) because post-A3.4 the bundle runtime
(``loadedspec_from_dict``) does NOT validate — it trusts already-validated
input. ``validate_project_data`` is the only place those checks run.
"""

from __future__ import annotations

import pytest
from _project_data_fixtures import make_project_data
from reg_monabundle.build.spec_loader import (
    project_data_to_loadedspec,
    validate_project_data,
)
from reg_monabundle.runtime.spec import LoadedSpec, loadedspec_from_dict

# -- structural validation gate -------------------------------------------


def test_validate_project_data_surfaces_structural_validation_errors():
    # Drop a required top-level field — the structural validator raises.
    payload = make_project_data(
        sources=[
            {
                "name": "lisa_2018.csv",
                "bindings": [
                    {"display_name": "LopNr", "type": "id", "id_subtype": "integer"},
                ],
            }
        ]
    )
    del payload["steward"]
    with pytest.raises(ValueError, match="structural validation"):
        validate_project_data(payload)


def test_validate_project_data_accepts_valid_spec():
    payload = make_project_data(
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
    pd = validate_project_data(payload)
    assert [s.name for s in pd.sources] == ["lisa_2018.csv"]


# -- reg_monabundle namespaced block --------------------------------------
#
# Per-rule validator coverage lives in
# reg_monabundle/tests/test_validate_block.py alongside the validator
# (§15 step 5 phase 1 — owner-validates-its-block). The tests here
# exercise the namespaced-block validator routing + the cross-block
# referential checks (``_validate_column_options_against_columns``) that
# need the FQID-typed bindings — both run at the build-time gate.


def test_validate_project_data_invokes_namespaced_block_validator():
    """Smoke test that ``validate_project_data`` routes the
    ``reg_monabundle`` block through ``reg_monabundle.validate_block``.
    One representative failure mode is enough — the validator's own test
    suite owns the per-rule coverage."""
    payload = make_project_data(
        sources=[
            {
                "name": "x.csv",
                "register_variant": "scb/test/_default",
                "bindings": [
                    {
                        "variable": "scb/test/lopnr",
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
        validate_project_data(payload)


def test_column_options_rejects_orphan_fqid_not_matching_any_column():
    """A well-formed FQID that doesn't match any binding.variable in
    sources silently no-ops at lookup time without this check. Pin the
    referential-integrity guard so a typo surfaces at the build gate."""
    payload = make_project_data(
        sources=[
            {
                "name": "x.csv",
                "register_variant": "scb/test/_default",
                "bindings": [
                    {
                        "variable": "scb/test/lopnr",
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
                "scb/test/typo_here": {"suppress_k": 25},
            }
        },
    )
    with pytest.raises(ValueError, match="don't match any binding FQID"):
        validate_project_data(payload)


def test_column_options_accepts_matching_fqid():
    """Sanity: the referential check doesn't reject a key that does
    match a declared column."""
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
    pd = validate_project_data(payload)
    spec = project_data_to_loadedspec(pd)
    assert spec.lookup_options("x.csv", "Kon") == {"suppress_k": 25}


def test_suppress_k_rejected_when_fqid_non_categorical_in_any_source():
    # Model A shares the 3-seg binding FQID across period-sources. If the same
    # FQID is bound non-categorical in ANY source, suppress_k is a no-op there
    # and must be rejected even when a sibling source binds it categorical
    # (Codex P2 #155 — the per-FQID dict used to keep only the last binding,
    # masking the numeric occurrence and passing validation).
    payload = make_project_data(
        sources=[
            {
                "name": "y2018.csv",
                "register_variant": "scb/test/_default",
                "period": 2018,
                "bindings": [
                    {
                        "variable": "scb/test/foo",
                        "display_name": "Foo",
                        "type": "numeric",
                        "numeric_subtype": "integer",
                    }
                ],
            },
            {
                "name": "y2019.csv",
                "register_variant": "scb/test/_default",
                "period": 2019,
                "bindings": [
                    {
                        "variable": "scb/test/foo",
                        "display_name": "Foo",
                        "type": "categorical",
                    }
                ],
            },
        ],
        reg_monabundle={"column_options": {"scb/test/foo": {"suppress_k": 25}}},
    )
    with pytest.raises(ValueError, match="only honored on categorical"):
        validate_project_data(payload)


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
    so accepting it there is a silent no-op. Reject at the build gate and
    point at the future panels[*].suppress_k for panel-level k."""
    fqid = f"scb/test/{suffix}"
    payload = make_project_data(
        sources=[
            {
                "name": "x.csv",
                "register_variant": "scb/test/_default",
                "bindings": [
                    {"variable": fqid, "display_name": suffix.upper(), **col},
                ],
            }
        ],
        reg_monabundle={"column_options": {fqid: {"suppress_k": 25}}},
    )
    with pytest.raises(ValueError, match="only honored on categorical"):
        validate_project_data(payload)


# -- conversion boundary (§9.6) -------------------------------------------


def test_project_data_to_loadedspec_round_trips_with_loadedspec_from_dict():
    """A validated Pydantic ProjectData converts to a LoadedSpec whose
    lookup surface matches deserializing the same dict directly. This is
    the §9.6 conversion boundary — the build-time Pydantic side and the
    runtime dataclass side must agree."""
    payload = make_project_data(
        sources=[
            {
                "name": "x.csv",
                "register_variant": "scb/test/_default",
                "bindings": [
                    {
                        "variable": "scb/test/lopnr",
                        "display_name": "LopNr",
                        "type": "id",
                        "id_subtype": "integer",
                    },
                    {
                        "variable": "scb/test/kon",
                        "display_name": "Kon",
                        "type": "categorical",
                    },
                ],
            }
        ],
        reg_monabundle={"column_options": {"scb/test/kon": {"suppress_k": 25}}},
    )
    pd = validate_project_data(payload)
    converted = project_data_to_loadedspec(pd)
    direct = loadedspec_from_dict(payload)

    assert isinstance(converted, LoadedSpec)
    # lookup_type round-trips identically.
    for col in ("LopNr", "Kon"):
        c_ov = converted.lookup_type("x.csv", col)
        d_ov = direct.lookup_type("x.csv", col)
        assert c_ov is not None
        assert d_ov is not None
        assert c_ov == d_ov
    # lookup_options resolves through the binding FQID identically.
    assert (
        converted.lookup_options("x.csv", "Kon")
        == direct.lookup_options("x.csv", "Kon")
        == {"suppress_k": 25}
    )


def test_project_data_to_loadedspec_round_trips_panels():
    """Panels (with int + str time_keys) survive the Pydantic->dict->dataclass
    round-trip — the discriminated time-key wrappers and the ``period``
    ``from`` alias must dump back to a shape ``loadedspec_from_dict`` accepts.
    """
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
            },
            {
                "name": "b.csv",
                "bindings": [
                    {"display_name": "LopNr", "type": "id", "id_subtype": "integer"},
                ],
            },
        ],
        panels=[
            {
                "panel_id": "P1",
                "entity_key": "LopNr",
                "members": [
                    {"source": "a.csv", "time_key": "Ar"},
                    {"source": "b.csv", "time_key": 2019},
                ],
            }
        ],
    )
    pd = validate_project_data(payload)
    spec = project_data_to_loadedspec(pd)
    assert len(spec.panels) == 1
    p = spec.panels[0]
    assert p.panel_id == "P1"
    assert p.entity_key == "LopNr"
    assert {m.source: m.time_key for m in p.members} == {"a.csv": "Ar", "b.csv": 2019}
