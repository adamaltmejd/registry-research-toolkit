"""Structural regression guard for the committed SWECOV steward catalog.

The catalog (`reg_webapp/stewards/swecov/`) is generated against a flavored
reg_meta DB (see its README). A FULL boot/admission check needs that DB and is
the maintainer's real-data validation, not a CI fixture — so this guards the
committed artifact WITHOUT a DB: it must load as a steward, pass structural
validation, and be self-consistent (well-formed FQIDs, every binding under its
source's register, a pinned representation, no duplicate bindings). A corrupt or
schema-drifted regenerate fails here.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from reg_schema.project_data import ProjectData
from reg_schema.structural import validate_structural

from reg_webapp.stewards import load_steward

_STEWARDS_DIR = Path(__file__).resolve().parents[2] / "stewards"
_SWECOV = _STEWARDS_DIR / "swecov"


def test_swecov_steward_toml_loads() -> None:
    steward = load_steward("swecov", root=_STEWARDS_DIR)
    assert steward.id == "swecov"
    assert steward.has_catalog_filter  # ships a steward.project_data.json


@pytest.fixture(scope="module")
def project() -> ProjectData:
    raw = (_SWECOV / "steward.project_data.json").read_text(encoding="utf-8")
    data = json.loads(raw)
    result = validate_structural(data)
    errors = [i for i in result.issues if i.level == "error"]
    assert not errors, f"structural errors: {[(i.code, i.path) for i in errors]}"
    return ProjectData.model_validate(data)


def test_catalog_shape(project: ProjectData) -> None:
    assert project.steward == "swecov"
    assert project.sources, "catalog must declare at least one source"
    assert not project.panels, "a steward catalog is sources-only (no panels)"


def test_bindings_are_self_consistent(project: ProjectData) -> None:
    seen: set[tuple[str, str, str | None]] = set()
    for source in project.sources:
        coord = source.register_variant.split("/")
        assert len(coord) == 3, (
            f"register_variant not 3-part: {source.register_variant}"
        )
        reg_prefix = "/".join(coord[:2])
        assert source.bindings, f"source {source.name} has no bindings"
        for binding in source.bindings:
            fqid = binding.variable.split("/")
            assert len(fqid) == 3, f"binding FQID not 3-part: {binding.variable}"
            # A binding lives under its source's register (#206 admission coord).
            assert "/".join(fqid[:2]) == reg_prefix, (
                f"binding {binding.variable} outside register {reg_prefix}"
            )
            # The generator pins the resolved delivery column on every binding —
            # the load-bearing property that keeps `_default` resolution
            # unambiguous (no co-existing-column drift).
            assert binding.representation, (
                f"binding {binding.variable} missing representation"
            )
            key = (source.register_variant, binding.variable, binding.representation)
            assert key not in seen, f"duplicate binding {key}"
            seen.add(key)


def test_issue_428_sun_grouping_columns_are_flavor_bindings(
    project: ProjectData,
) -> None:
    bindings = {
        (source.register_variant, binding.variable, binding.representation)
        for source in project.sources
        for binding in source.bindings
    }

    assert {
        (
            "swecov/hreg-sun-groupings/_default",
            "swecov/hreg-sun-groupings/sun2000inr3-amnegrupp",
            "SUN2000Inr3_amnegrupp",
        ),
        (
            "swecov/hreg-sun-groupings/_default",
            "swecov/hreg-sun-groupings/sun2020inr3-amnegrupp",
            "SUN2020Inr3_amnegrupp",
        ),
        (
            "swecov/hreg-sun-groupings/_default",
            "swecov/hreg-sun-groupings/sun2000inr3-lprog",
            "SUN2000Inr3_lprog",
        ),
        (
            "swecov/hreg-sun-groupings/_default",
            "swecov/hreg-sun-groupings/sun2020inr3-lprog",
            "SUN2020Inr3_lprog",
        ),
        (
            "swecov/hreg-sun-groupings/_default",
            "swecov/hreg-sun-groupings/sun2000niva-lprog",
            "SUN2000Niva_lprog",
        ),
        (
            "swecov/hreg-sun-groupings/_default",
            "swecov/hreg-sun-groupings/sun2020niva-lprog",
            "SUN2020Niva_lprog",
        ),
    } <= bindings
