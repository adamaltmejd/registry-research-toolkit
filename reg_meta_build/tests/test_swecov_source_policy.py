"""SWECOV generator: the committed source policy's validation.

`reg_webapp/stewards/swecov/source_policy.toml` is the authored routing every
generator stage reads; it is checked through `reg_meta_build.swecov_policy` before
any stage runs. Fixture provenance and the generator's boundary: `_swecov_fixtures`.
"""

from __future__ import annotations

import json
import tomllib
from typing import TYPE_CHECKING

import pytest
from _swecov_fixtures import build_catalog
from reg_meta_build.swecov_policy import SourcePolicy, SourceRoute, load_source_policy

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize(
    ("case", "message"),
    [
        ("duplicate_route", "duplicate route"),
        ("duplicate_flavor", "duplicate flavor coordinate"),
        ("unknown_section", "Extra inputs are not permitted"),
        ("unknown_field", "Extra inputs are not permitted"),
        ("unknown_status", "Input should be"),
        ("unknown_selector", "unknown or invalid route selector"),
        ("duplicate_selector", "duplicate route selector"),
        ("duplicate_tables", "flavor tables must be nonblank and unique"),
        ("duplicate_register", "duplicate flavor register"),
        ("unknown_register", "policy registers must be register FQIDs"),
        ("blank_category", "non-catalog categories and reasons must be nonblank"),
        ("unknown_target", "3-part variant coordinate"),
        ("unknown_split_target", "3-part variant coordinate"),
        ("unknown_graft", "policy registers must be register FQIDs"),
        ("unknown_scope_register", "policy registers must be register FQIDs"),
    ],
)
def test_source_policy_rejects_unchecked_or_duplicate_decisions(
    tmp_path: Path, case: str, message: str
) -> None:
    from pydantic import ValidationError

    raw = tomllib.loads(build_catalog.SOURCE_POLICY_PATH.read_text(encoding="utf-8"))
    if case == "duplicate_route":
        raw["route"].append(raw["route"][0])
    elif case == "duplicate_flavor":
        raw["flavor"].append(raw["flavor"][0])
    elif case == "unknown_section":
        raw["unknown"] = []
    elif case == "unknown_field":
        raw["route"][0]["typo"] = "unchecked"
    elif case == "unknown_status":
        raw["route"][0]["status"] = "guess"
    elif case in {"unknown_selector", "duplicate_selector"}:
        entry = next(entry for entry in raw["route"] if entry["status"] == "split")
        if case == "unknown_selector":
            entry["split"][0]["selector"] = "guess:AKU"
        else:
            entry["split"].append(entry["split"][0])
    elif case == "duplicate_tables":
        raw["flavor"][0]["tables"] = ["same", "same"]
    elif case == "duplicate_register":
        raw["flavor_registers"].append(raw["flavor_registers"][0])
    elif case == "unknown_register":
        raw["flavor_registers"] = ["scb"]
    elif case == "unknown_target":
        raw["route"][0]["target"] = "scb/agi"
    elif case == "unknown_split_target":
        entry = next(entry for entry in raw["route"] if entry["status"] == "split")
        entry["split"][0]["target"] = "scb/agi"
    elif case == "unknown_graft":
        raw["route"][0]["graft"] = "scb"
    elif case == "unknown_scope_register":
        raw["register_scope"][0]["register"] = "scb"
    else:
        raw["non_catalog_categories"]["blank"] = " "
    with pytest.raises(ValidationError, match=message):
        SourcePolicy.model_validate(raw)


def test_source_policy_loader_refuses_unknown_sections(tmp_path: Path) -> None:
    path = tmp_path / "policy.toml"
    path.write_text(build_catalog.SOURCE_POLICY_PATH.read_text() + "\n[[unknown]]\n")
    with pytest.raises(SystemExit, match="invalid SWECOV source policy"):
        load_source_policy(path)


@pytest.mark.parametrize(
    ("section", "field", "value"),
    [
        ("flavor", "provider", "../escape"),
        ("flavor", "provider", "/absolute"),
        ("provider_scope", "provider", "../escape"),
        ("provider_scope", "provider", "/absolute"),
        ("flavor", "register", "../escape"),
        ("flavor", "register", "Not a slug"),
        ("flavor", "variant_slug", "../escape"),
        ("flavor", "variant_slug", "Not a slug"),
    ],
)
def test_source_policy_rejects_unsafe_output_and_catalog_slugs(
    tmp_path: Path, section: str, field: str, value: str
) -> None:
    text = build_catalog.SOURCE_POLICY_PATH.read_text(encoding="utf-8")
    raw = tomllib.loads(text)
    old = raw[section][0][field]
    marker = f"[[{section}]]"
    start = text.index(marker)
    replacement = f"{field} = {json.dumps(value)}"
    text = text[:start] + text[start:].replace(
        f"{field} = {json.dumps(old, ensure_ascii=False)}", replacement, 1
    )
    path = tmp_path / "source_policy.toml"
    path.write_text(text, encoding="utf-8")
    with pytest.raises(SystemExit, match="invalid SWECOV source policy"):
        load_source_policy(path)


@pytest.mark.parametrize(
    "changes",
    [
        {},
        {"unmapped_reason": " "},
        {"unmapped_reason": "Reviewed", "graft": "sos/bu"},
        {"unmapped_reason": "Reviewed", "target": "sos/bu/bu-insats"},
    ],
)
def test_unmapped_source_route_requires_reason_and_no_catalog_target(
    changes: dict,
) -> None:
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        SourceRoute.model_validate(
            {
                "category": "Socialstyrelsen",
                "detail": "Barn",
                "status": "unmapped",
                **changes,
            }
        )
