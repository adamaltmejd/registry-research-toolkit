"""Shared filtered-steward test data and writers."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

IFAU_TOML = """\
id = "ifau"
name = "IFAU"
long_name = "Institute for Evaluation of Labour Market and Education Policy"
hostname = "ifau.example.org"
"""


def steward_project(sources: list[dict]) -> dict:
    return {
        "schema_version": "2.0.0",
        "steward": "ifau",
        "reg_meta_version": "5.1.0",
        "name": "ifau-catalog",
        "sources": sources,
    }


# The fixture DB resolves scb/lisa/individer-15plus (binding scb/lisa/kon, state
# 2018+) and scb/rams/standard (binding scb/rams/syss).
CLEAN_SOURCES = [
    {
        "name": "lisa",
        "register_variant": "scb/lisa/individer-15plus",
        "period": 2018,
        "bindings": [
            {
                "variable": "scb/lisa/kon",
                "type": "categorical",
                "value_set": "class/sun2020",
            }
        ],
    },
    {
        "name": "rams",
        "register_variant": "scb/rams/standard",
        "period": 2019,
        "bindings": [{"variable": "scb/rams/syss", "type": "numeric"}],
    },
]


# A named steward must ship a delivery inventory or the deployment refuses to
# boot (stewards.load_delivery_inventory), so a written test steward is only a
# COMPLETE deployment with one. Minimal but valid: reg_meta's loader is DB-free,
# so this need only be well-formed — it covers CLEAN_SOURCES' two holdings.
IFAU_INVENTORY = """\
version = 1
steward = "ifau"

[[table]]
id = "LISA_Individ_2018.csv"
edition = 2018

[[table.column]]
name = "Kon"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/kon"

[[table]]
id = "RAMS_2019.csv"
edition = 2019

[[table.column]]
name = "Syss"
[[table.column.mapping]]
register_variant = "scb/rams/standard"
variable = "scb/rams/syss"
"""


def write_steward(
    stewards_dir: Path,
    steward_id: str,
    sources: list[dict],
    *,
    inventory: bool = True,
) -> None:
    """Write a complete named-steward deployment directory.

    ``inventory=False`` writes the INCOMPLETE one — the deployment that must
    fail at boot (see ``test_project_order`` → the named-steward boot guard).
    """
    base = stewards_dir / steward_id
    base.mkdir(parents=True)
    (base / "steward.toml").write_text(IFAU_TOML, encoding="utf-8")
    (base / "steward.project_data.json").write_text(
        json.dumps(steward_project(sources)), encoding="utf-8"
    )
    if inventory:
        (base / "inventory.toml").write_text(IFAU_INVENTORY, encoding="utf-8")


def write_global(stewards_dir: Path) -> None:
    base = stewards_dir / "global"
    base.mkdir(parents=True)
    (base / "steward.toml").write_text(
        'id = "global"\nname = "Global"\nlong_name = "Full universe"\n'
        'hostname = "global.example.org"\n',
        encoding="utf-8",
    )
