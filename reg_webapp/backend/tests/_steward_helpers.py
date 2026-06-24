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


def write_steward(stewards_dir: Path, steward_id: str, sources: list[dict]) -> None:
    base = stewards_dir / steward_id
    base.mkdir(parents=True)
    (base / "steward.toml").write_text(IFAU_TOML, encoding="utf-8")
    (base / "steward.project_data.json").write_text(
        json.dumps(steward_project(sources)), encoding="utf-8"
    )


def write_global(stewards_dir: Path) -> None:
    base = stewards_dir / "global"
    base.mkdir(parents=True)
    (base / "steward.toml").write_text(
        'id = "global"\nname = "Global"\nlong_name = "Full universe"\n'
        'hostname = "global.example.org"\n',
        encoding="utf-8",
    )
