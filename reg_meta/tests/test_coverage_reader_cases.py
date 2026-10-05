"""Compiled coverage preserves resolver projections, gaps and unioned partitions."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from reader_artifacts import CASES, build_reader_artifact
from reg_meta.catalog import Catalog
from reg_meta.db import open_db

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize(
    "case", sorted((CASES / "coverage").iterdir()), ids=lambda p: p.name
)
def test_compiled_coverage_projection(case: Path, tmp_path: Path) -> None:
    request = json.loads((case / "request.json").read_text())
    expected = json.loads((case / "expected.json").read_text())
    path = build_reader_artifact(
        tmp_path / "artifact", request["fixture"], request["artifact"]
    )
    with open_db(path) as conn:
        catalog = Catalog(conn)
        actual = {
            "deliveries": {
                variable: [delivery.model_dump(mode="json") for delivery in offered]
                for variable, offered in catalog.register_variable_deliveries(
                    request["provider"], request["register"]
                ).items()
            },
            "provider_columns": [
                {
                    "register": register,
                    "variable": variable,
                    "column": column,
                    "coverage": coverage.model_dump(mode="json"),
                }
                for register, columns in sorted(
                    catalog.provider_column_coverage(request["provider"]).items()
                )
                for (variable, column), coverage in sorted(columns.items())
            ],
            "provider_registers": {
                register: coverage.model_dump(mode="json")
                for register, coverage in catalog.provider_register_coverage(
                    request["provider"]
                ).items()
            },
        }
    assert actual == expected
