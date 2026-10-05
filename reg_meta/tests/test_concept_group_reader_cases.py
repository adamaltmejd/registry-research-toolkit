"""Concept-group members observed against compiled, independently authored holdings."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from reader_artifacts import CASES, build_reader_artifact
from reg_meta.catalog import Catalog
from reg_meta.db import open_db

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize("scope", ["holdings", "reference"])
def test_group_admission_uses_case_twin_identity_and_known_holdings(
    tmp_path: Path, scope
) -> None:
    case = CASES / "reader/concept-group-admission"
    request = json.loads((case / "request.json").read_text())
    expected = json.loads((case / "expected.json").read_text())
    path = build_reader_artifact(
        tmp_path / "artifact", request["fixture"], request["artifact"]
    )
    conn = open_db(path)
    try:
        catalog = Catalog(conn, scope=scope)
        actual = {
            group.key: [member.delivery_column for member in group.members]
            for group in catalog.list_concept_groups(
                request["provider"], request["register"]
            )
        }
        assert actual == expected[scope]
    finally:
        conn.close()
