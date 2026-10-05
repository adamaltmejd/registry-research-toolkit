"""CLI JSON and order bytes observed against independently authored case data."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from reader_artifacts import CASES, build_reader_artifact
from reg_meta.cli import run
from reg_meta.db import open_db
from reg_meta.order import materialize_order, project_from_raw

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize(
    "case", sorted((CASES / "order").iterdir()), ids=lambda p: p.name
)
def test_order_manifest_or_located_finding(case: Path, tmp_path: Path, capsys) -> None:
    request = json.loads((case / "request.json").read_text())
    expected = json.loads((case / "expected.json").read_text())
    path = build_reader_artifact(
        tmp_path / "artifact", request["fixture"], request["artifact"]
    )
    conn = open_db(path)
    try:
        result = materialize_order(project_from_raw(request["project"]), conn)
        model = result.manifest or result
        actual = model.model_dump(mode="json", exclude_none=True)
        assert {field: actual[field] for field in request["observe"]} == expected
        if result.manifest is not None:
            project = tmp_path / "project.json"
            project.write_text(json.dumps(request["project"]))
            assert run(["--db", str(path.parent), "order", str(project)]) == 0
            assert capsys.readouterr().out == result.manifest.to_json()
            again = materialize_order(project_from_raw(request["project"]), conn)
            assert again.manifest is not None
            assert again.manifest.to_json() == result.manifest.to_json()
    finally:
        conn.close()
