"""Artifact admission and located refusals, synthetic or tier 3.

Failure messages deliberately omit real binding and physical identifiers.
"""

from __future__ import annotations

import json

import pytest
from artifact_requests import require, sample_project
from reg_meta.cli import run
from reg_meta.db import get_manifest, open_db
from reg_meta.order import materialize_order, project_from_raw
from reg_meta_build.validate import validate_built_db


def test_sqlite_integrity(artifact_dir):
    with open_db(artifact_dir / "reg_meta.db") as conn:
        assert [r[0] for r in conn.execute("PRAGMA integrity_check")] == ["ok"]


def test_selected_artifact_passes_the_build_validator(artifact_dir, request):
    """`validate_built_db` is the one structural authority: foreign keys,
    manifest identity and the holdings accounting included."""
    if request.config.getoption("--artifact-dir") is None:
        pytest.skip("synthetic artifacts are validated when they are built")
    result = validate_built_db(artifact_dir / "reg_meta.db")
    # Counts only: the report can name private physical identifiers.
    require(result.passed, f"{len(result.failures)} build validator checks failed")


def test_unresolved_binding_refusal_is_located_and_shared(
    artifact_dir, artifact_client, tmp_path, capsys
):
    """A catalog supports global fallback, so an unresolved binding pins its
    located refusal. Steward refusals of unheld bindings are pinned by
    test_acceptance_agreement; fixture boot cases pin steward mismatch."""
    with open_db(artifact_dir / "reg_meta.db") as conn:
        if get_manifest(conn)["catalog_artifact_kind"] != "catalog":
            pytest.skip("steward refusals are pinned in test_acceptance_agreement")
        project = sample_project(conn)
        project["sources"][0]["bindings"][0]["variable"] += "-conformance-missing"
        result = materialize_order(project_from_raw(project), conn)
    binding = project["sources"][0]["bindings"][0]["variable"]
    require(result.manifest is None, "Unresolved binding was ordered")
    require(
        any(f.variable == binding and f.source == "Sample" for f in result.findings),
        "Order refusal lacks binding coordinates",
    )
    response = artifact_client.post("/api/project/order", json=project)
    require(response.status_code == 422, "HTTP refusal did not fail closed")
    require(
        response.json()["findings"]
        == [f.model_dump(mode="json") for f in result.findings],
        "HTTP refusal findings disagree with materializer",
    )
    project_file = tmp_path / "blocked.json"
    project_file.write_text(json.dumps(project))
    require(
        run(["--db", str(artifact_dir), "--format", "json", "order", str(project_file)])
        != 0,
        "CLI refused project returned success",
    )
    output = json.loads(capsys.readouterr().out)
    require(
        output["error"]["message"] == response.json()["detail"],
        "CLI/HTTP refusal messages disagree",
    )
