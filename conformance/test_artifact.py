"""Artifact admission/accounting and public adapter agreement, synthetic or tier 3.

Failure messages deliberately omit real binding and physical identifiers.
"""

from __future__ import annotations

import json
import re

from artifact_requests import assert_sampled_agreement, require, sample_project
from reg_meta.cli import run
from reg_meta.db import get_manifest, open_db
from reg_meta.order import materialize_order, project_from_raw


def test_sqlite_integrity_and_foreign_keys(artifact_dir):
    with open_db(artifact_dir / "reg_meta.db") as conn:
        assert [r[0] for r in conn.execute("PRAGMA integrity_check")] == ["ok"]
        assert len(conn.execute("PRAGMA foreign_key_check").fetchall()) == 0


def test_admission_manifest_and_existing_accounting(artifact_dir):
    with open_db(artifact_dir / "reg_meta.db") as conn:
        manifest = get_manifest(conn)
        # Successful open_db is the authoritative schema/publication admission.
        assert manifest["catalog_publishable"] == "true"
        assert manifest["catalog_completeness"] == "complete"
        assert re.fullmatch(r"[0-9a-f]{64}", manifest["generation_id"])
        kind = manifest["catalog_artifact_kind"]
        assert kind in {"catalog", "steward"}
        if kind == "catalog":
            assert conn.execute("SELECT count(*) FROM holding_table").fetchone()[0] == 0
            return
        assert manifest["steward"]
        counts = json.loads(manifest["holdings_accounting_counts"])
        for scope, category in (
            ("intervals", "dated"),
            ("year_independent", "year_independent"),
            ("unknown", "retained_unknown"),
        ):
            tables = conn.execute(
                "SELECT count(*) FROM holding_table WHERE scope=?", (scope,)
            ).fetchone()[0]
            columns = conn.execute(
                "SELECT count(*) FROM holding_column JOIN holding_table USING(table_id) WHERE scope=?",
                (scope,),
            ).fetchone()[0]
            assert counts[category] == {"tables": tables, "columns": columns}
        for metric in ("tables", "columns"):
            assert counts["raw"][metric] == sum(
                counts[category][metric]
                for category in (
                    "dated",
                    "year_independent",
                    "retained_unknown",
                    "excluded",
                    "lookup",
                )
            )


def test_sampled_browse_search_validate_order_agreement(
    artifact_dir, artifact_client, tmp_path, capsys
):
    assert_sampled_agreement(artifact_dir, artifact_client, tmp_path, capsys)


def test_reference_binding_refusal_is_located_and_shared(
    artifact_dir, artifact_client, tmp_path, capsys
):
    with open_db(artifact_dir / "reg_meta.db") as conn:
        held = get_manifest(conn)["catalog_artifact_kind"] == "steward"
        project = sample_project(conn, unheld=held)
        if not held:
            # A catalog supports global fallback. An unresolved binding pins its
            # located refusal; fixture boot cases separately pin steward mismatch.
            project["sources"][0]["bindings"][0]["variable"] += "-conformance-missing"
        result = materialize_order(project_from_raw(project), conn)
    binding = project["sources"][0]["bindings"][0]["variable"]
    if held:
        reference = artifact_client.get(
            "/api/catalog/" + binding, params={"scope": "reference"}
        )
        require(
            reference.status_code == 200, "Unheld sample missing in reference scope"
        )
        require(
            artifact_client.get("/api/catalog/" + binding).status_code == 404,
            "Unheld reference binding admitted to holdings browse",
        )
    require(result.manifest is None, "Unheld/unresolved reference binding was ordered")
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
