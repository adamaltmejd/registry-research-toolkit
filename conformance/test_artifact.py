"""Artifact admission/accounting and public adapter agreement, synthetic or tier 3.

Failure messages deliberately omit real binding and physical identifiers.
"""

from __future__ import annotations

import json
import re

from artifact_requests import sample_project
from normalization import order_bytes
from reg_meta.cli import run
from reg_meta.db import get_manifest, open_db
from reg_meta.order import materialize_order, project_from_raw


def require(condition, message):
    if not condition:
        raise AssertionError(message)


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
    with open_db(artifact_dir / "reg_meta.db") as conn:
        project = sample_project(conn)
        result = materialize_order(project_from_raw(project), conn)
    require(result.manifest is not None, "Sampled admitted binding did not materialize")
    fqid = project["sources"][0]["bindings"][0]["variable"]
    scope = "reference" if project["steward"] == "global" else "holdings"
    browse = artifact_client.get("/api/catalog/" + fqid, params={"scope": scope})
    require(browse.status_code == 200, "Sampled binding missing from browse")
    require(browse.json()["fqid"] == fqid, "Browse identity disagrees with sample")
    query = browse.json()["name"]
    params = {"q": query, "type": "variable", "limit": 100, "scope": scope}
    search = artifact_client.get("/api/search", params=params)
    require(search.status_code == 200, "Sample search failed")
    require(
        search.content == artifact_client.get("/api/search", params=params).content,
        "Repeated HTTP first page differs",
    )
    hits = [hit for group in search.json()["groups"] for hit in group["results"]]
    require(
        any(hit.get("fqid") == fqid for hit in hits),
        "Sample missing from search first page",
    )
    argv = [
        "--db",
        str(artifact_dir),
        "--format",
        "json",
        "search",
        "--query",
        query,
        "--type",
        "variable",
        "--no-fold",
        "--limit",
        "100",
        "--scope",
        scope,
    ]
    require(run(argv) == 0, "CLI sample search failed")
    first = capsys.readouterr().out
    require(run(argv) == 0, "Repeated CLI sample search failed")
    require(capsys.readouterr().out == first, "Repeated CLI first page differs")
    require(
        any(hit.get("fqid") == fqid for hit in json.loads(first)["results"]),
        "CLI search identity disagrees",
    )
    validated = artifact_client.post("/api/project/validate", json=project)
    require(
        validated.status_code == 200 and validated.json()["ok"],
        "Sample validation disagrees with admission",
    )
    project_file = tmp_path / "project.json"
    project_file.write_text(json.dumps(project))
    require(
        run(["--db", str(artifact_dir), "--format", "json", "order", str(project_file)])
        == 0,
        "CLI sample order failed",
    )
    cli = capsys.readouterr().out
    response = artifact_client.post("/api/project/order", json=project)
    require(response.status_code == 200, "HTTP sample order failed")
    require(
        order_bytes(cli) == order_bytes(response.text),
        "Normalized CLI/HTTP order bytes differ",
    )
    require(
        cli == result.manifest.to_json() == response.text,
        "Raw adapter serialization differs",
    )
    require(
        run(["--db", str(artifact_dir), "order", str(project_file)]) == 0,
        "Repeated CLI order failed",
    )
    require(capsys.readouterr().out == cli, "Repeated CLI order bytes differ")
    require(
        artifact_client.post("/api/project/order", json=project).content
        == response.content,
        "Repeated HTTP order bytes differ",
    )


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
