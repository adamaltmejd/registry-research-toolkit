"""Independent artifact census and deterministic multi-binding public agreement.

Real failures contain counts and contract descriptions, never private identities.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import pytest
from acceptance_requests import (
    acceptance_sample,
    admitted_bindings,
    identity_digest,
    response_fqids,
)
from artifact_requests import (
    cli_json,
    http_search_contains,
    require,
    require_search_reaches,
    sample_project,
)
from fastapi.testclient import TestClient
from reader_artifacts import (
    CASES,
    FIXTURE_IMPORT_DATE,
    build_reader_artifact,
    replicate_filler,
)
from reg_meta.cli import run
from reg_meta.db import get_manifest, open_db
from reg_meta.order import materialize_order, project_from_raw
from reg_webapp.app import create_app


def scopes(manifest):
    return (
        ("holdings", "reference")
        if manifest["catalog_artifact_kind"] == "steward"
        else ("reference",)
    )


def test_complete_admitted_set_agrees_with_http(artifact_dir, artifact_client):
    with open_db(artifact_dir / "reg_meta.db") as conn:
        manifest = get_manifest(conn)
        expected = {scope: admitted_bindings(conn, scope) for scope in scopes(manifest)}
    observations = []
    for scope, bindings in expected.items():
        held_registers = {binding.rsplit("/", 1)[0] for binding in bindings}
        registers = sorted(
            {binding.rsplit("/", 1)[0] for binding in expected["reference"]}
        )
        http_seen = set()
        for register in registers:
            response = artifact_client.get(
                "/api/catalog/" + register, params={"scope": scope}
            )
            if scope == "holdings" and register not in held_registers:
                require(
                    response.status_code == 404,
                    "Unheld register entered complete HTTP admission set",
                )
                continue
            require(response.status_code == 200, "Admitted-set HTTP register failed")
            http_seen.update(
                child["fqid"]
                for child in response.json()["children"]
                if child.get("fqid")
            )
        require(
            http_seen == bindings,
            "Complete HTTP binding set disagrees with independent artifact census",
        )
        observations.append(
            {
                "scope": scope,
                "admitted_count": len(bindings),
                "admitted_sha256": identity_digest(bindings),
            }
        )
    # These counts/digests provide a receipt without retaining artifact identities.
    print(json.dumps({"complete_http_admitted_sets": observations}, sort_keys=True))


def check_generation_seeded_stratified_binding_agreement(
    artifact_dir, artifact_client, tmp_path, capsys
):
    with open_db(artifact_dir / "reg_meta.db") as conn:
        manifest = get_manifest(conn)
        steward = manifest["catalog_artifact_kind"] == "steward"
        sample, available_strata, available_bindings = acceptance_sample(
            conn, manifest["generation_id"], steward=steward
        )
        repeated, _, _ = acceptance_sample(
            conn, manifest["generation_id"], steward=steward
        )
    require(
        sample
        and len(sample) <= min(50, available_bindings)
        and (available_bindings < 50 or len(sample) == 50),
        "Sample did not select 50 applicable bindings from a large proposal set",
    )
    require(sample == repeated, "Generation-seeded sample is not reproducible")
    ceiling_refined = 0
    for candidate in sample:
        for scope in scopes(manifest):
            browse = artifact_client.get(
                "/api/catalog/" + candidate.variable, params={"scope": scope}
            )
            require(browse.status_code == 200, "Sample missing from HTTP browse")
            require(
                browse.json()["fqid"] == candidate.variable,
                "Sample browse identity disagrees",
            )
            point = artifact_client.get(
                "/api/catalog/" + candidate.variable,
                params={
                    "scope": scope,
                    "period": candidate.period,
                    "variant": candidate.variant.rsplit("/", 1)[1],
                },
            )
            require(
                point.status_code == 200
                and any(
                    state["delivery_column_name"] == candidate.representation
                    for state in point.json()["states"]
                ),
                "Sample native representation missing from point/variant browse",
            )
            query = browse.json()["name"]
            cli_browse = cli_json(
                artifact_dir,
                capsys,
                [
                    "--scope",
                    scope,
                    "get",
                    "varinfo",
                    query,
                    "--register",
                    candidate.variable.rsplit("/", 1)[0],
                ],
            )
            require(
                candidate.variable in response_fqids(cli_browse),
                "Sample missing from CLI logical browse",
            )
            ceiling_refined += require_search_reaches(
                artifact_dir, artifact_client, capsys, query, scope, candidate.variable
            )
        project = candidate.project(manifest.get("steward", "global"))
        validated = artifact_client.post("/api/project/validate", json=project)
        require(
            validated.status_code == 200
            and validated.json()["ok"]
            and not any(
                issue["code"].endswith("outside_steward_catalog")
                for issue in validated.json()["issues"]
            ),
            "Sample validation disagrees with physical/semantic admission",
        )
        project_path = tmp_path / "acceptance-project.json"
        project_path.write_text(json.dumps(project))
        code = run(["--db", str(artifact_dir), "validate", str(project_path)])
        require(
            code == 0 and capsys.readouterr().out == validated.text,
            "Sample CLI/HTTP validation bytes disagree",
        )
        code = run(["--db", str(artifact_dir), "order", str(project_path)])
        cli_bytes = capsys.readouterr().out
        require(code == 0, "Sample CLI order disagrees with admission")
        ordered = artifact_client.post("/api/project/order", json=project)
        require(
            ordered.status_code == 200, "Sample HTTP order disagrees with admission"
        )
        require(cli_bytes == ordered.text, "Sample CLI/HTTP order bytes disagree")
        if candidate is sample[0]:
            # Repeat and materializer bytes on one candidate keep tier 3 bounded.
            require_repeatable(
                artifact_dir, artifact_client, capsys, project_path, ordered, query
            )
    receipt = {
        "sample_count": len(sample),
        "sample_sha256": identity_digest([c.identity() for c in sample]),
        "generation_id": manifest["generation_id"],
        "raw_proposal_strata": available_strata,
        "raw_proposal_bindings": available_bindings,
        "selected_strata": dict(
            Counter(stratum for c in sample for stratum in c.strata)
        ),
        "search_ceiling_refined": ceiling_refined,
    }
    print(json.dumps(receipt, sort_keys=True))
    return receipt


def require_repeatable(artifact_dir, client, capsys, project_path, ordered, query):
    """Materializer, CLI and HTTP order bytes agree and repeat; search first
    pages repeat byte for byte."""
    project = json.loads(project_path.read_text())
    with open_db(artifact_dir / "reg_meta.db") as conn:
        result = materialize_order(project_from_raw(project), conn)
        steward = get_manifest(conn)["catalog_artifact_kind"] == "steward"
    require(
        result.manifest is not None and result.manifest.to_json() == ordered.text,
        "Materializer and adapter order bytes differ",
    )
    code = run(["--db", str(artifact_dir), "order", str(project_path)])
    require(
        code == 0 and capsys.readouterr().out == ordered.text,
        "Repeated CLI order bytes differ",
    )
    require(
        client.post("/api/project/order", json=project).content == ordered.content,
        "Repeated HTTP order bytes differ",
    )
    scope = "holdings" if steward else "reference"
    params = {"q": query, "type": "variable", "limit": 100, "scope": scope}
    require(
        client.get("/api/search", params=params).content
        == client.get("/api/search", params=params).content,
        "Repeated HTTP first page differs",
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


def test_generation_seeded_stratified_binding_agreement(
    artifact_dir, artifact_client, tmp_path, capsys
):
    check_generation_seeded_stratified_binding_agreement(
        artifact_dir, artifact_client, tmp_path, capsys
    )


def test_unheld_deep_link_and_reference_search_do_not_admit_order(
    artifact_dir, artifact_client, tmp_path, capsys
):
    with open_db(artifact_dir / "reg_meta.db") as conn:
        if get_manifest(conn)["catalog_artifact_kind"] != "steward":
            pytest.skip("catalog refusals are pinned in test_artifact")
        project = sample_project(conn, unheld=True)
        refused = materialize_order(project_from_raw(project), conn)
    binding = project["sources"][0]["bindings"][0]["variable"]
    reference = artifact_client.get(
        "/api/catalog/" + binding, params={"scope": "reference"}
    )
    require(reference.status_code == 200, "Unheld reference deep link is missing")
    require_search_reaches(
        artifact_dir,
        artifact_client,
        capsys,
        reference.json()["name"],
        "reference",
        binding,
    )
    require(
        not http_search_contains(
            artifact_client, reference.json()["name"], "holdings", binding
        ),
        "Unheld binding entered holdings search",
    )
    require(
        artifact_client.get(
            "/api/catalog/" + binding, params={"scope": "holdings"}
        ).status_code
        == 404,
        "Unheld deep link admitted to holdings",
    )
    validated = artifact_client.post("/api/project/validate", json=project)
    require(
        validated.status_code == 200
        and any(
            issue["code"] == "fqid_outside_steward_catalog"
            and issue["path"] == "/sources/0/bindings/0/variable"
            for issue in validated.json()["issues"]
        ),
        "Reference validation failed to locate its holdings warning",
    )
    ordered = artifact_client.post("/api/project/order", json=project)
    require(ordered.status_code == 422, "Reference node was orderable without holdings")
    require(
        any(
            f.get("variable") == binding and f.get("source") == "Sample"
            for f in ordered.json()["findings"]
        ),
        "Unheld order refusal is not located",
    )
    require(refused.manifest is None, "Unheld reference binding was ordered")
    require(
        ordered.json()["findings"]
        == [f.model_dump(mode="json") for f in refused.findings],
        "HTTP refusal findings disagree with materializer",
    )
    path = tmp_path / "unheld-acceptance.json"
    path.write_text(json.dumps(project))
    code = run(["--db", str(artifact_dir), "--format", "json", "order", str(path)])
    captured = capsys.readouterr()
    require(code != 0, "CLI admitted unheld reference order")
    output = json.loads(captured.out)
    require(
        output["error"]["message"] == ordered.json()["detail"],
        "Unheld CLI/HTTP refusal differs",
    )


@pytest.mark.parametrize(
    "fixture",
    [
        "reader/case-twins",
        "reader/temporal-case-twins",
        "alias-window-only",
        "range",
        "list",
        "partitions",
    ],
)
def test_source_built_stratified_boundary_agreement(
    fixture, tmp_path, monkeypatch, capsys
):
    path = build_reader_artifact(
        tmp_path / "artifact",
        fixture,
        "steward",
        identity_overrides={"import_date": FIXTURE_IMPORT_DATE},
    )
    monkeypatch.setenv("REG_META_DB", str(path.parent))
    monkeypatch.setenv("REG_WEBAPP_STEWARD", "swecov")
    monkeypatch.setenv(
        "REG_WEBAPP_STEWARDS_DIR",
        str(Path(__file__).resolve().parents[1] / "reg_webapp/stewards"),
    )
    with TestClient(create_app(rate_limit_per_minute=1000)) as client:
        check_generation_seeded_stratified_binding_agreement(
            path.parent, client, tmp_path, capsys
        )


def test_binding_past_search_depth_ceiling_is_reached_by_refinement(
    tmp_path, monkeypatch, capsys
):
    # Unheld fillers sharing the sampled "Year"'s exact name, as many as the depth
    # ceiling, outrank it in reference scope: exact-name matches alone fill it.
    source = replicate_filler(CASES / "reader/search-ceiling", tmp_path / "source")
    path = build_reader_artifact(
        tmp_path / "artifact",
        source,
        "steward",
        identity_overrides={"import_date": FIXTURE_IMPORT_DATE},
    )
    monkeypatch.setenv("REG_META_DB", str(path.parent))
    monkeypatch.setenv("REG_WEBAPP_STEWARD", "swecov")
    monkeypatch.setenv(
        "REG_WEBAPP_STEWARDS_DIR",
        str(Path(__file__).resolve().parents[1] / "reg_webapp/stewards"),
    )
    with TestClient(create_app(rate_limit_per_minute=1000)) as client:
        receipt = check_generation_seeded_stratified_binding_agreement(
            path.parent, client, tmp_path, capsys
        )
    # Holdings scope sees no fillers; only the reference traversal hits the ceiling.
    assert receipt["search_ceiling_refined"] == 1
