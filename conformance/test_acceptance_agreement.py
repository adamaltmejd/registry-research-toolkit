"""Independent artifact census and deterministic multi-binding public agreement.

Real failures contain counts and contract descriptions, never private identities.
"""

from __future__ import annotations

import json
import shutil
from collections import Counter
from pathlib import Path

import pytest
from acceptance_requests import (
    acceptance_sample,
    admitted_bindings,
    identity_digest,
    response_fqids,
)
from artifact_requests import require, sample_project
from fastapi.testclient import TestClient
from reader_artifacts import CASES, FIXTURE_IMPORT_DATE, build_reader_artifact
from reg_meta.cli import run
from reg_meta.db import get_manifest, open_db
from reg_webapp.app import create_app


def cli_json(directory, capsys, arguments):
    code = run(["--db", str(directory), "--format", "json", *arguments])
    captured = capsys.readouterr()
    require(code == 0, "Acceptance CLI request failed")
    return json.loads(captured.out)


# reg_meta/DESIGN.md: search continuation has a hard 1,000-result depth ceiling and a
# researcher who reaches it must refine the query. A binding whose generic name (the
# real catalogs share "År" across dozens of registers) ranks it past that depth is,
# by contract, unreachable through that name alone.
SEARCH_DEPTH_CEILING = 1_000


def http_search_traversal(client, query, scope, binding):
    """Follow HTTP variable cursors; return (found, top-level rows consumed)."""
    params = {"q": query, "type": "variable", "limit": 100, "scope": scope}
    cursors = set()
    consumed = 0
    while True:
        response = client.get("/api/search", params=params)
        require(response.status_code == 200, "Acceptance HTTP search failed")
        groups = response.json()["groups"]
        if binding in response_fqids(groups):
            return True, consumed
        consumed += sum(len(group["results"]) for group in groups)
        group = next((group for group in groups if group["has_more"]), None)
        if group is None:
            return False, consumed
        cursor = group["next_cursor"]
        require(
            cursor and cursor not in cursors, "Acceptance HTTP search cursor stalled"
        )
        cursors.add(cursor)
        params["cursor"] = cursor


def http_search_contains(client, query, scope, binding):
    return http_search_traversal(client, query, scope, binding)[0]


def cli_search_traversal(directory, capsys, argv, binding):
    """Follow CLI cursors; return (found, rows consumed)."""
    page = cli_json(directory, capsys, argv)
    cursors = set()
    consumed = 0
    while binding not in response_fqids(page["results"]):
        consumed += len(page["results"])
        if not page["has_more"]:
            return False, consumed
        cursor = page["next_cursor"]
        require(cursor and cursor not in cursors, "Sample CLI search cursor stalled")
        cursors.add(cursor)
        page = cli_json(directory, capsys, [*argv, "--cursor", cursor])
    return True, consumed


def require_search_reaches(directory, client, capsys, query, scope, binding):
    """Require CLI and HTTP name search to reach an admitted binding.

    A traversal may miss it only after consuming the whole depth ceiling; the
    researcher's documented refinement, the reader's register-scoped search, must
    then find it. HTTP search has no register refinement, so a ceiling-bound HTTP
    miss is proven through that same refined reader search. Returns whether the
    refinement was needed.
    """
    argv = [
        "--scope",
        scope,
        "search",
        "--query",
        query,
        "--type",
        "variable",
        "--no-fold",
        "--limit",
        "100",
    ]
    cli_found, cli_consumed = cli_search_traversal(directory, capsys, argv, binding)
    require(
        cli_found or cli_consumed >= SEARCH_DEPTH_CEILING,
        "Sample missing from CLI search traversal",
    )
    http_found, http_consumed = http_search_traversal(client, query, scope, binding)
    require(
        http_found or http_consumed >= SEARCH_DEPTH_CEILING,
        "Sample missing from HTTP search traversal",
    )
    if cli_found and http_found:
        return False
    refined, _ = cli_search_traversal(
        directory, capsys, [*argv, "--register", binding.rsplit("/", 1)[0]], binding
    )
    require(
        refined,
        "Sample past the search depth ceiling missing from register-refined CLI search",
    )
    return True


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


def test_generation_seeded_stratified_binding_agreement(
    artifact_dir, artifact_client, tmp_path, capsys
):
    check_generation_seeded_stratified_binding_agreement(
        artifact_dir, artifact_client, tmp_path, capsys
    )


def test_unknown_physical_tables_cannot_supply_logical_bindings(artifact_dir):
    with open_db(artifact_dir / "reg_meta.db") as conn:
        tables = conn.execute(
            "SELECT count(*) FROM holding_table WHERE scope='unknown'"
        ).fetchone()[0]
        mappings = conn.execute("""
            SELECT count(*) FROM holding_mapping JOIN holding_column USING(column_id)
            JOIN holding_table USING(table_id) WHERE scope='unknown'
        """).fetchone()[0]
    require(
        mappings == 0, "Unknown physical tables unexpectedly claim logical bindings"
    )
    print(
        json.dumps(
            {"unknown_physical_tables": tables, "unknown_logical_bindings": mappings}
        )
    )


def test_unheld_deep_link_and_reference_search_do_not_admit_order(
    artifact_dir, artifact_client, tmp_path, capsys
):
    with open_db(artifact_dir / "reg_meta.db") as conn:
        manifest = get_manifest(conn)
        if manifest["catalog_artifact_kind"] != "steward":
            # The catalog's explicit scope refusal is a configuration boundary.
            response = artifact_client.get("/api/catalog", params={"scope": "holdings"})
            require(
                response.status_code == 422,
                "Catalog unexpectedly supports holdings scope",
            )
            return
        project = sample_project(conn, unheld=True)
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
        "year-independent",
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
    # Unheld same-token fillers, as many as the depth ceiling, outrank the sampled
    # "Year" in reference scope once identity promotion is gated off.
    case = CASES / "reader/search-ceiling"
    request = json.loads((case / "request.json").read_text())
    source = tmp_path / "source"
    shutil.copytree(case, source)
    catalog = json.loads((case / "catalog.json").read_text())
    filler = next(
        variable
        for variable in catalog
        if "/".join(
            (
                variable["register"]["provider"],
                variable["register"]["slug"],
                variable["slug"],
            )
        )
        == request["filler"]
    )
    catalog.remove(filler)
    catalog.extend(
        {
            **filler,
            "slug": f"{filler['slug']}-{index}",
            "provider_key": f"{filler['provider_key']}-{index}",
        }
        for index in range(request["copies"])
    )
    (source / "catalog.json").write_text(json.dumps(catalog))
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
