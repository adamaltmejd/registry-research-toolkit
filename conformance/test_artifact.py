"""Artifact admission and located refusals, synthetic or tier 3.

Failure messages deliberately omit real binding and physical identifiers.
"""

from __future__ import annotations

import pytest
from artifact_requests import require, sample_project, server_client
from reg_meta_build.db import get_manifest, open_db
from reg_meta_build.validate import validate_built_db


def test_sqlite_integrity(artifact_dir):
    with open_db(artifact_dir / "reg_meta.db") as conn:
        assert [r[0] for r in conn.execute("PRAGMA integrity_check")] == ["ok"]


def test_selected_artifact_passes_the_build_validator(artifact_dir, request):
    """`validate_built_db` is the one structural authority: foreign keys,
    manifest identity and the holdings accounting included. It runs in the
    mode each published asset is built in: a catalog with the real-corpus
    floors, a steward artifact with the minted-id band for steward providers.
    The curation `slug_dir` is build input that no asset ships, so the
    entity-key curation gate stays with the build."""
    if request.config.getoption("--artifact-dir") is None:
        pytest.skip("synthetic artifacts are validated when they are built")
    db = artifact_dir / "reg_meta.db"
    with open_db(db) as conn:
        kind = get_manifest(conn)["catalog_artifact_kind"]
    result = validate_built_db(db, corpus=kind == "catalog", flavored=kind == "steward")
    # Counts only: the report can name private physical identifiers.
    require(result.passed, f"{len(result.failures)} build validator checks failed")


def test_unresolved_binding_refusal_is_located(artifact_dir, request):
    """A catalog supports global fallback, so an unresolved binding pins its
    located refusal, on the Rust server (`--server-cmd`). Steward refusals of
    unheld bindings are pinned by test_acceptance_agreement; the `api` startup
    cases pin steward mismatch.

    Fails when `order` or its download orders the project, refuses it as anything
    but `order_blocked`, or stops locating a `variable_unresolved` finding at the
    binding's source and variable (`api/order-errors` pins the code)."""
    with open_db(artifact_dir / "reg_meta.db") as conn:
        if get_manifest(conn)["catalog_artifact_kind"] != "catalog":
            pytest.skip("steward refusals are pinned in test_acceptance_agreement")
        project = sample_project(conn)
    project["sources"][0]["bindings"][0]["variable"] += "-conformance-missing"
    client = server_client(request, artifact_dir)
    binding = project["sources"][0]["bindings"][0]["variable"]
    response = client.post("/api/project/order", json=project)
    require(response.status_code == 422, "HTTP refusal did not fail closed")
    error = response.json()["error"]
    require(error["code"] == "order_blocked", "Refusal is not order_blocked")
    require(
        any(
            (f["code"], f["source"], f["variable"])
            == ("variable_unresolved", "Sample", binding)
            for f in error["fields"]["findings"]
        ),
        "Order refusal lacks a located variable_unresolved finding",
    )
    download = client.post("/api/project/order/manifest", json=project)
    require(
        (download.status_code, download.json()) == (422, response.json()),
        "Download refusal differs from the order refusal",
    )
