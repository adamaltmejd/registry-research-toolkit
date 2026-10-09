"""Independent artifact census and deterministic multi-binding public agreement.

Real failures contain counts and contract descriptions, never private identities.
"""

from __future__ import annotations

import json
from collections import Counter

import pytest
from acceptance_requests import (
    acceptance_sample,
    admitted_bindings,
    identity_digest,
)
from artifact_requests import (
    MAX_QUERY_CHARS,
    http_search_contains,
    require,
    require_query_refused,
    require_search_reaches,
    sample_project,
    server_client,
)
from reader_artifacts import (
    CASES,
    FIXTURE_IMPORT_DATE,
    build_reader_artifact,
    replicate_filler,
)
from reg_meta_build.db import get_manifest, open_db


def scopes(manifest):
    return (
        ("holdings", "reference")
        if manifest["catalog_artifact_kind"] == "steward"
        else ("reference",)
    )


def browse_states(server, fqid, params):
    """Every state of `fqid` the Rust server's `states` serves for `params`."""
    params = {**params, "limit": 200}
    cursors = set()
    items = []
    while True:
        response = server.get("/api/states/" + fqid, params=params)
        require(response.status_code == 200, "Acceptance HTTP states failed")
        page = response.json()["data"]
        items.extend(page["items"])
        cursor = page["next_cursor"]
        if cursor is None:
            return items
        require(cursor not in cursors, "Acceptance HTTP states cursor stalled")
        cursors.add(cursor)
        params["cursor"] = cursor


def require_not_found(response, message):
    """A ref outside the read scope is the Rust server's located `not_found`."""
    require(
        response.status_code == 404 and response.json()["error"]["code"] == "not_found",
        message,
    )


def test_complete_admitted_set_agrees_with_http(artifact_dir, request):
    server = server_client(request, artifact_dir)
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
            response = server.get("/api/catalog/" + register, params={"scope": scope})
            if scope == "holdings" and register not in held_registers:
                require_not_found(
                    response, "Unheld register entered complete HTTP admission set"
                )
                continue
            require(response.status_code == 200, "Admitted-set HTTP register failed")
            http_seen.update(
                child["fqid"] for child in response.json()["data"]["children"]
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


def check_generation_seeded_stratified_binding_agreement(artifact_dir, server):
    """`server` is the Rust server's client (`server_client`)."""

    def resolve(variable, period, variant):
        states = browse_states(
            server,
            variable,
            {"scope": "reference", "period": period, "variant": variant},
        )
        return [state["delivery_column_name"] for state in states]

    with open_db(artifact_dir / "reg_meta.db") as conn:
        manifest = get_manifest(conn)
        steward = manifest["catalog_artifact_kind"] == "steward"
        sample, available_strata, available_bindings = acceptance_sample(
            conn, manifest["generation_id"], steward=steward, resolve=resolve
        )
        repeated, _, _ = acceptance_sample(
            conn, manifest["generation_id"], steward=steward, resolve=resolve
        )
    require(
        sample
        and len(sample) <= min(50, available_bindings)
        and (available_bindings < 50 or len(sample) == 50),
        "Sample did not select 50 applicable bindings from a large proposal set",
    )
    require(sample == repeated, "Generation-seeded sample is not reproducible")
    ceiling_refined = 0
    query_cap_refused = 0
    for candidate in sample:
        for scope in scopes(manifest):
            browse = server.get(
                "/api/catalog/" + candidate.variable, params={"scope": scope}
            )
            require(browse.status_code == 200, "Sample missing from HTTP browse")
            require(
                browse.json()["data"]["fqid"] == candidate.variable,
                "Sample browse identity disagrees",
            )
            point = browse_states(
                server,
                candidate.variable,
                {
                    "scope": scope,
                    "period": candidate.period,
                    "variant": candidate.variant.rsplit("/", 1)[1],
                },
            )
            require(
                any(
                    state["delivery_column_name"] == candidate.representation
                    for state in point
                ),
                "Sample native representation missing from point/variant states",
            )
            query = browse.json()["data"]["name"]
            ceiling_refined += require_search_reaches(
                server, query, scope, candidate.variable
            )
            query_cap_refused += len(query) > MAX_QUERY_CHARS
        project = candidate.project(manifest.get("steward", "global"))
        validated = server.post("/api/project/validate", json=project)
        require(
            validated.status_code == 200
            and validated.json()["data"]["ok"]
            and not any(
                issue["code"].endswith("outside_steward_catalog")
                for issue in validated.json()["data"]["issues"]
            ),
            "Sample validation disagrees with physical/semantic admission",
        )
        ordered = server.post("/api/project/order/manifest", json=project)
        require(
            ordered.status_code == 200, "Sample HTTP order disagrees with admission"
        )
        if candidate is sample[0]:
            # Repeats on one candidate keep tier 3 bounded.
            require_repeatable(server, project, ordered, query, steward=steward)
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
        "query_cap_refused": query_cap_refused,
    }
    print(json.dumps(receipt, sort_keys=True))
    return receipt


def require_repeatable(server, project, ordered, query, *, steward):
    """`order`'s data is its download's document, and the download bytes and the
    search first page repeat byte for byte."""
    answer = server.post("/api/project/order", json=project)
    require(
        answer.status_code == 200
        and answer.json()["data"] == json.loads(ordered.content),
        "Order data and its download disagree",
    )
    require(
        server.post("/api/project/order/manifest", json=project).content
        == ordered.content,
        "Repeated HTTP order bytes differ",
    )
    scope = "holdings" if steward else "reference"
    params = {"q": query, "type": "variable", "limit": 100, "scope": scope}
    require(
        server.get("/api/search", params=params).content
        == server.get("/api/search", params=params).content,
        "Repeated HTTP first page differs",
    )


def test_generation_seeded_stratified_binding_agreement(artifact_dir, request):
    check_generation_seeded_stratified_binding_agreement(
        artifact_dir, server_client(request, artifact_dir)
    )


def test_unheld_deep_link_and_reference_search_do_not_admit_order(
    artifact_dir, request
):
    with open_db(artifact_dir / "reg_meta.db") as conn:
        if get_manifest(conn)["catalog_artifact_kind"] != "steward":
            pytest.skip("catalog refusals are pinned in test_artifact")
        project = sample_project(conn, unheld=True)
    server = server_client(request, artifact_dir)
    binding = project["sources"][0]["bindings"][0]["variable"]
    reference = server.get("/api/catalog/" + binding, params={"scope": "reference"})
    require(reference.status_code == 200, "Unheld reference deep link is missing")
    name = reference.json()["data"]["name"]
    require_search_reaches(server, name, "reference", binding)
    if len(name) > MAX_QUERY_CHARS:
        # The name is out of contract for HTTP search (search has no FQID arm);
        # prove holdings exclusion by its in-cap prefix, which reference search
        # must reach for the holdings miss to count.
        require_query_refused(server, name, "holdings")
        name = name[:MAX_QUERY_CHARS]
        require(
            http_search_contains(server, name, "reference", binding),
            "Unheld binding missing from reference search by name prefix",
        )
    require(
        not http_search_contains(server, name, "holdings", binding),
        "Unheld binding entered holdings search",
    )
    require_not_found(
        server.get("/api/catalog/" + binding, params={"scope": "holdings"}),
        "Unheld deep link admitted to holdings",
    )
    validated = server.post("/api/project/validate", json=project)
    require(
        validated.status_code == 200
        and any(
            issue["code"] == "fqid_outside_steward_catalog"
            and issue["path"] == "/sources/0/bindings/0/variable"
            for issue in validated.json()["data"]["issues"]
        ),
        "Reference validation failed to locate its holdings warning",
    )
    ordered = server.post("/api/project/order", json=project)
    require(ordered.status_code == 422, "Reference node was orderable without holdings")
    blocked = ordered.json()["error"]
    require(
        blocked["code"] == "order_blocked"
        and any(
            f.get("variable") == binding and f.get("source") == "Sample"
            for f in blocked["fields"]["findings"]
        ),
        "Unheld order refusal is not located",
    )
    download = server.post("/api/project/order/manifest", json=project)
    require(
        (download.status_code, download.json()) == (422, ordered.json()),
        "Unheld download refusal differs from the order refusal",
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
def test_source_built_stratified_boundary_agreement(fixture, tmp_path, request):
    path = build_reader_artifact(
        tmp_path / "artifact",
        fixture,
        "steward",
        identity_overrides={"import_date": FIXTURE_IMPORT_DATE},
    )
    check_generation_seeded_stratified_binding_agreement(
        path.parent, server_client(request, path.parent)
    )


def test_binding_past_search_depth_ceiling_is_reached_by_refinement(tmp_path, request):
    # Unheld fillers sharing the sampled "Year"'s exact name, as many as the depth
    # ceiling, outrank it in reference scope: exact-name matches alone fill it.
    source = replicate_filler(CASES / "reader/search-ceiling", tmp_path / "source")
    path = build_reader_artifact(
        tmp_path / "artifact",
        source,
        "steward",
        identity_overrides={"import_date": FIXTURE_IMPORT_DATE},
    )
    receipt = check_generation_seeded_stratified_binding_agreement(
        path.parent, server_client(request, path.parent)
    )
    # Holdings scope sees no fillers; only the reference traversal hits the ceiling.
    assert receipt["search_ceiling_refined"] == 1


def test_binding_named_past_query_cap_is_refused_by_http_search(tmp_path, request):
    # The sampled variable's name is longer than the 200-character `q` cap: HTTP
    # search must refuse it on `q` in both scopes, while browse still reaches it.
    # Fails if the Rust server accepts an over-cap `q` or stops locating the refusal.
    path = build_reader_artifact(
        tmp_path / "artifact",
        "reader/search-query-cap",
        "steward",
        identity_overrides={"import_date": FIXTURE_IMPORT_DATE},
    )
    receipt = check_generation_seeded_stratified_binding_agreement(
        path.parent, server_client(request, path.parent)
    )
    assert receipt["query_cap_refused"] == 2  # reference and holdings
