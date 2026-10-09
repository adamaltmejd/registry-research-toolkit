"""Public requests and adapter agreement; never retain real identifiers as files."""

from __future__ import annotations

import json
import unicodedata

import pytest
from acceptance_requests import response_fqids
from http_cases import CASES
from reg_meta_build.db import get_manifest, open_db


def sample_project(conn, *, unheld=False):
    """First addressable state with an applicable physical claim, in stable FQID order."""
    manifest = get_manifest(conn)
    steward = manifest.get("steward", "global")
    held = manifest["catalog_artifact_kind"] == "steward"
    fields = """
        SELECT p.slug provider, r.slug register_slug, v.slug variable,
               rv.slug variant, vs.valid_from, vs.valid_to, vs.period_scope,
               vs.delivery_column_name representation
        FROM variable_state vs
        JOIN variable v USING(variable_id)
        JOIN register r ON r.register_id=v.register_id
        JOIN provider p USING(provider_id)
        JOIN register_variant rv ON rv.register_variant_id=vs.register_variant_id
    """
    if held and not unheld:
        fields = fields.replace(
            "vs.delivery_column_name representation",
            "vs.delivery_column_name representation, hp.lo, hp.hi",
        )
        fields += """
            JOIN holding_mapping hm ON hm.variable_id=vs.variable_id
                AND hm.variant_id=vs.register_variant_id
                AND py_lower(hm.representation_canonical)=py_lower(vs.delivery_column_name)
            JOIN holding_column hc USING(column_id)
            JOIN holding_table ht USING(table_id)
            LEFT JOIN holding_period hp USING(table_id)
            WHERE ht.scope = vs.period_scope
              AND (ht.scope='year_independent'
                   OR (hp.lo<=vs.valid_to AND hp.hi>=vs.valid_from))
        """
    elif unheld:
        fields += """
            WHERE NOT EXISTS (
                SELECT 1 FROM holding_mapping hm
                JOIN holding_column hc USING(column_id)
                JOIN holding_table ht USING(table_id)
                WHERE hm.variable_id=v.variable_id AND ht.scope != 'unknown'
            )
        """
    else:
        fields += " WHERE 1 "
    fields += """
        AND v.slug IS NOT NULL AND rv.slug IS NOT NULL
        AND vs.delivery_column_name IS NOT NULL
        AND vs.period_scope IN ('intervals', 'year_independent')
        ORDER BY p.slug,r.slug,v.slug,rv.slug,vs.valid_from,vs.valid_to,
                 vs.period_scope,vs.delivery_column_name,vs.state_id
    """
    if held and not unheld:
        fields += ", ht.physical_id,hc.name,hm.representation_canonical,hp.lo,hp.hi"
    fields += " LIMIT 1"
    row = conn.execute(fields).fetchone()
    if row is None:
        raise AssertionError("Artifact has no addressable sample for this contract")
    if row["period_scope"] == "year_independent":
        period = "_default"
    else:
        period = (
            max(row["valid_from"], row["lo"])
            if held and not unheld
            else row["valid_from"]
        )
    fqid = "/".join((row["provider"], row["register_slug"], row["variable"]))
    return {
        "schema_version": "3.0.0",
        "steward": steward,
        "reg_meta_version": "conformance",
        "name": "Artifact sample",
        "sources": [
            {
                "name": "Sample",
                "register_variant": "/".join(
                    (row["provider"], row["register_slug"], row["variant"])
                ),
                "period": period,
                "bindings": [
                    {
                        "variable": fqid,
                        "type": "categorical",
                        "representation": row["representation"],
                    }
                ],
            }
        ],
    }


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def server_client(request, directory):
    """An HTTP client on the Rust server (`--server-cmd`) for the artifact in
    `directory`: it answers `/api/search`, `/api/catalog`, `/api/states` and
    `/api/project/order`.

    Without `--server-cmd` the test skips, except under `--run-release`: release
    admission must not pass without its browse and search traversals."""
    servers = request.getfixturevalue("http_servers")
    if servers is None:
        if request.config.getoption("--run-release"):
            pytest.fail(
                "Release admission browses and searches the Rust server: "
                "pass --server-cmd"
            )
        pytest.skip("browse and search run against --server-cmd")
    with open_db(directory / "reg_meta.db") as conn:
        steward = get_manifest(conn).get("steward", "global")
    return servers.client(
        {
            "REG_META_DB": str(directory),
            "REG_WEBAPP_STEWARD": steward,
            "REG_WEBAPP_STEWARDS_DIR": str(CASES.parents[1] / "reg_webapp/stewards"),
        }
    )


# operations.toml (search): paging stops at a hard 1,000-result depth and a
# researcher who reaches it must refine the query. Exact identity matches always
# lead the order (#1180), so a binding searched by its own name ranks past that
# depth only when more exact matches than the ceiling share the name; it is then,
# by contract, unreachable through that name alone.
SEARCH_DEPTH_CEILING = 1_000
# conformance/api/operations.toml (search): `q` is at most 200 characters (Unicode
# scalar values; Python `len`). A longer name is out of contract for search, which
# refuses it located on `q`.
MAX_QUERY_CHARS = 200


def _fold(text):
    """The documented search fold (`fold_search`, RUST_RUNTIME_SPEC.md section 5),
    restated so the oracle does not share the reader's code: casefold, NFKD, drop
    combining marks, until nothing changes; then one space between words."""
    for _ in range(3):
        decomposed = unicodedata.normalize("NFKD", text.casefold())
        folded = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
        if folded == text:
            return " ".join(text.split())
        text = folded
    raise AssertionError(f"search fold did not settle in 3 passes: {text!r}")


def _identity_texts(row):
    """The published identity texts the reader scores for a variable-search row.

    A leaf row offers its FQID and slug leaf, name and delivery columns; a group
    row offers its key and label and each member's FQID, name, delivery column
    and facets (member slug leaves are not identity texts).
    """
    fqid = row.get("fqid")
    texts = [
        fqid,
        fqid.rsplit("/", 1)[-1] if fqid else None,
        row.get("name"),
        *(row.get("delivery_column_names") or ()),
        row.get("key"),
        row.get("label"),
    ]
    for member in row.get("members") or ():
        texts.extend((member.get("fqid"), member.get("name")))
        texts.append(member.get("delivery_column"))
        for facet in member.get("facets") or ():
            texts.extend((facet.get("value"), facet.get("label")))
    return [text for text in texts if isinstance(text, str)]


def exact_identity_match(query, row):
    """Whether a consumed row matches the query exactly by a published identity."""
    folded = _fold(query)
    return any(_fold(text) == folded for text in _identity_texts(row))


def ceiling_exhausted(query, rows):
    """A miss is contractual only when exact matches alone fill the depth ceiling."""
    return len(rows) >= SEARCH_DEPTH_CEILING and all(
        exact_identity_match(query, row) for row in rows
    )


def http_search_traversal(client, query, scope, binding, register=None):
    """Follow HTTP variable cursors; return (found, top-level rows consumed)."""
    params = {"q": query, "type": "variable", "limit": 100, "scope": scope}
    if register is not None:
        params["register"] = register
    cursors = set()
    rows = []
    while True:
        response = client.get("/api/search", params=params)
        require(response.status_code == 200, "Acceptance HTTP search failed")
        page = response.json()["data"]
        if binding in response_fqids(page["items"]):
            return True, rows
        rows.extend(page["items"])
        cursor = page["next_cursor"]
        if cursor is None:
            return False, rows
        require(
            cursor and cursor not in cursors, "Acceptance HTTP search cursor stalled"
        )
        cursors.add(cursor)
        params["cursor"] = cursor


def http_search_contains(client, query, scope, binding):
    return http_search_traversal(client, query, scope, binding)[0]


def require_query_refused(search, query, scope):
    """Require HTTP search to refuse an over-cap query as `invalid_parameter` on `q`."""
    params = {"q": query, "type": "variable", "limit": 100, "scope": scope}
    response = search.get("/api/search", params=params)
    require(
        response.status_code == 422
        and response.json()["error"]["code"] == "invalid_parameter"
        and response.json()["error"]["fields"]["parameter"] == "q",
        "Over-cap HTTP search query was not refused on q",
    )


def require_search_reaches(search, query, scope, binding):
    """Require HTTP name search to reach an admitted binding.

    The traversal may miss it only when the miss is contractual: it consumed the
    whole depth ceiling and every consumed row matches the query exactly, so exact
    matches alone outnumber the ceiling. The researcher's documented refinement,
    search narrowed to the binding's register, must then find it.

    A name longer than `MAX_QUERY_CHARS` is out of contract for search: it must be
    refused on `q`, and the caller's `/api/catalog/<fqid>` browse checks are the
    binding's whole reachability proof.

    Edge cases: a query with exactly 1,000 results looks like a truncated one.
    Returns whether the refinement was needed.
    """
    if len(query) > MAX_QUERY_CHARS:
        require_query_refused(search, query, scope)
        return False
    # The pre-ceiling miss below is the guard for a genuine reader defect; no
    # readable fixture can pin it without introducing one, so only the ceiling
    # branch has a source-built case (reader/search-ceiling).
    found, rows = http_search_traversal(search, query, scope, binding)
    require(
        found or ceiling_exhausted(query, rows),
        "Sample missing from HTTP search traversal",
    )
    if found:
        return False
    refined, _ = http_search_traversal(
        search, query, scope, binding, register=binding.rsplit("/", 1)[0]
    )
    require(
        refined,
        "Sample past the search depth ceiling missing from register-refined search",
    )
    return True


def assert_sampled_agreement(artifact_dir, server):
    """Observe the same public contracts for admitted and regression artifacts;
    return the sampled order's `data`. `server` is the Rust server's client
    (`server_client`)."""
    with open_db(artifact_dir / "reg_meta.db") as conn:
        project = sample_project(conn)
    fqid = project["sources"][0]["bindings"][0]["variable"]
    scope = "reference" if project["steward"] == "global" else "holdings"
    browse = server.get("/api/catalog/" + fqid, params={"scope": scope})
    require(browse.status_code == 200, "Sampled binding missing from browse")
    require(
        browse.json()["data"]["fqid"] == fqid, "Browse identity disagrees with sample"
    )
    query = browse.json()["data"]["name"]
    params = {"q": query, "type": "variable", "limit": 100, "scope": scope}
    first_page = server.get("/api/search", params=params)
    require(
        first_page.content == server.get("/api/search", params=params).content,
        "Repeated HTTP first page differs",
    )
    require_search_reaches(server, query, scope, fqid)
    validated = server.post("/api/project/validate", json=project)
    require(
        validated.status_code == 200 and validated.json()["data"]["ok"],
        "Sample validation disagrees with admission",
    )
    ordered = server.post("/api/project/order", json=project)
    require(ordered.status_code == 200, "Sampled admitted binding did not order")
    response = server.post("/api/project/order/manifest", json=project)
    require(response.status_code == 200, "HTTP sample order download failed")
    require(
        ordered.json()["data"] == json.loads(response.content),
        "Order data and its download disagree",
    )
    require(
        server.post("/api/project/order/manifest", json=project).content
        == response.content,
        "Repeated HTTP order bytes differ",
    )
    return ordered.json()["data"]
