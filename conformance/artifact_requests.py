"""Public requests and adapter agreement; never retain real identifiers as files."""

from __future__ import annotations

import json
import unicodedata

from acceptance_requests import response_fqids
from reg_meta.cli import run
from reg_meta.db import get_manifest, open_db
from reg_meta.order import materialize_order, project_from_raw


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


def cli_json(directory, capsys, arguments):
    code = run(["--db", str(directory), "--format", "json", *arguments])
    captured = capsys.readouterr()
    require(code == 0, "Acceptance CLI request failed")
    return json.loads(captured.out)


# reg_meta/DESIGN.md: search continuation has a hard 1,000-result depth ceiling and a
# researcher who reaches it must refine the query. Exact identity matches always
# lead the order (#1180), so a binding searched by its own name ranks past that
# depth only when more exact matches than the ceiling share the name; it is then,
# by contract, unreachable through that name alone.
SEARCH_DEPTH_CEILING = 1_000


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

    A leaf row offers its FQID and slug leaf, names and delivery columns; a group
    row offers its key and label and each member's FQID, name, delivery column
    and facets (member slug leaves are not identity texts).
    """
    fqid = row.get("fqid")
    texts = [
        fqid,
        fqid.rsplit("/", 1)[-1] if fqid else None,
        row.get("name"),
        row.get("datacolumn"),
        *(row.get("delivery_column_names") or ()),
        row.get("group_key"),
        row.get("group_label"),
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


def http_search_traversal(client, query, scope, binding):
    """Follow HTTP variable cursors; return (found, top-level rows consumed)."""
    params = {"q": query, "type": "variable", "limit": 100, "scope": scope}
    cursors = set()
    rows = []
    while True:
        response = client.get("/api/search", params=params)
        require(response.status_code == 200, "Acceptance HTTP search failed")
        groups = response.json()["groups"]
        if binding in response_fqids(groups):
            return True, rows
        rows.extend(row for group in groups for row in group["results"])
        group = next((group for group in groups if group["has_more"]), None)
        if group is None:
            return False, rows
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
    rows = []
    while binding not in response_fqids(page["results"]):
        rows.extend(page["results"])
        if not page["has_more"]:
            return False, rows
        cursor = page["next_cursor"]
        require(cursor and cursor not in cursors, "Sample CLI search cursor stalled")
        cursors.add(cursor)
        page = cli_json(directory, capsys, [*argv, "--cursor", cursor])
    return True, rows


def require_search_reaches(directory, client, capsys, query, scope, binding):
    """Require CLI and HTTP name search to reach an admitted binding.

    A traversal may miss it only when the miss is contractual: the traversal
    consumed the whole depth ceiling and every consumed row matches the query
    exactly, so exact matches alone outnumber the ceiling. The researcher's
    documented refinement, the reader's register-scoped search, must then find the
    binding. That proves READER reachability, not HTTP search reachability: HTTP
    search has no register refinement, and the binding's HTTP reachability is
    proven by the caller's `/api/catalog/<fqid>` browse checks.

    Edge cases: a query with exactly 1,000 results looks like a truncated one, and
    HTTP rows include net-new golden pins (latent: no variable pins exist today).
    Returns whether the refinement was needed.
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
    # The pre-ceiling miss below is the guard for a genuine reader defect; no
    # readable fixture can pin it without introducing one, so only the ceiling
    # branch has a source-built case (reader/search-ceiling).
    cli_found, cli_rows = cli_search_traversal(directory, capsys, argv, binding)
    require(
        cli_found or ceiling_exhausted(query, cli_rows),
        "Sample missing from CLI search traversal",
    )
    http_found, http_rows = http_search_traversal(client, query, scope, binding)
    require(
        http_found or ceiling_exhausted(query, http_rows),
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


def assert_sampled_agreement(artifact_dir, artifact_client, tmp_path, capsys):
    """Observe the same adapter contracts for admitted and regression artifacts."""
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
    require_search_reaches(artifact_dir, artifact_client, capsys, query, scope, fqid)
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
