"""Public requests and adapter agreement; never retain real identifiers as files."""

from __future__ import annotations

import json

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
    hits = [hit for group in search.json()["groups"] for hit in group["results"]]
    seen_cursors = set()
    while not any(hit.get("fqid") == fqid for hit in hits):
        group = search.json()["groups"][0]
        if not group["has_more"]:
            break
        cursor = group["next_cursor"]
        require(cursor and cursor not in seen_cursors, "HTTP search cursor stalled")
        seen_cursors.add(cursor)
        search = artifact_client.get("/api/search", params={**params, "cursor": cursor})
        require(search.status_code == 200, "Sample search continuation failed")
        hits = [hit for group in search.json()["groups"] for hit in group["results"]]
    require(
        any(hit.get("fqid") == fqid for hit in hits),
        "Sample missing from HTTP search traversal",
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
    page = json.loads(first)
    seen_cursors = set()
    while not any(hit.get("fqid") == fqid for hit in page["results"]):
        if not page["has_more"]:
            break
        cursor = page["next_cursor"]
        require(cursor and cursor not in seen_cursors, "CLI search cursor stalled")
        seen_cursors.add(cursor)
        require(run([*argv, "--cursor", cursor]) == 0, "CLI search continuation failed")
        page = json.loads(capsys.readouterr().out)
    require(
        any(hit.get("fqid") == fqid for hit in page["results"]),
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
