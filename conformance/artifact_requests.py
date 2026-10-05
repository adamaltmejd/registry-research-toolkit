"""Build public requests from artifact content; never retain real identifiers as files."""

from __future__ import annotations

from reg_meta.db import get_manifest


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
            "hm.representation_canonical representation, hp.lo, hp.hi",
        )
        fields += """
            JOIN holding_mapping hm ON hm.variable_id=vs.variable_id
                AND hm.variant_id=vs.register_variant_id
                AND lower(hm.representation_canonical)=lower(vs.delivery_column_name)
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
