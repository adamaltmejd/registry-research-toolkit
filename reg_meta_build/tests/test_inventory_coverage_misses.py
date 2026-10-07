"""Delivery-inventory coverage misses against a catalog's delivery states."""

from __future__ import annotations

from _slugged_db import add_variable, build_slugged_db


def test_year_independent_inventory_requires_exact_physical_column() -> None:
    from reg_meta.inventory import DeliveryInventory
    from reg_meta_build.inventory_coverage import (
        coverage_misses,
        errata_stanzas,
        errata_worklist,
    )

    conn = build_slugged_db(classification=None)
    try:
        add_variable(conn, register_id=1, var_id=90, name="A", slug="vara")
        variable_id = conn.execute(
            "SELECT variable_id FROM variable WHERE slug='vara'"
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO variable_state (variable_id, register_variant_id, "
            "period_scope, delivery_column_name) VALUES (?, 10, 'year_independent', 'Foo')",
            (variable_id,),
        )
        inventory = DeliveryInventory.model_validate(
            {
                "version": 1,
                "steward": "example",
                "table": [
                    {
                        "id": "dbo.Birth",
                        "edition": "_default",
                        "period_scope": "year_independent",
                        "column": [
                            {
                                "name": "Foo",
                                "mapping": [
                                    {
                                        "register_variant": "scb/lisa/individer-15plus",
                                        "variable": "scb/lisa/vara",
                                        "representation": "Foo",
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
        )
        assert not coverage_misses(conn, inventory).misses
        conn.execute(
            "UPDATE variable_state SET delivery_column_name='Other' WHERE variable_id=?",
            (variable_id,),
        )
        report = coverage_misses(conn, inventory)
        assert len(report.misses) == 1
        miss = report.misses[0]
        assert miss.period_scope == "year_independent"
        assert not miss.editions and not miss.versions
        assert not report.delivered_misses and not report.errata_column_misses
        assert errata_stanzas(report.misses) == ""
        assert "no exact independent delivery state" in errata_worklist(report)
    finally:
        conn.close()
