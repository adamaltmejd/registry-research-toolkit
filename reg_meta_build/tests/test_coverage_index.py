"""#371: the covering index for the #351 coverage aggregates.

`idx_variable_state_coverage` on `variable_state(variable_id, valid_from,
valid_to)` lets the per-variable / per-register MIN(valid_from)/MAX(valid_to) span
compute index-only (no table b-tree lookup).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from _slugged_db import add_state, add_variable, build_slugged_db

if TYPE_CHECKING:
    from pathlib import Path


def test_coverage_index_exists(fixture_db: Path) -> None:
    import sqlite3

    conn = sqlite3.connect(fixture_db)
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'index' AND name = 'idx_variable_state_coverage'"
        ).fetchone()
        assert row is not None
    finally:
        conn.close()


def test_coverage_aggregate_uses_covering_index() -> None:
    """EXPLAIN QUERY PLAN proves the MIN/MAX span scans the COVERING INDEX — no
    table lookup. Probed on the direct `variable_state` aggregate (the shape the
    #351 join's grouped side reduces to); the plan text is deterministic given the
    index."""
    conn = build_slugged_db(classification=None)
    add_variable(conn, register_id=1, var_id=90, name="A", slug="vara")
    add_state(
        conn,
        register_id=1,
        variable_slug="vara",
        register_variant_id=10,
        valid_from="2018-01-01",
        valid_to="9999-12-31",
        delivery_column_name="A",
    )
    plan = conn.execute(
        "EXPLAIN QUERY PLAN "
        "SELECT variable_id, MIN(valid_from), MAX(valid_to) "
        "FROM variable_state GROUP BY variable_id"
    ).fetchall()
    detail = " ".join(str(r[-1]) for r in plan)
    assert "COVERING INDEX idx_variable_state_coverage" in detail, detail


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
