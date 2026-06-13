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
