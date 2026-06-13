"""#352 codes-search perf fix: the `variable_id` index on `code_variable_map`.

`idx_code_variable_map_variable` lets the codes-search owning-variable annotation's
per-variable count correlated subquery
(`SELECT COUNT(*) FROM code_variable_map c2 WHERE c2.variable_id = v.variable_id`)
use the index instead of full-scanning the 4.1M-row map (its WITHOUT ROWID
`(code_id, variable_id)` PK can't serve a bare `variable_id` lookup). Mirrors the
#371 covering-index test.
"""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

from _slugged_db import build_slugged_db

if TYPE_CHECKING:
    from pathlib import Path


def test_code_variable_map_variable_index_exists(fixture_db: Path) -> None:
    conn = sqlite3.connect(fixture_db)
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'index' AND name = 'idx_code_variable_map_variable'"
        ).fetchone()
        assert row is not None
    finally:
        conn.close()


def test_owner_count_subquery_uses_variable_index() -> None:
    """EXPLAIN QUERY PLAN proves the owner-count correlated subquery searches the
    `variable_id` index — no `SCAN code_variable_map`. This is the exact subquery
    shape from `reg_meta.queries._code_owner_annotations_batch` that hung the live
    omnibox (full-scan per owner row); the plan is deterministic given the index."""
    conn = build_slugged_db(classification=None)
    plan = conn.execute(
        "EXPLAIN QUERY PLAN "
        "SELECT (SELECT COUNT(*) FROM code_variable_map c2 "
        "          WHERE c2.variable_id = v.variable_id) "
        "FROM variable v"
    ).fetchall()
    detail = " ".join(str(r[-1]) for r in plan)
    assert "idx_code_variable_map_variable" in detail, detail
    assert "SCAN code_variable_map" not in detail, detail
