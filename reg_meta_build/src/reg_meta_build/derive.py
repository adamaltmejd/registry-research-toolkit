"""Derived tables: pure functions of a built artifact's core graph.

Derive drops and recomputes its tables in a fixed insertion order, so identical
inputs give identical bytes. `validate_built_db` recomputes them and requires
equality.
"""

from __future__ import annotations

import os
import sqlite3
from concurrent.futures import ProcessPoolExecutor

# Bootstrap by moving (RUST_RUNTIME_SPEC.md section 4): the resolver rules are read
# from the reader, so the derived rows equal what it returns by construction.
from reg_meta.catalog import Catalog
from reg_meta.db import register_py_lower

# simplify: worker startup costs more than the work below this many pairs, so
# fixtures stay in-process. The pinned global artifact has ~71k pairs: 88 s on one
# core, 31 s on ten. Replace the moved resolver with set-based SQL if G1 needs more.
_PARALLEL_MIN_PAIRS = 5_000

_worker_catalog: Catalog | None = None


def _catalog(conn: sqlite3.Connection) -> Catalog:
    conn.row_factory = sqlite3.Row
    register_py_lower(conn)
    return Catalog(conn)


def _emitted(
    catalog: Catalog, pairs: list[tuple[int, int]]
) -> list[tuple[int, int, str, str]]:
    return [
        (variable_id, variant_id, name.lower(), name)
        for variable_id, variant_id in pairs
        for name in sorted(
            catalog.delivery_columns(variable_id, variant_id), key=str.lower
        )
    ]


def _open_worker(path: str) -> None:
    global _worker_catalog
    # A plain connection, never immutable: an extend-db overlay lives in the WAL.
    _worker_catalog = _catalog(sqlite3.connect(path))


def _worker_emitted(pairs: list[tuple[int, int]]) -> list[tuple[int, int, str, str]]:
    assert _worker_catalog is not None
    return _emitted(_worker_catalog, pairs)


def resolver_columns(conn: sqlite3.Connection) -> list[tuple[int, int, str, str]]:
    """Whole-history resolver-emitted delivery columns per (variable, variant).

    Rows are `(variable_id, register_variant_id, lowercase name, canonical
    spelling)` in primary-key order. A pair without a base state emits nothing,
    so the pairs come from `variable_state`. A large artifact is computed by
    worker processes over its committed rows.
    """
    pairs = [
        (variable_id, variant_id)
        for variable_id, variant_id in conn.execute(
            "SELECT DISTINCT variable_id, register_variant_id FROM variable_state "
            "ORDER BY variable_id, register_variant_id"
        )
    ]
    path = next(
        row[2] for row in conn.execute("PRAGMA database_list") if row[1] == "main"
    )
    if len(pairs) < _PARALLEL_MIN_PAIRS or not path:
        factory = conn.row_factory
        try:
            return _emitted(_catalog(conn), pairs)
        finally:
            conn.row_factory = factory
    if conn.in_transaction:
        raise ValueError("Derive workers read committed rows; commit before deriving")
    workers = os.process_cpu_count() or 1
    size = -(-len(pairs) // (4 * workers))
    chunks = [pairs[start : start + size] for start in range(0, len(pairs), size)]
    with ProcessPoolExecutor(
        workers, initializer=_open_worker, initargs=(path,)
    ) as pool:
        return [row for rows in pool.map(_worker_emitted, chunks) for row in rows]


def derive(conn: sqlite3.Connection) -> None:
    """Recompute every derived table from the committed core graph on `conn`."""
    rows = resolver_columns(conn)
    conn.execute("DELETE FROM resolver_column")
    conn.executemany(
        "INSERT INTO resolver_column (variable_id, register_variant_id, "
        "delivery_column_lower, delivery_column_name) VALUES (?, ?, ?, ?)",
        rows,
    )
