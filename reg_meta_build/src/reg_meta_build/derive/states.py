"""Expanded states: the resolver's whole-history output per (variable, variant).

`expanded_state` holds every representation a request can emit: each base state,
and each alias window that participates in it, with its kind, bounds and canonical
column. `resolver_column` is its SQL projection. The request-dependent rules (the
window fallback, held and requested clipping, warning attribution) stay in the
reader over these rows (reg_meta/DESIGN.md, "Compiled states").
"""

from __future__ import annotations

import os
import sqlite3
from concurrent.futures import ProcessPoolExecutor
from contextlib import contextmanager
from typing import TYPE_CHECKING, Literal

# Bootstrap by calling the reader (RUST_RUNTIME_SPEC.md section 4): the resolver
# rules are read from it, so the derived rows equal what it emits by construction.
# These reader internals move into reg_meta_build in stage 4.
from reg_meta.catalog import Catalog, _applicable_alias_windows
from reg_meta.db import register_py_lower

if TYPE_CHECKING:
    from collections.abc import Iterator

    from reg_meta_build.validate import ValidationResult

# simplify: worker startup costs more than the work below this many pairs, so
# fixtures stay in-process. The pinned global artifact has ~71k pairs. Replace the
# moved resolver with set-based SQL if G1 needs more.
_PARALLEL_MIN_PAIRS = 5_000

# Columns of `expanded_state` after its id, in declaration order.
EXPANDED_COLUMNS = (
    "state_id",
    "variable_id",
    "register_variant_id",
    "kind",
    "delivery_column_name",
    "window_valid_from",
    "valid_from",
    "valid_to",
    "canonical_column",
)

type ExpandedRow = tuple[
    int, int, int, str, str | None, str | None, str | None, str | None, str | None
]

# The projection holdings canonicalize against: every column some request emits.
# A replaceable base is emitted only where no source window overlaps the request,
# and its column is always one of theirs.
RESOLVER_COLUMN_SOURCE = (
    "SELECT DISTINCT variable_id, register_variant_id, py_lower(canonical_column), "
    "canonical_column FROM expanded_state "
    "WHERE kind != 'base_fallback' AND canonical_column IS NOT NULL"
)

_worker_catalog: Catalog | None = None


def _reader(
    conn: sqlite3.Connection, scope: Literal["reference", "holdings"] = "reference"
) -> Catalog:
    conn.row_factory = sqlite3.Row
    register_py_lower(conn)
    return Catalog(conn, scope=scope)


@contextmanager
def reader_catalog(
    conn: sqlite3.Connection, scope: Literal["reference", "holdings"] = "reference"
) -> Iterator[Catalog]:
    """The reader over `conn` as derive calls it, with the row factory it needs;
    `conn`'s own row factory is restored on exit."""
    factory = conn.row_factory
    try:
        yield _reader(conn, scope)
    finally:
        conn.row_factory = factory


def _window_kind(window: tuple) -> str:
    if window[11] == "per_column":
        return "coded_window"
    return "source_window" if window[3] is None else "curated_window"


def _expanded(catalog: Catalog, pairs: list[tuple[int, int]]) -> list[ExpandedRow]:
    out: list[ExpandedRow] = []
    for variable_id, variant_id in pairs:
        windows = catalog._variable_windows(variable_id).get(variant_id, [])
        for row in catalog._states_in_bounds(variable_id, variant_id, None):
            keep_base, applied = _applicable_alias_windows(row, windows, None)
            state = (row["state_id"], variable_id, variant_id)
            column = row["delivery_column_name"]
            out.append(
                (
                    *state,
                    "base" if keep_base else "base_fallback",
                    column,
                    None,
                    row["valid_from"],
                    row["valid_to"],
                    catalog.canonical_delivery_column(variable_id, variant_id, column),
                )
            )
            # A per-column (metadata or coding) window is clipped to the state;
            # index 16 keeps its own start, the key of its `variable_alias_window` row.
            out.extend(
                (
                    *state,
                    _window_kind(window),
                    window[0],
                    window[16],
                    window[1],
                    window[2],
                    catalog.canonical_delivery_column(
                        variable_id, variant_id, window[0]
                    ),
                )
                for window in applied
            )
    return out


def _open_worker(path: str) -> None:
    global _worker_catalog
    # A plain connection, never immutable: an extend-db overlay lives in the WAL.
    _worker_catalog = _reader(sqlite3.connect(path))


def _worker_expanded(pairs: list[tuple[int, int]]) -> list[ExpandedRow]:
    assert _worker_catalog is not None
    return _expanded(_worker_catalog, pairs)


def expanded_states(conn: sqlite3.Connection) -> list[ExpandedRow]:
    """Every representation the resolver can emit, in `expanded_state_id` order.

    Per (variable, variant) in key order, per base state in the reader's
    chronological order: the base row, then its participating windows in the
    resolver's order. A base that some source window spelled like it replaces is
    kind `base_fallback`: the reader emits it only for a request no source window
    overlaps. A large artifact is computed by worker processes over its committed
    rows.
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
        with reader_catalog(conn) as catalog:
            return _expanded(catalog, pairs)
    if conn.in_transaction:
        raise ValueError("Derive workers read committed rows; commit before deriving")
    workers = os.process_cpu_count() or 1
    size = -(-len(pairs) // (4 * workers))
    chunks = [pairs[start : start + size] for start in range(0, len(pairs), size)]
    with ProcessPoolExecutor(
        workers, initializer=_open_worker, initargs=(path,)
    ) as pool:
        return [row for rows in pool.map(_worker_expanded, chunks) for row in rows]


def derive_states(conn: sqlite3.Connection) -> None:
    """Refill `expanded_state` and its `resolver_column` projection."""
    rows = expanded_states(conn)
    conn.execute("DELETE FROM resolver_column")
    conn.execute("DELETE FROM expanded_state")
    conn.executemany(
        f"INSERT INTO expanded_state (expanded_state_id, {', '.join(EXPANDED_COLUMNS)}) "
        f"VALUES ({', '.join('?' * (len(EXPANDED_COLUMNS) + 1))})",
        ((n, *row) for n, row in enumerate(rows, 1)),
    )
    register_py_lower(conn)
    conn.execute(
        "INSERT INTO resolver_column (variable_id, register_variant_id, "
        f"delivery_column_lower, delivery_column_name) {RESOLVER_COLUMN_SOURCE} "
        "ORDER BY 1, 2, 3"
    )


def check_states(
    conn: sqlite3.Connection, result: ValidationResult, tables: set[str]
) -> None:
    """`expanded_state` equals one recomputation, ids included, and
    `resolver_column` equals its projection; one spelling per column fold.

    Holdings canonicalize against `resolver_column`, so this also guards their
    mappings."""
    result.section("[expanded_state]")
    if not {"expanded_state", "resolver_column"} <= tables:
        return  # _check_schema_shape already failed.
    stored = set(
        map(
            tuple,
            conn.execute(
                f"SELECT expanded_state_id, {', '.join(EXPANDED_COLUMNS)} "
                "FROM expanded_state"
            ),
        )
    )
    try:
        expected = {(n, *row) for n, row in enumerate(expanded_states(conn), 1)}
    except (ValueError, TypeError, KeyError) as exc:
        result.fail(f"expanded_state cannot be recomputed: {exc}")
        return
    missing, surplus = expected - stored, stored - expected
    if missing or surplus:
        located = sorted({row[2] for row in missing | surplus})[:5]
        result.fail(
            f"expanded_state disagrees with the resolver: {len(missing):,} missing, "
            f"{len(surplus):,} surplus row(s) at variable_id {located}"
        )
    else:
        result.ok(f"expanded_state equals the resolver ({len(stored):,} rows)")
    register_py_lower(conn)
    stored_columns = "SELECT * FROM resolver_column"
    (unprojected,) = conn.execute(
        f"SELECT COUNT(*) FROM ({RESOLVER_COLUMN_SOURCE} EXCEPT {stored_columns})"
    ).fetchone()
    (unemitted,) = conn.execute(
        f"SELECT COUNT(*) FROM ({stored_columns} EXCEPT {RESOLVER_COLUMN_SOURCE})"
    ).fetchone()
    if unprojected or unemitted:
        result.fail(
            "resolver_column is not the projection of expanded_state: "
            f"{unprojected:,} missing, {unemitted:,} surplus row(s)"
        )
    else:
        result.ok("resolver_column is the projection of expanded_state")
    respelled = conn.execute(
        "SELECT variable_id, register_variant_id, py_lower(canonical_column) "
        "FROM expanded_state WHERE canonical_column IS NOT NULL GROUP BY 1, 2, 3 "
        "HAVING COUNT(DISTINCT canonical_column) > 1 ORDER BY 1, 2, 3 LIMIT 5"
    ).fetchall()
    if respelled:
        result.fail(
            "expanded_state spells a column fold more than one way at "
            f"(variable_id, register_variant_id, fold) {[tuple(r) for r in respelled]}"
        )
    else:
        result.ok("expanded_state spells each column fold one way")
    for table in ("expanded_state", "resolver_column"):
        orphans = conn.execute(f"PRAGMA foreign_key_check({table})").fetchall()
        if orphans:
            result.fail(f"{len(orphans):,} {table} row(s) with dangling keys")
