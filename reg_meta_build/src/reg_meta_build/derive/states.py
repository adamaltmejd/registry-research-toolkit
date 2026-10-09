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
from functools import lru_cache
from typing import TYPE_CHECKING

from reg_meta_build.db import register_py_lower

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

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

_worker_conn: sqlite3.Connection | None = None


def _prepare(conn: sqlite3.Connection) -> sqlite3.Connection:
    conn.row_factory = sqlite3.Row
    register_py_lower(conn)
    return conn


@contextmanager
def named_rows(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """`conn` with the row factory and the `py_lower` fold derive's reads need;
    `conn`'s own row factory is restored on exit."""
    factory = conn.row_factory
    try:
        yield _prepare(conn)
    finally:
        conn.row_factory = factory


# The resolver's whole-history rules, moved from `reg_meta.catalog` (package 4.2).

type _StoredAliasWindow = tuple[
    str,
    str,
    str,
    str | None,
    str,
    str | None,
    str | None,
    str | None,
    str | None,
    str | None,
    str | None,
    str,
    int | None,
    str,
    str | None,
    str | None,
    str,
]


def representative_columns(
    stated: Iterable[str | None], aliased: Iterable[str | None] = ()
) -> dict[str, str]:
    """Each case-folded delivery column mapped to the ONE spelling a reader names
    it under: the state's own spelling where a state names the column, else the
    lowest alias spelling by byte order. The single home of that rule — see
    DESIGN.md → One spelling per delivery column for why the readers owe each
    other one spelling, and `reg_meta_build.db.register_py_lower` for the fold.

    A `None` column (a state SCB named no delivery column for) has no spelling to
    pick and contributes nothing."""
    spellings: dict[str, str] = {}
    for columns in (aliased, stated):
        # Descending, so each fold's LOWEST spelling is the last one written; the
        # stated pass runs second, so a state's own spelling takes the fold.
        named = sorted((c for c in columns if c is not None), reverse=True)
        spellings |= {column.lower(): column for column in named}
    return spellings


def _delivery_column_spellings(
    conn: sqlite3.Connection, variable_id: int, variant_id: int
) -> dict[str, str]:
    # Without sqlite_stat1 the planner picks the register-variant index and
    # filters every state of the variant. Builds now end with ANALYZE, and on an
    # analyzed artifact the planner picks this index unaided. The hint stays until
    # the latency oracle is re-run on an analyzed artifact. INDEXED BY fails
    # loudly if the schema ever renames the index.
    stated = conn.execute(
        "SELECT delivery_column_name FROM variable_state "
        "INDEXED BY idx_variable_state_variable "
        "WHERE variable_id = ? AND register_variant_id = ?",
        (variable_id, variant_id),
    )
    aliased = conn.execute(
        "SELECT delivery_column_name FROM variable_alias_window "
        "WHERE variable_id = ? AND register_variant_id = ?",
        (variable_id, variant_id),
    )
    return representative_columns(
        (row[0] for row in stated), (row[0] for row in aliased)
    )


def register_catalog_udfs(conn: sqlite3.Connection) -> None:
    """Register semantic column normalization without retaining a Catalog.

    The immutable artifact's spelling map has a bounded connection-local cache.
    Physical holding identifiers and compiled representations stay exact.
    """
    if (
        conn.execute(
            "SELECT 1 FROM pragma_function_list WHERE name = 'py_catalog_column'"
        ).fetchone()
        is not None
    ):
        return

    @lru_cache(maxsize=1024)
    def spellings(variable_id: int, variant_id: int) -> dict[str, str]:
        return _delivery_column_spellings(conn, variable_id, variant_id)

    def canonical_column(
        variable_id: int, variant_id: int, column: str | None
    ) -> str | None:
        if column is None:
            return None
        return spellings(variable_id, variant_id).get(column.lower(), column)

    conn.create_function("py_catalog_column", 3, canonical_column, deterministic=True)


def canonical_delivery_column(
    spellings: dict[str, str], column: str | None
) -> str | None:
    """Name a semantic source spelling by the shared whole-history rule.

    `spellings` is `_delivery_column_spellings` of the column's (variable,
    variant). This normalizes resolver metadata only; compiled physical
    identities and canonical holding representations remain exact strings.
    """
    if column is None:
        return None
    return spellings.get(column.lower(), column)


def _states_in_bounds(
    conn: sqlite3.Connection,
    variable_id: int,
    register_variant_id: int | None,
    bounds: tuple[str, str] | None,
) -> list[sqlite3.Row]:
    """`variable_state` rows for the variable whose validity range intersects
    `bounds` (an inclusive ISO `(lo, hi)` date interval), **chronological
    ascending** (oldest first) for the public surface. `register_variant_id`
    None spans every variant; `bounds` None returns every state (the
    `_default` / no-period-filter case). `conn` must read `sqlite3.Row`s.

    A2.5 generalizes the interim year-granular overlap test: bounds
    are full ISO dates, so sub-annual queries (`HT2020`, `2020-08`, a range)
    intersect precisely against the stored full-date validity ranges (see DESIGN.md → Two-level variable model) —
    the year-only INTERIM limit is lifted. The interval test is the standard
    `valid_from <= hi AND valid_to >= lo` (string compare is chronologically
    correct because every stored value is a full date)."""
    sql = (
        "SELECT vs.*, v.is_identifier, rv.name AS variant_label "
        "FROM variable_state vs JOIN variable v USING(variable_id) "
        "JOIN register_variant rv USING(register_variant_id) "
        "WHERE vs.variable_id = ? "
    )
    args = [variable_id]
    if register_variant_id is not None:
        sql += "AND vs.register_variant_id = ? "
        args.append(register_variant_id)
    rows = conn.execute(
        sql + "ORDER BY vs.valid_from, vs.valid_to, vs.value_set_version_label, "
        "vs.register_variant_id, vs.state_id",
        args,
    ).fetchall()
    if bounds is None:
        return rows
    lo, hi = bounds
    return [
        r
        for r in rows
        if r["period_scope"] == "intervals"
        and r["valid_from"] <= hi
        and r["valid_to"] >= lo
    ]


def _variable_windows(
    conn: sqlite3.Connection, variable_id: int
) -> dict[int, list[_StoredAliasWindow]]:
    """`variable_alias_window` rows (#319/#945/Y-132) grouped by
    `register_variant_id` → [(delivery_column_name, valid_from, valid_to,
    provenance, column_metadata, data_type, data_length, operational_definition,
    source_register_text, definition, measurement_unit, coding_metadata,
    value_set_id, value_set_version_label, name, description), …] sorted by window start.
    EMPTY for variables with no resolver-visible alias representations, so expansion is a no-op there.
    One indexed point-lookup on `idx_variable_alias_window_lookup`."""
    out: dict[int, list[_StoredAliasWindow]] = {}
    for (
        rvid,
        col,
        wfrom,
        wto,
        provenance,
        mode,
        dtype,
        length,
        operation,
        source_text,
        definition,
        unit,
        coding_mode,
        value_set_id,
        version_label,
        name,
        description,
    ) in conn.execute(
        "SELECT register_variant_id, delivery_column_name, valid_from, valid_to, "
        "provenance, column_metadata, data_type, data_length, "
        "operational_definition, source_register_text, definition, measurement_unit, "
        "coding_metadata, value_set_id, value_set_version_label, name, description "
        "FROM variable_alias_window WHERE variable_id = ? "
        "ORDER BY register_variant_id, valid_from, delivery_column_name",
        (variable_id,),
    ):
        out.setdefault(rvid, []).append(
            (
                col,
                wfrom,
                wto,
                provenance,
                mode,
                dtype,
                length,
                operation,
                source_text,
                definition,
                unit,
                coding_mode,
                value_set_id,
                version_label,
                name,
                description,
                wfrom,
            )
        )
    return out


def _applicable_alias_windows(
    row: sqlite3.Row,
    windows: list[_StoredAliasWindow],
    bounds: tuple[str, str] | None,
) -> tuple[bool, list[_StoredAliasWindow]]:
    """Shared resolver projection for full states and lightweight coverage rows."""
    if row["period_scope"] == "year_independent":
        return True, []
    lo, hi = bounds if bounds is not None else ("0001-01-01", "9999-12-31")
    participating = []
    for window in windows:
        if window[4] == "per_column" or window[11] == "per_column":
            start, end = (
                max(row["valid_from"], window[1]),
                min(row["valid_to"], window[2]),
            )
            if start <= end:
                participating.append((window[0], start, end, *window[3:]))
        elif row["valid_from"] <= window[1] and window[2] <= row["valid_to"]:
            participating.append(window)
    source = [w for w in participating if w[3] is None or w[11] == "per_column"]
    curated = [w for w in participating if w[3] is not None and w[11] != "per_column"]
    has_base = row["delivery_column_name"] is not None and any(
        w[0].lower() == row["delivery_column_name"].lower() for w in source
    )
    matched = [w for w in source if w[1] <= hi and w[2] >= lo]
    replace = bool(source and has_base and matched)
    return not replace, (matched if replace else []) + [
        w for w in curated if w[1] <= hi and w[2] >= lo
    ]


def _window_kind(window: tuple) -> str:
    if window[11] == "per_column":
        return "coded_window"
    return "source_window" if window[3] is None else "curated_window"


def _expanded(
    conn: sqlite3.Connection, pairs: list[tuple[int, int]]
) -> list[ExpandedRow]:
    out: list[ExpandedRow] = []
    for variable_id, variant_id in pairs:
        windows = _variable_windows(conn, variable_id).get(variant_id, [])
        spellings = _delivery_column_spellings(conn, variable_id, variant_id)
        for row in _states_in_bounds(conn, variable_id, variant_id, None):
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
                    canonical_delivery_column(spellings, column),
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
                    canonical_delivery_column(spellings, window[0]),
                )
                for window in applied
            )
    return out


def _open_worker(path: str) -> None:
    global _worker_conn
    # A plain connection, never immutable: an extend-db overlay lives in the WAL.
    _worker_conn = _prepare(sqlite3.connect(path))


def _worker_expanded(pairs: list[tuple[int, int]]) -> list[ExpandedRow]:
    assert _worker_conn is not None
    return _expanded(_worker_conn, pairs)


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
        with named_rows(conn):
            return _expanded(conn, pairs)
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
