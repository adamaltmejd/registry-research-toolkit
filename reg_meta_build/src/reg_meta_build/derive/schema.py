"""Schema-family aggregates: `get coded-variables` per scope.

Counting a name's distinct codes is an aggregate over every coded state (5.5 s
on the pinned global artifact), so it is compiled once (RUST_RUNTIME_SPEC.md
stage 3b-3e decision 2). `schema`, `diff` and `coverage` read `expanded_state`,
`variable_state` and `browse_delivery` per register instead: a plain join that
measured at most 250 ms for a whole register's history.
"""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

# Bootstrap by calling the reader (RUST_RUNTIME_SPEC.md section 4); this moves
# into reg_meta_build in stage 4.
from reg_meta.queries import get_coded_variables

from reg_meta_build.derive.browse import browse_scopes

if TYPE_CHECKING:
    from reg_meta_build.derive.browse import Scope
    from reg_meta_build.validate import ValidationResult

# (scope, variable_name, n_distinct_codes, n_registers, n_instances)
type CodedRow = tuple[str, str, int, int, int]


def coded_variables(conn: sqlite3.Connection, scope: Scope) -> list[CodedRow]:
    """The reader's unfiltered `get coded-variables` under `scope`, by name.

    The reader's default `min_codes` and `min_registers` of 1 drop nothing, so
    the table holds every ranked name; the Rust operation applies no filter.
    """
    factory = conn.row_factory
    try:
        conn.row_factory = sqlite3.Row
        rows = get_coded_variables(conn, limit=-1, scope=scope)
    finally:
        conn.row_factory = factory
    return sorted(
        (
            scope,
            row["variable_name"],
            row["n_distinct_codes"],
            row["n_registers"],
            row["n_instances"],
        )
        for row in rows
    )


def derive_coded(conn: sqlite3.Connection, scopes: tuple[Scope, ...]) -> None:
    """Replace the `scopes` rows of `coded_variable_stats`."""
    conn.execute(
        "DELETE FROM coded_variable_stats WHERE scope IN "
        f"({', '.join('?' * len(scopes))})",
        scopes,
    )
    conn.executemany(
        "INSERT INTO coded_variable_stats (scope, variable_name, n_distinct_codes, "
        "n_registers, n_instances) VALUES (?, ?, ?, ?, ?)",
        (row for scope in scopes for row in coded_variables(conn, scope)),
    )


def check_coded(
    conn: sqlite3.Connection, result: ValidationResult, tables: set[str]
) -> None:
    """`coded_variable_stats` equals one recomputation per served scope."""
    result.section("[coded_variable_stats]")
    if not {"coded_variable_stats", "value_set", "value_set_member"} <= tables:
        return  # _check_schema_shape already failed.
    stored = set(
        map(
            tuple,
            conn.execute(
                "SELECT scope, variable_name, n_distinct_codes, n_registers, "
                "n_instances FROM coded_variable_stats"
            ),
        )
    )
    try:
        scopes = browse_scopes(conn)
        expected = {row for scope in scopes for row in coded_variables(conn, scope)}
    except (ValueError, TypeError, KeyError) as exc:
        result.fail(f"coded_variable_stats cannot be recomputed: {exc}")
        return
    missing, surplus = expected - stored, stored - expected
    if missing or surplus:
        located = sorted({row[:2] for row in missing | surplus})[:5]
        result.fail(
            "coded_variable_stats disagrees with get_coded_variables: "
            f"{len(missing):,} missing, {len(surplus):,} surplus row(s) at "
            f"(scope, variable_name) {located}"
        )
    else:
        result.ok(
            "coded_variable_stats equals get_coded_variables over "
            f"{', '.join(scopes)} ({len(stored):,} rows)"
        )
