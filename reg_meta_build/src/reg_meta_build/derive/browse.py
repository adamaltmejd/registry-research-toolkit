"""Browse deliveries: each register page's per-variable deliveries, per scope.

Browse eligibility is not the resolver's: it keeps alias windows no state
contains (`register_variable_deliveries`), so it never reads `resolver_column`.
Scope is data: `reference` rows in every artifact, `holdings` rows (the held
projection, clipped to physical periods) only in a steward artifact.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from reg_meta_build.derive.states import reader_catalog

if TYPE_CHECKING:
    import sqlite3

    from reg_meta_build.validate import ValidationResult

type Scope = Literal["reference", "holdings"]
# (scope, variable_id, register_variant_id, column, period_scope, state_count,
# windows as (valid_from, valid_to) in order)
type BrowseRow = tuple[str, int, int, str | None, str, int, tuple[tuple[str, str], ...]]


def browse_scopes(conn: sqlite3.Connection) -> tuple[Scope, ...]:
    """The scopes an artifact serves: holdings only in a steward artifact."""
    kind = conn.execute(
        "SELECT value FROM import_manifest WHERE key = 'catalog_artifact_kind'"
    ).fetchone()
    return (
        ("reference", "holdings") if kind and kind[0] == "steward" else ("reference",)
    )


def browse_deliveries(conn: sqlite3.Connection, scope: Scope) -> list[BrowseRow]:
    """`register_variable_deliveries` for every register under `scope`.

    Rows in `browse_delivery_id` order: registers by (provider, register) slug,
    variables by slug, then each variable's deliveries in the reader's order.
    """
    factory = conn.row_factory
    try:
        catalog = reader_catalog(conn, scope)
        registers = conn.execute(
            "SELECT p.slug, r.slug, r.register_id FROM register r "
            "JOIN provider p USING(provider_id) "
            "WHERE r.slug IS NOT NULL ORDER BY p.slug, r.slug"
        ).fetchall()
        # A delivery names a variant some row of its variable references; the
        # reader joins it without requiring it to share the register.
        keys = {
            (register_id, variable, variant): (variable_id, variant_id)
            for register_id, variable, variable_id, variant, variant_id in conn.execute(
                "SELECT v.register_id, v.slug, v.variable_id, rv.slug, "
                "rv.register_variant_id FROM (SELECT variable_id, register_variant_id "
                "FROM variable_state UNION SELECT variable_id, register_variant_id "
                "FROM variable_alias_window UNION SELECT variable_id, "
                "register_variant_id FROM variable_alias) JOIN variable v "
                "USING(variable_id) JOIN register_variant rv USING(register_variant_id) "
                # A slug twin across registers resolves to its highest id, so the
                # last write is fixed whatever plan SQLite picks.
                "ORDER BY 1, 2, 3, 4, 5"
            )
        }
        out: list[BrowseRow] = []
        for provider, register, register_id in registers:
            deliveries = catalog.register_variable_deliveries(provider, register)
            out.extend(
                (
                    scope,
                    *keys[register_id, slug, delivery.variant],
                    delivery.column,
                    delivery.period_scope,
                    delivery.coverage.state_count,
                    tuple((w.valid_from, w.valid_to) for w in delivery.windows),
                )
                for slug in sorted(deliveries)
                for delivery in deliveries[slug]
            )
        return out
    finally:
        conn.row_factory = factory


def derive_browse(conn: sqlite3.Connection, scopes: tuple[Scope, ...]) -> None:
    """Replace the `scopes` rows of `browse_delivery` and `delivery_window`.

    Ids continue after the rows kept, so the reference pass followed by the
    holdings pass numbers rows as one pass over both scopes would.
    """
    placeholders = ", ".join("?" * len(scopes))
    conn.execute(
        "DELETE FROM delivery_window WHERE browse_delivery_id IN (SELECT "
        f"browse_delivery_id FROM browse_delivery WHERE scope IN ({placeholders}))",
        scopes,
    )
    conn.execute(f"DELETE FROM browse_delivery WHERE scope IN ({placeholders})", scopes)
    (last,) = conn.execute(
        "SELECT COALESCE(MAX(browse_delivery_id), 0) FROM browse_delivery"
    ).fetchone()
    rows = [row for scope in scopes for row in browse_deliveries(conn, scope)]
    conn.executemany(
        "INSERT INTO browse_delivery (browse_delivery_id, scope, variable_id, "
        "register_variant_id, delivery_column_name, period_scope, state_count) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ((last + n, *row[:6]) for n, row in enumerate(rows, 1)),
    )
    conn.executemany(
        "INSERT INTO delivery_window (browse_delivery_id, valid_from, valid_to) "
        "VALUES (?, ?, ?)",
        ((last + n, *window) for n, row in enumerate(rows, 1) for window in row[6]),
    )


def check_browse(
    conn: sqlite3.Connection, result: ValidationResult, tables: set[str]
) -> None:
    """`browse_delivery` and its windows equal one recomputation per served
    scope, ids included; each delivery's windows are disjoint."""
    result.section("[browse_delivery]")
    if not {"browse_delivery", "delivery_window"} <= tables:
        return  # _check_schema_shape already failed.
    windows: dict[int, list[tuple[str, str]]] = {}
    for delivery_id, lo, hi in conn.execute(
        "SELECT browse_delivery_id, valid_from, valid_to FROM delivery_window "
        "ORDER BY 1, 2"
    ):
        windows.setdefault(delivery_id, []).append((lo, hi))
    stored = {
        (*row, tuple(windows.pop(row[0], ())))
        for row in conn.execute(
            "SELECT browse_delivery_id, scope, variable_id, register_variant_id, "
            "delivery_column_name, period_scope, state_count FROM browse_delivery"
        )
    }
    try:
        scopes = browse_scopes(conn)
        expected = {
            (n, *row)
            for n, row in enumerate(
                (row for scope in scopes for row in browse_deliveries(conn, scope)), 1
            )
        }
    except (ValueError, TypeError, KeyError) as exc:
        result.fail(f"browse_delivery cannot be recomputed: {exc}")
        return
    missing, surplus = expected - stored, stored - expected
    if missing or surplus or windows:
        located = sorted({(row[1], row[2]) for row in missing | surplus})[:5]
        result.fail(
            "browse_delivery disagrees with register_variable_deliveries: "
            f"{len(missing):,} missing, {len(surplus):,} surplus row(s), "
            f"{len(windows):,} window set(s) without a delivery; (scope, "
            f"variable_id) {located}"
        )
    else:
        result.ok(
            "browse_delivery equals register_variable_deliveries over "
            f"{', '.join(scopes)} ({len(stored):,} rows)"
        )
    overlapping = conn.execute(
        "SELECT DISTINCT d.scope, d.variable_id, a.browse_delivery_id "
        "FROM delivery_window a JOIN delivery_window b "
        "ON b.browse_delivery_id = a.browse_delivery_id "
        "AND b.valid_from > a.valid_from AND b.valid_from <= a.valid_to "
        "JOIN browse_delivery d ON d.browse_delivery_id = a.browse_delivery_id "
        "ORDER BY 3 LIMIT 5"
    ).fetchall()
    if overlapping:
        result.fail(
            "delivery_window overlaps within (scope, variable_id, browse_delivery_id) "
            f"{[tuple(row) for row in overlapping]}"
        )
    else:
        result.ok("delivery windows are disjoint")
