"""Browse deliveries: each register page's per-variable deliveries, per scope.

Browse eligibility is not the resolver's: it keeps alias windows no state
contains (`_reference_deliveries`), so it never reads `resolver_column`.
Scope is data: `reference` rows in every artifact, `holdings` rows (the held
projection, clipped to physical periods) only in a steward artifact.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from reg_meta.errors import EXIT_NOT_FOUND, RegMetaError
from reg_meta.inventory import _merge

from reg_meta_build.derive.states import (
    _applicable_alias_windows,
    _variable_windows,
    named_rows,
    representative_columns,
)

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
    """The deliveries of every register under `scope`: `_reference_deliveries`
    or `_held_deliveries`.

    Rows in `browse_delivery_id` order: registers by (provider, register) slug,
    variables by slug, then each variable's deliveries in the order those return.
    """
    with named_rows(conn):
        registers = conn.execute(
            "SELECT p.slug, r.slug, r.register_id FROM register r "
            "JOIN provider p USING(provider_id) "
            "WHERE r.slug IS NOT NULL ORDER BY p.slug, r.slug"
        ).fetchall()
        # A delivery names a variant some row of its variable references; the
        # listing joins it without requiring it to share the register.
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
            deliveries = (
                _held_deliveries(conn, provider, register)
                if scope == "holdings"
                else _reference_deliveries(conn, provider, register)
            )
            out.extend(
                (scope, *keys[register_id, slug, variant], column, period, count, eras)
                for slug in sorted(deliveries)
                for variant, column, period, count, eras in deliveries[slug]
            )
        return out


# The reader's `Catalog.register_variable_deliveries`, moved from `reg_meta.catalog`
# (package 4.2b) with its holdings branch, reduced to the fields browse stores.

# (variant, column, period_scope, state_count, windows as (valid_from, valid_to))
type _Delivery = tuple[str, str | None, str, int, tuple[tuple[str, str], ...]]


def _reference_deliveries(
    conn: sqlite3.Connection, provider_slug: str, register_slug: str
) -> dict[str, list[_Delivery]]:
    """Per-variable delivery listing for a register (Y-82), keyed by variable slug.

    The column names a variable is delivered under, with NULL columns kept, per
    delivering variant; a NULL-slug variant is not browse-addressable and drops.
    Each delivery carries its DISJOINT windows (Y-104), so the reads are per row.

    Y-93: alias-backed columns join from `variable_alias_window` (whose rows
    REPLACE the base state's claim when the alias IS the state's own column) and
    `variable_alias` (over the variant's states). Unlike the resolver, a window no
    state contains is kept: a browse row names columns rather than promising a
    resolution. Columns fold case-insensitively; spellings with windows pool their
    eras under the one spelling `representative_columns` picks. No step reads the
    row order, so the reads stay unordered.

    simplify: the fuse is one `_merge` per column over one row per state or alias
    window, about 22,684 rows on the largest register (scb/ulf). Gap-and-islands
    SQL (a window function over `MAX(valid_to) OVER (...)`) is the upgrade if a
    register ever makes that slow.

    Every join is LEFT purely to pin the join order to a per-register `SEARCH v
    USING COVERING INDEX idx_variable_slug`; the `IS NOT NULL` predicates restore
    inner-join semantics."""
    state_rows = conn.execute(
        "SELECT v.slug AS slug, rv.slug AS variant, "
        "vs.delivery_column_name AS col, vs.period_scope, "
        "vs.valid_from AS valid_from, vs.valid_to AS valid_to "
        "FROM variable v "
        "JOIN register r ON v.register_id = r.register_id "
        "JOIN provider p ON r.provider_id = p.provider_id "
        "LEFT JOIN variable_state vs ON vs.variable_id = v.variable_id "
        "LEFT JOIN register_variant rv "
        "  ON rv.register_variant_id = vs.register_variant_id "
        "WHERE p.slug = ? AND r.slug = ? AND v.slug IS NOT NULL AND 1 "
        "  AND rv.slug IS NOT NULL ",
        (provider_slug, register_slug),
    ).fetchall()
    window_rows = conn.execute(
        "SELECT v.slug AS slug, rv.slug AS variant, "
        "w.delivery_column_name AS col, "
        "w.valid_from AS valid_from, w.valid_to AS valid_to "
        "FROM variable v "
        "JOIN register r ON v.register_id = r.register_id "
        "JOIN provider p ON r.provider_id = p.provider_id "
        "LEFT JOIN variable_alias_window w ON w.variable_id = v.variable_id "
        "LEFT JOIN register_variant rv "
        "  ON rv.register_variant_id = w.register_variant_id "
        "WHERE p.slug = ? AND r.slug = ? AND v.slug IS NOT NULL AND 1 "
        "  AND rv.slug IS NOT NULL AND w.delivery_column_name IS NOT NULL ",
        (provider_slug, register_slug),
    ).fetchall()
    alias_rows = conn.execute(
        "SELECT v.slug AS slug, rv.slug AS variant, "
        "va.delivery_column_name AS col "
        "FROM variable v "
        "JOIN register r ON v.register_id = r.register_id "
        "JOIN provider p ON r.provider_id = p.provider_id "
        "LEFT JOIN variable_alias va ON va.variable_id = v.variable_id "
        "LEFT JOIN register_variant rv "
        "  ON rv.register_variant_id = va.register_variant_id "
        "WHERE p.slug = ? AND r.slug = ? AND v.slug IS NOT NULL AND 1 "
        "  AND rv.slug IS NOT NULL AND va.delivery_column_name IS NOT NULL",
        (provider_slug, register_slug),
    ).fetchall()

    # (slug, variant) -> {delivery column -> its raw eras}: the state grain first,
    # widened by the alias history below. Raw, because the fold merges eras ACROSS
    # spellings and only then knows what one column's windows are.
    columns: dict[tuple[str, str], dict[str | None, list[tuple[str, str]]]] = {}
    # The spellings the alias history names each column with, per variant.
    aliased: dict[tuple[str, str], set[str]] = {}
    independent: dict[tuple[str, str, str | None], int] = {}
    for r in state_rows:
        if r["period_scope"] == "year_independent":
            independent_key = (r["slug"], r["variant"], r["col"])
            independent[independent_key] = independent.get(independent_key, 0) + 1
            continue
        key = (r["slug"], r["variant"])
        eras = columns.setdefault(key, {}).setdefault(r["col"], [])
        if r["valid_from"] is not None and r["valid_to"] is not None:
            eras.append((r["valid_from"], r["valid_to"]))
    # Spellings that fold together are ONE column, so their eras pool rather than
    # the last row read winning (which would answer off the query plan).
    windows: dict[tuple[str, str], dict[str, list[tuple[str, str]]]] = {}
    for r in window_rows:
        key = (r["slug"], r["variant"])
        aliased.setdefault(key, set()).add(r["col"])
        windows.setdefault(key, {}).setdefault(r["col"].lower(), []).append(
            (r["valid_from"], r["valid_to"])
        )
    for r in alias_rows:
        aliased.setdefault((r["slug"], r["variant"]), set()).add(r["col"])
    # Each folded column listed under its ONE spelling, the windows on it REPLACING
    # the state's claim, and a column the states never named added over the
    # variant's states.
    for key, alias_columns in aliased.items():
        stated = columns.get(key, {})
        if not stated and any(
            (slug, variant) == key for slug, variant, _ in independent
        ):
            continue
        spelled = representative_columns(stated, alias_columns)
        windowed = windows.get(key, {})
        variant_columns = {
            col: eras
            for col, eras in stated.items()
            if col is None or col.lower() not in windowed
        }
        for folded, eras in windowed.items():
            variant_columns[spelled[folded]] = eras
        # Off the STATES alone, so an added alias column can never widen it.
        variant_span = [era for eras in stated.values() for era in eras]
        for column in spelled.values():
            variant_columns.setdefault(column, variant_span)
        columns[key] = variant_columns

    out: dict[str, list[_Delivery]] = {}
    for (slug, variant), variant_columns in sorted(columns.items()):
        # A NULL column sorts first, where Y-82's SQL ORDER BY put it.
        for column, eras in sorted(
            variant_columns.items(), key=lambda kv: (kv[0] is not None, kv[0] or "")
        ):
            out.setdefault(slug, []).append(
                (variant, column, "intervals", len(eras), _merge(eras))
            )
    for slug, variant, column in sorted(
        independent, key=lambda x: (x[0], x[1], x[2] or "")
    ):
        out.setdefault(slug, []).append(
            (
                variant,
                column,
                "year_independent",
                independent[(slug, variant, column)],
                (),
            )
        )
    return out


def _held_deliveries(
    conn: sqlite3.Connection, provider: str, register: str
) -> dict[str, list[_Delivery]]:
    """The register's deliveries in holdings scope: each semantic projection the
    resolver's alias-window participation rule emits, clipped to the physical
    periods `holding_mapping` holds for it (`_fuse_provider_held_deliveries`)."""
    # Function-local: `schema` imports this module for `browse_scopes`.
    from reg_meta_build.derive.schema import scope_predicate

    params = [provider, register]
    register_filter = " AND r.slug IN (?)"
    rows = conn.execute(
        "SELECT v.variable_id, v.slug AS variable_slug, r.slug AS register_slug, "
        "vs.state_id, vs.register_variant_id, rv.slug AS variant, vs.period_scope, "
        "vs.delivery_column_name, vs.valid_from, vs.valid_to "
        "FROM register r JOIN provider p USING(provider_id) "
        "LEFT JOIN variable v USING(register_id) "
        "LEFT JOIN variable_state vs USING(variable_id) "
        "LEFT JOIN register_variant rv ON rv.register_variant_id = vs.register_variant_id "
        "WHERE p.slug = ? AND r.slug IS NOT NULL AND v.slug IS NOT NULL "
        "AND vs.state_id IS NOT NULL AND "
        + scope_predicate("holdings", "variable", "v")
        + register_filter,
        params,
    ).fetchall()
    windows_by_variable = {
        row[0]: _variable_windows(conn, row[0])
        for row in conn.execute(
            "SELECT DISTINCT w.variable_id FROM variable_alias_window w "
            "JOIN variable v USING(variable_id) JOIN register r USING(register_id) "
            "JOIN provider p USING(provider_id) WHERE p.slug = ? AND "
            + scope_predicate("holdings", "variable", "v")
            + register_filter,
            params,
        )
    }
    stated: dict[tuple[int, int], list[str | None]] = {}
    for row in rows:
        stated.setdefault((row["variable_id"], row["register_variant_id"]), []).append(
            row["delivery_column_name"]
        )
    spellings = {
        key: representative_columns(
            columns,
            (w[0] for w in windows_by_variable.get(key[0], {}).get(key[1], [])),
        )
        for key, columns in stated.items()
    }
    physical: dict[tuple[int, int, str, str], list[tuple[str, str]]] = {}
    for row in conn.execute(
        "SELECT hm.variable_id, hm.variant_id, hm.representation_canonical, "
        "ht.scope, hp.lo, hp.hi FROM holding_mapping hm "
        "JOIN variable v USING(variable_id) JOIN register r USING(register_id) "
        "JOIN provider p USING(provider_id) JOIN holding_column hc USING(column_id) "
        "JOIN holding_table ht USING(table_id) LEFT JOIN holding_period hp USING(table_id) "
        "WHERE p.slug = ? AND ht.scope != 'unknown' " + register_filter,
        params,
    ):
        intervals = physical.setdefault(tuple(row[:4]), [])
        if row[4] is not None:
            intervals.append((row[4], row[5]))
    physical = {key: list(_merge(intervals)) for key, intervals in physical.items()}
    groups: dict[tuple[str, str, str, str], list[tuple[str, str] | None]] = {}
    for row in rows:
        variable_id, variant_id = row["variable_id"], row["register_variant_id"]
        variant = _require_state_variant(row["state_id"], variant_id, row["variant"])
        keep_base, windows = _applicable_alias_windows(
            row, windows_by_variable.get(variable_id, {}).get(variant_id, []), None
        )
        projections = [(w[0], w[1], w[2]) for w in windows]
        if keep_base:
            projections.insert(
                0, (row["delivery_column_name"], row["valid_from"], row["valid_to"])
            )
        for column, lo, hi in projections:
            if column is None:
                continue
            column = spellings[(variable_id, variant_id)].get(column.lower(), column)
            key = (variable_id, variant_id, column, row["period_scope"])
            if key not in physical:
                continue
            offered = (
                [None]
                if row["period_scope"] == "year_independent"
                else _merge(
                    [
                        (max(lo, start), min(hi, end))
                        for start, end in physical[key]
                        if max(lo, start) <= min(hi, end)
                    ]
                )
            )
            if offered:
                groups.setdefault(
                    (row["variable_slug"], variant, column, row["period_scope"]), []
                ).extend(offered)
    out: dict[str, list[_Delivery]] = {}
    for (variable, variant, column, period_scope), offered in sorted(groups.items()):
        out.setdefault(variable, []).append(
            (
                variant,
                column,
                period_scope,
                len(offered),
                _merge([window for window in offered if window is not None]),
            )
        )
    return out


def _require_state_variant(state_id: int, variant_id: int, slug: str | None) -> str:
    if slug is None:
        raise RegMetaError(
            exit_code=EXIT_NOT_FOUND,
            code="state_variant_unresolved",
            error_class="query",
            message=f"variable_state {state_id} references register_variant {variant_id} with no slug",
            remediation="Rebuild the reg_meta DB (slug population is incomplete).",
        )
    return slug


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
            "browse_delivery disagrees with its recomputation from the states and "
            "alias history: "
            f"{len(missing):,} missing, {len(surplus):,} surplus row(s), "
            f"{len(windows):,} window set(s) without a delivery; (scope, "
            f"variable_id) {located}"
        )
    else:
        result.ok(
            "browse_delivery equals its recomputation over "
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
