"""Schema-family aggregates: `get coded-variables` per scope.

Counting a name's distinct codes is an aggregate over every coded state (5.5 s
on the pinned global artifact), so it is compiled once (RUST_RUNTIME_SPEC.md
stage 3b-3e decision 2). `schema`, `diff` and `coverage` read `expanded_state`,
`variable_state` and `browse_delivery` per register instead: a plain join that
measured at most 250 ms for a whole register's history.
"""

from __future__ import annotations

import sqlite3
from itertools import groupby
from typing import TYPE_CHECKING, Any, Literal

from reg_meta_build.derive.browse import browse_scopes
from reg_meta_build.derive.states import named_rows, register_catalog_udfs

if TYPE_CHECKING:
    from reg_meta_build.derive.browse import Scope
    from reg_meta_build.validate import ValidationResult

# (scope, variable_name, n_distinct_codes, n_registers, n_instances)
type CodedRow = tuple[str, str, int, int, int]


# The reader's aggregate and its read-scope predicate, moved from
# `reg_meta.queries` and `reg_meta.holdings` (package 4.2). Derive passes a scope
# `browse_scopes` already admitted, so the reader's `resolve_scope` stays behind.


def scope_predicate(
    scope: Scope,
    kind: Literal["variable", "register", "provider", "variant"],
    alias: str,
    *,
    bounds: tuple[str, str] | None = None,
    variant_sql: str | None = None,
    representation_sql: str | None = None,
    period_scope_sql: str | None = None,
    bounds_sql: tuple[str, str] | None = None,
) -> str:
    """Trusted SQL identifiers only; apply before aggregation and pagination."""
    if scope == "reference":
        return "1"
    # Mapping identity is authored, never inherited through a same_as edge.
    member = (
        "SELECT hm.variable_id FROM holding_mapping hm "
        "JOIN holding_column hc USING(column_id) "
        "JOIN holding_table ht USING(table_id) WHERE ht.scope != 'unknown'"
    )
    if bounds is not None:
        # Bounds are ISO dates produced by the period/year grammar, never SQL input.
        lo, hi = bounds
        if any(
            len(value) != 10 or any(c not in "0123456789-" for c in value)
            for value in bounds
        ):
            raise ValueError("scope bounds must be ISO dates")
        member += (
            " AND ht.scope = 'intervals' AND EXISTS (SELECT 1 FROM holding_period hp "
            f"WHERE hp.table_id = ht.table_id AND hp.lo <= '{hi}' AND hp.hi >= '{lo}')"
        )
    if variant_sql is not None:
        member += f" AND hm.variant_id = {variant_sql}"
    if representation_sql is not None:
        member += f" AND hm.representation_canonical = {representation_sql}"
    if period_scope_sql is not None:
        member += f" AND ht.scope = {period_scope_sql}"
    if bounds_sql is not None:
        lo_sql, hi_sql = bounds_sql
        member += (
            " AND (ht.scope = 'year_independent' OR EXISTS (SELECT 1 FROM holding_period hp "
            f"WHERE hp.table_id = ht.table_id AND hp.lo <= {hi_sql} AND hp.hi >= {lo_sql}))"
        )

    def correlated(source: str, anchor: str) -> str:
        """`member` as an EXISTS reading from `source`, correlated by `anchor`."""
        return (
            "EXISTS ("
            + member.replace(
                "SELECT hm.variable_id FROM holding_mapping hm",
                f"SELECT 1 FROM {source}",
                1,
            ).replace(
                "WHERE ht.scope != 'unknown'",
                f"WHERE {anchor} AND ht.scope != 'unknown'",
                1,
            )
            + ")"
        )

    if kind == "variable":
        return correlated("holding_mapping hm", f"hm.variable_id = {alias}.variable_id")
    if kind == "variant":
        return (
            f"{alias}.register_variant_id IN ("
            + member.replace("SELECT hm.variable_id", "SELECT hm.variant_id")
            + ")"
        )

    # Correlated through idx_variable_natkey(register_id) and
    # idx_holding_mapping_variable_variant: an uncorrelated `IN (...)` list here
    # scans every holding_mapping row per evaluation, and register/provider
    # admission is evaluated once per register in several listings.
    def held_register(register_sql: str) -> str:
        return correlated(
            "variable hv JOIN holding_mapping hm ON hm.variable_id = hv.variable_id",
            f"hv.register_id = {register_sql}",
        )

    if kind == "register":
        return held_register(f"{alias}.register_id")
    return (
        f"EXISTS (SELECT 1 FROM register hr WHERE hr.provider_id = {alias}.provider_id "
        f"AND {held_register('hr.register_id')})"
    )


def get_coded_variables(
    conn: sqlite3.Connection,
    *,
    min_codes: int = 1,
    min_registers: int = 1,
    limit: int = 100,
    scope: Scope,
) -> list[dict[str, Any]]:
    """Find variables that have value sets, ranked by usage.

    Returns a list of dicts with "variable_name", "n_distinct_codes",
    "n_registers", "n_instances". Rows are keyed by the variable's common name;
    variables without a common name are not ranked (search finds them by their
    delivery names).

    A2.7: sourced from `variable_state` (was per-cvid `variable_instance`).
    `n_instances` counts distinct states now — the per-era shape is the unit the
    shipped DB carries.
    """
    if limit == 0:
        return []
    register_catalog_udfs(conn)
    # Counting distinct codes is the expensive part: joining every coded state
    # to its value-set members fans out to ~50M rows on the release catalog
    # (~70 s). So aggregate at state grain first, carrying each name's distinct
    # value sets, and count codes only for the n_registers tiers the ranking
    # reaches. A state counts only when its value set has a member, exactly as
    # the member join used to require.
    rows = conn.execute(
        "WITH coded AS ("
        "SELECT v.name, v.register_id, vs.state_id, coding.value_set_id "
        "FROM variable v "
        "JOIN variable_state vs ON vs.variable_id = v.variable_id "
        "JOIN (SELECT state_id, value_set_id FROM variable_state WHERE value_set_id IS NOT NULL "
        "UNION SELECT vs.state_id, w.value_set_id FROM variable_state vs "
        "JOIN variable_alias_window w ON w.variable_id = vs.variable_id "
        "AND w.register_variant_id = vs.register_variant_id "
        "AND w.valid_from <= vs.valid_to AND w.valid_to >= vs.valid_from "
        "WHERE w.coding_metadata = 'per_column') coding ON coding.state_id = vs.state_id "
        # The ranking key is the common name. A variable without one (its
        # deliveries name it differently) has no key to rank under; grouping its
        # NULL would merge unrelated variables into one nameless row (#1177).
        "WHERE v.name IS NOT NULL AND "
        + scope_predicate(
            scope,
            "variable",
            "v",
            variant_sql="vs.register_variant_id",
            representation_sql="py_catalog_column(v.variable_id, vs.register_variant_id, vs.delivery_column_name)",
            period_scope_sql="vs.period_scope",
            bounds_sql=("vs.valid_from", "vs.valid_to"),
        )
        + " AND EXISTS (SELECT 1 FROM value_set_member vsm "
        "WHERE vsm.value_set_id = coding.value_set_id)) "
        "SELECT name AS variable_name, "
        "COUNT(DISTINCT register_id) AS n_registers, "
        "COUNT(DISTINCT state_id) AS n_instances, "
        "json_group_array(DISTINCT value_set_id) AS value_set_ids "
        "FROM coded GROUP BY name "
        "HAVING n_registers >= ? "
        "ORDER BY n_registers DESC, name",
        (min_registers,),
    ).fetchall()
    ranked: list[dict[str, Any]] = []
    for _, group in groupby(rows, key=lambda r: r["n_registers"]):
        tier = list(group)
        # One query per tier, one value-set list per name, in tier order. The
        # `IN` subqueries dedupe integer code_ids before any code text is
        # compared, which is several times cheaper than COUNT(DISTINCT) over
        # every member row of a name with many overlapping vintages.
        counts = [
            count
            for (count,) in conn.execute(
                "SELECT (SELECT COUNT(DISTINCT vc.code) FROM value_code vc "
                "WHERE vc.code_id IN (SELECT vsm.code_id FROM value_set_member vsm "
                "WHERE vsm.value_set_id IN (SELECT value FROM json_each(t.value)))) "
                "FROM json_each(?) t ORDER BY t.key",
                ("[" + ",".join(r["value_set_ids"] for r in tier) + "]",),
            )
        ]
        found = [
            {
                "variable_name": r["variable_name"],
                "n_distinct_codes": counts[position],
                "n_registers": r["n_registers"],
                "n_instances": r["n_instances"],
            }
            for position, r in enumerate(tier)
            if counts[position] >= min_codes
        ]
        # Stable sort: equal code counts keep the tier query's name order.
        found.sort(key=lambda r: -r["n_distinct_codes"])
        ranked.extend(found)
        if 0 < limit <= len(ranked):
            break
    # A negative limit means no limit, as SQLite's LIMIT read it.
    return ranked if limit < 0 else ranked[:limit]


def coded_variables(conn: sqlite3.Connection, scope: Scope) -> list[CodedRow]:
    """The reader's unfiltered `get coded-variables` under `scope`, by name.

    The reader's default `min_codes` and `min_registers` of 1 drop nothing, so
    the table holds every ranked name; the Rust operation applies no filter.
    """
    with named_rows(conn):
        rows = get_coded_variables(conn, limit=-1, scope=scope)
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
    if "coded_variable_stats" not in tables:
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
    except (sqlite3.Error, ValueError, TypeError, KeyError) as exc:
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
