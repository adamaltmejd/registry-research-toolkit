"""Compiled physical facts and the shared SQL read-scope predicate.

No resolver results are cached here: semantic applicability remains Catalog's job.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from .db import get_manifest
from .errors import EXIT_CONFIG, RegMetaError

if TYPE_CHECKING:
    import sqlite3

ReadScope = Literal["holdings", "reference"]


def resolve_scope(
    conn: sqlite3.Connection, scope: ReadScope | None = None
) -> ReadScope:
    kind = get_manifest(conn).get("catalog_artifact_kind")
    if scope == "holdings" and kind != "steward":
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="scope_unavailable",
            error_class="configuration",
            message="Holdings scope requires a steward artifact; this artifact is a catalog.",
            remediation="Select a steward catalog or use --scope reference.",
        )
    return scope or ("holdings" if kind == "steward" else "reference")


def scope_predicate(
    scope: ReadScope,
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
    if kind == "variable":
        return (
            "EXISTS ("
            + member.replace("SELECT hm.variable_id", "SELECT 1", 1).replace(
                "WHERE ht.scope != 'unknown'",
                f"WHERE hm.variable_id = {alias}.variable_id AND ht.scope != 'unknown'",
                1,
            )
            + ")"
        )
    if kind == "variant":
        return (
            f"{alias}.register_variant_id IN ("
            + member.replace("SELECT hm.variable_id", "SELECT hm.variant_id")
            + ")"
        )
    registers = (
        f"SELECT hv.register_id FROM variable hv WHERE hv.variable_id IN ({member})"
    )
    if kind == "register":
        return f"{alias}.register_id IN ({registers})"
    return (
        f"{alias}.provider_id IN (SELECT hr.provider_id FROM register hr "
        f"WHERE hr.register_id IN ({registers}))"
    )


@dataclass(frozen=True)
class HoldingMatch:
    table_id: int
    table: str
    column: str
    partition: str | None
    period_scope: str
    periods: tuple[tuple[str, str], ...]
    source_ref: str


class Holdings:
    """Indexed physical lookups at authored variable/variant/representation grain."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def binding_ids(self, fqid: str, variant: str) -> tuple[int, int] | None:
        provider, register, variable = fqid.split("/")
        vp, vr, slug = variant.split("/")
        if (provider, register) != (vp, vr):
            return None
        row = self.conn.execute(
            "SELECT v.variable_id, rv.register_variant_id FROM variable v "
            "JOIN register r USING(register_id) JOIN provider p USING(provider_id) "
            "JOIN register_variant rv USING(register_id) "
            "WHERE p.slug = ? AND r.slug = ? AND v.slug = ? AND rv.slug = ?",
            (provider, register, variable, slug),
        ).fetchone()
        return (row[0], row[1]) if row is not None else None

    def representations(
        self, table_id: int, column: str, variable_id: int, variant_id: int
    ) -> set[str]:
        return {
            row[0]
            for row in self.conn.execute(
                "SELECT hm.representation_canonical FROM holding_mapping hm "
                "JOIN holding_column hc USING(column_id) "
                "WHERE hc.table_id = ? AND hc.name = ? AND hm.variable_id = ? AND hm.variant_id = ?",
                (table_id, column, variable_id, variant_id),
            )
        }

    def matches(
        self,
        variable_id: int,
        variant_id: int,
        representation: str | None = None,
        *,
        bounds: tuple[str, str] | None = None,
        period_scope: str | None = None,
    ) -> tuple[HoldingMatch, ...]:
        clauses = ["hm.variable_id = ?", "hm.variant_id = ?", "ht.scope != 'unknown'"]
        params: list[object] = [variable_id, variant_id]
        if representation is not None:
            clauses.append("hm.representation_canonical = ?")
            params.append(representation)
        if period_scope is not None:
            clauses.append("ht.scope = ?")
            params.append(period_scope)
        if bounds is not None:
            clauses.append(
                "EXISTS (SELECT 1 FROM holding_period hp WHERE hp.table_id = ht.table_id "
                "AND hp.lo <= ? AND hp.hi >= ?)"
            )
            params.extend([bounds[1], bounds[0]])
        rows = self.conn.execute(
            "SELECT DISTINCT ht.table_id, ht.physical_id, hc.name, ht.partition, ht.scope, ht.source_ref, hp.lo, hp.hi "
            "FROM holding_mapping hm JOIN holding_column hc USING(column_id) "
            "JOIN holding_table ht USING(table_id) "
            "LEFT JOIN holding_period hp USING(table_id) WHERE "
            + " AND ".join(clauses)
            + " ORDER BY ht.physical_id, hc.name, hp.lo, hp.hi",
            params,
        )
        periods: dict[
            tuple[int, str, str, str | None, str, str], list[tuple[str, str]]
        ] = {}
        for row in rows:
            intervals = periods.setdefault(tuple(row[:6]), [])
            if row[6] is not None:
                intervals.append((row[6], row[7]))
        return tuple(
            HoldingMatch(
                table_id=key[0],
                table=key[1],
                column=key[2],
                partition=key[3],
                period_scope=key[4],
                source_ref=key[5],
                periods=tuple(intervals),
            )
            for key, intervals in periods.items()
        )

    def periods(self, table_id: int) -> tuple[tuple[str, str], ...]:
        """Exact physical periods for a table, independent of a matching request."""
        return tuple(
            (row[0], row[1])
            for row in self.conn.execute(
                "SELECT lo, hi FROM holding_period WHERE table_id = ? ORDER BY lo, hi",
                (table_id,),
            )
        )

    def columns(
        self, variable_id: int, variant_id: int | None = None
    ) -> frozenset[str]:
        return frozenset(
            row[0]
            for row in self.conn.execute(
                "SELECT DISTINCT hm.representation_canonical FROM holding_mapping hm "
                "JOIN holding_column hc USING(column_id) JOIN holding_table ht USING(table_id) "
                "WHERE ht.scope != 'unknown' AND hm.variable_id = ? "
                + ("AND hm.variant_id = ? " if variant_id is not None else ""),
                (variable_id, variant_id) if variant_id is not None else (variable_id,),
            )
        )

    @property
    def admitted_variable_fqids(self) -> frozenset[str]:
        return frozenset(
            row[0]
            for row in self.conn.execute(
                "SELECT p.slug || '/' || r.slug || '/' || v.slug FROM variable v "
                "JOIN register r USING(register_id) JOIN provider p USING(provider_id) WHERE "
                + scope_predicate("holdings", "variable", "v")
            )
        )

    @property
    def held_register_fqids(self) -> frozenset[str]:
        return frozenset(
            row[0]
            for row in self.conn.execute(
                "SELECT p.slug || '/' || r.slug FROM register r "
                "JOIN provider p USING(provider_id) WHERE "
                + scope_predicate("holdings", "register", "r")
            )
        )

    @property
    def held_provider_slugs(self) -> frozenset[str]:
        return frozenset(
            row[0]
            for row in self.conn.execute(
                "SELECT p.slug FROM provider p WHERE "
                + scope_predicate("holdings", "provider", "p")
            )
        )

    def held_columns(self, fqid: str) -> frozenset[str]:
        provider, register, variable = fqid.split("/")
        row = self.conn.execute(
            "SELECT v.variable_id FROM variable v JOIN register r USING(register_id) "
            "JOIN provider p USING(provider_id) WHERE p.slug = ? AND r.slug = ? AND v.slug = ?",
            (provider, register, variable),
        ).fetchone()
        return self.columns(row[0]) if row is not None else frozenset()

    def held_columns_for_variant(self, fqid: str, variant_coord: str) -> frozenset[str]:
        ids = self.binding_ids(fqid, variant_coord)
        return self.columns(*ids) if ids is not None else frozenset()

    def held_variant_coords_for_register(self, register_fqid: str) -> frozenset[str]:
        provider, register = register_fqid.split("/")
        return frozenset(
            row[0]
            for row in self.conn.execute(
                "SELECT p.slug || '/' || r.slug || '/' || rv.slug FROM register_variant rv "
                "JOIN register r USING(register_id) JOIN provider p USING(provider_id) "
                "WHERE p.slug = ? AND r.slug = ? AND "
                + scope_predicate("holdings", "variant", "rv"),
                (provider, register),
            )
        )
