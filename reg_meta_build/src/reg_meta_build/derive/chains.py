"""Succession chains: terminal successors, classification edition chains and
classification families, compiled at the manifest's classification policy year."""

from __future__ import annotations

from typing import TYPE_CHECKING

from reg_meta.db import classification_succession_as_of_year
from reg_meta.fqid import Fqid

from reg_meta_build.derive.states import reader_catalog

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable, Iterator

    from reg_meta_build.validate import ValidationResult

# Each succession table, the FQID of a node from its key columns, and its key arity.
_SUCCESSION: tuple[tuple[str, Callable[..., Fqid], int], ...] = (
    ("register_replaced_by", Fqid.register_fqid, 2),
    ("variable_replaced_by", Fqid.binding_fqid, 3),
    ("classification_replaced_by", Fqid.classification_fqid, 1),
)
_KEYS = {
    2: ("provider", "register"),
    3: ("provider", "register", "variable"),
    1: ("slug",),
}


def _edges(
    conn: sqlite3.Connection, table: str, arity: int, *, active_year: int | None
) -> Iterator[tuple[tuple[str, ...], tuple[str, ...]]]:
    columns = [
        f"{side}_{key}" for side in ("predecessor", "successor") for key in _KEYS[arity]
    ]
    where = (
        ""
        if active_year is None
        else " WHERE effective_year IS NULL OR effective_year <= ?"
    )
    for row in conn.execute(
        f"SELECT {', '.join(columns)} FROM {table}{where} ORDER BY {', '.join(columns)}",
        () if active_year is None else (active_year,),
    ):
        yield tuple(row[:arity]), tuple(row[arity:])


def succession_terminals(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """`(fqid, terminal_fqid)` for every register, variable or classification whose
    active terminal is another node, in FQID order.

    An edge is active when its `effective_year` is unset or at most the manifest's
    `classification_succession_as_of_year`. The walk follows the sole active
    successor until a node has none or several (a split, which is its own
    terminal), as search's classification fold does. Unlike the frozen reader's
    `resolve_terminal_successor`, it never picks one branch of a split and it
    applies the policy year to registers and variables too. A cycle stops the walk
    at its first repeat; `validate_built_db` refuses cycles.
    """
    year = classification_succession_as_of_year(conn)
    rows: list[tuple[str, str]] = []
    for table, fqid, arity in _SUCCESSION:
        successors: dict[tuple[str, ...], list[tuple[str, ...]]] = {}
        for predecessor, successor in _edges(conn, table, arity, active_year=year):
            successors.setdefault(predecessor, []).append(successor)
        for start in successors:
            seen, current = {start}, start
            while len(nxt := successors.get(current, ())) == 1 and nxt[0] not in seen:
                current = nxt[0]
                seen.add(current)
            if current != start:
                rows.append((str(fqid(*start)), str(fqid(*current))))
    return sorted(rows)


def classification_chains(
    conn: sqlite3.Connection,
) -> list[tuple[str, int, str, int | None, int]]:
    """`(anchor_slug, position, slug, effective_year, is_current)`: the reader's
    `classification_chain` of every classification edition, oldest first.

    `effective_year` is the year of the edge by which that edition is superseded on
    the chain; `is_self` is `slug = anchor_slug`, and names join from
    `classification`.
    """
    slugs = [
        slug
        for (slug,) in conn.execute(
            "SELECT slug FROM classification WHERE slug IS NOT NULL ORDER BY slug"
        )
    ]
    factory = conn.row_factory
    try:
        catalog = reader_catalog(conn)
        return [
            (
                anchor,
                position,
                edition.slug,
                edition.effective_year,
                int(edition.is_current),
            )
            for anchor in slugs
            for position, edition in enumerate(
                catalog.classification_chain(Fqid.classification_fqid(anchor))
            )
        ]
    finally:
        conn.row_factory = factory


def classification_families(
    conn: sqlite3.Connection,
) -> list[tuple[str, str, int, str, int | None, int, int]]:
    """`(family_key, family_label, position, slug, effective_year, is_current,
    is_self)`: the reader's one-dimensional classification families in key order,
    each edition as its family lists it."""
    factory = conn.row_factory
    try:
        families = reader_catalog(conn).list_classification_families()
    finally:
        conn.row_factory = factory
    return [
        (
            family.key,
            family.label,
            position,
            edition.slug,
            edition.effective_year,
            int(edition.is_current),
            int(edition.is_self),
        )
        for family in families
        for position, edition in enumerate(family.editions)
    ]


# Each chain table, its recomputation and its columns in key order.
CHAIN_TABLES: dict[str, tuple[Callable[[sqlite3.Connection], list], str]] = {
    "succession_terminal": (succession_terminals, "fqid, terminal_fqid"),
    "classification_chain": (
        classification_chains,
        "anchor_slug, position, slug, effective_year, is_current",
    ),
    "classification_family": (
        classification_families,
        "family_key, family_label, position, slug, effective_year, is_current, is_self",
    ),
}


def derive_chains(conn: sqlite3.Connection) -> None:
    """Replace the chain tables with their recomputation, in key order."""
    for table, (compute, columns) in CHAIN_TABLES.items():
        rows = compute(conn)
        conn.execute(f"DELETE FROM {table}")
        placeholders = ", ".join("?" * len(columns.split(", ")))
        conn.executemany(
            f"INSERT INTO {table} ({columns}) VALUES ({placeholders})", rows
        )


def _cycle_node(
    successors: dict[tuple[str, ...], list[tuple[str, ...]]],
) -> tuple[str, ...] | None:
    """A node on a cycle, or None. Peels nodes that reach no cycle, then walks the
    remainder from its least node until a node repeats."""
    remaining = set(successors) | {s for nxt in successors.values() for s in nxt}
    while sinks := {
        node
        for node in remaining
        if not any(s in remaining for s in successors.get(node, ()))
    }:
        remaining -= sinks
    if not remaining:
        return None
    seen: set[tuple[str, ...]] = set()
    node = min(remaining)
    while node not in seen:
        seen.add(node)
        node = min(s for s in successors[node] if s in remaining)
    return node


def check_chains(
    conn: sqlite3.Connection, result: ValidationResult, tables: set[str]
) -> None:
    """Succession is acyclic and the chain tables equal a recomputation."""
    result.section("[succession chains]")
    cyclic = False
    for table, fqid, arity in _SUCCESSION:
        if table not in tables:
            continue  # its own check reports the missing table
        successors: dict[tuple[str, ...], list[tuple[str, ...]]] = {}
        for predecessor, successor in _edges(conn, table, arity, active_year=None):
            successors.setdefault(predecessor, []).append(successor)
        node = _cycle_node(successors)
        if node is not None:
            cyclic = True
            result.fail(f"{table} has a succession cycle through {fqid(*node)}")
    if not cyclic:
        result.ok("succession tables are acyclic")
    for table, (compute, columns) in CHAIN_TABLES.items():
        if table not in tables:
            continue  # _check_schema_shape already failed
        stored = [
            tuple(row)
            for row in conn.execute(f"SELECT {columns} FROM {table} ORDER BY {columns}")
        ]
        try:
            expected = sorted(compute(conn))
        except (ValueError, TypeError) as exc:
            result.fail(f"{table} cannot be recomputed: {exc}")
            continue
        if stored != expected:
            missing = len(set(expected) - set(stored))
            surplus = len(set(stored) - set(expected))
            result.fail(
                f"{table} disagrees with its recomputation: {missing:,} missing, "
                f"{surplus:,} surplus row(s)"
            )
        else:
            result.ok(f"{table} equals its recomputation ({len(stored):,} rows)")
