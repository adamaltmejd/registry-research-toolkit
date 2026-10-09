"""Succession chains: terminal successors, classification edition chains and
classification families, compiled at the manifest's classification policy year."""

from __future__ import annotations

from graphlib import CycleError, TopologicalSorter
from typing import TYPE_CHECKING

from reg_core_py import parse_fqid
from reg_meta.db import classification_succession_as_of_year

from reg_meta_build.derive.states import reader_catalog

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable

    from reg_meta_build.validate import ValidationResult

# Each succession table, the FQID prefix of its nodes, and their key columns.
_SUCCESSION: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("register_replaced_by", "", ("provider", "register")),
    ("variable_replaced_by", "", ("provider", "register", "variable")),
    ("classification_replaced_by", "class/", ("slug",)),
)

_Successors = dict[tuple[str, ...], list[tuple[str, ...]]]


def _fqid(prefix: str, key: tuple[str, ...]) -> str:
    return str(parse_fqid(prefix + "/".join(key)))


def _successors(
    conn: sqlite3.Connection,
    table: str,
    keys: tuple[str, ...],
    *,
    active_year: int | None,
) -> _Successors:
    """Each predecessor's successors in key order; with `active_year`, only edges
    whose `effective_year` is unset or at most that year."""
    columns = ", ".join(
        f"{side}_{key}" for side in ("predecessor", "successor") for key in keys
    )
    where = (
        ""
        if active_year is None
        else " WHERE effective_year IS NULL OR effective_year <= ?"
    )
    successors: _Successors = {}
    for row in conn.execute(
        f"SELECT {columns} FROM {table}{where} ORDER BY {columns}",
        () if active_year is None else (active_year,),
    ):
        successors.setdefault(tuple(row[: len(keys)]), []).append(
            tuple(row[len(keys) :])
        )
    return successors


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
    for table, prefix, keys in _SUCCESSION:
        successors = _successors(conn, table, keys, active_year=year)
        for start in successors:
            seen, current = {start}, start
            while len(nxt := successors.get(current, ())) == 1 and nxt[0] not in seen:
                current = nxt[0]
                seen.add(current)
            if current != start:
                rows.append((_fqid(prefix, start), _fqid(prefix, current)))
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
    with reader_catalog(conn) as catalog:
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
                catalog.classification_chain(f"class/{anchor}")
            )
        ]


def classification_families(
    conn: sqlite3.Connection,
) -> list[tuple[str, str, int, str, int | None, int, int]]:
    """`(family_key, family_label, position, slug, effective_year, is_current,
    is_self)`: the reader's one-dimensional classification families in key order,
    each edition as its family lists it."""
    with reader_catalog(conn) as catalog:
        families = catalog.list_classification_families()
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


def check_chains(
    conn: sqlite3.Connection, result: ValidationResult, tables: set[str]
) -> None:
    """Succession is acyclic and the chain tables equal a recomputation."""
    result.section("[succession chains]")
    cyclic = False
    for table, prefix, keys in _SUCCESSION:
        if table not in tables:
            continue  # its own check reports the missing table
        try:
            TopologicalSorter(
                _successors(conn, table, keys, active_year=None)
            ).prepare()
        except CycleError as exc:
            cyclic = True
            node = exc.args[1][0]
            result.fail(f"{table} has a succession cycle through {_fqid(prefix, node)}")
    if not cyclic:
        result.ok("succession tables are acyclic")
    for table, (compute, columns) in CHAIN_TABLES.items():
        if table not in tables:
            continue  # _check_schema_shape already failed
        stored = set(map(tuple, conn.execute(f"SELECT {columns} FROM {table}")))
        expected = set(compute(conn))
        if stored != expected:
            missing, surplus = len(expected - stored), len(stored - expected)
            result.fail(
                f"{table} disagrees with its recomputation: {missing:,} missing, "
                f"{surplus:,} surplus row(s)"
            )
        else:
            result.ok(f"{table} equals its recomputation ({len(stored):,} rows)")
