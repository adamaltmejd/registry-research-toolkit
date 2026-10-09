"""Succession chains: terminal successors, classification edition chains and
classification families, compiled at the manifest's classification policy year."""

from __future__ import annotations

from graphlib import CycleError, TopologicalSorter
from typing import TYPE_CHECKING

from reg_core_py import parse_fqid

from reg_meta_build.db import classification_succession_as_of_year

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


# The reader's edition walk and one-dimensional families, moved from
# `reg_meta.catalog` (package 4.2b): `Catalog.classification_chain` and
# `Catalog.list_classification_families` with what they call, reduced to the fields
# derive stores. Every anchor is a live `classification` slug, so the reader's FQID
# parse and its `same_as` canonicalization are the identity here and stay behind.

_CLASSIFICATION_FAMILY_LABELS = {
    "icd": "ICD",
    "lkf": "LKF",
    "sni": "SNI",
    "ssyk": "SSYK",
}

# (slug, effective_year, is_current, is_self)
type _Edition = tuple[str, int | None, bool, bool]


def _classification_family_key(slug: str) -> str | None:
    """Stable browse-family key for one-dimensional succession chains."""
    for key in _CLASSIFICATION_FAMILY_LABELS:
        if slug == key or slug.startswith(f"{key}-"):
            return key
        suffix = slug.removeprefix(key)
        if suffix != slug and suffix[:1].isdigit():
            return key
    return None


def _successor_edges(
    conn: sqlite3.Connection, slug: str
) -> list[tuple[str, int | None]]:
    """OUTBOUND succession `(successor_slug, effective_year)`, keyed on the literal
    slug (succession is per edition), in successor order."""
    return [
        (row[0], row[1])
        for row in conn.execute(
            "SELECT successor_slug, effective_year FROM classification_replaced_by "
            "WHERE predecessor_slug = ? ORDER BY successor_slug",
            (slug,),
        )
    ]


def _predecessor_edges(
    conn: sqlite3.Connection, slug: str
) -> list[tuple[str, int | None]]:
    """INBOUND succession `(predecessor_slug, effective_year)`, keyed on the literal
    slug, in predecessor order."""
    return [
        (row[0], row[1])
        for row in conn.execute(
            "SELECT predecessor_slug, effective_year FROM classification_replaced_by "
            "WHERE successor_slug = ? ORDER BY predecessor_slug",
            (slug,),
        )
    ]


def _forward_closure(
    conn: sqlite3.Connection, root: str, seen: set[str]
) -> list[tuple[str, int | None]]:
    """The forward succession closure from `root` (exclusive of `root`) as
    `(slug, effective_year)` in DFS order, each node's successors in slug order, so
    a split fans out into every branch. A node's `effective_year` is its own first
    outbound edge's year (None at a terminal); `seen` is the shared cycle guard."""
    out: list[tuple[str, int | None]] = []
    for slug, _ in _successor_edges(conn, root):
        if slug in seen:
            continue
        seen.add(slug)
        child_succ = _successor_edges(conn, slug)
        out.append((slug, child_succ[0][1] if child_succ else None))
        out.extend(_forward_closure(conn, slug, seen))
    return out


def _edition_started(
    conn: sqlite3.Connection,
    slug: str,
    valid_from: dict[str, int | None],
    as_of_year: int,
) -> bool:
    started = valid_from.get(slug)
    if started is not None:
        return started <= as_of_year
    inbound_years = [
        year for _, year in _predecessor_edges(conn, slug) if year is not None
    ]
    return not inbound_years or min(inbound_years) <= as_of_year


def _classification_chain(
    conn: sqlite3.Connection, canonical: str, as_of_year: int
) -> list[_Edition]:
    """The full edition timeline of `canonical`'s succession chain, oldest first:
    the first-predecessor walk back to the root, reversed, then `canonical`, then
    its forward closure. The order is the walk; `effective_year` is the year of the
    first outbound edge by which an edition is superseded. Every started edition
    with no active outbound edge is current, so a split can have several."""
    seen = {canonical}
    forward = _forward_closure(conn, canonical, seen)
    canonical_succ = _successor_edges(conn, canonical)
    canonical_year = canonical_succ[0][1] if canonical_succ else None
    backward: list[tuple[str, int | None]] = []
    pred = _predecessor_edges(conn, canonical)
    while pred and pred[0][0] not in seen:
        slug, year = pred[0]
        seen.add(slug)
        backward.append((slug, year))
        pred = _predecessor_edges(conn, slug)
    ordered = [*reversed(backward), (canonical, canonical_year), *forward]
    slugs = [slug for slug, _ in ordered]
    valid_from = {
        row[0]: row[1]
        for row in conn.execute(
            "SELECT slug, valid_from FROM classification "
            f"WHERE slug IN ({','.join('?' * len(slugs))})",
            slugs,
        )
    }
    terminals = {
        slug
        for slug in slugs
        if _edition_started(conn, slug, valid_from, as_of_year)
        and not any(
            year is None or year <= as_of_year
            for _, year in _successor_edges(conn, slug)
        )
    }
    return [
        (slug, year, slug in terminals, slug == canonical) for slug, year in ordered
    ]


def classification_chains(
    conn: sqlite3.Connection,
) -> list[tuple[str, int, str, int | None, int]]:
    """`(anchor_slug, position, slug, effective_year, is_current)`: the edition
    chain of every classification edition, oldest first.

    `effective_year` is the year of the edge by which that edition is superseded on
    the chain; `is_self` is `slug = anchor_slug`, and names join from
    `classification`.
    """
    as_of_year = classification_succession_as_of_year(conn)
    slugs = [
        slug
        for (slug,) in conn.execute(
            "SELECT slug FROM classification WHERE slug IS NOT NULL ORDER BY slug"
        )
    ]
    return [
        (anchor, position, slug, year, int(is_current))
        for anchor in slugs
        for position, (slug, year, is_current, _) in enumerate(
            _classification_chain(conn, anchor, as_of_year)
        )
    ]


def _families(
    conn: sqlite3.Connection, as_of_year: int
) -> list[tuple[str, str, list[_Edition]]]:
    """Derived one-dimensional classification succession families (#771): each
    `classification_replaced_by` component with a known family key, its editions
    walked from the component's roots. Multi-dimensional umbrellas such as SUN get
    no key and stay on the curated classification-group surface."""
    parent: dict[str, str] = {}
    successors: set[str] = set()

    def find(slug: str) -> str:
        parent.setdefault(slug, slug)
        while parent[slug] != slug:
            parent[slug] = parent[parent[slug]]
            slug = parent[slug]
        return slug

    def union(left: str, right: str) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    for predecessor, successor in conn.execute(
        "SELECT predecessor_slug, successor_slug FROM classification_replaced_by "
        "ORDER BY predecessor_slug, successor_slug"
    ).fetchall():
        successors.add(successor)
        union(predecessor, successor)

    components: dict[str, set[str]] = {}
    for slug in list(parent):
        components.setdefault(find(slug), set()).add(slug)

    slugs_by_key: dict[str, set[str]] = {}
    for slugs in components.values():
        keys = {
            key
            for slug in slugs
            if (key := _classification_family_key(slug)) is not None
        }
        if keys:
            slugs_by_key.setdefault(min(keys), set()).update(slugs)

    families: list[tuple[str, str, list[_Edition]]] = []
    for key, slugs in sorted(slugs_by_key.items()):
        roots = sorted(slug for slug in slugs if slug not in successors)
        editions: list[_Edition] = []
        seen: set[str] = set()
        for anchor in roots or sorted(slugs):
            for edition in _classification_chain(conn, anchor, as_of_year):
                if edition[0] in seen or _classification_family_key(edition[0]) != key:
                    continue
                seen.add(edition[0])
                editions.append(edition)
        if editions:
            families.append((key, _CLASSIFICATION_FAMILY_LABELS[key], editions))
    return families


def classification_families(
    conn: sqlite3.Connection,
) -> list[tuple[str, str, int, str, int | None, int, int]]:
    """`(family_key, family_label, position, slug, effective_year, is_current,
    is_self)`: the one-dimensional classification families in key order, each
    edition as its family lists it."""
    return [
        (key, label, position, slug, year, int(is_current), int(is_self))
        for key, label, editions in _families(
            conn, classification_succession_as_of_year(conn)
        )
        for position, (slug, year, is_current, is_self) in enumerate(editions)
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
