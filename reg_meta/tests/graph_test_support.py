"""Shared seeders for the relationship-graph tests (#761).

Raw SQL over the shared ``_slugged_db`` fixture DB, used by more than one
``test_graph_*`` module. The modules import these under their historical
underscore names so the moved test bodies stay byte-identical.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import sqlite3

KON = "scb/lisa/kon"


def seed_replaced_by(
    conn: sqlite3.Connection,
    *,
    predecessor: tuple[str, str, str],
    successor: tuple[str, str, str],
    reason: str | None = None,
    effective_year: int | None = None,
) -> None:
    conn.execute(
        "INSERT INTO variable_replaced_by ("
        "predecessor_provider, predecessor_register, predecessor_variable, "
        "successor_provider, successor_register, successor_variable, "
        "effective_year, note, beskrivning) VALUES (?,?,?,?,?,?,?,?,?)",
        (*predecessor, *successor, effective_year, "auto:test", reason),
    )
    conn.commit()


def add_concept_group(
    conn: sqlite3.Connection,
    *,
    group_id: int,
    register_id: int,
    group_key: str,
    member_slugs: list[str],
    facet_axis: str | None = None,
    facets: dict[str, tuple[str, str]] | None = None,
) -> None:
    # `facet_axis` is the group's single axis (None = edge group, facet-less
    # members); when set it lands as the group's one `concept_group_axis` row
    # (#819, multi-axis shape — the inline `concept_group.facet_axis` column is
    # gone). `facets` maps a member slug → (value, label) on that axis; members
    # absent from it (and every member of an axis-less group) get no facet row, so
    # the accessor surfaces empty `member.facets`.
    conn.execute(
        "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
        "label, source) VALUES (?, 'variable', ?, ?, ?, 'curated')",
        (group_id, register_id, group_key, f"Group {group_key}"),
    )
    if facet_axis is not None:
        conn.execute(
            "INSERT INTO concept_group_axis (group_id, axis, ordinal, label) "
            "VALUES (?, ?, 0, ?)",
            (group_id, facet_axis, facet_axis),
        )
    facets = facets or {}
    for slug in member_slugs:
        vid = conn.execute(
            "SELECT variable_id FROM variable WHERE register_id = ? AND slug = ?",
            (register_id, slug),
        ).fetchone()[0]
        cur = conn.execute(
            "INSERT INTO concept_group_variable "
            "(group_id, variable_id, delivery_column_name) VALUES (?, ?, NULL)",
            (group_id, vid),
        )
        if facet_axis is not None and (facet := facets.get(slug)) is not None:
            value, label = facet
            conn.execute(
                "INSERT INTO concept_group_variable_facet "
                "(member_id, axis, value, label) VALUES (?, ?, ?, ?)",
                (cur.lastrowid, facet_axis, value, label),
            )
    conn.commit()
