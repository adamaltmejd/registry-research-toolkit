"""Shared fixtures for the concept-group search and catalog tests (#322 / #325).

Groups are hand-seeded onto the slugged fixture DB (the shared ``_slugged_db``
factory). The test modules import these under their historical underscore
names so the moved test bodies stay byte-identical.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from _slugged_db import add_state, add_variable, build_slugged_db

if TYPE_CHECKING:
    import sqlite3

# (slug, month value, month label) for the curated month family. The labels
# double as searchable variable names ("Lönesumma <month>").
MONTH_MEMBERS = [
    ("agiinkjan", "01", "januari"),
    ("agiinkfeb", "02", "februari"),
    ("agiinkmar", "03", "mars"),
]


def seeded_conn() -> sqlite3.Connection:
    """scb/lisa (slugged fixture: variable `kon` under variant 10) plus a
    curated month group, and a classification vintage group over sun2000 +
    sun2020. Group label 'Lönesumma per månad' deliberately shares no token
    with the member names' 'Lönesumma <month>' LIKE matches only via the
    'Lönesumma' stem — tests pick query terms to isolate each match path."""
    conn = build_slugged_db()
    conn.execute(
        "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
        "label, source) VALUES (10, 'variable', 1, 'agiink', "
        "'Lönesumma per månad', 'curated')"
    )
    # The group's single 'month' axis lives in concept_group_axis (#819).
    conn.execute(
        "INSERT INTO concept_group_axis (group_id, axis, ordinal, label) "
        "VALUES (10, 'month', 0, 'månad')"
    )
    for i, (slug, month, month_label) in enumerate(MONTH_MEMBERS):
        add_variable(
            conn,
            register_id=1,
            var_id=800 + i,
            name=f"Lönesumma {month_label}",
            slug=slug,
        )
        vid = conn.execute(
            "SELECT variable_id FROM variable WHERE register_id = 1 AND slug = ?",
            (slug,),
        ).fetchone()[0]
        # Whole-variable member (delivery_column NULL) + one facet on the month axis.
        cur = conn.execute(
            "INSERT INTO concept_group_variable "
            "(group_id, variable_id, delivery_column_name) VALUES (10, ?, NULL)",
            (vid,),
        )
        conn.execute(
            "INSERT INTO concept_group_variable_facet "
            "(member_id, axis, value, label) VALUES (?, 'month', ?, ?)",
            (cur.lastrowid, month, month_label),
        )
    # One member delivers under variant 10 so `get schema` has a grouped column.
    add_state(
        conn,
        register_id=1,
        variable_slug="agiinkjan",
        register_variant_id=10,
        valid_from="2018-01-01",
        valid_to="9999-12-31",
        delivery_column_name="AgiInkJan",
    )
    # Classification vintage group (catalog-scoped, register_id NULL).
    conn.execute(
        "INSERT INTO classification (id, short_name, name, slug) "
        "VALUES (50, 'SUN2000', 'Svensk utbildningsnomenklatur', 'sun2000')"
    )
    conn.execute(
        "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
        "label, source) VALUES (12, 'classification', NULL, 'sun', "
        "'Svensk utbildningsnomenklatur', 'token')"
    )
    conn.execute(
        "INSERT INTO concept_group_axis (group_id, axis, ordinal, label) "
        "VALUES (12, 'vintage', 0, 'vintage')"
    )
    sun2020_id = conn.execute(
        "SELECT id FROM classification WHERE slug = 'sun2020'"
    ).fetchone()[0]
    conn.executemany(
        "INSERT INTO concept_group_classification (classification_id, group_id, "
        "facet_value, facet_label) VALUES (?, 12, ?, ?)",
        [(50, "2000", "2000"), (sun2020_id, "2020", "2020")],
    )
    conn.commit()
    return conn
