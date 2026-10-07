"""Catalog `classification_dimensions` for an aggregate leaf and through an alias."""

from __future__ import annotations

from typing import TYPE_CHECKING

from _slugged_db import build_slugged_db
from reg_meta.catalog import Catalog

if TYPE_CHECKING:
    import sqlite3


class TestClassificationDimensions:
    """#609: `classification_dimensions(fqid)` surfaces the curated umbrella
    group(s) (e.g. `group:sun`) this edition belongs to — the niva ↔ aggregate
    granularity cross-reference, read off the EXISTING
    `concept_group_classification` table."""

    @staticmethod
    def _seed_sun_umbrella(conn: sqlite3.Connection) -> None:
        """A `group:sun` umbrella (dimension axis) over sun2020 (nivå) + two flat
        granularity aggregates (niva-old / niva-grov), mirroring the real curation.
        sun2020 already exists from the fixture; only the two aggregates are minted."""
        for cid, short, slug in (
            (61, "NIVA-OLD", "niva-oldv1"),
            (62, "NIVA-GROV", "niva-grovv1"),
        ):
            conn.execute(
                "INSERT INTO classification (id, short_name, name, slug) "
                "VALUES (?, ?, 'SUN-aggregat', ?)",
                (cid, short, slug),
            )
        sun_id = conn.execute(
            "SELECT id FROM classification WHERE slug = 'sun2020'"
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
            "label, source) VALUES "
            "(40, 'classification', NULL, 'sun', 'Utbildningsnivå', 'curated')"
        )
        conn.execute(
            "INSERT INTO concept_group_axis (group_id, axis, ordinal, label) "
            "VALUES (40, 'dimension', 0, 'dimension')"
        )
        conn.executemany(
            "INSERT INTO concept_group_classification (classification_id, group_id, "
            "facet_value, facet_label) VALUES (?, 40, ?, ?)",
            [
                (sun_id, "niva", "Nivå"),
                (61, "niva-old", "Nivå (7 nivåer)"),
                (62, "niva-grov", "Nivå (5 nivåer)"),
            ],
        )
        conn.commit()

    def test_aggregate_leaf_also_sees_the_group(self) -> None:
        # Browsing an aggregate edition surfaces the same umbrella (the relationship
        # is symmetric — every member sees its siblings).
        conn = build_slugged_db()
        self._seed_sun_umbrella(conn)
        groups = Catalog(conn).classification_dimensions("class/niva-oldv1")
        assert [g.key for g in groups] == ["sun"]

    def test_resolves_through_same_as(self) -> None:
        conn = build_slugged_db()
        self._seed_sun_umbrella(conn)
        for a_slug, b_slug in (("sun-eqf", "sun2020"), ("sun2020", "sun-eqf")):
            conn.execute(
                "INSERT INTO classification_same_as "
                "(a_provider, a_classification_slug, b_provider, b_classification_slug) "
                "VALUES ('scb', ?, 'scb', ?)",
                (a_slug, b_slug),
            )
        conn.commit()
        groups = Catalog(conn).classification_dimensions("class/sun-eqf")
        assert [g.key for g in groups] == ["sun"]
