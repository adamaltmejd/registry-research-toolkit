"""Catalog same_as traversal for variables and classifications."""

from __future__ import annotations

from typing import TYPE_CHECKING

from _slugged_db import build_slugged_db
from reg_meta.catalog import (
    Catalog,
    ResolvedClassification,
    ResolvedVariable,
)

if TYPE_CHECKING:
    import sqlite3


class TestSameAsTraversal:
    """See DESIGN.md → Composite registers and source tracking / Canonical vs observed codes: resolver follows curated same_as links transitively when
    direct lookup misses. Traversal path surfaces on `via_same_as` (info, not
    warning per spec)."""

    @staticmethod
    def _add_var_edge(
        conn: sqlite3.Connection,
        *,
        a: tuple[str, str, str],
        b: tuple[str, str, str],
    ) -> None:
        """Insert both directions of a variable-grain same_as edge (see DESIGN.md → Composite registers and source tracking)."""
        for src, tgt in ((a, b), (b, a)):
            conn.execute(
                "INSERT INTO variable_same_as ("
                "a_provider, a_register, a_variable, "
                "b_provider, b_register, b_variable"
                ") VALUES (?, ?, ?, ?, ?, ?)",
                (*src, *tgt),
            )
        conn.commit()

    def test_same_as_one_hop_resolves(self) -> None:
        # Curated equivalence: kon ↔ civilstand-legacy (constructed scenario —
        # the fixture has only `kon`, but querying `civilstand-legacy` traverses
        # the edge and lands on `kon`'s variable).
        conn = build_slugged_db()
        self._add_var_edge(
            conn,
            a=("scb", "lisa", "kon"),
            b=("scb", "lisa", "civilstand-legacy"),
        )
        r = Catalog(conn).resolve("scb/lisa/civilstand-legacy")
        assert isinstance(r, ResolvedVariable)
        # Resolved to the kon variable (provider_key 44, name Kön), but the FQID
        # on the result preserves the caller's input — researchers reading
        # results back match against what they asked for.
        assert r.provider_key == "44"
        assert r.name == "Kön"
        assert str(r.fqid) == "scb/lisa/civilstand-legacy"
        assert r.via_same_as is not None
        assert len(r.via_same_as) == 1
        assert str(r.via_same_as[0]) == "scb/lisa/kon"

    def test_same_as_transitive_two_hops(self) -> None:
        # A → B → C, only C resolves. BFS finds it through B.
        conn = build_slugged_db()
        self._add_var_edge(
            conn,
            a=("scb", "lisa", "kon"),
            b=("scb", "lisa", "intermediate"),
        )
        self._add_var_edge(
            conn,
            a=("scb", "lisa", "intermediate"),
            b=("scb", "lisa", "legacy-name"),
        )
        r = Catalog(conn).resolve("scb/lisa/legacy-name")
        assert isinstance(r, ResolvedVariable)
        assert r.provider_key == "44"
        assert r.via_same_as is not None
        # BFS order: legacy-name → intermediate (no hit) → kon (hit).
        assert len(r.via_same_as) == 2
        path = [str(f) for f in r.via_same_as]
        assert path[-1] == "scb/lisa/kon"

    # A2.1.5 (see DESIGN.md → Composite registers and source tracking): variable same_as is variable-grain — edges carry no
    # variant/period narrowing, so the former `test_same_as_variant_narrowing`
    # and `test_visited_key_separates_variant_scopes` (which exercised
    # variant-scoped edges + a variant-keyed visited set) no longer have a
    # behaviour to test and were removed with the demotion.


class TestSameAsClassificationTraversal:
    """Classification same_as traversal (see DESIGN.md → Classifications)."""

    @staticmethod
    def _add_class_edge(
        conn: sqlite3.Connection,
        *,
        a: tuple[str, str],
        b: tuple[str, str],
    ) -> None:
        for src, tgt in ((a, b), (b, a)):
            conn.execute(
                "INSERT INTO classification_same_as ("
                "a_provider, a_classification_slug, "
                "b_provider, b_classification_slug) VALUES (?, ?, ?, ?)",
                (*src, *tgt),
            )
        conn.commit()

    def test_same_as_traverses_from_retired_slug(self) -> None:
        # The curated-equivalence case: a caller's FQID names a RETIRED slug
        # (`sun1996-legacy`) with no row in this DB. A same_as edge links it to
        # a present slug (`sun2020`), keeping the old FQID resolvable. The BFS
        # seeds the provider from the edge source (the retired slug has no row
        # to read a publisher from) and hops to the present row.
        conn = build_slugged_db()  # fixture seeds 'sun2020'
        self._add_class_edge(
            conn,
            a=("scb", "sun1996-legacy"),
            b=("scb", "sun2020"),
        )
        r = Catalog(conn).resolve("class/sun1996-legacy")
        assert isinstance(r, ResolvedClassification)
        assert r.short_name == "SUN2020"
        assert r.via_same_as is not None
        assert str(r.via_same_as[0]) == "class/sun2020"
        # Caller's (retired) FQID is preserved on the returned record.
        assert str(r.fqid) == "class/sun1996-legacy"
