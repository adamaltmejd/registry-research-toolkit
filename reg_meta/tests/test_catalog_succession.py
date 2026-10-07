"""Catalog classification succession edges and the variable/classification
chains (`classification_chain`, `variable_chain`)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from _slugged_db import (
    add_variable,
    build_slugged_db,
)
from catalog_test_support import KON as _KON
from reg_meta.catalog import (
    Catalog,
    ClassificationDerivedFromRef,
    ResolvedClassification,
    VariableEdition,
)

if TYPE_CHECKING:
    import sqlite3


class TestEdgeAccessors:
    """see DESIGN.md → Catalog API surface: predecessors/successors/lineage/lineage_warnings + the
    edges surfaced on ResolvedVariable (variable grain)."""

    @staticmethod
    def _seed_classification_replaced_by(
        conn: sqlite3.Connection,
        *,
        predecessor: str,
        successor: str,
        effective_year: int | None = None,
        note: str | None = "derived:vintage_chain",
    ) -> None:
        conn.execute(
            "INSERT INTO classification_replaced_by "
            "(predecessor_slug, successor_slug, effective_year, note) "
            "VALUES (?,?,?,?)",
            (predecessor, successor, effective_year, note),
        )
        conn.commit()

    @staticmethod
    def _seed_classification_derived_from(
        conn: sqlite3.Connection,
        *,
        derived: str,
        source: str,
        note: str | None = "variant edge",
    ) -> None:
        conn.execute(
            "INSERT INTO classification_derived_from "
            "(derived_slug, source_slug, note) VALUES (?, ?, ?)",
            (derived, source, note),
        )
        conn.commit()

        # (ResolvedClassification.replaced_by coverage is below — it keys on the
        # resolved edition's OWN slug, so it needs a live row to resolve.)

    def test_classification_derived_from_refs_are_embedded(self) -> None:
        conn = build_slugged_db()
        self._seed_classification(
            conn,
            slug="ks87-p",
            short_name="KS87-P",
            name="Klassifikation av sjukdomar 1987, primärvård",
        )
        self._seed_classification_derived_from(
            conn,
            derived="ks87-p",
            source="sun2020",
            note="Primary-care setting variant",
        )

        resolved = Catalog(conn).resolve("class/ks87-p")

        assert isinstance(resolved, ResolvedClassification)
        assert len(resolved.derived_from) == 1
        ref = resolved.derived_from[0]
        assert isinstance(ref, ClassificationDerivedFromRef)
        assert ref.slug == "sun2020"
        assert str(ref.fqid) == "class/sun2020"
        assert ref.short_name == "SUN2020"
        assert ref.name == "Svensk utbildningsnomenklatur"
        assert ref.note == "Primary-care setting variant"
        assert resolved.derivatives == ()

    @staticmethod
    def _seed_classification(
        conn: sqlite3.Connection,
        *,
        slug: str,
        short_name: str,
        name: str,
        valid_from: int | None = None,
    ) -> None:
        conn.execute(
            "INSERT INTO classification (short_name, name, slug, valid_from) "
            "VALUES (?, ?, ?, ?)",
            (short_name, name, slug, valid_from),
        )
        conn.commit()

    def test_classification_families_derive_known_one_dimensional_successions(
        self,
    ) -> None:
        conn = build_slugged_db(classification=None)
        for slug, short_name in (
            ("ssyk1996", "SSYK1996"),
            ("ssyk2012", "SSYK2012"),
            ("sun1996", "SUN1996"),
            ("sun2020", "SUN2020"),
        ):
            self._seed_classification(conn, slug=slug, short_name=short_name, name=slug)
        self._seed_classification_replaced_by(
            conn, predecessor="ssyk1996", successor="ssyk2012", effective_year=2012
        )
        self._seed_classification_replaced_by(
            conn, predecessor="sun1996", successor="sun2020", effective_year=2020
        )

        families = Catalog(conn).list_classification_families()

        assert [family.key for family in families] == ["ssyk"]
        family = Catalog(conn).classification_family("ssyk")
        assert family is not None
        assert family.label == "SSYK"
        assert [edition.slug for edition in family.editions] == [
            "ssyk1996",
            "ssyk2012",
        ]
        assert [edition.is_current for edition in family.editions] == [False, True]
        assert Catalog(conn).classification_family("sun") is None
        assert Catalog(conn).classification_family_for_fqid("class/ssyk1996") == family
        assert Catalog(conn).classification_family_for_fqid("class/ssyk2012") == family
        assert Catalog(conn).classification_family_for_fqid("class/sun2020") is None

    def test_classification_chain_future_successor_waits_for_as_of_year(self) -> None:
        conn = build_slugged_db(classification=None)
        self._seed_classification(
            conn,
            slug="icd-10-se",
            short_name="ICD-10-SE",
            name="ICD-10-SE",
            valid_from=1997,
        )
        self._seed_classification(
            conn,
            slug="icd-11-se",
            short_name="ICD-11-SE",
            name="ICD-11-SE",
            valid_from=2027,
        )
        self._seed_classification_replaced_by(
            conn,
            predecessor="icd-10-se",
            successor="icd-11-se",
            effective_year=2027,
        )

        chain_2026 = Catalog(conn, classification_as_of_year=2026).classification_chain(
            "class/icd-10-se"
        )
        assert [e.slug for e in chain_2026] == ["icd-10-se", "icd-11-se"]
        assert [e.effective_year for e in chain_2026] == [2027, None]
        assert [e.slug for e in chain_2026 if e.is_current] == ["icd-10-se"]

        chain_2027 = Catalog(conn, classification_as_of_year=2027).classification_chain(
            "class/icd-10-se"
        )
        assert [e.slug for e in chain_2027 if e.is_current] == ["icd-11-se"]

    def test_classification_chain_resolves_same_as_alias_to_canonical(self) -> None:
        # The queried slug is a curated same_as ALIAS for the live sun2020 (no row
        # of its own). The chain anchors on the canonical edition: is_self lands on
        # sun2020 (the resolved live slug), not the alias.
        conn = build_slugged_db()
        for src, tgt in (
            (("scb", "sun-alias"), ("scb", "sun2020")),
            (("scb", "sun2020"), ("scb", "sun-alias")),
        ):
            conn.execute(
                "INSERT INTO classification_same_as ("
                "a_provider, a_classification_slug, "
                "b_provider, b_classification_slug) VALUES (?, ?, ?, ?)",
                (*src, *tgt),
            )
        conn.commit()
        chain = Catalog(conn).classification_chain("class/sun-alias")
        assert [e.slug for e in chain] == ["sun2020"]
        assert chain[0].is_self is True
        assert chain[0].is_current is True

    @staticmethod
    def _seed_var_replaced_by(
        conn: sqlite3.Connection,
        *,
        predecessor: tuple[str, str, str],
        successor: tuple[str, str, str],
        effective_year: int | None = None,
        reason: str | None = None,
    ) -> None:
        conn.execute(
            "INSERT INTO variable_replaced_by ("
            "predecessor_provider, predecessor_register, predecessor_variable, "
            "successor_provider, successor_register, successor_variable, "
            "effective_year, note, beskrivning) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (*predecessor, *successor, effective_year, "auto:timeseries_event", reason),
        )
        conn.commit()

    def test_variable_chain_multi_hop_ordered_oldest_first(self) -> None:
        # kon → anninkf04 → anninkf18 (the live terminal). All three are seeded as
        # LIVE `variable` rows here, so each edition hydrates a name + non-None fqid
        # (the dead-predecessor case — tolerated by design, #355/#411, since unlike
        # classifications no validator forbids it — is covered by the webapp fixture).
        # The chain returns ALL three, oldest first, the terminal last, with
        # is_self/is_current on the queried/terminal edition, and every edition carries
        # its edge's reason.
        conn = build_slugged_db()  # seeds the live scb/lisa/kon
        add_variable(
            conn, register_id=1, var_id=200, name="Annan inkomst 2004", slug="anninkf04"
        )
        add_variable(
            conn, register_id=1, var_id=201, name="Annan inkomst 2018", slug="anninkf18"
        )
        self._seed_var_replaced_by(
            conn,
            predecessor=("scb", "lisa", "kon"),
            successor=("scb", "lisa", "anninkf04"),
            effective_year=2004,
            reason="2004 omdefinierad",
        )
        self._seed_var_replaced_by(
            conn,
            predecessor=("scb", "lisa", "anninkf04"),
            successor=("scb", "lisa", "anninkf18"),
            effective_year=2018,
            reason="2018 ny variabel",
        )
        # Query an intermediate edition — the chain still spans the whole succession
        # and marks the queried variable as is_self.
        chain = Catalog(conn).variable_chain("scb/lisa/anninkf04")
        assert all(isinstance(e, VariableEdition) for e in chain)
        assert [e.variable for e in chain] == ["kon", "anninkf04", "anninkf18"]
        assert [e.effective_year for e in chain] == [2004, 2018, None]
        # reason carried from the edge's beskrivning (terminal is no edge's successor).
        assert [e.reason for e in chain] == [
            "2004 omdefinierad",
            "2018 ny variabel",
            None,
        ]
        assert [e.name for e in chain] == [
            "Kön",
            "Annan inkomst 2004",
            "Annan inkomst 2018",
        ]
        # Every edition is a live row → non-None fqid (no dead-edition shape).
        assert all(e.fqid is not None for e in chain)
        assert [str(e.fqid) for e in chain] == [
            "scb/lisa/kon",
            "scb/lisa/anninkf04",
            "scb/lisa/anninkf18",
        ]
        self_edition = next(e for e in chain if e.is_self)
        assert self_edition.variable == "anninkf04"
        current = next(e for e in chain if e.is_current)
        assert current.variable == "anninkf18"
        assert sum(e.is_current for e in chain) == 1
        assert sum(e.is_self for e in chain) == 1

    def test_variable_chain_resolves_same_as_alias_to_canonical(self) -> None:
        # The queried binding `scb/rtb/kon` has NO live `variable` row of its own;
        # a curated `variable_same_as` edge points it at the live canonical
        # scb/lisa/kon. `_resolve_edge_triple` follows the edge (the direct lookup
        # misses), so the chain anchors on the canonical triple: is_self lands on
        # scb/lisa/kon (the resolved live variable), not the alias.
        conn = build_slugged_db()  # seeds the live scb/lisa/kon
        for src, tgt in (
            (("scb", "rtb", "kon"), ("scb", "lisa", "kon")),
            (("scb", "lisa", "kon"), ("scb", "rtb", "kon")),
        ):
            conn.execute(
                "INSERT INTO variable_same_as (a_provider,a_register,a_variable,"
                "b_provider,b_register,b_variable) VALUES (?,?,?,?,?,?)",
                (*src, *tgt),
            )
        conn.commit()
        chain = Catalog(conn).variable_chain("scb/rtb/kon")
        # Resolves to the canonical scb/lisa/kon (the same_as target).
        assert [(e.register_name, e.variable) for e in chain] == [("lisa", "kon")]
        assert chain[0].is_self is True
        assert chain[0].is_current is True

    # ── #588: order-by-traversal, robust to undated edges + merges/splits ──

    def test_classification_chain_undated_edge_orders_by_traversal(self) -> None:
        # The a→b edge is UNDATED (NULL effective_year), b→c dated 2018. The old
        # sort-by-effective_year inverted on the undated edge (a's None sank below
        # b's 2018); the #588 order-by-traversal walk keeps [a, b, c].
        conn = build_slugged_db()  # seeds the live sun2020
        self._seed_classification(conn, slug="cls-a", short_name="CA", name="C A")
        self._seed_classification(conn, slug="cls-b", short_name="CB", name="C B")
        self._seed_classification(conn, slug="cls-c", short_name="CC", name="C C")
        self._seed_classification_replaced_by(
            conn, predecessor="cls-a", successor="cls-b", effective_year=None
        )
        self._seed_classification_replaced_by(
            conn, predecessor="cls-b", successor="cls-c", effective_year=2018
        )
        chain = Catalog(conn).classification_chain("class/cls-a")
        assert [e.slug for e in chain] == ["cls-a", "cls-b", "cls-c"]
        # effective_year is display-only now: cls-a undated, cls-b 2018, terminal None.
        assert [e.effective_year for e in chain] == [None, 2018, None]
        assert chain[0].is_self is True
        assert chain[-1].is_current is True

    def test_classification_chain_merge_excludes_sibling_branch(self) -> None:
        # mrg-a→mrg-c and mrg-b→mrg-c: mrg-c is a merge. Querying mrg-a returns its
        # OWN path [mrg-a, mrg-c], NOT the sibling mrg-b (a different inbound branch);
        # querying mrg-b returns [mrg-b, mrg-c]. The old collect-all-from-terminal
        # walk wrongly rendered mrg-b when browsing mrg-a.
        conn = build_slugged_db()
        for slug in ("mrg-a", "mrg-b", "mrg-c"):
            self._seed_classification(
                conn, slug=slug, short_name=slug.upper(), name=slug
            )
        self._seed_classification_replaced_by(
            conn, predecessor="mrg-a", successor="mrg-c", effective_year=2010
        )
        self._seed_classification_replaced_by(
            conn, predecessor="mrg-b", successor="mrg-c", effective_year=2010
        )
        chain_a = Catalog(conn).classification_chain("class/mrg-a")
        assert [e.slug for e in chain_a] == ["mrg-a", "mrg-c"]
        chain_b = Catalog(conn).classification_chain("class/mrg-b")
        assert [e.slug for e in chain_b] == ["mrg-b", "mrg-c"]

    def test_variable_chain_undated_edge_orders_by_traversal(self) -> None:
        # kon → uA (UNDATED) → uB (2018). Order-by-traversal keeps [kon, uA, uB]
        # despite kon's outbound edge being undated (the old year-sort inverted it).
        conn = build_slugged_db()
        add_variable(conn, register_id=1, var_id=300, name="U A", slug="uA")
        add_variable(conn, register_id=1, var_id=301, name="U B", slug="uB")
        self._seed_var_replaced_by(
            conn,
            predecessor=("scb", "lisa", "kon"),
            successor=("scb", "lisa", "uA"),
            effective_year=None,
        )
        self._seed_var_replaced_by(
            conn,
            predecessor=("scb", "lisa", "uA"),
            successor=("scb", "lisa", "uB"),
            effective_year=2018,
        )
        chain = Catalog(conn).variable_chain("scb/lisa/kon")
        assert [e.variable for e in chain] == ["kon", "uA", "uB"]
        assert [e.effective_year for e in chain] == [None, 2018, None]
        assert chain[0].is_self is True
        assert chain[-1].is_current is True

    def test_variable_chain_split_follows_deterministic_first(self) -> None:
        # kon → aaa and kon → bbb: kon is a split. chain(kon) follows the
        # deterministic-first successor ([0] of ORDER BY successor triple, aaa < bbb),
        # so [kon, aaa]. Each branch tip resolves its own walk.
        conn = build_slugged_db()
        add_variable(conn, register_id=1, var_id=500, name="AAA", slug="aaa")
        add_variable(conn, register_id=1, var_id=501, name="BBB", slug="bbb")
        self._seed_var_replaced_by(
            conn,
            predecessor=("scb", "lisa", "kon"),
            successor=("scb", "lisa", "aaa"),
            effective_year=2020,
        )
        self._seed_var_replaced_by(
            conn,
            predecessor=("scb", "lisa", "kon"),
            successor=("scb", "lisa", "bbb"),
            effective_year=2020,
        )
        chain_kon = Catalog(conn).variable_chain(_KON)
        assert [e.variable for e in chain_kon] == ["kon", "aaa"]
        # bbb's backward walk reaches kon; kon's det-first successor is aaa, so the
        # forward walk goes kon→aaa — bbb is reached only as is_self. Its own path is
        # [kon, bbb] (backward to kon, no forward edge off bbb).
        chain_bbb = Catalog(conn).variable_chain("scb/lisa/bbb")
        assert [e.variable for e in chain_bbb] == ["kon", "bbb"]
        assert next(e for e in chain_bbb if e.is_self).variable == "bbb"

    def test_variable_chain_includes_dead_renamed_predecessor(self) -> None:
        # #582 / #355 / #411: a variable legitimately tolerates a DEAD/renamed
        # predecessor — a `variable_replaced_by` edge `dead-old → kon` where the
        # predecessor `dead-old` has NO live `variable` row (no validator forbids it,
        # UNLIKE classifications). Querying the LIVE current edition (`kon`) anchors
        # the walk there, then the backward walk steps onto the dead predecessor. It
        # renders in the chain with a syntactically-valid binding fqid (so a citation
        # 301-redirects to the current edition) but `name` None (no live row to read).
        conn = build_slugged_db()  # seeds the live scb/lisa/kon
        # NOTE: dead-old is intentionally NOT add_variable'd — the edge alone exists.
        self._seed_var_replaced_by(
            conn,
            predecessor=("scb", "lisa", "dead-old"),
            successor=("scb", "lisa", "kon"),
            effective_year=2015,
            reason="2015 omdöpt",
        )
        chain = Catalog(conn).variable_chain(_KON)
        assert [e.variable for e in chain] == ["dead-old", "kon"]
        dead, current = chain
        # Dead predecessor: valid (redirecting) binding fqid, but no name (dead row).
        assert str(dead.fqid) == "scb/lisa/dead-old"
        assert dead.name is None
        assert dead.is_self is False
        assert dead.is_current is False
        assert dead.effective_year == 2015  # carried from its OUTBOUND edge
        assert dead.reason == "2015 omdöpt"
        # The queried live edition is both is_self and is_current (terminal).
        assert current.variable == "kon"
        assert current.name == "Kön"
        assert current.is_self is True
        assert current.is_current is True
