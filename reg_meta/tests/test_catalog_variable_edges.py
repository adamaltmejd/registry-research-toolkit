"""Catalog same_as traversal, variable_identity and the variable edge
accessors (successors, predecessors, lineage)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import catalog_test_support
import pytest
from _slugged_db import (
    add_binding,
    add_register,
    add_state,
    add_variable,
    add_variant,
    add_version,
    build_slugged_db,
)
from catalog_test_support import KON as _KON
from reg_meta.catalog import (
    Catalog,
    ResolvedClassification,
    ResolvedVariable,
)
from reg_meta.errors import RegMetaError

if TYPE_CHECKING:
    import sqlite3

# The shared fixture, bound by assignment: an imported name used only as a
# test parameter reads as an unused import redefined (ruff F401/F811).
slugged_conn = catalog_test_support.slugged_conn


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

    def test_same_as_no_match_still_raises(self) -> None:
        # An equivalence edge whose other end doesn't exist either — the
        # traversal exhausts and we still raise fqid_not_found.
        conn = build_slugged_db()
        self._add_var_edge(
            conn,
            a=("scb", "lisa", "phantom-a"),
            b=("scb", "lisa", "phantom-b"),
        )
        with pytest.raises(RegMetaError) as exc:
            Catalog(conn).resolve("scb/lisa/phantom-a")
        assert exc.value.code == "fqid_not_found"

    # A2.1.5 (see DESIGN.md → Composite registers and source tracking): variable same_as is variable-grain — edges carry no
    # variant/period narrowing, so the former `test_same_as_variant_narrowing`
    # and `test_visited_key_separates_variant_scopes` (which exercised
    # variant-scoped edges + a variant-keyed visited set) no longer have a
    # behaviour to test and were removed with the demotion.

    def test_cross_register_same_as_resolves_target_variable(self) -> None:
        # lisa/phantom ≡ rtb/kon, RTB's variant is 'personer' (not lisa's
        # 'individer-15plus'). A2.5: variable identity is variant-independent
        # (the slug is the natural key), so the cross-register target resolves
        # regardless of which variant the query inherited. The resolved variable
        # is RTB's kon — `via_same_as` carries the traversal breadcrumb.
        conn = build_slugged_db()  # lisa / individer-15plus / 2018 / kon
        add_register(conn, register_id=2, slug="rtb", name="RTB")
        add_variant(
            conn, register_variant_id=20, register_id=2, slug="personer", name="P"
        )
        add_version(conn, regver_id=200, register_variant_id=20, name="RTB 2018")
        add_variable(conn, register_id=2, var_id=99, name="Kön", slug="kon")
        add_binding(
            conn,
            cvid=5001,
            register_id=2,
            register_variant_id=20,
            regver_id=200,
            var_id=99,
            delivery_column_name="Kon",
        )
        self._add_var_edge(conn, a=("scb", "lisa", "phantom"), b=("scb", "rtb", "kon"))
        r = Catalog(conn).resolve("scb/lisa/phantom")
        assert isinstance(r, ResolvedVariable)
        # Resolved to RTB's kon variable (register_id 2), under its own variant.
        assert r.register_id == 2
        assert r.states[0].variant == "personer"
        assert r.via_same_as is not None


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

    def test_direct_hit_leaves_via_same_as_none(self) -> None:
        # Sanity: a direct hit doesn't touch the same_as graph.
        conn = build_slugged_db()
        r = Catalog(conn).resolve("class/sun2020")
        assert isinstance(r, ResolvedClassification)
        assert r.via_same_as is None

    def test_same_as_present_slug_is_direct_hit(self) -> None:
        # A2.6.1: each version-baked slug is its own row, globally UNIQUE.
        # Querying a slug that HAS a row is always a direct hit — the same_as
        # graph is only consulted on a direct miss, so it's never touched here.
        conn = build_slugged_db()
        conn.execute(
            "INSERT INTO classification (short_name, name, slug) "
            "VALUES ('LEGACY', 'Legacy SUN', 'sun1996')"
        )
        conn.commit()
        self._add_class_edge(
            conn,
            a=("scb", "sun2020"),
            b=("scb", "sun1996"),
        )
        r = Catalog(conn).resolve("class/sun1996")
        assert isinstance(r, ResolvedClassification)
        assert r.short_name == "LEGACY"
        assert r.via_same_as is None  # direct hit, no traversal

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

    def test_same_as_no_match_still_raises(self) -> None:
        # Edge from a retired slug to a target slug that ALSO has no row →
        # BFS exhausts → not found.
        conn = build_slugged_db()
        self._add_class_edge(
            conn,
            a=("scb", "sun1996-legacy"),
            b=("scb", "ghost-classification"),
        )
        with pytest.raises(RegMetaError) as exc:
            Catalog(conn).resolve("class/sun1996-legacy")
        assert exc.value.code == "fqid_not_found"


class TestVariableIdentity:
    """`variable_identity` — the identity + succession half of the `resolve`
    binding arm, without the state history (and so without its code lists)."""

    def test_direct_hit_matches_resolve(self, slugged_conn: sqlite3.Connection) -> None:
        cat = Catalog(slugged_conn)
        TestEdgeAccessors._seed_replaced_by(slugged_conn, effective_year=2001)
        full = cat.resolve(_KON)
        assert isinstance(full, ResolvedVariable)
        identity = cat.variable_identity(_KON)
        assert str(identity.fqid) == _KON
        assert identity.canonical_fqid == full.canonical_fqid
        assert identity.via_same_as is None
        assert identity.deprecated is full.deprecated is False
        assert identity.replaced_by == full.replaced_by

    def test_deprecated_flag_round_trips(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        slugged_conn.execute("UPDATE variable SET deprecated = 1 WHERE slug = 'kon'")
        slugged_conn.commit()
        assert Catalog(slugged_conn).variable_identity(_KON).deprecated is True

    def test_same_as_alias_reports_target_identity_and_edges(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        # The alias resolves to `kon`: canonical + edges are the TARGET's, the
        # caller's FQID is preserved, and `via_same_as` carries the path — the
        # `resolve` semantics `_check_binding_hints` relies on.
        TestSameAsTraversal._add_var_edge(
            slugged_conn,
            a=("scb", "lisa", "kon"),
            b=("scb", "lisa", "civilstand-legacy"),
        )
        TestEdgeAccessors._seed_replaced_by(slugged_conn)
        cat = Catalog(slugged_conn)
        full = cat.resolve("scb/lisa/civilstand-legacy")
        assert isinstance(full, ResolvedVariable)
        identity = cat.variable_identity("scb/lisa/civilstand-legacy")
        assert str(identity.fqid) == "scb/lisa/civilstand-legacy"
        assert str(identity.canonical_fqid) == _KON
        assert identity.via_same_as == full.via_same_as
        assert identity.replaced_by == full.replaced_by

    def test_unknown_binding_raises_fqid_not_found(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        with pytest.raises(RegMetaError) as exc:
            Catalog(slugged_conn).variable_identity("scb/lisa/nonexistent")
        assert exc.value.code == "fqid_not_found"

    def test_non_binding_fqid_raises_usage(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        with pytest.raises(RegMetaError) as exc:
            Catalog(slugged_conn).variable_identity("class/sun2020")
        assert exc.value.code == "not_a_binding_fqid"


class TestEdgeAccessors:
    """see DESIGN.md → Catalog API surface: predecessors/successors/lineage/lineage_warnings + the
    edges surfaced on ResolvedVariable (variable grain)."""

    @staticmethod
    def _seed_replaced_by(
        conn: sqlite3.Connection,
        *,
        reason: str | None = None,
        effective_year: int | None = None,
    ) -> None:
        # kon (predecessor) → civilstand (successor), variable grain.
        conn.execute(
            "INSERT INTO variable_replaced_by ("
            "predecessor_provider, predecessor_register, predecessor_variable, "
            "successor_provider, successor_register, successor_variable, "
            "effective_year, note, beskrivning) "
            "VALUES ('scb','lisa','kon','scb','lisa','civilstand',?,?,?)",
            (effective_year, "auto:timeseries_event", reason),
        )
        conn.commit()

    def test_successors(self) -> None:
        conn = build_slugged_db()
        self._seed_replaced_by(conn)
        succ = Catalog(conn).successors(_KON)
        assert [(s.provider, s.register_name, s.variable) for s in succ] == [
            ("scb", "lisa", "civilstand")
        ]
        # Outbound edges also ride on ResolvedVariable.replaced_by.
        assert Catalog(conn).resolve(_KON).replaced_by == tuple(succ)

    def test_predecessors_uses_successor_index(self) -> None:
        # Query the SUCCESSOR side: civilstand was preceded by kon. Proves the
        # A2.5 successor-keyed reverse lookup works.
        conn = build_slugged_db(variable_slug="civilstand", delivery_column_name="Civ")
        # Add a kon variable too (the predecessor endpoint must exist to resolve,
        # but predecessors() reads the edge table, not the predecessor variable).
        self._seed_replaced_by(conn)
        pred = Catalog(conn).predecessors("scb/lisa/civilstand")
        assert [(p.provider, p.register_name, p.variable) for p in pred] == [
            ("scb", "lisa", "kon")
        ]

    def test_succession_carries_reason_and_effective_year(self) -> None:
        # #142: predecessors/successors carry beskrivning (reason) + effective_year.
        conn = build_slugged_db()
        self._seed_replaced_by(
            conn, reason="2001 byttes SUN96 till SUN2000", effective_year=2001
        )
        succ = Catalog(conn).successors(_KON)[0]
        assert succ.reason == "2001 byttes SUN96 till SUN2000"
        assert succ.effective_year == 2001

    def test_same_as_on_resolved_variable(self) -> None:
        conn = build_slugged_db()
        for src, tgt in (
            (("scb", "lisa", "kon"), ("scb", "rtb", "kon")),
            (("scb", "rtb", "kon"), ("scb", "lisa", "kon")),
        ):
            conn.execute(
                "INSERT INTO variable_same_as (a_provider,a_register,a_variable,"
                "b_provider,b_register,b_variable) VALUES (?,?,?,?,?,?)",
                (*src, *tgt),
            )
        conn.commit()
        r = Catalog(conn).resolve(_KON)
        assert [(x.provider, x.register_name, x.variable) for x in r.same_as] == [
            ("scb", "rtb", "kon")
        ]
        # same_as refs carry no reason/effective_year (succession-only).
        assert r.same_as[0].reason is None
        assert r.same_as[0].effective_year is None
        # A2.6: the edge endpoint now carries its 3-seg binding FQID (the triple
        # IS the binding FQID once variant/period left the grammar).
        assert str(r.same_as[0].fqid) == "scb/rtb/kon"

    def test_register_bearing_model_dumps_with_wire_alias(self) -> None:
        # #681: the register-bearing models alias the `register_name` Python attr
        # (the `BaseModel.register`-method clash) to the public wire key
        # `register`. With `serialize_by_alias` on the base, a DIRECT `model_dump`
        # / `model_dump_json` (library/CLI path, not just FastAPI's by_alias
        # response path) must emit `register`, never the internal `register_name`.
        conn = build_slugged_db()
        for src, tgt in (
            (("scb", "lisa", "kon"), ("scb", "rtb", "kon")),
            (("scb", "rtb", "kon"), ("scb", "lisa", "kon")),
        ):
            conn.execute(
                "INSERT INTO variable_same_as (a_provider,a_register,a_variable,"
                "b_provider,b_register,b_variable) VALUES (?,?,?,?,?,?)",
                (*src, *tgt),
            )
        conn.commit()
        r = Catalog(conn).resolve(_KON)
        ref = r.same_as[0]
        dumped = ref.model_dump()
        assert dumped["register"] == "rtb"
        assert "register_name" not in dumped
        # The JSON dump carries the alias too (no `register_name` key on the wire).
        ref_json = ref.model_dump_json()
        assert '"register":"rtb"' in ref_json
        assert "register_name" not in ref_json
        # Nested: a parent dump propagates alias serialization into the nested
        # VariableRef under `same_as`, so the wire key is `register` there too.
        nested = r.model_dump()["same_as"][0]
        assert nested["register"] == "rtb"
        assert "register_name" not in nested

    def test_lineage_and_warnings(self) -> None:
        # Seed a consumer→source lineage edge + a warning on the consumer state.
        conn = build_slugged_db()  # kon state_id is the consumer state
        consumer_state = conn.execute(
            "SELECT vs.state_id FROM variable_state vs "
            "JOIN variable v ON vs.variable_id = v.variable_id "
            "WHERE v.register_id = 1 AND v.provider_key = '44'"
        ).fetchone()[0]
        # A source state under a separate source variable.
        add_register(conn, register_id=2, slug="rtb", name="RTB")
        add_variant(
            conn, register_variant_id=20, register_id=2, slug="personer", name="P"
        )
        add_variable(conn, register_id=2, var_id=70, name="Kön", slug="kon")
        source_state = add_state(
            conn,
            register_id=2,
            var_id=70,
            register_variant_id=20,
            valid_from="2018-01-01",
            delivery_column_name="Kon",
        )
        conn.execute(
            "INSERT INTO variable_state_lineage "
            "(consumer_state_id, source_state_id, valid_from, valid_to) "
            "VALUES (?, ?, '2018-01-01', '9999-12-31')",
            (consumer_state, source_state),
        )
        conn.execute(
            "INSERT INTO variable_state_lineage_warning "
            "(consumer_state_id, warning_kind, message) "
            "VALUES (?, 'ambiguous_source_variant', 'two source variants match')",
            (consumer_state,),
        )
        conn.commit()
        cat = Catalog(conn)
        edges = cat.lineage(_KON)
        assert len(edges) == 1
        assert edges[0].consumer_state_id == consumer_state
        assert edges[0].source_state_id == source_state
        assert edges[0].valid_from == "2018-01-01"
        warns = cat.lineage_warnings(_KON)
        assert len(warns) == 1
        assert warns[0].warning_kind == "ambiguous_source_variant"
        # Surfaced on the ResolvedVariable too.
        assert cat.resolve(_KON).lineage == tuple(edges)

    def test_accessors_raise_on_unknown_binding(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        cat = Catalog(slugged_conn)
        bad = "scb/lisa/nonexistent"
        for fn in (
            cat.predecessors,
            cat.successors,
            cat.lineage,
            cat.lineage_warnings,
        ):
            with pytest.raises(RegMetaError) as exc:
                fn(bad)
            assert exc.value.code == "fqid_not_found"

    def test_accessors_resolve_through_same_as(self) -> None:
        # Edge accessors report the TARGET variable's edges when the binding
        # resolves via same_as (consistent with resolve()).
        conn = build_slugged_db()
        for src, tgt in (
            (("scb", "lisa", "kon"), ("scb", "lisa", "phantom")),
            (("scb", "lisa", "phantom"), ("scb", "lisa", "kon")),
        ):
            conn.execute(
                "INSERT INTO variable_same_as (a_provider,a_register,a_variable,"
                "b_provider,b_register,b_variable) VALUES (?,?,?,?,?,?)",
                (*src, *tgt),
            )
        self._seed_replaced_by(conn)  # kon → civilstand
        conn.commit()
        # Querying the phantom slug resolves to kon, so successors() reports
        # kon's outbound edge.
        succ = Catalog(conn).successors("scb/lisa/phantom")
        assert [(s.provider, s.register_name, s.variable) for s in succ] == [
            ("scb", "lisa", "civilstand")
        ]
