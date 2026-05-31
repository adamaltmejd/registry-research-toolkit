"""Tests for Catalog.resolve() (REFACTOR_SPEC.md §5.8)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _slugged_db import (
    add_binding,
    add_register,
    add_state,
    add_value_set,
    add_variable,
    add_variant,
    add_version,
    build_slugged_db,
)
from reg_meta.catalog import (
    Catalog,
    RelatedRef,
    ResolvedClassification,
    ResolvedProvider,
    ResolvedRegister,
    ResolvedVariable,
)
from reg_meta.errors import RegMetaError
from reg_meta.fqid import Fqid, FqidError

if TYPE_CHECKING:
    import sqlite3


@pytest.fixture
def slugged_conn() -> sqlite3.Connection:
    return build_slugged_db()


class TestResolveProvider:
    def test_resolves_known_provider(self, slugged_conn: sqlite3.Connection) -> None:
        r = Catalog(slugged_conn).resolve("scb")
        assert isinstance(r, ResolvedProvider)
        assert r.name == "Statistics Sweden"
        assert r.fqid.provider == "scb"
        assert str(r.fqid) == "scb"

    def test_unknown_provider_raises_not_found(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        with pytest.raises(RegMetaError) as exc:
            Catalog(slugged_conn).resolve("nope")
        assert exc.value.code == "fqid_not_found"


class TestResolveRegister:
    def test_resolves(self, slugged_conn: sqlite3.Connection) -> None:
        r = Catalog(slugged_conn).resolve("scb/lisa")
        assert isinstance(r, ResolvedRegister)
        assert r.register_id == 1
        assert r.fqid.provider == "scb"
        assert r.name == "LISA"

    def test_wrong_provider_misses(self, slugged_conn: sqlite3.Connection) -> None:
        with pytest.raises(RegMetaError) as exc:
            Catalog(slugged_conn).resolve("sos/lisa")
        assert exc.value.code == "fqid_not_found"


class TestVariantAndVersionKindsGone:
    """A2.6: the variant and register_version FQID kinds were removed (§5.2). A
    3-segment string is a binding now; a 4-segment string doesn't parse. (The
    `_default` variant + its synthesis live on as a `resolve_at` coordinate, not
    an addressable FQID — see TestResolveAt.)"""

    def test_three_seg_is_a_binding_not_a_variant(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        # `scb/lisa/individer-15plus` was the variant FQID; it is now a binding
        # whose variable slug is `individer-15plus` (which doesn't exist here).
        with pytest.raises(RegMetaError) as exc:
            Catalog(slugged_conn).resolve("scb/lisa/individer-15plus")
        assert exc.value.code == "fqid_not_found"

    def test_old_four_seg_version_fqid_rejected(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        with pytest.raises(FqidError, match="4 segments"):
            Catalog(slugged_conn).resolve("scb/lisa/individer-15plus/2018")

    def test_old_five_seg_binding_fqid_rejected(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        with pytest.raises(FqidError, match="5 segments"):
            Catalog(slugged_conn).resolve("scb/lisa/individer-15plus/2018/kon")


class TestResolveBinding:
    """A2.5 (§5.10): `resolve()` returns the longitudinal `ResolvedVariable` —
    the variable's shared metadata + its `variable_state` history, no per-edition
    cvid. The interim `ResolvedVariableBinding` and the `editions()` path that
    returned it were removed in A2.6."""

    def test_resolves(self, slugged_conn: sqlite3.Connection) -> None:
        # Kolumnnamn "Kon" derives to variable slug "kon".
        r = Catalog(slugged_conn).resolve("scb/lisa/kon")
        assert isinstance(r, ResolvedVariable)
        assert r.provider_key == "44"
        assert r.name == "Kön"
        assert r.fqid.variable == "kon"
        # The shared shape exposes state-grain delivery column through states.
        assert len(r.states) == 1
        assert r.states[0].delivery_column_name == "Kon"
        assert r.states[0].variant == "individer-15plus"

    def test_swedish_kolumnnamn_folds_to_ascii_slug(self) -> None:
        # "Kön" → "kon" via NFKD ASCII fold; binding FQIDs are ASCII (§5.2).
        # The raw delivery column is preserved on the state.
        conn = build_slugged_db(delivery_column_name="Kön")
        r = Catalog(conn).resolve("scb/lisa/kon")
        assert isinstance(r, ResolvedVariable)
        assert r.states[0].delivery_column_name == "Kön"

    def test_unknown_variable_misses(self, slugged_conn: sqlite3.Connection) -> None:
        with pytest.raises(RegMetaError) as exc:
            Catalog(slugged_conn).resolve("scb/lisa/nonexistent")
        assert exc.value.code == "fqid_not_found"

    def test_default_fixture_has_no_edges(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        # A bare variable with no curated/auto edges exposes empty edge tuples.
        r = Catalog(slugged_conn).resolve("scb/lisa/kon")
        assert isinstance(r, ResolvedVariable)
        assert r.same_as == ()
        assert r.replaced_by == ()
        assert r.related_to == ()
        assert r.lineage == ()
        assert r.via_same_as is None


class TestStoredVariableSlug:
    """A2.1.5 (§5.3): the resolver reads the stored `variable.slug`, not a slug
    derived from `delivery_column_name` at query time. A2.5: resolution is now
    longitudinal (`ResolvedVariable`, period-independent) — split siblings
    resolve to distinct `variable_id`s, and point selection moved to
    `resolve_at`."""

    def test_resolves_via_stored_slug(self) -> None:
        # Stored slug == derived slug for the common single-column case.
        conn = build_slugged_db()
        r = Catalog(conn).resolve("scb/lisa/kon")
        assert isinstance(r, ResolvedVariable)
        assert r.provider_key == "44"

    def test_two_aliases_one_slug_still_resolves(self) -> None:
        # A variable with two aliases (`Kon` + `Kön`) both folding to one slug:
        # the stored slug is single, so the variable resolves unambiguously.
        # A2.7: `variable_alias` is variable_id-keyed.
        conn = build_slugged_db()
        conn.execute(
            "INSERT INTO variable_alias (variable_id, register_variant_id, delivery_column_name) "
            "SELECT variable_id, 10, 'Kön' FROM variable WHERE slug = 'kon'"
        )
        conn.commit()
        r = Catalog(conn).resolve("scb/lisa/kon")
        assert isinstance(r, ResolvedVariable)
        assert r.provider_key == "44"

    def test_stored_slug_overrides_derived_unblocks_triage(self) -> None:
        # The A2.2 unblocking proof: delivery_column_name is `Ssyk` (which would
        # derive to `ssyk`), but the stored slug is `ssyk-3pos`. The variable
        # resolves under the stored slug — proving a build-time triage split can
        # give a sibling sharing a delivery column a distinct, resolvable
        # identity even though derive-at-resolve never produces it.
        conn = build_slugged_db(delivery_column_name="Ssyk", variable_slug="ssyk-3pos")
        r = Catalog(conn).resolve("scb/lisa/ssyk-3pos")
        assert isinstance(r, ResolvedVariable)
        assert r.provider_key == "44"
        assert r.states[0].delivery_column_name == "Ssyk"
        # The derive-at-resolve slug `ssyk` no longer resolves — identity is the
        # stored slug, not the (honest, shared) delivery column.
        with pytest.raises(RegMetaError) as exc:
            Catalog(conn).resolve("scb/lisa/ssyk")
        assert exc.value.code == "fqid_not_found"

    def test_split_siblings_resolve_to_distinct_variables(self) -> None:
        # A2.2 split → A2.5 longitudinal: two sibling variables share provider_key
        # '44' (a §5.7 split puts several variables under one source key) but have
        # distinct slugs + distinct `variable_id`s and own DISJOINT delivery
        # columns. Each resolves to its OWN `ResolvedVariable` with its own state
        # — no shared cvid fan-out (the interim hazard the A2.2 flip removed).
        conn = build_slugged_db(delivery_column_name="Ssyk3", variable_slug="ssyk-3pos")
        conn.execute(
            "INSERT INTO variable (register_id, provider_key, name, slug) "
            "VALUES (1, '44', 'SSYK 5-pos', 'ssyk-5pos')"
        )
        # The Ssyk5 sibling's own state under the same variant. Target by slug:
        # provider_key '44' is shared across the split siblings, so var_id can't
        # disambiguate — the register-unique slug can.
        add_state(
            conn,
            register_id=1,
            variable_slug="ssyk-5pos",
            register_variant_id=10,
            valid_from="2018-01-01",
            delivery_column_name="Ssyk5",
        )
        conn.commit()
        r3 = Catalog(conn).resolve("scb/lisa/ssyk-3pos")
        r5 = Catalog(conn).resolve("scb/lisa/ssyk-5pos")
        assert isinstance(r3, ResolvedVariable)
        assert isinstance(r5, ResolvedVariable)
        # Distinct variables (distinct variable_id), each with its own column.
        assert r3.variable_id != r5.variable_id
        assert [s.delivery_column_name for s in r3.states] == ["Ssyk3"]
        assert [s.delivery_column_name for s in r5.states] == ["Ssyk5"]

    def test_absent_sibling_still_misses(self) -> None:
        # A slug that names no variable misses, even when a same-provider_key
        # sibling exists. (Longitudinal resolution keys on the stored slug, so a
        # nonexistent slug can't borrow a sibling's identity.)
        conn = build_slugged_db(delivery_column_name="Ssyk3", variable_slug="ssyk-3pos")
        r3 = Catalog(conn).resolve("scb/lisa/ssyk-3pos")
        assert isinstance(r3, ResolvedVariable)
        with pytest.raises(RegMetaError) as exc:
            Catalog(conn).resolve("scb/lisa/ssyk-7pos")
        assert exc.value.code == "fqid_not_found"


class TestResolveBindingLineage:
    """§5.6 consumer-side lineage exposure on the longitudinal resolution
    (A2.5). Lineage is the `variable_state_lineage` table (A2.4, state grain),
    surfaced via `resolve(fqid).lineage` and `lineage(fqid)` — NOT the deleted
    interim per-cvid `via_source_id` FQID."""

    @staticmethod
    def _build_consumer_db() -> sqlite3.Connection:
        # RTB owns Kön (the source variable); LISA delivers it as a consumer-side
        # variable. A `variable_state_lineage` edge ties LISA's state to RTB's.
        conn = build_slugged_db(
            register=("RTB", "rtb", 1, 1),
            variant=("Personer", "personer", 10),
            version=("RTB 2018", "2018", 100),
            variable=("Kön", 44, 5000, "Kon"),
        )
        # Consumer register (LISA) with its own variable + state.
        add_register(conn, register_id=2, slug="lisa", name="LISA")
        add_variant(
            conn,
            register_variant_id=20,
            register_id=2,
            slug="individer-15plus",
            name="Individer 15+",
        )
        add_version(conn, regver_id=200, register_variant_id=20, name="LISA 2018")
        add_variable(
            conn, register_id=2, var_id=99, name="Kön", source_register_id=1, slug="kon"
        )
        consumer_state = add_state(
            conn,
            register_id=2,
            var_id=99,
            register_variant_id=20,
            valid_from="2018-01-01",
            delivery_column_name="Kon",
        )
        # RTB's source state (the fixture's variable() seeded one at 2018-01-01).
        source_state = conn.execute(
            "SELECT vs.state_id FROM variable_state vs "
            "JOIN variable v ON vs.variable_id = v.variable_id "
            "WHERE v.register_id = 1 AND v.provider_key = '44'"
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO variable_state_lineage "
            "(consumer_state_id, source_state_id, valid_from, valid_to) "
            "VALUES (?, ?, '2018-01-01', '9999-12-31')",
            (consumer_state, source_state),
        )
        conn.commit()
        return conn

    def test_consumer_resolve_exposes_lineage_edge(self) -> None:
        conn = self._build_consumer_db()
        r = Catalog(conn).resolve("scb/lisa/kon")
        assert isinstance(r, ResolvedVariable)
        assert len(r.lineage) == 1
        edge = r.lineage[0]
        assert edge.valid_from == "2018-01-01"
        assert edge.valid_to == "9999-12-31"
        assert edge.consumer_state_id == r.states[0].state_id

    def test_lineage_accessor_matches_resolve(self) -> None:
        conn = self._build_consumer_db()
        cat = Catalog(conn)
        fqid = "scb/lisa/kon"
        assert cat.lineage(fqid) == list(cat.resolve(fqid).lineage)

    def test_canonical_source_has_no_lineage(self) -> None:
        # RTB's source variable is the lineage SOURCE, not a consumer — its own
        # `lineage` (consumer-side) is empty.
        conn = self._build_consumer_db()
        r = Catalog(conn).resolve("scb/rtb/kon")
        assert isinstance(r, ResolvedVariable)
        assert r.lineage == ()


# A2.6: the variant/period left the FQID grammar, so the elided-variant
# parse (TestResolveElidedFqid), the curated version-slot resolution
# (TestResolveVersionWithCuratedSlug), and the per-edition discovery path
# Catalog.editions (TestEditions) are all gone — the resolver reads
# variable.slug + variable_state, period is a resolve_at coordinate, and a
# variant is a register sub-resource without a slash-path FQID (§5.2).


class TestResolveClassification:
    def test_resolves(self, slugged_conn: sqlite3.Connection) -> None:
        # A2.6.1: 2-seg FQID; the slug bakes in the vintage.
        r = Catalog(slugged_conn).resolve("class/sun2020")
        assert isinstance(r, ResolvedClassification)
        assert r.classification_id is not None
        assert r.fqid.classification == "sun2020"

    def test_unknown_slug_misses(self, slugged_conn: sqlite3.Connection) -> None:
        with pytest.raises(RegMetaError) as exc:
            Catalog(slugged_conn).resolve("class/sun2099")
        assert exc.value.code == "fqid_not_found"


class TestResolveFqidObject:
    def test_accepts_parsed_fqid_object(self, slugged_conn: sqlite3.Connection) -> None:
        r = Catalog(slugged_conn).resolve(Fqid.register_fqid("scb", "lisa"))
        assert isinstance(r, ResolvedRegister)

    def test_rejects_incomplete_fqid_object(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        # A hand-constructed Fqid with the wrong fields for its kind round-
        # trips to a different kind on emit-then-parse; the resolver must
        # fail fast with FqidError instead of TypeError inside a resolver.
        from reg_meta.fqid import FqidKind

        # Claims to be a binding but carries no `variable`, so emit-then-parse
        # yields a 2-segment REGISTER kind — a mismatch the resolver rejects.
        bad = Fqid(
            kind=FqidKind.VARIABLE_BINDING,
            provider="scb",
            register="lisa",
            variable=None,
        )
        with pytest.raises(FqidError, match="incomplete"):
            Catalog(slugged_conn).resolve(bad)


class TestNullSlugMisses:
    def test_null_register_slug_does_not_resolve(self) -> None:
        # Before 1c populates slugs, register rows have slug = NULL; the
        # resolver must miss rather than match arbitrary NULL rows.
        conn = build_slugged_db(
            register=("LISA", None, 1, 1), variant=None, version=None, variable=None
        )
        with pytest.raises(RegMetaError) as exc:
            Catalog(conn).resolve("scb/lisa")
        assert exc.value.code == "fqid_not_found"


class TestSameAsTraversal:
    """§5.5 / §6.7: resolver follows curated same_as links transitively when
    direct lookup misses. Traversal path surfaces on `via_same_as` (info, not
    warning per spec)."""

    @staticmethod
    def _add_var_edge(
        conn: sqlite3.Connection,
        *,
        a: tuple[str, str, str],
        b: tuple[str, str, str],
    ) -> None:
        """Insert both directions of a variable-grain same_as edge (§5.5)."""
        for src, tgt in ((a, b), (b, a)):
            conn.execute(
                "INSERT INTO variable_same_as ("
                "a_provider, a_register, a_variable, "
                "b_provider, b_register, b_variable"
                ") VALUES (?, ?, ?, ?, ?, ?)",
                (*src, *tgt),
            )
        conn.commit()

    def test_direct_hit_leaves_via_same_as_none(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        # Sanity: a direct resolution doesn't touch the same_as graph.
        r = Catalog(slugged_conn).resolve("scb/lisa/kon")
        assert isinstance(r, ResolvedVariable)
        assert r.via_same_as is None

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

    # A2.1.5 (§5.5): variable same_as is variable-grain — edges carry no
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
    """§5.5 classification same_as traversal."""

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


# ── A2.5 longitudinal resolution + resolve_at + edge accessors (§5.10) ──────

_KON = "scb/lisa/kon"


class TestResolveVariableLongitudinal:
    """§5.10: `resolve()` returns the variable's shared metadata + full state
    history, each state tagged with its variant."""

    def test_resolve_returns_resolved_variable(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        r = Catalog(slugged_conn).resolve(_KON)
        assert isinstance(r, ResolvedVariable)
        assert r.name == "Kön"
        assert r.provider_key == "44"
        assert r.is_sensitive is False
        assert r.is_identifier is False
        assert len(r.states) >= 1
        assert r.states[0].variant == "individer-15plus"

    def test_states_tagged_with_variant(self) -> None:
        # The same variable delivered in two variants → two states, each carrying
        # its own variant coordinate.
        conn = build_slugged_db()
        add_variant(
            conn, register_variant_id=11, register_id=1, slug="foretag", name="Företag"
        )
        add_state(
            conn,
            register_id=1,
            var_id=44,
            register_variant_id=11,
            valid_from="2019-01-01",
            delivery_column_name="Kon",
        )
        r = Catalog(conn).resolve(_KON)
        assert {s.variant for s in r.states} == {"individer-15plus", "foretag"}

    def test_states_chronological_ascending(self) -> None:
        # History reads oldest → newest.
        conn = build_slugged_db()
        add_state(
            conn,
            register_id=1,
            var_id=44,
            register_variant_id=10,
            valid_from="2020-01-01",
            valid_to="2020-12-31",
            delivery_column_name="Kon",
        )
        r = Catalog(conn).resolve(_KON)
        froms = [s.valid_from for s in r.states]
        assert froms == sorted(froms)

    def test_states_accessor_equiv_resolve(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        cat = Catalog(slugged_conn)
        assert cat.states(_KON) == list(cat.resolve(_KON).states)

    def test_states_rejects_non_binding_fqid(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        # states() must fail like its sibling accessors on a non-binding FQID
        # (a register here) — a structured not_a_binding_fqid usage error, not a
        # raw AttributeError off the polymorphic resolve() (the A2.5 review fix).
        with pytest.raises(RegMetaError) as exc:
            Catalog(slugged_conn).states("scb/lisa")
        assert exc.value.code == "not_a_binding_fqid"

    def test_resolve_states_round_trip_with_resolve_at(self) -> None:
        # §5.10 / MIGRATION_PLAN A2.5: the full history via resolve(fqid).states
        # equals the union of per-year resolve_at() results on the unambiguous
        # single-variant case.
        conn = build_slugged_db()
        add_state(
            conn,
            register_id=1,
            var_id=44,
            register_variant_id=10,
            valid_from="2019-01-01",
            valid_to="2019-12-31",
            delivery_column_name="Kon",
        )
        cat = Catalog(conn)
        all_states = set(cat.resolve(_KON).states)
        via_at = {
            s
            for year in (2018, 2019)
            for s in cat.resolve_at(_KON, year, variant="individer-15plus")
        }
        assert via_at == all_states

    def test_value_set_hydrated_on_state(self) -> None:
        # A state carrying a value_set exposes its (code, label) pairs. The 2020
        # state is distinct from the fixture's open-ended 2018 base state, but
        # the base state's open `valid_to` also covers 2020, so query its own
        # year and assert on the value-set-bearing state directly.
        conn = build_slugged_db()
        add_value_set(conn, value_set_id=7, codes=[("1", "Man"), ("2", "Kvinna")])
        add_state(
            conn,
            register_id=1,
            variable_slug="kon",
            register_variant_id=10,
            valid_from="2020-01-01",
            valid_to="2020-12-31",
            delivery_column_name="Kon",
            value_set_id=7,
        )
        states = Catalog(conn).resolve_at(_KON, 2020, variant="individer-15plus")
        coded = [s for s in states if s.value_set_id == 7]
        assert len(coded) == 1
        assert coded[0].value_set == (("1", "Man"), ("2", "Kvinna"))

    def test_state_without_value_set_is_none(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        r = Catalog(slugged_conn).resolve(_KON)
        assert r.states[0].value_set is None


class TestResolveAt:
    """§5.10: `resolve_at` — period/variant/version-narrowed list of states."""

    @staticmethod
    def _two_state_year_db() -> sqlite3.Connection:
        # One variable, two sub-annual states inside calendar 2020 under one
        # variant: spring (Jan-Jun) and autumn (Jul-Dec). Lets sub-annual period
        # tokens prove the year-only limit is lifted. We drop the fixture's
        # auto-seeded open-ended 2018 base state so only these two remain.
        conn = build_slugged_db()
        conn.execute(
            "DELETE FROM variable_state WHERE variable_id = "
            "(SELECT variable_id FROM variable WHERE register_id=1 AND slug='kon')"
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="kon",
            register_variant_id=10,
            valid_from="2020-01-01",
            valid_to="2020-06-30",
            delivery_column_name="KonVT",
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="kon",
            register_variant_id=10,
            valid_from="2020-07-01",
            valid_to="2020-12-31",
            delivery_column_name="KonHT",
        )
        conn.commit()
        return conn

    def test_int_year(self, slugged_conn: sqlite3.Connection) -> None:
        states = Catalog(slugged_conn).resolve_at(
            _KON, 2018, variant="individer-15plus"
        )
        assert len(states) == 1
        assert states[0].variant == "individer-15plus"

    def test_period_token_month(self) -> None:
        conn = self._two_state_year_db()
        states = Catalog(conn).resolve_at(_KON, "2020-08", variant="individer-15plus")
        # Only the autumn state covers August (precise — not year-granular).
        assert [s.delivery_column_name for s in states] == ["KonHT"]

    def test_period_token_quarter(self) -> None:
        conn = self._two_state_year_db()
        # Q1 (Jan-Mar) → spring state only.
        states = Catalog(conn).resolve_at(_KON, "2020-Q1", variant="individer-15plus")
        assert [s.delivery_column_name for s in states] == ["KonVT"]

    def test_period_token_htvt(self) -> None:
        conn = self._two_state_year_db()
        ht = Catalog(conn).resolve_at(_KON, "HT2020", variant="individer-15plus")
        vt = Catalog(conn).resolve_at(_KON, "VT2020", variant="individer-15plus")
        assert [s.delivery_column_name for s in ht] == ["KonHT"]
        assert [s.delivery_column_name for s in vt] == ["KonVT"]

    def test_period_token_iso_date(self) -> None:
        conn = self._two_state_year_db()
        states = Catalog(conn).resolve_at(
            _KON, "2020-03-15", variant="individer-15plus"
        )
        assert [s.delivery_column_name for s in states] == ["KonVT"]

    def test_range_period_crosses_states(self) -> None:
        conn = self._two_state_year_db()
        states = Catalog(conn).resolve_at(
            _KON, {"from": "2020-01-01", "to": "2020-12-31"}, variant="individer-15plus"
        )
        # The range spans both states; chronological ascending.
        assert [s.delivery_column_name for s in states] == ["KonVT", "KonHT"]

    def test_default_sentinel_returns_all(self) -> None:
        conn = self._two_state_year_db()
        states = Catalog(conn).resolve_at(_KON, "_default")
        # No period filter → every state (both sub-annual ones).
        assert len(states) == 2
        assert [s.delivery_column_name for s in states] == ["KonVT", "KonHT"]

    def test_variant_narrows(self) -> None:
        # Two variants deliver the variable at the same year; omitting `variant`
        # returns both, supplying it returns one.
        conn = build_slugged_db()
        add_variant(
            conn, register_variant_id=11, register_id=1, slug="foretag", name="Företag"
        )
        add_state(
            conn,
            register_id=1,
            var_id=44,
            register_variant_id=11,
            valid_from="2018-01-01",
            delivery_column_name="Kon",
        )
        cat = Catalog(conn)
        assert len(cat.resolve_at(_KON, 2018)) == 2
        assert len(cat.resolve_at(_KON, 2018, variant="foretag")) == 1

    def test_value_set_version_narrows_multivintage(self) -> None:
        # §5.7 multi-vintage fold: two overlapping states, same variant + year,
        # distinct value_set_version_label (SNI92 + SNI2007 in a crosswalk year).
        # resolve_at returns both; value_set_version narrows to one.
        conn = build_slugged_db()
        add_state(
            conn,
            register_id=1,
            var_id=44,
            register_variant_id=10,
            valid_from="2007-01-01",
            valid_to="2007-12-31",
            delivery_column_name="Sni",
            value_set_version_label="sni92",
        )
        add_state(
            conn,
            register_id=1,
            var_id=44,
            register_variant_id=10,
            valid_from="2007-01-01",
            valid_to="2007-12-31",
            delivery_column_name="Sni",
            value_set_version_label="sni2007",
        )
        cat = Catalog(conn)
        both = cat.resolve_at(_KON, 2007, variant="individer-15plus")
        assert len(both) == 2
        narrowed = cat.resolve_at(
            _KON, 2007, variant="individer-15plus", value_set_version="sni2007"
        )
        assert len(narrowed) == 1
        assert narrowed[0].value_set_version_label == "sni2007"

    def test_empty_when_no_state_covers_period(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        # No exception — an empty list signals "binding exists, no state here".
        assert Catalog(slugged_conn).resolve_at(_KON, 1850) == []

    def test_empty_when_variant_unknown(self, slugged_conn: sqlite3.Connection) -> None:
        assert Catalog(slugged_conn).resolve_at(_KON, 2018, variant="nope") == []

    def test_unknown_binding_fqid_raises(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        # The binding itself not resolving is the 404 case (distinct from empty).
        with pytest.raises(RegMetaError) as exc:
            Catalog(slugged_conn).resolve_at("scb/lisa/nonexistent", 2018)
        assert exc.value.code == "fqid_not_found"

    def test_invalid_period_raises_usage(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        with pytest.raises(RegMetaError) as exc:
            Catalog(slugged_conn).resolve_at(_KON, "not-a-period")
        assert exc.value.code == "invalid_period"


class TestEdgeAccessors:
    """§5.10: predecessors/successors/related/lineage/lineage_warnings + the
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
        assert [(s.provider, s.register, s.variable) for s in succ] == [
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
        assert [(p.provider, p.register, p.variable) for p in pred] == [
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
        assert [(x.provider, x.register, x.variable) for x in r.same_as] == [
            ("scb", "rtb", "kon")
        ]
        # same_as refs carry no reason/effective_year (succession-only).
        assert r.same_as[0].reason is None
        assert r.same_as[0].effective_year is None
        # A2.6: the edge endpoint now carries its 3-seg binding FQID (the triple
        # IS the binding FQID once variant/period left the grammar).
        assert str(r.same_as[0].fqid) == "scb/rtb/kon"

    def test_related(self) -> None:
        conn = build_slugged_db()
        for src, tgt in (
            (("scb", "lisa", "kon"), ("scb", "lisa", "kon-alt")),
            (("scb", "lisa", "kon-alt"), ("scb", "lisa", "kon")),
        ):
            conn.execute(
                "INSERT INTO variable_related_to (a_provider,a_register,a_variable,"
                "b_provider,b_register,b_variable,relation_kind,note) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (*src, *tgt, "same_definition_different_column", "auto:triage"),
            )
        conn.commit()
        rel = Catalog(conn).related(_KON)
        assert len(rel) == 1
        assert isinstance(rel[0], RelatedRef)
        assert rel[0].variable == "kon-alt"
        assert rel[0].relation_kind == "same_definition_different_column"
        assert Catalog(conn).resolve(_KON).related_to == tuple(rel)

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
            cat.related,
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
        assert [(s.provider, s.register, s.variable) for s in succ] == [
            ("scb", "lisa", "civilstand")
        ]
