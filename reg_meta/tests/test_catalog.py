"""Tests for Catalog.resolve()."""

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
    ClassificationEdition,
    ClassificationRef,
    RelatedRef,
    ResolvedClassification,
    ResolvedProvider,
    ResolvedRegister,
    ResolvedVariable,
    VariableEdition,
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
    """A2.6: the variant and register_version FQID kinds were removed (see DESIGN.md → FQID grammar). A
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
    """A2.5 (see DESIGN.md → Catalog API surface): `resolve()` returns the longitudinal `ResolvedVariable` —
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
        # "Kön" → "kon" via NFKD ASCII fold; binding FQIDs are ASCII (see DESIGN.md → FQID grammar).
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
    """A2.1.5 (see reg_meta_build/DESIGN.md → Slug curation): the resolver reads the stored `variable.slug`, not a slug
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
        # '44' (a split puts several variables under one source key; see reg_meta_build/DESIGN.md → Build-time triage (SCB)) but have
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
    """Consumer-side lineage exposure (see reg_meta_build/DESIGN.md → Consumer-side lineage (variable_state_lineage)) on the longitudinal resolution
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
# variant is a register sub-resource without a slash-path FQID (see DESIGN.md → FQID grammar).


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


# ── A2.5 longitudinal resolution + resolve_at + edge accessors (see DESIGN.md → Catalog API surface) ──

_KON = "scb/lisa/kon"


class TestResolveVariableLongitudinal:
    """see DESIGN.md → Catalog API surface: `resolve()` returns the variable's shared metadata + full state
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

    def test_identifier_flag_denormalized_onto_states(self) -> None:
        # The variable-grain `is_identifier` is exposed on the ResolvedVariable
        # AND denormalized onto every state via the `_states_in_bounds` JOIN
        # (no-variant branch) — distinct from the variable-meta path — so
        # consumers with no ResolvedVariable in scope still read it.
        conn = build_slugged_db()
        conn.execute("UPDATE variable SET is_identifier = 1 WHERE slug = 'kon'")
        conn.commit()
        r = Catalog(conn).resolve(_KON)
        assert r.is_identifier is True
        assert r.states[0].is_identifier is True

    def test_classification_slug_resolved_per_state(self) -> None:
        # `variable_state.classification_id` is per-state, so the slug resolves
        # per-state via the LEFT JOIN. The fixture's auto-seeded base state is
        # code-less (classification_id NULL → slug None); seed a second state
        # pointing at the 'sun2020' classification and assert its slug comes
        # through alongside the base state's None.
        conn = build_slugged_db()
        cls_id = conn.execute(
            "SELECT id FROM classification WHERE slug = 'sun2020'"
        ).fetchone()[0]
        add_state(
            conn,
            register_id=1,
            variable_slug="kon",
            register_variant_id=10,
            valid_from="2019-01-01",
            valid_to="2019-12-31",
            delivery_column_name="Kon",
            classification_id=cls_id,
        )
        conn.commit()
        by_from = {
            s.valid_from: s.classification_slug for s in Catalog(conn).states(_KON)
        }
        assert by_from["2018-01-01"] is None
        assert by_from["2019-01-01"] == "sun2020"

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
        # see DESIGN.md → Catalog API surface (migration stage A2.5): the full history via resolve(fqid).states
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
    """see DESIGN.md → Catalog API surface: `resolve_at` — period/variant/version-narrowed list of states."""

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

    def test_identifier_flag_on_variant_scoped_state(self) -> None:
        # Resolving with an explicit variant takes the variant-scoped
        # (`register_variant_id IS NOT NULL`) branch of `_states_in_bounds`; the
        # denormalized `is_identifier` must come through there too.
        conn = build_slugged_db()
        conn.execute("UPDATE variable SET is_identifier = 1 WHERE slug = 'kon'")
        conn.commit()
        states = Catalog(conn).resolve_at(_KON, 2018, variant="individer-15plus")
        assert len(states) == 1
        assert states[0].is_identifier is True

    def test_classification_slug_on_variant_scoped_state(self) -> None:
        # Mirror of the is_identifier variant-scoped test, for classification: the
        # variant-scoped (`register_variant_id IS NOT NULL`) SELECT branch must
        # also resolve the per-state slug. A pre-2018 window keeps the seed clear
        # of the fixture's open-ended base state so it's the sole match.
        conn = build_slugged_db()
        cls_id = conn.execute(
            "SELECT id FROM classification WHERE slug = 'sun2020'"
        ).fetchone()[0]
        add_state(
            conn,
            register_id=1,
            variable_slug="kon",
            register_variant_id=10,
            valid_from="2017-01-01",
            valid_to="2017-12-31",
            delivery_column_name="Kon",
            classification_id=cls_id,
        )
        conn.commit()
        states = Catalog(conn).resolve_at(_KON, 2017, variant="individer-15plus")
        assert len(states) == 1
        assert states[0].classification_slug == "sun2020"

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
        # Multi-vintage fold (see reg_meta_build/DESIGN.md → Build-time triage (SCB)): two overlapping states, same variant + year,
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

    @pytest.mark.parametrize("bad", ["2019-02-29", "2018-02-30", "2021-04-31"])
    def test_calendar_invalid_period_raises_usage(
        self, slugged_conn: sqlite3.Connection, bad: str
    ) -> None:
        # #239: a calendar-impossible day is now rejected by the period grammar,
        # so `resolve_at` raises `invalid_period` instead of silently tolerating
        # the string (which previously risked phantom coverage results).
        with pytest.raises(RegMetaError) as exc:
            Catalog(slugged_conn).resolve_at(_KON, bad)
        assert exc.value.code == "invalid_period"


class TestEdgeAccessors:
    """see DESIGN.md → Catalog API surface: predecessors/successors/related/lineage/lineage_warnings + the
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

    def test_classification_successors(self) -> None:
        # sun2000 (predecessor) → sun2020 (successor); sun2020 is the live default
        # classification from build_slugged_db.
        conn = build_slugged_db()
        self._seed_classification_replaced_by(
            conn, predecessor="sun2000", successor="sun2020", effective_year=2020
        )
        succ = Catalog(conn).classification_successors("class/sun2000")
        assert len(succ) == 1
        assert isinstance(succ[0], ClassificationRef)
        assert succ[0].slug == "sun2020"
        assert str(succ[0].fqid) == "class/sun2020"
        assert succ[0].effective_year == 2020
        assert succ[0].note == "derived:vintage_chain"
        # (ResolvedClassification.replaced_by coverage is below — it keys on the
        # resolved edition's OWN slug, so it needs a live row to resolve.)

    def test_classification_predecessors_uses_successor_index(self) -> None:
        # Query the SUCCESSOR side: sun2020 was preceded by sun2000. Proves the
        # successor-keyed reverse lookup (idx_classification_replaced_by_successor).
        conn = build_slugged_db()
        self._seed_classification_replaced_by(
            conn, predecessor="sun2000", successor="sun2020", effective_year=2020
        )
        pred = Catalog(conn).classification_predecessors("class/sun2020")
        assert [p.slug for p in pred] == ["sun2000"]
        assert pred[0].effective_year == 2020

    def test_classification_succession_on_resolved_classification(self) -> None:
        # ResolvedClassification.replaced_by carries OUTBOUND edges keyed on the
        # resolved edition's own slug. sun2020 → sun-future (a hypothetical
        # successor edition).
        conn = build_slugged_db()
        self._seed_classification_replaced_by(
            conn, predecessor="sun2020", successor="sun-future", effective_year=2030
        )
        r = Catalog(conn).resolve("class/sun2020")
        assert isinstance(r, ResolvedClassification)
        assert [e.slug for e in r.replaced_by] == ["sun-future"]
        assert r.replaced_by[0].effective_year == 2030

    def test_classification_terminal_edition_has_no_replaced_by(self) -> None:
        # sun2020 with no outbound edge is a terminal (current) edition.
        conn = build_slugged_db()
        r = Catalog(conn).resolve("class/sun2020")
        assert isinstance(r, ResolvedClassification)
        assert r.replaced_by == ()

    def test_classification_edges_tolerate_dead_predecessor(self) -> None:
        # Succession edges reference the literal slug — a DEAD predecessor edition
        # (no `classification` row) still has edges. classification_successors must
        # NOT require the slug to resolve to a live row.
        conn = build_slugged_db()
        self._seed_classification_replaced_by(
            conn, predecessor="ssyk1996", successor="ssyk2012"
        )
        succ = Catalog(conn).classification_successors("class/ssyk1996")
        assert [s.slug for s in succ] == ["ssyk2012"]
        # No outbound edge for a slug not in the table → empty list, no raise.
        assert Catalog(conn).classification_successors("class/lkf2026") == []

    def test_classification_accessors_reject_non_classification_fqid(self) -> None:
        conn = build_slugged_db()
        with pytest.raises(RegMetaError) as exc:
            Catalog(conn).classification_successors("scb/lisa/kon")
        assert exc.value.code == "not_a_classification_fqid"
        with pytest.raises(RegMetaError) as exc:
            Catalog(conn).classification_predecessors("scb")
        assert exc.value.code == "not_a_classification_fqid"

    @staticmethod
    def _seed_classification(
        conn: sqlite3.Connection, *, slug: str, short_name: str, name: str
    ) -> None:
        conn.execute(
            "INSERT INTO classification (short_name, name, slug) VALUES (?, ?, ?)",
            (short_name, name, slug),
        )
        conn.commit()

    def test_classification_chain_multi_hop_ordered_oldest_first(self) -> None:
        # sun1996 → sun2000 → sun2020 (the live terminal). All three editions are
        # LIVE `classification` rows — the validator forbids succession edges to
        # dead slugs (validate.py, the classification_replaced_by check), so every
        # chain endpoint resolves. The chain returns ALL three, oldest first, the
        # terminal last, with is_self/is_current on the queried/terminal edition,
        # and every edition carries a non-None fqid.
        conn = build_slugged_db()  # seeds the live sun2020
        self._seed_classification(
            conn, slug="sun1996", short_name="SUN1996", name="SUN 1996"
        )
        self._seed_classification(
            conn, slug="sun2000", short_name="SUN2000", name="SUN 2000"
        )
        self._seed_classification_replaced_by(
            conn, predecessor="sun1996", successor="sun2000", effective_year=2000
        )
        self._seed_classification_replaced_by(
            conn, predecessor="sun2000", successor="sun2020", effective_year=2020
        )
        # Query an intermediate edition — the chain still spans the whole
        # succession and marks the queried slug as is_self.
        chain = Catalog(conn).classification_chain("class/sun2000")
        assert [e.slug for e in chain] == ["sun1996", "sun2000", "sun2020"]
        assert [e.effective_year for e in chain] == [2000, 2020, None]
        assert all(isinstance(e, ClassificationEdition) for e in chain)
        # Every edition is a live row → non-None fqid (no dead-edition shape).
        assert all(e.fqid is not None for e in chain)
        assert [str(e.fqid) for e in chain] == [
            "class/sun1996",
            "class/sun2000",
            "class/sun2020",
        ]
        self_edition = next(e for e in chain if e.is_self)
        assert self_edition.slug == "sun2000"
        current = next(e for e in chain if e.is_current)
        assert current.slug == "sun2020"
        assert sum(e.is_current for e in chain) == 1
        assert sum(e.is_self for e in chain) == 1

    def test_classification_chain_standalone_returns_single_self_current(self) -> None:
        # sun2020 has no succession edges → a one-edition chain, both is_self and
        # is_current True.
        conn = build_slugged_db()
        chain = Catalog(conn).classification_chain("class/sun2020")
        assert len(chain) == 1
        assert chain[0].slug == "sun2020"
        assert chain[0].is_self is True
        assert chain[0].is_current is True
        assert chain[0].effective_year is None

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

    def test_variable_chain_standalone_returns_single_self_current(self) -> None:
        # kon has no succession edges → a one-edition chain, both is_self and
        # is_current True, no reason/effective_year.
        conn = build_slugged_db()
        chain = Catalog(conn).variable_chain(_KON)
        assert len(chain) == 1
        assert chain[0].variable == "kon"
        assert chain[0].is_self is True
        assert chain[0].is_current is True
        assert chain[0].effective_year is None
        assert chain[0].reason is None

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
        assert [(e.register, e.variable) for e in chain] == [("lisa", "kon")]
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

    def test_classification_chain_split_follows_deterministic_first(self) -> None:
        # spl-a→spl-b and spl-a→spl-c: spl-a is a split. chain(spl-a) follows the
        # deterministic-first successor ([0] of the ORDER BY successor_slug edges =
        # lexicographic-first, spl-b < spl-c), so [spl-a, spl-b]. Each branch tip
        # resolves its own walk.
        conn = build_slugged_db()
        for slug in ("spl-a", "spl-b", "spl-c"):
            self._seed_classification(
                conn, slug=slug, short_name=slug.upper(), name=slug
            )
        self._seed_classification_replaced_by(
            conn, predecessor="spl-a", successor="spl-b", effective_year=2012
        )
        self._seed_classification_replaced_by(
            conn, predecessor="spl-a", successor="spl-c", effective_year=2012
        )
        chain_a = Catalog(conn).classification_chain("class/spl-a")
        assert [e.slug for e in chain_a] == ["spl-a", "spl-b"]
        # Querying spl-b walks back to spl-a then forward via the det-first successor
        # (spl-b), so its path is [spl-a, spl-b] with is_self on spl-b.
        chain_b = Catalog(conn).classification_chain("class/spl-b")
        assert [e.slug for e in chain_b] == ["spl-a", "spl-b"]
        assert next(e for e in chain_b if e.is_self).slug == "spl-b"
        # spl-c's backward walk reaches spl-a; spl-a's det-first successor is spl-b,
        # so the forward walk goes spl-a→spl-b — spl-c is reached only as is_self.
        chain_c = Catalog(conn).classification_chain("class/spl-c")
        assert next(e for e in chain_c if e.is_self).slug == "spl-c"

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

    def test_variable_chain_merge_excludes_sibling_branch(self) -> None:
        # kon → syss and other → syss: syss is a merge. chain(kon) returns its OWN
        # path [kon, syss], NOT the sibling `other`; chain(other) returns
        # [other, syss]. The pre-#588 collect-all walk wrongly rendered `other`.
        conn = build_slugged_db()
        add_variable(conn, register_id=1, var_id=400, name="Syss", slug="syss")
        add_variable(conn, register_id=1, var_id=401, name="Other", slug="other")
        self._seed_var_replaced_by(
            conn,
            predecessor=("scb", "lisa", "kon"),
            successor=("scb", "lisa", "syss"),
            effective_year=2019,
        )
        self._seed_var_replaced_by(
            conn,
            predecessor=("scb", "lisa", "other"),
            successor=("scb", "lisa", "syss"),
            effective_year=2019,
        )
        chain_kon = Catalog(conn).variable_chain(_KON)
        assert [e.variable for e in chain_kon] == ["kon", "syss"]
        chain_other = Catalog(conn).variable_chain("scb/lisa/other")
        assert [e.variable for e in chain_other] == ["other", "syss"]

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
        chain_bbb = Catalog(conn).variable_chain("scb/lisa/bbb")
        assert next(e for e in chain_bbb if e.is_self).variable == "bbb"

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


class TestDimensions:
    """#489: `dimensions(fqid)` returns the register's concept groups whose
    members include this binding's variable. Like the other edge accessors it
    resolves `same_as` (via `_resolve_edge_triple`), so an alias cites its
    resolved target's groups — the regression guard for the bug where the old
    webapp handler keyed the filter on the REQUESTED register/fqid and returned
    `[]` for an alias."""

    @staticmethod
    def _add_group(
        conn: sqlite3.Connection,
        *,
        group_id: int,
        register_id: int,
        group_key: str,
        member_slugs: list[str],
    ) -> None:
        conn.execute(
            "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
            "label, source) VALUES (?, 'variable', ?, ?, ?, 'curated')",
            (group_id, register_id, group_key, f"Group {group_key}"),
        )
        for slug in member_slugs:
            vid = conn.execute(
                "SELECT variable_id FROM variable WHERE register_id = ? AND slug = ?",
                (register_id, slug),
            ).fetchone()[0]
            conn.execute(
                "INSERT INTO concept_group_variable (variable_id, group_id) "
                "VALUES (?, ?)",
                (vid, group_id),
            )
        conn.commit()

    def test_returns_group_containing_variable(self) -> None:
        conn = build_slugged_db()
        # A sibling variable so the group has >1 member and the filter is real.
        add_variable(
            conn, register_id=1, var_id=45, name="Civilstånd", slug="civilstand"
        )
        self._add_group(
            conn,
            group_id=30,
            register_id=1,
            group_key="demog",
            member_slugs=["kon", "civilstand"],
        )
        groups = Catalog(conn).dimensions(_KON)
        assert [g.key for g in groups] == ["demog"]
        assert {str(m.fqid) for m in groups[0].members} == {
            "scb/lisa/kon",
            "scb/lisa/civilstand",
        }

    def test_excludes_group_without_variable(self) -> None:
        conn = build_slugged_db()
        # A group over a DIFFERENT variable only — kon is not a member.
        add_variable(
            conn, register_id=1, var_id=45, name="Civilstånd", slug="civilstand"
        )
        self._add_group(
            conn,
            group_id=31,
            register_id=1,
            group_key="other",
            member_slugs=["civilstand"],
        )
        assert Catalog(conn).dimensions(_KON) == []

    def test_resolves_through_same_as_to_target_group(self) -> None:
        # P2-A guard: lisa/phantom ≡ rtb/kon (cross-register same_as). The group
        # lives under RTB over rtb/kon; querying the lisa alias must cite the
        # TARGET register's group, not lisa's (which has none).
        conn = build_slugged_db()
        add_register(conn, register_id=2, slug="rtb", name="RTB")
        add_variant(
            conn, register_variant_id=20, register_id=2, slug="personer", name="P"
        )
        add_version(conn, regver_id=200, register_variant_id=20, name="RTB 2018")
        add_variable(conn, register_id=2, var_id=99, name="Kön", slug="kon")
        for src, tgt in (
            (("scb", "lisa", "phantom"), ("scb", "rtb", "kon")),
            (("scb", "rtb", "kon"), ("scb", "lisa", "phantom")),
        ):
            conn.execute(
                "INSERT INTO variable_same_as (a_provider,a_register,a_variable,"
                "b_provider,b_register,b_variable) VALUES (?,?,?,?,?,?)",
                (*src, *tgt),
            )
        self._add_group(
            conn,
            group_id=32,
            register_id=2,
            group_key="rtbdemog",
            member_slugs=["kon"],
        )
        groups = Catalog(conn).dimensions("scb/lisa/phantom")
        assert [g.key for g in groups] == ["rtbdemog"]
        assert [str(m.fqid) for m in groups[0].members] == ["scb/rtb/kon"]

    def test_raises_on_non_binding_fqid(self, slugged_conn: sqlite3.Connection) -> None:
        with pytest.raises(RegMetaError) as exc:
            Catalog(slugged_conn).dimensions("scb/lisa")
        assert exc.value.code == "not_a_binding_fqid"

    def test_raises_on_unknown_binding(self, slugged_conn: sqlite3.Connection) -> None:
        with pytest.raises(RegMetaError) as exc:
            Catalog(slugged_conn).dimensions("scb/lisa/nonexistent")
        assert exc.value.code == "fqid_not_found"


class TestResolveTerminalSuccessor:
    """#355 PART 2 / #412 / #571: walk a succession chain from a (possibly
    dead/renamed) FQID to its terminal successor — the chain end with no outbound
    edge. The walk dispatches on FQID kind: bindings walk `variable_replaced_by`
    (raw string triples), registers walk `register_replaced_by` (raw string
    pairs), classifications walk `classification_replaced_by` (raw edition slugs).
    A DEAD predecessor needs no live row (the whole point — its row is gone after
    the rename)."""

    @staticmethod
    def _add_edge(
        conn: sqlite3.Connection,
        predecessor: tuple[str, str, str],
        successor: tuple[str, str, str],
    ) -> None:
        conn.execute(
            "INSERT INTO variable_replaced_by ("
            "predecessor_provider, predecessor_register, predecessor_variable, "
            "successor_provider, successor_register, successor_variable, note) "
            "VALUES (?,?,?,?,?,?,'auto:test')",
            (*predecessor, *successor),
        )
        conn.commit()

    @staticmethod
    def _add_register_edge(
        conn: sqlite3.Connection,
        predecessor: tuple[str, str],
        successor: tuple[str, str],
    ) -> None:
        conn.execute(
            "INSERT INTO register_replaced_by ("
            "predecessor_provider, predecessor_register, "
            "successor_provider, successor_register, note) "
            "VALUES (?,?,?,?,'auto:test')",
            (*predecessor, *successor),
        )
        conn.commit()

    def test_multi_hop_chain_returns_terminal(self) -> None:
        # old-a → old-b → kon (the live, edge-free leaf from build_slugged_db).
        # old-a / old-b are dead: NO variable rows, only edges.
        conn = build_slugged_db()
        self._add_edge(conn, ("scb", "lisa", "old-a"), ("scb", "lisa", "old-b"))
        self._add_edge(conn, ("scb", "lisa", "old-b"), ("scb", "lisa", "kon"))
        terminal = Catalog(conn).resolve_terminal_successor("scb/lisa/old-a")
        assert terminal is not None
        assert str(terminal) == "scb/lisa/kon"

    def test_no_outbound_edge_returns_none(self) -> None:
        # kon is the live terminal with no outbound edge → genuinely unknown.
        conn = build_slugged_db()
        assert Catalog(conn).resolve_terminal_successor(_KON) is None

    def test_unsupported_kinds_return_none(self) -> None:
        # PROVIDER has NO succession table, so a rename there has nowhere to
        # redirect → None (no SQL). Register and classification grains ARE in
        # scope (#412 / #571): a register/classification with no outbound edge is
        # None too — genuinely unknown, same as the no-edge binding case ("no
        # edge", not "out of scope"). sun2020 is the live, edge-free classification
        # from build_slugged_db.
        conn = build_slugged_db()
        cat = Catalog(conn)
        assert cat.resolve_terminal_successor("scb") is None
        assert cat.resolve_terminal_successor("class/sun2020") is None
        assert cat.resolve_terminal_successor("scb/lisa") is None

    def test_cycle_guard_terminates(self) -> None:
        # Malformed double-rename loop A→B→A. The walk must terminate (not hang)
        # and land deterministically on B (start=A hops to B, B→A is already
        # seen → stop).
        conn = build_slugged_db()
        self._add_edge(conn, ("scb", "lisa", "loop-a"), ("scb", "lisa", "loop-b"))
        self._add_edge(conn, ("scb", "lisa", "loop-b"), ("scb", "lisa", "loop-a"))
        terminal = Catalog(conn).resolve_terminal_successor("scb/lisa/loop-a")
        assert terminal is not None
        assert str(terminal) == "scb/lisa/loop-b"

    def test_split_pick_is_lexicographically_first(self) -> None:
        # Deterministic split pick: when a predecessor has TWO distinct
        # successors, the walk takes the lexicographically-FIRST per
        # `_first_successor_triple`'s `ORDER BY successor_provider,
        # successor_register, successor_variable LIMIT 1`. Both successors are
        # dead leaves (no variable rows, no further edges) so each is itself
        # terminal — this isolates the split pick, not the walk depth.
        conn = build_slugged_db()
        self._add_edge(conn, ("scb", "lisa", "split-src"), ("scb", "lisa", "zzz-high"))
        self._add_edge(conn, ("scb", "lisa", "split-src"), ("scb", "lisa", "aaa-low"))
        terminal = Catalog(conn).resolve_terminal_successor("scb/lisa/split-src")
        assert terminal is not None
        # "aaa-low" < "zzz-high" → the lower-sorted successor wins.
        assert str(terminal) == "scb/lisa/aaa-low"

    # #412: register-grain renames now redirect too. These mirror the binding
    # tests above on `register_replaced_by`; dead predecessor registers need no
    # `register` row (same dead-slug premise), and `scb/lisa` is the live,
    # edge-free terminal from build_slugged_db.

    def test_register_multi_hop_chain_returns_terminal(self) -> None:
        # old-reg-a → old-reg-b → lisa (the live, edge-free register). old-reg-a /
        # old-reg-b are dead: NO register rows, only edges.
        conn = build_slugged_db()
        self._add_register_edge(conn, ("scb", "old-reg-a"), ("scb", "old-reg-b"))
        self._add_register_edge(conn, ("scb", "old-reg-b"), ("scb", "lisa"))
        terminal = Catalog(conn).resolve_terminal_successor("scb/old-reg-a")
        assert terminal is not None
        assert str(terminal) == "scb/lisa"

    def test_register_cycle_guard_terminates(self) -> None:
        # Malformed double-rename loop A→B→A. The walk must terminate (not hang)
        # and land deterministically on B (start=A hops to B, B→A is already
        # seen → stop).
        conn = build_slugged_db()
        self._add_register_edge(conn, ("scb", "loop-reg-a"), ("scb", "loop-reg-b"))
        self._add_register_edge(conn, ("scb", "loop-reg-b"), ("scb", "loop-reg-a"))
        terminal = Catalog(conn).resolve_terminal_successor("scb/loop-reg-a")
        assert terminal is not None
        assert str(terminal) == "scb/loop-reg-b"

    def test_register_split_pick_is_lexicographically_first(self) -> None:
        # Deterministic split pick: when a predecessor register has TWO distinct
        # successors, the walk takes the lexicographically-FIRST per
        # `_first_register_successor_pair`'s `ORDER BY successor_provider,
        # successor_register LIMIT 1`. Both successors are dead leaves (no register
        # rows, no further edges) so each is itself terminal — this isolates the
        # split pick, not the walk depth.
        conn = build_slugged_db()
        self._add_register_edge(conn, ("scb", "split-reg"), ("scb", "zzz-reg"))
        self._add_register_edge(conn, ("scb", "split-reg"), ("scb", "aaa-reg"))
        terminal = Catalog(conn).resolve_terminal_successor("scb/split-reg")
        assert terminal is not None
        # "aaa-reg" < "zzz-reg" → the lower-sorted successor wins.
        assert str(terminal) == "scb/aaa-reg"

    # #571: classification-edition renames now redirect too. These mirror the
    # binding/register tests on `classification_replaced_by` (raw edition slugs);
    # dead predecessor editions need no `classification` row.

    @staticmethod
    def _add_class_edge(
        conn: sqlite3.Connection, predecessor: str, successor: str
    ) -> None:
        conn.execute(
            "INSERT INTO classification_replaced_by "
            "(predecessor_slug, successor_slug, note) VALUES (?,?,'derived:test')",
            (predecessor, successor),
        )
        conn.commit()

    def test_classification_multi_hop_chain_returns_terminal(self) -> None:
        # ssyk1996 → ssyk2001 → ssyk2012. Predecessor editions are dead (no
        # `classification` rows, only edges); ssyk2012 is the terminal.
        conn = build_slugged_db()
        self._add_class_edge(conn, "ssyk1996", "ssyk2001")
        self._add_class_edge(conn, "ssyk2001", "ssyk2012")
        terminal = Catalog(conn).resolve_terminal_successor("class/ssyk1996")
        assert terminal is not None
        assert str(terminal) == "class/ssyk2012"

    def test_classification_no_outbound_edge_returns_none(self) -> None:
        # sun2020 is the live, edge-free classification → genuinely unknown.
        conn = build_slugged_db()
        assert Catalog(conn).resolve_terminal_successor("class/sun2020") is None

    def test_classification_cycle_guard_terminates(self) -> None:
        # Malformed double-rename loop A→B→A: terminate and land on B.
        conn = build_slugged_db()
        self._add_class_edge(conn, "loop-cls-a", "loop-cls-b")
        self._add_class_edge(conn, "loop-cls-b", "loop-cls-a")
        terminal = Catalog(conn).resolve_terminal_successor("class/loop-cls-a")
        assert terminal is not None
        assert str(terminal) == "class/loop-cls-b"

    def test_classification_split_pick_is_lexicographically_first(self) -> None:
        # A predecessor edition with TWO successors takes the lexicographically
        # first (ORDER BY successor_slug LIMIT 1); both are dead terminal leaves.
        conn = build_slugged_db()
        self._add_class_edge(conn, "split-cls", "zzz-cls")
        self._add_class_edge(conn, "split-cls", "aaa-cls")
        terminal = Catalog(conn).resolve_terminal_successor("class/split-cls")
        assert terminal is not None
        assert str(terminal) == "class/aaa-cls"
