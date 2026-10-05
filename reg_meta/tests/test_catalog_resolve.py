"""Catalog.resolve() by FQID kind: providers, registers, bindings (stored
slugs, lineage), classifications and parsed FQID objects."""

from __future__ import annotations

import sqlite3

import catalog_test_support
import pytest
from _slugged_db import (
    add_register,
    add_state,
    add_variable,
    add_variant,
    add_version,
    build_slugged_db,
)
from reg_meta.catalog import (
    Catalog,
    ResolvedClassification,
    ResolvedProvider,
    ResolvedRegister,
    ResolvedVariable,
)
from reg_meta.doc_db import DOC_SCHEMA_VERSION
from reg_meta.errors import RegMetaError
from reg_meta.fqid import Fqid, FqidError
from reg_meta_build.doc_db import DOC_DDL

# The shared fixture, bound by assignment: an imported name used only as a
# test parameter reads as an unused import redefined (ruff F401/F811).
slugged_conn = catalog_test_support.slugged_conn


def _related_doc_conn(register: str = "lisa") -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(DOC_DDL)
    conn.execute(
        "INSERT INTO doc_meta (key, value) VALUES ('schema_version', ?)",
        (DOC_SCHEMA_VERSION,),
    )
    conn.execute(
        "INSERT INTO related_document ("
        "register, title, filename, source_url, license, fetched, "
        "sha256, byte_size, content"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            register,
            "LISA manual",
            "lisa_manual.pdf",
            "https://example.test/lisa_manual.pdf",
            "CC BY 4.0",
            "2026-06-29",
            "c" * 64,
            7,
            b"manual",
        ),
    )
    conn.commit()
    return conn


class TestResolveProvider:
    def test_resolves_known_provider(self, slugged_conn: sqlite3.Connection) -> None:
        r = Catalog(slugged_conn).resolve("scb")
        assert isinstance(r, ResolvedProvider)
        assert r.name == "Statistiska Centralbyrån"
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
        assert r.related_documents == ()

    def test_resolves_with_related_documents(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        r = Catalog(slugged_conn, doc_conn=_related_doc_conn()).resolve("scb/lisa")
        assert isinstance(r, ResolvedRegister)
        assert [doc.filename for doc in r.related_documents] == ["lisa_manual.pdf"]

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
        # The variant DISPLAY name is surfaced on the state (the contract field the
        # picker shows in place of the slug). `_DEFAULT_VARIANT` names it "Individer
        # 15+"; the slug remains the add coordinate above.
        assert r.states[0].variant_label == "Individer 15+"
        assert r.related_documents == ()

    def test_resolves_register_related_documents(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        r = Catalog(slugged_conn, doc_conn=_related_doc_conn()).resolve("scb/lisa/kon")
        assert isinstance(r, ResolvedVariable)
        assert [doc.filename for doc in r.related_documents] == ["lisa_manual.pdf"]

    def test_variant_label_is_none_for_a_null_named_variant(self) -> None:
        # A NULL-named variant → variant_label None (the consumer falls back to the
        # slug for display). The slug stays present as the add coordinate.
        conn = build_slugged_db(variant=(None, "snoskotrar", 10))
        r = Catalog(conn).resolve("scb/lisa/kon")
        assert isinstance(r, ResolvedVariable)
        assert r.states[0].variant == "snoskotrar"
        assert r.states[0].variant_label is None

    def test_swedish_kolumnnamn_folds_to_ascii_slug(self) -> None:
        # "Kön" → "kon" via NFKD ASCII fold; binding FQIDs are ASCII (see DESIGN.md → FQID grammar).
        # The raw delivery column is preserved on the state.
        conn = build_slugged_db(delivery_column_name="Kön")
        r = Catalog(conn).resolve("scb/lisa/kon")
        assert isinstance(r, ResolvedVariable)
        assert r.states[0].delivery_column_name == "Kön"

    def test_operational_definition_flows_through_resolve(self) -> None:
        # #892/#932: the per-(split-)variable distinguishing text is a first-class
        # column surfaced on `ResolvedVariable`. Default fixture leaves it NULL.
        conn = build_slugged_db()
        r = Catalog(conn).resolve("scb/lisa/kon")
        assert isinstance(r, ResolvedVariable)
        assert r.operational_definition is None
        conn.execute(
            "UPDATE variable SET operational_definition = ? WHERE slug = 'kon'",
            ("Avser personens registrerade kön vid årets slut.",),
        )
        conn.commit()
        r = Catalog(conn).resolve("scb/lisa/kon")
        assert isinstance(r, ResolvedVariable)
        assert (
            r.operational_definition
            == "Avser personens registrerade kön vid årets slut."
        )

    def test_state_operational_definition_flows_through_resolve(self) -> None:
        # #736: state-grain operational definitions are distinct from the
        # variable-level summary and must survive in `ResolvedVariable.states`.
        conn = build_slugged_db()
        conn.execute(
            "UPDATE variable_state SET operational_definition = ? "
            "WHERE variable_id = (SELECT variable_id FROM variable WHERE slug = 'kon')",
            ("Avser just denna leveranskolumn.",),
        )
        conn.commit()
        r = Catalog(conn).resolve("scb/lisa/kon")
        assert isinstance(r, ResolvedVariable)
        assert r.operational_definition is None
        assert r.states[0].operational_definition == "Avser just denna leveranskolumn."

    def test_state_provenance_flows_through_resolve(self) -> None:
        conn = build_slugged_db()
        provenance = (
            "errata:omitted-column-in-version\n"
            "The steward holds this delivery, which SCB omits."
        )
        conn.execute(
            "UPDATE variable_state SET provenance = ? "
            "WHERE variable_id = (SELECT variable_id FROM variable WHERE slug = 'kon')",
            (provenance,),
        )
        conn.commit()

        r = Catalog(conn).resolve("scb/lisa/kon")

        assert isinstance(r, ResolvedVariable)
        assert r.states[0].provenance == provenance

    def test_alias_window_expansion_suppresses_base_state_operational_definition(
        self,
    ) -> None:
        # #736: alias-window expansion clones one stored base state into several
        # delivery-column windows. `variable_alias_window` has no per-window
        # operational-definition field, so reusing the base state text would
        # misdescribe the expanded aliases.
        conn = build_slugged_db()
        conn.execute(
            "UPDATE variable_state SET operational_definition = ? "
            "WHERE variable_id = (SELECT variable_id FROM variable WHERE slug = 'kon')",
            ("Base state text must not leak to expanded aliases.",),
        )
        conn.executemany(
            "INSERT INTO variable_alias_window ("
            "variable_id, register_variant_id, delivery_column_name, valid_from, valid_to"
            ") VALUES ((SELECT variable_id FROM variable WHERE slug = 'kon'), 10, ?, ?, ?)",
            [
                ("Kon", "2018-01-01", "2018-01-31"),
                ("Kon_feb", "2018-02-01", "2018-02-28"),
            ],
        )
        conn.commit()
        r = Catalog(conn).resolve("scb/lisa/kon")
        assert isinstance(r, ResolvedVariable)
        assert [s.delivery_column_name for s in r.states] == ["Kon", "Kon_feb"]
        assert [s.operational_definition for s in r.states] == [None, None]

    def test_deprecated_flag_flows_through_resolve(self) -> None:
        conn = build_slugged_db()
        r = Catalog(conn).resolve("scb/lisa/kon")
        assert isinstance(r, ResolvedVariable)
        assert r.deprecated is False
        conn.execute("UPDATE variable SET deprecated = 1 WHERE slug = 'kon'")
        conn.commit()
        r = Catalog(conn).resolve("scb/lisa/kon")
        assert isinstance(r, ResolvedVariable)
        assert r.deprecated is True

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
        assert r.lineage == ()
        assert r.via_same_as is None


class TestStoredVariableSlug:
    """A2.1.5 (see reg_meta_build/DESIGN.md → Slug curation): the resolver reads the stored `variable.slug`, not a slug
    derived from `delivery_column_name` at query time. A2.5: resolution is now
    longitudinal (`ResolvedVariable`, period-independent) — split siblings
    resolve to distinct `variable_id`s, and point selection moved to
    `resolve_at`."""

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
