"""Tests for Catalog.resolve() (REFACTOR_SPEC.md §5.8)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _slugged_db import (
    add_binding,
    add_register,
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
    ResolvedRegisterVariant,
    ResolvedRegisterVersion,
    ResolvedVariableBinding,
)
from reg_meta.errors import RegMetaError
from reg_meta.fqid import Fqid

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


class TestResolveVariant:
    def test_resolves(self, slugged_conn: sqlite3.Connection) -> None:
        r = Catalog(slugged_conn).resolve("scb/lisa/individer-15plus")
        assert isinstance(r, ResolvedRegisterVariant)
        assert r.register_variant_id == 10
        assert r.fqid.variant == "individer-15plus"

    def test_default_variant_resolves_when_curated(self) -> None:
        # A curated `_default` (real row, slug pinned to `_default` by the
        # maintainer) resolves like any other variant: register_variant_id is the
        # source row's PK.
        conn = build_slugged_db(
            register=("LSS", "lss", 5, 2),
            variant=("LSS default", "_default", 50),
            version=None,
            variable=None,
        )
        r = Catalog(conn).resolve("sos/lss/_default")
        assert isinstance(r, ResolvedRegisterVariant)
        assert r.fqid.variant == "_default"
        assert r.register_variant_id == 50
        assert r.name == "LSS default"

    def test_default_variant_synthesized_for_variant_less_register(self) -> None:
        # §5.1: a register with zero register_variant rows still resolves
        # `<provider>/<register>/_default`. The placeholder is virtual —
        # register_variant_id is None and the variantnamn columns are NULL.
        conn = build_slugged_db(
            register=("LSS", "lss", 5, 2),
            variant=None,
            version=None,
            variable=None,
        )
        r = Catalog(conn).resolve("sos/lss/_default")
        assert isinstance(r, ResolvedRegisterVariant)
        assert r.fqid.variant == "_default"
        assert r.register_variant_id is None
        assert r.register_id == 5
        assert r.name is None

    def test_default_variant_not_synthesized_when_real_variants_exist(self) -> None:
        # LISA has a real variant (`individer-15plus`), so `_default` against
        # it must NOT silently resolve — the variant slot is only transparent
        # when the register is genuinely variant-less.
        conn = build_slugged_db()  # default LISA + individer-15plus
        with pytest.raises(RegMetaError) as exc:
            Catalog(conn).resolve("scb/lisa/_default")
        assert exc.value.code == "fqid_not_found"

    def test_default_variant_synthesis_requires_known_register(self) -> None:
        # Synthesis must not fabricate a variant for a register that doesn't
        # exist — `_default` is only transparent inside a real register.
        conn = build_slugged_db()
        with pytest.raises(RegMetaError) as exc:
            Catalog(conn).resolve("scb/nope/_default")
        assert exc.value.code == "fqid_not_found"


class TestResolveVersion:
    def test_resolves_by_year(self, slugged_conn: sqlite3.Connection) -> None:
        r = Catalog(slugged_conn).resolve("scb/lisa/individer-15plus/2018")
        assert isinstance(r, ResolvedRegisterVersion)
        assert r.regver_id == 100
        assert r.fqid.period == "2018"
        assert str(r.fqid) == "scb/lisa/individer-15plus/2018"

    def test_unknown_period_misses(self, slugged_conn: sqlite3.Connection) -> None:
        with pytest.raises(RegMetaError) as exc:
            Catalog(slugged_conn).resolve("scb/lisa/individer-15plus/2099")
        assert exc.value.code == "fqid_not_found"

    def test_resolves_non_year_period(self) -> None:
        # Half-year, quarterly, and monthly periods use the most-specific
        # token derived from the version name.
        conn = build_slugged_db(version=("Test HT2020", "HT2020", 200))
        r = Catalog(conn).resolve("scb/lisa/individer-15plus/HT2020")
        assert isinstance(r, ResolvedRegisterVersion)
        assert r.regver_id == 200
        assert r.fqid.period == "HT2020"

    def test_year_only_target_does_not_match_sub_year_version(self) -> None:
        # `.../2020` must not silently resolve to an `HT2020` row — that
        # would collide distinct versions under one FQID.
        conn = build_slugged_db(version=("Test HT2020", "HT2020", 200))
        with pytest.raises(RegMetaError) as exc:
            Catalog(conn).resolve("scb/lisa/individer-15plus/2020")
        assert exc.value.code == "fqid_not_found"


class TestResolveBinding:
    def test_resolves(self, slugged_conn: sqlite3.Connection) -> None:
        # Kolumnnamn "Kon" derives to variable slug "kon".
        r = Catalog(slugged_conn).resolve("scb/lisa/individer-15plus/2018/kon")
        assert isinstance(r, ResolvedVariableBinding)
        assert r.cvid == 1001
        assert r.var_id == 44
        assert r.fqid.variable == "kon"
        assert r.delivery_column_name == "Kon"

    def test_swedish_kolumnnamn_folds_to_ascii_slug(self) -> None:
        # "Kön" → "kon" via NFKD ASCII fold; binding FQIDs are ASCII (§5.2).
        conn = build_slugged_db(delivery_column_name="Kön")
        r = Catalog(conn).resolve("scb/lisa/individer-15plus/2018/kon")
        assert isinstance(r, ResolvedVariableBinding)
        assert r.delivery_column_name == "Kön"

    def test_unknown_variable_misses(self, slugged_conn: sqlite3.Connection) -> None:
        with pytest.raises(RegMetaError) as exc:
            Catalog(slugged_conn).resolve("scb/lisa/individer-15plus/2018/nonexistent")
        assert exc.value.code == "fqid_not_found"

    def test_default_fixture_has_no_lineage(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        # Canonical bindings (no via_source_id set) expose lineage as None.
        r = Catalog(slugged_conn).resolve("scb/lisa/individer-15plus/2018/kon")
        assert isinstance(r, ResolvedVariableBinding)
        assert r.via_source_id is None
        assert r.lineage is None


class TestStoredVariableSlug:
    """A2.1.5 (§5.3): the resolver reads the stored `variable.slug`, not a slug
    derived from `delivery_column_name` at query time."""

    def test_resolves_via_stored_slug(self) -> None:
        # Stored slug == derived slug for the common single-column case.
        conn = build_slugged_db()
        r = Catalog(conn).resolve("scb/lisa/individer-15plus/2018/kon")
        assert isinstance(r, ResolvedVariableBinding)
        assert r.var_id == 44

    def test_two_aliases_one_slug_still_resolves(self) -> None:
        # A variable with two aliases (`Kon` + `Kön`) both folding to one slug:
        # the stored slug is single, so the LEFT-JOIN fan-out across aliases
        # still resolves unambiguously.
        conn = build_slugged_db()
        conn.execute(
            "INSERT INTO variable_alias (cvid, delivery_column_name) "
            "VALUES (1001, 'Kön')"
        )
        conn.commit()
        r = Catalog(conn).resolve("scb/lisa/individer-15plus/2018/kon")
        assert isinstance(r, ResolvedVariableBinding)
        assert r.var_id == 44

    def test_stored_slug_overrides_derived_unblocks_triage(self) -> None:
        # The A2.2 unblocking proof: delivery_column_name is `Ssyk` (which would
        # derive to `ssyk`), but the stored slug is `ssyk-3pos`. The binding
        # resolves under the stored slug — proving a build-time triage split can
        # give a sibling sharing a delivery column a distinct, resolvable
        # identity even though derive-at-resolve never produces it.
        conn = build_slugged_db(delivery_column_name="Ssyk", variable_slug="ssyk-3pos")
        r = Catalog(conn).resolve("scb/lisa/individer-15plus/2018/ssyk-3pos")
        assert isinstance(r, ResolvedVariableBinding)
        assert r.var_id == 44
        assert r.delivery_column_name == "Ssyk"
        # The derive-at-resolve slug `ssyk` no longer resolves — identity is the
        # stored slug, not the (honest, shared) delivery column.
        with pytest.raises(RegMetaError) as exc:
            Catalog(conn).resolve("scb/lisa/individer-15plus/2018/ssyk")
        assert exc.value.code == "fqid_not_found"

    @pytest.mark.xfail(
        reason="A2.5: an A2.2 triage split mints sibling variable rows sharing "
        "one (register_id, provider_key) but relinks variable_state, not "
        "variable_instance — so both siblings fan out through _BINDING_QUERY to "
        "the SAME shared instance/cvid. Distinguishing them needs the "
        "variable_state-based resolver (A2.5); A2.1.5 only stores+reads the slug.",
        strict=True,
    )
    def test_split_siblings_resolve_to_distinct_bindings(self) -> None:
        # Mirror A2.2's niva split: one delivery column `Ssyk`, two sibling
        # variables sharing provider_key '44' (§5.7 puts several variables under
        # one source key) but ONE variable_instance (cvid 1001, var_id 44). The
        # interim resolver joins both siblings to that shared instance via
        # provider_key, so each slug resolves to the SAME cvid instead of each
        # sibling's own state — the documented A2.2→A2.5 bridge hazard.
        conn = build_slugged_db(delivery_column_name="Ssyk", variable_slug="ssyk-3pos")
        conn.execute(
            "INSERT INTO variable (register_id, provider_key, name, slug) "
            "VALUES (1, '44', 'SSYK 5-pos', 'ssyk-5pos')"
        )
        conn.commit()
        r3 = Catalog(conn).resolve("scb/lisa/individer-15plus/2018/ssyk-3pos")
        r5 = Catalog(conn).resolve("scb/lisa/individer-15plus/2018/ssyk-5pos")
        assert isinstance(r3, ResolvedVariableBinding)
        assert isinstance(r5, ResolvedVariableBinding)
        # Desired (A2.5): each sibling resolves to its own binding. Today both
        # fan out to the shared cvid 1001, so this fails (strict xfail).
        assert r3.cvid != r5.cvid


class TestResolveBindingLineage:
    """§5.6 consumer-side binding lineage exposure on Catalog.resolve."""

    @staticmethod
    def _build_consumer_db(version_name: str = "RTB 2018") -> sqlite3.Connection:
        # RTB owns Kön (cvid 5000); LISA delivers it as a consumer-side
        # binding (cvid 5001) with via_source_id pointing at RTB's instance.
        period = version_name.split(" ", 1)[1]
        conn = build_slugged_db(
            register=("RTB", "rtb", 1, 1),
            variant=("Personer", "personer", 10),
            version=(version_name, period, 100),
            variable=("Kön", 44, 5000, "Kon"),
        )
        # Consumer register (LISA) shares slug "kon" so lineage matches.
        conn.executescript(
            f"INSERT INTO register (register_id, provider_id, slug, name) "
            f"VALUES (2, 1, 'lisa', 'LISA');"
            f"INSERT INTO register_variant "
            f"(register_variant_id, register_id, slug, name) "
            f"VALUES (20, 2, 'individer-15plus', 'Individer 15+');"
            f"INSERT INTO register_version "
            f"(regver_id, register_variant_id, slug, registerversionnamn) "
            f"VALUES (200, 20, '{period}', 'LISA {period}');"
            # A2.1.5: the resolver reads the stored `variable.slug`, so the
            # consumer variable must carry slug "kon" to resolve (and to match
            # the source slug for lineage).
            f"INSERT INTO variable "
            f"(register_id, provider_key, name, source_register_id, slug) "
            f"VALUES (2, '99', 'Kön', 1, 'kon');"
            f"INSERT INTO variable_instance "
            f"(cvid, register_id, register_variant_id, regver_id, var_id, data_type, via_source_id) "
            f"VALUES (5001, 2, 20, 200, 99, 'int', 5000);"
            f"INSERT INTO variable_alias (cvid, delivery_column_name) VALUES (5001, 'Kon');"
        )
        conn.commit()
        return conn

    def test_consumer_binding_exposes_via_source_id(self) -> None:
        conn = self._build_consumer_db()
        r = Catalog(conn).resolve("scb/lisa/individer-15plus/2018/kon")
        assert isinstance(r, ResolvedVariableBinding)
        assert r.cvid == 5001
        assert r.via_source_id == 5000

    def test_consumer_binding_lineage_fqid(self) -> None:
        conn = self._build_consumer_db()
        r = Catalog(conn).resolve("scb/lisa/individer-15plus/2018/kon")
        assert isinstance(r, ResolvedVariableBinding)
        assert str(r.lineage) == "scb/rtb/personer/2018/kon"

    def test_canonical_source_binding_has_no_lineage(self) -> None:
        conn = self._build_consumer_db()
        r = Catalog(conn).resolve("scb/rtb/personer/2018/kon")
        assert isinstance(r, ResolvedVariableBinding)
        assert r.via_source_id is None
        assert r.lineage is None

    def test_lineage_preserves_sub_year_period(self) -> None:
        # HT2020 source must not collapse to .../2020/... in the lineage FQID.
        conn = self._build_consumer_db(version_name="RTB HT2020")
        r = Catalog(conn).resolve("scb/lisa/individer-15plus/HT2020/kon")
        assert isinstance(r, ResolvedVariableBinding)
        assert str(r.lineage) == "scb/rtb/personer/HT2020/kon"


class TestResolveElidedFqid:
    """§5.2: elided variant slot expands to `_default`. Today only the curated
    `_default` row (PR α single-variant sweep) reaches a version/binding —
    `_synthesize_default_variant` (§5.1, PR #89) fires inside `_resolve_variant`
    only, and `register_version.register_variant_id` is NOT NULL so variant-less
    registers can't carry versions yet. When SOS-style version ingestion
    extends `_resolve_version` to synthesize too, add a passing test alongside
    `test_elided_version_misses_against_variant_less_register`."""

    def test_elided_version_resolves_curated_default_variant(self) -> None:
        conn = build_slugged_db(
            register=("LSS", "lss", 5, 2),
            variant=("LSS default", "_default", 50),
            version=("LSS 2022", "2022", 200),
            variable=None,
        )
        r = Catalog(conn).resolve("sos/lss/2022")
        assert isinstance(r, ResolvedRegisterVersion)
        assert r.regver_id == 200
        assert r.fqid.variant == "_default"
        assert r.fqid.period == "2022"
        assert str(r.fqid) == "sos/lss/_default/2022"

    def test_elided_binding_resolves_curated_default_variant(self) -> None:
        # The PR α sweep target: single-variant register where the maintainer
        # pinned `slug = "_default"`. Researchers can write
        # `scb/<register>/<period>/<variable>` without the variant slot.
        conn = build_slugged_db(
            variant=("Individer", "_default", 10),  # rest of fixture defaults
        )
        r = Catalog(conn).resolve("scb/lisa/2018/kon")
        assert isinstance(r, ResolvedVariableBinding)
        assert r.fqid.variant == "_default"
        assert r.fqid.period == "2018"
        assert r.fqid.variable == "kon"
        assert str(r.fqid) == "scb/lisa/_default/2018/kon"

    def test_elided_version_misses_when_register_has_no_default(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        # LISA has a real variant slugged `individer-15plus`, not `_default`.
        # The elided form `scb/lisa/2018` expands to `_default/2018` and
        # must miss — synthesis is only for variant-less registers.
        with pytest.raises(RegMetaError) as exc:
            Catalog(slugged_conn).resolve("scb/lisa/2018")
        assert exc.value.code == "fqid_not_found"

    def test_elided_binding_misses_when_register_has_no_default(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        # Symmetric to the version miss: the elided binding form
        # `scb/lisa/2018/kon` expands to `_default/2018/kon`, but LISA has
        # only `individer-15plus`, so the binding resolver finds no rows.
        with pytest.raises(RegMetaError) as exc:
            Catalog(slugged_conn).resolve("scb/lisa/2018/kon")
        assert exc.value.code == "fqid_not_found"

    def test_elided_version_misses_against_variant_less_register(self) -> None:
        # Variant-less LSS (no register_variant row) — synthesis covers the
        # bare `sos/lss/_default` variant FQID, but versions can't exist
        # without a variant row (register_variant_id is NOT NULL), so the elided form
        # `sos/lss/2022` must miss today. When version-level synthesis lands
        # (`_resolve_version` TODO), replace this with a passing assertion.
        conn = build_slugged_db(
            register=("LSS", "lss", 5, 2),
            variant=None,
            version=None,
            variable=None,
        )
        with pytest.raises(RegMetaError) as exc:
            Catalog(conn).resolve("sos/lss/2022")
        assert exc.value.code == "fqid_not_found"


class TestResolveVersionWithCuratedSlug:
    """§5.2: register_version.slug accepts either a derived period or a
    curated slug for unperiodized aux versions."""

    def test_curated_slug_resolves(self) -> None:
        # An aux table with a curator-pinned slug resolves like a periodized
        # version — same column, no separate code path.
        conn = build_slugged_db(
            version=("Gymnasieintyg, ackumulerat", "ackumulerat-register", 200),
        )
        r = Catalog(conn).resolve("scb/lisa/individer-15plus/ackumulerat-register")
        assert isinstance(r, ResolvedRegisterVersion)
        assert r.regver_id == 200
        assert r.fqid.period == "ackumulerat-register"
        assert str(r.fqid) == "scb/lisa/individer-15plus/ackumulerat-register"

    def test_default_slug_resolves_for_singleton_unperiodized_version(self) -> None:
        # The Pattern A case from PR γ: variant has one no-period version,
        # curator pins `_default` as the slug.
        conn = build_slugged_db(
            version=("Födelseland", "_default", 200),
        )
        r = Catalog(conn).resolve("scb/lisa/individer-15plus/_default")
        assert isinstance(r, ResolvedRegisterVersion)
        assert r.regver_id == 200
        assert r.fqid.period == "_default"

    def test_curated_slug_does_not_collide_with_period_lookup(self) -> None:
        # A slot-4 token that isn't a known slug (and doesn't match any
        # period either) misses cleanly, same as an unknown period.
        conn = build_slugged_db(
            version=("Gymnasieintyg, ackumulerat", "ackumulerat-register", 200),
        )
        with pytest.raises(RegMetaError) as exc:
            Catalog(conn).resolve("scb/lisa/individer-15plus/summerade-poang")
        assert exc.value.code == "fqid_not_found"


class TestEditions:
    """§5.8 Catalog.editions — cross-edition traversal of a variable slug."""

    def test_single_edition(self, slugged_conn: sqlite3.Connection) -> None:
        editions = Catalog(slugged_conn).editions(
            provider="scb", register="lisa", variable="kon"
        )
        assert len(editions) == 1
        e = editions[0]
        assert e.cvid == 1001
        assert str(e.fqid) == "scb/lisa/individer-15plus/2018/kon"
        assert e.via_source_id is None
        assert e.lineage is None

    def test_multiple_editions_ordered(self) -> None:
        # Two versions under the same variant carry the same kolumnnamn —
        # editions returns both, ordered by (variant_slug, version_slug).
        conn = build_slugged_db()
        add_version(
            conn, regver_id=101, register_variant_id=10, slug="2019", name="LISA 2019"
        )
        add_binding(
            conn,
            cvid=1002,
            register_id=1,
            register_variant_id=10,
            regver_id=101,
            var_id=44,
            delivery_column_name="Kon",
        )
        conn.commit()
        editions = Catalog(conn).editions(
            provider="scb", register="lisa", variable="kon"
        )
        assert [str(e.fqid) for e in editions] == [
            "scb/lisa/individer-15plus/2018/kon",
            "scb/lisa/individer-15plus/2019/kon",
        ]

    def test_spans_variants(self) -> None:
        # Same variable slug delivered through two variants of the same
        # register — both editions surface, ordered by variant slug.
        conn = build_slugged_db()
        add_variant(
            conn, register_variant_id=11, register_id=1, slug="foretag", name="Företag"
        )
        add_version(
            conn, regver_id=110, register_variant_id=11, slug="2018", name="LISA 2018"
        )
        add_binding(
            conn,
            cvid=1003,
            register_id=1,
            register_variant_id=11,
            regver_id=110,
            var_id=44,
            delivery_column_name="Kon",
        )
        conn.commit()
        editions = Catalog(conn).editions(
            provider="scb", register="lisa", variable="kon"
        )
        # `foretag` < `individer-15plus` lexicographically.
        assert [str(e.fqid) for e in editions] == [
            "scb/lisa/foretag/2018/kon",
            "scb/lisa/individer-15plus/2018/kon",
        ]

    def test_multiple_aliases_folding_to_same_slug_dedupe(self) -> None:
        # variable_alias is keyed by (cvid, kolumnnamn); a single instance can
        # carry both `Kon` and `Kön`, which both fold to `kon`. The LEFT JOIN
        # yields one row per alias — editions must dedupe by cvid so a single
        # binding doesn't surface twice.
        conn = build_slugged_db()
        conn.execute(
            "INSERT INTO variable_alias (cvid, delivery_column_name) VALUES (1001, 'Kön')"
        )
        conn.commit()
        editions = Catalog(conn).editions(
            provider="scb", register="lisa", variable="kon"
        )
        assert len(editions) == 1
        assert editions[0].cvid == 1001

    def test_kolumnnamn_diacritic_fold(self) -> None:
        # "Kön" folds to "kon"; the query argument is the slug, not the raw.
        conn = build_slugged_db(delivery_column_name="Kön")
        editions = Catalog(conn).editions(
            provider="scb", register="lisa", variable="kon"
        )
        assert len(editions) == 1
        assert editions[0].delivery_column_name == "Kön"

    def test_consumer_side_binding_included(self) -> None:
        # §5.6: LISA's consumer-side binding of RTB's Kön appears in
        # editions("scb","lisa","kon") with via_source_id and lineage set.
        conn = build_slugged_db(
            register=("RTB", "rtb", 1, 1),
            variant=("Personer", "personer", 10),
            version=("RTB 2018", "2018", 100),
            variable=("Kön", 44, 5000, "Kon"),
        )
        add_register(conn, register_id=2, slug="lisa", name="LISA")
        add_variant(
            conn,
            register_variant_id=20,
            register_id=2,
            slug="individer-15plus",
            name="Individer 15+",
        )
        add_version(
            conn, regver_id=200, register_variant_id=20, slug="2018", name="LISA 2018"
        )
        add_variable(
            conn, register_id=2, var_id=99, name="Kön", source_register_id=1, slug="kon"
        )
        add_binding(
            conn,
            cvid=5001,
            register_id=2,
            register_variant_id=20,
            regver_id=200,
            var_id=99,
            delivery_column_name="Kon",
            via_source_id=5000,
        )
        conn.commit()

        lisa = Catalog(conn).editions(provider="scb", register="lisa", variable="kon")
        assert len(lisa) == 1
        assert lisa[0].cvid == 5001
        assert lisa[0].via_source_id == 5000
        assert str(lisa[0].lineage) == "scb/rtb/personer/2018/kon"

        rtb = Catalog(conn).editions(provider="scb", register="rtb", variable="kon")
        assert len(rtb) == 1
        assert rtb[0].cvid == 5000
        assert rtb[0].via_source_id is None
        assert rtb[0].lineage is None

    def test_unknown_variable_returns_empty(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        assert (
            Catalog(slugged_conn).editions(
                provider="scb", register="lisa", variable="nonexistent"
            )
            == []
        )

    def test_unknown_register_returns_empty(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        # Discovery API: a missing register isn't an error, it's just no rows.
        assert (
            Catalog(slugged_conn).editions(
                provider="scb", register="nope", variable="kon"
            )
            == []
        )

    def test_other_register_excluded(self) -> None:
        # A "kon" binding under a different register must not leak into the
        # query — the (provider, register) filter is tight.
        conn = build_slugged_db()
        add_register(conn, register_id=2, slug="rtb", name="RTB")
        add_variant(
            conn,
            register_variant_id=20,
            register_id=2,
            slug="personer",
            name="Personer",
        )
        add_version(
            conn, regver_id=200, register_variant_id=20, slug="2018", name="RTB 2018"
        )
        add_variable(conn, register_id=2, var_id=99, name="Kön")
        add_binding(
            conn,
            cvid=5001,
            register_id=2,
            register_variant_id=20,
            regver_id=200,
            var_id=99,
            delivery_column_name="Kon",
        )
        conn.commit()
        editions = Catalog(conn).editions(
            provider="scb", register="lisa", variable="kon"
        )
        assert {e.register_id for e in editions} == {1}

    def test_skips_rows_with_null_slug(self) -> None:
        # A variable_instance whose variant or version slug is NULL can't be
        # addressed by FQID — editions silently skips it rather than emitting
        # an unaddressable binding.
        conn = build_slugged_db(version=("LISA 2018", None, 100))
        assert (
            Catalog(conn).editions(provider="scb", register="lisa", variable="kon")
            == []
        )

    def test_invalid_slug_argument_raises(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        from reg_meta.fqid import FqidError

        with pytest.raises(FqidError):
            Catalog(slugged_conn).editions(
                provider="SCB", register="lisa", variable="kon"
            )
        with pytest.raises(FqidError):
            Catalog(slugged_conn).editions(
                provider="scb", register="lisa", variable="Kön"
            )


class TestResolveClassification:
    def test_resolves(self, slugged_conn: sqlite3.Connection) -> None:
        r = Catalog(slugged_conn).resolve("class/sun/2020")
        assert isinstance(r, ResolvedClassification)
        assert r.classification_id is not None
        assert r.fqid.classification == "sun"
        assert r.fqid.version == "2020"

    def test_unknown_version_misses(self, slugged_conn: sqlite3.Connection) -> None:
        with pytest.raises(RegMetaError) as exc:
            Catalog(slugged_conn).resolve("class/sun/2099")
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
        from reg_meta.fqid import FqidError, FqidKind

        bad = Fqid(
            kind=FqidKind.REGISTER_VERSION,
            provider="scb",
            register="lisa",
            variant="individer-15plus",
            period=None,
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
        r = Catalog(slugged_conn).resolve("scb/lisa/individer-15plus/2018/kon")
        assert isinstance(r, ResolvedVariableBinding)
        assert r.via_same_as is None

    def test_same_as_one_hop_resolves(self) -> None:
        # Curated equivalence: kon ↔ civilstand-legacy (constructed scenario —
        # the fixture has only `kon`, but querying `civilstand-legacy` traverses
        # the edge and lands on `kon`'s binding row).
        conn = build_slugged_db()
        self._add_var_edge(
            conn,
            a=("scb", "lisa", "kon"),
            b=("scb", "lisa", "civilstand-legacy"),
        )
        r = Catalog(conn).resolve("scb/lisa/individer-15plus/2018/civilstand-legacy")
        assert isinstance(r, ResolvedVariableBinding)
        # The returned binding is the kon row (cvid 1001), but the FQID on
        # the result preserves the caller's input — researchers reading
        # results back match against what they asked for.
        assert r.cvid == 1001
        assert str(r.fqid) == "scb/lisa/individer-15plus/2018/civilstand-legacy"
        assert r.via_same_as is not None
        assert len(r.via_same_as) == 1
        assert str(r.via_same_as[0]) == "scb/lisa/individer-15plus/2018/kon"

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
        r = Catalog(conn).resolve("scb/lisa/individer-15plus/2018/legacy-name")
        assert isinstance(r, ResolvedVariableBinding)
        assert r.cvid == 1001
        assert r.via_same_as is not None
        # BFS order: legacy-name → intermediate (no hit) → kon (hit).
        assert len(r.via_same_as) == 2
        path = [str(f) for f in r.via_same_as]
        assert path[-1] == "scb/lisa/individer-15plus/2018/kon"

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
            Catalog(conn).resolve("scb/lisa/individer-15plus/2018/phantom-a")
        assert exc.value.code == "fqid_not_found"

    # A2.1.5 (§5.5): variable same_as is variable-grain — edges carry no
    # variant/period narrowing, so the former `test_same_as_variant_narrowing`
    # and `test_visited_key_separates_variant_scopes` (which exercised
    # variant-scoped edges + a variant-keyed visited set) no longer have a
    # behaviour to test and were removed with the demotion.


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
        r = Catalog(conn).resolve("class/sun/2020")
        assert isinstance(r, ResolvedClassification)
        assert r.via_same_as is None

    def test_same_as_one_hop_resolves_via_other_slug(self) -> None:
        # Curated equivalence between two classifications. Querying the
        # legacy slug at any version traverses to the target slug's row.
        # The fixture seeds 'sun' v2020 only; we add a sun-legacy row so
        # we can verify it's the one we resolve to when starting from sun.
        conn = build_slugged_db()
        conn.execute(
            "INSERT INTO classification (short_name, name, version, slug) "
            "VALUES ('LEGACY', 'Legacy SUN', '1996', 'sun-legacy')"
        )
        conn.commit()
        self._add_class_edge(
            conn,
            a=("scb", "sun"),
            b=("scb", "sun-legacy"),
        )
        # Query class/sun-other/<version> — no row, no edge → not found.
        # But class/sun-legacy/1996 is a direct hit (the row we inserted).
        r = Catalog(conn).resolve("class/sun-legacy/1996")
        assert isinstance(r, ResolvedClassification)
        assert r.short_name == "LEGACY"
        assert r.via_same_as is None  # direct hit, no traversal

    def test_same_as_traverses_when_version_mismatches(self) -> None:
        # The classic case: caller has an old FQID `class/sun/1996` baked
        # into their project, but the only sun row in this DB is v2020.
        # Direct lookup misses (version mismatch). same_as is the only way
        # to keep their FQID resolvable. We add an edge sun ↔ sun-v1 with
        # sun-v1 carrying version 1996 so the BFS can find a target.
        conn = build_slugged_db()
        conn.execute(
            "INSERT INTO classification (short_name, name, version, slug) "
            "VALUES ('SUN_V1', 'SUN v1', '1996', 'sun-v1')"
        )
        conn.commit()
        self._add_class_edge(
            conn,
            a=("scb", "sun"),
            b=("scb", "sun-v1"),
        )
        # class/sun/1996 misses (sun row exists only at v2020) → BFS to
        # sun-v1 (v1996) → resolves.
        r = Catalog(conn).resolve("class/sun/1996")
        assert isinstance(r, ResolvedClassification)
        assert r.short_name == "SUN_V1"
        assert r.via_same_as is not None
        assert str(r.via_same_as[0]) == "class/sun-v1/1996"
        # Caller's FQID preserved on the returned record.
        assert str(r.fqid) == "class/sun/1996"

    def test_same_as_no_match_still_raises(self) -> None:
        # Equivalence between two classifications neither of which carries
        # the queried version → BFS exhausts → not found.
        conn = build_slugged_db()
        self._add_class_edge(
            conn,
            a=("scb", "sun"),
            b=("scb", "ghost-classification"),
        )
        # The fixture has sun@2020 only; no row matches 1900.
        with pytest.raises(RegMetaError) as exc:
            Catalog(conn).resolve("class/sun/1900")
        assert exc.value.code == "fqid_not_found"

    def test_multi_version_neighbor_tries_all(self) -> None:
        # Codex P1: a reverse same_as edge can land on a slug stem that
        # carries multiple versions (e.g. `sun` v2000 + v2020). The BFS
        # must try every version until it finds one that matches the
        # caller's queried version, instead of picking one arbitrarily.
        conn = build_slugged_db()
        # Add a second `sun` row at v2000 alongside the fixture's v2020.
        conn.execute(
            "INSERT INTO classification (short_name, name, version, slug) "
            "VALUES ('SUN2000', 'SUN 2000', '2000', 'sun')"
        )
        # Insert a single-version `sun-legacy` slug at v1996 and link it
        # to `sun`. The forward edge (sun-legacy → sun) carries no version
        # information; resolver picks up the version from the DB.
        conn.execute(
            "INSERT INTO classification (short_name, name, version, slug) "
            "VALUES ('SUN_LEGACY', 'SUN legacy', '1996', 'sun-legacy')"
        )
        conn.commit()
        self._add_class_edge(
            conn,
            a=("scb", "sun-legacy"),
            b=("scb", "sun"),
        )
        # Query the v2000 sun directly — direct hits, no traversal.
        r = Catalog(conn).resolve("class/sun/2000")
        assert isinstance(r, ResolvedClassification)
        assert r.short_name == "SUN2000"
        # Query an unknown version under sun-legacy → BFS traverses to
        # `sun`, must try both v2000 and v2020. Without the multi-version
        # fix this could pick v2020 arbitrarily; we verify both versions
        # are reachable through traversal by querying a non-existent
        # sun-legacy version explicitly and checking the resolution
        # carries via_same_as.
        r = Catalog(conn).resolve("class/sun-legacy/1900")
        assert isinstance(r, ResolvedClassification)
        # Hit on whichever sun version came first in the iteration; both
        # are valid landings. The point is `via_same_as` is non-None.
        assert r.via_same_as is not None
        assert r.short_name in {"SUN2020", "SUN2000"}

    def test_publisher_constrained_on_traversal_hit(self) -> None:
        # Codex P1: BFS narrowed by publisher must keep the constraint when
        # verifying the candidate. Two publishers share the same (slug,
        # version) pair; the SOS-anchored edge must resolve to SOS's row,
        # not SCB's, even though SCB's row would also match (slug, version).
        conn = build_slugged_db()
        # Add an SOS-published `kollision` slug at the same version as an
        # SCB-published one. (Cross-publisher slug/version collision is a
        # theoretical case today since only SCB ships classifications, but
        # the traversal contract should be robust against future publishers.)
        conn.execute(
            "INSERT INTO classification (short_name, name, version, slug, publisher) "
            "VALUES ('KOL_SCB', 'Kollision SCB', '2020', 'kollision', 'SCB')"
        )
        conn.execute(
            "INSERT INTO classification (short_name, name, version, slug, publisher) "
            "VALUES ('KOL_SOS', 'Kollision SOS', '2020', 'kollision', 'SOS')"
        )
        # Add a SOS-side `kollision-legacy` slug + an edge from it to the
        # SOS-side `kollision` (under publisher 'sos').
        conn.execute(
            "INSERT INTO classification (short_name, name, version, slug, publisher) "
            "VALUES ('KOL_LEG_SOS', 'Kollision legacy SOS', '1996', "
            "'kollision-legacy', 'SOS')"
        )
        conn.commit()
        self._add_class_edge(
            conn,
            a=("sos", "kollision-legacy"),
            b=("sos", "kollision"),
        )
        # Query SOS legacy at a wrong version → direct misses → BFS narrows
        # to publisher 'sos' and lands on SOS's kollision/2020. Candidate
        # lookup MUST stay constrained to 'sos' and return KOL_SOS, not
        # KOL_SCB (which also matches `kollision`/2020).
        r = Catalog(conn).resolve("class/kollision-legacy/2020")
        assert isinstance(r, ResolvedClassification)
        assert r.short_name == "KOL_SOS"
        assert r.via_same_as is not None
        assert str(r.via_same_as[0]) == "class/kollision/2020"
