"""Tests for Catalog.resolve() (REFACTOR_SPEC.md §5.8)."""

from __future__ import annotations

import sqlite3

import pytest

from regmeta.catalog import (
    Catalog,
    ResolvedClassification,
    ResolvedProvider,
    ResolvedRegister,
    ResolvedRegisterVariant,
    ResolvedRegisterVersion,
    ResolvedVariableBinding,
)
from regmeta.errors import RegmetaError
from regmeta.fqid import Fqid

from _slugged_db import build_slugged_db


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
        with pytest.raises(RegmetaError) as exc:
            Catalog(slugged_conn).resolve("nope")
        assert exc.value.code == "fqid_not_found"


class TestResolveRegister:
    def test_resolves(self, slugged_conn: sqlite3.Connection) -> None:
        r = Catalog(slugged_conn).resolve("scb/lisa")
        assert isinstance(r, ResolvedRegister)
        assert r.register_id == 1
        assert r.fqid.provider == "scb"
        assert r.registernamn == "LISA"

    def test_wrong_provider_misses(self, slugged_conn: sqlite3.Connection) -> None:
        with pytest.raises(RegmetaError) as exc:
            Catalog(slugged_conn).resolve("sos/lisa")
        assert exc.value.code == "fqid_not_found"


class TestResolveVariant:
    def test_resolves(self, slugged_conn: sqlite3.Connection) -> None:
        r = Catalog(slugged_conn).resolve("scb/lisa/individer-15plus")
        assert isinstance(r, ResolvedRegisterVariant)
        assert r.regvar_id == 10
        assert r.fqid.variant == "individer-15plus"

    def test_default_variant_resolves_when_curated(self) -> None:
        # A curated `_default` (real row, slug pinned to `_default` by the
        # maintainer) resolves like any other variant: regvar_id is the
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
        assert r.regvar_id == 50
        assert r.registervariantnamn == "LSS default"

    def test_default_variant_synthesized_for_variant_less_register(self) -> None:
        # §5.1: a register with zero register_variant rows still resolves
        # `<provider>/<register>/_default`. The placeholder is virtual —
        # regvar_id is None and the variantnamn columns are NULL.
        conn = build_slugged_db(
            register=("LSS", "lss", 5, 2),
            variant=None,
            version=None,
            variable=None,
        )
        r = Catalog(conn).resolve("sos/lss/_default")
        assert isinstance(r, ResolvedRegisterVariant)
        assert r.fqid.variant == "_default"
        assert r.regvar_id is None
        assert r.register_id == 5
        assert r.registervariantnamn is None

    def test_default_variant_not_synthesized_when_real_variants_exist(self) -> None:
        # LISA has a real variant (`individer-15plus`), so `_default` against
        # it must NOT silently resolve — the variant slot is only transparent
        # when the register is genuinely variant-less.
        conn = build_slugged_db()  # default LISA + individer-15plus
        with pytest.raises(RegmetaError) as exc:
            Catalog(conn).resolve("scb/lisa/_default")
        assert exc.value.code == "fqid_not_found"

    def test_default_variant_synthesis_requires_known_register(self) -> None:
        # Synthesis must not fabricate a variant for a register that doesn't
        # exist — `_default` is only transparent inside a real register.
        conn = build_slugged_db()
        with pytest.raises(RegmetaError) as exc:
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
        with pytest.raises(RegmetaError) as exc:
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
        with pytest.raises(RegmetaError) as exc:
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
        assert r.kolumnnamn == "Kon"

    def test_swedish_kolumnnamn_folds_to_ascii_slug(self) -> None:
        # "Kön" → "kon" via NFKD ASCII fold; binding FQIDs are ASCII (§5.2).
        conn = build_slugged_db(kolumnnamn="Kön")
        r = Catalog(conn).resolve("scb/lisa/individer-15plus/2018/kon")
        assert isinstance(r, ResolvedVariableBinding)
        assert r.kolumnnamn == "Kön"

    def test_unknown_variable_misses(self, slugged_conn: sqlite3.Connection) -> None:
        with pytest.raises(RegmetaError) as exc:
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
            f"INSERT INTO register (register_id, provider_id, slug, registernamn) "
            f"VALUES (2, 1, 'lisa', 'LISA');"
            f"INSERT INTO register_variant "
            f"(regvar_id, register_id, slug, registervariantnamn) "
            f"VALUES (20, 2, 'individer-15plus', 'Individer 15+');"
            f"INSERT INTO register_version "
            f"(regver_id, regvar_id, slug, registerversionnamn) "
            f"VALUES (200, 20, '{period}', 'LISA {period}');"
            f"INSERT INTO variable (register_id, var_id, variabelnamn, source_register_id) "
            f"VALUES (2, 99, 'Kön', 1);"
            f"INSERT INTO variable_instance "
            f"(cvid, register_id, regvar_id, regver_id, var_id, datatyp, via_source_id) "
            f"VALUES (5001, 2, 20, 200, 99, 'int', 5000);"
            f"INSERT INTO variable_alias (cvid, kolumnnamn) VALUES (5001, 'Kon');"
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
    only, and `register_version.regvar_id` is NOT NULL so variant-less
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
        with pytest.raises(RegmetaError) as exc:
            Catalog(slugged_conn).resolve("scb/lisa/2018")
        assert exc.value.code == "fqid_not_found"

    def test_elided_binding_misses_when_register_has_no_default(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        # Symmetric to the version miss: the elided binding form
        # `scb/lisa/2018/kon` expands to `_default/2018/kon`, but LISA has
        # only `individer-15plus`, so the binding resolver finds no rows.
        with pytest.raises(RegmetaError) as exc:
            Catalog(slugged_conn).resolve("scb/lisa/2018/kon")
        assert exc.value.code == "fqid_not_found"

    def test_elided_version_misses_against_variant_less_register(self) -> None:
        # Variant-less LSS (no register_variant row) — synthesis covers the
        # bare `sos/lss/_default` variant FQID, but versions can't exist
        # without a variant row (regvar_id is NOT NULL), so the elided form
        # `sos/lss/2022` must miss today. When version-level synthesis lands
        # (`_resolve_version` TODO), replace this with a passing assertion.
        conn = build_slugged_db(
            register=("LSS", "lss", 5, 2),
            variant=None,
            version=None,
            variable=None,
        )
        with pytest.raises(RegmetaError) as exc:
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
        with pytest.raises(RegmetaError) as exc:
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
        conn.executescript(
            "INSERT INTO register_version "
            "(regver_id, regvar_id, slug, registerversionnamn) "
            "VALUES (101, 10, '2019', 'LISA 2019');"
            "INSERT INTO variable_instance "
            "(cvid, register_id, regvar_id, regver_id, var_id, datatyp) "
            "VALUES (1002, 1, 10, 101, 44, 'int');"
            "INSERT INTO variable_alias (cvid, kolumnnamn) VALUES (1002, 'Kon');"
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
        conn.executescript(
            "INSERT INTO register_variant "
            "(regvar_id, register_id, slug, registervariantnamn) "
            "VALUES (11, 1, 'foretag', 'Företag');"
            "INSERT INTO register_version "
            "(regver_id, regvar_id, slug, registerversionnamn) "
            "VALUES (110, 11, '2018', 'LISA 2018');"
            "INSERT INTO variable_instance "
            "(cvid, register_id, regvar_id, regver_id, var_id, datatyp) "
            "VALUES (1003, 1, 11, 110, 44, 'int');"
            "INSERT INTO variable_alias (cvid, kolumnnamn) VALUES (1003, 'Kon');"
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

    def test_kolumnnamn_diacritic_fold(self) -> None:
        # "Kön" folds to "kon"; the query argument is the slug, not the raw.
        conn = build_slugged_db(kolumnnamn="Kön")
        editions = Catalog(conn).editions(
            provider="scb", register="lisa", variable="kon"
        )
        assert len(editions) == 1
        assert editions[0].kolumnnamn == "Kön"

    def test_consumer_side_binding_included(self) -> None:
        # §5.6: LISA's consumer-side binding of RTB's Kön appears in
        # editions("scb","lisa","kon") with via_source_id and lineage set.
        conn = build_slugged_db(
            register=("RTB", "rtb", 1, 1),
            variant=("Personer", "personer", 10),
            version=("RTB 2018", "2018", 100),
            variable=("Kön", 44, 5000, "Kon"),
        )
        conn.executescript(
            "INSERT INTO register (register_id, provider_id, slug, registernamn) "
            "VALUES (2, 1, 'lisa', 'LISA');"
            "INSERT INTO register_variant "
            "(regvar_id, register_id, slug, registervariantnamn) "
            "VALUES (20, 2, 'individer-15plus', 'Individer 15+');"
            "INSERT INTO register_version "
            "(regver_id, regvar_id, slug, registerversionnamn) "
            "VALUES (200, 20, '2018', 'LISA 2018');"
            "INSERT INTO variable (register_id, var_id, variabelnamn, source_register_id) "
            "VALUES (2, 99, 'Kön', 1);"
            "INSERT INTO variable_instance "
            "(cvid, register_id, regvar_id, regver_id, var_id, datatyp, via_source_id) "
            "VALUES (5001, 2, 20, 200, 99, 'int', 5000);"
            "INSERT INTO variable_alias (cvid, kolumnnamn) VALUES (5001, 'Kon');"
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
        conn.executescript(
            "INSERT INTO register (register_id, provider_id, slug, registernamn) "
            "VALUES (2, 1, 'rtb', 'RTB');"
            "INSERT INTO register_variant "
            "(regvar_id, register_id, slug, registervariantnamn) "
            "VALUES (20, 2, 'personer', 'Personer');"
            "INSERT INTO register_version "
            "(regver_id, regvar_id, slug, registerversionnamn) "
            "VALUES (200, 20, '2018', 'RTB 2018');"
            "INSERT INTO variable (register_id, var_id, variabelnamn) "
            "VALUES (2, 99, 'Kön');"
            "INSERT INTO variable_instance "
            "(cvid, register_id, regvar_id, regver_id, var_id, datatyp) "
            "VALUES (5001, 2, 20, 200, 99, 'int');"
            "INSERT INTO variable_alias (cvid, kolumnnamn) VALUES (5001, 'Kon');"
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
        from regmeta.fqid import FqidError

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
        with pytest.raises(RegmetaError) as exc:
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
        from regmeta.fqid import FqidError, FqidKind

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
        with pytest.raises(RegmetaError) as exc:
            Catalog(conn).resolve("scb/lisa")
        assert exc.value.code == "fqid_not_found"
