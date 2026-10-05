"""Catalog group reads: `dimensions`, `concept_group_by_key`, a resolved
variable's group ref and `classification_dimensions`."""

from __future__ import annotations

from typing import TYPE_CHECKING

import catalog_test_support
import pytest
from _slugged_db import (
    add_register,
    add_variable,
    add_variant,
    add_version,
    build_slugged_db,
)
from catalog_test_support import KON as _KON
from reg_meta.catalog import (
    BindingGroupRef,
    Catalog,
    GroupAxis,
)
from reg_meta.errors import RegMetaError

if TYPE_CHECKING:
    import sqlite3

# The shared fixture, bound by assignment: an imported name used only as a
# test parameter reads as an unused import redefined (ruff F401/F811).
slugged_conn = catalog_test_support.slugged_conn


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


def _add_concept_group(
    conn: sqlite3.Connection,
    *,
    group_id: int,
    register_id: int,
    group_key: str,
    member_slugs: list[str],
    source: str = "curated",
) -> None:
    """Seed one `kind='variable'` concept group over existing variables (by slug),
    facet-less. `source` exercises the curated/edge/token surfaces (`key` is
    present for all)."""
    conn.execute(
        "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
        "label, source) VALUES (?, 'variable', ?, ?, ?, ?)",
        (group_id, register_id, group_key, f"Group {group_key}", source),
    )
    for slug in member_slugs:
        vid = conn.execute(
            "SELECT variable_id FROM variable WHERE register_id = ? AND slug = ?",
            (register_id, slug),
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO concept_group_variable (variable_id, group_id) VALUES (?, ?)",
            (vid, group_id),
        )
    conn.commit()


class TestConceptGroupByKey:
    """#616: `concept_group(provider, register, key)` returns the one group
    addressed by its scope-unique `key` (a filter over `list_concept_groups`),
    or None for an unknown key / unknown pair — matching the sibling list
    accessor's tolerance of an unknown pair (`[]`)."""

    def test_returns_group_by_key(self) -> None:
        conn = build_slugged_db()
        add_variable(
            conn, register_id=1, var_id=45, name="Civilstånd", slug="civilstand"
        )
        _add_concept_group(
            conn,
            group_id=40,
            register_id=1,
            group_key="demog",
            member_slugs=["kon", "civilstand"],
        )
        group = Catalog(conn).concept_group("scb", "lisa", "demog")
        assert group is not None
        assert group.key == "demog"
        assert group.source == "curated"
        assert {str(m.fqid) for m in group.members} == {
            "scb/lisa/kon",
            "scb/lisa/civilstand",
        }

    def test_returns_group_by_key_edge_source(self) -> None:
        # `key` is present for every source, not just curated — an edge group is
        # addressable too.
        conn = build_slugged_db()
        add_variable(conn, register_id=1, var_id=46, name="Sun 2000", slug="sun2000")
        _add_concept_group(
            conn,
            group_id=41,
            register_id=1,
            group_key="sun-edge",
            member_slugs=["kon", "sun2000"],
            source="edge",
        )
        group = Catalog(conn).concept_group("scb", "lisa", "sun-edge")
        assert group is not None
        assert group.source == "edge"

    def test_unknown_key_returns_none(self) -> None:
        conn = build_slugged_db()
        _add_concept_group(
            conn,
            group_id=42,
            register_id=1,
            group_key="demog",
            member_slugs=["kon"],
        )
        assert Catalog(conn).concept_group("scb", "lisa", "nope") is None

    def test_unknown_pair_returns_none(self, slugged_conn: sqlite3.Connection) -> None:
        cat = Catalog(slugged_conn)
        assert cat.concept_group("scb", "nope", "demog") is None
        assert cat.concept_group("nope", "lisa", "demog") is None


class TestResolvedVariableGroupRef:
    """#616: `ResolvedVariable.group` is the binding's owning group as a
    `(provider, register, key)` ref (membership is 1:1 — the
    `concept_group_variable.variable_id` PK), or None for an ungrouped variable,
    so a member page renders group-aware without a second fetch."""

    def test_grouped_member_carries_group_ref(self) -> None:
        conn = build_slugged_db()
        add_variable(
            conn, register_id=1, var_id=45, name="Civilstånd", slug="civilstand"
        )
        _add_concept_group(
            conn,
            group_id=43,
            register_id=1,
            group_key="demog",
            member_slugs=["kon", "civilstand"],
        )
        r = Catalog(conn).resolve(_KON)
        assert r.group == BindingGroupRef(provider="scb", register="lisa", key="demog")

    def test_ungrouped_variable_has_no_group_ref(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        assert Catalog(slugged_conn).resolve(_KON).group is None

    def test_group_ref_follows_same_as_to_target(self) -> None:
        # The ref keys off the RESOLVED variable's triple (like the edges), so a
        # same_as alias reports its target's group, not the requested register's.
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
        _add_concept_group(
            conn,
            group_id=44,
            register_id=2,
            group_key="rtbdemog",
            member_slugs=["kon"],
        )
        r = Catalog(conn).resolve("scb/lisa/phantom")
        assert r.group == BindingGroupRef(
            provider="scb", register="rtb", key="rtbdemog"
        )


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

    def test_returns_umbrella_group_with_sibling_aggregates(self) -> None:
        conn = build_slugged_db()
        self._seed_sun_umbrella(conn)
        groups = Catalog(conn).classification_dimensions("class/sun2020")
        assert [g.key for g in groups] == ["sun"]
        assert groups[0].axes == (GroupAxis(name="dimension", label="dimension"),)
        assert {str(m.fqid) for m in groups[0].members} == {
            "class/sun2020",
            "class/niva-oldv1",
            "class/niva-grovv1",
        }

    def test_aggregate_leaf_also_sees_the_group(self) -> None:
        # Browsing an aggregate edition surfaces the same umbrella (the relationship
        # is symmetric — every member sees its siblings).
        conn = build_slugged_db()
        self._seed_sun_umbrella(conn)
        groups = Catalog(conn).classification_dimensions("class/niva-oldv1")
        assert [g.key for g in groups] == ["sun"]

    def test_empty_for_ungrouped_classification(self) -> None:
        conn = build_slugged_db()  # sun2020 in no umbrella group
        assert Catalog(conn).classification_dimensions("class/sun2020") == []

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

    def test_raises_on_non_classification_fqid(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        with pytest.raises(RegMetaError) as exc:
            Catalog(slugged_conn).classification_dimensions("scb/lisa")
        assert exc.value.code == "not_a_classification_fqid"

    def test_raises_on_unknown_classification(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        # Fail-fast parity with `classification_codes`: a syntactically valid but
        # ABSENT classification must raise, not silently return [] (which would make
        # a missing classification indistinguishable from an ungrouped one).
        with pytest.raises(RegMetaError) as exc:
            Catalog(slugged_conn).classification_dimensions("class/sun2099")
        assert exc.value.code == "fqid_not_found"
