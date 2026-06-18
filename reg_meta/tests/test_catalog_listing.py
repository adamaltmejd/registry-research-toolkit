"""Tests for the Catalog children-enumeration API (A5.1b-i; see DESIGN.md → Catalog API surface).

`list_providers` / `list_registers` / `list_bindings` back the webapp's
catalog browse tree. They return thin slug-ordered Summary lists; an unknown
parent slug returns an empty list (the webapp 404s a genuinely-absent node via
`resolve()`, and renders a present-but-childless parent as an empty list).
"""

from __future__ import annotations

import json

from _slugged_db import add_register, add_variable, build_slugged_db
from reg_meta.catalog import (
    BindingSummary,
    Catalog,
    ProviderSummary,
    RegisterSummary,
    VariantSummary,
)
from reg_meta.fqid import FqidKind


def _catalog() -> Catalog:
    """A catalog with two scb registers (slug-out-of-order to prove ordering)
    and the default `sos` provider with no registers."""
    conn = build_slugged_db()  # scb/lisa with variable `kon`
    add_register(conn, register_id=2, slug="rams", name="RAMS")
    add_variable(conn, register_id=2, var_id=70, name="Sysselsattning", slug="syss")
    add_variable(conn, register_id=2, var_id=71, name="Bransch", slug="bransch")
    # A NULL-slug variable isn't addressable by a binding FQID → excluded.
    add_variable(conn, register_id=2, var_id=72, name="Unslugged", slug=None)
    conn.commit()
    return Catalog(conn)


class TestListProviders:
    def test_lists_seeded_providers_slug_ordered(self) -> None:
        providers = _catalog().list_providers()
        assert [p.fqid.provider for p in providers] == [
            "fk",
            "fohm",
            "lakemedelsverket",
            "pliktverket",
            "riksarkivet",
            "scb",
            "sos",
            "umu",
        ]
        assert all(isinstance(p, ProviderSummary) for p in providers)

    def test_carries_fqid_and_name(self) -> None:
        scb = next(p for p in _catalog().list_providers() if p.fqid.provider == "scb")
        assert scb.fqid.kind is FqidKind.PROVIDER
        assert str(scb.fqid) == "scb"
        assert scb.name == "Statistics Sweden"


class TestListRegisters:
    def test_lists_provider_registers_slug_ordered(self) -> None:
        registers = _catalog().list_registers("scb")
        assert [str(r.fqid) for r in registers] == ["scb/lisa", "scb/rams"]
        assert all(isinstance(r, RegisterSummary) for r in registers)

    def test_carries_name_and_purpose(self) -> None:
        lisa = next(
            r for r in _catalog().list_registers("scb") if r.fqid.register == "lisa"
        )
        assert lisa.name == "LISA"
        assert lisa.purpose is None  # fixture register has no purpose

    def test_provider_with_no_registers_is_empty(self) -> None:
        assert _catalog().list_registers("sos") == []

    def test_unknown_provider_is_empty_not_error(self) -> None:
        assert _catalog().list_registers("nope") == []

    def test_excludes_null_slug_registers(self) -> None:
        # A register with a NULL slug isn't addressable by a register FQID, so
        # it's excluded — symmetric with the NULL-slug variable exclusion.
        conn = build_slugged_db()  # scb/lisa (slugged)
        conn.execute(
            "INSERT INTO register (register_id, provider_id, slug, name) "
            "VALUES (8, 1, NULL, 'Unslugged Register')"
        )
        conn.commit()
        registers = Catalog(conn).list_registers("scb")
        assert [r.fqid.register for r in registers] == ["lisa"]


class TestListBindings:
    def test_lists_register_variable_slugs_slug_ordered(self) -> None:
        bindings = _catalog().list_bindings("scb", "rams")
        assert [str(b.fqid) for b in bindings] == [
            "scb/rams/bransch",
            "scb/rams/syss",
        ]
        assert all(isinstance(b, BindingSummary) for b in bindings)

    def test_excludes_null_slug_variables(self) -> None:
        # rams has 3 variables but only 2 are slugged/addressable.
        assert len(_catalog().list_bindings("scb", "rams")) == 2

    def test_carries_fqid_and_name(self) -> None:
        syss = next(
            b
            for b in _catalog().list_bindings("scb", "rams")
            if b.fqid.variable == "syss"
        )
        assert syss.fqid.kind is FqidKind.VARIABLE_BINDING
        assert syss.name == "Sysselsattning"

    def test_unknown_register_is_empty_not_error(self) -> None:
        assert _catalog().list_bindings("scb", "nope") == []

    def test_unknown_provider_is_empty_not_error(self) -> None:
        assert _catalog().list_bindings("nope", "rams") == []


def test_listing_disambiguates_by_parent_scope() -> None:
    """Register slugs are provider-scoped and variable slugs register-scoped, so
    a listing must return ONLY the queried parent's children even when a sibling
    provider/register reuses the same slug. Locks the `WHERE p.slug = ?` /
    `AND r.slug = ?` join predicates against a dropped-clause regression.
    """
    conn = build_slugged_db()  # scb/lisa (register_id 1) with variable `kon`
    # Cross-provider register-slug collision: sos ALSO has a register `lisa`.
    add_register(conn, register_id=9, slug="lisa", name="SOS LISA", provider_id=2)
    add_variable(conn, register_id=9, var_id=90, name="SOS Kon", slug="kon")
    # Cross-register variable-slug collision: scb/rams ALSO has a variable `kon`.
    add_register(conn, register_id=2, slug="rams", name="RAMS")
    add_variable(conn, register_id=2, var_id=70, name="RAMS Kon", slug="kon")
    conn.commit()
    cat = Catalog(conn)

    # list_registers returns only the queried provider's registers.
    assert [str(r.fqid) for r in cat.list_registers("scb")] == ["scb/lisa", "scb/rams"]
    assert [str(r.fqid) for r in cat.list_registers("sos")] == ["sos/lisa"]

    # list_bindings returns only the queried register's variables — the shared
    # `kon` slug resolves to a distinct binding per (provider, register).
    assert [str(b.fqid) for b in cat.list_bindings("scb", "lisa")] == ["scb/lisa/kon"]
    assert [str(b.fqid) for b in cat.list_bindings("scb", "rams")] == ["scb/rams/kon"]
    assert [str(b.fqid) for b in cat.list_bindings("sos", "lisa")] == ["sos/lisa/kon"]


def test_listing_roundtrips_into_resolve() -> None:
    """Every listed FQID resolves — the browse tree is navigable end to end."""
    cat = _catalog()
    for prov in cat.list_providers():
        assert cat.resolve(prov.fqid).fqid == prov.fqid
        for reg in cat.list_registers(prov.fqid.provider):
            assert cat.resolve(reg.fqid).fqid == reg.fqid
            for binding in cat.list_bindings(prov.fqid.provider, reg.fqid.register):
                assert cat.resolve(binding.fqid).fqid == binding.fqid


def _variants_catalog() -> Catalog:
    """scb/rams with three register_variants (slug out-of-order + a NULL-slug one,
    to prove ordering + exclusion). Seeded via raw SQL because the `_slugged_db`
    `add_variant` helper sets only slug+name, not description/display_group."""
    conn = build_slugged_db()  # scb/lisa + its default variant
    add_register(conn, register_id=2, slug="rams", name="RAMS")
    # `standard` carries A4.4c panel data (composite entity key, stored JSON);
    # `extended` leaves the panel columns NULL.
    conn.executemany(
        "INSERT INTO register_variant "
        "(register_variant_id, register_id, slug, name, description, display_group, "
        " panel_entity_key, panel_time_key, panel_time_grain) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                21,
                2,
                "standard",
                "Standard",
                "the standard delivery",
                "Surveys",
                json.dumps(["foretag", "arbetsstalle"]),
                "period",
                "delivery",
            ),
            (22, 2, "extended", "Extended", None, None, None, None, None),
            (23, 2, None, "Unslugged", None, None, None, None, None),
        ],
    )
    conn.commit()
    return Catalog(conn)


class TestListVariants:
    def test_lists_register_variants_slug_ordered(self) -> None:
        variants = _variants_catalog().list_variants("scb", "rams")
        assert [v.slug for v in variants] == ["extended", "standard"]
        assert all(isinstance(v, VariantSummary) for v in variants)

    def test_carries_name_description_display_group(self) -> None:
        std = next(
            v
            for v in _variants_catalog().list_variants("scb", "rams")
            if v.slug == "standard"
        )
        assert std.name == "Standard"
        assert std.description == "the standard delivery"
        assert std.display_group == "Surveys"

    def test_nullable_fields_pass_through_as_none(self) -> None:
        ext = next(
            v
            for v in _variants_catalog().list_variants("scb", "rams")
            if v.slug == "extended"
        )
        assert ext.description is None
        assert ext.display_group is None

    def test_panel_fields_none_by_default(self) -> None:
        # A4.4c: `extended` carries no panel data → all three are None.
        ext = next(
            v
            for v in _variants_catalog().list_variants("scb", "rams")
            if v.slug == "extended"
        )
        assert ext.panel_entity_key is None
        assert ext.panel_time_key is None
        assert ext.panel_time_grain is None

    def test_panel_composite_entity_key_decoded_to_tuple(self) -> None:
        # A4.4c: `standard`'s JSON-array entity key decodes to a tuple on read.
        std = next(
            v
            for v in _variants_catalog().list_variants("scb", "rams")
            if v.slug == "standard"
        )
        assert std.panel_entity_key == ("foretag", "arbetsstalle")
        assert std.panel_time_key == "period"
        assert std.panel_time_grain == "delivery"

    def test_excludes_null_slug_variants(self) -> None:
        # rams has 3 register_variants but only 2 are slugged/browse-addressable.
        assert len(_variants_catalog().list_variants("scb", "rams")) == 2

    def test_unknown_register_is_empty(self) -> None:
        assert _variants_catalog().list_variants("scb", "nope") == []

    def test_unknown_provider_is_empty(self) -> None:
        assert _variants_catalog().list_variants("nope", "rams") == []


def _groups_catalog() -> Catalog:
    """scb/lisa with a curated month×rank matrix group, an edge group, and a
    classification vintage group — seeded directly (the derivation passes are
    reg_meta_build territory; this exercises the READ surface)."""
    conn = build_slugged_db()  # scb/lisa + variable `kon`; classification sun2020
    # Curated matrix group: 2 ranks × 2 months.
    conn.execute(
        "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
        "label, source) VALUES (10, 'variable', 1, 'agiink', 'Inkomst', 'curated')"
    )
    for i, (slug, month, month_label, rank) in enumerate(
        [
            ("agi1inkjan", "01", "januari", "1"),
            ("agi1inkfeb", "02", "februari", "1"),
            ("agi2inkjan", "01", "januari", "2"),
            ("agi2inkfeb", "02", "februari", "2"),
        ]
    ):
        add_variable(
            conn, register_id=1, var_id=800 + i, name=f"Inkomst {i}", slug=slug
        )
        vid = conn.execute(
            "SELECT variable_id FROM variable WHERE register_id = 1 AND slug = ?",
            (slug,),
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO concept_group_variable (variable_id, group_id) VALUES (?, 10)",
            (vid,),
        )
        conn.executemany(
            "INSERT INTO concept_group_variable_facet (variable_id, axis, value, "
            "label) VALUES (?, ?, ?, ?)",
            [(vid, "month", month, month_label), (vid, "rank", rank, f"källa {rank}")],
        )
    # Edge group (no facets): two split siblings.
    conn.execute(
        "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
        "label, source) VALUES (11, 'variable', 1, 'sun2000', 'Utbildning', 'edge')"
    )
    for i, slug in enumerate(["sun2000", "sun2020"]):
        add_variable(conn, register_id=1, var_id=810 + i, name="Utbildning", slug=slug)
        vid = conn.execute(
            "SELECT variable_id FROM variable WHERE register_id = 1 AND slug = ?",
            (slug,),
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO concept_group_variable (variable_id, group_id) VALUES (?, 11)",
            (vid,),
        )
    # Classification vintage group over the fixture's sun2020 + an added sun2000.
    conn.execute(
        "INSERT INTO classification (id, short_name, name, slug) "
        "VALUES (50, 'SUN2000', 'Svensk utbildningsnomenklatur', 'sun2000')"
    )
    conn.execute(
        "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
        "label, source, facet_axis) VALUES "
        "(12, 'classification', NULL, 'sun', 'Svensk utbildningsnomenklatur', "
        "'token', 'vintage')"
    )
    conn.executemany(
        "INSERT INTO concept_group_classification (classification_id, group_id, "
        "facet_value, facet_label) VALUES (?, 12, ?, ?)",
        [
            (50, "2000", "2000"),
            (
                conn.execute(
                    "SELECT id FROM classification WHERE slug = 'sun2020'"
                ).fetchone()[0],
                "2020",
                "2020",
            ),
        ],
    )
    conn.commit()
    return Catalog(conn)


class TestListConceptGroups:
    def test_groups_ordered_by_key_with_axes(self) -> None:
        groups = _groups_catalog().list_concept_groups("scb", "lisa")
        assert [g.key for g in groups] == ["agiink", "sun2000"]
        matrix, edge = groups
        assert matrix.source == "curated"
        assert matrix.axes == ("month", "rank")
        assert edge.source == "edge"
        assert edge.axes == ()
        assert edge.label == "Utbildning"

    def test_members_ordered_by_facet_values_then_slug(self) -> None:
        matrix = _groups_catalog().list_concept_groups("scb", "lisa")[0]
        # axes = (month, rank): jan/rank1, jan/rank2, feb/rank1, feb/rank2.
        assert [str(m.fqid) for m in matrix.members] == [
            "scb/lisa/agi1inkjan",
            "scb/lisa/agi2inkjan",
            "scb/lisa/agi1inkfeb",
            "scb/lisa/agi2inkfeb",
        ]
        first = matrix.members[0]
        assert [(f.axis, f.value, f.label) for f in first.facets] == [
            ("month", "01", "januari"),
            ("rank", "1", "källa 1"),
        ]

    def test_edge_members_carry_no_facets(self) -> None:
        edge = _groups_catalog().list_concept_groups("scb", "lisa")[1]
        assert [str(m.fqid) for m in edge.members] == [
            "scb/lisa/sun2000",
            "scb/lisa/sun2020",
        ]
        assert all(m.facets == () for m in edge.members)

    def test_unknown_register_or_provider_is_empty(self) -> None:
        cat = _groups_catalog()
        assert cat.list_concept_groups("scb", "nope") == []
        assert cat.list_concept_groups("nope", "lisa") == []

    def test_register_without_groups_is_empty(self) -> None:
        cat = _catalog()  # scb/lisa + scb/rams, no concept groups seeded
        assert cat.list_concept_groups("scb", "rams") == []


class TestListClassificationGroups:
    def test_vintage_group_with_facets(self) -> None:
        groups = _groups_catalog().list_classification_groups()
        assert len(groups) == 1
        (group,) = groups
        assert group.key == "sun"
        assert group.axes == ("vintage",)
        assert [str(m.fqid) for m in group.members] == [
            "class/sun2000",
            "class/sun2020",
        ]
        assert [m.facets[0].value for m in group.members] == ["2000", "2020"]

    def test_empty_without_groups(self) -> None:
        assert _catalog().list_classification_groups() == []

    def test_axis_honors_stored_facet_axis(self) -> None:
        """#516: the read surface reads `concept_group.facet_axis` for the group's
        axis — NOT the old hardcoded 'vintage'. Seed a curated SUN umbrella with a
        'dimension' axis over distinct classification dimensions."""
        conn = build_slugged_db()  # ships sun2020
        for cid, short, slug in (
            (60, "SUN2020-NIVA", "sun-niva2020"),
            (61, "SUN2020-INR", "sun-inriktning2020"),
        ):
            conn.execute(
                "INSERT INTO classification (id, short_name, name, slug) "
                "VALUES (?, ?, 'SUN', ?)",
                (cid, short, slug),
            )
        conn.execute(
            "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
            "label, source, facet_axis) VALUES "
            "(20, 'classification', NULL, 'sun', 'SUN', 'curated', 'dimension')"
        )
        conn.executemany(
            "INSERT INTO concept_group_classification (classification_id, group_id, "
            "facet_value, facet_label) VALUES (?, 20, ?, ?)",
            [(60, "niva", "Nivå"), (61, "inriktning", "Inriktning")],
        )
        conn.commit()
        (group,) = Catalog(conn).list_classification_groups()
        assert group.axes == ("dimension",)
        assert [f.axis for m in group.members for f in m.facets] == [
            "dimension",
            "dimension",
        ]
