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
    CatalogSizes,
    GroupAxis,
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
        assert scb.name == "Statistiska Centralbyrån"


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


class TestCatalogSizes:
    """`catalog_sizes()` returns the headline browse-addressable counts (#675).

    The grain is slug-aware: it mirrors each `list_*` filter, so a NULL-slug
    register/variable (un-addressable, dropped by the browse listings) is
    EXCLUDED from the register/variable counts — the regression guard for the
    whole point of moving this off the webapp's raw `COUNT(*)`."""

    def test_counts_match_the_slug_aware_listings(self) -> None:
        cat = _catalog()
        sizes = cat.catalog_sizes()
        assert isinstance(sizes, CatalogSizes)

        # providers: every provider (always slugged).
        assert sizes.providers == len(cat.list_providers())
        # registers: slugged registers across all providers.
        assert sizes.registers == sum(
            len(cat.list_registers(p.fqid.provider)) for p in cat.list_providers()
        )
        # variables: slugged bindings across all registers.
        assert sizes.variables == sum(
            len(cat.list_bindings(p.fqid.provider, r.fqid.register))
            for p in cat.list_providers()
            for r in cat.list_registers(p.fqid.provider)
        )

    def test_excludes_null_slug_variables(self) -> None:
        # `_catalog()` seeds scb/rams with 3 variables, one NULL-slug; the count
        # tracks the 2 slugged ones (kon in lisa + syss/bransch in rams = 3),
        # NOT a raw COUNT(*) of 4.
        cat = _catalog()
        assert cat.catalog_sizes().variables == 3
        # Proof the filter is load-bearing: a raw COUNT(*) would see the NULL row.
        raw = cat._conn.execute("SELECT COUNT(*) FROM variable").fetchone()[0]
        assert raw == 4

    def test_excludes_null_slug_registers(self) -> None:
        # scb/lisa (slugged) + a NULL-slug register → only the slugged one counts.
        # The NULL-slug register also gets a SLUGGED variable: the browse can't
        # navigate into a NULL-slug register, so that variable is unreachable and
        # must NOT be counted — proving the variable count joins the parent
        # register's slug, not just `variable.slug IS NOT NULL`.
        conn = build_slugged_db()  # scb/lisa (register_id 1) with variable `kon`
        conn.execute(
            "INSERT INTO register (register_id, provider_id, slug, name) "
            "VALUES (8, 1, NULL, 'Unslugged Register')"
        )
        add_variable(conn, register_id=8, var_id=80, name="Stranded", slug="stranded")
        conn.commit()
        cat = Catalog(conn)
        assert cat.catalog_sizes().registers == 1
        raw = cat._conn.execute("SELECT COUNT(*) FROM register").fetchone()[0]
        assert raw == 2
        # The stranded variable is slugged but lives under the NULL-slug register,
        # so only `kon` (under slugged scb/lisa) counts. A `variable.slug IS NOT
        # NULL`-only query would wrongly see 2.
        assert cat.catalog_sizes().variables == 1
        raw_vars = cat._conn.execute(
            "SELECT COUNT(*) FROM variable WHERE slug IS NOT NULL"
        ).fetchone()[0]
        assert raw_vars == 2


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
    # `extended` leaves the panel columns NULL. `quarterly` carries a composite
    # `panel_time_key` (#567 — UHT's (year, quarter) coordinate, stored JSON).
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
            (
                24,
                2,
                "quarterly",
                "Quarterly",
                None,
                None,
                "peorgnr",
                json.dumps(["ar", "kvartal"]),
                "row",
            ),
        ],
    )
    conn.commit()
    return Catalog(conn)


class TestListVariants:
    def test_lists_register_variants_slug_ordered(self) -> None:
        variants = _variants_catalog().list_variants("scb", "rams")
        assert [v.slug for v in variants] == ["extended", "quarterly", "standard"]
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

    def test_panel_composite_time_key_decoded_to_tuple(self) -> None:
        # #567: `quarterly`'s JSON-array time key decodes to a tuple on read,
        # mirroring the composite entity key.
        q = next(
            v
            for v in _variants_catalog().list_variants("scb", "rams")
            if v.slug == "quarterly"
        )
        assert q.panel_entity_key == "peorgnr"
        assert q.panel_time_key == ("ar", "kvartal")
        assert q.panel_time_grain == "row"

    def test_excludes_null_slug_variants(self) -> None:
        # rams has 4 register_variants but only 3 are slugged/browse-addressable.
        assert len(_variants_catalog().list_variants("scb", "rams")) == 3

    def test_unknown_register_is_empty(self) -> None:
        assert _variants_catalog().list_variants("scb", "nope") == []

    def test_unknown_provider_is_empty(self) -> None:
        assert _variants_catalog().list_variants("nope", "rams") == []


def _groups_catalog() -> Catalog:
    """scb/lisa with a curated single-axis (rank) group, an edge group, and a
    classification vintage group — seeded directly (the derivation passes are
    reg_meta_build territory; this exercises the READ surface)."""
    conn = build_slugged_db()  # scb/lisa + variable `kon`; classification sun2020
    # Curated single-axis (rank) group: 3 members on one axis (#819 shape).
    conn.execute(
        "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
        "label, source) VALUES (10, 'variable', 1, 'agiink', 'Inkomst', 'curated')"
    )
    conn.execute(
        "INSERT INTO concept_group_axis (group_id, axis, ordinal, label) "
        "VALUES (10, 'rank', 0, 'rank')"
    )
    for i, (slug, rank) in enumerate(
        [
            ("agiink1", "1"),
            ("agiink2", "2"),
            ("agiink3", "3"),
        ]
    ):
        add_variable(
            conn, register_id=1, var_id=800 + i, name=f"Inkomst {i}", slug=slug
        )
        vid = conn.execute(
            "SELECT variable_id FROM variable WHERE register_id = 1 AND slug = ?",
            (slug,),
        ).fetchone()[0]
        cur = conn.execute(
            "INSERT INTO concept_group_variable "
            "(group_id, variable_id, delivery_column_name) VALUES (10, ?, NULL)",
            (vid,),
        )
        conn.execute(
            "INSERT INTO concept_group_variable_facet "
            "(member_id, axis, value, label) VALUES (?, 'rank', ?, ?)",
            (cur.lastrowid, rank, f"källa {rank}"),
        )
    # Edge group (no facets): two split siblings. Inserted in REVERSE slug order
    # (sun2020 before sun2000) so `test_edge_members_carry_no_facets`'s slug-order
    # assertion genuinely depends on the `v.slug` tiebreak in
    # `list_concept_groups`' ORDER BY — dropping it would surface insertion order
    # and fail the test, locking the ordering contract.
    conn.execute(
        "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
        "label, source) VALUES (11, 'variable', 1, 'sun2000', 'Utbildning', 'edge')"
    )
    for i, slug in enumerate(["sun2020", "sun2000"]):
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
        "label, source) VALUES "
        "(12, 'classification', NULL, 'sun', 'Svensk utbildningsnomenklatur', "
        "'token')"
    )
    conn.execute(
        "INSERT INTO concept_group_axis (group_id, axis, ordinal, label) "
        "VALUES (12, 'vintage', 0, 'vintage')"
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
        ranked, edge = groups
        assert ranked.source == "curated"
        # Single-axis group: the `axes` tuple holds exactly the one axis (#585),
        # carrying its stable name + authored label (#819).
        assert ranked.axes == (GroupAxis(name="rank", label="rank"),)
        assert edge.source == "edge"
        assert edge.axes == ()
        assert edge.label == "Utbildning"

    def test_members_ordered_by_facet_values_then_slug(self) -> None:
        ranked = _groups_catalog().list_concept_groups("scb", "lisa")[0]
        # Ordered by the single facet value, then slug.
        assert [str(m.fqid) for m in ranked.members] == [
            "scb/lisa/agiink1",
            "scb/lisa/agiink2",
            "scb/lisa/agiink3",
        ]
        first = ranked.members[0]
        # Each member carries exactly one inline facet on the group's axis.
        assert [(f.axis, f.value, f.label) for f in first.facets] == [
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
        assert group.axes == (GroupAxis(name="vintage", label="vintage"),)
        assert [str(m.fqid) for m in group.members] == [
            "class/sun2000",
            "class/sun2020",
        ]
        assert [m.facets[0].value for m in group.members] == ["2000", "2020"]

    def test_empty_without_groups(self) -> None:
        assert _catalog().list_classification_groups() == []

    def test_axis_honors_stored_facet_axis(self) -> None:
        """#819: the read surface reads the group's axis from `concept_group_axis`
        — NOT the old hardcoded 'vintage' or the dropped `facet_axis` column. Seed a
        curated SUN umbrella with a 'dimension' axis over distinct classification
        dimensions."""
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
            "label, source) VALUES (20, 'classification', NULL, 'sun', 'SUN', "
            "'curated')"
        )
        conn.execute(
            "INSERT INTO concept_group_axis (group_id, axis, ordinal, label) "
            "VALUES (20, 'dimension', 0, 'dimension')"
        )
        conn.executemany(
            "INSERT INTO concept_group_classification (classification_id, group_id, "
            "facet_value, facet_label) VALUES (?, 20, ?, ?)",
            [(60, "niva", "Nivå"), (61, "inriktning", "Inriktning")],
        )
        conn.commit()
        (group,) = Catalog(conn).list_classification_groups()
        assert group.axes == (GroupAxis(name="dimension", label="dimension"),)
        assert [f.axis for m in group.members for f in m.facets] == [
            "dimension",
            "dimension",
        ]

    def test_axis_less_umbrella(self) -> None:
        """The curated classification umbrellas are AXIS-LESS (#516 stage 1): zero
        `concept_group_axis` rows (#819), so `axes` is the empty tuple and each
        member's `GroupFacet.axis` is None — yet the members still carry their
        own short facet `value`/`label` (the picker label)."""
        conn = build_slugged_db()  # ships sun2020
        for cid, short, slug in (
            (70, "SUN2020-NIVA", "sun-niva2020"),
            (71, "SUN2020-INR", "sun-inriktning2020"),
        ):
            conn.execute(
                "INSERT INTO classification (id, short_name, name, slug) "
                "VALUES (?, ?, 'SUN', ?)",
                (cid, short, slug),
            )
        conn.execute(
            "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
            "label, source) VALUES (21, 'classification', NULL, 'sun', 'SUN', "
            "'curated')"
        )
        conn.executemany(
            "INSERT INTO concept_group_classification (classification_id, group_id, "
            "facet_value, facet_label) VALUES (?, 21, ?, ?)",
            [(70, "niva", "Nivå"), (71, "inriktning", "Inriktning")],
        )
        conn.commit()
        (group,) = Catalog(conn).list_classification_groups()
        assert group.axes == ()
        # Members keep their inline value/label, but the facet axis is None.
        assert [
            (f.axis, f.value, f.label) for m in group.members for f in m.facets
        ] == [
            (None, "inriktning", "Inriktning"),
            (None, "niva", "Nivå"),
        ]
