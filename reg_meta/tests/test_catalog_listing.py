"""Tests for the Catalog children-enumeration API (A5.1b-i, §5.10).

`list_providers` / `list_registers` / `list_bindings` back the webapp's
catalog browse tree. They return thin slug-ordered Summary lists; an unknown
parent slug returns an empty list (the webapp 404s a genuinely-absent node via
`resolve()`, and renders a present-but-childless parent as an empty list).
"""

from __future__ import annotations

from _slugged_db import add_register, add_variable, build_slugged_db
from reg_meta.catalog import (
    BindingSummary,
    Catalog,
    ProviderSummary,
    RegisterSummary,
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
    def test_lists_both_seeded_providers_slug_ordered(self) -> None:
        providers = _catalog().list_providers()
        assert [p.fqid.provider for p in providers] == ["scb", "sos"]
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
