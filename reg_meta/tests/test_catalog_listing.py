"""Tests for the Catalog children-enumeration API (A5.1b-i; see DESIGN.md → Catalog API surface).

`list_providers` / `list_registers` / `list_bindings` back the webapp's
catalog browse tree. They return thin slug-ordered Summary lists; an unknown
parent slug returns an empty list (the webapp 404s a genuinely-absent node via
`resolve()`, and renders a present-but-childless parent as an empty list).
"""

from __future__ import annotations

import json

from _slugged_db import add_register, add_variable, add_variant, build_slugged_db
from reg_meta.catalog import Catalog


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
    conn.execute(
        "INSERT INTO register_version "
        "(regver_id, register_variant_id, registerversionnamn, "
        "registerversionbeskrivning, registerversionmatinformation) "
        "VALUES (201, 21, '2019', 'RAMS 2019 description', "
        "'RAMS measurement information')"
    )
    conn.execute(
        "INSERT INTO population (regver_id, name, definition, comment, date_range) "
        "VALUES (201, 'Employees', 'People with employment income', "
        "'Fixture population note', '2019')"
    )
    conn.execute(
        "INSERT INTO population (regver_id, name, definition, comment, date_range) "
        "VALUES (201, '', '', '', '')"
    )
    conn.execute(
        "INSERT INTO object_type (regver_id, name, definition) "
        "VALUES (201, 'Person', 'Individual worker')"
    )
    conn.execute(
        "INSERT INTO object_type (regver_id, name, definition) VALUES (201, '', '')"
    )
    conn.commit()
    return Catalog(conn)


class TestListVariants:
    def test_variant_family_metadata_from_successions(self) -> None:
        cat = _variants_catalog()
        conn = cat._conn
        add_variant(
            conn,
            register_variant_id=25,
            register_id=2,
            slug="legacy",
            name="Legacy",
        )
        conn.execute(
            "UPDATE register_variant SET display_group = ? WHERE register_variant_id = ?",
            ("Survey frame, old", 25),
        )
        conn.execute(
            "UPDATE register_variant SET display_group = ? WHERE register_variant_id = ?",
            ("Survey frame, current", 21),
        )
        conn.execute(
            "INSERT INTO variant_replaced_by ("
            "predecessor_provider, predecessor_register, predecessor_variant, "
            "successor_provider, successor_register, successor_variant, "
            "effective_year, note) VALUES "
            "('scb', 'rams', 'legacy', 'scb', 'rams', 'standard', 2019, "
            "'curated:slug_toml')"
        )
        conn.commit()

        by_slug = {v.slug: v for v in cat.list_variants("scb", "rams")}
        assert by_slug["legacy"].variant_family == "standard"
        assert by_slug["standard"].variant_family == "standard"
        assert by_slug["legacy"].variant_family_label == "Survey frame"
        assert by_slug["extended"].variant_family is None

    def test_identical_comma_variant_family_labels_stay_whole(self) -> None:
        cat = _variants_catalog()
        conn = cat._conn
        add_variant(
            conn,
            register_variant_id=25,
            register_id=2,
            slug="legacy",
            name="Legacy",
        )
        conn.execute(
            "UPDATE register_variant SET display_group = ? WHERE register_variant_id IN (?, ?)",
            ("Survey frame, baseline", 21, 25),
        )
        conn.execute(
            "INSERT INTO variant_replaced_by ("
            "predecessor_provider, predecessor_register, predecessor_variant, "
            "successor_provider, successor_register, successor_variant, "
            "effective_year, note) VALUES "
            "('scb', 'rams', 'legacy', 'scb', 'rams', 'standard', 2019, "
            "'curated:slug_toml')"
        )
        conn.commit()

        by_slug = {v.slug: v for v in cat.list_variants("scb", "rams")}
        assert by_slug["legacy"].variant_family_label == "Survey frame, baseline"
