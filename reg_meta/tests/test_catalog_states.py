"""Catalog state history: state metadata on `resolve` and `resolve_at` narrowing."""

from __future__ import annotations

from _slugged_db import (
    add_state,
    add_variant,
    build_slugged_db,
)
from catalog_test_support import KON as _KON
from reg_meta.catalog import Catalog


class TestResolveVariableLongitudinal:
    """see DESIGN.md → Catalog API surface: `resolve()` returns the variable's shared metadata + full state
    history, each state tagged with its variant."""

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

    def test_state_carries_variant_family_metadata(self) -> None:
        conn = build_slugged_db()
        add_variant(
            conn,
            register_variant_id=11,
            register_id=1,
            slug="individer-16plus",
            name="Individer 16+",
        )
        conn.execute(
            "UPDATE register_variant SET display_group = ? WHERE register_variant_id = 10",
            ("Individer, 15 år och äldre",),
        )
        conn.execute(
            "UPDATE register_variant SET display_group = ? WHERE register_variant_id = 11",
            ("Individer, 16 år och äldre",),
        )
        conn.execute(
            "INSERT INTO variant_replaced_by ("
            "predecessor_provider, predecessor_register, predecessor_variant, "
            "successor_provider, successor_register, successor_variant, "
            "effective_year, note) VALUES "
            "('scb', 'lisa', 'individer-16plus', 'scb', 'lisa', "
            "'individer-15plus', 2010, 'curated:slug_toml')"
        )
        conn.commit()

        state = Catalog(conn).resolve(_KON).states[0]
        assert state.variant == "individer-15plus"
        assert state.variant_family == "individer-15plus"
        assert state.variant_family_label == "Individer"


class TestResolveAt:
    """see DESIGN.md → Catalog API surface: `resolve_at` — period/variant/version-narrowed list of states."""

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
