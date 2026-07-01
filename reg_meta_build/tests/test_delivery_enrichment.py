"""Tests for the delivery-list enrichment overlay (#365 PR1a;
`delivery_enrichment.py`).

Covers the TOML loader (structural validation, EXIT_CONFIG on defects) and the
description-backfill apply pass against hand-curated slugged DBs: gap-fill-only
(never overwrites), lenient unresolved-slug handling, idempotency, and the
provider gate."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _slugged_db import add_variable, build_slugged_db
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.delivery_enrichment import (
    CuratedAlias,
    DeliveryEnrichment,
    DescriptionBackfill,
    apply_delivery_enrichment,
    load_delivery_enrichment,
)

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path

_SCB = frozenset({"scb"})


def _desc(counts: dict[str, int]) -> dict[str, int]:
    """The description-backfill counts only (apply also returns alias_* keys)."""
    return {k: counts[k] for k in ("applied", "skipped", "unresolved")}


def _alias(counts: dict[str, int]) -> dict[str, int]:
    return {
        k.removeprefix("alias_"): counts[k] for k in counts if k.startswith("alias_")
    }


def _bf(variable: str, description: str, register: str = "lisa") -> DescriptionBackfill:
    return DescriptionBackfill(
        provider="scb",
        register=register,
        variable=variable,
        description=description,
        provenance="test.xlsx",
    )


def _description(conn: sqlite3.Connection, slug: str) -> str | None:
    return conn.execute(
        "SELECT description FROM variable WHERE slug = ?", (slug,)
    ).fetchone()[0]


def _set_description(conn: sqlite3.Connection, slug: str, value: str) -> None:
    conn.execute("UPDATE variable SET description = ? WHERE slug = ?", (value, slug))


def _write(path: Path, body: str) -> Path:
    path.write_text(body, encoding="utf-8")
    return path


# ── loader ───────────────────────────────────────────────────────────────────


class TestLoader:
    def test_none_path_is_empty(self) -> None:
        assert load_delivery_enrichment(None).descriptions == ()

    def test_valid_entries_parse(self, tmp_path: Path) -> None:
        toml = _write(
            tmp_path / "delivery_enrichment.toml",
            '[[description]]\nregister = "scb/agi"\nvariable = "kon"\n'
            'description = "Kön"\nprovenance = "p.xlsx"\n\n'
            '[[description]]\nregister = "scb/lisa"\nvariable = "ink"\n'
            'description = "Inkomst"\n',
        )
        enr = load_delivery_enrichment(toml)
        assert [
            (d.provider, d.register, d.variable, d.description)
            for d in enr.descriptions
        ] == [
            ("scb", "agi", "kon", "Kön"),
            ("scb", "lisa", "ink", "Inkomst"),
        ]
        # provenance is optional
        assert enr.descriptions[1].provenance == ""

    def test_unknown_top_level_key_fails(self, tmp_path: Path) -> None:
        toml = _write(
            tmp_path / "delivery_enrichment.toml",
            '[[descriptions]]\nregister = "scb/agi"\nvariable = "kon"\n'
            'description = "Kön"\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_delivery_enrichment(toml)
        assert exc.value.exit_code == EXIT_CONFIG

    @pytest.mark.parametrize(
        "body",
        [
            '[[description]]\nvariable = "kon"\ndescription = "Kön"\n',  # no register
            '[[description]]\nregister = "scb/agi"\ndescription = "Kön"\n',  # no variable
            '[[description]]\nregister = "scb/agi"\nvariable = "kon"\n',  # no description
            '[[description]]\nregister = "agi"\nvariable = "kon"\ndescription = "K"\n',  # 1-seg FQID
            '[[description]]\nregister = "scb/x/y"\nvariable = "kon"\ndescription = "K"\n',  # 3-seg
            '[[description]]\nregister = "scb/agi"\nvariable = "a/b"\ndescription = "K"\n',  # variable path
        ],
    )
    def test_malformed_entry_fails(self, tmp_path: Path, body: str) -> None:
        toml = _write(tmp_path / "delivery_enrichment.toml", body)
        with pytest.raises(RegMetaError) as exc:
            load_delivery_enrichment(toml)
        assert exc.value.exit_code == EXIT_CONFIG

    def test_duplicate_register_variable_fails(self, tmp_path: Path) -> None:
        toml = _write(
            tmp_path / "delivery_enrichment.toml",
            '[[description]]\nregister = "scb/agi"\nvariable = "kon"\n'
            'description = "A"\n\n'
            '[[description]]\nregister = "scb/agi"\nvariable = "kon"\n'
            'description = "B"\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_delivery_enrichment(toml)
        assert exc.value.exit_code == EXIT_CONFIG

    def test_same_variable_slug_different_register_ok(self, tmp_path: Path) -> None:
        toml = _write(
            tmp_path / "delivery_enrichment.toml",
            '[[description]]\nregister = "scb/agi"\nvariable = "kon"\n'
            'description = "A"\n\n'
            '[[description]]\nregister = "scb/lisa"\nvariable = "kon"\n'
            'description = "B"\n',
        )
        assert len(load_delivery_enrichment(toml).descriptions) == 2

    def test_non_string_provenance_fails(self, tmp_path: Path) -> None:
        toml = _write(
            tmp_path / "delivery_enrichment.toml",
            '[[description]]\nregister = "scb/agi"\nvariable = "kon"\n'
            'description = "Kön"\nprovenance = 7\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_delivery_enrichment(toml)
        assert exc.value.exit_code == EXIT_CONFIG


# ── apply ────────────────────────────────────────────────────────────────────


class TestApply:
    def test_fills_empty_description(self) -> None:
        conn = build_slugged_db(classification=None)
        add_variable(conn, register_id=1, var_id=90, name="Inkomst", slug="ink")
        assert _description(conn, "ink") is None

        counts = apply_delivery_enrichment(
            conn, DeliveryEnrichment((_bf("ink", "Inkomst av tjänst"),)), providers=_SCB
        )
        assert _desc(counts) == {"applied": 1, "skipped": 0, "unresolved": 0}
        assert _description(conn, "ink") == "Inkomst av tjänst"

    def test_does_not_overwrite_existing_description(self) -> None:
        conn = build_slugged_db(classification=None)
        add_variable(conn, register_id=1, var_id=90, name="Inkomst", slug="ink")
        _set_description(conn, "ink", "Official SCB description")

        counts = apply_delivery_enrichment(
            conn,
            DeliveryEnrichment((_bf("ink", "Delivery-list text"),)),
            providers=_SCB,
        )
        assert _desc(counts) == {"applied": 0, "skipped": 1, "unresolved": 0}
        assert _description(conn, "ink") == "Official SCB description"

    def test_blank_whitespace_description_is_gap_filled(self) -> None:
        conn = build_slugged_db(classification=None)
        add_variable(conn, register_id=1, var_id=90, name="Inkomst", slug="ink")
        _set_description(conn, "ink", "   ")  # whitespace-only counts as empty

        counts = apply_delivery_enrichment(
            conn, DeliveryEnrichment((_bf("ink", "Filled"),)), providers=_SCB
        )
        assert counts["applied"] == 1
        assert _description(conn, "ink") == "Filled"

    def test_unresolved_slug_is_counted_not_fatal(self) -> None:
        conn = build_slugged_db(classification=None)
        warnings: list[str] = []

        counts = apply_delivery_enrichment(
            conn,
            DeliveryEnrichment((_bf("does-not-exist", "x"),)),
            providers=_SCB,
            warn=warnings.append,
        )
        assert _desc(counts) == {"applied": 0, "skipped": 0, "unresolved": 1}
        assert any("did not resolve" in w for w in warnings)

    def test_wrong_register_does_not_cross_resolve(self) -> None:
        conn = build_slugged_db(classification=None)
        add_variable(conn, register_id=1, var_id=90, name="Inkomst", slug="ink")
        # register 'agi' isn't in the fixture (only lisa) → unresolved, not applied
        counts = apply_delivery_enrichment(
            conn, DeliveryEnrichment((_bf("ink", "x", register="agi"),)), providers=_SCB
        )
        assert _desc(counts) == {"applied": 0, "skipped": 0, "unresolved": 1}

    def test_provider_gate_skips_inactive_provider(self) -> None:
        conn = build_slugged_db(classification=None)
        add_variable(conn, register_id=1, var_id=90, name="Inkomst", slug="ink")
        # scb entry, but only sos is active → filtered out entirely (not unresolved)
        counts = apply_delivery_enrichment(
            conn,
            DeliveryEnrichment((_bf("ink", "x"),)),
            providers=frozenset({"sos"}),
        )
        assert _desc(counts) == {"applied": 0, "skipped": 0, "unresolved": 0}
        assert _description(conn, "ink") is None

    def test_idempotent_second_run_is_skip(self) -> None:
        conn = build_slugged_db(classification=None)
        add_variable(conn, register_id=1, var_id=90, name="Inkomst", slug="ink")
        enr = DeliveryEnrichment((_bf("ink", "Inkomst av tjänst"),))

        first = apply_delivery_enrichment(conn, enr, providers=_SCB)
        second = apply_delivery_enrichment(conn, enr, providers=_SCB)
        assert first["applied"] == 1
        assert _desc(second) == {"applied": 0, "skipped": 1, "unresolved": 0}


# ── alias loader ─────────────────────────────────────────────────────────────


def _ca(variable: str, delivery_column: str, register: str = "lisa") -> CuratedAlias:
    return CuratedAlias(
        provider="scb",
        register=register,
        variable=variable,
        delivery_column=delivery_column,
        provenance="",
    )


def _alias_columns(conn: sqlite3.Connection, slug: str) -> set[str]:
    return {
        r[0]
        for r in conn.execute(
            "SELECT va.delivery_column_name FROM variable_alias va "
            "JOIN variable v ON va.variable_id = v.variable_id WHERE v.slug = ?",
            (slug,),
        )
    }


class TestAliasLoader:
    def test_mixed_file_parses_both_kinds(self, tmp_path: Path) -> None:
        toml = _write(
            tmp_path / "delivery_enrichment.toml",
            '[[description]]\nregister = "scb/agi"\nvariable = "kon"\n'
            'description = "Kön"\n\n'
            '[[alias]]\nregister = "scb/fek"\nvariable = "foradlingsvarde"\n'
            'delivery_column = "Foradlingsvarde"\n',
        )
        enr = load_delivery_enrichment(toml)
        assert len(enr.descriptions) == 1
        assert [(a.register, a.variable, a.delivery_column) for a in enr.aliases] == [
            ("fek", "foradlingsvarde", "Foradlingsvarde")
        ]

    def test_alias_only_file_parses(self, tmp_path: Path) -> None:
        toml = _write(
            tmp_path / "delivery_enrichment.toml",
            '[[alias]]\nregister = "scb/fek"\nvariable = "v"\n'
            'delivery_column = "Col"\n',
        )
        enr = load_delivery_enrichment(toml)
        assert enr.descriptions == ()
        assert len(enr.aliases) == 1

    @pytest.mark.parametrize(
        "body",
        [
            '[[alias]]\nregister = "scb/fek"\nvariable = "v"\n',  # no delivery_column
            '[[alias]]\nregister = "fek"\nvariable = "v"\ndelivery_column = "C"\n',  # 1-seg
            '[[alias]]\nregister = "scb/fek"\nvariable = "a/b"\ndelivery_column = "C"\n',  # path
        ],
    )
    def test_malformed_alias_fails(self, tmp_path: Path, body: str) -> None:
        toml = _write(tmp_path / "delivery_enrichment.toml", body)
        with pytest.raises(RegMetaError) as exc:
            load_delivery_enrichment(toml)
        assert exc.value.exit_code == EXIT_CONFIG

    def test_duplicate_alias_triple_fails(self, tmp_path: Path) -> None:
        toml = _write(
            tmp_path / "delivery_enrichment.toml",
            '[[alias]]\nregister = "scb/fek"\nvariable = "v"\ndelivery_column = "Col"\n\n'
            '[[alias]]\nregister = "scb/fek"\nvariable = "v"\ndelivery_column = "col"\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_delivery_enrichment(toml)
        assert exc.value.exit_code == EXIT_CONFIG

    def test_same_variable_can_have_multiple_delivery_aliases(
        self, tmp_path: Path
    ) -> None:
        toml = _write(
            tmp_path / "delivery_enrichment.toml",
            '[[alias]]\nregister = "scb/gymnasieskola-betyg"\n'
            'variable = "kurs"\ndelivery_column = "Amneskod_omkodad"\n\n'
            '[[alias]]\nregister = "scb/gymnasieskola-betyg"\n'
            'variable = "kurs"\ndelivery_column = "Kurskod_omkodad"\n',
        )
        aliases = load_delivery_enrichment(toml).aliases
        assert [(a.variable, a.delivery_column) for a in aliases] == [
            ("kurs", "Amneskod_omkodad"),
            ("kurs", "Kurskod_omkodad"),
        ]


class TestApplyAliases:
    def test_inserts_alias_on_variant_with_state(self) -> None:
        conn = build_slugged_db()  # scb/lisa/kon, state + alias "Kon" under variant 10
        counts = apply_delivery_enrichment(
            conn, DeliveryEnrichment((), (_ca("kon", "LopNr_Kon"),)), providers=_SCB
        )
        assert _alias(counts) == {"applied": 1, "skipped": 0, "unresolved": 0}
        assert _alias_columns(conn, "kon") == {"Kon", "LopNr_Kon"}

    def test_existing_column_is_skipped(self) -> None:
        conn = build_slugged_db()
        counts = apply_delivery_enrichment(
            conn, DeliveryEnrichment((), (_ca("kon", "Kon"),)), providers=_SCB
        )
        assert _alias(counts) == {"applied": 0, "skipped": 1, "unresolved": 0}

    def test_idempotent_second_run_skips(self) -> None:
        conn = build_slugged_db()
        enr = DeliveryEnrichment((), (_ca("kon", "LopNr_Kon"),))
        apply_delivery_enrichment(conn, enr, providers=_SCB)
        second = apply_delivery_enrichment(conn, enr, providers=_SCB)
        assert _alias(second) == {"applied": 0, "skipped": 1, "unresolved": 0}

    def test_unresolved_variable_counted(self) -> None:
        conn = build_slugged_db()
        counts = apply_delivery_enrichment(
            conn, DeliveryEnrichment((), (_ca("nope", "X"),)), providers=_SCB
        )
        assert _alias(counts) == {"applied": 0, "skipped": 0, "unresolved": 1}

    def test_variable_without_state_is_unresolved(self) -> None:
        conn = build_slugged_db()
        # a variable with NO variable_state → no variant to attach to
        add_variable(conn, register_id=1, var_id=91, name="Bistånd", slug="stateless")
        counts = apply_delivery_enrichment(
            conn, DeliveryEnrichment((), (_ca("stateless", "X"),)), providers=_SCB
        )
        assert _alias(counts) == {"applied": 0, "skipped": 0, "unresolved": 1}

    def test_provider_gate_skips_inactive(self) -> None:
        conn = build_slugged_db()
        counts = apply_delivery_enrichment(
            conn,
            DeliveryEnrichment((), (_ca("kon", "LopNr_Kon"),)),
            providers=frozenset({"sos"}),
        )
        assert _alias(counts) == {"applied": 0, "skipped": 0, "unresolved": 0}
        assert "LopNr_Kon" not in _alias_columns(conn, "kon")
