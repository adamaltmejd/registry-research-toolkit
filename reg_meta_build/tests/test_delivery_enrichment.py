"""Validate accepted delivery-description and alias input declarations."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.delivery_enrichment import (
    load_delivery_enrichment,
)

if TYPE_CHECKING:
    from pathlib import Path


def _write(path: Path, body: str) -> Path:
    path.write_text(body, encoding="utf-8")
    return path


# ── loader ───────────────────────────────────────────────────────────────────


class TestLoader:
    def test_none_path_is_empty(self) -> None:
        assert load_delivery_enrichment(None).descriptions == ()

    def test_valid_entries_parse(self, tmp_path: Path) -> None:
        toml = _write(
            tmp_path / "delivery_enrichment.generated.toml",
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
            tmp_path / "delivery_enrichment.generated.toml",
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
        toml = _write(tmp_path / "delivery_enrichment.generated.toml", body)
        with pytest.raises(RegMetaError) as exc:
            load_delivery_enrichment(toml)
        assert exc.value.exit_code == EXIT_CONFIG

    def test_duplicate_register_variable_fails(self, tmp_path: Path) -> None:
        toml = _write(
            tmp_path / "delivery_enrichment.generated.toml",
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
            tmp_path / "delivery_enrichment.generated.toml",
            '[[description]]\nregister = "scb/agi"\nvariable = "kon"\n'
            'description = "A"\n\n'
            '[[description]]\nregister = "scb/lisa"\nvariable = "kon"\n'
            'description = "B"\n',
        )
        assert len(load_delivery_enrichment(toml).descriptions) == 2

    def test_non_string_provenance_fails(self, tmp_path: Path) -> None:
        toml = _write(
            tmp_path / "delivery_enrichment.generated.toml",
            '[[description]]\nregister = "scb/agi"\nvariable = "kon"\n'
            'description = "Kön"\nprovenance = 7\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_delivery_enrichment(toml)
        assert exc.value.exit_code == EXIT_CONFIG

    def test_excluded_provider_shape_is_still_validated(self, tmp_path: Path) -> None:
        toml = _write(
            tmp_path / "delivery_enrichment.generated.toml",
            '[[description]]\nregister = "sos/par"\ndescription = "missing variable"\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_delivery_enrichment(toml)
        assert exc.value.exit_code == EXIT_CONFIG


# ── alias loader ─────────────────────────────────────────────────────────────


class TestAliasLoader:
    def test_mixed_file_parses_both_kinds(self, tmp_path: Path) -> None:
        toml = _write(
            tmp_path / "delivery_enrichment.generated.toml",
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
            tmp_path / "delivery_enrichment.generated.toml",
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
        toml = _write(tmp_path / "delivery_enrichment.generated.toml", body)
        with pytest.raises(RegMetaError) as exc:
            load_delivery_enrichment(toml)
        assert exc.value.exit_code == EXIT_CONFIG

    def test_duplicate_alias_triple_fails(self, tmp_path: Path) -> None:
        toml = _write(
            tmp_path / "delivery_enrichment.generated.toml",
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
            tmp_path / "delivery_enrichment.generated.toml",
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
