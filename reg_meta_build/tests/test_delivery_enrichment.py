"""Register-scoped delivery enrichment reader validation."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.delivery_enrichment import load_delivery_enrichment

if TYPE_CHECKING:
    from pathlib import Path


def _write_register(tmp_path: Path, body: str) -> Path:
    root = tmp_path / "curation"
    path = root / "registers" / "scb" / "agi.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        '[register]\nprovider = "scb"\nslug = "agi"\nnative_id = "1"\n\n' + body,
        encoding="utf-8",
    )
    return root


def test_valid_description_and_alias_parse(tmp_path: Path) -> None:
    root = _write_register(
        tmp_path,
        '[[enrichment.description]]\nregister = "scb/agi"\n'
        'variable = "kon"\ndescription = "Kön"\nprovenance = "p.xlsx"\n\n'
        '[[enrichment.alias]]\nregister = "scb/agi"\nvariable = "kon"\n'
        'delivery_column = "KON"\n',
    )
    enrichment = load_delivery_enrichment(root)
    assert [
        (d.provider, d.register, d.variable, d.description)
        for d in enrichment.descriptions
    ] == [("scb", "agi", "kon", "Kön")]
    assert [(a.variable, a.delivery_column) for a in enrichment.aliases] == [
        ("kon", "KON")
    ]


def test_unknown_key_is_rejected_with_file_and_entry(tmp_path: Path) -> None:
    root = _write_register(
        tmp_path,
        '[[enrichment.description]]\nregister = "scb/agi"\n'
        'variable = "kon"\ndescription = "Kön"\ndescripton = "typo"\n',
    )
    with pytest.raises(RegMetaError) as exc:
        load_delivery_enrichment(root)
    assert exc.value.exit_code == EXIT_CONFIG
    assert "registers/scb/agi.toml" in exc.value.message
    assert "entry 1" in exc.value.message


def test_wrong_register_is_rejected(tmp_path: Path) -> None:
    root = _write_register(
        tmp_path,
        '[[enrichment.description]]\nregister = "scb/lisa"\n'
        'variable = "kon"\ndescription = "Kön"\n',
    )
    with pytest.raises(RegMetaError) as exc:
        load_delivery_enrichment(root)
    assert exc.value.exit_code == EXIT_CONFIG
    assert "does not match" in exc.value.message


def test_duplicate_description_target_fails(tmp_path: Path) -> None:
    root = _write_register(
        tmp_path,
        '[[enrichment.description]]\nregister = "scb/agi"\n'
        'variable = "kon"\ndescription = "A"\n\n'
        '[[enrichment.description]]\nregister = "scb/agi"\n'
        'variable = "kon"\ndescription = "B"\n',
    )
    with pytest.raises(RegMetaError) as exc:
        load_delivery_enrichment(root)
    assert exc.value.exit_code == EXIT_CONFIG


def test_none_path_is_empty() -> None:
    assert load_delivery_enrichment(None).descriptions == ()
