"""Register-scoped delivery enrichment reader validation."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.curation_tree import load_register_files

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
    (register,) = load_register_files(root)
    enrichment = register.enrichment
    assert [
        (d.register_fqid, d.variable, d.description) for d in enrichment.description
    ] == [("scb/agi", "kon", "Kön")]
    assert [(a.variable, a.delivery_column) for a in enrichment.alias] == [
        ("kon", "KON")
    ]


def test_unknown_key_is_rejected_with_file_and_entry(tmp_path: Path) -> None:
    root = _write_register(
        tmp_path,
        '[[enrichment.description]]\nregister = "scb/agi"\n'
        'variable = "kon"\ndescription = "Kön"\ndescripton = "typo"\n',
    )
    with pytest.raises(RegMetaError) as exc:
        load_register_files(root)
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
        load_register_files(root)
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
        load_register_files(root)
    assert exc.value.exit_code == EXIT_CONFIG


@pytest.mark.parametrize(
    "body",
    [
        '[[enrichment.description]]\nregister = "scb/agi"\n'
        'variable = "kon"\ndescription = " "\n',
        '[[enrichment.alias]]\nregister = "scb/agi"\n'
        'variable = "scb/agi/kon"\ndelivery_column = "KON"\n',
        '[[enrichment.alias]]\nregister = "scb/agi"\n'
        'variable = "kon"\ndelivery_column = "KON"\n'
        '[[enrichment.alias]]\nregister = "scb/agi"\n'
        'variable = "kon"\ndelivery_column = "kon"\n',
    ],
)
def test_enrichment_contract_rejects_blank_text_paths_and_duplicate_aliases(
    tmp_path: Path, body: str
) -> None:
    root = _write_register(tmp_path, body)
    with pytest.raises(RegMetaError) as exc:
        load_register_files(root)
    assert exc.value.exit_code == EXIT_CONFIG
