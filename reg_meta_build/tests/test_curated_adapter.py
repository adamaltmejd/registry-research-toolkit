"""Steward extension adapter contract; global sources use curated_records."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.sources.curated import CuratedAdapter

if TYPE_CHECKING:
    from pathlib import Path


def test_classification_reference_uses_supplied_books(tmp_path: Path) -> None:
    src = tmp_path / "providers"
    src.mkdir()
    (src / "private.toml").write_text(
        '[provider]\nname = "Private"\nsource_label = "fixture"\n'
        '[[register]]\nkey = "r"\nname = "R"\n'
        '[[register.variant]]\nkey = "a"\nname = "A"\n'
        '[[register.variable]]\nkey = "v"\nname = "V"\n'
        'classification = "NEW-BOOK"\nvariants = ["a"]\n'
        '[[register.variable.state]]\ncolumn = "V"\n',
        encoding="utf-8",
    )
    with pytest.raises(RegMetaError) as exc:
        list(CuratedAdapter("private", steward="swecov").emit(src))
    assert "NEW-BOOK" in exc.value.message
    assert list(
        CuratedAdapter(
            "private",
            steward="swecov",
            classification_short_names=frozenset({"NEW-BOOK"}),
        ).emit(src)
    )


@pytest.mark.parametrize(
    "body, expected",
    [
        (
            'register = [1]\n\n[provider]\nname = "Private"\nsource_label = "test"\n',
            "[[register]]",
        ),
        (
            '[provider]\nname = "Private"\nsource_label = "test"\n\n'
            '[[register]]\nkey = "r"\nname = "R"\nvariant = [1]\n'
            '[[register.variable]]\nkey = "v"\nname = "V"\n'
            '[[register.variable.state]]\ncolumn = "C"\n',
            "variant",
        ),
        (
            '[provider]\nname = "Private"\nsource_label = "test"\n\n'
            '[[register]]\nkey = "r"\nname = "R"\nvariable = [1]\n',
            "[[register.variable]]",
        ),
    ],
)
def test_steward_table_arrays_reject_non_table_elements(
    tmp_path: Path, body: str, expected: str
) -> None:
    src = tmp_path / "src"
    src.mkdir()
    (src / "private.toml").write_text(body, encoding="utf-8")

    with pytest.raises(RegMetaError) as exc:
        list(CuratedAdapter("private", steward="swecov").emit(src))

    assert exc.value.exit_code == EXIT_CONFIG
    assert exc.value.code == "curated_toml_invalid"
    assert expected in exc.value.message
