"""Steward extension adapter contract; global sources use curated_records."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.id import mint
from reg_meta_build.ir import (
    IRRegister,
    IRVariable,
    IRVariableAliasWindow,
    IRVariableState,
    IRVariant,
)
from reg_meta_build.sources.curated import CuratedAdapter

if TYPE_CHECKING:
    from pathlib import Path


def test_steward_contract_emits_multistate_alias_windows_and_prefixed_ids(
    tmp_path: Path,
) -> None:
    toml = """\
[provider]
name = "Private Provider"
source_label = "steward-delivery-2026-01-01"

[[register]]
key = "r"
name = "R"

  [[register.variant]]
  key = "a"
  name = "A"

  [[register.variable]]
  key = "amount"
  name = "Amount"
  variants = ["a"]

    [[register.variable.state]]
    column = "AMOUNT"
    data_type = "float"
    valid_to = "2020"

    [[register.variable.state]]
    column = "AMOUNT_SEK"
    data_type = "float"
    valid_from = "2021"
    aliases = ["Amount-SEK"]
"""
    src = tmp_path / "src"
    src.mkdir()
    (src / "private.toml").write_text(toml, encoding="utf-8")
    adapter = CuratedAdapter("private", steward="swecov")
    objs = list(adapter.emit(src))

    register = next(o for o in objs if isinstance(o, IRRegister))
    variant = next(o for o in objs if isinstance(o, IRVariant))
    variable = next(o for o in objs if isinstance(o, IRVariable))
    states = [o for o in objs if isinstance(o, IRVariableState)]
    windows = [o for o in objs if isinstance(o, IRVariableAliasWindow)]
    assert adapter.provider_name == "Private Provider"
    assert register.register_id == mint("register", "private", "r")
    assert variant.register_variant_id == mint("variant", "private", "r", "a")
    assert variable.variable_id == mint("variable", "private", "r", "amount")
    assert variable.provider_key == "amount"
    assert variable.source_label == "steward-delivery-2026-01-01"
    assert [(s.delivery_column_name, s.valid_from, s.valid_to) for s in states] == [
        ("AMOUNT", None, "2020-12-31"),
        ("AMOUNT_SEK", "2021-01-01", None),
    ]
    assert {w.delivery_column_name for w in windows} == {"AMOUNT_SEK", "Amount-SEK"}


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
