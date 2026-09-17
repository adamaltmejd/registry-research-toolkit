"""Accepted period-family declaration loader validation."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.period_family_merges import PeriodFamily, load_period_family_merges

if TYPE_CHECKING:
    from pathlib import Path


def test_load_period_family_merges_parses(tmp_path: Path) -> None:
    path = tmp_path / "period_family_merges.toml"
    path.write_text(
        '[[period_family]]\nregister = "scb/lisa"\n'
        'family_stem = "lonfink"\nlabel = "Lön"\n',
        encoding="utf-8",
    )
    families = load_period_family_merges(path)
    assert len(families) == 1
    assert families[0] == PeriodFamily("scb", "lisa", "lonfink", "Lön")


def test_load_period_family_merges_empty_when_no_file() -> None:
    assert load_period_family_merges(None) == ()


@pytest.mark.parametrize(
    "body",
    [
        'register = "lisa"\nfamily_stem = "x"\nlabel = "L"',  # 1-seg register
        'register = "scb/lisa/x"\nfamily_stem = "x"\nlabel = "L"',  # 3-seg register
        'register = "scb/lisa"\nlabel = "L"',  # missing family_stem
        'register = "scb/lisa"\nfamily_stem = "x"',  # missing label
    ],
)
def test_load_period_family_merges_rejects_malformed(tmp_path: Path, body: str) -> None:
    path = tmp_path / "period_family_merges.toml"
    path.write_text(f"[[period_family]]\n{body}\n", encoding="utf-8")
    with pytest.raises(RegMetaError) as exc:
        load_period_family_merges(path)
    assert exc.value.exit_code == EXIT_CONFIG
    assert exc.value.code == "period_family_merges_invalid"


def test_load_period_family_merges_rejects_duplicate(tmp_path: Path) -> None:
    path = tmp_path / "period_family_merges.toml"
    path.write_text(
        '[[period_family]]\nregister = "scb/lisa"\nfamily_stem = "x"\nlabel = "A"\n'
        '[[period_family]]\nregister = "scb/lisa"\nfamily_stem = "x"\nlabel = "B"\n',
        encoding="utf-8",
    )
    with pytest.raises(RegMetaError) as exc:
        load_period_family_merges(path)
    assert exc.value.code == "period_family_merges_invalid"
