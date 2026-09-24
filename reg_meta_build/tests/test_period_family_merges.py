"""Register-scoped representation-period loader validation."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.period_family_merges import PeriodFamily, load_period_family_merges

if TYPE_CHECKING:
    from pathlib import Path


def _write_register(tmp_path: Path, body: str) -> Path:
    root = tmp_path / "curation"
    path = root / "registers" / "scb" / "lisa.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        '[register]\nprovider = "scb"\nslug = "lisa"\nnative_id = "34"\n\n' + body,
        encoding="utf-8",
    )
    return root


def test_load_period_family_parses_explicit_slug(tmp_path: Path) -> None:
    root = _write_register(
        tmp_path,
        '[[representation.period_family]]\nregister = "scb/lisa"\n'
        'family_stem = "lonfink"\nlabel = "Lön per månad"\n'
        'slug = "lone-eller-foretagarinkomst-manad"\n',
    )
    assert load_period_family_merges(root) == (
        PeriodFamily(
            "scb",
            "lisa",
            "lonfink",
            "Lön per månad",
            "lone-eller-foretagarinkomst-manad",
        ),
    )


def test_load_period_family_rejects_unknown_key(tmp_path: Path) -> None:
    root = _write_register(
        tmp_path,
        '[[representation.period_family]]\nregister = "scb/lisa"\n'
        'family_stem = "lonfink"\nlabel = "Lön"\nunknown = "x"\n',
    )
    with pytest.raises(RegMetaError) as exc:
        load_period_family_merges(root)
    assert exc.value.exit_code == EXIT_CONFIG
    assert "entry 1" in exc.value.message


def test_load_period_family_rejects_wrong_register(tmp_path: Path) -> None:
    root = _write_register(
        tmp_path,
        '[[representation.period_family]]\nregister = "scb/rams"\n'
        'family_stem = "lonfink"\nlabel = "Lön"\n',
    )
    with pytest.raises(RegMetaError) as exc:
        load_period_family_merges(root)
    assert exc.value.exit_code == EXIT_CONFIG
    assert "does not match" in exc.value.message


def test_load_period_family_rejects_duplicate_stem(tmp_path: Path) -> None:
    root = _write_register(
        tmp_path,
        '[[representation.period_family]]\nregister = "scb/lisa"\n'
        'family_stem = "x"\nlabel = "A"\n\n'
        '[[representation.period_family]]\nregister = "scb/lisa"\n'
        'family_stem = "x"\nlabel = "B"\n',
    )
    with pytest.raises(RegMetaError) as exc:
        load_period_family_merges(root)
    assert exc.value.code == "period_family_merges_invalid"


def test_load_period_family_empty_when_no_tree() -> None:
    assert load_period_family_merges(None) == ()
