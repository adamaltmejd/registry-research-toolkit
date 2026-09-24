"""Register-scoped alias-window loader validation."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.alias_windows import load_alias_windows

if TYPE_CHECKING:
    from pathlib import Path


def _write_register(tmp_path: Path, entry: str) -> Path:
    root = tmp_path / "curation"
    path = root / "registers" / "scb" / "testreg.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        '[register]\nprovider = "scb"\nslug = "testreg"\nnative_id = "1"\n\n'
        + entry,
        encoding="utf-8",
    )
    return root


def _entry(variable: str = "scb/testreg/test-variable", extra: str = "") -> str:
    return (
        "[[representation.alias_window]]\n"
        f'variable = "{variable}"\n'
        'variant = "test-variant"\ncolumn = "AEBUY"\n'
        'source_editions = ["2018"]\nevidence = "held"\n'
        'noted = "2026-09-13"\n'
        + extra
    )


def test_valid_alias_window_parses(tmp_path: Path) -> None:
    root = _write_register(tmp_path, _entry())
    (alias,) = load_alias_windows(root)
    assert (alias.fqid, alias.variant, alias.column, alias.source_editions) == (
        "scb/testreg/test-variable",
        "test-variant",
        "AEBUY",
        ("2018",),
    )


def test_unknown_key_is_rejected(tmp_path: Path) -> None:
    root = _write_register(tmp_path, _entry(extra='witness = "2015"\n'))
    with pytest.raises(RegMetaError) as exc:
        load_alias_windows(root)
    assert exc.value.exit_code == EXIT_CONFIG
    assert "entry 1" in exc.value.message


def test_wrong_register_is_rejected(tmp_path: Path) -> None:
    root = _write_register(tmp_path, _entry("scb/other/test-variable"))
    with pytest.raises(RegMetaError) as exc:
        load_alias_windows(root)
    assert exc.value.exit_code == EXIT_CONFIG
    assert "does not match" in exc.value.message


def test_invalid_noted_date_is_rejected(tmp_path: Path) -> None:
    root = _write_register(tmp_path, _entry().replace("2026-09-13", "soon"))
    with pytest.raises(RegMetaError) as exc:
        load_alias_windows(root)
    assert "YYYY-MM-DD" in exc.value.message
