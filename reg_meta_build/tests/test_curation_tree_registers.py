"""Strict per-register TOML contracts."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.curation_tree import load_register_files

if TYPE_CHECKING:
    from pathlib import Path


_TABLES = (
    (
        "errata.delivered",
        '[[errata.delivered]]\nvariant = "v"\ncolumn = "C"\n'
        'versions = ["2020"]\nevidence = "source"\nnoted = "2026-09-24"\n',
        None,
    ),
    (
        "errata.column",
        '[[errata.column]]\nvariant = "v"\ncolumn = "C"\nname = "Name"\n'
        'definition = "Definition"\nsource = "scb-docs"\nevidence = "source"\n'
        'noted = "2026-09-24"\n',
        None,
    ),
    (
        "errata.version",
        '[[errata.version]]\nvariant = "v"\nname = "2020"\n'
        'evidence = "source"\nnoted = "2026-09-24"\n',
        None,
    ),
    (
        "enrichment.description",
        '[[enrichment.description]]\nregister = "scb/test"\n'
        'variable = "v"\ndescription = "Description"\n',
        ('register = "scb/test"', 'register = "scb/other"'),
    ),
    (
        "enrichment.alias",
        '[[enrichment.alias]]\nregister = "scb/test"\n'
        'variable = "v"\ndelivery_column = "C"\n',
        ('register = "scb/test"', 'register = "scb/other"'),
    ),
    (
        "group",
        '[[group]]\nregister = "scb/test"\nkey = "family"\nlabel = "Family"\n'
        'axis = "rank"\nmembers = [{ variable = "v", value = "1", label = "One" }]\n',
        ('register = "scb/test"', 'register = "scb/other"'),
    ),
    (
        "code_label_pair",
        '[[code_label_pair]]\ncode = "scb/test/code"\n'
        'label = "scb/test/label"\n',
        ('code = "scb/test/code"', 'code = "scb/other/code"'),
    ),
    (
        "representation.period_family",
        '[[representation.period_family]]\nregister = "scb/test"\n'
        'family_stem = "income"\nlabel = "Income"\n',
        ('register = "scb/test"', 'register = "scb/other"'),
    ),
    (
        "representation.alias_window",
        '[[representation.alias_window]]\nvariable = "scb/test/v"\n'
        'variant = "variant"\ncolumn = "C"\nsource_editions = ["2020"]\n'
        'evidence = "source"\nnoted = "2026-09-24"\n',
        ('variable = "scb/test/v"', 'variable = "scb/other/v"'),
    ),
    (
        "identity.partition",
        '[[identity.partition]]\nvariable = "1.2"\ncolumns = { C = "1.2.c" }\n',
        ('variable = "1.2"', 'variable = "2.2"'),
    ),
    (
        "identity.column_owner",
        '[[identity.column_owner]]\nvariable = "1.2"\nvariant = "1.3"\n'
        'column = "C"\nowner = "1.2.c"\nref = "source"\n',
        ('variable = "1.2"', 'variable = "2.2"'),
    ),
    (
        "identity.route",
        '[[identity.route]]\ndeldatamangd = "TOKEN"\nvariants = ["Variant"]\n',
        None,
    ),
    (
        "identity.split",
        '[[identity.split]]\nvariable = "NAME"\nby = "data_type"\n'
        'parts = [{ data_type = "text", owner = "1.NAME.text" }]\n',
        None,
    ),
    (
        "identity.rename",
        '[[identity.rename]]\ndeldatamangd = "TOKEN"\nvariable = "OLD"\n'
        'name = "Name"\ncolumn = "NEW"\n',
        None,
    ),
    (
        "acknowledge",
        '[[acknowledge]]\ncode = "code"\nsubject = "subject"\n'
        'refs = ["source"]\nreason = "reason"\nevidence = "evidence"\n',
        None,
    ),
)


def _write_register(root: Path, body: str, *, slug: str = "test") -> Path:
    path = root / "registers" / "scb" / f"{slug}.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'[register]\nprovider = "scb"\nslug = "{slug}"\n'
        'native_id = "1"\n\n' + body,
        encoding="utf-8",
    )
    return path


@pytest.mark.parametrize(("table", "body", "wrong_register"), _TABLES)
def test_each_register_table_loads(
    tmp_path: Path, table: str, body: str, wrong_register: tuple[str, str] | None
) -> None:
    root = tmp_path / "curation"
    _write_register(root, body)
    (entry,) = load_register_files(root)
    assert entry.register_info.slug == "test"
    assert table


@pytest.mark.parametrize(("table", "body", "wrong_register"), _TABLES)
def test_each_register_table_rejects_unknown_keys(
    tmp_path: Path, table: str, body: str, wrong_register: tuple[str, str] | None
) -> None:
    root = tmp_path / "curation"
    _write_register(root, body + 'unknown_key = "typo"\n')
    with pytest.raises(RegMetaError) as exc:
        load_register_files(root)
    assert exc.value.exit_code == EXIT_CONFIG
    assert "registers/scb/test.toml" in exc.value.message
    assert "entry 1" in exc.value.message


@pytest.mark.parametrize(
    ("table", "body", "wrong_register"),
    [(table, body, mismatch) for table, body, mismatch in _TABLES if mismatch],
)
def test_register_scoped_entries_reject_wrong_register(
    tmp_path: Path,
    table: str,
    body: str,
    wrong_register: tuple[str, str],
) -> None:
    root = tmp_path / "curation"
    _write_register(root, body.replace(*wrong_register))
    with pytest.raises(RegMetaError) as exc:
        load_register_files(root)
    assert exc.value.exit_code == EXIT_CONFIG
    assert table in exc.value.message
    assert "does not match" in exc.value.message or "does not belong" in exc.value.message


def test_register_path_mismatch_fails(tmp_path: Path) -> None:
    root = tmp_path / "curation"
    _write_register(root, "", slug="other")
    path = root / "registers" / "scb" / "test.toml"
    path.write_text(
        '[register]\nprovider = "scb"\nslug = "other"\nnative_id = "1"\n',
        encoding="utf-8",
    )
    with pytest.raises(RegMetaError) as exc:
        load_register_files(root)
    assert "does not match its path" in exc.value.message


def test_two_files_for_one_register_fail(tmp_path: Path) -> None:
    root = tmp_path / "curation"
    _write_register(root, "")
    duplicate = root / "registers" / "scb" / "family" / "test.toml"
    duplicate.parent.mkdir(parents=True)
    duplicate.write_text(
        '[register]\nprovider = "scb"\nslug = "test"\nnative_id = "1"\n',
        encoding="utf-8",
    )
    with pytest.raises(RegMetaError) as exc:
        load_register_files(root)
    assert "duplicate register scb/test" in exc.value.message


def test_duplicate_native_id_fails(tmp_path: Path) -> None:
    root = tmp_path / "curation"
    _write_register(root, "", slug="first")
    _write_register(root, "", slug="second")
    with pytest.raises(RegMetaError) as exc:
        load_register_files(root)
    assert "duplicate native_id '1'" in exc.value.message


def test_unknown_top_level_table_fails(tmp_path: Path) -> None:
    root = tmp_path / "curation"
    _write_register(root, '[unexpected]\nvalue = "typo"\n')
    with pytest.raises(RegMetaError) as exc:
        load_register_files(root)
    assert "unexpected" in exc.value.message
    assert "registers/scb/test.toml" in exc.value.message


def test_duplicate_table_entry_fails_with_file_and_index(tmp_path: Path) -> None:
    root = tmp_path / "curation"
    body = (
        '[[identity.partition]]\nvariable = "1.2"\ncolumns = { C = "1.2.c" }\n'
        '[[identity.partition]]\nvariable = "1.2"\ncolumns = { C = "1.2.c" }\n'
    )
    _write_register(root, body)
    with pytest.raises(RegMetaError) as exc:
        load_register_files(root)
    assert "registers/scb/test.toml" in exc.value.message
    assert "identity.partition" in exc.value.message
    assert "entry 2" in exc.value.message
