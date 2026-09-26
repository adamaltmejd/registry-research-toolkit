"""The curation tree reader rejects malformed classification files."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from reg_meta.errors import RegMetaError
from reg_meta_build.curation_tree import (
    load_classification_families,
    load_classifications,
    load_curation_tree,
)

if TYPE_CHECKING:
    from pathlib import Path


def _book(short_name: str, slug: str, *, binding: str = "", extra: str = "") -> str:
    return (
        f'[classification]\nshort_name = "{short_name}"\nslug = "{slug}"\n'
        f'name = "{short_name}"\ncodes_file = "{slug}.csv"\n{extra}\n'
        f"[binding]\n{binding}"
    )


def _write(root: Path, **files: str) -> Path:
    directory = root / "classifications"
    directory.mkdir(exist_ok=True)
    for short_name, text in files.items():
        (directory / f"{short_name}.toml").write_text(text, encoding="utf-8")
    return root


def _error(root: Path) -> RegMetaError:
    with pytest.raises(RegMetaError) as excinfo:
        load_classifications(root)
    assert excinfo.value.code == "classification_curation_invalid"
    return excinfo.value


def test_reads_files_in_sorted_order(tmp_path: Path) -> None:
    _write(
        tmp_path,
        B=_book("B", "b", binding='value_set_labels = ["Bee"]\n'),
        A=_book(
            "A",
            "a",
            extra='sentinel_codes = [{ code = "99", meaning = "Uppgift saknas" }]',
            binding='[[binding.variable]]\nvariable = "scb/ulf/kod"\n',
        ),
    )
    entries = load_classifications(tmp_path)
    assert [entry.classification.short_name for entry in entries] == ["A", "B"]
    assert [code.code for code in entries[0].classification.sentinel_codes] == ["99"]
    assert [bound.variable for bound in entries[0].binding.variable] == ["scb/ulf/kod"]
    assert entries[1].binding.value_set_labels == ("Bee",)


def test_aliases_and_family_members_are_strict_curated_data(tmp_path: Path) -> None:
    _write(
        tmp_path,
        A=_book(
            "A",
            "a",
            extra='aliases = ["Source A"]\nfamily = "pair"\n'
            'family_aliases = ["Source family"]\n',
        ),
        B=_book("B", "b", extra='family = "pair"\n'),
    )
    books = load_classifications(tmp_path)
    assert books[0].classification.aliases == ("Source A",)
    families = load_classification_families(books).family
    assert families[0].members == ("a", "b")
    assert families[0].aliases == ("Source family",)


def test_single_member_family_fails(tmp_path: Path) -> None:
    _write(tmp_path, A=_book("A", "a", extra='family = "pair"\n'))
    with pytest.raises(RegMetaError) as excinfo:
        load_classification_families(load_classifications(tmp_path))
    assert "at least two distinct slugs" in excinfo.value.message


def test_duplicate_alias_in_one_classification_fails(tmp_path: Path) -> None:
    _write(tmp_path, A=_book("A", "a", extra='aliases = ["same", "same"]\n'))
    assert "classification.aliases" in _error(tmp_path).message


def test_unknown_key_names_file_and_key(tmp_path: Path) -> None:
    _write(tmp_path, A=_book("A", "a", extra='version = "1"'))
    error = _error(tmp_path)
    assert "classifications/A.toml" in error.message
    assert "classification.version" in error.message


def test_unknown_top_level_table_fails(tmp_path: Path) -> None:
    _write(tmp_path, A=_book("A", "a") + '\n[[link]]\nvariable = "scb/ulf/kod"\n')
    assert "`link`" in _error(tmp_path).message


def test_unnormalized_label_fails(tmp_path: Path) -> None:
    _write(tmp_path, A=_book("A", "a", binding='value_set_labels = ["Bee "]\n'))
    error = _error(tmp_path)
    assert "classifications/A.toml" in error.message
    assert "'Bee'" in error.message


def test_label_in_two_files_fails(tmp_path: Path) -> None:
    _write(
        tmp_path,
        A=_book("A", "a", binding='value_set_labels = ["Bee"]\n'),
        B=_book("B", "b", binding='value_set_labels = ["Bee"]\n'),
    )
    assert _error(tmp_path).message == (
        "classifications/B.toml: label 'Bee' is also declared in "
        "classifications/A.toml."
    )


def test_slug_in_two_files_fails(tmp_path: Path) -> None:
    _write(tmp_path, A=_book("A", "same"), B=_book("B", "same"))
    assert _error(tmp_path).message == (
        "classifications/B.toml: slug 'same' is also declared in "
        "classifications/A.toml."
    )


def test_variable_bound_twice_in_one_file_fails(tmp_path: Path) -> None:
    bound = '[[binding.variable]]\nvariable = "scb/ulf/kod"\n'
    _write(tmp_path, A=_book("A", "a", binding=bound + bound))
    assert _error(tmp_path).message == (
        "classifications/A.toml: variable 'scb/ulf/kod' is also declared in "
        "classifications/A.toml."
    )


def test_duplicate_sentinel_code_fails(tmp_path: Path) -> None:
    sentinels = (
        'sentinel_codes = [{ code = "99", meaning = "x" }, '
        '{ code = "99", meaning = "y" }]'
    )
    _write(tmp_path, A=_book("A", "a", extra=sentinels))
    error = _error(tmp_path)
    assert "classifications/A.toml" in error.message
    assert "more than once" in error.message


def test_short_name_must_match_file_name(tmp_path: Path) -> None:
    _write(tmp_path, A=_book("B", "b"))
    assert "does not match the file name" in _error(tmp_path).message


def test_lineage_override_fails(tmp_path: Path) -> None:
    _write(tmp_path, A=_book("A", "a"))
    (tmp_path / "lineage.toml").write_text(
        '[lineage."scb/lisa/kon"]\nsource_register = "rams"\n'
        'source_variant = "individregister"\n',
        encoding="utf-8",
    )
    with pytest.raises(RegMetaError) as excinfo:
        load_curation_tree(tmp_path)
    assert excinfo.value.code == "lineage_invalid"
