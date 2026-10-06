"""Out-of-slice references into a register whose identity is curated.

A register-slice build classifies an edge into an unselected register through that
register's deferred naming: a name the full build would mint defers with a warning; a
name it would not mint fails the build. Fixtures are synthetic SCB rows (see
`_curation_partition_boundary_support`); `scb/anchor` is selected and `scb/sample`,
which carries the curation, is not. Each case is run as a full build too, so the
deferred classification is checked against what the full build actually mints.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _curation_partition_boundary_support import (
    REGISTER,
    build_referenced,
    partition,
    row,
    summary,
)
from reg_meta_build.catalog_dependencies import CatalogDependencyError

if TYPE_CHECKING:
    from pathlib import Path

_PARTITION = REGISTER + partition({"A": "1.5.a", "B": "1.5.b"})
_PARTITION_ROWS = [row("A", 21), row("B", 22)]


def _owner(owner: str, editions: str) -> str:
    return (
        '[[identity.column_owner]]\nvariable = "1.5"\nvariant = "1.2"\n'
        f'column = "ANSWER"\nowner = "{owner}"\nref = "fixture basis"\n'
        f"source_editions = {editions}\n"
    )


_EDITION_OWNERS = (
    REGISTER
    + '[[variable]]\nnative_id = "1.5.old"\nslug = "old"\n'
    + '[[variable]]\nnative_id = "1.5.new"\nslug = "new"\n'
    + _owner("1.5.old", '["2020"]')
    + _owner("1.5.new", '["2021"]')
)
_REVIEWED = (
    REGISTER
    + '[[variable]]\nnative_id = "1.5.reviewed"\nslug = "reviewed"\n'
    + _owner("1.5.reviewed", '["2020"]')
)
_GENERATED = {
    "registers/scb/sample.auto.toml": (
        '[[variable]]\nnative_id = "1.5.answer"\nslug = "answer"\n'
    )
}

_CASES = {
    "partition": (
        _PARTITION_ROWS,
        [summary("A"), summary("B")],
        {"registers/scb/sample.toml": _PARTITION},
    ),
    "edition-owners": (
        [row("ANSWER", 20, year="2020"), row("ANSWER", 21, year="2021")],
        [summary("ANSWER"), summary("ANSWER", year="2021")],
        {"registers/scb/sample.toml": _EDITION_OWNERS},
    ),
    "reviewed-owner": (
        [row("ANSWER", 20, year="2020")],
        [summary("ANSWER")],
        {"registers/scb/sample.toml": _REVIEWED, **_GENERATED},
    ),
}


def _build(tmp_path: Path, case: str, target: str, *, slice_anchor: bool):
    rows, summaries, curation = _CASES[case]
    return build_referenced(
        tmp_path, rows, summaries, curation, target, slice_anchor=slice_anchor
    )


@pytest.mark.parametrize(
    ("case", "target"),
    [
        ("partition", "a"),
        ("partition", "b"),
        ("edition-owners", "old"),
        ("edition-owners", "new"),
        ("reviewed-owner", "reviewed"),
    ],
)
def test_slice_defers_edge_to_a_name_the_full_build_mints(
    tmp_path: Path, case: str, target: str
) -> None:
    full = _build(tmp_path / "full", case, target, slice_anchor=False)
    assert full.codes() == []
    assert ("anchor/value", f"sample/{target}") in full.same_as()
    sliced = _build(tmp_path / "slice", case, target, slice_anchor=True)
    assert sliced.codes() == [("deferred_out_of_slice_reference", "variable_same_as:0")]
    assert sliced.same_as() == []


@pytest.mark.parametrize(
    ("case", "target"),
    [("partition", "unsplit"), ("reviewed-owner", "answer")],
)
def test_slice_refuses_edge_to_a_name_the_full_build_does_not_mint(
    tmp_path: Path, case: str, target: str
) -> None:
    """`answer` is the generated name a complete reviewed owner replaces."""
    with pytest.raises(CatalogDependencyError, match=f"scb/sample/{target}"):
        _build(tmp_path, case, target, slice_anchor=True)
