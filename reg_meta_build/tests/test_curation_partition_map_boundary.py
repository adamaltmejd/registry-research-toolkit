"""A curated `[[identity.partition]]` literal map in a full build.

Out-of-slice references to partition owners live in
`test_curation_partition_deferred_boundary.py`.

Fixtures are synthetic SCB rows (see `_curation_partition_boundary_support`). Expected
values follow the documented contract: an exact literal map moves each column to its
named owner; a map that leaves a delivered literal uncovered, or names a literal the
source no longer delivers, is stale and withholds the family (the expected detail text
is the compiler's own located message).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _curation_partition_boundary_support import (
    NATIVE,
    REGISTER,
    build_sample,
    partition,
    row,
    summary,
)

if TYPE_CHECKING:
    from pathlib import Path

_MAP = {"ANSWER": "1.5.answer", "OTHER": "1.5.other"}


def test_exact_literal_map_moves_each_column_to_its_owner(tmp_path: Path) -> None:
    built = build_sample(
        tmp_path,
        [row("ANSWER", 21), row("OTHER", 22)],
        REGISTER + partition(_MAP),
        summaries=[summary("ANSWER"), summary("OTHER")],
    )
    assert built.codes() == []
    assert built.variables() == [
        ("answer", "5.answer", None),
        ("other", "5.other", None),
    ]
    assert built.states() == [
        ("answer", "people", "2020-01-01", "ANSWER"),
        ("other", "people", "2020-01-01", "OTHER"),
    ]


@pytest.mark.parametrize(
    ("columns", "uncovered", "absent"),
    [(("ANSWER", "OTHER", "MISSING"), ["MISSING"], []), (("ANSWER",), [], ["OTHER"])],
)
def test_map_that_misses_a_delivered_or_names_an_absent_literal_is_stale(
    tmp_path: Path, columns: tuple[str, ...], uncovered: list, absent: list
) -> None:
    built = build_sample(
        tmp_path,
        [row(column, 21 + index) for index, column in enumerate(columns)],
        REGISTER + partition(_MAP),
        summaries=[summary(column) for column in columns],
    )
    assert built.details("stale_curation_entry") == [
        "curation/registers/scb/sample.toml#/identity.partition/1: explicit column "
        "ownership must cover the complete columns and split keys; "
        f"uncovered literals={uncovered}; absent literals={absent}; "
        "unnamed owners=[]; unowned splits=[]"
    ]
    assert ("unresolved_catalog_identity", NATIVE) in built.codes()
    assert built.states() == []
