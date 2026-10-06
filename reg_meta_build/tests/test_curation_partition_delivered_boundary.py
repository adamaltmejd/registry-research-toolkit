"""`[[errata.delivered]]` additions on a variable split by `[[identity.partition]]`.

Fixtures are synthetic SCB rows (see `_curation_partition_boundary_support`); a quoted
empty Kolumnnamn cell (`'""'`) is SCB's literal blank, the explicit "no physical
column" claim. Expected values follow `reg_meta_build/DESIGN.md` (errata.delivered
adds the omitted edition of a column documented elsewhere on the variant) and the
partition contract (a literal column belongs to its one split owner). Refusal details
are the converter's own located blocker strings.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _curation_partition_boundary_support import (
    DELIVERED,
    NATIVE,
    REGISTER,
    SECOND_VARIANT,
    build_sample,
    partition,
    row,
    summary,
)

if TYPE_CHECKING:
    from pathlib import Path

_AB = {"A": "1.5.a", "B": "1.5.b"}
_ABC = {"A": "1.5.a", "B": "1.5.b", "C": "1.5.c"}
_ERRATA = "errata:omitted-column-in-version\naccepted delivery\n"


def test_delivered_addition_lands_on_the_literal_columns_split_owner(
    tmp_path: Path,
) -> None:
    """Column A is undocumented in 2021; the added 2021 occurrence belongs to `a`."""
    built = build_sample(
        tmp_path,
        [row("A", 20), row("B", 21), row("B", 22, year="2021", variable=6)],
        REGISTER
        + partition(_AB)
        + '[[variable]]\nnative_id = "1.6"\nslug = "b6"\n'
        + DELIVERED,
        summaries=[summary("A"), summary("B"), summary("B", year="2021")],
    )
    assert built.codes() == []
    assert built.states() == [
        ("a", "people", "2020-01-01", "A"),
        ("a", "people", "2021-01-01", "A"),
        ("b", "people", "2020-01-01", "B"),
        ("b6", "people", "2021-01-01", "B"),
    ]
    assert built.provenance("a", "2021-01-01").startswith(_ERRATA)


def test_delivered_blank_row_is_rewritten_and_owned_by_the_split(
    tmp_path: Path,
) -> None:
    """The 2021 member with a literal blank column becomes column A under `a`."""
    built = build_sample(
        tmp_path,
        [row("A", 20), row("B", 23, variant=3), row('""', 22, year="2021")],
        REGISTER + SECOND_VARIANT + partition(_AB) + DELIVERED,
        summaries=[summary("A"), summary("B")],
    )
    assert built.codes() == []
    assert built.states() == [
        ("a", "people", "2020-01-01", "A"),
        ("a", "people", "2021-01-01", "A"),
        ("b", "others", "2020-01-01", "B"),
    ]
    assert built.provenance("a", "2021-01-01").startswith(_ERRATA)


def _components(tmp_path: Path, blanks: list[str]):
    """Columns A, B, C of 2020 are reviewed components of one 2021 blank member."""
    entries = "".join(
        DELIVERED.replace('column = "A"', f'column = "{column}"') for column in "ABC"
    )
    return build_sample(
        tmp_path,
        [row(column, 20 + index) for index, column in enumerate("ABC")] + blanks,
        REGISTER + partition(_ABC, ref="exact simultaneous components") + entries,
        summaries=[summary(column) for column in "ABC"],
    )


def test_simultaneous_components_are_added_beside_the_blank_native_base(
    tmp_path: Path,
) -> None:
    """Each component gets its own 2021 addition; the blank row stays as evidence."""
    built = _components(tmp_path, [row('""', 30, year="2021")])
    assert built.codes() == [("omitted_columnless_occurrence", NATIVE)]
    assert built.states() == [
        (owner, "people", year, owner.upper())
        for owner in "abc"
        for year in ("2020-01-01", "2021-01-01")
    ]
    for owner in "abc":
        assert built.provenance(owner, "2021-01-01").startswith(_ERRATA)


@pytest.mark.parametrize(
    ("blanks", "code", "blocker"),
    [
        # A second blank member in 2021: the target is no longer unique.
        (
            [row('""', 30, year="2021"), row('""', 31, year="2021")],
            "overbroad_curation_entry",
            "ambiguous_target",
        ),
        # An undelivered (unknown) cell does not prove the negative base.
        (
            [row("", 30, year="2021")],
            "stale_curation_entry",
            "unproved_negative_native_base",
        ),
        # No 2021 member at all: the named version is missing.
        ([], "stale_curation_entry", None),
    ],
)
def test_components_refuse_an_unproved_blank_base(
    tmp_path: Path, blanks: list[str], code: str, blocker: str | None
) -> None:
    built = _components(tmp_path, blanks)
    expected = (
        f"{blocker}:2021:variable:5" if blocker else "missing named version ['2021']"
    )
    assert built.details(code) == [
        f"curation/registers/scb/sample.toml#/errata.delivered/{index}: {expected}"
        for index in (1, 2, 3)
    ]
    assert [state for state in built.states() if state[2] == "2021-01-01"] == []


def test_components_refuse_a_blank_base_under_another_column(tmp_path: Path) -> None:
    """A 2021 member that names a column is not the blank base the entries name."""
    built = _components(tmp_path, [row("OTHER", 30, year="2021")])
    assert [
        detail
        for detail in built.details("stale_curation_entry")
        if "#/errata.delivered/" in detail
    ] == [
        f"curation/registers/scb/sample.toml#/errata.delivered/{index}: "
        "target_under_other_column:2021:variable:5"
        for index in (1, 2, 3)
    ]
    assert built.states() == []
