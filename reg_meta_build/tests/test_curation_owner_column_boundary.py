"""`[[identity.column_owner]]` assigns one variant's column, optionally per edition.

Fixtures are synthetic SCB rows (see `_curation_partition_boundary_support`). Expected
values follow the column-owner contract: the owner takes exactly the rows of its
variant and named source editions; every other row of the native family keeps its
original identity, which the build then withholds as unresolved rather than folding
into the owner. A reviewed owner replaces a generated name only when it covers every
row. The issue codes asserted here (for example `unresolved_catalog_identity` for an
uncovered original row) were recorded from a build run, not authored from
documentation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _curation_partition_boundary_support import (
    NATIVE,
    REGISTER,
    SECOND_VARIANT,
    build,
    build_sample,
    row,
    summary,
)

if TYPE_CHECKING:
    from pathlib import Path


def _owner(owner: str, ref: str, editions: str | None = None) -> str:
    return (
        '[[identity.column_owner]]\nvariable = "1.5"\nvariant = "1.2"\n'
        f'column = "ANSWER"\nowner = "{owner}"\nref = "{ref}"\n'
        + (f"source_editions = {editions}\n" if editions else "")
    )


@pytest.mark.parametrize(
    ("years", "extra"),
    [
        (("2020",), []),
        (
            ("2020", "2021"),
            [
                ("ambiguous_named_identity", "scb/sample/answer"),
                ("unresolved_catalog_identity", NATIVE),
            ],
        ),
    ],
)
def test_reviewed_owner_replaces_generated_name_only_with_complete_coverage(
    tmp_path: Path, years: tuple[str, ...], extra: list[tuple[str, str]]
) -> None:
    """`sample.auto.toml` generates `1.5.answer`; the reviewed owner takes 2020.

    Covering every row retires the generated name. Leaving the 2021 row uncovered
    keeps it, and the build reports it as an ambiguous name beside the withheld row.
    """
    built = build(
        tmp_path,
        [row("ANSWER", 20 + index, year=year) for index, year in enumerate(years)],
        [summary("ANSWER", year=year) for year in years],
        {
            "registers/scb/sample.toml": REGISTER
            + '[[variable]]\nnative_id = "1.5.reviewed"\nslug = "reviewed"\n'
            + _owner("1.5.reviewed", "reviewed basis", '["2020"]'),
            "registers/scb/sample.auto.toml": (
                '[[variable]]\nnative_id = "1.5.answer"\nslug = "answer"\n'
            ),
        },
    )
    assert built.codes() == extra
    assert built.variables() == [("reviewed", "5.reviewed", None)]
    assert built.states() == [("reviewed", "people", "2020-01-01", "ANSWER")]


def test_variant_scoped_owner_leaves_other_variants_original(tmp_path: Path) -> None:
    built = build_sample(
        tmp_path,
        [row("ANSWER", 21), row("ANSWER", 22, variant=3)],
        REGISTER
        + SECOND_VARIANT
        + '[[variable]]\nnative_id = "1.5.answer"\nslug = "answer"\n'
        + _owner("1.5.answer", "fixture variant"),
        summaries=[summary("ANSWER")],
    )
    assert built.codes() == [("unresolved_catalog_identity", NATIVE)]
    assert built.states() == [("answer", "people", "2020-01-01", "ANSWER")]


@pytest.mark.parametrize("reverse", [False, True])
def test_edition_scoped_owners_take_only_their_editions(
    tmp_path: Path, reverse: bool
) -> None:
    """2020 goes to `old`, 2021 to `new`; 2022 keeps the original identity."""
    years = ("2020", "2021", "2022")
    rows = [row("ANSWER", 20 + index, year=year) for index, year in enumerate(years)]
    built = build_sample(
        tmp_path,
        rows[::-1] if reverse else rows,
        REGISTER
        + '[[variable]]\nnative_id = "1.5.old"\nslug = "old"\n'
        + '[[variable]]\nnative_id = "1.5.new"\nslug = "new"\n'
        + _owner("1.5.old", "old basis", '["2020"]')
        + _owner("1.5.new", "new basis", '["2021"]'),
        summaries=[summary("ANSWER", year=year) for year in years],
    )
    assert built.codes() == [("unresolved_catalog_identity", NATIVE)]
    assert built.states() == [
        ("new", "people", "2021-01-01", "ANSWER"),
        ("old", "people", "2020-01-01", "ANSWER"),
    ]
