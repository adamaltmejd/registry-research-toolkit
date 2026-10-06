"""`[[enrichment.description]]` and `[[enrichment.alias]]` reach the variable they name.

Fixtures are synthetic SCB rows (see `_curation_partition_boundary_support`). Expected
values follow the enrichment contract: a description fills an empty source description
and is stale when the source already carries one; a search alias attaches to every
variant the named variable occurs in, including a variable that exists only through a
partition split or an `[[errata.column]]` declaration.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from _curation_partition_boundary_support import (
    ALIAS,
    DESCRIPTION,
    REGISTER,
    SECOND_VARIANT,
    build_sample,
    partition,
    row,
    summary,
)

if TYPE_CHECKING:
    from pathlib import Path

_A = '[[variable]]\nnative_id = "1.5"\nslug = "a"\n'


def test_description_and_alias_apply_across_every_variant(tmp_path: Path) -> None:
    built = build_sample(
        tmp_path,
        [row("A", 20), row("A", 21, year="2021", variant=3)],
        REGISTER + SECOND_VARIANT + _A + DESCRIPTION + ALIAS,
        summaries=[summary("A"), summary("A", year="2021")],
    )
    assert built.codes() == []
    assert built.variables() == [("a", "5", "Accepted prose")]
    assert built.aliases() == [
        ("a", "others", "A"),
        ("a", "others", "FormerA"),
        ("a", "people", "A"),
        ("a", "people", "FormerA"),
    ]


def test_description_over_an_existing_source_description_is_stale(
    tmp_path: Path,
) -> None:
    built = build_sample(
        tmp_path,
        [row("A", 20, vardesc="Already")],
        REGISTER + _A + DESCRIPTION,
        summaries=[summary("A")],
    )
    assert built.details("stale_curation_entry") == [
        "curation/registers/scb/sample.toml#/enrichment.description/1: "
        "target has no original records or already has a description"
    ]
    assert built.variables() == [("a", "5", "Already")]


def test_enrichment_of_a_partition_owner_reaches_only_its_column(
    tmp_path: Path,
) -> None:
    built = build_sample(
        tmp_path,
        [row("A", 21), row("B", 22)],
        REGISTER
        + partition({"A": "1.5.a", "B": "1.5.b"})
        + DESCRIPTION.replace('"a"', '"b"')
        + ALIAS.replace('"a"', '"b"'),
        summaries=[summary("A"), summary("B")],
    )
    assert built.codes() == []
    assert built.variables() == [("a", "5.a", None), ("b", "5.b", "Accepted prose")]
    assert built.aliases() == [
        ("a", "people", "A"),
        ("b", "people", "B"),
        ("b", "people", "FormerA"),
    ]


def test_alias_of_a_declared_errata_column_attaches_to_its_variant(
    tmp_path: Path,
) -> None:
    """`new-col` exists only through `[[errata.column]]`; its alias still binds."""
    built = build_sample(
        tmp_path,
        [row("A", 20)],
        REGISTER
        + _A
        + '[[variable]]\nnative_id = "1.NewCol"\nslug = "new-col"\n'
        + '\n[[errata.column]]\nvariant = "people"\ncolumn = "NewCol"\n'
        'name = "New column"\ndefinition = "Documented"\ndata_type = "integer"\n'
        "is_identifier = false\nis_sensitive = false\n"
        'source = "scb-docs"\nevidence = "held"\nnoted = "2026-09-25"\n'
        'versions = ["2020"]\n' + ALIAS.replace('"a"', '"new-col"'),
        summaries=[summary("A")],
    )
    assert built.codes() == []
    assert built.aliases() == [
        ("a", "people", "A"),
        ("new-col", "people", "FormerA"),
        ("new-col", "people", "NewCol"),
    ]
