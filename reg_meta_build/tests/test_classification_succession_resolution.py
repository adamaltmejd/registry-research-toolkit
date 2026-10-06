"""Classification edition chains resolve into checked succession edges before writing."""

from contextlib import closing

import pytest
from _catalog_dependency_support import variable as _variable
from catalog_manifest import synthetic_manifest
from reg_meta_build.catalog_dependencies import (
    resolve_classification_successions,
)
from reg_meta_build.db import open_built_db
from reg_meta_build.resolved_catalog import (
    ResolvedClassification,
    ResolvedClassificationCode,
    ResolvedClassificationSuccession,
    ResolvedVariant,
    write_resolved_catalog,
)


def _edition(slug, name):
    return ResolvedClassification(
        slug=slug,
        short_name=slug.upper(),
        name=name,
        codes=(ResolvedClassificationCode(code="01", label="One"),),
    )


def test_classification_chains_resolve_before_writing_with_explicit_split(tmp_path):
    books = (
        _edition("sun1996", "SUN 1996"),
        _edition("sun2000-niva", "SUN 2000 nivå"),
        _edition("sun2020-niva", "SUN 2020 nivå"),
        _edition("sun2025-niva", "SUN 2025 nivå"),
    )
    declared = (
        ResolvedClassificationSuccession(
            predecessor="sun1996", successor="sun2000-niva", effective_year=2000
        ),
    )
    edges = resolve_classification_successions(books, declared)
    assert edges == resolve_classification_successions(tuple(reversed(books)), declared)
    assert [(e.predecessor, e.successor, e.effective_year) for e in edges] == [
        ("sun1996", "sun2000-niva", 2000),
        ("sun2000-niva", "sun2020-niva", 2020),
        ("sun2020-niva", "sun2025-niva", 2025),
    ]
    assert all(e.note == "derived:vintage_chain" for e in edges[1:])
    path = write_resolved_catalog(
        (_variable(ResolvedVariant(slug="people", name="People")),),
        tmp_path / "diagnostic.db",
        manifest=synthetic_manifest(),
        diagnostic=True,
        classifications=books,
        classification_successions=edges,
    )
    with closing(open_built_db(path)) as conn:
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT predecessor_slug, successor_slug, effective_year "
                "FROM classification_replaced_by ORDER BY predecessor_slug"
            )
        ] == [(e.predecessor, e.successor, e.effective_year) for e in edges]


@pytest.mark.parametrize(
    ("predecessor", "successor", "error"),
    [
        ("sni2020", "sni2000", "cyclic"),
        ("sni2000", "sni2020", "duplicate classification succession"),
        ("sni1990", "sni2000", "unknown classification succession endpoint"),
    ],
)
def test_classification_succession_checks_combined_graph(predecessor, successor, error):
    books = (_edition("sni2000", "SNI 2000"), _edition("sni2020", "SNI 2020"))
    with pytest.raises(ValueError, match=error):
        resolve_classification_successions(
            books,
            (
                ResolvedClassificationSuccession(
                    predecessor=predecessor, successor=successor
                ),
            ),
        )


def test_classification_edition_rules_require_matching_names_and_unique_identity():
    books = (
        _edition("sni2000", "SNI 2000"),
        _edition("sni2020", "Another classification 2020"),
    )
    assert resolve_classification_successions(books) == ()
    with pytest.raises(ValueError, match="duplicate classification identity"):
        resolve_classification_successions((books[0], books[0]))
