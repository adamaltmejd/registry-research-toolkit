"""Query-layer coverage for the curated tag layer (#311):
`Catalog.list_tags` / `tags_for_variable` / `tags_for_register`.

Seeds tags via `materialize_tags` (the build-side writer) over the slugged
fixture DB so the read path is exercised against real materialized rows.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from _slugged_db import add_register, add_variable, build_slugged_db
from reg_meta.catalog import Catalog
from reg_meta.fqid import Fqid
from reg_meta_build.tags import CuratedTag, TagMember, materialize_tags

if TYPE_CHECKING:
    import sqlite3

_SCB = frozenset({"scb"})


def _seeded_conn() -> sqlite3.Connection:
    """scb/lisa (fixture: variable `kon`) + scb/rams with `syss`. Two tags:
    `income` (a starred variable-grain member `kon` + a register-grain `lisa`
    member) and `employment` (a register-grain `rams` member) — exercises both
    grains and the cross-register global vocabulary."""
    conn = build_slugged_db(classification=None)
    add_register(conn, register_id=2, slug="rams", name="RAMS")
    add_variable(conn, register_id=2, var_id=50, name="Sysselsättning", slug="syss")
    materialize_tags(
        conn,
        (
            CuratedTag(
                slug="income",
                label="Income & earnings",
                description="Income measures",
                members=(
                    TagMember(
                        "scb", "lisa", "kon", rank=0, starred=True, note="primary"
                    ),
                    TagMember("scb", "lisa", None, rank=1, starred=False, note=None),
                ),
            ),
            CuratedTag(
                slug="employment",
                label="Employment",
                description=None,
                members=(
                    TagMember("scb", "rams", None, rank=0, starred=False, note=None),
                ),
            ),
        ),
        providers=_SCB,
    )
    return conn


def test_list_tags_vocab_with_counts() -> None:
    cat = Catalog(_seeded_conn())
    tags = cat.list_tags()
    # Ordered by slug: employment, income.
    assert [t.slug for t in tags] == ["employment", "income"]
    income = next(t for t in tags if t.slug == "income")
    assert income.label == "Income & earnings"
    assert income.description == "Income measures"
    assert income.member_count == 2
    assert income.starred_count == 1
    employment = next(t for t in tags if t.slug == "employment")
    assert employment.member_count == 1
    assert employment.starred_count == 0


def test_list_tags_empty_when_none() -> None:
    cat = Catalog(build_slugged_db(classification=None))
    assert cat.list_tags() == []


def test_tags_for_variable() -> None:
    cat = Catalog(_seeded_conn())
    memberships = cat.tags_for_variable(Fqid.binding_fqid("scb", "lisa", "kon"))
    assert len(memberships) == 1
    m = memberships[0]
    assert m.slug == "income"
    assert m.label == "Income & earnings"
    assert m.starred is True
    assert m.note == "primary"
    assert m.rank == 0


def test_tags_for_variable_empty_for_untagged() -> None:
    cat = Catalog(_seeded_conn())
    # `syss` carries no variable-grain membership (only its register is tagged).
    assert cat.tags_for_variable(Fqid.binding_fqid("scb", "rams", "syss")) == []


def test_tags_for_register() -> None:
    cat = Catalog(_seeded_conn())
    lisa = cat.tags_for_register(Fqid.register_fqid("scb", "lisa"))
    assert [m.slug for m in lisa] == ["income"]
    assert lisa[0].starred is False  # the register-grain member is not starred
    rams = cat.tags_for_register(Fqid.register_fqid("scb", "rams"))
    assert [m.slug for m in rams] == ["employment"]


def test_tags_for_register_empty_for_untagged() -> None:
    cat = Catalog(_seeded_conn())
    # A register that exists but carries no register-grain tag membership.
    add_register(cat._conn, register_id=3, slug="bas", name="BAS")
    assert cat.tags_for_register(Fqid.register_fqid("scb", "bas")) == []
