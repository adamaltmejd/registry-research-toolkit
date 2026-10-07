"""Query-layer coverage for the curated tag layer (#311):
`Catalog.list_tags` / `tags_for_variable` / `tags_for_register`.

Seeds literal tag rows over the slugged fixture DB so the read path exercises
the catalog contract independently of the retired build-time curation pass.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from _slugged_db import add_register, add_variable, build_slugged_db, seed_tags
from reg_meta.catalog import Catalog
from reg_meta.fqid import Fqid
from reg_meta_build.tags import CuratedTag, TagMember

if TYPE_CHECKING:
    import sqlite3


def _seeded_conn() -> sqlite3.Connection:
    """scb/lisa (fixture: variable `kon`) + scb/rams with `syss`. Two tags:
    `income` (a starred variable-grain member `kon` + a register-grain `lisa`
    member) and `employment` (a register-grain `rams` member) — exercises both
    grains and the cross-register global vocabulary."""
    conn = build_slugged_db(classification=None)
    add_register(conn, register_id=2, slug="rams", name="RAMS")
    add_variable(conn, register_id=2, var_id=50, name="Sysselsättning", slug="syss")
    seed_tags(
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


def test_concept_group_tags_aggregate_members_and_inherit_to_siblings() -> None:
    conn = _seeded_conn()
    add_variable(conn, register_id=1, var_id=51, name="Civilstånd", slug="civilstand")
    conn.execute(
        "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
        "label, source) VALUES (910, 'variable', 1, 'demog', 'Demographics', 'curated')"
    )
    conn.execute(
        "INSERT INTO concept_group_variable "
        "(group_id, variable_id, delivery_column_name) "
        "SELECT 910, variable_id, NULL FROM variable "
        "WHERE register_id = 1 AND slug IN ('kon', 'civilstand')"
    )

    cat = Catalog(conn)
    group = cat.concept_group("scb", "lisa", "demog")
    assert group is not None
    assert [tag.slug for tag in group.tags] == ["income"]
    assert group.tags[0].starred is True
    assert group.tags[0].note == "primary"

    inherited = cat.tags_for_variable(Fqid.binding_fqid("scb", "lisa", "civilstand"))
    assert [tag.slug for tag in inherited] == ["income"]
    assert inherited[0].rank == 0
    assert inherited[0].starred is False
    assert inherited[0].note is None

    direct = cat.tags_for_variable(Fqid.binding_fqid("scb", "lisa", "kon"))
    assert [tag.slug for tag in direct] == ["income"]
    assert direct[0].starred is True
    assert direct[0].note == "primary"


def test_concept_group_tag_note_prefers_noted_member_with_equal_rank() -> None:
    conn = build_slugged_db(classification=None)
    add_variable(conn, register_id=1, var_id=51, name="Civilstånd", slug="civilstand")
    seed_tags(
        conn,
        (
            CuratedTag(
                slug="income",
                label="Income",
                description=None,
                members=(
                    TagMember("scb", "lisa", "kon", rank=0, starred=False, note=None),
                    TagMember(
                        "scb",
                        "lisa",
                        "civilstand",
                        rank=0,
                        starred=False,
                        note="documented sibling",
                    ),
                ),
            ),
        ),
    )
    conn.execute(
        "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
        "label, source) VALUES (911, 'variable', 1, 'demog', 'Demographics', 'curated')"
    )
    conn.execute(
        "INSERT INTO concept_group_variable "
        "(group_id, variable_id, delivery_column_name) "
        "SELECT 911, variable_id, NULL FROM variable "
        "WHERE register_id = 1 AND slug IN ('kon', 'civilstand')"
    )

    group = Catalog(conn).concept_group("scb", "lisa", "demog")
    assert group is not None
    assert [tag.slug for tag in group.tags] == ["income"]
    assert group.tags[0].rank == 0
    assert group.tags[0].starred is False
    assert group.tags[0].note == "documented sibling"


def _multi_membership_tag(slug: str, label: str, member: TagMember) -> CuratedTag:
    return CuratedTag(slug=slug, label=label, description=None, members=(member,))


def test_tags_for_variable_orders_by_rank_then_slug() -> None:
    # `kon` belongs to TWO tags with different rank: higher-rank `aaa` (rank 5)
    # must come AFTER lower-rank `income` (rank 0).
    conn = build_slugged_db(classification=None)
    seed_tags(
        conn,
        (
            _multi_membership_tag(
                "aaa", "AAA", TagMember("scb", "lisa", "kon", 5, False, None)
            ),
            _multi_membership_tag(
                "income", "Income", TagMember("scb", "lisa", "kon", 0, True, None)
            ),
        ),
    )
    memberships = Catalog(conn).tags_for_variable(
        Fqid.binding_fqid("scb", "lisa", "kon")
    )
    # rank-then-slug: income (rank 0) before aaa (rank 5), despite slug order.
    assert [m.slug for m in memberships] == ["income", "aaa"]


def test_tags_for_register_orders_by_rank_then_slug() -> None:
    conn = build_slugged_db(classification=None)
    seed_tags(
        conn,
        (
            _multi_membership_tag(
                "aaa", "AAA", TagMember("scb", "lisa", None, 5, False, None)
            ),
            _multi_membership_tag(
                "income", "Income", TagMember("scb", "lisa", None, 0, False, None)
            ),
        ),
    )
    memberships = Catalog(conn).tags_for_register(Fqid.register_fqid("scb", "lisa"))
    assert [m.slug for m in memberships] == ["income", "aaa"]


def test_starred_count_counts_register_grain_starred_member() -> None:
    # A tag whose ONLY member is register-grain AND starred → starred_count == 1.
    # Locks the grain-agnostic `starred` design (starred isn't variable-only).
    conn = build_slugged_db(classification=None)
    seed_tags(
        conn,
        (
            _multi_membership_tag(
                "regstar", "RegStar", TagMember("scb", "lisa", None, 0, True, None)
            ),
        ),
    )
    (tag,) = Catalog(conn).list_tags()
    assert tag.member_count == 1
    assert tag.starred_count == 1
