"""Query-layer coverage for the curated tag layer (#311):
`Catalog.list_tags` / `tags_for_variable` / `tags_for_register`.

Seeds tags via `materialize_tags` (the build-side writer) over the slugged
fixture DB so the read path is exercised against real materialized rows.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from _slugged_db import add_register, add_variable, build_slugged_db
from reg_meta.catalog import Catalog, ResolvedRegister, ResolvedVariable
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


def test_resolve_register_embeds_tags() -> None:
    resolved = Catalog(_seeded_conn()).resolve(Fqid.register_fqid("scb", "lisa"))
    assert isinstance(resolved, ResolvedRegister)
    assert [m.slug for m in resolved.tags] == ["income"]
    assert resolved.tags[0].label == "Income & earnings"


def test_resolve_variable_embeds_tags() -> None:
    resolved = Catalog(_seeded_conn()).resolve(Fqid.binding_fqid("scb", "lisa", "kon"))
    assert isinstance(resolved, ResolvedVariable)
    assert [m.slug for m in resolved.tags] == ["income"]
    assert resolved.tags[0].starred is True
    assert resolved.tags[0].note == "primary"


def _multi_membership_tag(slug: str, label: str, member: TagMember) -> CuratedTag:
    return CuratedTag(slug=slug, label=label, description=None, members=(member,))


def test_tags_for_variable_orders_by_rank_then_slug() -> None:
    # `kon` belongs to TWO tags with different rank: higher-rank `aaa` (rank 5)
    # must come AFTER lower-rank `income` (rank 0).
    conn = build_slugged_db(classification=None)
    materialize_tags(
        conn,
        (
            _multi_membership_tag(
                "aaa", "AAA", TagMember("scb", "lisa", "kon", 5, False, None)
            ),
            _multi_membership_tag(
                "income", "Income", TagMember("scb", "lisa", "kon", 0, True, None)
            ),
        ),
        providers=_SCB,
    )
    memberships = Catalog(conn).tags_for_variable(
        Fqid.binding_fqid("scb", "lisa", "kon")
    )
    # rank-then-slug: income (rank 0) before aaa (rank 5), despite slug order.
    assert [m.slug for m in memberships] == ["income", "aaa"]


def test_tags_for_register_orders_by_rank_then_slug() -> None:
    conn = build_slugged_db(classification=None)
    materialize_tags(
        conn,
        (
            _multi_membership_tag(
                "aaa", "AAA", TagMember("scb", "lisa", None, 5, False, None)
            ),
            _multi_membership_tag(
                "income", "Income", TagMember("scb", "lisa", None, 0, False, None)
            ),
        ),
        providers=_SCB,
    )
    memberships = Catalog(conn).tags_for_register(Fqid.register_fqid("scb", "lisa"))
    assert [m.slug for m in memberships] == ["income", "aaa"]


def test_starred_count_counts_register_grain_starred_member() -> None:
    # A tag whose ONLY member is register-grain AND starred → starred_count == 1.
    # Locks the grain-agnostic `starred` design (starred isn't variable-only).
    conn = build_slugged_db(classification=None)
    materialize_tags(
        conn,
        (
            _multi_membership_tag(
                "regstar", "RegStar", TagMember("scb", "lisa", None, 0, True, None)
            ),
        ),
        providers=_SCB,
    )
    (tag,) = Catalog(conn).list_tags()
    assert tag.member_count == 1
    assert tag.starred_count == 1
