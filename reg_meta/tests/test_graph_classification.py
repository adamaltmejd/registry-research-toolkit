"""Classification graphs (#761, #792): edition chains, SUN-style umbrella
groups and the classification leaf graph, built over the synthetic slugged DB
(the shared ``_slugged_db`` factory).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _slugged_db import build_slugged_db
from reg_meta.catalog import Catalog
from reg_meta.errors import RegMetaError
from reg_meta.graph import ClassificationGraphNode

if TYPE_CHECKING:
    import sqlite3


def _add_classification(
    conn: sqlite3.Connection,
    *,
    cid: int,
    slug: str,
    name: str = "C",
    valid_from: int | None = None,
) -> None:
    conn.execute(
        "INSERT INTO classification (id, short_name, name, slug, valid_from) "
        "VALUES (?, ?, ?, ?, ?)",
        (cid, slug.upper(), name, slug, valid_from),
    )
    conn.commit()


def _add_class_succession(
    conn: sqlite3.Connection,
    *,
    predecessor: str,
    successor: str,
    effective_year: int | None = None,
) -> None:
    conn.execute(
        "INSERT INTO classification_replaced_by "
        "(predecessor_slug, successor_slug, effective_year, note) "
        "VALUES (?, ?, ?, 'derived:test')",
        (predecessor, successor, effective_year),
    )
    conn.commit()


# ── Classification chains + SUN-style groups ─────────────────────────────────


class TestClassificationChains:
    def test_chain_nodes_are_point_year(self) -> None:
        conn = build_slugged_db(classification=None)
        # version_year = each edition's OWN vintage (classification.valid_from), NOT
        # the year it was superseded. Seed valid_from per edition distinct from the
        # succession effective_year so a regression that reuses effective_year fails.
        _add_classification(conn, cid=1, slug="sun1996", valid_from=1996)
        _add_classification(conn, cid=2, slug="sun2000", valid_from=2000)
        _add_classification(conn, cid=3, slug="sun2020", valid_from=2020)
        _add_class_succession(
            conn, predecessor="sun1996", successor="sun2000", effective_year=2000
        )
        _add_class_succession(
            conn, predecessor="sun2000", successor="sun2020", effective_year=2020
        )
        # Reach the chain via a 1-member umbrella group on the head edition.
        conn.execute(
            "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
            "label, source) VALUES (12, 'classification', NULL, 'sun', 'SUN', 'curated')"
        )
        conn.execute(
            "INSERT INTO concept_group_classification (classification_id, group_id, "
            "facet_value, facet_label) VALUES (3, 12, '2020', '2020')"
        )
        conn.commit()
        g = Catalog(conn).graph_for_classification_group("sun")
        assert g is not None
        nodes = {n.id: n for n in g.nodes}
        assert set(nodes) == {"class/sun1996", "class/sun2000", "class/sun2020"}
        assert all(isinstance(n, ClassificationGraphNode) for n in g.nodes)
        # No `group:sun` node — the umbrella is metadata, not a node.
        assert "class/group:sun" not in nodes
        assert "group:sun" not in nodes
        # version_year is each edition's OWN point-in-time vintage (valid_from), NOT
        # the supersession year. Crucially the TERMINAL current edition keeps its
        # own vintage (2020), not None (which `effective_year` would yield there).
        assert nodes["class/sun1996"].version_year == 1996
        assert nodes["class/sun2000"].version_year == 2000
        assert nodes["class/sun2020"].version_year == 2020
        assert nodes["class/sun2020"].short_name == "SUN2020"
        assert nodes["class/sun2020"].is_current is True
        # Two directed succession edges, deduped (no duplicate from co-membership).
        succ = [e for e in g.edges if e.kind == "succession"]
        assert len(succ) == 2
        assert {(e.source, e.target) for e in succ} == {
            ("class/sun1996", "class/sun2000"),
            ("class/sun2000", "class/sun2020"),
        }
        # #794 P2: each succession edge carries its `classification_replaced_by`
        # effective_year (the supersession year), so the #678 timeline can annotate
        # the transition even though the edition succession reason is suppressed (the
        # internal `note` tag is never shown). The year rides on the edge, NOT on the
        # node's `version_year` (the edition's own vintage).
        by_pair = {(e.source, e.target): e for e in succ}
        assert by_pair[("class/sun1996", "class/sun2000")].effective_year == 2000
        assert by_pair[("class/sun2000", "class/sun2020")].effective_year == 2020

    def test_umbrella_members_carry_group_label_heading(self) -> None:
        # #794 P3: a curated umbrella member carries the group's display `label` as
        # `group_label` so the renderer can title the classification cluster (its
        # `group_key` is the bare `class/sun` slug, no display string). A non-member
        # spine edition pulled in by the chain walk stays headless.
        conn = build_slugged_db(classification=None)
        _add_classification(conn, cid=1, slug="sun1996", valid_from=1996)
        _add_classification(conn, cid=2, slug="sun2000-niva", valid_from=2000)
        _add_class_succession(
            conn, predecessor="sun1996", successor="sun2000-niva", effective_year=2000
        )
        # Only the 2000-niva edition is a curated member; sun1996 is its (non-member)
        # spine predecessor.
        _add_class_umbrella_group(conn, members=[(2, "niva")])
        g = Catalog(conn).graph_for_classification_group("sun")
        assert g is not None
        nodes = {n.id: n for n in g.nodes}
        member = nodes["class/sun2000-niva"]
        assert isinstance(member, ClassificationGraphNode)
        assert member.group_key == "class/sun"
        assert member.group_label == "SUN"  # the curated group's display label
        # The non-member spine edition is headless (and ungrouped).
        spine = nodes["class/sun1996"]
        assert isinstance(spine, ClassificationGraphNode)
        assert spine.group_key is None
        assert spine.group_label is None

    def test_split_predecessor_branches_walked_when_descendant_first(self) -> None:
        # P2-3 regression: a #579 SPLIT root P (sun1996) fans out into 3 branches
        # (niva/inriktning/grupp); a descendant D (sun2020-niva) sits on ONE branch.
        # A group contains BOTH P and D, with D ordered FIRST (lower facet_value). When
        # D is processed first, `classification_chain(D)` returns only D's linear path
        # (sun1996 → sun2000-niva → sun2020-niva), so P's node exists but P's OTHER
        # branches were never walked. The early-out must gate on whether P ANCHORED a
        # walk (not on P's node-presence), so P's own walk still surfaces its sibling
        # branches' editions + edges.
        conn = build_slugged_db(classification=None)
        _add_classification(conn, cid=1, slug="sun1996", valid_from=1996)
        _add_classification(conn, cid=2, slug="sun2000-niva", valid_from=2000)
        _add_classification(conn, cid=3, slug="sun2000-inriktning", valid_from=2000)
        _add_classification(conn, cid=4, slug="sun2000-grupp", valid_from=2000)
        _add_classification(conn, cid=5, slug="sun2020-niva", valid_from=2020)
        # P splits into 3 branches.
        for succ in ("sun2000-niva", "sun2000-inriktning", "sun2000-grupp"):
            _add_class_succession(
                conn, predecessor="sun1996", successor=succ, effective_year=2000
            )
        # D extends the niva branch.
        _add_class_succession(
            conn,
            predecessor="sun2000-niva",
            successor="sun2020-niva",
            effective_year=2020,
        )
        conn.execute(
            "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
            "label, source) VALUES (12, 'classification', NULL, 'sun', 'SUN', 'curated')"
        )
        # facet_value orders members: D (1) precedes P (2), so D is processed first.
        conn.executemany(
            "INSERT INTO concept_group_classification (classification_id, group_id, "
            "facet_value, facet_label) VALUES (?, 12, ?, ?)",
            [(5, "1", "niva-2020"), (1, "2", "root-1996")],
        )
        conn.commit()
        g = Catalog(conn).graph_for_classification_group("sun")
        assert g is not None
        ids = {n.id for n in g.nodes}
        # P's OTHER branches (reached only via P's own walk) are present.
        assert {"class/sun2000-inriktning", "class/sun2000-grupp"} <= ids
        edges = {(e.source, e.target) for e in g.edges if e.kind == "succession"}
        assert ("class/sun1996", "class/sun2000-inriktning") in edges
        assert ("class/sun1996", "class/sun2000-grupp") in edges

    def test_shared_spine_split_preserves_all_edges_deduped(self) -> None:
        # An umbrella whose members share an ancestor spine (the SUN
        # niva/inriktning/grupp case) re-walks the spine once per member. Every split
        # + extension edge must be present and deduped: a spine slug's edges, added
        # under the first member walk, stay in `_edges` (dedup by id) for the others.
        # Two members (the two leaf branches) share the root P (sun1996) and the mid
        # spine. (This is a SPLIT shape, not a merge — see
        # `test_classification_merge_preserves_both_predecessor_edges` for the
        # convergent case the per-successor re-read protects.)
        conn = build_slugged_db(classification=None)
        _add_classification(conn, cid=1, slug="sun1996", valid_from=1996)
        _add_classification(conn, cid=2, slug="sun2000-niva", valid_from=2000)
        _add_classification(conn, cid=3, slug="sun2000-inriktning", valid_from=2000)
        _add_classification(conn, cid=4, slug="sun2020-niva", valid_from=2020)
        _add_classification(conn, cid=5, slug="sun2020-inriktning", valid_from=2020)
        # P splits into two branches; each branch extends to a 2020 leaf.
        for succ in ("sun2000-niva", "sun2000-inriktning"):
            _add_class_succession(
                conn, predecessor="sun1996", successor=succ, effective_year=2000
            )
        _add_class_succession(
            conn,
            predecessor="sun2000-niva",
            successor="sun2020-niva",
            effective_year=2020,
        )
        _add_class_succession(
            conn,
            predecessor="sun2000-inriktning",
            successor="sun2020-inriktning",
            effective_year=2020,
        )
        # Curated members = both 2020 leaves; both walks traverse the shared root P.
        _add_class_umbrella_group(conn, members=[(4, "niva"), (5, "inriktning")])
        g = Catalog(conn).graph_for_classification_group("sun")
        assert g is not None
        edges = {(e.source, e.target) for e in g.edges if e.kind == "succession"}
        # Every split + extension edge is present despite the shared-spine memo.
        assert edges == {
            ("class/sun1996", "class/sun2000-niva"),
            ("class/sun1996", "class/sun2000-inriktning"),
            ("class/sun2000-niva", "class/sun2020-niva"),
            ("class/sun2000-inriktning", "class/sun2020-inriktning"),
        }
        # No duplicate edges (dedup by id holds across the per-walk re-reads).
        succ_ids = [e.id for e in g.edges if e.kind == "succession"]
        assert len(succ_ids) == len(set(succ_ids))

    def test_classification_merge_preserves_both_predecessor_edges(self) -> None:
        # A classification MERGE: successor C (sun-cc) has TWO predecessors A (sun-aa)
        # and B (sun-bb) on different branches — edges A→C and B→C. C and B are both
        # curated umbrella members; C is anchored FIRST (lower facet_value). The
        # C-anchored walk's `classification_chain(C)` walks backward via the
        # deterministic-first predecessor only (`pred[0]` = sun-aa, alphabetically
        # first), so its `slug_to_id` holds A and C but NOT B — that walk can add A→C
        # but not B→C (B absent). B→C must come from B's OWN later walk, whose
        # `slug_to_id` holds B and C.
        #
        # This is the regression for a successor-keyed predecessor memo: memoizing on
        # the successor slug marks C "read" after the A-branch walk, so B's later walk
        # skips reading C's predecessors and B→C is dropped FOREVER. The per-walk
        # re-read (each walk re-attempts edges with its own `slug_to_id`; `_edges`
        # dedups) is what keeps both edges. FAILS against a `_pred_walked` memo; PASSES
        # after the revert.
        conn = build_slugged_db(classification=None)
        _add_classification(conn, cid=1, slug="sun-aa", valid_from=1996)  # A
        _add_classification(conn, cid=2, slug="sun-bb", valid_from=1996)  # B
        _add_classification(conn, cid=3, slug="sun-cc", valid_from=2000)  # C (merge)
        _add_class_succession(
            conn, predecessor="sun-aa", successor="sun-cc", effective_year=2000
        )
        _add_class_succession(
            conn, predecessor="sun-bb", successor="sun-cc", effective_year=2000
        )
        # C (facet 1) is anchored before B (facet 2); A is pulled in only as C's
        # deterministic-first ancestor, so B is absent from C's walk.
        _add_class_umbrella_group(conn, members=[(3, "1"), (2, "2")])
        g = Catalog(conn).graph_for_classification_group("sun")
        assert g is not None
        edges = {(e.source, e.target) for e in g.edges if e.kind == "succession"}
        # BOTH inbound merge edges must be present.
        assert ("class/sun-aa", "class/sun-cc") in edges
        assert ("class/sun-bb", "class/sun-cc") in edges
        # No duplicate succession edges (dedup by id holds across re-reads).
        succ_ids = [e.id for e in g.edges if e.kind == "succession"]
        assert len(succ_ids) == len(set(succ_ids))

    def test_lone_classification_group_of_one_is_empty(self) -> None:
        # A classification umbrella whose single member has NO succession chain →
        # the solo edition is empty (the `_is_empty_solo` ClassificationGraphNode
        # path via the group accessor, not just via graph_for_fqid).
        conn = build_slugged_db(classification=None)
        _add_classification(conn, cid=1, slug="sun2020")
        conn.execute(
            "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
            "label, source) VALUES (12, 'classification', NULL, 'sun', 'SUN', 'curated')"
        )
        conn.execute(
            "INSERT INTO concept_group_classification (classification_id, group_id, "
            "facet_value, facet_label) VALUES (1, 12, '2020', '2020')"
        )
        conn.commit()
        g = Catalog(conn).graph_for_classification_group("sun")
        assert g is not None
        assert g.nodes == []
        assert g.edges == []


def _add_class_umbrella_group(
    conn: sqlite3.Connection,
    *,
    group_id: int = 12,
    members: list[tuple[int, str]],
) -> None:
    """A curated classification umbrella group (`group:sun`) with the given
    `(classification_id, facet_value)` members — the fixture shape the umbrella
    tests share (mirrors the inline INSERTs in `TestClassificationChains`)."""
    conn.execute(
        "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
        "label, source) VALUES (?, 'classification', NULL, 'sun', 'SUN', 'curated')",
        (group_id,),
    )
    conn.executemany(
        "INSERT INTO concept_group_classification (classification_id, group_id, "
        "facet_value, facet_label) VALUES (?, ?, ?, ?)",
        [(cid, group_id, fv, fv) for cid, fv in members],
    )
    conn.commit()


class TestClassificationLeafGraph:
    """`graph_for_classification_fqid` (#792) — the classification analog of
    `graph_for_fqid`: a leaf edition's own chain unioned with its umbrella group(s),
    `focus_id` on the canonical edition."""

    def test_standalone_classification_is_empty(self) -> None:
        # A classification with no chain and no umbrella → the empty (don't-render)
        # graph (`_is_empty_solo` for a solo classification), parity with today's
        # panels showing nothing for a 1-element chain / no dimensions.
        conn = build_slugged_db(classification=None)
        _add_classification(conn, cid=1, slug="sun2020", valid_from=2020)
        g = Catalog(conn).graph_for_classification_fqid("class/sun2020")
        assert g.nodes == []
        assert g.edges == []
        assert g.focus_id is None

    def test_non_classification_fqid_raises(self) -> None:
        # A binding FQID handed to the classification accessor raises the standard
        # usage error (the route's 4xx path) — parity with the sibling accessors.
        conn = build_slugged_db(classification=None)
        with pytest.raises(RegMetaError) as exc:
            Catalog(conn).graph_for_classification_fqid("p/r/v")
        assert exc.value.code == "not_a_classification_fqid"
