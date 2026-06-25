"""Tests for the catalog relationship-graph contract (#761).

The graph model + builders live in ``reg_meta/graph.py``; ``Catalog`` exposes
``graph_for_fqid`` / ``graph_for_group`` / ``graph_for_classification_group``.
These build a synthetic slugged DB via the shared ``_slugged_db`` factory (the
same one ``test_catalog.py`` uses) and assert the topology + representation-run +
empty-graph + dedup semantics the issue pins.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from _slugged_db import (
    add_state,
    add_value_set,
    add_variable,
    add_variant,
    build_slugged_db,
)
from reg_meta.catalog import Catalog
from reg_meta.graph import ClassificationGraphNode, VariableGraphNode

if TYPE_CHECKING:
    import sqlite3

_KON = "scb/lisa/kon"


def _seed_replaced_by(
    conn: sqlite3.Connection,
    *,
    predecessor: tuple[str, str, str],
    successor: tuple[str, str, str],
    reason: str | None = None,
    effective_year: int | None = None,
) -> None:
    conn.execute(
        "INSERT INTO variable_replaced_by ("
        "predecessor_provider, predecessor_register, predecessor_variable, "
        "successor_provider, successor_register, successor_variable, "
        "effective_year, note, beskrivning) VALUES (?,?,?,?,?,?,?,?,?)",
        (*predecessor, *successor, effective_year, "auto:test", reason),
    )
    conn.commit()


def _seed_same_as(
    conn: sqlite3.Connection,
    alias: tuple[str, str, str],
    canonical: tuple[str, str, str],
) -> None:
    # variable_same_as: the ALIAS triple is an a-side source key with no live
    # `variable` row, so it resolves THROUGH to the live canonical b-side.
    conn.execute(
        "INSERT INTO variable_same_as (a_provider,a_register,a_variable,"
        "b_provider,b_register,b_variable) VALUES (?,?,?,?,?,?)",
        (*alias, *canonical),
    )
    conn.commit()


def _seed_related(
    conn: sqlite3.Connection,
    a: tuple[str, str, str],
    b: tuple[str, str, str],
    kind: str = "code_vs_label_pair",
) -> None:
    # variable_related_to stores BOTH directions (the build writes a↔b and b↔a).
    for src, tgt in ((a, b), (b, a)):
        conn.execute(
            "INSERT INTO variable_related_to (a_provider,a_register,a_variable,"
            "b_provider,b_register,b_variable,relation_kind,note) "
            "VALUES (?,?,?,?,?,?,?,'auto:test')",
            (*src, *tgt, kind),
        )
    conn.commit()


def _add_concept_group(
    conn: sqlite3.Connection,
    *,
    group_id: int,
    register_id: int,
    group_key: str,
    member_slugs: list[str],
) -> None:
    conn.execute(
        "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
        "label, source) VALUES (?, 'variable', ?, ?, ?, 'curated')",
        (group_id, register_id, group_key, f"Group {group_key}"),
    )
    for slug in member_slugs:
        vid = conn.execute(
            "SELECT variable_id FROM variable WHERE register_id = ? AND slug = ?",
            (register_id, slug),
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO concept_group_variable (variable_id, group_id) VALUES (?, ?)",
            (vid, group_id),
        )
    conn.commit()


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


# ── Empty vs non-empty ───────────────────────────────────────────────────────


class TestEmptyGraph:
    def test_lone_variable_type_only_split_is_empty(self) -> None:
        # The `akters` case: a lone variable, no succession / related / group, whose
        # two states differ ONLY by `data_type` `int` -> `bigint` (same column, no
        # value-set, no classification). `data_type` is NOT a boundary signal at all
        # (low-trust passthrough #526 blanks), so both states share one
        # representation run → empty (don't render).
        conn = build_slugged_db()  # seed state on kon: data_type `int`, column `Kon`
        add_state(
            conn,
            register_id=1,
            variable_slug="kon",
            register_variant_id=10,
            valid_from="2019-01-01",
            delivery_column_name="Kon",
            data_type="bigint",
        )
        conn.commit()
        g = Catalog(conn).graph_for_fqid(_KON)
        assert g.nodes == []
        assert g.edges == []
        assert g.focus_id is None

    def test_lone_variable_text_family_wobble_is_empty(self) -> None:
        # char↔varchar wobble is likewise not a boundary signal → one run → empty.
        conn = build_slugged_db()
        add_state(
            conn,
            register_id=1,
            variable_slug="kon",
            register_variant_id=10,
            valid_from="2019-01-01",
            delivery_column_name="Kon",
            data_type="varchar",
        )
        conn.execute(
            "UPDATE variable_state SET data_type = 'char' "
            "WHERE valid_from = '2018-01-01'"
        )
        conn.commit()
        g = Catalog(conn).graph_for_fqid(_KON)
        assert g.nodes == []
        assert g.edges == []

    def test_lone_classification_no_chain_is_empty(self) -> None:
        conn = build_slugged_db()  # seeds sun2020, no succession
        g = Catalog(conn).graph_for_classification_group("nope")
        assert g is None  # unknown group key
        # A lone classification reached via a group of one would be empty too, but a
        # standalone classification is only reachable via the group accessor here;
        # the variable-graph empty path is the akters case above.

    def test_lone_variable_value_set_change_renders(self) -> None:
        # A lone variable WITH a meaningful value-set change but no succession → one
        # node whose states span ≥2 representation runs → renders.
        conn = build_slugged_db()
        add_value_set(conn, value_set_id=1, codes=[("1", "Man"), ("2", "Kvinna")])
        add_value_set(conn, value_set_id=2, codes=[("1", "M"), ("2", "K"), ("3", "X")])
        # Replace the seed's single state with two value-set-distinct states.
        conn.execute("DELETE FROM variable_state")
        add_state(
            conn,
            register_id=1,
            variable_slug="kon",
            register_variant_id=10,
            valid_from="2018-01-01",
            valid_to="2018-12-31",
            delivery_column_name="Kon",
            value_set_id=1,
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="kon",
            register_variant_id=10,
            valid_from="2019-01-01",
            delivery_column_name="Kon",
            value_set_id=2,
        )
        conn.commit()
        g = Catalog(conn).graph_for_fqid(_KON)
        assert len(g.nodes) == 1
        (node,) = g.nodes
        assert isinstance(node, VariableGraphNode)
        runs = [s.representation_run_id for s in node.states]
        assert runs == [0, 1]  # two cells
        assert g.focus_id == _KON


# ── Representation runs ──────────────────────────────────────────────────────


class TestRepresentationRuns:
    def test_cross_era_column_rename_is_a_boundary(self) -> None:
        conn = build_slugged_db()
        conn.execute("DELETE FROM variable_state")
        for vf, vt, col in (
            ("2018-01-01", "2019-12-31", "Kon"),
            ("2020-01-01", "9999-12-31", "Konkod"),  # renamed column → new run
        ):
            add_state(
                conn,
                register_id=1,
                variable_slug="kon",
                register_variant_id=10,
                valid_from=vf,
                valid_to=vt,
                delivery_column_name=col,
            )
        conn.commit()
        node = Catalog(conn).graph_for_fqid(_KON).nodes[0]
        assert isinstance(node, VariableGraphNode)
        assert [s.representation_run_id for s in node.states] == [0, 1]

    def test_value_set_version_label_change_is_a_boundary(self) -> None:
        # Two valued states sharing a value_set_id but differing in
        # value_set_version_label are DISTINCT materialized states (the #526
        # state-identity gkey keys on id + label) → two representation runs. Label is
        # part of value-set identity, not a low-trust wobble.
        conn = build_slugged_db()
        add_value_set(conn, value_set_id=1, codes=[("1", "Man"), ("2", "Kvinna")])
        conn.execute("DELETE FROM variable_state")
        for vf, label in (("2018-01-01", "v1"), ("2019-01-01", "v2")):
            add_state(
                conn,
                register_id=1,
                variable_slug="kon",
                register_variant_id=10,
                valid_from=vf,
                delivery_column_name="Kon",
                value_set_id=1,
                value_set_version_label=label,
            )
        conn.commit()
        node = Catalog(conn).graph_for_fqid(_KON).nodes[0]
        assert isinstance(node, VariableGraphNode)
        assert [s.representation_run_id for s in node.states] == [0, 1]

    def test_run_never_spans_variants(self) -> None:
        # Two variants delivering identical-shaped states must still break the run at
        # the variant change (a run never spans variants), even with NO #526 boundary.
        conn = build_slugged_db()
        add_variant(
            conn,
            register_variant_id=11,
            register_id=1,
            slug="individer-all",
            name="All",
        )
        conn.execute("DELETE FROM variable_state")
        for variant_id in (10, 11):
            add_state(
                conn,
                register_id=1,
                variable_slug="kon",
                register_variant_id=variant_id,
                valid_from="2018-01-01",
                delivery_column_name="Kon",
            )
        conn.commit()
        node = Catalog(conn).graph_for_fqid(_KON).nodes[0]
        assert isinstance(node, VariableGraphNode)
        variants = [s.variant for s in node.states]
        runs = [s.representation_run_id for s in node.states]
        # Ordered by (variant, valid_from): the two variants → two distinct runs, and
        # no single run id spans both variants.
        assert len(set(variants)) == 2
        per_variant = {v: set() for v in variants}
        for s in node.states:
            per_variant[s.variant].add(s.representation_run_id)
        assert per_variant["individer-15plus"].isdisjoint(per_variant["individer-all"])
        assert runs == sorted(runs)

    def test_open_ended_valid_to_sentinel_is_none(self) -> None:
        # The `9999-12-31` open-end sentinel normalizes to None on the wire (so the
        # renderer reads "ongoing", not a year-9999 tick). Two value-set-distinct
        # states keep the node renderable; the open-ended later state is the one to
        # check (the bounded earlier one keeps its explicit valid_to).
        conn = build_slugged_db()
        add_value_set(conn, value_set_id=1, codes=[("1", "Man"), ("2", "Kvinna")])
        add_value_set(conn, value_set_id=2, codes=[("1", "M"), ("2", "K"), ("3", "X")])
        conn.execute("DELETE FROM variable_state")
        add_state(
            conn,
            register_id=1,
            variable_slug="kon",
            register_variant_id=10,
            valid_from="2018-01-01",
            valid_to="2018-12-31",
            delivery_column_name="Kon",
            value_set_id=1,
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="kon",
            register_variant_id=10,
            valid_from="2019-01-01",
            valid_to="9999-12-31",
            delivery_column_name="Kon",
            value_set_id=2,
        )
        conn.commit()
        node = Catalog(conn).graph_for_fqid(_KON).nodes[0]
        assert isinstance(node, VariableGraphNode)
        by_from = {s.valid_from: s.valid_to for s in node.states}
        assert by_from["2018-01-01"] == "2018-12-31"
        assert by_from["2019-01-01"] is None


# ── Edges ────────────────────────────────────────────────────────────────────


class TestEdges:
    def test_succession_edge_directed(self) -> None:
        conn = build_slugged_db()
        add_variable(conn, register_id=1, var_id=45, name="Civ", slug="civilstand")
        add_state(
            conn,
            register_id=1,
            variable_slug="civilstand",
            register_variant_id=10,
            delivery_column_name="Civ",
        )
        _seed_replaced_by(
            conn,
            predecessor=("scb", "lisa", "kon"),
            successor=("scb", "lisa", "civilstand"),
            reason="renamed",
        )
        g = Catalog(conn).graph_for_fqid(_KON)
        succ = [e for e in g.edges if e.kind == "succession"]
        assert len(succ) == 1
        assert succ[0].source == "scb/lisa/kon"
        assert succ[0].target == "scb/lisa/civilstand"
        assert succ[0].label == "renamed"

    def test_undirected_related_dedups_from_both_ends(self) -> None:
        # The same relation seen from kon's and kon-alt's side must collapse to ONE
        # edge (canonicalized endpoints). Both members are reached (kon-alt via the
        # related neighbor walk), and the related table holds both directions.
        conn = build_slugged_db()
        add_variable(conn, register_id=1, var_id=46, name="Kön alt", slug="kon-alt")
        add_state(
            conn,
            register_id=1,
            variable_slug="kon-alt",
            register_variant_id=10,
            delivery_column_name="KonAlt",
        )
        _seed_related(conn, ("scb", "lisa", "kon"), ("scb", "lisa", "kon-alt"))
        g = Catalog(conn).graph_for_fqid(_KON)
        related = [e for e in g.edges if e.kind == "related"]
        assert len(related) == 1
        assert {related[0].source, related[0].target} == {
            "scb/lisa/kon",
            "scb/lisa/kon-alt",
        }
        # Both endpoints are real nodes.
        assert {n.id for n in g.nodes} >= {"scb/lisa/kon", "scb/lisa/kon-alt"}

    def test_related_expansion_is_one_hop(self) -> None:
        # A related B, B related C. Querying A pulls A + its immediate related
        # neighbor B (one hop), but NOT C (related-of-related). The #761 union is the
        # subject + its succession chains + related edges among/from the union, not
        # the transitive closure of related.
        conn = build_slugged_db()  # A = kon
        for vid, slug, col in ((45, "btwo", "BTwo"), (46, "cthree", "CThree")):
            add_variable(conn, register_id=1, var_id=vid, name=slug, slug=slug)
            add_state(
                conn,
                register_id=1,
                variable_slug=slug,
                register_variant_id=10,
                delivery_column_name=col,
            )
        _seed_related(conn, ("scb", "lisa", "kon"), ("scb", "lisa", "btwo"))
        _seed_related(conn, ("scb", "lisa", "btwo"), ("scb", "lisa", "cthree"))
        g = Catalog(conn).graph_for_fqid(_KON)
        ids = {n.id for n in g.nodes}
        assert "scb/lisa/kon" in ids
        assert "scb/lisa/btwo" in ids  # one hop
        assert "scb/lisa/cthree" not in ids  # NOT related-of-related
        # Only the A--B related edge, never B--C.
        related = {(e.source, e.target) for e in g.edges if e.kind == "related"}
        assert related == {("scb/lisa/btwo", "scb/lisa/kon")}

    def test_member_pre_seeded_as_neighbor_still_expands_related(self) -> None:
        # Order-dependent dropped-edge regression: group {A, B} with A related B and
        # B related C (C outside the group). graph_for_fqid(A) reaches B FIRST as A's
        # one-hop related neighbor (follow_related=False, B built but un-expanded),
        # THEN as a group member (follow_related=True). The member arrival must
        # COMPLETE B's related expansion so its one-hop neighbor C appears — B is a
        # member, so its one-hop neighbor C is in the union regardless of which order
        # B/its neighbors are reached. Before the fix the unconditional node-dedup
        # early-out dropped C.
        conn = build_slugged_db()  # A = kon
        for vid, slug, col in ((45, "btwo", "BTwo"), (46, "cthree", "CThree")):
            add_variable(conn, register_id=1, var_id=vid, name=slug, slug=slug)
            add_state(
                conn,
                register_id=1,
                variable_slug=slug,
                register_variant_id=10,
                delivery_column_name=col,
            )
        _seed_related(conn, ("scb", "lisa", "kon"), ("scb", "lisa", "btwo"))
        _seed_related(conn, ("scb", "lisa", "btwo"), ("scb", "lisa", "cthree"))
        _add_concept_group(
            conn,
            group_id=40,
            register_id=1,
            group_key="demog",
            member_slugs=["kon", "btwo"],
        )
        catalog = Catalog(conn)

        g = catalog.graph_for_fqid(_KON)
        ids = {n.id for n in g.nodes}
        # C is B's one-hop neighbor and B is a member, so C is in the union.
        assert ids >= {"scb/lisa/kon", "scb/lisa/btwo", "scb/lisa/cthree"}
        related = {(e.source, e.target) for e in g.edges if e.kind == "related"}
        assert ("scb/lisa/btwo", "scb/lisa/cthree") in related

        # Same union via the group accessor, independent of member traversal order.
        gg = catalog.graph_for_group("scb", "lisa", "demog")
        assert gg is not None
        assert {n.id for n in gg.nodes} >= {
            "scb/lisa/kon",
            "scb/lisa/btwo",
            "scb/lisa/cthree",
        }

    def test_alias_entry_keys_on_canonical_node(self) -> None:
        # A pure-alias FQID (no live variable row) resolving via same_as to a
        # DIFFERENT canonical variable must mint exactly ONE node for that variable,
        # keyed on the CANONICAL id — not the alias. The focus node's succession edge
        # must reference the canonical id (no orphan/duplicate alias node).
        conn = build_slugged_db()
        add_variable(conn, register_id=1, var_id=45, name="Civ", slug="civilstand")
        add_state(
            conn,
            register_id=1,
            variable_slug="civilstand",
            register_variant_id=10,
            delivery_column_name="Civ",
        )
        # kon has a succession edge → its graph is non-empty.
        _seed_replaced_by(
            conn,
            predecessor=("scb", "lisa", "kon"),
            successor=("scb", "lisa", "civilstand"),
            reason="renamed",
        )
        # `kon-alias` has NO live variable row; it resolves THROUGH to `kon`.
        _seed_same_as(conn, ("scb", "lisa", "kon-alias"), ("scb", "lisa", "kon"))
        g = Catalog(conn).graph_for_fqid("scb/lisa/kon-alias")
        # The focus is the CANONICAL node, never the alias.
        assert g.focus_id == "scb/lisa/kon"
        assert "scb/lisa/kon-alias" not in {n.id for n in g.nodes}
        # Exactly one node per variable — no duplicate alias/canonical pair for kon.
        kon_nodes = [n for n in g.nodes if n.id == "scb/lisa/kon"]
        assert len(kon_nodes) == 1
        # The focus node's succession edge references the canonical id, and the focus
        # is actually connected to its own edges.
        succ = [e for e in g.edges if e.kind == "succession"]
        assert len(succ) == 1
        assert succ[0].source == "scb/lisa/kon"
        assert g.focus_id in {succ[0].source, succ[0].target}


# ── Variable groups (Fork B) ─────────────────────────────────────────────────


class TestVariableGroups:
    def test_member_renders_group_union_with_focus(self) -> None:
        conn = build_slugged_db()
        add_variable(conn, register_id=1, var_id=45, name="Civ", slug="civilstand")
        add_state(
            conn,
            register_id=1,
            variable_slug="civilstand",
            register_variant_id=10,
            delivery_column_name="Civ",
        )
        _add_concept_group(
            conn,
            group_id=40,
            register_id=1,
            group_key="demog",
            member_slugs=["kon", "civilstand"],
        )
        g = Catalog(conn).graph_for_fqid(_KON)
        ids = {n.id for n in g.nodes}
        assert ids == {"scb/lisa/kon", "scb/lisa/civilstand"}
        assert g.focus_id == _KON
        assert all(n.group_key == "demog" for n in g.nodes)

    def test_group_addressed_has_no_focus(self) -> None:
        conn = build_slugged_db()
        add_variable(conn, register_id=1, var_id=45, name="Civ", slug="civilstand")
        add_state(
            conn,
            register_id=1,
            variable_slug="civilstand",
            register_variant_id=10,
            delivery_column_name="Civ",
        )
        _add_concept_group(
            conn,
            group_id=40,
            register_id=1,
            group_key="demog",
            member_slugs=["kon", "civilstand"],
        )
        g = Catalog(conn).graph_for_group("scb", "lisa", "demog")
        assert g is not None
        assert g.focus_id is None
        assert {n.id for n in g.nodes} == {"scb/lisa/kon", "scb/lisa/civilstand"}

    def test_unknown_group_is_none(self) -> None:
        conn = build_slugged_db()
        assert Catalog(conn).graph_for_group("scb", "lisa", "nope") is None

    def test_group_shared_succession_edge_deduped(self) -> None:
        # Two group members where A (kon) is the predecessor of B (civilstand): the
        # union surfaces the SAME succession edge from both members, but dedup-by-id
        # collapses it to ONE edge (source=A, target=B).
        conn = build_slugged_db()
        add_variable(conn, register_id=1, var_id=45, name="Civ", slug="civilstand")
        add_state(
            conn,
            register_id=1,
            variable_slug="civilstand",
            register_variant_id=10,
            delivery_column_name="Civ",
        )
        _seed_replaced_by(
            conn,
            predecessor=("scb", "lisa", "kon"),
            successor=("scb", "lisa", "civilstand"),
            reason="renamed",
        )
        _add_concept_group(
            conn,
            group_id=40,
            register_id=1,
            group_key="demog",
            member_slugs=["kon", "civilstand"],
        )
        g = Catalog(conn).graph_for_group("scb", "lisa", "demog")
        assert g is not None
        succ = [e for e in g.edges if e.kind == "succession"]
        assert len(succ) == 1
        assert (succ[0].source, succ[0].target) == (
            "scb/lisa/kon",
            "scb/lisa/civilstand",
        )

    def test_fork_b_entry_independent(self) -> None:
        # Fork B: graph_for_fqid on two DIFFERENT members of the same group yields
        # identical node/edge SETS — only focus_id differs.
        conn = build_slugged_db()
        add_variable(conn, register_id=1, var_id=45, name="Civ", slug="civilstand")
        add_state(
            conn,
            register_id=1,
            variable_slug="civilstand",
            register_variant_id=10,
            delivery_column_name="Civ",
        )
        _add_concept_group(
            conn,
            group_id=40,
            register_id=1,
            group_key="demog",
            member_slugs=["kon", "civilstand"],
        )
        catalog = Catalog(conn)
        g_kon = catalog.graph_for_fqid(_KON)
        g_civ = catalog.graph_for_fqid("scb/lisa/civilstand")
        assert {n.id for n in g_kon.nodes} == {n.id for n in g_civ.nodes}
        assert {e.id for e in g_kon.edges} == {e.id for e in g_civ.edges}
        assert g_kon.focus_id == _KON
        assert g_civ.focus_id == "scb/lisa/civilstand"
        assert g_kon.focus_id != g_civ.focus_id


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
        assert nodes["class/sun2020"].is_current is True
        # Two directed succession edges, deduped (no duplicate from co-membership).
        succ = [e for e in g.edges if e.kind == "succession"]
        assert len(succ) == 2
        assert {(e.source, e.target) for e in succ} == {
            ("class/sun1996", "class/sun2000"),
            ("class/sun2000", "class/sun2020"),
        }

    def test_sun_umbrella_members_present_and_deduped(self) -> None:
        # A SUN-style umbrella with two members sharing a chain: editions present,
        # deduped (no double nodes), no `group:sun` node.
        conn = build_slugged_db(classification=None)
        _add_classification(conn, cid=1, slug="sun1996")
        _add_classification(conn, cid=2, slug="sun2000-niva")
        _add_class_succession(
            conn, predecessor="sun1996", successor="sun2000-niva", effective_year=2000
        )
        conn.execute(
            "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
            "label, source) VALUES (12, 'classification', NULL, 'sun', 'SUN', 'curated')"
        )
        conn.executemany(
            "INSERT INTO concept_group_classification (classification_id, group_id, "
            "facet_value, facet_label) VALUES (?, 12, ?, ?)",
            [(1, "1996", "1996"), (2, "2000", "2000")],
        )
        conn.commit()
        g = Catalog(conn).graph_for_classification_group("sun")
        assert g is not None
        ids = [n.id for n in g.nodes]
        assert sorted(ids) == ["class/sun1996", "class/sun2000-niva"]
        assert len(ids) == len(set(ids))  # deduped
        assert all(not n.id.startswith("group:") for n in g.nodes)
        # Exactly one shared succession edge across the two members (no double from
        # co-membership reaching the same chain).
        succ = [e for e in g.edges if e.kind == "succession"]
        assert len(succ) == 1
        assert (succ[0].source, succ[0].target) == (
            "class/sun1996",
            "class/sun2000-niva",
        )

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
