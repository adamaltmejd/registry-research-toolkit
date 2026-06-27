"""Tests for the catalog relationship-graph contract (#761).

The graph model + builders live in ``reg_meta/graph.py``; ``Catalog`` exposes
``graph_for_fqid`` / ``graph_for_group`` / ``graph_for_classification_group``.
These build a synthetic slugged DB via the shared ``_slugged_db`` factory (the
same one ``test_catalog.py`` uses) and assert the topology + representation-run +
empty-graph + dedup semantics the issue pins.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _slugged_db import (
    add_register,
    add_state,
    add_value_set,
    add_variable,
    add_variant,
    build_slugged_db,
)
from reg_meta.catalog import BindingGroupRef, Catalog, ResolvedVariable
from reg_meta.errors import RegMetaError
from reg_meta.fqid import Fqid
from reg_meta.graph import (
    ClassificationGraphNode,
    VariableGraphNode,
    _GraphBuilder,
    graph_for_classification_fqid,
)

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
    facet_axis: str | None = None,
    facets: dict[str, tuple[str, str]] | None = None,
) -> None:
    # `facet_axis` is the group's single axis (None = edge group, facet-less
    # members); when set it lands as the group's one `concept_group_axis` row
    # (#819, multi-axis shape — the inline `concept_group.facet_axis` column is
    # gone). `facets` maps a member slug → (value, label) on that axis; members
    # absent from it (and every member of an axis-less group) get no facet row, so
    # the accessor surfaces empty `member.facets`.
    conn.execute(
        "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
        "label, source) VALUES (?, 'variable', ?, ?, ?, 'curated')",
        (group_id, register_id, group_key, f"Group {group_key}"),
    )
    if facet_axis is not None:
        conn.execute(
            "INSERT INTO concept_group_axis (group_id, axis, ordinal, label) "
            "VALUES (?, ?, 0, ?)",
            (group_id, facet_axis, facet_axis),
        )
    facets = facets or {}
    for slug in member_slugs:
        vid = conn.execute(
            "SELECT variable_id FROM variable WHERE register_id = ? AND slug = ?",
            (register_id, slug),
        ).fetchone()[0]
        cur = conn.execute(
            "INSERT INTO concept_group_variable "
            "(group_id, variable_id, delivery_column_name) VALUES (?, ?, NULL)",
            (group_id, vid),
        )
        if facet_axis is not None and (facet := facets.get(slug)) is not None:
            value, label = facet
            conn.execute(
                "INSERT INTO concept_group_variable_facet "
                "(member_id, axis, value, label) VALUES (?, ?, ?, ?)",
                (cur.lastrowid, facet_axis, value, label),
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

    def test_unknown_start_valid_from_sentinel_is_none(self) -> None:
        # The `0001-01-01` unknown-START sentinel normalizes to None on the wire
        # (mirroring the `9999-12-31` open-END → None), so the renderer reads "unknown
        # start", not a year-1 tick. Two value-set-distinct states keep the node
        # renderable; the earlier state carries the unknown-start sentinel.
        conn = build_slugged_db()
        add_value_set(conn, value_set_id=1, codes=[("1", "Man"), ("2", "Kvinna")])
        add_value_set(conn, value_set_id=2, codes=[("1", "M"), ("2", "K"), ("3", "X")])
        conn.execute("DELETE FROM variable_state")
        add_state(
            conn,
            register_id=1,
            variable_slug="kon",
            register_variant_id=10,
            valid_from="0001-01-01",
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
            valid_to="2019-12-31",
            delivery_column_name="Kon",
            value_set_id=2,
        )
        conn.commit()
        node = Catalog(conn).graph_for_fqid(_KON).nodes[0]
        assert isinstance(node, VariableGraphNode)
        by_to = {s.valid_to: s.valid_from for s in node.states}
        # The unknown-start state's valid_from is normalized to None.
        assert by_to["2018-12-31"] is None
        assert by_to["2019-12-31"] == "2019-01-01"


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

    def test_variable_succession_edge_carries_effective_year(self) -> None:
        # #794 P2: the `variable_replaced_by.effective_year` (the transition year the
        # retired LineagePanels showed) must ride on the succession edge so the #678
        # timeline can annotate the transition with its year — independently of the
        # human reason (a year-only edge would otherwise render as an unlabelled
        # arrow). Here the edge has BOTH a reason and an effective_year.
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
            effective_year=2009,
        )
        g = Catalog(conn).graph_for_fqid(_KON)
        (succ,) = [e for e in g.edges if e.kind == "succession"]
        assert succ.effective_year == 2009
        assert succ.label == "renamed"  # year is carried ALONGSIDE the reason

    def test_thin_chain_node_hydrated_when_later_a_member(self) -> None:
        # P2-1 regression: focus A (kon) succeeds-to a LIVE successor B (civilstand)
        # that is ALSO a group member. Walking A's succession chain reaches B FIRST as
        # a thin `_ensure_edition_node` placeholder (states=[], group_key=None). When B
        # later arrives as a group member, the node-dedup early-out must NOT leave it
        # thin — B is a live variable node and must carry its full state history + its
        # group_key, regardless of A being processed first.
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
        g = Catalog(conn).graph_for_fqid(_KON)
        nodes = {n.id: n for n in g.nodes}
        b = nodes["scb/lisa/civilstand"]
        assert isinstance(b, VariableGraphNode)
        # B is hydrated: it carries its own state history and its group_key, not the
        # thin placeholder it was first reached as.
        assert b.states != []
        assert b.group_key == "scb/lisa/demog"

    def test_live_chain_only_successor_carries_states(self) -> None:
        # F1 regression: focus A (kon) succeeds-to a LIVE successor B (civilstand)
        # that is NOT a group member and is NOT separately added — B is reached ONLY
        # via A's succession chain. Every LIVE variable node must carry its full
        # `variable_state` history (the #678 timeline renders states as cells), so B
        # must NOT stay the thin `_ensure_edition_node` placeholder. Its group_key (B
        # is grouped here, A is not) must also be carried.
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
        # B (civilstand) is grouped; A (kon) is NOT in the group and NOT a member of
        # the chain-only successor's group — B is reached purely via A's chain.
        _add_concept_group(
            conn,
            group_id=40,
            register_id=1,
            group_key="civ-only",
            member_slugs=["civilstand"],
        )
        g = Catalog(conn).graph_for_fqid(_KON)
        b = {n.id: n for n in g.nodes}["scb/lisa/civilstand"]
        assert isinstance(b, VariableGraphNode)
        # B is live → hydrated with its own states + group_key, despite being reached
        # only through the succession chain.
        assert b.states != []
        assert b.group_key == "scb/lisa/civ-only"

    def test_dead_predecessor_stays_thin(self) -> None:
        # F1 boundary: a genuinely DEAD/renamed predecessor (no live `variable` row,
        # #355/#411) must STILL render as a THIN node (states=[]) — hydration is
        # gated on liveness (`resolve` raising fqid_not_found / name None), so it must
        # NOT accidentally try to hydrate an unresolvable edition. The dead edition
        # keeps its 301-redirecting fqid + label but carries no states.
        conn = build_slugged_db()  # live scb/lisa/kon
        # dead-old is NOT add_variable'd — only the succession edge exists.
        _seed_replaced_by(
            conn,
            predecessor=("scb", "lisa", "dead-old"),
            successor=("scb", "lisa", "kon"),
            reason="2015 omdöpt",
        )
        g = Catalog(conn).graph_for_fqid(_KON)
        nodes = {n.id: n for n in g.nodes}
        dead = nodes["scb/lisa/dead-old"]
        assert isinstance(dead, VariableGraphNode)
        assert dead.states == []  # thin — no live row to hydrate
        assert dead.group_key is None
        # The succession edge still connects the dead predecessor to the live current.
        succ = [e for e in g.edges if e.kind == "succession"]
        assert (succ[0].source, succ[0].target) == ("scb/lisa/dead-old", "scb/lisa/kon")

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
        # group_key is namespaced by provider/register (register-only-unique keys must
        # not collide across registers in a cross-register graph).
        assert all(n.group_key == "scb/lisa/demog" for n in g.nodes)

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

    def test_group_key_namespaced_by_register(self) -> None:
        # P2-2 regression: concept-group keys are only register-unique, so a graph
        # spanning >1 register (a cross-register related edge here) must NOT emit the
        # same `group_key` for two unrelated groups that happen to share a bare key.
        # Two registers (lisa, other) each carry a group with the SAME bare key
        # "demog"; their members are joined into one graph by a related edge.
        # Namespacing by provider/register keeps the two clusters distinct.
        conn = build_slugged_db()
        add_register(conn, register_id=2, slug="other", name="OTHER")
        add_variant(
            conn, register_variant_id=20, register_id=2, slug="v-other", name="V"
        )
        add_variable(conn, register_id=2, var_id=90, name="Ink", slug="inkomst")
        add_state(
            conn,
            register_id=2,
            variable_slug="inkomst",
            register_variant_id=20,
            valid_from="2018-01-01",
            delivery_column_name="Ink",
        )
        # Cross-register related edge joins kon (lisa) and inkomst (other) into one
        # graph.
        _seed_related(conn, ("scb", "lisa", "kon"), ("scb", "other", "inkomst"))
        # Both registers have a group with the SAME bare key "demog".
        _add_concept_group(
            conn, group_id=40, register_id=1, group_key="demog", member_slugs=["kon"]
        )
        _add_concept_group(
            conn,
            group_id=41,
            register_id=2,
            group_key="demog",
            member_slugs=["inkomst"],
        )
        g = Catalog(conn).graph_for_fqid(_KON)
        by_id = {n.id: n for n in g.nodes}
        kon = by_id["scb/lisa/kon"]
        inkomst = by_id["scb/other/inkomst"]
        # Same bare key, but namespaced → DIFFERENT group_key values.
        assert kon.group_key == "scb/lisa/demog"
        assert inkomst.group_key == "scb/other/demog"
        assert kon.group_key != inkomst.group_key

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


# ── Variable-node facets / group_label (#792, #670 header identity) ──────────


class TestVariableNodeFacets:
    def test_grouped_variable_carries_facets_and_label(self) -> None:
        # A grouped variable's node carries its own member facets (axis + label) from
        # the canonical group, plus the group's display label — the #670 header
        # identity, derivable from the graph alone (no /dimensions fetch).
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
            facet_axis="rank",
            facets={"kon": ("1", "primary"), "civilstand": ("2", "secondary")},
        )
        g = Catalog(conn).graph_for_fqid(_KON)
        nodes = {n.id: n for n in g.nodes}
        kon = nodes["scb/lisa/kon"]
        civ = nodes["scb/lisa/civilstand"]
        assert isinstance(kon, VariableGraphNode)
        assert isinstance(civ, VariableGraphNode)
        # Each member carries its OWN facet (not the whole group's), with the group's
        # axis and the member's label.
        assert [(f.axis, f.value, f.label) for f in kon.facets] == [
            ("rank", "1", "primary")
        ]
        assert [(f.axis, f.value, f.label) for f in civ.facets] == [
            ("rank", "2", "secondary")
        ]
        # group_label is the group's display label on both members.
        assert kon.group_label == "Group demog"
        assert civ.group_label == "Group demog"

    def test_multi_representation_member_picks_representative_not_union(self) -> None:
        # #819: one variable can be SEVERAL members of a group (one per
        # delivery_column), each carrying its own facet. These per-column
        # representations are MUTUALLY EXCLUSIVE, so the variable-grain node must NOT
        # union them (that produced an incoherent #678 leaf header mixing both
        # variants — P2) — it carries ONE REPRESENTATIVE member's facets: the first
        # matching member in group-member order.
        conn = build_slugged_db()
        add_variable(conn, register_id=1, var_id=45, name="Civ", slug="civilstand")
        add_state(
            conn,
            register_id=1,
            variable_slug="civilstand",
            register_variant_id=10,
            delivery_column_name="Civ",
        )
        # Base group: kon (whole-variable member, rank '1') + civilstand (so the
        # graph is multi-node and renders).
        _add_concept_group(
            conn,
            group_id=40,
            register_id=1,
            group_key="demog",
            member_slugs=["kon", "civilstand"],
            facet_axis="rank",
            facets={"kon": ("1", "first"), "civilstand": ("3", "other")},
        )
        # Second representation member for kon: same variable, different delivery
        # column + facet ('2', 'second') — must NOT be unioned onto the first; the
        # node keeps the representative (first member) facet only.
        vid = conn.execute(
            "SELECT variable_id FROM variable WHERE register_id = 1 AND slug = 'kon'"
        ).fetchone()[0]
        cur = conn.execute(
            "INSERT INTO concept_group_variable "
            "(group_id, variable_id, delivery_column_name) VALUES (40, ?, 'KonB')",
            (vid,),
        )
        conn.execute(
            "INSERT INTO concept_group_variable_facet "
            "(member_id, axis, value, label) VALUES (?, 'rank', '2', 'second')",
            (cur.lastrowid,),
        )
        conn.commit()
        kon = {n.id: n for n in Catalog(conn).graph_for_fqid(_KON).nodes}[
            "scb/lisa/kon"
        ]
        assert isinstance(kon, VariableGraphNode)
        # ONLY the representative (first) member's facet — the mutually-exclusive
        # 'second' representation is NOT mixed in.
        assert [(f.axis, f.value, f.label) for f in kon.facets] == [
            ("rank", "1", "first"),
        ]
        assert kon.group_label == "Group demog"

    def test_edge_group_member_has_empty_facets_but_label(self) -> None:
        # An axis-less (edge) group: members carry NO facets (facet-less), but the
        # node still gets the group's label so the renderer can link the group.
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
            facet_axis=None,
        )
        kon = {n.id: n for n in Catalog(conn).graph_for_fqid(_KON).nodes}[
            "scb/lisa/kon"
        ]
        assert isinstance(kon, VariableGraphNode)
        assert kon.facets == []
        assert kon.group_label == "Group demog"

    def test_ungrouped_variable_has_no_facets_or_label(self) -> None:
        # An ungrouped variable: facets == [] and group_label is None. Use a node that
        # renders (≥2 representation runs) so the empty-graph gate doesn't drop it.
        conn = build_slugged_db()
        add_value_set(conn, value_set_id=1, codes=[("1", "Man"), ("2", "Kvinna")])
        add_value_set(conn, value_set_id=2, codes=[("1", "M"), ("2", "K"), ("3", "X")])
        conn.execute("DELETE FROM variable_state")
        for vf, vsid in (("2018-01-01", 1), ("2019-01-01", 2)):
            add_state(
                conn,
                register_id=1,
                variable_slug="kon",
                register_variant_id=10,
                valid_from=vf,
                delivery_column_name="Kon",
                value_set_id=vsid,
            )
        conn.commit()
        (node,) = Catalog(conn).graph_for_fqid(_KON).nodes
        assert isinstance(node, VariableGraphNode)
        assert node.group_key is None
        assert node.facets == []
        assert node.group_label is None

    def test_facet_skew_degrades_gracefully(self) -> None:
        # Skew: a `resolved.group` ref whose group/member the summary can't surface
        # must degrade to facets == [] and group_label None — never crash. Exercised
        # at the builder helper boundary directly (a DB-level skew is unreachable:
        # `ResolvedVariable.group` and the group member list read the same
        # `concept_group_variable` row, so they can't disagree there). Two misses:
        # (a) a stale group ADDRESS `concept_group` returns None for; (b) a real group
        # whose member list omits the canonical FQID.
        conn = build_slugged_db()
        catalog = Catalog(conn)
        builder = _GraphBuilder(catalog)
        kon_resolved = catalog.resolve(_KON)
        assert isinstance(kon_resolved, ResolvedVariable)

        # (a) stale group ADDRESS — no such group → concept_group None.
        stale = kon_resolved.model_copy(
            update={
                "group": BindingGroupRef(provider="scb", register="lisa", key="nope")
            }
        )
        assert builder._group_facets(stale) == ([], None)

        # (b) real group, but its member list omits this canonical FQID (the member
        # whose fqid == canonical_fqid isn't found → fall through).
        _add_concept_group(
            conn,
            group_id=40,
            register_id=1,
            group_key="demog",
            member_slugs=["kon"],
            facet_axis="rank",
            facets={"kon": ("1", "primary")},
        )
        mismatched = kon_resolved.model_copy(
            update={
                "group": BindingGroupRef(provider="scb", register="lisa", key="demog"),
                "canonical_fqid": Fqid.binding_fqid("scb", "lisa", "ghost"),
            }
        )
        assert _GraphBuilder(catalog)._group_facets(mismatched) == ([], None)


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
        # #794 P2: each succession edge carries its `classification_replaced_by`
        # effective_year (the supersession year), so the #678 timeline can annotate
        # the transition even though the edition succession reason is suppressed (the
        # internal `note` tag is never shown). The year rides on the edge, NOT on the
        # node's `version_year` (the edition's own vintage).
        by_pair = {(e.source, e.target): e for e in succ}
        assert by_pair[("class/sun1996", "class/sun2000")].effective_year == 2000
        assert by_pair[("class/sun2000", "class/sun2020")].effective_year == 2020

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

    def test_umbrella_members_carry_group_key(self) -> None:
        # F2 regression: a classification umbrella's curated MEMBER editions must carry
        # `group_key = "class/<key>"` so the renderer can cluster umbrella membership
        # (the contract models classification group membership as shared group_key
        # metadata + clustering, NO `group:<key>` node). A non-member edition surfaced
        # by the chain walk keeps `group_key=None` (mirrors the variable side: only the
        # node's OWN membership sets its key).
        conn = build_slugged_db(classification=None)
        _add_classification(conn, cid=1, slug="sun1996")
        _add_classification(conn, cid=2, slug="sun2000-niva")
        _add_classification(conn, cid=3, slug="sun2020-niva")
        _add_class_succession(
            conn, predecessor="sun1996", successor="sun2000-niva", effective_year=2000
        )
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
        # The curated members are the two ENDPOINT editions; sun2000-niva is a mid-
        # chain edition surfaced by the walk but NOT a curated member.
        conn.executemany(
            "INSERT INTO concept_group_classification (classification_id, group_id, "
            "facet_value, facet_label) VALUES (?, 12, ?, ?)",
            [(1, "1996", "1996"), (3, "2020", "2020")],
        )
        conn.commit()
        g = Catalog(conn).graph_for_classification_group("sun")
        assert g is not None
        nodes = {n.id: n for n in g.nodes}
        # Curated members carry the umbrella key.
        assert nodes["class/sun1996"].group_key == "class/sun"
        assert nodes["class/sun2020-niva"].group_key == "class/sun"
        # The non-member mid-chain ancestor keeps group_key None.
        assert nodes["class/sun2000-niva"].group_key is None

    def test_split_non_member_ancestor_group_key_none(self) -> None:
        # F2 boundary at a #579 split: a curated member D (sun2020-niva) and the split
        # ROOT P (sun1996, also a curated member) share a chain, but P's OTHER branches
        # (sun2000-inriktning / sun2000-grupp) are surfaced by P's walk and are NOT
        # curated members → they must carry group_key None, while the curated members
        # carry "class/sun". Confirms only the node's OWN membership sets the key, even
        # when a member is first built by a non-member-anchored walk.
        conn = build_slugged_db(classification=None)
        _add_classification(conn, cid=1, slug="sun1996", valid_from=1996)
        _add_classification(conn, cid=2, slug="sun2000-niva", valid_from=2000)
        _add_classification(conn, cid=3, slug="sun2000-inriktning", valid_from=2000)
        _add_classification(conn, cid=4, slug="sun2000-grupp", valid_from=2000)
        _add_classification(conn, cid=5, slug="sun2020-niva", valid_from=2020)
        for succ in ("sun2000-niva", "sun2000-inriktning", "sun2000-grupp"):
            _add_class_succession(
                conn, predecessor="sun1996", successor=succ, effective_year=2000
            )
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
        # D (facet 1) precedes P (facet 2) → D processed first; P built as D's
        # ancestor (non-member-anchored) BEFORE P's own member-anchored walk upgrades
        # it. Both D and P are curated members.
        conn.executemany(
            "INSERT INTO concept_group_classification (classification_id, group_id, "
            "facet_value, facet_label) VALUES (?, 12, ?, ?)",
            [(5, "1", "niva-2020"), (1, "2", "root-1996")],
        )
        conn.commit()
        g = Catalog(conn).graph_for_classification_group("sun")
        assert g is not None
        nodes = {n.id: n for n in g.nodes}
        # Curated members carry the umbrella key (P even though built first as an
        # ancestor, then upgraded).
        assert nodes["class/sun1996"].group_key == "class/sun"
        assert nodes["class/sun2020-niva"].group_key == "class/sun"
        # Non-member editions surfaced by the walk stay ungrouped.
        assert nodes["class/sun2000-niva"].group_key is None
        assert nodes["class/sun2000-inriktning"].group_key is None
        assert nodes["class/sun2000-grupp"].group_key is None

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

    def test_leaf_in_umbrella_carries_chain_and_co_members(self) -> None:
        # An umbrella where each curated member sits on a SEPARATE chain: querying
        # one member's leaf graph must pull in BOTH the member's own edition chain
        # AND its umbrella co-members' chains (Fork B, deduped), `focus_id` = the
        # queried edition.
        conn = build_slugged_db(classification=None)
        # Member A's chain: sun1996 → sun2000-niva.
        _add_classification(conn, cid=1, slug="sun1996", valid_from=1996)
        _add_classification(conn, cid=2, slug="sun2000-niva", valid_from=2000)
        _add_class_succession(
            conn, predecessor="sun1996", successor="sun2000-niva", effective_year=2000
        )
        # Member B's chain: ssyk1996 → ssyk2012 (disjoint from A).
        _add_classification(conn, cid=3, slug="ssyk1996", valid_from=1996)
        _add_classification(conn, cid=4, slug="ssyk2012", valid_from=2012)
        _add_class_succession(
            conn, predecessor="ssyk1996", successor="ssyk2012", effective_year=2012
        )
        # Curated members = the two terminal editions (one per chain).
        _add_class_umbrella_group(conn, members=[(2, "niva"), (4, "ssyk")])
        g = Catalog(conn).graph_for_classification_fqid("class/sun2000-niva")
        ids = {n.id for n in g.nodes}
        # The queried edition's chain AND the co-member's chain are both present,
        # deduped.
        assert ids == {
            "class/sun1996",
            "class/sun2000-niva",
            "class/ssyk1996",
            "class/ssyk2012",
        }
        assert len([n.id for n in g.nodes]) == len(ids)  # no double nodes
        assert g.focus_id == "class/sun2000-niva"
        # The curated members carry the umbrella key; the non-member predecessors
        # surfaced by the chain walk stay ungrouped (own-membership rule).
        nodes = {n.id: n for n in g.nodes}
        assert nodes["class/sun2000-niva"].group_key == "class/sun"
        assert nodes["class/ssyk2012"].group_key == "class/sun"
        assert nodes["class/sun1996"].group_key is None
        # The chains' succession edges are present.
        edges = {(e.source, e.target) for e in g.edges if e.kind == "succession"}
        assert ("class/sun1996", "class/sun2000-niva") in edges
        assert ("class/ssyk1996", "class/ssyk2012") in edges

    def test_leaf_chain_no_umbrella_is_chain_only_with_focus(self) -> None:
        # A classification with a succession chain but NO umbrella group → just its
        # own edition chain, `focus_id` on the queried edition, all ungrouped.
        conn = build_slugged_db(classification=None)
        _add_classification(conn, cid=1, slug="sun1996", valid_from=1996)
        _add_classification(conn, cid=2, slug="sun2000", valid_from=2000)
        _add_classification(conn, cid=3, slug="sun2020", valid_from=2020)
        _add_class_succession(
            conn, predecessor="sun1996", successor="sun2000", effective_year=2000
        )
        _add_class_succession(
            conn, predecessor="sun2000", successor="sun2020", effective_year=2020
        )
        g = Catalog(conn).graph_for_classification_fqid("class/sun2000")
        assert {n.id for n in g.nodes} == {
            "class/sun1996",
            "class/sun2000",
            "class/sun2020",
        }
        assert g.focus_id == "class/sun2000"
        assert all(n.group_key is None for n in g.nodes)
        edges = {(e.source, e.target) for e in g.edges if e.kind == "succession"}
        assert edges == {
            ("class/sun1996", "class/sun2000"),
            ("class/sun2000", "class/sun2020"),
        }

    def test_focus_node_present_when_not_a_walked_umbrella_member(self) -> None:
        # Fix-1 regression: the canonical focus edition references an umbrella but is
        # NOT itself a walked member of any group (a curation skew / #579 spine
        # edition). The umbrella's curated members sit on a DISJOINT chain, so unioning
        # only the members never reaches the focus — its `focus_id` would point at a
        # missing node. Adding the focus's OWN chain FIRST (mirroring `graph_for_fqid`)
        # guarantees the focus node exists. Driven at the module-function boundary
        # (`graph_for_classification_fqid`) with a hand-built `groups` list that omits
        # the focus, the exact skew the catalog resolver can't normally produce.
        conn = build_slugged_db(classification=None)
        # The umbrella members (a disjoint chain that does NOT include the focus).
        _add_classification(conn, cid=1, slug="ssyk1996", valid_from=1996)
        _add_classification(conn, cid=2, slug="ssyk2012", valid_from=2012)
        _add_class_succession(
            conn, predecessor="ssyk1996", successor="ssyk2012", effective_year=2012
        )
        # The focus edition — live, with NO succession chain and NOT in the umbrella.
        _add_classification(conn, cid=3, slug="sun2020", valid_from=2020)
        _add_class_umbrella_group(conn, members=[(2, "ssyk")])
        catalog = Catalog(conn)
        group = catalog.classification_group("sun")
        assert group is not None  # umbrella of the disjoint members

        g = graph_for_classification_fqid(catalog, "sun2020", [group])
        ids = {n.id for n in g.nodes}
        # The focus node is present even though it isn't a walked umbrella member.
        assert "class/sun2020" in ids
        assert g.focus_id == "class/sun2020"
        # The focus node really exists for that id (not a dangling focus_id).
        assert g.focus_id in ids
        # The disjoint umbrella members are still unioned in.
        assert {"class/ssyk1996", "class/ssyk2012"} <= ids

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

    def test_unknown_classification_raises_not_found(self) -> None:
        conn = build_slugged_db(classification=None)
        with pytest.raises(RegMetaError) as exc:
            Catalog(conn).graph_for_classification_fqid("class/nope")
        assert exc.value.code == "fqid_not_found"
