"""Graph edges on ``Catalog.graph_for_fqid`` (#761): variable and
representation succession, thin chain nodes and same_as aliases, built over the
synthetic slugged DB (the shared ``_slugged_db`` factory).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from _slugged_db import add_state, add_variable, add_variant, build_slugged_db
from graph_test_support import (
    KON as _KON,
    add_concept_group as _add_concept_group,
    seed_replaced_by as _seed_replaced_by,
)
from reg_meta.catalog import Catalog
from reg_meta.graph import VariableGraphNode

if TYPE_CHECKING:
    import sqlite3


def _seed_representation_replaced_by(
    conn: sqlite3.Connection,
    *,
    predecessor: tuple[str, str, str, str],
    successor: tuple[str, str, str, str],
    variant: str = "",
    reason: str | None = None,
    effective_year: int | None = None,
) -> None:
    conn.execute(
        "INSERT INTO representation_replaced_by ("
        "predecessor_provider, predecessor_register, predecessor_variable, "
        "predecessor_column, successor_provider, successor_register, "
        "successor_variable, successor_column, variant, effective_year, note, "
        "beskrivning) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (*predecessor, *successor, variant, effective_year, "curated:test", reason),
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


# ── Edges ────────────────────────────────────────────────────────────────────


class TestEdges:
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

    def test_live_chain_only_successor_walks_representation_succession(self) -> None:
        # #888 regression: if A reaches B only through variable succession, B is
        # hydrated by `_ensure_edition_node`. That live hydration must also walk B's
        # representation-grain succession edges, otherwise B:col -> C:col disappears
        # unless B or C is queried directly.
        conn = build_slugged_db()
        add_variable(
            conn, register_id=1, var_id=45, name="Chain member", slug="chain-member"
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="chain-member",
            register_variant_id=10,
            delivery_column_name="B1",
        )
        add_variable(
            conn, register_id=1, var_id=46, name="Rep successor", slug="rep-successor"
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="rep-successor",
            register_variant_id=10,
            delivery_column_name="C1",
        )
        _seed_replaced_by(
            conn,
            predecessor=("scb", "lisa", "kon"),
            successor=("scb", "lisa", "chain-member"),
            reason="renamed",
        )
        _seed_representation_replaced_by(
            conn,
            predecessor=("scb", "lisa", "chain-member", "B1"),
            successor=("scb", "lisa", "rep-successor", "C1"),
            effective_year=2020,
        )

        g = Catalog(conn).graph_for_fqid(_KON)

        assert {n.id for n in g.nodes} == {
            _KON,
            "scb/lisa/chain-member",
            "scb/lisa/rep-successor",
        }
        assert {
            (e.source, e.target, e.source_column, e.target_column) for e in g.edges
        } == {
            ("scb/lisa/kon", "scb/lisa/chain-member", None, None),
            ("scb/lisa/chain-member", "scb/lisa/rep-successor", "B1", "C1"),
        }

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

    def test_representation_succession_edge_carries_columns_and_year(self) -> None:
        # #888: representation-grain succession is a graph edge with variable-node
        # endpoints plus column endpoint metadata, so the renderer can map it to
        # representation-run cells instead of treating it as a variable-level rename.
        conn = build_slugged_db()
        conn.execute("DELETE FROM variable_state")
        add_state(
            conn,
            register_id=1,
            variable_slug="kon",
            register_variant_id=10,
            valid_from="2010-01-01",
            valid_to="2013-12-31",
            delivery_column_name="BorgNr",
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="kon",
            register_variant_id=10,
            valid_from="2014-01-01",
            delivery_column_name="PersOrgNr",
        )
        _seed_representation_replaced_by(
            conn,
            predecessor=("scb", "lisa", "kon", "BorgNr"),
            successor=("scb", "lisa", "kon", "PersOrgNr"),
            reason="identifier rename",
            effective_year=2014,
        )

        g = Catalog(conn).graph_for_fqid(_KON)

        assert {n.id for n in g.nodes} == {_KON}
        (edge,) = g.edges
        assert edge.source == _KON
        assert edge.target == _KON
        assert edge.source_column == "BorgNr"
        assert edge.target_column == "PersOrgNr"
        assert edge.variant is None
        assert edge.label == "identifier rename"
        assert edge.effective_year == 2014

    def test_variant_scoped_representation_edge_keeps_variant_scope(self) -> None:
        # #846/#888: a variant-local rename must not render as global. The graph
        # carries the scoped register-variant slug so consumers can filter the edge
        # when a different variant is in view.
        conn = build_slugged_db()
        add_variant(
            conn,
            register_variant_id=11,
            register_id=1,
            slug="punktskatter-for-energi",
            name="Punktskatter för energi",
        )
        conn.execute("DELETE FROM variable_state")
        add_state(
            conn,
            register_id=1,
            variable_slug="kon",
            register_variant_id=10,
            delivery_column_name="BorgNr",
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="kon",
            register_variant_id=11,
            valid_from="2014-01-01",
            valid_to="2017-12-31",
            delivery_column_name="PersOrgNr",
        )
        _seed_representation_replaced_by(
            conn,
            predecessor=("scb", "lisa", "kon", "BorgNr"),
            successor=("scb", "lisa", "kon", "PersOrgNr"),
            variant="punktskatter-for-energi",
            effective_year=2014,
        )

        (edge,) = Catalog(conn).graph_for_fqid(_KON).edges
        assert edge.source_column == "BorgNr"
        assert edge.target_column == "PersOrgNr"
        assert edge.variant == "punktskatter-for-energi"

    def test_variant_scoped_representation_round_trip_terminates(self) -> None:
        # #846 permits time-monotone variant-scoped round trips. The graph walk is
        # edge-key guarded, so reading both directions for one variable terminates
        # and emits each curated edge once.
        conn = build_slugged_db()
        add_variant(
            conn,
            register_variant_id=11,
            register_id=1,
            slug="punktskatter-for-energi",
            name="Punktskatter för energi",
        )
        conn.execute("DELETE FROM variable_state")
        for vf, vt, col in (
            ("2007-01-01", "2013-12-31", "BorgNr"),
            ("2014-01-01", "2017-12-31", "PersOrgNr"),
            ("2018-01-01", "9999-12-31", "BorgNr"),
        ):
            add_state(
                conn,
                register_id=1,
                variable_slug="kon",
                register_variant_id=11,
                valid_from=vf,
                valid_to=vt,
                delivery_column_name=col,
            )
        for pred_col, succ_col, year in (
            ("BorgNr", "PersOrgNr", 2014),
            ("PersOrgNr", "BorgNr", 2018),
        ):
            _seed_representation_replaced_by(
                conn,
                predecessor=("scb", "lisa", "kon", pred_col),
                successor=("scb", "lisa", "kon", succ_col),
                variant="punktskatter-for-energi",
                effective_year=year,
            )

        g = Catalog(conn).graph_for_fqid(_KON)

        assert len(g.edges) == 2
        assert {
            (e.source_column, e.target_column, e.variant, e.effective_year)
            for e in g.edges
        } == {
            ("BorgNr", "PersOrgNr", "punktskatter-for-energi", 2014),
            ("PersOrgNr", "BorgNr", "punktskatter-for-energi", 2018),
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
