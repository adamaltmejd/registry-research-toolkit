"""Graph nodes on ``Catalog.graph_for_fqid`` / ``graph_for_group`` (#761).

Empty-graph gating, node metadata and variable-group unions, built over the
synthetic slugged DB (the shared ``_slugged_db`` factory).
"""

from __future__ import annotations

from _slugged_db import (
    add_register,
    add_state,
    add_value_set,
    add_variable,
    add_variant,
    build_slugged_db,
)
from graph_test_support import (
    KON as _KON,
    add_concept_group as _add_concept_group,
    seed_replaced_by as _seed_replaced_by,
)
from reg_meta.catalog import Catalog
from reg_meta.graph import VariableGraphNode

# ── Empty vs non-empty ───────────────────────────────────────────────────────


class TestEmptyGraph:
    def test_lone_variable_type_only_split_is_empty(self) -> None:
        # The `akters` case: a lone variable, no succession / group, whose
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

        # A lone classification reached via a group of one would be empty too, but a
        # standalone classification is only reachable via the group accessor here;
        # the variable-graph empty path is the akters case above.


# ── Node metadata (definition / description) ─────────────────────────────────


class TestNodeMetadata:
    def test_definition_description_flow_onto_variable_node(self) -> None:
        # #678: the variable's shared concept text (`ResolvedVariable.definition` /
        # `description`) is carried on its graph node so the group page can surface
        # the shared concept definition/description from the member union alone.
        # #892/#932: `operational_definition` rides the same path — it's the per-member
        # distinguishing text that lets the group page tell parallel siblings apart.
        # Seed them on the resolving variable + a meaningful representation change so
        # the node renders.
        conn = build_slugged_db()
        conn.execute(
            "UPDATE variable SET definition = ?, description = ?, "
            "operational_definition = ? WHERE slug = 'kon'",
            (
                "The legal sex of the individual.",
                "Coded one digit, SCB standard.",
                "Registered sex at year end.",
            ),
        )
        add_value_set(conn, value_set_id=1, codes=[("1", "Man"), ("2", "Kvinna")])
        add_value_set(conn, value_set_id=2, codes=[("1", "M"), ("2", "K")])
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
        assert node.definition == "The legal sex of the individual."
        assert node.description == "Coded one digit, SCB standard."
        assert node.operational_definition == "Registered sex at year end."

    def test_metadata_absent_is_none(self) -> None:
        # The seed leaves definition/description NULL → the node carries None (the
        # common parallel-column-sibling case, which the group page dedups away).
        conn = build_slugged_db()
        add_value_set(conn, value_set_id=1, codes=[("1", "Man"), ("2", "Kvinna")])
        add_value_set(conn, value_set_id=2, codes=[("1", "M"), ("2", "K")])
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
        assert node.definition is None
        assert node.description is None
        assert node.operational_definition is None


# ── Variable groups (Fork B) ─────────────────────────────────────────────────


class TestVariableGroups:
    def test_grouped_solo_variable_renders(self) -> None:
        # #791 regression: a single-member variable group is still a group view.
        # Even with no succession edges and one representation run, the grouped
        # node must not be suppressed by the empty-solo gate.
        conn = build_slugged_db()
        _add_concept_group(
            conn,
            group_id=40,
            register_id=1,
            group_key="demog",
            member_slugs=["kon"],
        )
        g = Catalog(conn).graph_for_fqid(_KON)
        assert g.focus_id == _KON
        assert g.edges == []
        (node,) = g.nodes
        assert isinstance(node, VariableGraphNode)
        assert node.id == _KON
        assert node.group_key == "scb/lisa/demog"
        assert [s.representation_run_id for s in node.states] == [0]

    def test_group_key_namespaced_by_register(self) -> None:
        # P2-2 regression: concept-group keys are only register-unique, so a graph
        # spanning >1 register (a cross-register succession edge here) must NOT emit
        # the same `group_key` for two unrelated groups that happen to share a bare
        # key. Two registers (lisa, other) each carry a group with the SAME bare key
        # "demog"; their members are joined into one graph by a succession edge.
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
        # Cross-register succession edge joins kon (lisa) and inkomst (other) into
        # one graph (distinct nodes — succession is not identity).
        _seed_replaced_by(
            conn,
            predecessor=("scb", "lisa", "kon"),
            successor=("scb", "other", "inkomst"),
        )
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
