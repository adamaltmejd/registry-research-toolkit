"""Graph states and representation runs on ``Catalog.graph_for_fqid`` (#761).

A variable node carries its state history as ``GraphState`` rows; the
``representation_run_id`` groups consecutive states into rendered cells. The
node also carries its concept-group facets and group label. Readable-source
cases build through ``build_reader_artifact``; the rest use the synthetic
slugged DB (the shared ``_slugged_db`` factory).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _slugged_db import (
    add_state,
    add_value_set,
    add_variable,
    add_variant,
    build_slugged_db,
)
from graph_test_support import KON as _KON, add_concept_group as _add_concept_group
from reader_artifacts import build_reader_artifact
from reg_meta.catalog import Catalog
from reg_meta.db import open_db
from reg_meta.graph import VariableGraphNode

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture(scope="module")
def graph_states_catalog(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[Catalog]:
    path = build_reader_artifact(
        tmp_path_factory.mktemp("graph-states"), "reader/graph-states", "catalog"
    )
    conn = open_db(path)
    try:
        yield Catalog(conn)
    finally:
        conn.close()


def _annual_state(**fields: object) -> dict[str, object]:
    return {
        "period_scope": "intervals",
        "variant": "annual",
        "variant_label": "Annual",
        "variant_family": None,
        "variant_family_label": None,
        **fields,
    }


def test_month_family_columns_survive_in_one_representation_run(
    graph_states_catalog: Catalog,
) -> None:
    # One annual state delivered as monthly alias windows under distinct columns
    # (#678): every column stays selectable, and the column multiplex is one run.
    [node] = graph_states_catalog.graph_for_fqid("scb/example/pay").nodes
    assert [s.model_dump(mode="json") for s in node.states] == [
        _annual_state(
            state_id="4262345308094636571",
            representation_run_id=0,
            delivery_column_name=column,
            value_set_id="4795145473227566770",
            value_set_version_label="",
            classification_slugs=[],
            valid_from=valid_from,
            valid_to=valid_to,
        )
        for column, valid_from, valid_to in (
            ("Payjan", "2010-01-01", "2010-01-31"),
            ("Payfeb", "2010-02-01", "2010-02-28"),
            ("Paymar", "2010-03-01", "2010-03-31"),
        )
    ] + [
        _annual_state(
            state_id="2574516926826139583",
            representation_run_id=1,
            delivery_column_name="Pay",
            value_set_id="6876754561505949491",
            value_set_version_label="",
            classification_slugs=[],
            valid_from="2011-01-01",
            valid_to="2011-12-31",
        )
    ]


def test_graph_states_carry_column_and_coding_metadata(
    graph_states_catalog: Catalog,
) -> None:
    # The graph is metadata-only, but each state keeps its delivery column,
    # value-set identity (id + version label) and classification books.
    [node] = graph_states_catalog.graph_for_fqid("scb/example/sex").nodes
    assert [s.model_dump(mode="json") for s in node.states] == [
        _annual_state(
            state_id="2947100613193185023",
            representation_run_id=0,
            delivery_column_name="Sex",
            value_set_id="8787005968282406975",
            value_set_version_label="wave-1",
            classification_slugs=["example-sex"],
            valid_from="2018-01-01",
            valid_to="2018-12-31",
        ),
        _annual_state(
            state_id="2403136623055090788",
            representation_run_id=1,
            delivery_column_name="Sexnew",
            value_set_id="8787005968282406975",
            value_set_version_label="",
            classification_slugs=[],
            valid_from="2019-01-01",
            valid_to="2019-12-31",
        ),
    ]


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
