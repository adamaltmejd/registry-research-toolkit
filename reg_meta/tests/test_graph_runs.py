"""Graph states and representation runs on ``Catalog.graph_for_fqid`` (#761).

A variable node carries its state history as ``GraphState`` rows; the
``representation_run_id`` groups consecutive states into rendered cells.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from reader_artifacts import build_reader_artifact
from reg_meta.catalog import Catalog
from reg_meta.db import open_db

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
