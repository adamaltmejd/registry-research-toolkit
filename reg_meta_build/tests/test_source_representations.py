"""A dated parallel-column decision never forms a window over a year-independent table.

The reachable parallel-representation behavior is the build cases
`representation-parallel-*` (for example
`representation-parallel-shared-states-and-per-column-windows-resolve-per-register`) and
the `representation-parallel-*` loader cases. The one claim left here needs a
year-independent state, which only the LISA reader forms (`sources/lisa.py`); the
build-case runner materializes SCB and SOS deliveries only, so no case reaches it
(the same reason `test_delivery_coverage_obligations.py` keeps its year-independent
claim as a slim test).
"""

from __future__ import annotations

from _csv_fixtures import scb_record
from reg_meta_build.resolved_catalog import ResolvedState, ResolvedVariant
from reg_meta_build.source_curation import (
    ColumnRepresentation,
    CurationCase,
    RepresentationDecision,
    capture_expectations,
)
from reg_meta_build.source_representations import form_representations

KEY = ("accepted", "fixture", "income")
VARIANT_KEY = ("accepted", "fixture", "people")
PEOPLE = ResolvedVariant(slug="people", name="People")


def test_dated_representation_does_not_manufacture_an_independent_window():
    """Input: First and Second each formed as a year-independent state, and an
    applicable 2020 decision naming both. Expected: the states pass through
    unchanged, no alias window and no withheld window is formed, and the decision is
    reported as `unsupported_representation_scope`. Fails if `form_representations`
    gives the independent states the decision's calendar window or windows (a
    year-independent table would gain 2020 availability), or drops them.
    """
    records = tuple(
        scb_record(index, colname=column, cvid=99 + index, var_id=index, year="2020")
        for index, column in enumerate(("First", "Second"), 1)
    )
    case = CurationCase(
        case_id="representations",
        targets=capture_expectations(records, fields=("column_name",)),
        decision=RepresentationDecision(
            reviewed=True,
            variable_key=KEY,
            variant_key=VARIANT_KEY,
            valid_from="2020-01-01",
            valid_to="2020-12-31",
            columns=tuple(
                ColumnRepresentation(
                    column=column, valid_from="2020-01-01", valid_to="2020-12-31"
                )
                for column in ("First", "Second")
            ),
            reason="Existing accepted parallel columns",
            provenance="fixture",
        ),
    )
    independent = [
        ResolvedState(
            variant=PEOPLE,
            period_scope="year_independent",
            delivery_column_name=column,
            data_type="integer",
            data_length="1",
            operational_definition=None,
            provenance=None,
        )
        for column in ("First", "Second")
    ]
    states, aliases, issues, withheld, _ = form_representations(
        independent,
        (case,),
        variable_key=KEY,
        variants={VARIANT_KEY: PEOPLE},
        subject="fixture",
    )
    assert states == independent
    assert not aliases and not withheld
    assert [issue.code for issue in issues] == ["unsupported_representation_scope"]
