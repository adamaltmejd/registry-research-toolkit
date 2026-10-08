"""Known withheld facts prune panel, tag, group and relation dependencies; unexplained missing refs stay fatal."""

import pytest
from _catalog_dependency_support import cause as _cause, variable as _variable
from reg_meta_build.catalog_dependencies import (
    DEFERRED_REFERENCE,
    CatalogDependencies,
    CatalogDependencyError,
    resolve_metadata_dependencies,
)
from reg_meta_build.resolved_catalog import (
    ResolvedVariant,
)
from reg_meta_build.resolved_metadata import (
    ResolvedGroupVariable,
    ResolvedHistoricalPredecessor,
    ResolvedMetadata,
    ResolvedRepresentationRef,
    ResolvedRepresentationSuccession,
    ResolvedStateLineage,
    ResolvedStateRef,
    ResolvedSuccession,
    ResolvedVariableGroup,
)


def test_a_scoped_build_defers_only_what_unselected_scopes_declare():
    dependencies = CatalogDependencies(
        set(),
        {},
        {
            ("register", "scb/r"),
            ("variant", "scb/r", "v"),
            ("variable", "scb/r/x"),
        },
    )
    deferred = [
        ("variable", "scb/r/x"),
        ("representation", "scb/r/x", "COL"),
        ("succession_representation", "scb/r/x", "col", "v"),
        ("variant_states", "scb/r/x", "v"),
        ("state", "scb/r/x", "v", "2020-01-01", "2020-12-31", "COL", None),
    ]
    # An existing unselected register vouches for no undeclared variable or
    # variant, and shared inputs lie in no register.
    missing = [
        ("variable", "scb/r/no-such"),
        ("variant_states", "scb/r/x", "no-such"),
        ("representation", "scb/other/x", "COL"),
        ("classification", "no-such"),
    ]
    for key in (*deferred, *missing):
        assert not dependencies.require(key, output=repr(key))
    assert [d.code for d in dependencies.diagnostics] == [DEFERRED_REFERENCE] * len(
        deferred
    )
    assert [m.key for m in dependencies.missing] == missing


def _metadata_result(metadata, *, withheld=None, variables=None):
    variant = ResolvedVariant(slug="people", name="People")
    variables = variables or tuple(_variable(variant, slug) for slug in ("one", "two"))
    return resolve_metadata_dependencies(
        metadata,
        variables,
        registers=(variables[0].register_ref,),
        variants=((variables[0].register_ref, variant),),
        classifications=(),
        withheld=withheld or {},
    )


def test_representation_withholding_is_literal_but_succession_preserves_its_contract():
    metadata = ResolvedMetadata(
        representation_successions=(
            ResolvedRepresentationSuccession(
                predecessor=ResolvedRepresentationRef(
                    variable="scb/example/one", delivery_column_name="ONE"
                ),
                successor=ResolvedRepresentationRef(
                    variable="scb/example/two", delivery_column_name="TWO"
                ),
            ),
        )
    )
    assert _metadata_result(metadata).metadata == metadata
    group = ResolvedVariableGroup(
        register="scb/example",
        key="family",
        label="Family",
        source="curated",
        members=(
            ResolvedGroupVariable(
                variable="scb/example/one", delivery_column_name="ONE"
            ),
            ResolvedGroupVariable(
                variable="scb/example/two", delivery_column_name="two"
            ),
        ),
    )
    with pytest.raises(
        CatalogDependencyError, match="unexplained missing catalog dependenc"
    ):
        _metadata_result(ResolvedMetadata(variable_groups=(group,)))
    result = _metadata_result(
        ResolvedMetadata(variable_groups=(group,)),
        withheld={("representation", "scb/example/one", "ONE"): (_cause(),)},
    )
    assert result.metadata.variable_groups == ()


def test_exact_missing_state_withholds_lineage_without_removing_live_variables():
    common = {"variant": "people", "valid_from": "2000-01-01", "valid_to": "2000-12-31"}
    source = ResolvedStateRef(
        variable="scb/example/one", delivery_column_name="one", **common
    )
    consumer = ResolvedStateRef(
        variable="scb/example/two", delivery_column_name="missing", **common
    )
    metadata = ResolvedMetadata(
        state_lineage=(
            ResolvedStateLineage(
                consumer=consumer,
                source=source,
                valid_from=common["valid_from"],
                valid_to=common["valid_to"],
            ),
        )
    )
    with pytest.raises(
        CatalogDependencyError, match="unexplained missing catalog dependenc"
    ):
        _metadata_result(metadata)
    result = _metadata_result(
        metadata,
        withheld={
            (
                "state",
                "scb/example/two",
                "people",
                "2000-01-01",
                "2000-12-31",
                "missing",
                "",
            ): (_cause(),)
        },
    )
    assert result.metadata.state_lineage == ()
    assert result.diagnostics[0].withheld_output == ("state_lineage:0",)


def test_explicit_historical_predecessor_is_not_confused_with_withheld_entity():
    metadata = ResolvedMetadata(
        successions=(
            ResolvedSuccession(
                predecessor="scb/example/old", successor="scb/example/key"
            ),
        ),
        historical_predecessors=(
            ResolvedHistoricalPredecessor(
                target="scb/example/old",
                kind="variable",
                reason="Retired",
                decision_reference="accepted:7",
            ),
        ),
    )
    result = _metadata_result(
        metadata, withheld={("variable", "scb/example/key"): (_cause(),)}
    )
    assert result.metadata.successions == result.metadata.historical_predecessors == ()
    with pytest.raises(
        ValueError, match="historical predecessor is a withheld selected entity"
    ):
        _metadata_result(
            metadata, withheld={("variable", "scb/example/old"): (_cause(),)}
        )
