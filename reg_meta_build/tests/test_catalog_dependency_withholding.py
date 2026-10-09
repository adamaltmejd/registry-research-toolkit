"""Dependency-withholding refusals no build reaches.

Kept as unit tests of the public ``CatalogDependencies`` and
``resolve_metadata_dependencies`` under the maintainer decision on unreachable
load-bearing refusals (#1267): each states its input and the outcome it must
keep, why no build reaches it, and the product change that makes it fail. The
reachable claims are build cases: ``cases/build/dependency-*`` (withheld
variables, variant states and representations pruned, every missing reference
refused) and ``cases/build/scope-reference-*`` (a variable reference deferred
or refused in a register-scoped build).
"""

import pytest
from _catalog_dependency_support import cause as _cause, variable as _variable
from reg_meta_build.catalog_dependencies import (
    DEFERRED_REFERENCE,
    CatalogDependencies,
    CatalogDependencyError,
    resolve_metadata_dependencies,
)
from reg_meta_build.resolved_catalog import ResolvedVariant
from reg_meta_build.resolved_metadata import (
    ResolvedHistoricalPredecessor,
    ResolvedMetadata,
    ResolvedStateLineage,
    ResolvedStateRef,
    ResolvedSuccession,
)


def test_a_scoped_build_defers_a_representation_or_state_only_when_its_variant_is_declared():
    """A non-entity reference is deferred only when every entity it names is declared.

    Input: an unselected scope declares register scb/r, its variant v and its
    variable x. Deferred: x's representations, its variant-v states and one
    exact state. Missing: x's states in an undeclared variant, and a
    classification (a shared input in no register).

    No build reaches it: a group member lies in the group's own register
    (curation_compile.py:336) and a representation succession may not cross
    registers (relations.py:471), so an entry naming these keys in an unselected
    register is skipped whole; panel keys resolve without `unselected`
    (pipeline.py:1535); state keys come only from lineage, resolved after this
    check (catalog_lineage.py:124); a classification reference to an uncompiled
    book is refused at compile (curation_compile.py:365). A deferred or missing
    variable reference is a build case (scope-reference-*).

    Fails if `_declared_entities` (catalog_dependencies.py) proves a state or
    representation by its variable alone, so the undeclared variant is deferred,
    or `CatalogDependencies.require` drops its check that a key names at least
    one declared entity, so a classification (which names none) is deferred.
    """
    dependencies = CatalogDependencies(
        set(),
        {},
        {("register", "scb/r"), ("variant", "scb/r", "v"), ("variable", "scb/r/x")},
    )
    deferred = [
        ("representation", "scb/r/x", "COL"),
        ("succession_representation", "scb/r/x", "col", "v"),
        ("variant_states", "scb/r/x", "v"),
        ("state", "scb/r/x", "v", "2020-01-01", "2020-12-31", "COL", None),
    ]
    missing = [("variant_states", "scb/r/x", "no-such"), ("classification", "no-such")]
    for key in (*deferred, *missing):
        assert not dependencies.require(key, output=repr(key))
    assert [d.code for d in dependencies.diagnostics] == [DEFERRED_REFERENCE] * len(
        deferred
    )
    assert [m.key for m in dependencies.missing] == missing


def _metadata_result(metadata, *, withheld=None):
    variant = ResolvedVariant(slug="people", name="People")
    variables = tuple(_variable(variant, slug) for slug in ("one", "two"))
    return resolve_metadata_dependencies(
        metadata,
        variables,
        registers=(variables[0].register_ref,),
        variants=((variables[0].register_ref, variant),),
        classifications=(),
        withheld=withheld or {},
    )


def test_exact_missing_state_withholds_lineage_without_removing_live_variables():
    """A lineage edge whose consumer state is withheld is pruned; a missing one is refused.

    Input: one state-lineage edge from scb/example/one's state into a consumer
    state of scb/example/two in column 'missing', which two does not deliver.
    Refused while that exact state key is unexplained; pruned, with one
    diagnostic on output state_lineage:0, once it is withheld.

    No build reaches it: the pipeline checks metadata dependencies before it
    resolves lineage (pipeline.py:1608, 1620), lineage refuses metadata that
    already holds lineage (catalog_lineage.py:124), and it builds its edges
    from formed states only, so this check never sees a lineage edge.

    Fails if `resolve_metadata_dependencies` (catalog_dependencies.py) stops
    requiring both lineage endpoints, or prunes a lineage edge without a
    withheld cause.
    """
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
    """A historical predecessor goes with its pruned succession; a withheld one is refused.

    Input: scb/example/old replaced_by scb/example/key, with old declared a
    historical predecessor. With key withheld, the succession and its
    historical declaration are both dropped. With old itself withheld, the
    declaration is refused: a historical predecessor names no selected entity.

    No build reaches it: nothing compiles a historical predecessor
    (`ResolvedMetadata.historical_predecessors` has no producer under src/).
    A succession between withheld endpoints is pruned in the build case
    dependency-withheld-variable-prunes-its-dependents step 3b-acyclic.

    Fails if `resolve_metadata_dependencies` (catalog_dependencies.py) keeps
    a historical declaration whose succession it pruned, or stops refusing a
    historical predecessor that is a withheld selected entity.
    """
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
