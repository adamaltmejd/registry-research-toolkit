"""A dependency-withholding refusal no build reaches.

Kept as a unit test of the public ``resolve_metadata_dependencies`` under the
maintainer decision on unreachable load-bearing refusals (#1267): it states its
input and the outcome it must keep, why no build reaches it, and the product
change that makes it fail. The reachable claims are build cases:
``cases/build/dependency-*`` (withheld variables, variant states and
representations pruned, every missing reference refused) and
``cases/build/scope-reference-*`` (a variable reference deferred or refused in a
register-scoped build).
"""

import pytest
from _catalog_dependency_support import cause as _cause, variable as _variable
from reg_meta_build.catalog_dependencies import resolve_metadata_dependencies
from reg_meta_build.resolved_catalog import ResolvedVariant
from reg_meta_build.resolved_metadata import (
    ResolvedHistoricalPredecessor,
    ResolvedMetadata,
    ResolvedSuccession,
)


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
