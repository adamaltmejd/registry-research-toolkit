"""Complete-scope composition: lost delivery coverage is refused with its exact window.

The lost-coverage refusal arms stay as unit tests by maintainer decision (#1267).
No known build reaches them: formation mints each claim and its states from the
same segment and variant, which nothing afterwards re-slugs
(``resolve_panel_dependencies`` rewrites the variant object and keeps its slug),
so only a product defect that damages a written state can lose coverage. The
last one found wrote a folded case twin's sliced state on its raw spelling while
its claim moved to the folded one; its fixed form is
``cases/build/coverage-folded-twin-slices-are-delivered-on-the-folded-spelling``.
Each test forms a variable from a real record, damages its states the way such
a defect would, and expects the refusal. The allowed side is
``cases/build/coverage-supported-claims-are-delivered-or-explicitly-withdrawn``.
"""

from __future__ import annotations

import pytest
from _csv_fixtures import SCB_REVISION
from _source_scope_support import record, resolve
from reg_meta_build.catalog_dependencies import (
    check_delivery_coverage,
)
from reg_meta_build.resolved_catalog import (
    ResolvedAlias,
    ResolvedVariant,
)
from reg_meta_build.source_coordinates import (
    native_variable_key,
)
from reg_meta_build.source_effects import (
    record_ref,
)


def _damaged(variable, shape):
    """Damage one whole-2020 state the way an engineering defect would."""
    (state,) = variable.states
    if shape == "exact_day":
        holed = (
            state.model_copy(update={"valid_to": "2020-03-06"}),
            state.model_copy(update={"valid_from": "2020-03-08"}),
        )
        return variable.model_copy(update={"states": holed})
    kept = state.model_copy(update={"valid_to": "2020-06-30"})
    moved = state.model_copy(update={"valid_from": "2020-07-01"})
    if shape == "column":
        # The second half is written on another column; the claimed column keeps
        # only a windowless search alias, which delivers no period.
        return variable.model_copy(
            update={
                "states": (
                    kept,
                    moved.model_copy(update={"delivery_column_name": "OTHER"}),
                ),
                "aliases": (
                    ResolvedAlias(variant=state.variant, delivery_column_name="VALUE"),
                ),
            }
        )
    other_variant = ResolvedVariant(slug="people-3", name="Households")
    return variable.model_copy(
        update={"states": (kept, moved.model_copy(update={"variant": other_variant}))}
    )


@pytest.mark.parametrize(
    "shape,missing",
    [
        ("exact_day", "2020-03-07..2020-03-07"),
        ("column", "2020-07-01..2020-12-31"),
        ("variant", "2020-07-01..2020-12-31"),
    ],
)
def test_lost_delivery_coverage_is_refused_with_its_exact_window(shape, missing):
    """Input: one whole-2020 state holed for one day, or with its second half
    moved to another column (beside a windowless search alias on the claimed
    column) or to another variant. Refusal: "delivery coverage was lost" naming
    exactly the missing window, the coordinate and the source. Fails if the guard
    widens periods, accepts another column, a windowless search alias or another
    variant as delivery, or reports the hull instead of the exact gap.
    """
    item = record()
    result = resolve((item,))
    variable = result.variables[native_variable_key(item)]
    assert variable is not None
    assert [
        (o.fqid, o.variant, o.column, o.valid_from, o.valid_to, o.refs)
        for o in result.coverage
    ] == [
        (
            "scb/example/value-5",
            "people-2",
            "VALUE",
            "2020-01-01",
            "2020-12-31",
            (record_ref(item),),
        )
    ]
    check_delivery_coverage(
        (variable,), result.coverage, withheld=result.withheld_dependencies
    )
    damaged = _damaged(variable, shape)
    with pytest.raises(ValueError, match="delivery coverage was lost") as failure:
        check_delivery_coverage(
            (damaged,), result.coverage, withheld=result.withheld_dependencies
        )
    assert missing in str(failure.value)
    assert "scb/example/value-5 people-2/VALUE" in str(failure.value)
    assert f"claimed by {SCB_REVISION.dataset}/" in str(failure.value)
