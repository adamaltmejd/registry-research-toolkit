"""Complete-scope composition: partition-scoped coding and classification keys, withheld variants and parallel columns."""

from __future__ import annotations

import pytest
from _source_scope_support import guard, record, resolve
from reg_meta_build.source_coordinates import (
    native_variable_key,
    native_variant_key,
)
from reg_meta_build.source_curation import (
    AliasWindowDecision,
    CurationCase,
    capture_expectations,
)
from reg_meta_build.source_effects import (
    record_ref,
)


def test_alias_window_checks_competing_variables_in_the_whole_scope():
    first, second = record(), record(2, variable=6)
    variable_key, variant_key = native_variable_key(first), native_variant_key(first)
    assert variable_key is not None and variant_key is not None
    case = CurationCase(
        case_id="accepted-window",
        targets=capture_expectations((first,), fields=("column_name",)),
        peer_guards=(guard(first),),
        decision=AliasWindowDecision(
            reviewed=True,
            variable_key=variable_key,
            variant_key=variant_key,
            column="VALUE",
            valid_from="2020-01-01",
            valid_to="2020-12-31",
            reason="Existing bounded representation",
            provenance="Fixture decision",
        ),
    )
    result = resolve((first, second), cases=(case,))
    assert [e.status for e in result.evaluations] == ["applicable"]
    assert [d.code for d in result.diagnostics] == ["conflicting_alias_window_owner"]
    assert all(not v.aliases for v in result.variables.values())


def test_withheld_variant_keeps_supported_sibling_states():
    first, second = record(), record(2, variant=3)
    changed_parents = tuple(
        p.model_copy(update={"fields": p.fields.model_copy(update={"name": None})})
        if p.kind == "variant"
        else p
        for p in second.parent_facts
    )
    second = second.model_copy(update={"parent_facts": changed_parents})
    result = resolve((first, second))
    variable = result.variables[native_variable_key(first)]
    assert variable is not None
    assert [s.variant.slug for s in variable.states] == ["people-2"]
    problem = next(
        d for d in result.diagnostics if d.code == "withheld_variant_dependency"
    )
    assert problem.refs == (record_ref(second),) and problem.withheld_output == (
        "state",
    )


def test_missing_conversion_is_fatal():
    item = record()
    with pytest.raises(ValueError, match="missing explicit provider key"):
        resolve((item,), provider_keys={})
    with pytest.raises(ValueError, match="missing checked parent naming"):
        resolve((item,), naming=())
