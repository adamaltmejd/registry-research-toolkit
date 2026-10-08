"""Complete-scope composition: lost delivery coverage is refused and supported window facts reach the written state."""

from __future__ import annotations

import pytest
from _csv_fixtures import SCB_REVISION
from _source_scope_support import guard, record, resolve
from reg_meta_build.catalog_dependencies import (
    check_delivery_coverage,
)
from reg_meta_build.resolved_catalog import (
    ResolvedVariant,
)
from reg_meta_build.source_coordinates import (
    native_variable_key,
    native_variant_key,
)
from reg_meta_build.source_curation import (
    CurationCase,
    SearchAliasDecision,
    capture_expectations,
)
from reg_meta_build.source_effects import (
    record_ref,
)
from reg_meta_build.source_records import (
    SourceFields,
    value_field,
)


def _states(variable, shape):
    """Damage one whole-2020 state the way an engineering defect would."""
    (state,) = variable.states
    if shape == "half_year":
        return (state.model_copy(update={"valid_to": "2020-06-30"}),)
    if shape == "exact_day":
        return (
            state.model_copy(update={"valid_to": "2020-03-06"}),
            state.model_copy(update={"valid_from": "2020-03-08"}),
        )
    moved = state.model_copy(update={"valid_from": "2020-07-01"})
    return (
        state.model_copy(update={"valid_to": "2020-06-30"}),
        moved.model_copy(
            update={"delivery_column_name": "OTHER"}
            if shape == "column"
            else {"variant": ResolvedVariant(slug="people-3", name="Households")}
        ),
    )


@pytest.mark.parametrize(
    "shape,missing",
    [
        ("half_year", "2020-07-01..2020-12-31"),
        ("exact_day", "2020-03-07..2020-03-07"),
        ("column", "2020-07-01..2020-12-31"),
        ("variant", "2020-07-01..2020-12-31"),
    ],
)
def test_lost_delivery_coverage_is_refused_with_its_exact_window(shape, missing):
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
    damaged = variable.model_copy(update={"states": _states(variable, shape)})
    with pytest.raises(ValueError, match="delivery coverage was lost") as failure:
        check_delivery_coverage(
            (damaged,), result.coverage, withheld=result.withheld_dependencies
        )
    assert missing in str(failure.value)
    assert "scb/example/value-5 people-2/VALUE" in str(failure.value)
    assert f"claimed by {SCB_REVISION.dataset}/" in str(failure.value)


def test_a_search_only_alias_establishes_no_delivery_for_the_lost_window():
    item = record()
    variable_key, variant_key = native_variable_key(item), native_variant_key(item)
    assert variable_key is not None and variant_key is not None
    case = CurationCase(
        case_id="accepted-spelling",
        targets=capture_expectations((item,), fields=("column_name",)),
        peer_guards=(guard(item),),
        decision=SearchAliasDecision(
            reviewed=True,
            variable_key=variable_key,
            variant_keys=(variant_key,),
            column="VALUE",
            reason="Existing search spelling",
            provenance="Fixture decision",
        ),
    )
    result = resolve((item,), cases=(case,))
    variable = result.variables[variable_key]
    assert variable is not None
    assert [(a.delivery_column_name, a.windows) for a in variable.aliases] == [
        ("VALUE", ())
    ]
    damaged = variable.model_copy(
        update={
            "states": (
                variable.states[0].model_copy(update={"valid_to": "2020-06-30"}),
            )
        }
    )
    with pytest.raises(ValueError, match=r"2020-07-01\.\.2020-12-31"):
        check_delivery_coverage(
            (damaged,), result.coverage, withheld=result.withheld_dependencies
        )


def test_supported_window_facts_reach_the_written_state():
    item = record()
    result = resolve((item,))
    variable = result.variables[native_variable_key(item)]
    assert variable is not None
    (obligation,) = result.coverage
    assert (obligation.data_type_claim, obligation.data_length_claim) == (
        ("value", "integer"),
        ("value", "1"),
    )
    assert obligation.attributions == ()
    state = variable.states[0]
    assert (state.data_type, state.data_length) == ("integer", "1")
    check_delivery_coverage(
        (variable,), result.coverage, withheld=result.withheld_dependencies
    )
    for field, written in (("data_type", "text"), ("data_length", "0")):
        damaged = variable.model_copy(
            update={"states": (state.model_copy(update={field: written}),)}
        )
        with pytest.raises(
            ValueError,
            match="supported delivery facts changed without an explicit source outcome",
        ) as failure:
            check_delivery_coverage(
                (damaged,), result.coverage, withheld=result.withheld_dependencies
            )
        assert f"claimed {field}=" in str(failure.value)
        assert "scb/example/value-5 people-2/VALUE 2020-01-01..2020-12-31" in str(
            failure.value
        )


def test_copied_window_length_is_refused_as_a_changed_fact():
    first = record(year="2019")
    second = record(2, year="2021")
    second = second.model_copy(
        update={
            "fields": second.fields.model_copy(update={"data_length": value_field("2")})
        }
    )
    result = resolve((first, second))
    variable = result.variables[native_variable_key(first)]
    assert variable is not None
    assert [(o.valid_from, o.data_length_claim) for o in result.coverage] == [
        ("2019-01-01", ("value", "1")),
        ("2021-01-01", ("value", "2")),
    ]
    check_delivery_coverage(
        (variable,), result.coverage, withheld=result.withheld_dependencies
    )
    copied = tuple(
        s.model_copy(update={"data_length": "1"}) if s.valid_from >= "2021" else s
        for s in variable.states
    )
    damaged = variable.model_copy(update={"states": copied})
    with pytest.raises(
        ValueError, match="claimed data_length='2' written '1'"
    ) as failure:
        check_delivery_coverage(
            (damaged,), result.coverage, withheld=result.withheld_dependencies
        )
    assert "supported delivery facts changed without an explicit source outcome" in str(
        failure.value
    )


@pytest.mark.parametrize("change", ["type", "coding"])
def test_source_conflict_warning_rejects_changed_runtime_evidence(change):
    from reg_meta_build.data_warnings import scope_data_warnings
    from reg_meta_build.source_curation import SourceWarningDecision
    from reg_meta_build.source_occurrences import source_occurrence

    item = record()
    case = CurationCase(
        case_id="warning",
        targets=capture_expectations((item,), fields=tuple(SourceFields.model_fields)),
        decision=SourceWarningDecision(
            reviewed=True,
            column_key=source_occurrence(item).column_key,
            valid_from="2020-01-01",
            valid_to="2020-12-31",
            expected_codings=(),
            fields=("data_type", "coding"),
            data_warning="Conflict",
            reason="Retain both",
            provenance="Source rows",
        ),
    )
    if change == "type":
        item = item.model_copy(
            update={
                "fields": item.fields.model_copy(
                    update={"data_type": value_field("text")}
                )
            }
        )
    else:
        case = case.model_copy(
            update={
                "decision": case.decision.model_copy(
                    update={"expected_codings": ("0" * 64,)}
                )
            }
        )
    result = resolve((item,), cases=(case,))
    assert not scope_data_warnings(result)
    assert [(d.code, d.severity) for d in result.diagnostics] == [
        ("stale_curation_entry", "error")
    ]
    assert result.evaluations[0].status == "stale"
