"""Complete-scope composition: lost delivery coverage is refused and supported window facts reach the written state."""

from __future__ import annotations

import pytest
from _source_scope_support import REVISION, guard, record, resolve
from reg_meta.source_evidence import SourceField
from reg_meta_build.catalog_dependencies import (
    check_delivery_coverage,
)
from reg_meta_build.resolved_catalog import (
    ResolvedVariant,
)
from reg_meta_build.source_coordinates import (
    native_column_key,
    native_variable_key,
    native_variant_key,
)
from reg_meta_build.source_curation import (
    CheckedFieldChange,
    CodingDecision,
    CurationCase,
    FieldExpectation,
    OccurrenceCorrectionDecision,
    SearchAliasDecision,
    capture_expectations,
)
from reg_meta_build.source_effects import (
    record_ref,
)
from reg_meta_build.source_records import (
    SourceFields,
    TemporalScope,
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
    assert f"claimed by {REVISION.dataset}/" in str(failure.value)


@pytest.mark.parametrize("shape", ["gap", "negative", "unknown", "pooled"])
def test_genuine_source_gaps_and_unresolved_scopes_claim_no_delivery(shape):
    first, second = record(year="2019"), record(2, year="2021")
    if shape == "negative":
        second = second.model_copy(
            update={
                "fields": second.fields.model_copy(
                    update={"availability": SourceField(status="negative")}
                )
            }
        )
    elif shape == "unknown":
        second = second.model_copy(
            update={
                "edition_scope": TemporalScope(kind="unknown", label="okänd"),
                "edition_period_scope": TemporalScope(kind="unknown", label="okänd"),
            }
        )
    elif shape == "pooled":
        pooled = TemporalScope(kind="pooled", label="2021-2022")
        second = second.model_copy(
            update={"edition_scope": pooled, "edition_period_scope": pooled}
        )
    result = resolve((first, second))
    variable = result.variables[native_variable_key(first)]
    assert variable is not None
    claimed = [(o.valid_from, o.valid_to) for o in result.coverage]
    assert claimed == (
        [("2019-01-01", "2019-12-31"), ("2021-01-01", "2021-12-31")]
        if shape == "gap"
        else [("2019-01-01", "2019-12-31")]
    )
    # No obligation covers 2020, the withdrawn year, or any widened period.
    check_delivery_coverage(
        (variable,), result.coverage, withheld=result.withheld_dependencies
    )


def test_a_checked_state_omission_withdraws_only_its_own_period():
    first, second = record(year="2019"), record(2, year="2020")
    column_key = native_column_key(second)
    assert column_key is not None
    case = CurationCase(
        case_id="accepted-omission",
        targets=capture_expectations((second,), fields=("column_name",)),
        peer_guards=(
            guard(second).model_copy(
                update={"expected_members": (record_ref(first), record_ref(second))}
            ),
        ),
        decision=CodingDecision(
            reviewed=True,
            column_key=column_key,
            valid_from="2020-01-01",
            valid_to="2020-12-31",
            expected_codings=(),
            selection="omit_state",
            reason="Accepted omitted delivery state",
            provenance="Fixture decision",
        ),
    )
    result = resolve((first, second), cases=(case,))
    variable = result.variables[native_variable_key(first)]
    assert variable is not None
    assert [d.code for d in result.diagnostics] == ["curated_state_omission"]
    assert [(s.valid_from, s.valid_to) for s in variable.states] == [
        ("2019-01-01", "2019-12-31")
    ]
    assert [(o.valid_from, o.valid_to) for o in result.coverage] == [
        ("2019-01-01", "2019-12-31")
    ]
    check_delivery_coverage(
        (variable,), result.coverage, withheld=result.withheld_dependencies
    )
    lost = variable.model_copy(
        update={
            "states": (
                variable.states[0].model_copy(update={"valid_to": "2019-06-30"}),
            )
        }
    )
    with pytest.raises(ValueError, match=r"2019-07-01\.\.2019-12-31"):
        check_delivery_coverage(
            (lost,), result.coverage, withheld=result.withheld_dependencies
        )


def test_an_unrelated_field_diagnostic_is_no_permission_to_discard_its_state():
    item = record()
    item = item.model_copy(
        update={"fields": item.fields.model_copy(update={"data_type": None})}
    )
    result = resolve((item,))
    variable = result.variables[native_variable_key(item)]
    assert variable is not None
    assert [d.code for d in result.diagnostics] == ["unknown_data_type"]
    assert [(o.valid_from, o.valid_to) for o in result.coverage] == [
        ("2020-01-01", "2020-12-31")
    ]
    with pytest.raises(ValueError, match=r"2020-01-01\.\.2020-12-31"):
        check_delivery_coverage(
            (), result.coverage, withheld=result.withheld_dependencies
        )


def test_an_evidenced_whole_variable_withholding_answers_for_its_own_claim():
    item = record()
    result = resolve(
        (
            item.model_copy(
                update={
                    "fields": item.fields.model_copy(
                        update={"sensitivity": SourceField(status="unknown")}
                    )
                }
            ),
        )
    )
    assert result.variables == {native_variable_key(item): None}
    assert [d.code for d in result.diagnostics] == ["unresolved_flag"]
    # The occurrence still claims 2020. The evidenced whole-variable withholding is
    # what answers for it, so the loss stays a curation blocker, not an our-bug
    # failure — and nothing but that ledger entry excuses it.
    assert [(o.valid_from, o.valid_to) for o in result.coverage] == [
        ("2020-01-01", "2020-12-31")
    ]
    assert ("variable", "scb/example/value-5") in result.withheld_dependencies
    check_delivery_coverage((), result.coverage, withheld=result.withheld_dependencies)
    with pytest.raises(ValueError, match=r"2020-01-01\.\.2020-12-31"):
        check_delivery_coverage((), result.coverage, withheld={})


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


def test_member_correction_attributions_reach_the_written_state():
    item = record()
    case = CurationCase(
        case_id="fix-opdef",
        targets=capture_expectations((item,), fields=("operational_definition",)),
        peer_guards=(guard(item),),
        decision=OccurrenceCorrectionDecision(
            reviewed=True,
            reason="Checked attribution fix",
            provenance="fixture:attr",
            effects=(
                CheckedFieldChange(
                    ref=record_ref(item),
                    replacement=FieldExpectation(
                        name="operational_definition", status="value", value="Fixed"
                    ),
                ),
            ),
        ),
    )
    result = resolve((item,), cases=(case,))
    variable = result.variables[native_variable_key(item)]
    assert variable is not None
    (obligation,) = result.coverage
    assert obligation.attributions == ("fixture:attr",)
    assert variable.states[0].provenance == "fixture:attr"
    check_delivery_coverage(
        (variable,), result.coverage, withheld=result.withheld_dependencies
    )
    stripped = variable.model_copy(
        update={"states": (variable.states[0].model_copy(update={"provenance": None}),)}
    )
    with pytest.raises(ValueError, match="claimed attributions"):
        check_delivery_coverage(
            (stripped,), result.coverage, withheld=result.withheld_dependencies
        )


def test_source_conflict_warning_preserves_type_and_exact_source_window():
    from reg_meta_build.data_warnings import DataWarning, scope_data_warnings
    from reg_meta_build.source_curation import SourceWarningDecision
    from reg_meta_build.source_occurrences import source_occurrence

    item = record()
    occurrence = source_occurrence(item)
    case = CurationCase(
        case_id="curation/registers/scb/example.toml#/coding.warning/1/period/1",
        targets=capture_expectations((item,), fields=tuple(SourceFields.model_fields)),
        decision=SourceWarningDecision(
            reviewed=True,
            column_key=occurrence.column_key,
            valid_from="2020-01-01",
            valid_to="2020-12-31",
            expected_codings=(),
            fields=("data_type", "coding"),
            data_warning="Source integer type and textual coding disagree.",
            reason="Retain both source declarations.",
            provenance="Exact source rows.",
        ),
    )
    baseline = resolve((item,))
    result = resolve((item,), cases=(case,))
    assert result.variables == baseline.variables
    (warning,) = scope_data_warnings(result)
    assert warning.code == "source_metadata_conflict"
    assert warning.fields == ("data_type", "coding")
    assert (warning.valid_from, warning.valid_to) == ("2020-01-01", "2020-12-31")
    assert warning.variant == "people-2" and warning.delivery_column_name == "VALUE"
    assert warning.refs == (capture_expectations((item,), fields=())[0].ref,)
    assert DataWarning.model_validate_json(warning.model_dump_json()) == warning


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
