"""Native family formation: code validity, delivery coverage, units and source-native defaults."""

from __future__ import annotations

from dataclasses import replace

import pytest
from _source_formation_support import (
    FLAGS as _FLAGS,
    REGISTER as _REGISTER,
    VARIANT as _VARIANT,
    form_family as _form,
    formation_record as _record,
)
from reg_meta.source_evidence import SourceField
from reg_meta_build.catalog_dependencies import check_delivery_coverage
from reg_meta_build.source_coding import (
    CodeListClaim,
    CodeMembershipClaim,
    resolve_code_membership,
)
from reg_meta_build.source_coordinates import (
    native_column_key,
    native_variant_key,
)
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_occurrences import (
    AppliedCorrection,
    effective_occurrence,
    source_occurrence,
)
from reg_meta_build.source_records import (
    ScopeInterval,
    SourceCoordinate,
    TemporalScope,
    value_field,
)


def test_bound_code_validity_splits_ordinary_state_and_retains_version_label() -> None:
    claim = CodeListClaim(
        "list",
        TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start="2020", end="2020"),)
        ),
        (
            CodeMembershipClaim("0", "No", TemporalScope(kind="not_applicable")),
            CodeMembershipClaim(
                "1",
                "Yes",
                TemporalScope(
                    kind="intervals",
                    intervals=(ScopeInterval(start="2020-04-02", end="2020-09-01"),),
                ),
            ),
        ),
        version_label="Supplied list label",
    )
    result = _form((_record(2020),), claims=(claim,))
    assert result.variable is not None
    assert result.diagnostics == ()
    assert [
        (
            s.valid_from,
            s.valid_to,
            len(s.value_set.members) if s.value_set else 0,
            s.value_set_version_label,
        )
        for s in result.variable.states
    ] == [
        ("2020-01-01", "2020-04-01", 1, "Supplied list label"),
        ("2020-04-02", "2020-09-01", 2, "Supplied list label"),
        ("2020-09-02", "2020-12-31", 1, "Supplied list label"),
    ]


def test_coding_conflict_preserves_variable_and_period_but_withholds_membership() -> (
    None
):
    scope = TemporalScope(
        kind="intervals", intervals=(ScopeInterval(start="2020", end="2020"),)
    )
    claims = tuple(
        CodeListClaim(
            identity,
            scope,
            (CodeMembershipClaim(code, "Label", TemporalScope(kind="not_applicable")),),
        )
        for identity, code in (("a", "0"), ("b", "1"))
    )
    result = _form((_record(2020),), claims=claims)
    assert result.variable is not None
    assert result.variable.states[0].value_set is None
    assert result.diagnostics[0].code == "conflicting_code_memberships"
    assert (result.diagnostics[0].valid_from, result.diagnostics[0].valid_to) == (
        "2020-01-01",
        "2020-12-31",
    )
    assert result.coding[0].claims == claims


@pytest.mark.parametrize("missing", ["variant", "coding"])
def test_missing_implementation_mapping_is_fatal_not_curation_backlog(
    missing: str,
) -> None:
    record = _record(2020)
    variant_key, column_key = native_variant_key(record), native_column_key(record)
    assert variant_key is not None and column_key is not None
    with pytest.raises(ValueError, match="missing"):
        form_native_variable(
            (record,),
            register=_REGISTER,
            variants={} if missing == "variant" else {variant_key: _VARIANT},
            slug="value",
            provider_key="4",
            flags=_FLAGS,
            coding={}
            if missing == "coding"
            else {column_key: resolve_code_membership(())},
        )


def test_formation_coverage_carries_type_and_refuses_silent_retype() -> None:
    result = _form((_record(2020),))
    assert result.variable is not None
    (obligation,) = result.coverage
    assert (obligation.variant, obligation.column) == ("people", "VALUE")
    assert (obligation.valid_from, obligation.valid_to) == (
        "2020-01-01",
        "2020-12-31",
    )
    assert obligation.data_type_claim == ("value", "integer")
    assert obligation.attributions == ()
    check_delivery_coverage((result.variable,), result.coverage, withheld={})
    damaged = result.variable.model_copy(
        update={
            "states": (
                result.variable.states[0].model_copy(update={"data_type": "text"}),
            )
        }
    )
    with pytest.raises(
        ValueError,
        match="supported delivery facts changed without an explicit source outcome",
    ) as failure:
        check_delivery_coverage((damaged,), result.coverage, withheld={})
    assert "claimed data_type='integer' written 'text'" in str(failure.value)


def test_formation_coverage_carries_correction_attributions() -> None:
    from dataclasses import replace as _replace

    occurrence = _replace(
        source_occurrence(_record(2020)),
        corrections=(
            AppliedCorrection(
                case_id="fix-one", effect_index=0, provenance="fixture:fix-one"
            ),
        ),
    )
    result = _form((occurrence,))
    assert result.variable is not None
    (obligation,) = result.coverage
    assert obligation.attributions == ("fixture:fix-one",)
    assert result.variable.states[0].provenance == "fixture:fix-one"
    check_delivery_coverage((result.variable,), result.coverage, withheld={})
    stripped = result.variable.model_copy(
        update={
            "states": (
                result.variable.states[0].model_copy(update={"provenance": None}),
            )
        }
    )
    with pytest.raises(ValueError, match="claimed attributions"):
        check_delivery_coverage((stripped,), result.coverage, withheld={})


def test_mixed_columnless_and_unresolvable_occurrence_stays_an_error() -> None:
    base = _record(2020)
    columnless = base.model_copy(
        update={
            "fields": base.fields.model_copy(
                update={
                    "column_name": SourceField(status="negative", raw_value=""),
                }
            )
        }
    )
    periodless = _record(2021).model_copy(
        update={
            "edition_scope": TemporalScope(kind="unknown", label="okänd"),
        }
    )
    formed = _form((columnless, periodless))
    assert formed.variable is None
    (omitted,) = [
        d for d in formed.diagnostics if d.code == "omitted_columnless_occurrence"
    ]
    assert omitted.severity == "warning"
    (terminal,) = [d for d in formed.diagnostics if d.code == "no_supported_states"]
    assert terminal.severity == "error"


def test_independent_delivery_forms_without_calendar_dates_and_retains_source():
    scope = TemporalScope(kind="year_independent")
    record = _record(2020).model_copy(
        update={"edition_scope": scope, "edition_period_scope": scope}
    )
    claim = CodeListClaim(
        "native-list",
        scope,
        (CodeMembershipClaim("02", "EU25 utom Norden", scope),),
        "EU25",
    )
    formed = _form((record,), claims=(claim,))
    assert formed.diagnostics == ()
    assert formed.occurrences == (record,)
    (state,) = formed.variable.states
    assert state.period_scope == "year_independent"
    assert state.valid_from is state.valid_to is None
    assert state.pooled is False
    assert state.value_set.members == (("02", "EU25 utom Norden"),)
    (obligation,) = formed.coverage
    assert obligation.period_scope == "year_independent"
    assert obligation.valid_from is obligation.valid_to is None
    assert obligation.coding_claim == (state.value_set, "EU25")


@pytest.mark.parametrize("same_period", [False, True])
def test_delivery_units_keep_scales_and_refuse_overlapping_column_conflicts(
    same_period: bool,
) -> None:
    records = tuple(
        record.model_copy(
            update={
                "fields": record.fields.model_copy(
                    update={"measurement_unit": value_field(unit)}
                )
            }
        )
        for record, unit in (
            (_record(2020), "Kronor (SEK)"),
            (
                _record(2020 if same_period else 2021, row="second"),
                "1000-tal kronor (KSEK)",
            ),
        )
    )
    result = _form(records)
    assert result.variable is not None
    assert result.variable.measurement_unit is None
    assert any(d.code == "delivery_units_vary" for d in result.diagnostics)
    assert not any(d.code == "conflicting_variable_fact" for d in result.diagnostics)
    if same_period:
        assert any(
            d.code == "conflicting_occurrence_facts" and "measurement_unit" in d.fields
            for d in result.diagnostics
        )
        assert all(state.measurement_unit is None for state in result.variable.states)
    else:
        assert {state.measurement_unit for state in result.variable.states} == {
            "Kronor (SEK)",
            "1000-tal kronor (KSEK)",
        }


def test_columnless_source_unit_does_not_override_physical_delivery_summary() -> None:
    first = _record(2020).model_copy(
        update={
            "fields": _record(2020).fields.model_copy(
                update={"measurement_unit": value_field("SEK")}
            )
        }
    )
    second = _record(2021, column="").model_copy(
        update={
            "fields": _record(2021).fields.model_copy(
                update={
                    "column_name": SourceField(status="negative", raw_value=""),
                    "measurement_unit": value_field("KSEK"),
                }
            )
        }
    )
    result = _form((first, second))
    assert result.variable is not None
    assert result.variable.measurement_unit == "SEK"
    assert not any(d.code == "delivery_units_vary" for d in result.diagnostics)


def test_missing_physical_unit_withholds_common_summary_without_filling_delivery() -> (
    None
):
    first = _record(2020).model_copy(
        update={
            "fields": _record(2020).fields.model_copy(
                update={"measurement_unit": value_field("SEK")}
            )
        }
    )
    result = _form((first, _record(2021)))
    assert result.variable is not None
    assert result.variable.measurement_unit is None
    assert {
        state.valid_from: state.measurement_unit for state in result.variable.states
    } == {"2020-01-01": "SEK", "2021-01-01": None}
    assert any(d.code == "delivery_units_vary" for d in result.diagnostics)
    assert not any(d.severity == "error" for d in result.diagnostics)


@pytest.mark.parametrize(
    "missing", ["name", "definition", "column_name", "data_type", "period", "variant"]
)
def test_source_native_default_refuses_incomplete_complete_family(missing: str) -> None:
    first = _record(2020, column="OLD")
    second = _record(2021, column="NEW")
    if missing in {"name", "definition", "column_name", "data_type"}:
        second = second.model_copy(
            update={
                "fields": second.fields.model_copy(
                    update={missing: SourceField(status="unknown")}
                )
            }
        )
    elif missing == "period":
        second = second.model_copy(
            update={
                "edition_scope": TemporalScope(kind="unknown", label="Unknown"),
                "edition_period_scope": TemporalScope(kind="unknown", label="Unknown"),
            }
        )
    else:
        second = second.model_copy(
            update={
                "subject": second.subject.model_copy(
                    update={"variant": SourceCoordinate(status="unknown")}
                )
            }
        )
    third = _record(2022, column="THIRD")
    result = _form((first, second, third))
    assert result.variable is None
    assert any(d.code == "unresolved_native_identity" for d in result.diagnostics)
    assert result.occurrences == (first, second, third)


@pytest.mark.parametrize("field", ["name", "definition"])
def test_source_native_default_refuses_contrary_quantity_metadata(field: str) -> None:
    first = _record(2020, column="OLD")
    second = _record(2021, column="NEW")
    second = second.model_copy(
        update={
            "fields": second.fields.model_copy(
                update={field: value_field("Contrary quantity")}
            )
        }
    )
    result = _form((first, second))
    assert result.variable is None
    assert any(d.code == "unresolved_native_identity" for d in result.diagnostics)


def test_source_native_default_keeps_operations_types_and_units_at_delivery_grain() -> (
    None
):
    first = _record(2020, column="OLD", operational_definition="Survey formula")
    second = _record(2021, column="NEW", operational_definition="Register formula")
    first = first.model_copy(
        update={
            "fields": first.fields.model_copy(
                update={
                    "measurement_unit": value_field("SEK"),
                    "data_type": value_field("integer"),
                }
            )
        }
    )
    second = second.model_copy(
        update={
            "fields": second.fields.model_copy(
                update={
                    "measurement_unit": value_field("KSEK"),
                    "data_type": value_field("decimal"),
                }
            )
        }
    )
    result = _form((first, second))
    assert result.variable is not None
    assert result.variable.measurement_unit is None
    assert result.variable.operational_definition is None
    assert {
        (
            s.delivery_column_name,
            s.data_type,
            s.measurement_unit,
            s.operational_definition,
        )
        for s in result.variable.states
    } == {
        ("OLD", "integer", "SEK", "Survey formula"),
        ("NEW", "decimal", "KSEK", "Register formula"),
    }
    assert not any(d.severity == "error" for d in result.diagnostics)


def test_source_native_default_never_treats_a_checked_owner_as_automatic() -> None:
    checked = replace(
        effective_occurrence(_record(2020, column="OLD")), identity_checked=True
    )
    result = _form((checked, _record(2021, column="NEW")))
    assert result.variable is not None
    assert not any(
        d.code == "source_native_identity_retained" for d in result.diagnostics
    )
    assert {s.delivery_column_name for s in result.variable.states} == {"OLD", "NEW"}
