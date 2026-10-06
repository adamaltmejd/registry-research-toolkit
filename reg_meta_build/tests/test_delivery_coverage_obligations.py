"""Delivery coverage obligations refuse unexplained changes to claimed delivery facts and coverage."""

import pytest
from _catalog_dependency_support import cause as _cause, variable as _variable
from reg_meta_build.catalog_dependencies import (
    CoverageObligation,
    check_delivery_coverage,
)
from reg_meta_build.resolved_catalog import (
    ResolvedAlias,
    ResolvedAliasWindow,
    ResolvedClassificationLink,
    ResolvedCodeSet,
    ResolvedVariant,
)
from reg_meta_build.source_curation import SourceRecordRef


def _fact_obligation(
    *,
    fqid: str = "scb/example/value",
    variant: str = "people",
    column: str = "VALUE",
    valid_from: str = "2020-01-01",
    valid_to: str = "2020-12-31",
    refs: tuple[SourceRecordRef, ...] = (
        SourceRecordRef(source="fixture", semantic_record_key=("key",)),
    ),
    data_type_claim: tuple[str, str | None] | None = ("value", "integer"),
    data_length_claim: tuple[str, str | None] | None = ("value", "1"),
    attributions: tuple[str, ...] = ("correction:one",),
):
    return CoverageObligation(
        fqid=fqid,
        variant=variant,
        column=column,
        valid_from=valid_from,
        valid_to=valid_to,
        refs=refs,
        data_type_claim=data_type_claim,
        data_length_claim=data_length_claim,
        attributions=attributions,
    )


def _fact_variable(
    *, data_type="integer", data_length="1", provenance="correction:one"
):
    variant = ResolvedVariant(slug="people", name="People")
    variable = _variable(variant, "value")
    state = variable.states[0].model_copy(
        update={
            "delivery_column_name": "VALUE",
            "valid_from": "2020-01-01",
            "valid_to": "2020-12-31",
            "data_type": data_type,
            "data_length": data_length,
            "provenance": provenance,
        }
    )
    return variable.model_copy(update={"states": (state,), "aliases": ()})


def test_delivery_facts_retype_is_refused_with_claimed_and_written():
    obligation = _fact_obligation(attributions=())
    check_delivery_coverage((_fact_variable(),), (obligation,), withheld={})
    damaged = _fact_variable(data_type="text")
    with pytest.raises(
        ValueError,
        match="supported delivery facts changed without an explicit source outcome",
    ) as failure:
        check_delivery_coverage((damaged,), (obligation,), withheld={})
    message = str(failure.value)
    assert "scb/example/value people/VALUE 2020-01-01..2020-12-31" in message
    assert "claimed data_type='integer' written 'text'" in message
    assert "fixture/key" in message


def test_delivery_facts_length_mismatch_is_refused():
    obligation = _fact_obligation(attributions=())
    with pytest.raises(
        ValueError, match="claimed data_length='1' written '0'"
    ) as failure:
        check_delivery_coverage(
            (_fact_variable(data_length="0"),), (obligation,), withheld={}
        )
    assert "supported delivery facts changed without an explicit source outcome" in str(
        failure.value
    )


def test_delivery_facts_attributions_require_exact_provenance_elements():
    obligation = _fact_obligation()
    check_delivery_coverage(
        (_fact_variable(provenance="correction:one\n\ncomment"),),
        (obligation,),
        withheld={},
    )
    # Substring overlap is not enough; the exact correction element must remain.
    with pytest.raises(ValueError, match="claimed attributions") as failure:
        check_delivery_coverage(
            (_fact_variable(provenance="correction:one-extended"),),
            (obligation,),
            withheld={},
        )
    assert "supported delivery facts changed without an explicit source outcome" in str(
        failure.value
    )
    with pytest.raises(ValueError, match="claimed attributions"):
        check_delivery_coverage(
            (_fact_variable(provenance=None),), (obligation,), withheld={}
        )


def test_delivery_facts_shared_state_behind_alias_window_is_checked():
    variant = ResolvedVariant(slug="people", name="People")
    shared = (
        _fact_variable().states[0].model_copy(update={"delivery_column_name": "First"})
    )
    variable = _fact_variable().model_copy(
        update={
            "states": (shared,),
            "aliases": (
                ResolvedAlias(
                    variant=variant,
                    delivery_column_name="Second",
                    windows=(
                        ResolvedAliasWindow(
                            valid_from="2020-07-01",
                            valid_to="2020-12-31",
                        ),
                    ),
                ),
            ),
        }
    )
    obligation = _fact_obligation(column="Second", valid_from="2020-07-01")
    check_delivery_coverage((variable,), (obligation,), withheld={})
    damaged = variable.model_copy(
        update={
            "states": (shared.model_copy(update={"data_type": "text"}),),
        }
    )
    with pytest.raises(
        ValueError,
        match="supported delivery facts changed without an explicit source outcome",
    ) as failure:
        check_delivery_coverage((damaged,), (obligation,), withheld={})
    assert "scb/example/value people/Second 2020-07-01..2020-12-31" in str(
        failure.value
    )


def test_delivery_facts_unknown_claim_is_never_compared():
    unknown = _fact_obligation(
        data_type_claim=None, data_length_claim=None, attributions=()
    )
    # An unknown source fact is no claim: any written facts pass on a direct
    # state...
    check_delivery_coverage(
        (_fact_variable(data_type="integer", data_length="0", provenance=None),),
        (unknown,),
        withheld={},
    )
    variant = ResolvedVariant(slug="people", name="People")
    shared = (
        _fact_variable(data_type="text", data_length="9", provenance=None)
        .states[0]
        .model_copy(update={"delivery_column_name": "First"})
    )
    aliased = _fact_variable(
        data_type=None, data_length=None, provenance=None
    ).model_copy(
        update={
            "states": (shared,),
            "aliases": (
                ResolvedAlias(
                    variant=variant,
                    delivery_column_name="VALUE",
                    windows=(
                        ResolvedAliasWindow(
                            valid_from="2020-01-01",
                            valid_to="2020-12-31",
                        ),
                    ),
                ),
            ),
        }
    )
    check_delivery_coverage((aliased,), (unknown,), withheld={})
    check_delivery_coverage(
        (_fact_variable(data_type="text"),),
        (_fact_obligation(attributions=()),),
        withheld={("variable", "scb/example/value"): (_cause(),)},
    )
    # ... while a present claim on another window is still enforced.
    present = _fact_obligation(
        data_type_claim=("value", "integer"),
        data_length_claim=("value", "0"),
        attributions=(),
    )
    with pytest.raises(ValueError, match="claimed data_length='0' written '1'"):
        check_delivery_coverage(
            (_fact_variable(data_length="1", provenance=None),),
            (present,),
            withheld={},
        )


def test_delivery_facts_negative_claim_requires_absent_written_facts():
    negative = _fact_obligation(
        data_type_claim=("negative", None),
        data_length_claim=("negative", None),
        attributions=(),
    )
    check_delivery_coverage(
        (_fact_variable(data_type=None, data_length=None, provenance=None),),
        (negative,),
        withheld={},
    )
    with pytest.raises(
        ValueError,
        match="supported delivery facts changed without an explicit source outcome",
    ) as failure:
        check_delivery_coverage(
            (_fact_variable(data_type="integer", data_length="0", provenance=None),),
            (negative,),
            withheld={},
        )
    assert "negative source claim" in str(failure.value)


def test_delivery_facts_backfilled_absent_length_is_refused():
    base = _fact_variable(data_type="integer", data_length="0", provenance=None)
    first = base.states[0]
    leaked = first.model_copy(
        update={"valid_from": "2021-01-01", "valid_to": "2021-12-31"}
    )
    variable = base.model_copy(update={"states": (first, leaked)})
    obligations = (
        _fact_obligation(
            valid_from="2020-01-01",
            valid_to="2020-12-31",
            data_type_claim=("value", "integer"),
            data_length_claim=("value", "0"),
            attributions=(),
        ),
        _fact_obligation(
            valid_from="2021-01-01",
            valid_to="2021-12-31",
            data_type_claim=("negative", None),
            data_length_claim=("negative", None),
            attributions=(),
        ),
    )
    honest = variable.model_copy(
        update={
            "states": (
                first,
                leaked.model_copy(update={"data_type": None, "data_length": None}),
            )
        }
    )
    check_delivery_coverage((honest,), obligations, withheld={})
    with pytest.raises(
        ValueError,
        match="supported delivery facts changed without an explicit source outcome",
    ) as failure:
        check_delivery_coverage((variable,), obligations, withheld={})
    assert "people/VALUE 2021-01-01..2021-12-31" in str(failure.value)
    assert "negative source claim" in str(failure.value)


def test_deleted_or_truncated_shared_state_behind_alias_window_is_refused():
    variant = ResolvedVariant(slug="people", name="People")
    shared = (
        _fact_variable().states[0].model_copy(update={"delivery_column_name": "First"})
    )
    variable = _fact_variable().model_copy(
        update={
            "states": (shared,),
            "aliases": (
                ResolvedAlias(
                    variant=variant,
                    delivery_column_name="Second",
                    windows=(
                        ResolvedAliasWindow(
                            valid_from="2020-07-01",
                            valid_to="2020-12-31",
                        ),
                    ),
                ),
            ),
        }
    )
    obligations = (
        _fact_obligation(
            column="First",
            valid_from="2020-01-01",
            valid_to="2020-12-31",
            attributions=(),
        ),
        _fact_obligation(column="Second", valid_from="2020-07-01", attributions=()),
    )
    check_delivery_coverage((variable,), obligations, withheld={})
    deleted = variable.model_copy(update={"states": ()})
    with pytest.raises(
        ValueError,
        match="supported delivery facts changed without an explicit source outcome",
    ) as failure:
        check_delivery_coverage((deleted,), obligations, withheld={})
    assert (
        "no written state carries the claimed facts for 2020-07-01..2020-12-31"
        in str(failure.value)
    )
    truncated = variable.model_copy(
        update={"states": (shared.model_copy(update={"valid_to": "2020-09-30"}),)}
    )
    with pytest.raises(
        ValueError,
        match="supported delivery facts changed without an explicit source outcome",
    ) as failure:
        check_delivery_coverage((truncated,), obligations, withheld={})
    assert (
        "no written state carries the claimed facts for 2020-10-01..2020-12-31"
        in str(failure.value)
    )


def test_overlapping_backing_states_behind_alias_window_are_refused():
    variant = ResolvedVariant(slug="people", name="People")
    first = (
        _fact_variable().states[0].model_copy(update={"delivery_column_name": "First"})
    )
    second = (
        _fact_variable().states[0].model_copy(update={"delivery_column_name": "Extra"})
    )
    variable = _fact_variable().model_copy(
        update={
            "states": (first, second),
            "aliases": (
                ResolvedAlias(
                    variant=variant,
                    delivery_column_name="Second",
                    windows=(
                        ResolvedAliasWindow(
                            valid_from="2020-07-01",
                            valid_to="2020-12-31",
                        ),
                    ),
                ),
            ),
        }
    )
    obligation = _fact_obligation(
        column="Second", valid_from="2020-07-01", attributions=()
    )
    check_delivery_coverage(
        (variable.model_copy(update={"states": (first,)}),),
        (obligation,),
        withheld={},
    )
    with pytest.raises(
        ValueError,
        match="supported delivery facts changed without an explicit source outcome",
    ) as failure:
        check_delivery_coverage((variable,), (obligation,), withheld={})
    assert (
        "alias backing is ambiguous: 2 states of variant people "
        "overlap 2020-07-01..2020-12-31" in str(failure.value)
    )


def test_delivery_fact_change_in_diagnostic_mode_returns_an_error_diagnostic():
    obligation = _fact_obligation(attributions=())
    assert (
        check_delivery_coverage(
            (_fact_variable(),), (obligation,), withheld={}, diagnostic=True
        )
        == ()
    )
    damaged = _fact_variable(data_type="text")
    with pytest.raises(
        ValueError,
        match="supported delivery facts changed without an explicit source outcome",
    ) as failure:
        check_delivery_coverage((damaged,), (obligation,), withheld={})
    assert "claimed data_type='integer' written 'text'" in str(failure.value)
    (found,) = check_delivery_coverage(
        (damaged,), (obligation,), withheld={}, diagnostic=True
    )
    assert found.code == "unexplained_delivery_fact_change"
    assert found.severity == "error"
    assert found.refs == obligation.refs
    assert (
        "supported delivery facts changed without an explicit source outcome"
        in found.detail
    )
    assert "claimed data_type='integer' written 'text'" in found.detail
    assert "scb/example/value people/VALUE 2020-01-01..2020-12-31" in found.detail
    assert "fixture/key" in found.detail
    assert str(failure.value) == found.detail


def test_delivery_coverage_loss_in_diagnostic_mode_returns_an_error_diagnostic():
    obligation = _fact_obligation(attributions=())
    state = _fact_variable().states[0].model_copy(update={"valid_to": "2020-06-30"})
    truncated = _fact_variable().model_copy(update={"states": (state,)})
    with pytest.raises(
        ValueError,
        match="supported delivery coverage was lost without an explicit source outcome",
    ) as failure:
        check_delivery_coverage((truncated,), (obligation,), withheld={})
    assert "2020-07-01..2020-12-31" in str(failure.value)
    (found,) = check_delivery_coverage(
        (truncated,), (obligation,), withheld={}, diagnostic=True
    )
    assert found.code == "unexplained_delivery_coverage_loss"
    assert found.severity == "error"
    assert found.refs == obligation.refs
    assert (
        "supported delivery coverage was lost without an explicit source outcome"
        in found.detail
    )
    assert "2020-07-01..2020-12-31" in found.detail
    assert "fixture/key" in found.detail
    assert str(failure.value) == found.detail
    # A claim-free obligation still owes its window: the fact-claim gate must
    # not drop the loss on either path.
    bare = CoverageObligation(
        fqid=obligation.fqid,
        variant=obligation.variant,
        column=obligation.column,
        valid_from=obligation.valid_from,
        valid_to=obligation.valid_to,
        refs=obligation.refs,
    )
    with pytest.raises(
        ValueError,
        match="supported delivery coverage was lost without an explicit source outcome",
    ):
        check_delivery_coverage((truncated,), (bare,), withheld={})
    (bare_found,) = check_delivery_coverage(
        (truncated,), (bare,), withheld={}, diagnostic=True
    )
    assert bare_found.code == "unexplained_delivery_coverage_loss"
    assert "2020-07-01..2020-12-31" in bare_found.detail
    assert (
        check_delivery_coverage(
            (truncated,),
            (obligation,),
            withheld={("variable", "scb/example/value"): (_cause(),)},
            diagnostic=True,
        )
        == ()
    )


def test_year_independent_delivery_proof_compares_exact_column_facts_without_calendar_coverage():
    variant = ResolvedVariant(slug="birth", name="Birth")
    dated = _variable(variant)
    independent = dated.model_copy(
        update={
            "states": (
                dated.states[0].model_copy(
                    update={
                        "period_scope": "year_independent",
                        "valid_from": None,
                        "valid_to": None,
                    }
                ),
            )
        }
    )
    claim = CoverageObligation(
        "scb/example/value",
        "birth",
        "value",
        None,
        None,
        (),
        period_scope="year_independent",
        data_type_claim=("negative", None),
        attributions=("fixture",),
    )
    assert check_delivery_coverage((independent,), (claim,), withheld={}) == ()
    for candidate in (
        dated,
        independent.model_copy(
            update={
                "states": (
                    independent.states[0].model_copy(
                        update={"delivery_column_name": "other"}
                    ),
                )
            }
        ),
    ):
        with pytest.raises(ValueError, match="coverage was lost"):
            check_delivery_coverage((candidate,), (claim,), withheld={})
    dated_claim = CoverageObligation(
        "scb/example/value", "birth", "value", "2000-01-01", "2000-12-31", ()
    )
    with pytest.raises(ValueError, match="coverage was lost"):
        check_delivery_coverage((independent,), (dated_claim,), withheld={})
    wrong_fact = independent.model_copy(
        update={
            "states": (
                independent.states[0].model_copy(update={"data_type": "integer"}),
            )
        }
    )
    with pytest.raises(ValueError, match="facts changed"):
        check_delivery_coverage((wrong_fact,), (claim,), withheld={})
    from reg_meta_build.catalog_dependencies import variable_dependency_keys

    keys = variable_dependency_keys(independent)
    assert ("independent_state", "scb/example/value", "birth", "value", "") in keys
    assert not any(key[0] == "state" for key in keys)


@pytest.mark.parametrize(
    "damage", [None, "missing", "domain", "version", "common", "classified"]
)
def test_per_column_alias_coding_preserves_exact_delivery_claims(damage):
    from dataclasses import replace

    variant = ResolvedVariant(slug="people", name="People")
    first = ResolvedCodeSet(members=(("0", "No"), ("1", "Yes")))
    second = ResolvedCodeSet(members=(("1", "Yes"), ("2", "Unknown")))
    state = (
        _fact_variable()
        .states[0]
        .model_copy(
            update={
                "delivery_column_name": "First",
                "value_set": None,
                "value_set_version_label": "",
            }
        )
    )
    windows = tuple(
        ResolvedAliasWindow(
            valid_from="2020-01-01",
            valid_to="2020-12-31",
            coding_metadata="per_column",
            value_set=domain,
            value_set_version_label=version,
        )
        for domain, version in ((first, "First question"), (second, "Second question"))
    )
    if damage in {"missing", "domain", "version"}:
        windows = (
            windows[0],
            windows[1].model_copy(
                update={"value_set": None}
                if damage == "missing"
                else {"value_set": first}
                if damage == "domain"
                else {"value_set_version_label": "Changed"}
            ),
        )
    elif damage == "common":
        state = state.model_copy(
            update={"value_set": first, "value_set_version_label": "First question"}
        )
    elif damage == "classified":
        state = state.model_copy(
            update={
                "classification_links": (
                    ResolvedClassificationLink(classification="example"),
                )
            }
        )
    variable = _fact_variable().model_copy(
        update={
            "states": (state,),
            "aliases": tuple(
                ResolvedAlias(variant=variant, delivery_column_name=column).model_copy(
                    update={"windows": (window,)}
                )
                for column, window in zip(("First", "Second"), windows, strict=True)
            ),
        }
    )
    obligations = tuple(
        replace(
            _fact_obligation(column=column, attributions=()),
            coding_claim=(domain, version),
        )
        for column, domain, version in (
            ("First", first, "First question"),
            ("Second", second, "Second question"),
        )
    )
    if damage is None:
        check_delivery_coverage((variable,), obligations, withheld={})
    else:
        with pytest.raises(ValueError, match="supported delivery facts changed"):
            check_delivery_coverage((variable,), obligations, withheld={})
