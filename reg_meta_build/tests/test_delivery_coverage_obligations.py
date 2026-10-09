"""Delivery coverage obligations refuse unexplained changes to claimed delivery facts and coverage.

``check_delivery_coverage`` is the build's last guard before the catalog is placed.
Formation mints each claim from the segment it writes as a state
(``source_formation.form_native_variable``), but later stages narrow, copy and
rename those states (representation windows, disjoint slicing, the case-twin
spelling fold), so the guard catches a defect in any of them. No build reaches
its refusals: with every arm instrumented, the pipeline builds of the
non-integration suites mint 1,920 obligations and none of them fires. A build did reach the
loss arm once, through the fold writing a folded twin's copied slice on its raw
spelling; the fixed form is
``cases/build/coverage-folded-twin-slices-are-delivered-on-the-folded-spelling``.

So each refusal stays here as a slim unit test by maintainer decision (#1267):
the input is a written catalog damaged the way such a defect would damage it,
beside its undamaged twin, and the expected value is the located refusal, the
same in diagnostic and strict mode. Every hand-built obligation has the minted
shape: formation always claims the measurement unit. The allowed side is pinned
by ``cases/build/coverage-*``, and the withheld-variable skip by
``cases/build/dependency-withheld-variable-prunes-its-dependents``.
"""

from dataclasses import replace

import pytest
from _catalog_dependency_support import variable as _variable
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
from reg_meta_build.source_curation import ResolutionDiagnostic, SourceRecordRef

PEOPLE = ResolvedVariant(slug="people", name="People")
FACT = "unexplained_delivery_fact_change"
LOSS = "unexplained_delivery_coverage_loss"


def _fact_obligation(
    *,
    column: str = "VALUE",
    valid_from: str = "2020-01-01",
    valid_to: str = "2020-12-31",
    data_type_claim: tuple[str, str | None] | None = ("value", "integer"),
    data_length_claim: tuple[str, str | None] | None = ("value", "1"),
    attributions: tuple[str, ...] = (),
    measurement_unit_claim: tuple[str, str | None] = ("negative", None),
):
    return CoverageObligation(
        fqid="scb/example/value",
        variant="people",
        column=column,
        valid_from=valid_from,
        valid_to=valid_to,
        refs=(SourceRecordRef(source="fixture", semantic_record_key=("key",)),),
        data_type_claim=data_type_claim,
        data_length_claim=data_length_claim,
        attributions=attributions,
        measurement_unit_claim=measurement_unit_claim,
    )


def _fact_variable(*, column="VALUE", provenance=None, aliases=(), **facts):
    variable = _variable(PEOPLE, "value")
    state = variable.states[0].model_copy(
        update={
            "delivery_column_name": column,
            "valid_from": "2020-01-01",
            "valid_to": "2020-12-31",
            "data_type": "integer",
            "data_length": "1",
            "provenance": provenance,
            **facts,
        }
    )
    return variable.model_copy(update={"states": (state,), "aliases": aliases})


def _second_window(**metadata):
    """Second, delivered in 2020-07..12 by a representation window."""
    return ResolvedAlias(
        variant=PEOPLE,
        delivery_column_name="Second",
        windows=(
            ResolvedAliasWindow(
                valid_from="2020-07-01", valid_to="2020-12-31", **metadata
            ),
        ),
    )


def _with_states(variable, *states):
    return variable.model_copy(update={"states": states})


def _allowed(variables, obligations):
    assert check_delivery_coverage(variables, obligations, withheld={}) == ()
    assert (
        check_delivery_coverage(variables, obligations, withheld={}, diagnostic=True)
        == ()
    )


def _refusal(variables, obligations, code) -> ResolutionDiagnostic:
    """The one located error, whose detail is also the strict-mode refusal."""
    (found,) = check_delivery_coverage(
        variables, obligations, withheld={}, diagnostic=True
    )
    assert (found.code, found.severity) == (code, "error")
    with pytest.raises(ValueError) as failure:
        check_delivery_coverage(variables, obligations, withheld={})
    assert str(failure.value) == found.detail
    return found


def test_delivery_fact_change_is_a_located_error_naming_the_coordinate_and_source():
    """Input: the written type "text" against the claimed "integer". Expected: one
    ``unexplained_delivery_fact_change`` error with the obligation's refs, naming
    the coordinate, the claim and the source; strict mode raises the same text.
    Fails if the type comparison is dropped, or the diagnostic loses its code,
    severity or refs, or diverges from the strict refusal.
    """
    obligation = _fact_obligation()
    _allowed((_fact_variable(),), (obligation,))
    found = _refusal((_fact_variable(data_type="text"),), (obligation,), FACT)
    assert found.refs == obligation.refs
    assert found.detail.startswith(
        "supported delivery facts changed without an explicit source outcome"
    )
    assert "scb/example/value people/VALUE 2020-01-01..2020-12-31" in found.detail
    assert "claimed by fixture/key" in found.detail
    assert "claimed data_type='integer' written 'text'" in found.detail


def test_delivery_coverage_loss_is_a_located_error_naming_the_lost_window():
    """Input: the 2020 state truncated to 2020-06-30. Expected: one
    ``unexplained_delivery_coverage_loss`` error with the obligation's refs and
    exactly the lost 2020-07-01..2020-12-31; strict mode raises the same text.
    #1319 deleted its scope-minted twin citing this test. Fails if the guard
    widens periods, reports the hull, or the diagnostic loses its code or refs.
    """
    obligation = _fact_obligation()
    found = _refusal((_fact_variable(valid_to="2020-06-30"),), (obligation,), LOSS)
    assert found.refs == obligation.refs
    assert found.detail.startswith(
        "supported delivery coverage was lost without an explicit source outcome"
    )
    assert (
        "scb/example/value people/VALUE 2020-07-01..2020-12-31 claimed by fixture/key"
        in found.detail
    )


@pytest.mark.parametrize(
    "provenance",
    ["correction:one-extended", None],
    ids=["substring", "no-provenance"],
)
def test_claimed_correction_must_remain_an_exact_provenance_element(provenance):
    """Input: provenance "correction:one-extended", or none, against the claimed
    correction "correction:one"; the twin keeps it beside a comment. Expected:
    "claimed attributions". Fails if attributions match by substring or are
    skipped when provenance is empty.
    """
    obligation = _fact_obligation(attributions=("correction:one",))
    _allowed((_fact_variable(provenance="correction:one\n\ncomment"),), (obligation,))
    found = _refusal((_fact_variable(provenance=provenance),), (obligation,), FACT)
    assert "claimed attributions=('correction:one',)" in found.detail


def test_claimed_length_is_compared():
    """Input: the written length "1" against the claimed "0". Expected: "claimed
    data_length='0' written '1'". Fails if the length comparison is dropped.
    """
    obligation = _fact_obligation(data_length_claim=("value", "0"))
    _allowed((_fact_variable(data_length="0"),), (obligation,))
    found = _refusal((_fact_variable(),), (obligation,), FACT)
    assert "claimed data_length='0' written '1'" in found.detail


def test_negative_unit_claim_refuses_a_unit_backfilled_from_a_neighbouring_state():
    """Input: 2021 written with 2020's unit "Kronor" where 2021's source leaves the
    unit absent (formation's negative claim). Expected: "literal delivery unit
    changed" naming 2021. Fails if a negative claim is treated as no claim.
    """
    first = _fact_variable(measurement_unit="Kronor").states[0]
    leaked = first.model_copy(
        update={"valid_from": "2021-01-01", "valid_to": "2021-12-31"}
    )
    obligations = (
        _fact_obligation(measurement_unit_claim=("value", "Kronor")),
        _fact_obligation(valid_from="2021-01-01", valid_to="2021-12-31"),
    )
    variable = _fact_variable()
    _allowed(
        (
            _with_states(
                variable, first, leaked.model_copy(update={"measurement_unit": None})
            ),
        ),
        obligations,
    )
    found = _refusal((_with_states(variable, first, leaked),), obligations, FACT)
    assert (
        "people/VALUE 2021-01-01..2021-12-31 claimed by fixture/key: "
        "literal delivery unit changed" in found.detail
    )


def test_alias_window_claim_is_checked_against_the_shared_state_behind_it():
    """Input: only Second's 2020-07..12 window is claimed, and the First state
    behind it is retyped to text. Expected: facts changed, naming people/Second.
    Fails if alias-window claims skip the backing state's facts.
    """
    obligation = _fact_obligation(column="Second", valid_from="2020-07-01")
    variable = _fact_variable(column="First", aliases=(_second_window(),))
    _allowed((variable,), (obligation,))
    found = _refusal(
        (
            _with_states(
                variable, variable.states[0].model_copy(update={"data_type": "text"})
            ),
        ),
        (obligation,),
        FACT,
    )
    assert (
        "people/Second 2020-07-01..2020-12-31 claimed by fixture/key: "
        "claimed data_type='integer' written 'text'" in found.detail
    )


@pytest.mark.parametrize(
    "valid_to,missing",
    [(None, "2020-07-01..2020-12-31"), ("2020-09-30", "2020-10-01..2020-12-31")],
    ids=["deleted", "truncated"],
)
def test_alias_window_needs_a_backing_state_for_its_whole_window(valid_to, missing):
    """Input: the state behind Second's window deleted, or truncated to
    2020-09-30. Expected: "no written state carries the claimed facts for" exactly
    the missing window. Fails if an alias window counts as delivery without a
    backing state.
    """
    obligation = _fact_obligation(column="Second", valid_from="2020-07-01")
    variable = _fact_variable(column="First", aliases=(_second_window(),))
    states = (
        ()
        if valid_to is None
        else (variable.states[0].model_copy(update={"valid_to": valid_to}),)
    )
    found = _refusal((_with_states(variable, *states),), (obligation,), FACT)
    assert f"no written state carries the claimed facts for {missing}" in found.detail


def test_overlapping_backing_states_behind_an_alias_window_are_refused():
    """Input: First and Extra both cover Second's window. Expected: "alias backing
    is ambiguous: 2 states of variant people overlap 2020-07-01..2020-12-31".
    Fails if the guard picks one of the overlapping states instead of refusing.
    """
    obligation = _fact_obligation(column="Second", valid_from="2020-07-01")
    variable = _fact_variable(column="First", aliases=(_second_window(),))
    (first,) = variable.states
    _allowed((variable,), (obligation,))
    extra = first.model_copy(update={"delivery_column_name": "Extra"})
    found = _refusal((_with_states(variable, first, extra),), (obligation,), FACT)
    assert (
        "alias backing is ambiguous: 2 states of variant people "
        "overlap 2020-07-01..2020-12-31" in found.detail
    )


def test_year_independent_claim_is_met_only_by_a_year_independent_state():
    """Input: an independent claim against a dated state, against an independent
    state on another column or with another type, and a dated claim against the
    independent state. Expected: coverage lost, or facts changed for the type.
    Year-independent states come only from LISA (``source_intervals`` via
    ``sources/lisa.py``), and the build-case runner materializes SCB and SOS
    deliveries only, so no case reaches even the allowed side. Fails if calendar
    coverage stands in for a year-independent claim or the reverse.
    """
    dated = _fact_variable()
    independent = _with_states(
        dated,
        dated.states[0].model_copy(
            update={
                "period_scope": "year_independent",
                "valid_from": None,
                "valid_to": None,
            }
        ),
    )
    claim = replace(
        _fact_obligation(),
        period_scope="year_independent",
        valid_from=None,
        valid_to=None,
    )
    _allowed((independent,), (claim,))
    (state,) = independent.states
    for written, obligation, code, fragment in (
        (
            dated,
            claim,
            LOSS,
            "year_independent delivery",
        ),
        (
            _with_states(
                independent, state.model_copy(update={"delivery_column_name": "OTHER"})
            ),
            claim,
            LOSS,
            "year_independent delivery",
        ),
        (
            _with_states(independent, state.model_copy(update={"data_type": "text"})),
            claim,
            FACT,
            "claimed data_type='integer' written 'text'",
        ),
        (
            independent,
            _fact_obligation(),
            LOSS,
            "people/VALUE 2020-01-01..2020-12-31",
        ),
    ):
        assert fragment in _refusal((written,), (obligation,), code).detail


@pytest.mark.parametrize(
    "damage,claims,fragment",
    [
        (None, (), None),
        ("domain", ("Second",), "claimed coding="),
        ("text", ("Second",), "column operation/source attribution changed"),
        ("common", ("First", "Second"), "invalid per-column coding override"),
        ("classified", ("First", "Second"), "invalid per-column coding override"),
    ],
)
def test_per_column_alias_coding_preserves_exact_delivery_claims(
    damage, claims, fragment
):
    """Per-column coding windows must carry exactly their claimed value sets.

    Input: an undamaged pair (allowed); Second's window with First's domain or
    another source text, or a backing state that carries its own value set or a
    classification. Expected: one located fact change per damaged claim. Fails
    if per-column windows skip the coding claim or the column-text claim
    (operational definition and source attribution), or accept a coded or
    classified backing state. A window without a value set is refused when it is
    built (``ResolvedAliasWindow``), so it is no row here.
    """
    first = ResolvedCodeSet(members=(("0", "No"), ("1", "Yes")))
    second = ResolvedCodeSet(members=(("1", "Yes"), ("2", "Unknown")))
    state = (
        _fact_variable(column="First")
        .states[0]
        .model_copy(update={"value_set": None, "value_set_version_label": ""})
    )
    windows = tuple(
        ResolvedAliasWindow(
            valid_from="2020-01-01",
            valid_to="2020-12-31",
            coding_metadata="per_column",
            # Per-column metadata carries the window's own facts and text.
            column_metadata="per_column",
            data_type="integer",
            data_length="1",
            value_set=domain,
            value_set_version_label=version,
            source_register_text=text,
        )
        for domain, version, text in (
            (first, "First question", "First source"),
            (second, "Second question", "Second source"),
        )
    )
    if damage == "domain":
        windows = (windows[0], windows[1].model_copy(update={"value_set": first}))
    elif damage == "text":
        windows = (
            windows[0],
            windows[1].model_copy(update={"source_register_text": "Another source"}),
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
                ResolvedAlias(
                    variant=PEOPLE, delivery_column_name=column, windows=(window,)
                )
                for column, window in zip(("First", "Second"), windows, strict=True)
            ),
        }
    )
    obligations = tuple(
        replace(
            _fact_obligation(column=column),
            coding_claim=(domain, version),
            column_text_claim=(None, text),
        )
        for column, domain, version, text in (
            ("First", first, "First question", "First source"),
            ("Second", second, "Second question", "Second source"),
        )
    )
    if damage is None:
        _allowed((variable,), obligations)
        return
    found = check_delivery_coverage(
        (variable,), obligations, withheld={}, diagnostic=True
    )
    assert [(f.code, f.subject.split()[1]) for f in found] == [
        (FACT, f"people/{column}") for column in claims
    ]
    assert all(fragment in f.detail for f in found)
    with pytest.raises(ValueError, match="supported delivery facts changed"):
        check_delivery_coverage((variable,), obligations, withheld={})
