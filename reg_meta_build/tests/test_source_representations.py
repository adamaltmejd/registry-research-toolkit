"""Accepted parallel columns share metadata without inheriting a sibling's facts."""

from __future__ import annotations

from contextlib import closing
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, _var_row
from pydantic import ValidationError
from reg_meta.source_evidence import SourceRevision
from reg_meta_build.catalog_dependencies import check_delivery_coverage
from reg_meta_build.db import open_built_db
from reg_meta_build.resolved_catalog import (
    ResolvedClassificationLink,
    ResolvedRegister,
    ResolvedVariant,
    write_resolved_catalog,
)
from reg_meta_build.source_coding import (
    CodeListClaim,
    CodeMembershipClaim,
    resolve_code_membership,
)
from reg_meta_build.source_coordinates import column_identity
from reg_meta_build.source_curation import (
    CheckedFieldChange,
    CheckedIdentityChange,
    ColumnRepresentation,
    CurationCase,
    FieldExpectation,
    OccurrenceCorrectionDecision,
    PeerGuard,
    RepresentationDecision,
    capture_expectations,
)
from reg_meta_build.source_effects import (
    apply_occurrence_cases,
    record_ref,
)
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    NativeCoordinates,
    ScopeInterval,
    SourceFields,
    TemporalScope,
    value_field,
)
from reg_meta_build.source_representations import (
    form_representations,
    resolve_representation_cases,
)
from reg_meta_build.sources.scb_records import clean_scb_row

if TYPE_CHECKING:
    from pathlib import Path


KEY = ("accepted", "fixture", "income")


def _records(*, vardef="A generic family label", varopdef="", data_type="int"):
    revision = SourceRevision.create(
        dataset="fixture",
        publisher="SCB",
        purpose="test",
        upstream_revision="1",
        artifact_path="rows.csv",
        artifact_size=1,
        artifact_sha256="a" * 64,
    )
    header = REGISTERINFORMATION_HEADER.split("|")
    result = []
    for i, column in enumerate(("First", "Second")):
        values = _var_row(
            colname=column,
            cvid=100 + i,
            var_id=i + 1,
            varname=column,
            year="2020",
            vardef=vardef if i else "A generic family label",
            varopdef=varopdef if i else "",
            data_type=data_type if i else "int",
        ).split("|")
        result.append(
            clean_scb_row(
                header,
                i + 1,
                {
                    name: (True, value, value)
                    for name, value in zip(header, values, strict=True)
                },
                revision,
            ).record
        )
    return tuple(result)


def _setup(records=None, claims=None):
    records = _records() if records is None else records
    variant_key = source_occurrence(records[0]).variant_key
    assert variant_key is not None
    variant = ResolvedVariant(slug="people", name="People")
    coding = {
        column_identity(KEY, variant_key, column): resolve_code_membership(
            tuple((claims or {}).get(column, ()))
        )
        for column in ("First", "Second")
    }
    expectations = capture_expectations(
        records,
        fields=(
            "column_name",
            "name",
            "description",
            "definition",
            "measurement_unit",
            "data_type",
            "data_length",
            "operational_definition",
            "source_attribution",
        ),
        coding=True,
    )
    guard = PeerGuard(
        guard_id="exact-family",
        source=records[0].source,
        native=NativeCoordinates(register_id=1, register_variant_id=10),
        edition_scopes=(records[0].edition_scope,),
        expected_members=tuple(e.ref for e in expectations),
    )
    identity = CurationCase(
        case_id="identity",
        targets=expectations,
        peer_guards=(guard,),
        decision=OccurrenceCorrectionDecision(
            reviewed=True,
            effects=tuple(
                e
                for r in records
                for e in (
                    CheckedIdentityChange(ref=record_ref(r), variable_key=KEY),
                    CheckedFieldChange(
                        ref=record_ref(r),
                        replacement=FieldExpectation(
                            name="name", status="value", value="Income"
                        ),
                    ),
                )
            ),
            reason="Existing parallel family identity and label",
            provenance="accepted family",
        ),
    )
    effects = apply_occurrence_cases(records, (identity,))
    assert effects.diagnostics == ()
    representation = CurationCase(
        case_id="representations",
        targets=expectations,
        peer_guards=(guard,),
        decision=RepresentationDecision(
            reviewed=True,
            variable_key=KEY,
            variant_key=variant_key,
            valid_from="2020-01-01",
            valid_to="2020-12-31",
            columns=tuple(
                ColumnRepresentation(
                    column=column,
                    valid_from=start,
                    valid_to=end,
                )
                for column, start, end in (
                    ("First", "2020-01-01", "2020-06-30"),
                    ("Second", "2020-07-01", "2020-12-31"),
                )
            ),
            reason="Existing accepted precise period representations",
            provenance="accepted family",
        ),
    )
    return records, effects.occurrences, representation, {variant_key: variant}, coding


def _form(setup, cases=None):
    records, occurrences, case, variants, coding = setup
    resolution = resolve_representation_cases(
        records, (case,) if cases is None else cases, coding=coding
    )
    formed = form_native_variable(
        occurrences,
        register=ResolvedRegister(provider="scb", slug="example", name="Example"),
        variants=variants,
        slug="income",
        provider_key="family",
        flags=SourceFields(
            sensitivity=value_field(False), identifier=value_field(False)
        ),
        coding=coding,
        representations=resolution.cases,
    )
    return formed, resolution


def _column_storage_setup(
    first_type, first_length, second_type, second_length, *, claims=None
):
    records = tuple(
        record.model_copy(
            update={
                "fields": record.fields.model_copy(
                    update={
                        "data_type": value_field(dtype) if dtype is not None else None,
                        "data_length": value_field(length)
                        if length is not None
                        else None,
                    }
                )
            }
        )
        for record, dtype, length in zip(
            _records(),
            (first_type, second_type),
            (first_length, second_length),
            strict=True,
        )
    )
    setup = _setup(records, claims=claims)
    decision = setup[2].decision
    assert isinstance(decision, RepresentationDecision)
    case = setup[2].model_copy(
        update={
            "decision": decision.model_copy(
                update={
                    "column_metadata": "per_column",
                    "columns": tuple(
                        column.model_copy(
                            update={
                                "valid_from": decision.valid_from,
                                "valid_to": decision.valid_to,
                            }
                        )
                        for column in decision.columns
                    ),
                }
            )
        }
    )
    return (*setup[:2], case, *setup[3:])


@pytest.mark.parametrize(
    ("first_type", "first_length", "second_type", "second_length"),
    [
        ("text", "200", "text", "18"),
        ("decimal", "53", "integer", "0"),
        (None, None, "integer", "0"),
    ],
)
def test_per_column_storage_survives_formation_coverage_and_catalog_read(
    tmp_path: Path, first_type, first_length, second_type, second_length
) -> None:
    setup = _column_storage_setup(first_type, first_length, second_type, second_length)
    formed, proof = _form(setup)
    assert proof.diagnostics == ()
    assert [d.code for d in formed.diagnostics] == (
        ["unknown_data_type"] if first_type is None else []
    )
    variable = formed.variable
    assert variable is not None and len(variable.states) == 1
    expected = {
        "First": (first_type, first_length),
        "Second": (second_type, second_length),
    }
    assert {
        alias.delivery_column_name: (
            alias.windows[0].data_type,
            alias.windows[0].data_length,
        )
        for alias in variable.aliases
    } == expected
    assert all(
        w.column_metadata == "per_column" for a in variable.aliases for w in a.windows
    )
    check_delivery_coverage((variable,), formed.coverage, withheld={})
    last_alias = variable.aliases[-1]
    bad_alias = last_alias.model_copy(
        update={
            "windows": (
                last_alias.windows[0].model_copy(update={"data_length": "wrong"}),
            )
        }
    )
    with pytest.raises(ValueError, match="claimed data_length"):
        check_delivery_coverage(
            (
                variable.model_copy(
                    update={"aliases": (*variable.aliases[:-1], bad_alias)}
                ),
            ),
            formed.coverage,
            withheld={},
        )
    if first_type is None:
        # A known null on the physical column must override even a populated base.
        variable = variable.model_copy(
            update={
                "states": tuple(
                    state.model_copy(
                        update={"data_type": "integer", "data_length": "99"}
                    )
                    for state in variable.states
                )
            }
        )
    write_resolved_catalog((variable,), tmp_path / "reg_meta.db", manifest={})
    with closing(open_built_db(tmp_path / "reg_meta.db")) as conn:
        assert {
            row[0]: (row[1], row[2])
            for row in conn.execute(
                "SELECT delivery_column_name, data_type, data_length FROM variable_alias_window WHERE column_metadata='per_column'"
            )
        } == expected
    assert (
        _form((tuple(reversed(setup[0])), tuple(reversed(setup[1])), *setup[2:]))[
            0
        ].variable
        == formed.variable
    )
    changed = setup[0][0].model_copy(
        update={
            "fields": setup[0][0].fields.model_copy(
                update={"data_length": value_field("changed")}
            )
        }
    )
    assert (
        resolve_representation_cases(
            (changed, setup[0][1]), (setup[2],), coding=setup[4]
        ).cases
        == ()
    )


def test_per_column_window_spans_coding_states_without_losing_storage(
    tmp_path: Path,
) -> None:
    claims = tuple(
        CodeListClaim(
            name,
            TemporalScope(
                kind="intervals", intervals=(ScopeInterval(start=start, end=end),)
            ),
            (
                CodeMembershipClaim(
                    code, "Label", TemporalScope(kind="year_independent")
                ),
            ),
        )
        for name, code, start, end in (
            ("before", "01", "2020-01-01", "2020-06-30"),
            ("after", "02", "2020-07-01", "2020-12-31"),
        )
    )
    setup = _column_storage_setup(
        "text", "200", "text", "18", claims={"First": claims, "Second": claims}
    )
    formed, _ = _form(setup)
    assert formed.diagnostics == () and formed.variable is not None
    variable = formed.variable.model_copy(
        update={
            "aliases": tuple(
                alias.model_copy(
                    update={
                        "windows": (
                            alias.windows[0].model_copy(
                                update={
                                    "valid_from": "2020-01-01",
                                    "valid_to": "2020-12-31",
                                }
                            ),
                        )
                    }
                )
                for alias in formed.variable.aliases
            )
        }
    )
    assert len(variable.states) == 2
    check_delivery_coverage((variable,), formed.coverage, withheld={})
    write_resolved_catalog((variable,), tmp_path / "reg_meta.db", manifest={})
    with closing(open_built_db(tmp_path / "reg_meta.db")) as conn:
        assert {
            tuple(row)
            for row in conn.execute("SELECT valid_from, valid_to FROM variable_state")
        } == {("2020-01-01", "2020-06-30"), ("2020-07-01", "2020-12-31")}
        assert {
            tuple(row)
            for row in conn.execute(
                "SELECT delivery_column_name, valid_from, valid_to, data_length FROM variable_alias_window WHERE column_metadata='per_column'"
            )
        } == {
            ("First", "2020-01-01", "2020-12-31", "200"),
            ("Second", "2020-01-01", "2020-12-31", "18"),
        }
    from reg_meta.inventory_check import _expanded_columns

    assert _expanded_columns(
        [(s.valid_from, s.valid_to, s.delivery_column_name) for s in variable.states],
        [
            (
                a.delivery_column_name,
                w.valid_from,
                w.valid_to,
                w.provenance,
                w.column_metadata,
            )
            for a in variable.aliases
            for w in a.windows
        ],
    ) == {"First", "Second"}
    gap = variable.model_copy(
        update={
            "states": (
                variable.states[0].model_copy(update={"valid_to": "2020-06-29"}),
                variable.states[1],
            )
        }
    )
    with pytest.raises(ValueError, match="complete backing states"):
        write_resolved_catalog((gap,), tmp_path / "gap.db", manifest={})


def test_per_column_storage_keeps_same_column_conflict_and_rejects_mixed_modes() -> (
    None
):
    setup = _column_storage_setup("text", "200", "integer", "0")
    formed, _ = _form(setup)
    assert formed.variable is not None
    state = formed.variable.states[0]
    variant = state.variant
    members = [
        state.model_copy(
            update={
                "delivery_column_name": col,
                "data_type": dtype,
                "data_length": length,
            }
        )
        for col, dtype, length in (
            ("First", "text", "200"),
            ("First", "text", "18"),
            ("Second", "integer", "0"),
        )
    ]
    result, aliases, issues, _, conflicts = form_representations(
        members, (setup[2],), variable_key=KEY, variants=setup[3], subject="fixture"
    )
    assert len(result) == 1
    assert aliases[0].windows[0].data_length is None
    assert aliases[1].windows[0].data_length == "0"
    assert [(d.code, d.fields) for d in issues] == [
        ("conflicting_representation_fact", ("data_length",))
    ]
    assert conflicts == (
        (variant.slug, "First", "data_length", "2020-01-01", "2020-12-31"),
    )
    case = setup[2]
    assert isinstance(case.decision, RepresentationDecision)
    shared = case.model_copy(
        update={
            "case_id": "shared",
            "decision": case.decision.model_copy(update={"column_metadata": "shared"}),
        }
    )
    result, aliases, issues, _, _ = form_representations(
        members, (case, shared), variable_key=KEY, variants=setup[3], subject="fixture"
    )
    assert result == [] and aliases == ()
    assert issues[0].code == "conflicting_representation_decisions"


def test_shared_state_keeps_metadata_period_and_query_selects_precise_column(
    tmp_path: Path,
) -> None:
    setup = _setup()
    formed, proof = _form(setup)
    assert proof.diagnostics == () and formed.diagnostics == ()
    variable = formed.variable
    assert variable is not None and len(variable.states) == 1
    state = variable.states[0]
    assert (state.valid_from, state.valid_to) == ("2020-01-01", "2020-12-31")
    assert state.delivery_column_name == "First"  # Deterministic storage label only.
    assert [a.delivery_column_name for a in variable.aliases] == ["First", "Second"]
    assert all(w.provenance is None for a in variable.aliases for w in a.windows)
    # The decision reassigns each half-year to one column, and that narrowed claim
    # stays checked: the shared state and the alias windows have to deliver it.
    assert [
        (o.variant, o.column, o.valid_from, o.valid_to) for o in formed.coverage
    ] == [
        ("people", "First", "2020-01-01", "2020-06-30"),
        ("people", "Second", "2020-07-01", "2020-12-31"),
    ]
    check_delivery_coverage((variable,), formed.coverage, withheld={})
    dropped = variable.model_copy(update={"aliases": variable.aliases[:1]})
    with pytest.raises(ValueError, match=r"Second 2020-07-01\.\.2020-12-31"):
        check_delivery_coverage((dropped,), formed.coverage, withheld={})
    assert state.provenance is not None and "representations:" in state.provenance
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog((variable,), output, manifest={})
    with closing(open_built_db(output)) as conn:
        assert conn.execute("SELECT count(*) FROM variable_state").fetchone()[0] == 1
        assert (
            conn.execute("SELECT count(*) FROM variable_alias_window").fetchone()[0]
            == 2
        )
    with closing(open_built_db(output)) as conn:
        assert {
            tuple(row)
            for row in conn.execute(
                "SELECT delivery_column_name, valid_from, valid_to FROM variable_alias_window"
            )
        } == {
            ("First", "2020-01-01", "2020-06-30"),
            ("Second", "2020-07-01", "2020-12-31"),
        }
    reversed_setup = (tuple(reversed(setup[0])), tuple(reversed(setup[1])), *setup[2:])
    reversed_formed, _ = _form(reversed_setup)
    assert reversed_formed.variable == variable
    assert reversed_formed.diagnostics == formed.diagnostics
    assert reversed_formed.occurrences == tuple(reversed(formed.occurrences))


def test_sibling_descriptions_and_state_facts_are_not_copied_from_representative() -> (
    None
):
    formed, _ = _form(
        _setup(
            _records(vardef="Different definition", varopdef="Different interpretation")
        )
    )
    assert formed.variable is not None
    assert formed.variable.definition is None
    assert formed.variable.states[0].operational_definition is None
    assert {(d.code, d.fields) for d in formed.diagnostics} >= {
        ("conflicting_variable_fact", ("definition",)),
        ("conflicting_representation_fact", ("operational_definition",)),
    }


def test_coding_cut_inside_alias_window_keeps_a_participating_base(
    tmp_path: Path,
) -> None:
    claims = tuple(
        CodeListClaim(
            name,
            TemporalScope(
                kind="intervals", intervals=(ScopeInterval(start=start, end=end),)
            ),
            (
                CodeMembershipClaim(
                    code, "Label", TemporalScope(kind="year_independent")
                ),
            ),
        )
        for name, code, start, end in (
            ("before", "01", "2020-01-01", "2020-09-14"),
            ("after", "02", "2020-09-15", "2020-12-31"),
        )
    )
    formed, _ = _form(_setup(claims={"First": claims, "Second": claims}))
    assert formed.variable is not None and formed.diagnostics == ()
    assert [s.delivery_column_name for s in formed.variable.states] == [
        "First",
        "Second",
    ]
    write_resolved_catalog((formed.variable,), tmp_path / "reg_meta.db", manifest={})
    with closing(open_built_db(tmp_path / "reg_meta.db")) as conn:
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT valid_from, valid_to FROM variable_state ORDER BY valid_from"
            )
        ] == [("2020-01-01", "2020-09-14"), ("2020-09-15", "2020-12-31")]
        assert {
            tuple(row)
            for row in conn.execute(
                "SELECT delivery_column_name, valid_from, valid_to FROM variable_alias_window"
            )
        } == {
            ("First", "2020-01-01", "2020-06-30"),
            ("Second", "2020-07-01", "2020-09-14"),
            ("Second", "2020-09-15", "2020-12-31"),
        }


def _claim(name, code, year=2020):
    return CodeListClaim(
        name,
        TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start=str(year), end=str(year)),)
        ),
        (CodeMembershipClaim(code, "Label", TemporalScope(kind="year_independent")),),
    )


def test_coding_disagreement_is_withheld_without_a_representation_coding_pin() -> None:
    setup = _setup(
        claims={"First": (_claim("first", "01"),), "Second": (_claim("second", "02"),)}
    )
    formed, _ = _form(setup)
    assert formed.variable is not None and formed.variable.states[0].value_set is None
    assert any(
        d.code == "conflicting_representation_coding" for d in formed.diagnostics
    )
    records, _, case, variants, coding = _setup()
    variant_key = next(iter(variants))
    changed = dict(coding)
    changed[column_identity(KEY, variant_key, "First")] = resolve_code_membership(
        (_claim("new", "01"),)
    )
    proof = resolve_representation_cases(records, (case,), coding=changed)
    assert proof.cases == (case,) and proof.diagnostics == ()
    changed[column_identity(KEY, variant_key, "First")] = resolve_code_membership(
        (_claim("future", "01", 2021),)
    )
    assert (
        resolve_representation_cases(records, (case,), coding=changed).diagnostics == ()
    )


def test_changed_source_membership_is_stale_and_missing_coding_binding_is_fatal() -> (
    None
):
    records, _, case, _, coding = _setup()
    proof = resolve_representation_cases(records[:1], (case,), coding=coding)
    assert proof.cases == () and proof.evaluations[0].status == "stale"
    with pytest.raises(ValueError, match="unconverted column"):
        resolve_representation_cases(records, (case,), coding={})
    with pytest.raises(ValueError, match="guarded original membership"):
        resolve_representation_cases(
            records, (case.model_copy(update={"peer_guards": ()}),), coding=coding
        )


def test_missing_formed_member_withholds_group_without_manufacturing_metadata() -> None:
    setup = _setup()
    formed, _ = _form(setup)
    assert formed.variable is not None
    state = formed.variable.states[0]
    result, aliases, issues, waivers, _facts = form_representations(
        [state], (setup[2],), variable_key=KEY, variants=setup[3], subject="fixture"
    )
    assert result == [] and aliases == ()
    assert issues[0].code == "missing_representation_metadata"
    # The withheld period still belongs to the checked decision, not to a bug.
    assert {(variant, column) for variant, column, _, _ in waivers} == {
        ("people", "First"),
        ("people", "Second"),
    }


def test_equal_decisions_compose_and_conflicting_representation_windows_withhold() -> (
    None
):
    setup = _setup()
    case = setup[2]
    decision = case.decision
    assert isinstance(decision, RepresentationDecision)
    duplicate = case.model_copy(update={"case_id": "second"})
    formed, _ = _form(setup, (case, duplicate))
    assert formed.variable is not None and formed.diagnostics == ()
    assert (
        formed.variable.states[0].provenance is not None
        and "second:" in formed.variable.states[0].provenance
    )
    swapped = decision.model_copy(
        update={
            "columns": (
                decision.columns[0].model_copy(
                    update={"valid_from": "2020-07-01", "valid_to": "2020-12-31"}
                ),
                decision.columns[1].model_copy(
                    update={"valid_from": "2020-01-01", "valid_to": "2020-06-30"}
                ),
            )
        }
    )
    conflict = duplicate.model_copy(update={"decision": swapped})
    formed, _ = _form(setup, (case, conflict))
    assert formed.variable is None
    assert any(
        d.code == "conflicting_representation_decisions" for d in formed.diagnostics
    )


def test_conflicting_decision_withholds_only_its_intersection() -> None:
    setup = _setup()
    case = setup[2]
    decision = case.decision
    assert isinstance(decision, RepresentationDecision)
    overlap = RepresentationDecision(
        reviewed=True,
        variable_key=decision.variable_key,
        variant_key=decision.variant_key,
        valid_from="2020-07-01",
        valid_to="2020-12-31",
        columns=(
            ColumnRepresentation(
                column="First",
                valid_from="2020-07-01",
                valid_to="2020-09-30",
            ),
            ColumnRepresentation(
                column="Second",
                valid_from="2020-10-01",
                valid_to="2020-12-31",
            ),
        ),
        reason="Competing finite interpretation",
        provenance="fixture",
    )
    formed, _ = _form(
        setup,
        (case, case.model_copy(update={"case_id": "overlap", "decision": overlap})),
    )
    assert formed.variable is not None
    assert [(s.valid_from, s.valid_to) for s in formed.variable.states] == [
        ("2020-01-01", "2020-06-30")
    ]
    assert [a.delivery_column_name for a in formed.variable.aliases] == ["First"]
    conflict = next(
        d
        for d in formed.diagnostics
        if d.code == "conflicting_representation_decisions"
    )
    assert (conflict.valid_from, conflict.valid_to) == (
        "2020-07-01",
        "2020-12-31",
    )


@pytest.mark.parametrize("defect", ["gap", "outside", "duplicate", "future"])
def test_representation_contract_requires_exact_finite_cover(defect: str) -> None:
    decision = _setup()[2].decision.model_dump()
    if defect == "gap":
        decision["columns"][0]["valid_to"] = "2020-06-29"
    elif defect == "outside":
        decision["columns"][1]["valid_to"] = "2021-12-31"
    elif defect == "duplicate":
        decision["columns"][1]["column"] = "First"
    else:
        decision["valid_to"] = "9999-12-31"
    with pytest.raises(ValidationError):
        RepresentationDecision.model_validate(decision)


def test_shared_state_facts_reach_both_columns_behind_alias_windows() -> None:
    formed, _ = _form(_setup())
    assert formed.variable is not None
    assert [(o.column, o.valid_from, o.valid_to) for o in formed.coverage] == [
        ("First", "2020-01-01", "2020-06-30"),
        ("Second", "2020-07-01", "2020-12-31"),
    ]
    assert {o.data_type_claim for o in formed.coverage} == {("value", "integer")}
    check_delivery_coverage((formed.variable,), formed.coverage, withheld={})
    state = formed.variable.states[0]
    damaged = formed.variable.model_copy(
        update={"states": (state.model_copy(update={"data_type": "text"}),)}
    )
    with pytest.raises(
        ValueError,
        match="supported delivery facts changed without an explicit source outcome",
    ) as failure:
        check_delivery_coverage((damaged,), formed.coverage, withheld={})
    assert "claimed data_type='integer' written 'text'" in str(failure.value)


def test_conflicting_parallel_fact_nulls_only_that_claimed_fact() -> None:
    formed, _ = _form(_setup(_records(data_type="text")))
    assert formed.variable is not None
    assert ("conflicting_representation_fact", ("data_type",)) in {
        (d.code, d.fields) for d in formed.diagnostics
    }
    assert formed.variable.states[0].data_type is None
    nulled = {o.column: o for o in formed.coverage}
    assert set(nulled) == {"First", "Second"}
    assert all(o.data_type_claim is None for o in nulled.values())
    # The window is still owed; only the disputed fact is nulled.
    check_delivery_coverage((formed.variable,), formed.coverage, withheld={})


def test_dated_representation_does_not_manufacture_independent_window():
    setup = _setup()
    formed, _ = _form(setup)
    independent = [
        state.model_copy(
            update={
                "period_scope": "year_independent",
                "valid_from": None,
                "valid_to": None,
            }
        )
        for state in formed.variable.states
    ]
    states, aliases, issues, withheld, _ = form_representations(
        independent,
        (setup[2],),
        variable_key=KEY,
        variants=setup[3],
        subject="fixture",
    )
    assert states == independent
    assert not aliases and not withheld
    assert any(issue.code == "unsupported_representation_scope" for issue in issues)


def _column_coding_setup():
    from reg_meta_build.source_coding import copied_coding_fingerprints

    setup = _column_storage_setup(
        "text",
        "18",
        "integer",
        "0",
        claims={"First": (_claim("first", "01"),), "Second": (_claim("second", "02"),)},
    )
    decision = setup[2].decision
    assert isinstance(decision, RepresentationDecision)
    variant_key = next(iter(setup[3]))
    decision = decision.model_copy(
        update={
            "coding_metadata": "per_column",
            "columns": tuple(
                column.model_copy(
                    update={
                        "expected_codings": copied_coding_fingerprints(
                            setup[4][
                                column_identity(KEY, variant_key, column.column)
                            ].claims
                        ),
                    }
                )
                for column in decision.columns
            ),
        }
    )
    return (*setup[:2], setup[2].model_copy(update={"decision": decision}), *setup[3:])


def test_per_column_coding_preserves_native_domains_and_alias_only_index(
    tmp_path: Path,
) -> None:
    setup = _column_coding_setup()
    formed, proof = _form(setup)
    assert proof.diagnostics == () and formed.diagnostics == ()
    variable = formed.variable
    assert variable is not None
    assert variable.states[0].value_set is None
    assert {
        alias.delivery_column_name: alias.windows[0].value_set.members
        for alias in variable.aliases
    } == {
        "First": (("01", "Label"),),
        "Second": (("02", "Label"),),
    }
    assert all(
        window.coding_metadata == "per_column" and window.provenance is None
        for alias in variable.aliases
        for window in alias.windows
    )
    check_delivery_coverage((variable,), formed.coverage, withheld={})
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog((variable,), output, manifest={})
    with closing(open_built_db(output)) as conn:
        assert conn.execute("SELECT count(*) FROM value_set").fetchone()[0] == 2
        assert conn.execute("SELECT count(*) FROM code_variable_map").fetchone()[0] == 2
        assert (
            conn.execute(
                "SELECT count(*) FROM variable_alias_window WHERE value_set_id IS NOT NULL"
            ).fetchone()[0]
            == 2
        )


def test_per_column_coding_source_domain_drift_fails_closed() -> None:
    setup = _column_coding_setup()
    coding = dict(setup[4])
    key = column_identity(KEY, next(iter(setup[3])), "First")
    coding[key] = resolve_code_membership((_claim("first", "changed"),))
    proof = resolve_representation_cases(setup[0], (setup[2],), coding=coding)
    assert proof.cases == ()
    assert [issue.code for issue in proof.diagnostics] == [
        "stale_representation_coding"
    ]


@pytest.mark.parametrize("unsupported", ["missing", "classification", "conflict"])
def test_per_column_coding_withholds_unsupported_domain_without_shared_fallback(
    unsupported,
) -> None:
    setup = _column_coding_setup()
    formed, _ = _form(setup)
    variable = formed.variable
    assert variable is not None
    base = variable.states[0]
    states = [
        base.model_copy(
            update={
                "delivery_column_name": alias.delivery_column_name,
                "value_set": alias.windows[0].value_set,
                "value_set_version_label": alias.windows[0].value_set_version_label,
            }
        )
        for alias in variable.aliases
    ]
    if unsupported == "missing":
        states[0] = states[0].model_copy(update={"value_set": None})
    elif unsupported == "classification":
        states[0] = states[0].model_copy(
            update={
                "classification_links": (
                    ResolvedClassificationLink(classification="sni2007"),
                )
            }
        )
    else:
        states.append(states[0].model_copy(update={"value_set": states[1].value_set}))
    result, aliases, issues, _, _ = form_representations(
        states, (setup[2],), variable_key=KEY, variants=setup[3], subject="fixture"
    )
    assert result == [] and aliases == ()
    assert [issue.code for issue in issues] == ["unsupported_representation_coding"]


@pytest.mark.parametrize("second_source", [None, "Question 2 in another edition"])
def test_per_column_operation_and_attribution_do_not_borrow_sibling_facts(
    tmp_path, second_source
):
    setup = _column_storage_setup("integer", "0", "integer", "0")
    records = setup[0]
    # Rebuild target guards after changing the original source fixture.
    records = tuple(
        r.model_copy(
            update={
                "fields": r.fields.model_copy(
                    update={
                        "operational_definition": value_field(
                            "First operation" if i == 0 else "Second operation"
                        ),
                        "source_attribution": value_field("Question 1")
                        if i == 0
                        else value_field(second_source)
                        if second_source
                        else None,
                    }
                )
            }
        )
        for i, r in enumerate(records)
    )
    setup = _setup(records)
    case = setup[2].model_copy(
        update={
            "decision": setup[2].decision.model_copy(
                update={"column_metadata": "per_column"}
            )
        }
    )
    formed, proof = _form((*setup[:2], case, *setup[3:]))
    assert not proof.diagnostics and not formed.diagnostics
    variable = formed.variable
    assert variable.states[0].operational_definition is None
    assert variable.states[0].source_register_text is None
    assert {
        a.delivery_column_name: (
            a.windows[0].operational_definition,
            a.windows[0].source_register_text,
        )
        for a in variable.aliases
    } == {
        "First": ("First operation", "Question 1"),
        "Second": ("Second operation", second_source),
    }
    check_delivery_coverage((variable,), formed.coverage, withheld={})
    write_resolved_catalog((variable,), tmp_path / "reg_meta.db", manifest={})
    with closing(open_built_db(tmp_path / "reg_meta.db")) as conn:
        assert {
            tuple(row)
            for row in conn.execute(
                "SELECT delivery_column_name, operational_definition, source_register_text FROM variable_alias_window"
            )
        } == {
            ("First", "First operation", "Question 1"),
            ("Second", "Second operation", second_source),
        }
    from reg_meta_build.source_curation import evaluate_cases

    changed = (
        records[0].model_copy(
            update={
                "fields": records[0].fields.model_copy(
                    update={"source_attribution": None}
                )
            }
        ),
        records[1],
    )
    assert evaluate_cases((case,), changed)[0].status != "applicable"
    aliases = tuple(
        a.model_copy(
            update={
                "windows": (
                    a.windows[0].model_copy(
                        update={"source_register_text": "Question 1"}
                    ),
                )
            }
        )
        if a.delivery_column_name == "Second"
        else a
        for a in variable.aliases
    )
    with pytest.raises(ValueError, match="supported delivery facts changed"):
        check_delivery_coverage(
            (variable.model_copy(update={"aliases": aliases}),),
            formed.coverage,
            withheld={},
        )


def _unit_fixture(*, overlap=False):
    from reg_meta_build.source_curation import (
        DeliveryMetadataColumn,
        DeliveryMetadataDecision,
    )

    revision = SourceRevision.create(
        dataset="fixture",
        publisher="SCB",
        purpose="test",
        upstream_revision="1",
        artifact_path="units.csv",
        artifact_size=1,
        artifact_sha256="a" * 64,
    )
    header = REGISTERINFORMATION_HEADER.split("|")
    records = []
    for index, (year, unit) in enumerate(
        (("2020", "100-tal kronor"), ("2020" if overlap else "2021", "Kronor (SEK)"))
    ):
        values = _var_row(
            colname="VALUE",
            var_id=1,
            cvid=100 + index,
            varname="Income " + year,
            vardef="Source supplied income definition",
            unit=unit,
            year=year,
        ).split("|")
        records.append(
            clean_scb_row(
                header,
                index + 1,
                {
                    name: (True, value, value)
                    for name, value in zip(header, values, strict=True)
                },
                revision,
            ).record
        )
    records = tuple(records)
    first = source_occurrence(records[0])
    assert first.variable_key is not None and first.variant_key is not None
    expectations = capture_expectations(
        records, fields=tuple(SourceFields.model_fields), parents=True, coding=True
    )
    case = CurationCase(
        case_id="literal-delivery-units",
        targets=expectations,
        peer_guards=(
            PeerGuard(
                guard_id="full-unit-family",
                source=records[0].source,
                native=NativeCoordinates(
                    register_id=1, register_variant_id=10, variable_id=1
                ),
                expected_members=tuple(e.ref for e in expectations),
            ),
        ),
        decision=DeliveryMetadataDecision(
            fields=("name",),
            reviewed=True,
            variable_key=first.variable_key,
            columns=(
                DeliveryMetadataColumn(
                    variant_key=first.variant_key,
                    column="VALUE",
                    valid_from="2020-01-01",
                    valid_to="2020-12-31" if overlap else "2021-12-31",
                    expected_codings=(),
                ),
            ),
            reason="Retain literal source units",
            provenance="Source encoding only; no value conversion",
        ),
    )
    return (
        records,
        case,
        {first.variant_key: ResolvedVariant(slug="people", name="People")},
        {first.column_key: resolve_code_membership(())},
    )


def _form_units(fixture, *, cases=None, records=None):
    original, case, variants, coding = fixture
    records = original if records is None else records
    resolution = resolve_representation_cases(
        records, (case,) if cases is None else cases, coding=coding
    )
    formed = form_native_variable(
        tuple(source_occurrence(record) for record in records),
        register=ResolvedRegister(provider="scb", slug="example", name="Example"),
        variants=variants,
        slug="income",
        provider_key="1",
        flags=SourceFields(
            sensitivity=value_field(False), identifier=value_field(False)
        ),
        coding=coding,
        representations=resolution.cases,
    )
    return resolution, formed


def test_checked_delivery_metadata_retain_literals_and_coverage(tmp_path):
    fixture = _unit_fixture()
    records, _, _, _ = fixture
    _, baseline = _form_units(fixture, cases=())
    assert any(
        issue.code == "conflicting_variable_fact" and issue.fields == ("name",)
        for issue in baseline.diagnostics
    )
    resolution, formed = _form_units(fixture)
    assert not resolution.diagnostics
    assert formed.variable is not None and formed.variable.measurement_unit is None
    assert not any(issue.severity == "error" for issue in formed.diagnostics)
    assert {state.measurement_unit for state in formed.variable.states} == {
        "100-tal kronor",
        "Kronor (SEK)",
    }
    assert tuple(record.fields.measurement_unit.value for record in records) == (
        "100-tal kronor",
        "Kronor (SEK)",
    )
    check_delivery_coverage((formed.variable,), formed.coverage, withheld={})
    wrong = formed.variable.model_copy(
        update={
            "states": tuple(
                state.model_copy(update={"measurement_unit": "Kronor (SEK)"})
                for state in formed.variable.states
            )
        }
    )
    with pytest.raises(ValueError, match="literal delivery unit changed"):
        check_delivery_coverage((wrong,), formed.coverage, withheld={})
    write_resolved_catalog((formed.variable,), tmp_path / "reg_meta.db", manifest={})
    with closing(open_built_db(tmp_path / "reg_meta.db")) as conn:
        assert {
            row[0]
            for row in conn.execute("SELECT measurement_unit FROM variable_state")
        } == {"100-tal kronor", "Kronor (SEK)"}


@pytest.mark.parametrize("drift", ["missing", "changed", "new", "partial", "owner"])
def test_checked_delivery_metadata_fail_closed(drift):
    fixture = _unit_fixture()
    records, case, variants, coding = fixture
    if drift == "missing":
        records = records[:-1]
    elif drift == "changed":
        records = (
            records[0],
            records[1].model_copy(
                update={
                    "fields": records[1].fields.model_copy(
                        update={"measurement_unit": value_field("other unit")}
                    )
                }
            ),
        )
    elif drift == "new":
        records = (
            *records,
            records[1].model_copy(
                update={
                    "locators": (
                        records[1]
                        .locators[0]
                        .model_copy(
                            update={
                                "semantic_record_key": (
                                    *records[1].locators[0].semantic_record_key,
                                    "new-peer",
                                )
                            }
                        ),
                    )
                }
            ),
        )
    elif drift == "partial":
        case = case.model_copy(update={"targets": case.targets[:-1]})
    elif drift == "owner":
        case = case.model_copy(
            update={
                "decision": case.decision.model_copy(
                    update={"variable_key": ("different", "owner")}
                )
            }
        )
    fixture = (fixture[0], case, variants, coding)
    if drift == "owner":
        with pytest.raises(ValueError, match="unconverted column coding"):
            _form_units(fixture, records=records)
        return
    resolution, formed = _form_units(fixture, records=records)
    if drift in {"missing", "changed", "new"}:
        assert resolution.diagnostics and not resolution.cases
    else:
        assert any(
            issue.code == "conflicting_variable_fact" for issue in formed.diagnostics
        )


def test_checked_delivery_metadata_do_not_resolve_same_column_conflicts():
    _, formed = _form_units(_unit_fixture(overlap=True))
    assert any(
        issue.severity == "error" and "measurement_unit" in issue.fields
        for issue in formed.diagnostics
    )


def test_checked_delivery_metadata_reject_changed_coding():
    fixture = _unit_fixture()
    records, case, variants, coding = fixture
    key = next(iter(coding))
    changed_coding = {key: resolve_code_membership((_claim("new supplied list", "1"),))}
    resolution, _ = _form_units((records, case, variants, changed_coding))
    assert not resolution.cases
    assert any(
        issue.code == "stale_representation_coding" for issue in resolution.diagnostics
    )


@pytest.mark.parametrize("name_permission", [False, True])
def test_checked_delivery_text_keeps_source_names_without_common_winner(
    name_permission,
):
    records, case, variants, coding = _unit_fixture()
    records = tuple(
        record.model_copy(
            update={
                "fields": record.fields.model_copy(
                    update={
                        "name": value_field("Visit" if index == 0 else "Admission"),
                        "description": value_field(
                            "Visited facility" if index == 0 else "Admitting facility"
                        ),
                        "measurement_unit": value_field("Kronor (SEK)"),
                    }
                )
            }
        )
        for index, record in enumerate(records)
    )
    decision = case.decision.model_copy(
        update={
            "fields": ("name", "description") if name_permission else ("description",)
        }
    )
    case = case.model_copy(
        update={
            "decision": decision,
            "targets": capture_expectations(
                records,
                fields=tuple(SourceFields.model_fields),
                parents=True,
                coding=True,
            ),
        }
    )
    resolution, formed = _form_units((records, case, variants, coding))
    assert resolution.cases == (case,)
    if not name_permission:
        assert formed.variable is None
        assert any(d.code == "unresolved_variable_name" for d in formed.diagnostics)
        return
    variable = formed.variable
    assert variable is not None
    assert variable.name is None and variable.description is None
    assert [(s.name, s.description) for s in variable.states] == [
        ("Visit", "Visited facility"),
        ("Admission", "Admitting facility"),
    ]
    assert not any(d.severity == "error" for d in formed.diagnostics)
    check_delivery_coverage((variable,), formed.coverage, withheld={})
    changed = variable.model_copy(
        update={
            "states": (
                variable.states[0].model_copy(
                    update={"description": "Borrowed sibling description"}
                ),
                *variable.states[1:],
            )
        }
    )
    with pytest.raises(ValueError, match="literal delivery description changed"):
        check_delivery_coverage((changed,), formed.coverage, withheld={})
    unknown = records[0].model_copy(
        update={"fields": records[0].fields.model_copy(update={"name": None})}
    )
    unknown_case = case.model_copy(
        update={
            "targets": capture_expectations(
                (unknown, records[1]),
                fields=tuple(SourceFields.model_fields),
                parents=True,
                coding=True,
            )
        }
    )
    with pytest.raises(ValueError, match="positive permitted source facts"):
        resolve_representation_cases(
            (unknown, records[1]), (unknown_case,), coding=coding
        )


def test_delivery_metadata_permissions_do_not_cover_an_enlarged_effective_scope():
    records, case, variants, coding = _unit_fixture()
    column = case.decision.columns[0].model_copy(update={"valid_to": "2020-12-31"})
    case = case.model_copy(
        update={"decision": case.decision.model_copy(update={"columns": (column,)})}
    )
    resolution, formed = _form_units((records, case, variants, coding))
    assert resolution.cases == (case,)
    assert any(
        d.code == "conflicting_variable_fact" and d.fields == ("name",)
        for d in formed.diagnostics
    )


def test_delivery_metadata_requires_unique_explicit_field_permissions():
    _, case, _, _ = _unit_fixture()
    decision = type(case.decision)
    for fields in ((), ("name", "name"), ("data_type",)):
        with pytest.raises(ValueError):
            decision.model_validate({**case.decision.model_dump(), "fields": fields})


def test_delivery_metadata_exact_open_scope_is_not_a_finite_window_exemption():
    from reg_meta_build.source_curation import DeliveryMetadataColumn, SourceEvidence

    original, case, variants, coding = _unit_fixture()
    scope = TemporalScope(
        kind="intervals", intervals=(ScopeInterval(start="2020", end=None),)
    )
    record = original[0].model_copy(
        update={
            "edition_scope": scope,
            "edition_period_scope": TemporalScope(kind="not_applicable"),
        }
    )
    column = DeliveryMetadataColumn(
        variant_key=case.decision.columns[0].variant_key,
        column="VALUE",
        valid_from="2020-01-01",
        valid_to="9999-12-31",
        expected_codings=(),
        source_scope=scope,
    )
    case = case.model_copy(
        update={
            "targets": capture_expectations(
                (record,),
                fields=tuple(SourceFields.model_fields),
                parents=True,
                coding=True,
            ),
            "peer_guards": tuple(
                g.model_copy(update={"expected_members": (record_ref(record),)})
                for g in case.peer_guards
            ),
            "decision": case.decision.model_copy(update={"columns": (column,)}),
        }
    )
    resolution, formed = _form_units(((record,), case, variants, coding))
    assert resolution.cases == (case,) and formed.variable is not None
    assert formed.variable.states[0].valid_to == "9999-12-31"
    assert record.edition_scope.intervals[0].end is None
    for bad in (
        {"source_scope": None},
        {"valid_to": "2021-12-31"},
        {"source_scope": TemporalScope(kind="unknown", label="unknown supplied scope")},
        {"source_scope": TemporalScope(kind="year_independent")},
    ):
        with pytest.raises(ValueError):
            DeliveryMetadataColumn.model_validate({**column.model_dump(), **bad})
    # Equal derived dates still do not imply the same supplied source scope.
    other_scope = TemporalScope(
        kind="intervals", intervals=(ScopeInterval(start="2020-01-01", end=None),)
    )
    changed_column = column.model_copy(update={"source_scope": other_scope})
    changed_case = case.model_copy(
        update={
            "decision": case.decision.model_copy(update={"columns": (changed_column,)})
        }
    )
    rejected = resolve_representation_cases((record,), (changed_case,), coding=coding)
    assert not rejected.cases
    assert [d.code for d in rejected.diagnostics] == ["stale_delivery_metadata_scope"]
    for changed_scope in (
        TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start="2019", end=None),)
        ),
        TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start="2020", end="2021"),)
        ),
    ):
        effective = replace(source_occurrence(record), edition_scope=changed_scope)
        evidence = SourceEvidence((record,), effective_occurrences=(effective,))
        rejected = resolve_representation_cases(evidence, (case,), coding=coding)
        assert not rejected.cases
        assert any(
            d.code == "stale_delivery_metadata_scope" for d in rejected.diagnostics
        )


@pytest.mark.parametrize("changed_field", ["name", "measurement_unit"])
def test_delivery_metadata_keeps_unrelated_checked_field_corrections(changed_field):
    from reg_meta_build.source_curation import SourceEvidence

    records, metadata, variants, coding = _unit_fixture()
    correction = metadata.model_copy(
        update={
            "case_id": "checked-source-field",
            "decision": OccurrenceCorrectionDecision(
                reviewed=True,
                effects=tuple(
                    CheckedFieldChange(
                        ref=record_ref(record),
                        replacement=FieldExpectation(
                            name=changed_field,
                            status="value",
                            value="Checked source wording",
                        ),
                    )
                    for record in records[:1]
                ),
                reason="Independent supplied source correction",
                provenance="exact fixture",
            ),
        }
    )
    corrected = apply_occurrence_cases(records, (correction,))
    assert not corrected.diagnostics
    evidence = SourceEvidence(records, effective_occurrences=corrected.occurrences)
    representations = resolve_representation_cases(evidence, (metadata,), coding=coding)
    formed = form_native_variable(
        corrected.occurrences,
        register=ResolvedRegister(provider="scb", slug="example", name="Example"),
        variants=variants,
        slug="income",
        provider_key="1",
        flags=SourceFields(
            sensitivity=value_field(False), identifier=value_field(False)
        ),
        coding=coding,
        representations=representations.cases,
    )
    unit_conflicts = [
        d
        for d in formed.diagnostics
        if d.code == "conflicting_variable_fact" and d.fields == ("measurement_unit",)
    ]
    assert not unit_conflicts
    assert any(d.code == "delivery_units_vary" for d in formed.diagnostics)
    if changed_field == "name":
        # The checked text permission cannot waive an independent field correction.
        assert any(
            d.code == "conflicting_variable_fact" and d.fields == ("name",)
            for d in formed.diagnostics
        )
        assert corrected.occurrences[0].fields.name is not None
        assert corrected.occurrences[0].fields.name.value == "Checked source wording"
        assert corrected.occurrences[0].source_records == (records[0],)
