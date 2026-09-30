"""Accepted parallel columns share metadata without inheriting a sibling's facts."""

from __future__ import annotations

from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, _var_row
from pydantic import ValidationError
from reg_meta.catalog import Catalog
from reg_meta.db import open_db
from reg_meta.source_evidence import SourceRevision
from reg_meta_build.catalog_dependencies import check_delivery_coverage
from reg_meta_build.resolved_catalog import (
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
            "definition",
            "data_type",
            "data_length",
            "operational_definition",
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
                    "storage_metadata": "per_column",
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
        w.storage_metadata == "per_column" for a in variable.aliases for w in a.windows
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
    with closing(Catalog.open(tmp_path)) as catalog:
        assert {
            state.delivery_column_name: (state.data_type, state.data_length)
            for state in catalog.resolve_at("scb/example/income", "2020")
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
    with closing(Catalog.open(tmp_path)) as catalog:
        states = catalog.resolve_at("scb/example/income", "2020")
        assert {
            (s.delivery_column_name, s.valid_from, s.valid_to, s.data_length)
            for s in states
        } == {
            (col, start, end, width)
            for col, width in (("First", "200"), ("Second", "18"))
            for start, end in (
                ("2020-01-01", "2020-06-30"),
                ("2020-07-01", "2020-12-31"),
            )
        }
        assert {
            s.delivery_column_name
            for s in catalog.resolve_at("scb/example/income", "2020-10")
        } == {"First", "Second"}
    from reg_meta.inventory_check import _expanded_columns

    assert _expanded_columns(
        [(s.valid_from, s.valid_to, s.delivery_column_name) for s in variable.states],
        [
            (
                a.delivery_column_name,
                w.valid_from,
                w.valid_to,
                w.provenance,
                w.storage_metadata,
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
            "decision": case.decision.model_copy(update={"storage_metadata": "shared"}),
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
    with closing(open_db(output)) as conn:
        assert conn.execute("SELECT count(*) FROM variable_state").fetchone()[0] == 1
        assert (
            conn.execute("SELECT count(*) FROM variable_alias_window").fetchone()[0]
            == 2
        )
    catalog = Catalog.open(tmp_path)
    try:
        assert [
            s.delivery_column_name
            for s in catalog.resolve_at("scb/example/income", "2020-03")
        ] == ["First"]
        assert [
            s.delivery_column_name
            for s in catalog.resolve_at("scb/example/income", "2020-09")
        ] == ["Second"]
        assert catalog.resolve_at("scb/example/income", "2021-03") == []
    finally:
        catalog.close()
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
    catalog = Catalog.open(tmp_path)
    try:
        assert [
            s.delivery_column_name
            for s in catalog.resolve_at("scb/example/income", "2020-10")
        ] == ["Second"]
        september = catalog.resolve_at("scb/example/income", "2020-09")
        assert len(september) == 2
        assert {s.delivery_column_name for s in september} == {"Second"}
        assert [(s.valid_from, s.valid_to) for s in september] == [
            ("2020-07-01", "2020-09-14"),
            ("2020-09-15", "2020-12-31"),
        ]
    finally:
        catalog.close()


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
    with closing(open_db(output)) as conn:
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
        states[0] = states[0].model_copy(update={"classification": "scb/sni2007"})
    else:
        states.append(states[0].model_copy(update={"value_set": states[1].value_set}))
    result, aliases, issues, _, _ = form_representations(
        states, (setup[2],), variable_key=KEY, variants=setup[3], subject="fixture"
    )
    assert result == [] and aliases == ()
    assert [issue.code for issue in issues] == ["unsupported_representation_coding"]
