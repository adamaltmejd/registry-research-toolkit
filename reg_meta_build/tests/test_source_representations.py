"""Accepted parallel columns share metadata without inheriting a sibling's facts."""

from __future__ import annotations

from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, _var_row
from pydantic import ValidationError
from reg_meta.catalog import Catalog
from reg_meta.db import open_db
from reg_meta_build.convert_errata import capture_expectations
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
from reg_meta_build.source_coding_choices import coding_expectations
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
)
from reg_meta_build.source_effects import apply_occurrence_cases, record_ref
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    NativeCoordinates,
    ScopeInterval,
    SourceFields,
    SourceRevision,
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
                    expected_codings=coding_expectations(
                        coding[column_identity(KEY, variant_key, column)].claims,
                        "2020-01-01",
                        "2020-12-31",
                    ),
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


def test_coding_disagreement_is_withheld_and_new_coding_invalidates_old_decision() -> (
    None
):
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
    assert (
        proof.cases == ()
        and proof.diagnostics[0].code == "representation_coding_changed"
    )
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
    result, aliases, issues = form_representations(
        [state], (setup[2],), variable_key=KEY, variants=setup[3], subject="fixture"
    )
    assert result == [] and aliases == ()
    assert issues[0].code == "missing_representation_metadata"


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
                expected_codings=(),
            ),
            ColumnRepresentation(
                column="Second",
                valid_from="2020-10-01",
                valid_to="2020-12-31",
                expected_codings=(),
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
