"""Pooled and co-delivered parallel columns and case aliases compile with exact intersections and guards."""

from __future__ import annotations

from dataclasses import replace

import pytest
from _curation_compile_support import (
    pooled_parallel_fixture as _pooled_parallel_fixture,
)
from reg_meta.errors import RegMetaError
from reg_meta_build.curation_tree import (
    load_register_files,
)
from reg_meta_build.resolved_catalog import ResolvedRegister, ResolvedVariant
from reg_meta_build.source_coding import (
    CodeListClaim,
    CodeMembershipClaim,
    resolve_code_membership,
)
from reg_meta_build.source_coordinates import (
    column_identity,
)
from reg_meta_build.source_curation import (
    capture_expectations,
)
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    CodeSetReference,
    SourceCoordinate,
    SourceFields,
    TemporalScope,
    value_field,
)


def _compile_pooled_parallel(path, records, naming):
    from reg_meta_build.curation_compile import compile_parallel_representations

    (register,) = load_register_files(path.parents[2])
    return compile_parallel_representations(register, records, naming)


def _parallel_coding(case):
    from reg_meta_build.source_curation import RepresentationDecision

    decision = case.decision
    assert isinstance(decision, RepresentationDecision)
    return {
        column_identity(
            decision.variable_key, decision.variant_key, column.column
        ): resolve_code_membership(())
        for column in decision.columns
    }


@pytest.mark.parametrize("defect", [None, "definition", "missing", "new_member"])
def test_case_aliases_keep_distinct_members_and_refuse_source_drift(tmp_path, defect):
    from reg_meta_build.source_representations import resolve_representation_cases

    path, records, naming = _pooled_parallel_fixture(tmp_path, co_delivered=True)
    path.write_text(
        path.read_text()
        .replace('column = "Second"', 'column = "first"')
        .replace("co_delivered = true", "case_aliases = true")
    )
    peer = records[1].model_copy(
        update={
            "fields": records[1].fields.model_copy(
                update={"column_name": value_field("first")}
            ),
            "locators": tuple(
                locator.model_copy(
                    update={
                        "semantic_record_key": (
                            *locator.semantic_record_key,
                            "independent-member",
                        )
                    }
                )
                for locator in records[1].locators
            ),
        }
    )
    originals = (records[0], peer)
    # Naming membership covers both distinct source members, without claiming paired delivery.
    naming = tuple(
        name.model_copy(
            update={
                "target": name.target.model_copy(
                    update={
                        "expectations": capture_expectations(
                            originals, fields=("column_name",), coding=True
                        )
                    }
                )
            }
        )
        if name.target.kind == "variable"
        else name
        for name in naming
    )
    cases, diagnostics = _compile_pooled_parallel(path, originals, naming)
    assert not diagnostics and len(cases) == 1
    proof = resolve_representation_cases(
        originals, cases, coding=_parallel_coding(cases[0])
    )
    assert not proof.diagnostics
    changed = originals
    if defect == "definition":
        changed = (
            originals[0],
            peer.model_copy(
                update={
                    "fields": peer.fields.model_copy(
                        update={"definition": value_field("different quantity")}
                    )
                }
            ),
        )
    elif defect == "missing":
        changed = originals[:1]
    elif defect == "new_member":
        changed = (
            *originals,
            peer.model_copy(
                update={
                    "locators": tuple(
                        locator.model_copy(
                            update={
                                "semantic_record_key": (
                                    *locator.semantic_record_key,
                                    "extra",
                                )
                            }
                        )
                        for locator in peer.locators
                    )
                }
            ),
        )
    if defect:
        assert resolve_representation_cases(
            changed, cases, coding=_parallel_coding(cases[0])
        ).diagnostics
        if defect != "new_member":
            assert _compile_pooled_parallel(path, changed, naming)[1]
    path.write_text(
        path.read_text().replace("case_aliases = true", "co_delivered = true")
    )
    assert _compile_pooled_parallel(path, originals, naming)[1]


def test_co_delivered_parallel_requires_explicit_per_column_opt_in(tmp_path):
    from reg_meta_build.source_representations import resolve_representation_cases

    path, records, naming = _pooled_parallel_fixture(tmp_path, co_delivered=True)
    records = (
        records[0],
        records[1].model_copy(
            update={
                "fields": records[1].fields.model_copy(
                    update={"data_type": value_field("text")}
                )
            }
        ),
    )
    cases, diagnostics = _compile_pooled_parallel(path, records, naming)
    assert diagnostics == () and len(cases) == 1
    proof = resolve_representation_cases(
        records, cases, coding=_parallel_coding(cases[0])
    )
    assert proof.diagnostics == ()
    path.write_text(
        path.read_text().replace("co_delivered = true", "co_delivered = false")
    )
    assert _compile_pooled_parallel(path, records, naming)[0] == ()
    path.write_text(
        path.read_text()
        .replace('column_metadata = "per_column"', 'column_metadata = "shared"')
        .replace("co_delivered = false", "co_delivered = true")
    )
    with pytest.raises(RegMetaError) as failure:
        _compile_pooled_parallel(path, records, naming)
    assert "per-column metadata" in failure.value.message


@pytest.mark.parametrize(
    "defect", ["missing", "definition", "foreign_member", "identifier"]
)
def test_co_delivered_parallel_refuses_unsupported_common_quantity(tmp_path, defect):
    path, records, naming = _pooled_parallel_fixture(tmp_path, co_delivered=True)
    if defect == "missing":
        records = records[:1]
    elif defect == "definition":
        records = (
            records[0],
            records[1].model_copy(
                update={
                    "fields": records[1].fields.model_copy(
                        update={"definition": value_field("Different quantity")}
                    )
                }
            ),
        )
    elif defect == "foreign_member":
        records = (
            records[0],
            records[1].model_copy(
                update={
                    "locators": tuple(
                        locator.model_copy(
                            update={
                                "semantic_record_key": (
                                    *locator.semantic_record_key[:-1],
                                    "member:999",
                                )
                            }
                        )
                        for locator in records[1].locators
                    )
                }
            ),
        )
    else:
        records = tuple(
            record.model_copy(
                update={
                    "fields": record.fields.model_copy(
                        update={"identifier": value_field(index == 1)}
                    )
                }
            )
            for index, record in enumerate(records)
        )
    cases, diagnostics = _compile_pooled_parallel(path, records, naming)
    assert cases == ()
    assert diagnostics and all(d.severity == "error" for d in diagnostics)


@pytest.mark.parametrize("column_metadata", ["shared", "per_column"])
def test_pooled_parallel_compile_keeps_original_bounds_and_exact_intersection(
    tmp_path,
    column_metadata,
):
    from reg_meta_build.source_curation import RepresentationDecision
    from reg_meta_build.source_representations import resolve_representation_cases

    path, records, naming = _pooled_parallel_fixture(tmp_path)
    if column_metadata == "per_column":
        path.write_text(
            path.read_text().replace(
                'variant = "1.10"', 'variant = "1.10"\ncolumn_metadata = "per_column"'
            )
        )
    before = tuple(r.model_dump(mode="json") for r in records)
    cases, diagnostics = _compile_pooled_parallel(path, records, naming)
    assert diagnostics == () and len(cases) == 1
    case = cases[0]
    decision = case.decision
    assert isinstance(decision, RepresentationDecision)
    assert (decision.valid_from, decision.valid_to) == ("2022-01-01", "2022-12-31")
    assert [(c.column, c.valid_from, c.valid_to) for c in decision.columns] == [
        ("First", "2022-01-01", "2022-12-31"),
        ("Second", "2022-01-01", "2022-12-31"),
    ]
    assert decision.column_metadata == column_metadata
    proof = resolve_representation_cases(records, cases, coding=_parallel_coding(case))
    assert proof.cases == cases and proof.diagnostics == ()
    assert tuple(r.model_dump(mode="json") for r in records) == before
    assert _compile_pooled_parallel(path, records[::-1], naming) == (cases, diagnostics)


@pytest.mark.parametrize(
    "original,replacement",
    [
        ('valid_from = "2022-01-01"', 'valid_from = "2022-07-01"'),
        ('valid_to = "2022-12-31"', 'valid_to = "2022-06-30"'),
    ],
)
def test_pooled_parallel_partial_intersection_is_rejected(
    tmp_path, original, replacement
):
    path, records, naming = _pooled_parallel_fixture(tmp_path)
    # Only the shared window changes; both authored original windows stay exact.
    path.write_text(path.read_text().replace(original, replacement, 1))
    with pytest.raises(RegMetaError) as failure:
        _compile_pooled_parallel(path, records, naming)
    assert "source-window intersection" in failure.value.message


@pytest.mark.parametrize(
    "original,replacement",
    [
        ('variable = "1.1.income"', 'variable = "1.2.income"'),
        ('variant = "1.10"', 'variant = "1.11"'),
        ('column = "First"', 'column = "Absent"'),
        ('source_editions = ["2020-2022"]', 'source_editions = ["2020-2023"]'),
        ('valid_from = "2020-01-01"', 'valid_from = "2021-01-01"'),
        ('valid_to = "2024-12-31"', 'valid_to = "2025-12-31"'),
    ],
)
def test_pooled_parallel_authored_source_mismatch_withholds_case(
    tmp_path, original, replacement
):
    path, records, naming = _pooled_parallel_fixture(tmp_path)
    path.write_text(path.read_text().replace(original, replacement))
    cases, diagnostics = _compile_pooled_parallel(path, records, naming)
    assert cases == ()
    assert diagnostics and all(d.severity == "error" for d in diagnostics)


@pytest.mark.parametrize(
    "defect", ["missing", "new", "facts", "source_bounds", "coding"]
)
def test_pooled_parallel_original_membership_and_facts_remain_guarded(tmp_path, defect):
    from reg_meta_build.source_representations import resolve_representation_cases

    path, records, naming = _pooled_parallel_fixture(tmp_path)
    (case,), diagnostics = _compile_pooled_parallel(path, records, naming)
    assert diagnostics == ()
    changed = records
    if defect == "missing":
        changed = records[:1]
    elif defect == "new":
        peer = records[0].model_copy(
            update={
                "fields": records[0].fields.model_copy(
                    update={"column_name": value_field("Third")}
                ),
                "subject": records[0].subject.model_copy(
                    update={"member": SourceCoordinate(status="value", native_id=999)}
                ),
                "record_id": "new-peer",
            }
        )
        changed = (*records, peer)
    elif defect == "facts":
        changed = (
            records[0].model_copy(
                update={
                    "fields": records[0].fields.model_copy(
                        update={"operational_definition": value_field("Changed")}
                    )
                }
            ),
            records[1],
        )
    elif defect == "coding":
        changed = (
            records[0].model_copy(
                update={
                    "code_set_references": (
                        CodeSetReference(
                            reference_id="changed",
                            content_sha256="a" * 64,
                            physical_locator="codes.csv:1",
                        ),
                    )
                }
            ),
            records[1],
        )
    else:
        changed = (
            records[0].model_copy(
                update={
                    "edition_period_scope": TemporalScope(
                        kind="pooled",
                        label="2020-2023",
                        pooled_start="2020-01-01",
                        pooled_end="2023-12-31",
                    )
                }
            ),
            records[1],
        )
    proof = resolve_representation_cases(
        changed, (case,), coding=_parallel_coding(case)
    )
    assert proof.cases == ()
    assert proof.evaluations[0].status == "stale" and proof.diagnostics


@pytest.mark.parametrize("conflict", [None, "fact", "coding", "explicit_annual"])
def test_pooled_parallel_reconciliation_preserves_outer_windows_and_conflicts(
    tmp_path, conflict
):
    from reg_meta_build.catalog_dependencies import check_delivery_coverage
    from reg_meta_build.source_curation import RepresentationDecision
    from reg_meta_build.source_records import ScopeInterval
    from reg_meta_build.source_representations import resolve_representation_cases

    path, records, naming = _pooled_parallel_fixture(tmp_path)
    if conflict == "explicit_annual":
        records = (
            records[0],
            records[1].model_copy(
                update={
                    "edition_scope": TemporalScope(
                        kind="intervals",
                        intervals=(ScopeInterval(start="2022", end="2024"),),
                    ),
                    "edition_period_scope": TemporalScope(
                        kind="intervals",
                        intervals=(
                            ScopeInterval(start="2022-01-01", end="2024-12-31"),
                        ),
                    ),
                }
            ),
        )
    elif conflict == "fact":
        records = (
            records[0],
            records[1].model_copy(
                update={
                    "fields": records[1].fields.model_copy(
                        update={"data_type": value_field("text")}
                    )
                }
            ),
        )
    (case,), diagnostics = _compile_pooled_parallel(path, records, naming)
    assert diagnostics == ()
    decision = case.decision
    assert isinstance(decision, RepresentationDecision)
    coding = _parallel_coding(case)
    if conflict == "coding":
        for column, code in (("First", "01"), ("Second", "02")):
            coding[
                column_identity(decision.variable_key, decision.variant_key, column)
            ] = resolve_code_membership(
                (
                    CodeListClaim(
                        column,
                        TemporalScope(
                            kind="intervals",
                            intervals=(
                                ScopeInterval(start="2020-01-01", end="2024-12-31"),
                            ),
                        ),
                        (
                            CodeMembershipClaim(
                                code,
                                "Label",
                                TemporalScope(kind="year_independent"),
                            ),
                        ),
                    ),
                )
            )
    proof = resolve_representation_cases(records, (case,), coding=coding)
    assert proof.cases == (case,) and proof.diagnostics == ()
    occurrences = tuple(
        replace(
            source_occurrence(record),
            variable_key=decision.variable_key,
            identity_checked=True,
        )
        for record in records
    )
    formed = form_native_variable(
        occurrences,
        register=ResolvedRegister(provider="scb", slug="sample", name="Sample"),
        variants={decision.variant_key: ResolvedVariant(slug="people", name="People")},
        slug="income",
        provider_key="family",
        flags=SourceFields(
            sensitivity=value_field(False), identifier=value_field(False)
        ),
        coding=coding,
        representations=proof.cases,
    )
    assert formed.variable is not None
    assert [(s.valid_from, s.valid_to) for s in formed.variable.states] == [
        ("2020-01-01", "2021-12-31"),
        ("2022-01-01", "2022-12-31"),
        ("2023-01-01", "2024-12-31"),
    ]
    assert formed.variable.states[0].delivery_column_name == "First"
    assert formed.variable.states[2].delivery_column_name == "Second"
    check_delivery_coverage((formed.variable,), formed.coverage, withheld={})
    shared = formed.variable.states[1]
    assert [state.pooled for state in formed.variable.states] == (
        [True, False, False] if conflict == "explicit_annual" else [True, True, True]
    )
    assert shared.provenance is not None and case.case_id in shared.provenance
    witnesses = tuple(
        original
        for occurrence in formed.occurrences
        for original in occurrence.source_records
    )
    assert {r.record_id for r in witnesses} == {r.record_id for r in records}
    assert {r.record_id: r.edition_period_scope for r in witnesses} == {
        r.record_id: r.edition_period_scope for r in records
    }
    if conflict == "fact":
        assert shared.data_type is None
        assert ("conflicting_representation_fact", ("data_type",)) in {
            (d.code, d.fields) for d in formed.diagnostics
        }
        assert formed.variable.states[0].data_type == "integer"
        assert formed.variable.states[2].data_type == "text"
    elif conflict == "coding":
        assert shared.value_set is None
        assert any(
            d.code == "conflicting_representation_coding" for d in formed.diagnostics
        )
        assert [
            state.value_set.members
            for state in (formed.variable.states[0], formed.variable.states[2])
        ] == [(("01", "Label"),), (("02", "Label"),)]
        assert all(
            claim.coding_claim is None
            for claim in formed.coverage
            if claim.valid_from == "2022-01-01"
        )
        changed_outer = formed.variable.model_copy(
            update={
                "states": (
                    formed.variable.states[0].model_copy(update={"value_set": None}),
                    *formed.variable.states[1:],
                )
            }
        )
        with pytest.raises(ValueError, match="claimed coding"):
            check_delivery_coverage((changed_outer,), formed.coverage, withheld={})
    else:
        assert formed.diagnostics == ()
    assert [
        (r.edition_period_scope.pooled_start, r.edition_period_scope.pooled_end)
        for r in records
    ] == [
        ("2020-01-01", "2022-12-31"),
        (None, None) if conflict == "explicit_annual" else ("2022-01-01", "2024-12-31"),
    ]
    assert {
        (a.delivery_column_name, w.valid_from, w.valid_to)
        for a in formed.variable.aliases
        for w in a.windows
    } == {
        ("First", "2022-01-01", "2022-12-31"),
        ("Second", "2022-01-01", "2022-12-31"),
    }
