"""Complete-scope composition: acknowledgements turn exact errors into counted warnings and stale ones fail."""

from __future__ import annotations

import pytest
from _source_effects_support import effect_case, effect_field
from _source_scope_support import acknowledge, names, record, resolve
from reg_meta.source_evidence import SourceField, SourceRevision
from reg_meta_build.source_coordinates import (
    native_variable_key,
    source_register_key,
)
from reg_meta_build.source_curation import (
    ResolutionDiagnostic,
)
from reg_meta_build.source_effects import (
    record_ref,
)
from reg_meta_build.source_records import (
    SourceFields,
    SourceRecord,
    value_field,
)
from reg_meta_build.source_scope import resolve_source_scope
from reg_meta_build.source_support import SourceSupportBindings


def test_an_acknowledged_error_becomes_a_counted_warning_and_stays_withheld():
    item = record()
    key = native_variable_key(item)
    (issue,) = resolve((item,), provider_keys={key: None}).diagnostics
    case = acknowledge(issue, item)
    result = resolve((item,), cases=(case,), provider_keys={key: None})
    warning = issue.model_copy(
        update={"severity": "warning", "acknowledged_by": "acknowledged"}
    )
    assert result.diagnostics == (warning,)
    assert (result.error_count, result.warning_count) == (0, 1)
    assert result.acknowledged == {"unresolved_catalog_identity": 1}
    assert result.variables == {key: None}
    assert result.withheld_dependencies == {
        ("variable", "scb/example/value-5"): (warning,)
    }
    assert [(e.case_id, e.status) for e in result.evaluations] == [
        ("acknowledged", "applicable")
    ]
    with pytest.raises(ValueError, match="acknowledged twice"):
        resolve(
            (item,),
            cases=(case, case.model_copy(update={"case_id": "again"})),
            provider_keys={key: None},
        )


def test_a_stale_acknowledgement_is_an_error():
    item = record()
    key = native_variable_key(item)
    (issue,) = resolve((item,), provider_keys={key: None}).diagnostics
    # The issue no longer occurs: the variable forms and the acknowledgement errs.
    result = resolve((item,), cases=(acknowledge(issue, item),))
    assert result.variables[key] is not None
    (stale,) = result.diagnostics
    assert (stale.code, stale.severity, stale.case_id) == (
        "stale_curation_entry",
        "error",
        "acknowledged",
    )
    assert result.acknowledged == {}
    # Coordinates match exactly, never by containment.
    other = record(2, year="2021")
    result = resolve(
        (item, other),
        cases=(acknowledge(issue, item),),
        provider_keys={key: None},
    )
    assert [(d.code, d.severity) for d in result.diagnostics] == [
        ("unresolved_catalog_identity", "error"),
        ("stale_curation_entry", "error"),
    ]


def test_an_issue_naming_a_ref_outside_the_scope_cannot_be_acknowledged():
    """A stale correction names its authored target, absent from this scope. The
    resulting `target_missing` error names a ref no original carries, so an exact
    acknowledgement of it stays stale and the error stays an error."""
    item, absent = record(), record(2, year="2021")
    correction = effect_case(absent, effect_field(absent, "name", "Renamed"))
    (missing,) = (
        issue
        for issue in resolve((item,), cases=(correction,)).diagnostics
        if issue.code == "target_missing"
    )
    assert missing.refs == (record_ref(absent),)
    result = resolve((item,), cases=(correction, acknowledge(missing, item)))
    assert [
        (d.code, d.severity, d.case_id)
        for d in result.diagnostics
        if d.code in {"target_missing", "stale_curation_entry"}
    ] == [
        ("target_missing", "error", "accepted"),
        ("stale_curation_entry", "error", "acknowledged"),
    ]
    assert result.acknowledged == {}


def test_acknowledged_warning_persists_reviewed_reason_and_diagnostic_hash():
    from hashlib import sha256

    from reg_meta_build.data_warnings import scope_data_warnings

    item = record()
    key = native_variable_key(item)
    (issue,) = resolve((item,), provider_keys={key: None}).diagnostics
    case = acknowledge(issue, item)
    result = resolve((item,), cases=(case,), provider_keys={key: None})
    (warning,) = scope_data_warnings(result)
    assert warning.detail == case.decision.reason
    assert warning.diagnostic_detail_sha256 == sha256(issue.detail.encode()).hexdigest()
    assert warning.variable_fqid is None
    assert str(warning.register_fqid) == "scb/example"


def test_distinct_field_issues_can_each_be_acknowledged():
    items = tuple(
        item.model_copy(
            update={
                "fields": item.fields.model_copy(
                    update={
                        "definition": value_field(text),
                        "description": value_field(text),
                    }
                )
            }
        )
        for item, text in ((record(), "first"), (record(2, year="2021"), "second"))
    )
    conflicts = [
        d for d in resolve(items).diagnostics if d.code == "conflicting_variable_fact"
    ]
    assert len(conflicts) == 2
    assert len({(d.subject, d.refs) for d in conflicts}) == 1
    assert len({d.fields for d in conflicts}) == 2
    cases = tuple(
        acknowledge(issue, items[0]).model_copy(update={"case_id": f"ack-{index}"})
        for index, issue in enumerate(conflicts, 1)
    )
    result = resolve(items, cases=cases)
    warnings = tuple(
        d for d in result.diagnostics if d.code == "conflicting_variable_fact"
    )
    assert warnings == tuple(
        issue.model_copy(
            update={"severity": "warning", "acknowledged_by": f"ack-{index}"}
        )
        for index, issue in enumerate(conflicts, 1)
    )
    assert result.acknowledged == {"conflicting_variable_fact": 2}
    no_fields = acknowledge(conflicts[0], items[0])
    no_fields = no_fields.model_copy(
        update={"decision": no_fields.decision.model_copy(update={"fields": ()})}
    )
    stale = resolve(items, cases=(no_fields,))
    assert [d.code for d in stale.diagnostics] == [
        "conflicting_variable_fact",
        "conflicting_variable_fact",
        "stale_curation_entry",
    ]
    assert stale.acknowledged == {}


def test_explicit_source_diagnostic_uses_exact_register_acknowledgment():
    item = record()
    key = source_register_key(item)
    problem = ResolutionDiagnostic(
        code="unresolved_list_reference",
        severity="error",
        subject="('source','revision','descriptor','digest',0)",
        detail="No bound source members.",
        fields=("coding",),
        withheld_output=("unbound_value_membership",),
    )
    case = acknowledge(problem, item)
    result = resolve((item,), cases=(case,), source_diagnostics=((key, problem),))
    warning = next(d for d in result.diagnostics if d.code == problem.code)
    assert warning == problem.model_copy(
        update={"severity": "warning", "acknowledged_by": case.case_id}
    )
    assert result.acknowledged == {"unresolved_list_reference": 1}
    for problems in (
        (
            (
                key,
                problem.model_copy(
                    update={"subject": problem.subject + "changed revision"}
                ),
            ),
        ),
    ):
        stale = resolve((item,), cases=(case,), source_diagnostics=problems)
        assert not stale.acknowledged
        assert any(
            d.code in {"stale_curation_entry", "overbroad_curation_entry"}
            for d in stale.diagnostics
        )
    duplicates = resolve(
        (item,), cases=(case,), source_diagnostics=((key, problem), (key, problem))
    )
    assert duplicates.acknowledged == {problem.code: 1}
    assert [d for d in duplicates.diagnostics if d.code == problem.code] == [
        warning
    ] * 2
    wrong = case.model_copy(
        update={
            "decision": case.decision.model_copy(
                update={"register_key": (*key[:-1], 999)}
            )
        }
    )
    stale = resolve((item,), cases=(wrong,), source_diagnostics=((key, problem),))
    assert not stale.acknowledged and any(
        d.severity == "error" for d in stale.diagnostics
    )
    with pytest.raises(ValueError, match="positively observed"):
        resolve((item,), source_diagnostics=(((*key[:-1], 999), problem),))


@pytest.mark.parametrize("change", ["flag", "column", "physical_duplicate"])
def test_guarded_support_acknowledgement_pins_unattached_originals(change):
    from reg_meta_build.source_curation import acknowledgement_evidence_sha256
    from reg_meta_build.source_support import SourceSupportJoin

    delivery = record()
    revision = SourceRevision.create(
        dataset="summary",
        publisher="SCB",
        purpose="Support scope fixture",
        upstream_revision="1",
        artifact_path="summary.csv",
        artifact_size=1,
        artifact_sha256="b" * 64,
    )
    original = SourceRecord.create(
        revision=revision,
        locators=delivery.locators,
        subject=delivery.subject,
        edition_scope=delivery.edition_scope,
        edition_period_scope=delivery.edition_period_scope,
        fields=SourceFields(
            column_name=SourceField(status="unknown", raw_value=""),
            identifier=value_field(False),
        ),
    )
    peer = original.model_copy(
        update={
            "locators": (
                original.locators[0].model_copy(update={"physical_record": "second"}),
            )
        }
    )
    join = SourceSupportJoin(
        source="summary",
        target_sources=(delivery.source,),
        keys=("column_name",),
        fields=("identifier",),
        unique_variable=True,
        rule="Exact physical column",
        provenance=("fixture",),
    )
    register = source_register_key(delivery)

    def replay(rows, cases=()):
        support = SourceSupportBindings((join,), rows)
        support.observe(delivery)
        support.seal()
        result = resolve_source_scope(
            (delivery,),
            cases=cases,
            naming=names((delivery,)),
            provider_keys={native_variable_key(delivery): "5"},
            support=support,
            source_diagnostics=tuple((register, d) for d in support.diagnostics),
            value_sessions=(),
            classifications={},
            classification_references={},
        )
        assert support.bind(delivery) == ()
        return result, support

    before, support = replay((original, peer))
    (issue,) = support.diagnostics
    case = acknowledge(issue, delivery)
    case = case.model_copy(
        update={
            "decision": case.decision.model_copy(
                update={
                    "expected_evidence_sha256": acknowledgement_evidence_sha256(
                        (original, peer)
                    ),
                }
            )
        }
    )
    settled, unchanged = replay((original, peer), (case,))
    assert settled.acknowledged == {"unknown_support_key": 1}
    assert settled.variables == before.variables
    assert unchanged.accounting == support.accounting
    assert all(
        a.disposition == "unknown_key" and not a.targets for a in unchanged.accounting
    )
    rows = (original, peer)
    if change == "physical_duplicate":
        rows = rows[:1]
    else:
        field, value = (
            ("identifier", value_field(True))
            if change == "flag"
            else ("column_name", value_field("OTHER"))
        )
        rows = (
            original.model_copy(
                update={"fields": original.fields.model_copy(update={field: value})}
            ),
            peer,
        )
    rejected, _ = replay(rows, (case,))
    assert rejected.acknowledged == {}
    assert any(d.code == "stale_curation_entry" for d in rejected.diagnostics)


@pytest.mark.parametrize("contrast", ["output", "detail"])
def test_complete_diagnostic_guard_selects_only_one_distinct_issue(contrast):
    from reg_meta.source_evidence import canonical_sha256

    item = record()
    key = source_register_key(item)
    issue = ResolutionDiagnostic(
        code="unsupported_representation_coding",
        severity="error",
        subject="scb/example/value-5",
        detail="Literal LEFT has disputed coding.",
        refs=(record_ref(item),),
        fields=("coding",),
        withheld_output=("representation.LEFT.value_set",),
    )
    other = issue.model_copy(
        update={"withheld_output": ("representation.RIGHT.value_set",)}
        if contrast == "output"
        else {"detail": "Different own claim fingerprints."}
    )
    generic = acknowledge(issue, item)
    unguarded = resolve(
        (item,), cases=(generic,), source_diagnostics=((key, issue), (key, other))
    )
    assert not unguarded.acknowledged
    assert any(d.code == "overbroad_curation_entry" for d in unguarded.diagnostics)
    cases = tuple(
        generic.model_copy(
            update={
                "case_id": f"exact-{i}",
                "decision": generic.decision.model_copy(
                    update={
                        "expected_diagnostic_sha256": canonical_sha256(
                            d.model_dump(mode="json")
                        )
                    }
                ),
            }
        )
        for i, d in enumerate((issue, other))
    )
    settled = resolve(
        (item,),
        cases=cases,
        source_diagnostics=((key, issue), (key, other), (key, issue)),
    )
    assert settled.acknowledged == {issue.code: 2}
    problems = [d for d in settled.diagnostics if d.code == issue.code]
    assert len(problems) == 3 and all(d.severity == "warning" for d in problems)
    assert [d.acknowledged_by for d in problems].count("exact-0") == 2
    assert settled.warning_count >= 3
    streamed = []
    result = resolve(
        (item,),
        cases=cases,
        on_diagnostic=streamed.append,
        source_diagnostics=((key, issue), (key, other), (key, issue)),
    )
    assert result.diagnostics == () and result.warning_count == settled.warning_count
    assert [d for d in streamed if d.code == issue.code] == problems
    changed = issue.model_copy(update={"withheld_output": ("representation.CHANGED",)})
    drift = resolve((item,), cases=(cases[0],), source_diagnostics=((key, changed),))
    assert not drift.acknowledged
    assert changed in drift.diagnostics
    assert any(d.code == "stale_curation_entry" for d in drift.diagnostics)
