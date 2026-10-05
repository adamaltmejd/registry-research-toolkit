"""Source succession uses exact native namespaces and resolved identities only."""

from __future__ import annotations

from dataclasses import replace

import pytest
from catalog_manifest import synthetic_manifest
from reg_meta.errors import RegMetaError
from reg_meta.source_evidence import DeliveredCell, canonical_sha256
from reg_meta_build.resolved_catalog import ResolvedRegister
from reg_meta_build.resolved_metadata import ResolvedMetadata, ResolvedSuccession
from reg_meta_build.source_coordinates import source_register_key
from reg_meta_build.source_curation import (
    AcknowledgeDecision,
    CurationCase,
    SourceRecordRef,
    acknowledgement_hashes_sha256,
)
from reg_meta_build.source_effects import record_ref
from reg_meta_build.source_event_resolution import SourceEventBindings
from reg_meta_build.source_records import NativeCoordinates, value_field
from reg_meta_build.source_reference_records import SourceEventDeclaration
from test_source_reference_resolution import LOCATOR, REVISION
from test_source_scope import record, resolve


def _event(
    kind="member", first="1", second="2", *, action="replaced_by", text="Change"
):
    def cell(name, token):
        return DeliveredCell(
            name=name, present=True, raw_value=token, interpreted_value=token
        )

    return SourceEventDeclaration(
        revision=REVISION,
        locator=LOCATOR.model_copy(
            update={"semantic_record_key": (kind, first, second, action, text)}
        ),
        delivered_cells=(),
        name=value_field("Change"),
        action=action,
        action_label=value_field(action),
        description=value_field(text),
        entity_kind=kind,
        entity_label=value_field(kind),
        first_token=cell("first", first),
        second_token=cell("second", second),
        document_token=cell("document", "1"),
    )


def _observe(events):
    originals = (record(1, 5, 2), record(2, 6, 3, "OTHER"))
    result = resolve(originals)
    uses = {
        r.record_id: [{"variable": f"scb/example/value-{r.subject.native.variable_id}"}]
        for r in originals
    }
    bindings = SourceEventBindings(events, {REVISION.dataset: originals[0].source})
    return bindings, originals, result, uses


@pytest.mark.parametrize(
    "kind, first, second",
    [("member", "1", "2"), ("variable", "5", "6"), ("variant", "2", "3")],
)
def test_reciprocal_source_events_coalesce_in_the_same_direction(kind, first, second):
    events = (
        _event(kind, first, second),
        _event(kind, second, first, action="replaces"),
    )
    bindings, originals, scope, uses = _observe(events)
    bindings.observe_scope(originals, scope, uses)
    result = bindings.resolve(ResolvedMetadata())
    assert not result.diagnostics
    if kind == "variant":
        (edge,) = result.metadata.variant_successions
        assert (
            edge.predecessor.register_ref,
            edge.predecessor.variant,
            edge.successor.variant,
        ) == ("scb/example", "people-2", "people-3")
    else:
        (edge,) = result.metadata.successions
        assert (edge.predecessor, edge.successor) == (
            "scb/example/value-5",
            "scb/example/value-6",
        )
    assert (edge.note, edge.description, edge.effective_year) == (
        "auto:timeseries_event",
        "Change",
        None,
    )
    reversed_bindings, _, _, _ = _observe(reversed(events))
    reversed_bindings.observe_scope(tuple(reversed(originals)), scope, uses)
    assert reversed_bindings.resolve(ResolvedMetadata()) == result


def test_register_events_use_resolved_parents_independently_of_variable_omissions():
    bindings, originals, scope, _uses = _observe((_event("register"),))
    second = originals[1].model_copy(
        update={
            "subject": originals[1].subject.model_copy(
                update={
                    "native": originals[1].subject.native.model_copy(
                        update={"register_id": 2}
                    ),
                    "register_name": originals[1].subject.register_name.model_copy(
                        update={"native_id": 2}
                    ),
                }
            )
        }
    )
    parents = replace(
        scope.parents,
        registers={
            source_register_key(originals[0]): ResolvedRegister(
                provider="scb", slug="before", name="Before"
            ),
            source_register_key(second): ResolvedRegister(
                provider="scb", slug="after", name="After"
            ),
        },
    )
    bindings.observe_scope((originals[0], second), replace(scope, parents=parents), {})
    (edge,) = bindings.resolve(ResolvedMetadata()).metadata.successions
    assert (edge.predecessor, edge.successor) == ("scb/before", "scb/after")


@pytest.mark.parametrize(
    "targets", [[None], ["scb/example/value-5", "scb/example/other"]]
)
def test_unresolved_or_split_native_identity_withholds_only_its_event(targets):
    bindings, originals, scope, uses = _observe((_event(),))
    uses[originals[0].record_id] = [{"variable": v} for v in targets]
    bindings.observe_scope(originals, scope, uses)
    result = bindings.resolve(ResolvedMetadata())
    assert not result.metadata.successions
    (issue,) = result.diagnostics
    assert (
        issue.code == "unresolved_source_event_endpoint" and issue.severity == "error"
    )
    assert len(issue.refs) == 3


def test_event_binding_never_matches_another_source_or_coerces_native_ids():
    for target in ("unrelated-source", "scope-fixture"):
        bindings, originals, scope, uses = _observe((_event(first="01"),))
        bindings = SourceEventBindings(bindings.events, {REVISION.dataset: target})
        bindings.observe_scope(originals, scope, uses)
        assert (
            bindings.resolve(ResolvedMetadata()).diagnostics[0].code
            == "unresolved_source_event_endpoint"
        )
    with pytest.raises(ValueError, match="explicit occurrence-source binding"):
        SourceEventBindings((_event(),), {})


def test_competing_descriptions_withhold_prose_but_preserve_the_explicit_edge():
    bindings, originals, scope, uses = _observe(
        (_event(), _event(text="Other account"))
    )
    bindings.observe_scope(originals, scope, uses)
    result = bindings.resolve(ResolvedMetadata())
    assert len(result.metadata.successions) == 1
    assert result.metadata.successions[0].description is None
    assert result.diagnostics[0].code == "conflicting_source_event_description"
    assert len(result.diagnostics[0].refs) == 2
    assert {ref.source for ref in result.diagnostics[0].refs} == {REVISION.dataset}
    assert result.diagnostics[0].withheld_output == ("catalog_succession.description",)


def _missing_endpoint_acknowledgement():
    event = _event(first="1", second="missing")
    bindings, originals, scope, uses = _observe((event,))
    bindings.observe_scope(originals, scope, uses)
    (issue,) = bindings.resolve(ResolvedMetadata()).diagnostics
    evidence = (
        event,
        *(r for r in originals if record_ref(r) in issue.refs),
    )
    case = CurationCase(
        case_id="registers/scb/example.toml#/acknowledge/1",
        targets=(),
        decision=AcknowledgeDecision(
            code=issue.code,
            subject=issue.subject,
            refs=issue.refs,
            register_key=source_register_key(originals[0]),
            reason="The named successor is absent from the supplied occurrences.",
            evidence="Exact source event and complete positively observed endpoint.",
            expected_evidence_sha256=acknowledgement_hashes_sha256(
                canonical_sha256(r.model_dump(mode="json")) for r in evidence
            ),
            expected_diagnostic_sha256=canonical_sha256(issue.model_dump(mode="json")),
        ),
    )
    return event, originals, scope, uses, case


def test_exact_missing_event_acknowledgement_keeps_originals_and_withheld_edge(
    tmp_path,
):
    import json
    import sqlite3

    from reg_meta_build.data_warnings import acknowledged_data_warnings
    from reg_meta_build.resolved_catalog import write_resolved_catalog

    event, originals, scope, uses, case = _missing_endpoint_acknowledgement()
    bindings = SourceEventBindings(
        (event,), {REVISION.dataset: originals[0].source}, (case,)
    )
    bindings.observe_scope(originals, scope, uses)
    result = bindings.resolve(ResolvedMetadata())
    (issue,) = result.diagnostics
    assert (issue.severity, issue.acknowledged_by) == ("warning", case.case_id)
    assert issue.withheld_output == ("catalog_succession",)
    assert result.withheld == (
        SourceRecordRef(
            source=event.revision.dataset,
            semantic_record_key=event.locator.semantic_record_key,
        ),
    )
    assert not result.metadata.successions
    assert bindings.events == (event,)
    (warning,) = acknowledged_data_warnings(
        result.diagnostics, (case,), bindings.guarded_registers
    )
    assert warning.variable_fqid is None and warning.variant is None
    assert warning.detail == case.decision.reason
    assert warning.refs == issue.refs
    output = tmp_path / "catalog.db"
    write_resolved_catalog(
        (),
        output,
        manifest=synthetic_manifest(),
        diagnostic=True,
        parent_registers=tuple(bindings.guarded_registers.values()),
        data_warnings=(warning,),
    )
    with sqlite3.connect(f"file:{output}?mode=ro", uri=True) as conn:
        (stored,) = conn.execute("SELECT warning_json FROM data_warning").fetchone()
        assert json.loads(stored) == json.loads(warning.model_dump_json())
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        assert conn.execute("SELECT COUNT(*) FROM register_variant").fetchone() == (0,)


@pytest.mark.parametrize(
    "change", ["original", "event", "endpoint", "owner", "duplicate"]
)
def test_missing_event_acknowledgement_refuses_changed_source_or_resolution(change):
    event, originals, scope, uses, case = _missing_endpoint_acknowledgement()
    if change == "original":
        originals = (
            originals[0].model_copy(update={"original_period_text": "Changed source"}),
            originals[1],
        )
    elif change == "event":
        event = event.model_copy(update={"description": value_field("Changed source")})
    elif change == "endpoint":
        uses[originals[0].record_id] = [{"variable": "scb/example/different"}]
    elif change == "owner":
        case = case.model_copy(
            update={
                "decision": case.decision.model_copy(
                    update={"register_key": ("different",)}
                )
            }
        )
    else:
        originals = (originals[0], *originals)
    bindings = SourceEventBindings(
        (event,), {REVISION.dataset: originals[0].source}, (case,)
    )
    bindings.observe_scope(originals, scope, uses)
    result = bindings.resolve(ResolvedMetadata())
    assert not any(issue.acknowledged_by for issue in result.diagnostics)
    assert any(issue.code == "stale_curation_entry" for issue in result.diagnostics)
    assert not result.metadata.successions


def test_identical_event_copies_retain_counted_diagnostics_and_evidence_multiplicity():
    event, originals, scope, uses, case = _missing_endpoint_acknowledgement()
    evidence = (event, event, originals[0])
    decision = case.decision.model_copy(
        update={
            "expected_evidence_sha256": acknowledgement_hashes_sha256(
                canonical_sha256(r.model_dump(mode="json")) for r in evidence
            )
        }
    )
    case = case.model_copy(update={"decision": decision})
    bindings = SourceEventBindings(
        (event, event), {REVISION.dataset: originals[0].source}, (case,)
    )
    bindings.observe_scope(originals, scope, uses)
    result = bindings.resolve(ResolvedMetadata())
    assert len(result.diagnostics) == 2
    assert all(d.acknowledged_by == case.case_id for d in result.diagnostics)
    assert not result.metadata.successions


def test_explicit_edge_keeps_its_provenance_and_combined_cycles_fail():
    bindings, originals, scope, uses = _observe((_event(),))
    bindings.observe_scope(originals, scope, uses)
    declared = ResolvedSuccession(
        predecessor="scb/example/value-5",
        successor="scb/example/value-6",
        note="curated",
        description="Accepted",
    )
    metadata = ResolvedMetadata(successions=(declared,))
    assert bindings.resolve(metadata).metadata == metadata
    reverse = declared.model_copy(
        update={"predecessor": declared.successor, "successor": declared.predecessor}
    )
    with pytest.raises(RegMetaError) as error:
        bindings.resolve(ResolvedMetadata(successions=(reverse,)))
    assert error.value.code == "replaced_by_cycle"


def test_accepted_identity_merge_makes_a_native_succession_redundant():
    bindings, originals, scope, uses = _observe((_event(),))
    uses[originals[1].record_id] = [{"variable": "scb/example/value-5"}]
    bindings.observe_scope(originals, scope, uses)
    result = bindings.resolve(ResolvedMetadata())
    assert not result.metadata.successions and not result.diagnostics


def test_outside_anchor_defers_unknown_endpoint_only_in_scoped_build():
    bindings, originals, _scope, _uses = _observe((_event(),))
    bindings.observe_unselected(originals[0].source, (NativeCoordinates(member_id=1),))
    result = bindings.resolve(ResolvedMetadata())
    (issue,) = result.diagnostics
    assert (issue.code, issue.severity) == (
        "deferred_out_of_slice_reference",
        "warning",
    )
    assert "Unknown endpoints" in issue.detail and "'2'" in issue.detail
    assert "without inferred ownership" in issue.detail
    assert not result.metadata.successions
    assert len(bindings.unselected) == 1
    complete = SourceEventBindings(
        bindings.events, {REVISION.dataset: originals[0].source}
    )
    assert complete.resolve(ResolvedMetadata()).diagnostics[0].severity == "error"


@pytest.mark.parametrize(
    "targets",
    [["scb/example/value-6"], [None], ["scb/example/value-6", "scb/example/other"], []],
)
def test_selected_endpoint_evidence_prevents_outside_anchor_deferral(targets):
    bindings, originals, scope, uses = _observe((_event(),))
    uses[originals[1].record_id] = [{"variable": v} for v in targets]
    bindings.observe_scope((originals[1],), scope, uses)
    bindings.observe_unselected(
        originals[0].source,
        (NativeCoordinates(member_id=1), NativeCoordinates(member_id=2)),
    )
    (issue,) = bindings.resolve(ResolvedMetadata()).diagnostics
    assert (issue.code, issue.severity) == ("unresolved_source_event_endpoint", "error")
    assert any(
        ref.semantic_record_key != bindings.events[0].locator.semantic_record_key
        for ref in issue.refs
    )
    assert not bindings.skipped_events


def test_all_outside_events_skip_but_wholly_unknown_events_remain_errors():
    bindings, originals, _scope, _uses = _observe((_event(),))
    assert bindings.resolve(ResolvedMetadata()).diagnostics[0].severity == "error"
    bindings.observe_unselected(
        "another-source",
        (NativeCoordinates(member_id=1), NativeCoordinates(member_id=2)),
    )
    assert bindings.resolve(ResolvedMetadata()).diagnostics[0].severity == "error"
    bindings.observe_unselected(
        originals[0].source,
        (NativeCoordinates(member_id=1), NativeCoordinates(member_id=2)),
    )
    result = bindings.resolve(ResolvedMetadata())
    assert not result.metadata.successions and not result.diagnostics
    assert len(bindings.skipped_events) == 1
