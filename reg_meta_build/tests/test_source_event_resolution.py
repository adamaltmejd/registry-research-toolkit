"""Source succession uses exact native namespaces and resolved identities only."""

from __future__ import annotations

from dataclasses import replace

import pytest
from reg_meta.errors import RegMetaError
from reg_meta_build.resolved_catalog import ResolvedRegister
from reg_meta_build.resolved_metadata import ResolvedMetadata, ResolvedSuccession
from reg_meta_build.source_coordinates import source_register_key
from reg_meta_build.source_event_resolution import SourceEventBindings
from reg_meta_build.source_records import DeliveredCell, value_field
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
    assert result.diagnostics[0].withheld_output == ("catalog_succession.description",)


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
