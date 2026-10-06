"""Curation guard contracts: projection and source JSON share one validation and keep complete values."""

from __future__ import annotations

import json

import pytest
from _source_curation_support import field_value as _field, scope_interval as _interval
from pydantic import TypeAdapter, ValidationError
from reg_meta_build.source_curation import (
    FieldExpectation,
    GuardValidationContext,
    RecordExpectation,
    RecordProjection,
    SourceRecordRef,
)
from reg_meta_build.source_records import (
    NativeCoordinates,
    SourceCoordinate,
    SourceFields,
)


def test_projection_order_and_value_rules_reuse_the_common_source_contract() -> None:
    projection = RecordProjection(
        fields=(
            _field("representation", "value", "YYYY"),
            _field("data_type", "value", "integer"),
        )
    )

    assert tuple(field.name for field in projection.fields) == (
        "data_type",
        "representation",
    )
    with pytest.raises(ValueError, match="availability values must be true"):
        FieldExpectation(name="availability", status="value", value=False)
    with pytest.raises(ValueError, match="name must carry a string value"):
        FieldExpectation(name="name", status="value", value=True)


@pytest.mark.parametrize(
    ("name", "status", "value"),
    [
        ("availability", "value", 1),
        ("sensitivity", "value", 1),
        ("sensitivity", "value", "true"),
        ("identifier", "value", 0),
        ("conditional_sensitivity", "value", "true"),
        ("name", "value", True),
        ("measurement_unit", "negative", None),
        ("identifier", "negative", None),
        ("name", "unknown", "unsupplied"),
        ("name", "value", None),
        ("unrecognized", "value", "text"),
    ],
)
def test_projection_and_source_json_reject_the_same_invalid_named_field(
    name: str, status: str, value: str | int | bool | None
) -> None:
    with pytest.raises(ValidationError):
        FieldExpectation.model_validate_json(
            json.dumps({"name": name, "status": status, "value": value})
        )
    with pytest.raises(ValidationError):
        SourceFields.model_validate_json(
            json.dumps({name: {"status": status, "value": value}})
        )


@pytest.mark.parametrize("intern", [False, True])
@pytest.mark.parametrize(
    "alternatives",
    [
        [{"fields": [{"name": "name", "status": "value", "value": 1}]}],
        [{"edition_scope": {"kind": "unknown"}}],
        [{}],
        [
            {"fields": [{"name": "name", "status": "value", "value": "text"}]},
            {"fields": [{"name": "name", "status": "value", "value": "text"}]},
        ],
        [
            {"fields": [{"name": "name", "status": "value", "value": "text"}]},
            {"fields": [{"name": "definition", "status": "value", "value": "text"}]},
        ],
    ],
    ids=["singleton-field", "singleton-scope", "singleton-empty", "duplicate", "shape"],
)
def test_record_expectation_json_keeps_nested_and_multiple_alternative_guards(
    alternatives: list[dict[str, object]],
    intern: bool,
) -> None:
    with pytest.raises(ValidationError):
        RecordExpectation.model_validate_json(
            json.dumps(
                {
                    "ref": {"source": "fixture", "semantic_record_key": ["member:1"]},
                    "alternatives": alternatives,
                }
            ),
            context=GuardValidationContext() if intern else None,
        )


def test_json_guard_interning_keeps_full_values_and_scope_isolation() -> None:
    from reg_meta_build.source_curation import CodeSetExpectation, ParentFactProjection

    field = _field("name", "value", "Literal")
    parent = ParentFactProjection(
        kind="register",
        coordinate=SourceCoordinate(status="value", native_id=1),
        register=SourceCoordinate(status="value", native_id=1),
        fields=(field,),
    )
    projection = RecordProjection(
        fields=(field,),
        native=NativeCoordinates(variable_id=1),
        edition_scope=_interval("2020", "2020"),
        parent_facts=(parent,),
        code_set_references=(
            CodeSetExpectation(reference_id="book", content_sha256="a" * 64),
        ),
    )
    expected = RecordExpectation(
        ref=SourceRecordRef(source="fixture", semantic_record_key=("member:1",)),
        alternatives=(projection,),
    )
    variants = (
        expected.model_copy(
            update={"ref": expected.ref.model_copy(update={"source": "other"})}
        ),
        *(
            expected.model_copy(
                update={"alternatives": (projection.model_copy(update=update),)}
            )
            for update in (
                {"fields": (_field("name", "value", "Other"),)},
                {"fields": (_field("definition", "value", "Literal"),)},
                {"edition_scope": _interval("2021", "2021")},
                {"native": NativeCoordinates(variable_id=2)},
                {"parent_facts": ()},
                {"code_set_references": ()},
            )
        ),
    )
    adapter = TypeAdapter(tuple[RecordExpectation, ...])
    payload = adapter.dump_json((expected, expected, *variants))
    context = GuardValidationContext()
    parsed = adapter.validate_json(payload, context=context)
    assert adapter.dump_json(parsed) == payload
    assert parsed[0] is parsed[1]
    assert parsed[0].alternatives[0] is parsed[1].alternatives[0]
    assert all(item is not parsed[0] for item in parsed[2:])
    assert parsed[2].alternatives[0] is parsed[0].alternatives[0]
    assert all(
        item.alternatives[0] is not parsed[0].alternatives[0] for item in parsed[3:]
    )
    fresh = adapter.validate_json(payload, context=GuardValidationContext())
    assert fresh[0] is not parsed[0]
    assert fresh[0].alternatives[0] is not parsed[0].alternatives[0]
    assert adapter.dump_json(fresh) == payload


def test_json_guard_interning_preserves_reference_and_alternative_order() -> None:
    ref = SourceRecordRef(source="källa", semantic_record_key=("å", "b"))
    alternatives = tuple(
        RecordProjection(fields=(_field("name", "value", value),))
        for value in ("Första", "Andra")
    )
    expected = RecordExpectation(ref=ref, alternatives=alternatives)
    variants = (
        expected,
        expected,
        expected.model_copy(update={"alternatives": tuple(reversed(alternatives))}),
        expected.model_copy(
            update={"ref": ref.model_copy(update={"semantic_record_key": ("å/b",)})}
        ),
        expected.model_copy(update={"ref": ref.model_copy(update={"source": "KÄLLA"})}),
    )
    adapter = TypeAdapter(tuple[RecordExpectation, ...])
    payload = adapter.dump_json(variants)
    baseline = adapter.validate_json(payload)
    parsed = adapter.validate_json(payload, context=GuardValidationContext())
    assert adapter.dump_json(parsed) == adapter.dump_json(baseline) == payload
    assert parsed[0] is parsed[1]
    assert all(item is not parsed[0] for item in parsed[2:])
    assert parsed[2].alternatives == tuple(reversed(parsed[0].alternatives))
    assert parsed[2].alternatives[0] is parsed[0].alternatives[1]
    assert parsed[3].alternatives[0] is parsed[0].alternatives[0]
    invalid = json.loads(expected.model_dump_json())
    invalid["ref"]["semantic_record_key"] = [1]
    with pytest.raises(ValidationError):
        adapter.validate_json(
            json.dumps([json.loads(expected.model_dump_json()), invalid]),
            context=GuardValidationContext(),
        )


def test_json_guard_interning_keeps_complete_subclass_facts() -> None:
    class MarkedExpectation(RecordExpectation):
        marker: str

    first = MarkedExpectation(
        ref=SourceRecordRef(source="fixture", semantic_record_key=("member:1",)),
        alternatives=(RecordProjection(fields=(_field("name", "value", "Name"),)),),
        marker="First",
    )
    second = first.model_copy(update={"marker": "Second"})
    adapter = TypeAdapter(tuple[MarkedExpectation, ...])
    payload = adapter.dump_json((first, second, first))
    parsed = adapter.validate_json(payload, context=GuardValidationContext())
    assert adapter.dump_json(parsed) == payload
    assert parsed[0] is parsed[2]
    assert parsed[0] is not parsed[1]
    assert parsed[0].alternatives[0] is parsed[1].alternatives[0]
