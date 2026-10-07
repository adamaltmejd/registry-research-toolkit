"""Curation guard contracts: projection and source JSON share one validation and keep complete values."""

from __future__ import annotations

import json

import pytest
from _source_curation_support import field_value as _field
from pydantic import ValidationError
from reg_meta_build.source_curation import (
    FieldExpectation,
    GuardValidationContext,
    RecordExpectation,
    RecordProjection,
)
from reg_meta_build.source_records import (
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
