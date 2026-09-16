"""Selected canonical codebooks retain literal codes and competing evidence."""

from dataclasses import replace

import pytest
from reg_meta_build.source_classifications import resolve_canonical_codes
from reg_meta_build.source_records import RecordLocator
from reg_meta_build.source_values import SourceValue


def _value(key: str, code: str | None, label: str | None) -> SourceValue:
    return SourceValue(
        payload_key=key,
        code=code,
        label=label,
        locators=(
            RecordLocator(
                semantic_record_key=("codebook:fixture", key),
                physical_file="codes.csv",
                physical_table="codes.csv",
                physical_record=key,
                physical_cells=(),
            ),
        ),
    )


def test_literal_codes_and_normalized_duplicates_are_order_independent() -> None:
    values = (
        _value("first", "001", "Category"),
        _value("duplicate", "001", "Category"),
        _value("text", "A1", "Other"),
        _value("empty-label", "02", ""),
        _value("non-ascii", "٠١", "Literal"),
    )
    result = resolve_canonical_codes(values, source="fixture", subject="class/fixture")
    assert result == resolve_canonical_codes(
        reversed(values), source="fixture", subject="class/fixture"
    )
    assert result.diagnostics == () and result.source_payloads == 5
    assert [(item.code, item.label, item.level) for item in result.codes] == [
        ("001", "Category", 3),
        ("02", "", 2),
        ("A1", "Other", None),
        ("٠١", "Literal", None),
    ]


def test_conflicting_or_unknown_members_do_not_erase_independent_codes() -> None:
    values = (
        _value("a", "1", "First"),
        _value("b", "1", "Competing"),
        _value("c", "2", None),
        _value("d", None, "Unknown code"),
        _value("e", "03", "Independent"),
    )
    result = resolve_canonical_codes(values, source="fixture", subject="class/fixture")
    assert [item.code for item in result.codes] == ["03"]
    assert {issue.code for issue in result.diagnostics} == {
        "conflicting_classification_labels",
        "unknown_classification_member",
    }
    conflict = next(
        issue
        for issue in result.diagnostics
        if issue.code == "conflicting_classification_labels"
    )
    assert {ref.semantic_record_key for ref in conflict.refs} == {
        ("codebook:fixture", "a"),
        ("codebook:fixture", "b"),
    }
    assert all(
        issue.severity == "error" and issue.withheld_output == ("classification.code",)
        for issue in result.diagnostics
    )
    empty = resolve_canonical_codes(
        values[:-1], source="fixture", subject="class/fixture"
    )
    assert not empty.codes
    assert empty.diagnostics[-1].withheld_output == (
        "classification",
        "classification_bindings",
    )


def test_inconsistent_payload_identity_is_a_contract_error() -> None:
    value = _value("same", "1", "First")
    with pytest.raises(ValueError, match="payload key"):
        resolve_canonical_codes(
            (value, replace(value, label="Different")),
            source="fixture",
            subject="class/fixture",
        )
