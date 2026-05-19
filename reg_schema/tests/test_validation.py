"""Smoke tests for the §6.8.0 cross-runtime contract."""

from __future__ import annotations

import pytest

from reg_schema import ValidationIssue, ValidationResult


def _issue(level: str, code: str = "test_code") -> ValidationIssue:
    return ValidationIssue(level=level, code=code, path="/foo", message="x")  # type: ignore[arg-type]


def test_empty_result_is_ok() -> None:
    assert ValidationResult(issues=()).ok is True


def test_info_and_warning_do_not_block() -> None:
    result = ValidationResult(issues=(_issue("info"), _issue("warning")))
    assert result.ok is True


def test_single_error_blocks() -> None:
    assert ValidationResult(issues=(_issue("error"),)).ok is False


def test_error_among_warnings_still_blocks() -> None:
    issues = (_issue("warning"), _issue("error"), _issue("info"))
    assert ValidationResult(issues=issues).ok is False


def test_issue_and_result_are_frozen() -> None:
    issue = _issue("error")
    with pytest.raises(Exception):  # FrozenInstanceError
        issue.level = "warning"  # type: ignore[misc]
    with pytest.raises(Exception):
        ValidationResult(issues=()).issues = (issue,)  # type: ignore[misc]


def test_issue_is_hashable() -> None:
    a = _issue("error", code="x")
    b = _issue("error", code="x")
    assert hash(a) == hash(b)
    assert a == b


def test_results_compose_by_tuple_concatenation() -> None:
    structural = ValidationResult(issues=(_issue("warning", code="a"),))
    semantic = ValidationResult(issues=(_issue("error", code="b"),))
    combined = ValidationResult(issues=structural.issues + semantic.issues)
    assert combined.ok is False
    assert {i.code for i in combined.issues} == {"a", "b"}
