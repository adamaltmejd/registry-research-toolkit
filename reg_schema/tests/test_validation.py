"""Smoke tests for the cross-runtime contract."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from reg_schema import IssueLevel, ValidationIssue, ValidationResult


def _issue(level: IssueLevel, code: str = "test_code") -> ValidationIssue:
    return ValidationIssue(level=level, code=code, path="/foo", message="x")


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
    with pytest.raises(FrozenInstanceError):
        issue.level = "warning"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        ValidationResult(issues=()).issues = (issue,)  # type: ignore[misc]


def test_equal_issues_dedupe_in_sets_and_lookup_in_dicts() -> None:
    a = _issue("error", code="x")
    b = _issue("error", code="x")
    c = _issue("error", code="y")
    assert {a, b, c} == {a, c}
    assert {a: 1}[b] == 1


def test_results_compose_by_tuple_concatenation() -> None:
    structural = ValidationResult(issues=(_issue("warning", code="a"),))
    semantic = ValidationResult(issues=(_issue("error", code="b"),))
    combined = ValidationResult(issues=structural.issues + semantic.issues)
    assert combined.ok is False
    assert {i.code for i in combined.issues} == {"a", "b"}


def test_invalid_level_rejected_at_construction() -> None:
    # Literal is a typing hint, not a runtime guard. JSON / cross-runtime
    # paths could smuggle in mis-cased or unknown levels and silently
    # weaken `ok`; the post-init check is what stops that.
    for bad in ("ERROR", "fatal", "", "Warning"):
        with pytest.raises(ValueError, match="invalid level"):
            ValidationIssue(level=bad, code="x", path="/", message="m")  # type: ignore[arg-type]


def test_result_coerces_non_tuple_issues_to_tuple() -> None:
    issue_list = [_issue("error", code="a"), _issue("warning", code="b")]
    result = ValidationResult(issues=issue_list)  # type: ignore[arg-type]
    assert isinstance(result.issues, tuple)
    assert result.ok is False
    # Coercion takes a snapshot — mutating the input list after the fact
    # must not affect the result.
    issue_list.clear()
    assert len(result.issues) == 2
