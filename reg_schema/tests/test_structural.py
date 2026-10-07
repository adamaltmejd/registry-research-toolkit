"""Python-API properties of the structural validator that a corpus case cannot carry.

Every rule's input → issue-set behavior is a case under ``reg_schema/test_corpus/``
(run by ``test_corpus.py``). What stays here is what a JSON payload cannot express:
a non-dict ``Mapping`` input, the tuple return type, and emission order, which the
corpus deliberately compares unordered.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from reg_schema import ValidationResult, validate_structural

MINIMAL_CASE = Path(__file__).resolve().parent.parent / "test_corpus/minimal/input.json"


def _spec(**overrides: Any) -> dict[str, Any]:
    """The corpus's minimal valid payload; overrides exercise individual rules."""

    base: dict[str, Any] = json.loads(MINIMAL_CASE.read_text(encoding="utf-8"))
    base.update(overrides)
    return base


def test_unknown_top_level_fields_emit_one_issue_each_in_sorted_order() -> None:
    result = validate_structural(
        _spec(z_scalar=7, a_object={"nested": True}, m_array=[1, 2, 3])
    )
    issues = [issue for issue in result.issues if issue.code == "unexpected_field"]
    assert [issue.path for issue in issues] == ["/a_object", "/m_array", "/z_scalar"]
    assert all(issue.level == "error" for issue in issues)


def test_result_issues_is_tuple_of_validation_issues() -> None:
    result = validate_structural(_spec())
    assert isinstance(result, ValidationResult)
    assert isinstance(result.issues, tuple)


def test_validator_accepts_arbitrary_mapping() -> None:
    # The signature is ``Mapping[str, object]``, not ``dict``; any
    # mapping should work (matters for SPA-shaped or proxy inputs).
    class FrozenMap(Mapping):  # type: ignore[type-arg]
        def __init__(self, data: dict[str, Any]) -> None:
            self._d = data

        def __getitem__(self, key: str) -> Any:
            return self._d[key]

        def __iter__(self):
            return iter(self._d)

        def __len__(self) -> int:
            return len(self._d)

    result = validate_structural(FrozenMap(_spec()))
    assert result.ok, result.issues
