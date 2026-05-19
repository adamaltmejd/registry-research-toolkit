"""Corpus harness for §6.8.0 cross-runtime shape coherence.

See ``reg_schema/test_corpus/README.md`` for the corpus contract.
Pre-Phase 3, the harness round-trips each
``expected_ValidationResult.json`` through the §6.8.0 dataclasses;
Phase 3 of §15 step 3 swaps the body of ``test_input_is_json_object``
for a real ``validate_structural(input)`` call and an unordered-issue
equality assertion against the decoded expected result.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from reg_schema import ValidationIssue, ValidationResult

CORPUS_ROOT = Path(__file__).resolve().parent.parent / "test_corpus"

_ISSUE_KEYS = frozenset({"level", "code", "path", "message"})
_RESULT_KEYS = frozenset({"issues"})


def _discover_cases() -> list[Path]:
    """Return every case dir under ``test_corpus/`` with both files."""

    # Guard so a missing/renamed corpus surfaces via the dedicated
    # ``test_corpus_is_not_empty`` assertion rather than a
    # ``FileNotFoundError`` at pytest collection time.
    if not CORPUS_ROOT.is_dir():
        return []
    return sorted(
        p
        for p in CORPUS_ROOT.iterdir()
        if p.is_dir()
        and (p / "input.json").is_file()
        and (p / "expected_ValidationResult.json").is_file()
    )


def _decode_expected(payload: object) -> ValidationResult:
    """Decode the documented JSON shape into a ``ValidationResult``.

    Drift-protection: shape mismatches and unknown keys raise so a
    silent schema addition (or a corpus file whose top-level shape
    diverges from the contract) cannot slip past. ``code``, ``path``,
    and ``message`` are runtime-typed as ``str`` because the Python
    dataclass only enforces ``level`` — without that check a corpus
    case like ``{"code": 123}`` would pass here while still failing
    the SPA/bundle decoders the corpus exists to pin against.
    """

    if not isinstance(payload, dict):
        raise TypeError(
            f"expected_ValidationResult.json root must be an object, "
            f"got {type(payload).__name__}"
        )
    extra_top = set(payload) - _RESULT_KEYS
    if extra_top:
        raise ValueError(
            f"unexpected top-level keys in expected_ValidationResult.json: "
            f"{sorted(extra_top)}"
        )
    raw_issues = payload["issues"]
    if not isinstance(raw_issues, list):
        raise TypeError(f"`issues` must be an array, got {type(raw_issues).__name__}")
    issues: list[ValidationIssue] = []
    for i, raw in enumerate(raw_issues):
        if not isinstance(raw, dict):
            raise TypeError(f"issues[{i}] must be an object, got {type(raw).__name__}")
        extra = set(raw) - _ISSUE_KEYS
        if extra:
            raise ValueError(f"issues[{i}] has unexpected keys {sorted(extra)}")
        for key in ("code", "path", "message"):
            value = raw.get(key)
            if not isinstance(value, str):
                raise TypeError(
                    f"issues[{i}].{key} must be a string, got {type(value).__name__}"
                )
        issues.append(ValidationIssue(**raw))
    return ValidationResult(issues=tuple(issues))


_CASES = _discover_cases()
_CASE_IDS = [c.name for c in _CASES]


def test_corpus_is_not_empty() -> None:
    # Catches a silent test_corpus/ deletion or move; without this the
    # parametrize below would degenerate to zero tests and pass quietly.
    assert _CASES, f"no corpus cases found under {CORPUS_ROOT}"


@pytest.mark.parametrize("case_dir", _CASES, ids=_CASE_IDS)
def test_expected_result_decodes(case_dir: Path) -> None:
    """Every case's expected payload parses against the §6.8.0 contract —
    the cross-runtime shape coherence the corpus exists to pin."""

    payload = json.loads(
        (case_dir / "expected_ValidationResult.json").read_text(encoding="utf-8")
    )
    result = _decode_expected(payload)
    assert isinstance(result.ok, bool)


@pytest.mark.parametrize("case_dir", _CASES, ids=_CASE_IDS)
def test_input_is_json_object(case_dir: Path) -> None:
    """``input.json`` is at least a syntactically valid JSON object.

    Phase 3 swaps the body for ``validate_structural(input)`` and an
    unordered-issue equality check against ``_decode_expected(...)``.
    """

    payload = json.loads((case_dir / "input.json").read_text(encoding="utf-8"))
    assert isinstance(payload, dict), "project_data.json root must be an object"
