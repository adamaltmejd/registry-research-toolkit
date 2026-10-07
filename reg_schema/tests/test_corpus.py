"""Corpus harness for cross-runtime shape coherence.

See ``reg_schema/test_corpus/README.md`` for the corpus contract.
The harness runs ``validate_structural(input)`` against every case
and asserts an unordered-issue equality match with the decoded
``expected_ValidationResult.json`` — issue order is not part of the
cross-runtime contract, so the comparison is set-based.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import get_args

import pytest

import reg_schema
from reg_schema import (
    IssueLevel,
    ValidationIssue,
    ValidationResult,
    validate_structural,
)

CORPUS_ROOT = Path(__file__).resolve().parent.parent / "test_corpus"

_ISSUE_KEYS = frozenset({"level", "code", "path", "message"})
_RESULT_KEYS = frozenset({"issues"})
_LEVELS = frozenset(get_args(IssueLevel))


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
    diverges from the contract) cannot slip past. The issue fields are
    checked here because the dataclass annotations are typing hints only:
    without the check a corpus case like ``{"code": 123}`` or
    ``{"level": "ERROR"}`` would pass here while still failing the SPA's
    typed import of the same expected JSON (and a mis-cased level would
    silently flip ``ok``).
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
        if raw.get("level") not in _LEVELS:
            raise ValueError(
                f"issues[{i}].level must be one of {sorted(_LEVELS)}, "
                f"got {raw.get('level')!r}"
            )
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


def test_corpus_inputs_carry_the_current_schema_version() -> None:
    # Structural validation ignores the version value, so a stale corpus still
    # passes the matcher while describing documents the /validate door refuses
    # (`unsupported_schema_version`). Fails when `reg_schema.__version__` bumps
    # without the corpus being re-authored. Version-shape cases (no object
    # root, absent / null / non-string `schema_version`) are exempt: their
    # point is the malformed field, not the contract version.
    stale = []
    for case_dir in _CASES:
        payload = json.loads((case_dir / "input.json").read_text(encoding="utf-8"))
        version = payload.get("schema_version") if isinstance(payload, dict) else None
        if isinstance(version, str) and version != reg_schema.__version__:
            stale.append(f"{case_dir.name}: {version}")
    assert not stale, f"corpus inputs not on {reg_schema.__version__}: {stale}"


def _issue_key(i: ValidationIssue) -> tuple[str, str, str, str]:
    # `ValidationIssue` is frozen but has no natural ordering. Comparing
    # as sorted 4-tuples gives order-independent equality while still
    # surfacing duplicate-issue regressions (set comparison would
    # collapse them).
    return (i.level, i.code, i.path, i.message)


@pytest.mark.parametrize("case_dir", _CASES, ids=_CASE_IDS)
def test_validate_structural_matches_expected(case_dir: Path) -> None:
    """``validate_structural(input)`` produces the expected issue set.

    Compared unordered: rule-emission order is an implementation detail
    of the validator, not part of the corpus contract (see
    ``test_corpus/README.md``).
    """

    # No dict-only guard: malformed root shapes (array, scalar, null)
    # are in scope for this corpus — `validate_structural` emits
    # `invalid_root` for them. Gating those out here would create a
    # silent blind spot in the corpus contract.
    payload = json.loads((case_dir / "input.json").read_text(encoding="utf-8"))
    actual = validate_structural(payload)
    expected = _decode_expected(
        json.loads(
            (case_dir / "expected_ValidationResult.json").read_text(encoding="utf-8")
        )
    )
    actual_keys = sorted(_issue_key(i) for i in actual.issues)
    expected_keys = sorted(_issue_key(i) for i in expected.issues)
    assert actual_keys == expected_keys, (
        f"\nactual:   {actual_keys}\nexpected: {expected_keys}"
    )
