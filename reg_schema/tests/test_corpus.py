"""Corpus harness for §6.8.0 cross-runtime shape coherence.

See ``reg_schema/test_corpus/README.md`` for the corpus contract and
the three runtimes that consume the same JSON.

Until Phase 3 of §15 step 3 lands ``validate_structural()``, this
harness rides the corpus by round-tripping each
``expected_ValidationResult.json`` through the §6.8.0 dataclasses —
proving the JSON shape parses identically to what the validator must
emit, which is precisely the cross-runtime guarantee the corpus
exists to pin. Phase 3 extends the per-case test with a real
``validate_structural(input)`` call and an equality assertion against
the expected result (unordered issues).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from reg_schema import ValidationIssue, ValidationResult

CORPUS_ROOT = Path(__file__).resolve().parent.parent / "test_corpus"

_ISSUE_KEYS = frozenset({"level", "code", "path", "message"})
_RESULT_KEYS = frozenset({"issues"})


def _discover_cases() -> list[Path]:
    """Return every case dir under ``test_corpus/`` with both files.

    Directories missing either ``input.json`` or
    ``expected_ValidationResult.json`` are skipped, so README assets
    and helper files coexist without confusing the harness.
    """

    return sorted(
        p
        for p in CORPUS_ROOT.iterdir()
        if p.is_dir()
        and (p / "input.json").is_file()
        and (p / "expected_ValidationResult.json").is_file()
    )


def _decode_expected(payload: dict[str, Any]) -> ValidationResult:
    """Decode the documented JSON shape into a ``ValidationResult``.

    Drift-protection: unknown keys at either level raise so a silent
    schema addition cannot slip past the corpus. This is a stricter
    contract than ``ValidationIssue(**raw)`` alone (which would accept
    unexpected keyword-only kwargs as TypeErrors but with less useful
    location info).
    """

    extra_top = set(payload) - _RESULT_KEYS
    if extra_top:
        raise ValueError(
            f"unexpected top-level keys in expected_ValidationResult.json: "
            f"{sorted(extra_top)}"
        )
    issues: list[ValidationIssue] = []
    for i, raw in enumerate(payload["issues"]):
        if not isinstance(raw, dict):
            raise TypeError(f"issues[{i}] must be an object, got {type(raw).__name__}")
        extra = set(raw) - _ISSUE_KEYS
        if extra:
            raise ValueError(f"issues[{i}] has unexpected keys {sorted(extra)}")
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
    """``expected_ValidationResult.json`` decodes into §6.8.0 shape.

    Cross-runtime shape coherence — the bundle amalgamation and the
    SPA's TS port both read this same JSON, so every case's expected
    payload must parse cleanly against the contract module.
    """

    payload = json.loads((case_dir / "expected_ValidationResult.json").read_text())
    result = _decode_expected(payload)
    assert isinstance(result.ok, bool)


@pytest.mark.parametrize("case_dir", _CASES, ids=_CASE_IDS)
def test_input_is_json_object(case_dir: Path) -> None:
    """``input.json`` is at least a syntactically valid JSON object.

    Phase 3 of §15 step 3 swaps this assertion for the real
    ``validate_structural(input)`` call and an equality check against
    ``_decode_expected(...)`` (unordered issues). Until then the
    corpus inputs ride alongside the harness so Phase 3's diff is
    purely the validator wiring.
    """

    payload = json.loads((case_dir / "input.json").read_text())
    assert isinstance(payload, dict), "project_data.json root must be an object"
