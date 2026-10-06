"""Damaged-artifact cases: a fixture artifact plus readable SQL damage fails (or
passes) `validate_built_db` with a located message.

Each directory under `cases/validate/` holds `request.json` and `expected.json`.
The request names the SQL damage applied to a copy of the synthetic fixture
artifact, the validator mode (`corpus`, `flavored`) and, optionally, a slug
directory shipped beside the case. The expectation lists substrings that must (or
must not) appear in a FAIL line or anywhere in the rendered report.
"""

from __future__ import annotations

import json
from contextlib import closing
from pathlib import Path

import pytest
from _shared_fixtures import connect_built_db
from reg_meta_build.validate import validate_built_db

CASES = Path(__file__).parent / "cases/validate"


@pytest.mark.parametrize(
    "case",
    sorted(path for path in CASES.iterdir() if (path / "request.json").exists()),
    ids=lambda path: path.name,
)
def test_damaged_artifact_validation(case: Path, fixture_db: Path, tmp_path: Path):
    request = json.loads((case / "request.json").read_text(encoding="utf-8"))
    expected = json.loads((case / "expected.json").read_text(encoding="utf-8"))
    damaged = tmp_path / "reg_meta.db"
    damaged.write_bytes(fixture_db.read_bytes())
    if "damage" in request:
        with closing(connect_built_db(damaged)) as conn:
            if request.get("ignore_check_constraints"):
                conn.execute("PRAGMA ignore_check_constraints = ON")
            conn.executescript((case / request["damage"]).read_text(encoding="utf-8"))
            conn.commit()
    slug_dir = case / request["slug_dir"] if "slug_dir" in request else None

    result = validate_built_db(
        damaged,
        corpus=request.get("corpus", False),
        flavored=request.get("flavored", False),
        slug_dir=slug_dir,
    )

    report = result.format_report()
    if "passed" in expected:
        assert result.passed is expected["passed"], result.failures
    for text in expected.get("failures_contain", []):
        assert any(text in failure for failure in result.failures), (
            text,
            result.failures,
        )
    for text in expected.get("failures_absent", []):
        assert not any(text in failure for failure in result.failures), (
            text,
            result.failures,
        )
    for text in expected.get("report_contains", []):
        assert text in report, (text, report)
    for text in expected.get("report_absent", []):
        assert text not in report, (text, report)
