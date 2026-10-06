"""Enforce 800-line test modules. The frozen file allowlist only shrinks.

Only test_*.py and conftest.py under tests/ and conformance/ count. Source fixtures
and support scripts are not test modules. New untracked files are scanned as well.
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ALLOWLIST = {
    "reg_meta_build/tests/test_catalog_dependencies.py",
    "reg_meta_build/tests/test_classifications.py",
    "reg_meta_build/tests/test_curation_compile.py",
    "reg_meta_build/tests/test_fqid_slugs.py",
    "reg_meta_build/tests/test_pipeline.py",
    "reg_meta_build/tests/test_relations.py",
    "reg_meta_build/tests/test_repo_curation_tomls.py",
    "reg_meta_build/tests/test_resolved_catalog.py",
    "reg_meta_build/tests/test_resolved_metadata.py",
    "reg_meta_build/tests/test_source_classification_bindings.py",
    "reg_meta_build/tests/test_source_coding_choices.py",
    "reg_meta_build/tests/test_source_curation.py",
    "reg_meta_build/tests/test_source_effects.py",
    "reg_meta_build/tests/test_source_formation.py",
    "reg_meta_build/tests/test_source_intervals.py",
    "reg_meta_build/tests/test_source_representations.py",
    "reg_meta_build/tests/test_source_scope.py",
    "reg_meta_build/tests/test_source_value_bindings.py",
    "reg_meta_build/tests/test_swecov_build_catalog.py",
    "reg_schema/tests/test_structural.py",
}


def test_test_modules_stay_under_800_lines(python_test_files):
    files = [
        path
        for path in python_test_files
        if path.name.startswith("test_") or path.name == "conftest.py"
    ]
    assert files, "No test modules scanned"
    observed = {}
    for path in files:
        lines = len(path.read_text().splitlines())
        if lines > 800:
            observed[path.relative_to(ROOT).as_posix()] = lines
    assert set(observed) == ALLOWLIST, (
        "New oversized files: "
        + repr({name: observed[name] for name in sorted(set(observed) - ALLOWLIST)})
        + "; remove stale allowlist entries: "
        + repr(sorted(ALLOWLIST - set(observed)))
    )
