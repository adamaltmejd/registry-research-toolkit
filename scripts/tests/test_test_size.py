"""Enforce 800-line test modules. The frozen file allowlist only shrinks.

Only test_*.py and conftest.py under tests/ and conformance/ count. Source fixtures
and support scripts are not test modules. New untracked files are scanned as well.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ALLOWLIST = {
    "reg_meta/tests/test_catalog.py",
    "reg_meta/tests/test_commands.py",
    "reg_meta/tests/test_concept_group_surfaces.py",
    "reg_meta/tests/test_graph.py",
    "reg_meta/tests/test_search_classifications.py",
    "reg_meta/tests/test_value_code_search.py",
    "reg_meta_build/tests/test_build_db.py",
    "reg_meta_build/tests/test_catalog_dependencies.py",
    "reg_meta_build/tests/test_classifications.py",
    "reg_meta_build/tests/test_concept_group_candidates.py",
    "reg_meta_build/tests/test_curation_compile.py",
    "reg_meta_build/tests/test_entity_key_pins.py",
    "reg_meta_build/tests/test_fqid_slugs.py",
    "reg_meta_build/tests/test_input_snapshot.py",
    "reg_meta_build/tests/test_pipeline.py",
    "reg_meta_build/tests/test_prepared_sources.py",
    "reg_meta_build/tests/test_relations.py",
    "reg_meta_build/tests/test_repo_curation_tomls.py",
    "reg_meta_build/tests/test_resolved_catalog.py",
    "reg_meta_build/tests/test_resolved_metadata.py",
    "reg_meta_build/tests/test_sos_source_records.py",
    "reg_meta_build/tests/test_source_classification_bindings.py",
    "reg_meta_build/tests/test_source_coding_choices.py",
    "reg_meta_build/tests/test_source_curation.py",
    "reg_meta_build/tests/test_source_effects.py",
    "reg_meta_build/tests/test_source_formation.py",
    "reg_meta_build/tests/test_source_inspection.py",
    "reg_meta_build/tests/test_source_intervals.py",
    "reg_meta_build/tests/test_source_representations.py",
    "reg_meta_build/tests/test_source_scope.py",
    "reg_meta_build/tests/test_source_value_bindings.py",
    "reg_meta_build/tests/test_swecov_build_catalog.py",
    "reg_meta_build/tests/test_validate.py",
    "reg_schema/tests/test_structural.py",
    "reg_webapp/backend/tests/test_catalog_browse.py",
    "reg_webapp/backend/tests/test_search.py",
    "reg_webapp/backend/tests/test_semantic.py",
}


def test_test_modules_stay_under_800_lines():
    names = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "--", "*.py"],
        cwd=ROOT,
        text=True,
    ).splitlines()
    files = {
        ROOT / name
        for name in names
        if ("tests" in Path(name).parts or "conformance" in Path(name).parts)
        and (Path(name).name.startswith("test_") or Path(name).name == "conftest.py")
        and (ROOT / name).is_file()
    }
    assert files, "No test modules scanned"
    observed = {
        path.relative_to(ROOT).as_posix(): len(path.read_text().splitlines())
        for path in files
        if len(path.read_text().splitlines()) > 800
    }
    assert set(observed) == ALLOWLIST, (
        "New oversized files: "
        + repr({name: observed[name] for name in sorted(set(observed) - ALLOWLIST)})
        + "; remove stale allowlist entries: "
        + repr(sorted(ALLOWLIST - set(observed)))
    )
