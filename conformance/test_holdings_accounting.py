"""Accepted-input census comparison and a readable synthetic pipeline exercise."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from holdings_accounting import compare_holdings_input
from reader_artifacts import BUILDER_CASES, CASES, build_reader_artifact


def require_accounting(report):
    for key in (
        "counts_equal",
        "accounting_digest_equal",
        "policy_digest_equal",
        "canonical_fold_equal",
        "artifact_unchanged",
    ):
        if not report[key]:
            pytest.fail(f"Accepted-input accounting contract failed: {key}")
    for name, relation in report["relations"].items():
        if not relation["equal"]:
            pytest.fail(f"Accepted-input exact relation content differs: {name}")


@pytest.mark.parametrize(
    "fixture",
    [
        "reader",
        "range",
        "list",
        "partitions",
        "retained-unknown",
        "year-independent",
        "lookup-exclusion-annotations",
    ],
)
def test_readable_source_census_matches_compiled_artifact(tmp_path, fixture):
    directory = tmp_path / "artifact"
    build_reader_artifact(directory, fixture, "steward")
    source = (
        CASES / "reader/fixture" if fixture == "reader" else BUILDER_CASES / fixture
    )
    candidate = tmp_path / "input"
    (candidate / "policy").mkdir(parents=True)
    (candidate / "swecov").mkdir()
    for name in ("inventory", "inventory_overlay", "holdings_policy", "source_policy"):
        selected = source / f"{name}.toml"
        shutil.copyfile(
            selected if selected.exists() else CASES / f"reader/fixture/{name}.toml",
            candidate / f"policy/{name}.toml",
        )
    shutil.copyfile(
        source / "census.csv",
        candidate / "swecov/SWECOV_variables_full_fixture.csv",
    )
    require_accounting(compare_holdings_input(candidate, directory))


def test_accepted_input_census_matches_compiled_artifact(artifact_dir, request):
    selected = request.config.getoption("--holdings-input")
    if selected is None:
        pytest.fail(
            "Accepted-private-input comparison requires explicit --holdings-input"
        )
    require_accounting(
        compare_holdings_input(Path(selected).expanduser().resolve(), artifact_dir)
    )
