"""Accepted-input census comparison and a readable synthetic pipeline exercise."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from holdings_accounting import committed_spec_dir, compare_holdings_input
from reader_artifacts import BUILDER_CASES, CASES, build_reader_artifact


def require_accounting(report):
    for key in (
        "counts_equal",
        "accounting_digest_equal",
        "spec_digest_equal",
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
        "lookup",
    ],
)
def test_readable_source_census_matches_compiled_artifact(tmp_path, fixture):
    directory = tmp_path / "artifact"
    build_reader_artifact(directory, fixture, "steward")
    source = (
        CASES / "reader/fixture" if fixture == "reader" else BUILDER_CASES / fixture
    )
    candidate = tmp_path / "input"
    spec_dir = tmp_path / "spec"
    (candidate / "policy").mkdir(parents=True)
    (candidate / "swecov").mkdir()
    spec_dir.mkdir()
    for name, target in (
        ("inventory", candidate / "policy/inventory.toml"),
        ("scope", spec_dir / "scope.toml"),
    ):
        selected = source / f"{name}.toml"
        shutil.copyfile(
            selected if selected.exists() else CASES / f"reader/fixture/{name}.toml",
            target,
        )
    census = "swecov/SWECOV_variables_full_fixture.csv"
    shutil.copyfile(source / "census.csv", candidate / census)
    (candidate / "manifest.json").write_text(
        json.dumps({"roles": {"census": census, "inventory": "policy/inventory.toml"}})
    )
    require_accounting(compare_holdings_input(candidate, directory, spec_dir))


def test_accepted_input_census_matches_compiled_artifact(artifact_dir, request):
    selected = request.config.getoption("--holdings-input")
    if selected is None:
        pytest.fail(
            "Accepted-private-input comparison requires explicit --holdings-input"
        )
    require_accounting(
        compare_holdings_input(
            Path(selected).expanduser().resolve(),
            artifact_dir,
            committed_spec_dir(artifact_dir),
        )
    )
