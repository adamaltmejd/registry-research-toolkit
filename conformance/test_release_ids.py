"""Published ids retain an overlapping natural target across catalog releases."""

from __future__ import annotations

import json
import shutil
import subprocess

import pytest
from reader_artifacts import CASES, REG_BUILD, REPO, cached_reader_artifact


@pytest.mark.parametrize("change", ["variable", "table", "grown"])
def test_published_id_targets(tmp_path, change):
    # Fails if surviving pids conceal disjoint tables, spellings or periods, if exact
    # interval equality replaces overlap, or if register/implicit-table renames count
    # as table moves. Growth adds targets beside partial overlap. The golden follows
    # the source/ledgers; periods are half-open Unix days derived from source dates.
    fixture = CASES / "release-ids"
    expected = json.loads((fixture / "expected.json").read_text())[change]
    previous = cached_reader_artifact(fixture)
    curation = tmp_path / "curation"
    shutil.copytree(fixture / "curation", curation)
    if change != "grown":
        shutil.copyfile(
            fixture / f"{change}-ids.toml",
            curation / "registers/demo/r/ids.toml",
        )
    if change == "variable":
        shutil.copyfile(
            fixture / "variable-implicit-ids.toml",
            curation / "registers/demo/implicit/ids.toml",
        )

    def run(*args):
        return subprocess.run(
            [REG_BUILD, *args],
            cwd=REPO,
            capture_output=True,
            text=True,
            check=False,
        )

    current = tmp_path / "current.db"
    built = run(
        "build",
        "--inputs",
        fixture / ("grown" if change == "grown" else "inputs"),
        "--curation",
        curation,
        "--out",
        current,
        "--report",
        tmp_path / "report.json",
    )
    assert built.returncode == 0, built.stderr
    verified = run("verify", current, "--previous", previous)
    assert verified.returncode == (1 if expected else 0), verified.stderr
    if expected:
        first = expected[0]
        assert f"previous release: moved id {first['pid']}" in verified.stderr
        for target in [*first["old"], *first["new"]]:
            for value in target.values():
                if isinstance(value, list):
                    for day in value:
                        assert str(day) in verified.stderr
                else:
                    assert value in verified.stderr
        assert "retire" in verified.stderr and "mint" in verified.stderr

    diff = run("diff", previous, current, "--json")
    assert diff.returncode == 1, diff.stderr
    assert json.loads(diff.stdout)["ids"]["moved"] == expected
    text = run("diff", previous, current)
    assert text.returncode == 1, text.stderr
    ids_section = text.stdout.split("== ids ==\n")[1].split("==", 1)[0]
    assert ("moved:" in ids_section) == bool(expected)
    for item in expected:
        assert item["pid"] in ids_section
