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
    # Text goldens catch Rust Debug output or unreadable dates at both CLI boundaries.
    # A v1 candidate also catches pre-v1 leniency accidentally extending past the freeze.
    fixture = CASES / "release-ids"
    expected = json.loads((fixture / "expected.json").read_text())[change]
    expected_text = json.loads((fixture / "expected-text.json").read_text())[change]
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
        "--catalog-version",
        "1.0.0",
        "--report",
        tmp_path / "report.json",
    )
    assert built.returncode == 0, built.stderr
    verified = run("verify", current, "--previous", previous)
    assert verified.returncode == (1 if expected else 0), verified.stderr
    if expected:
        assert f"previous release: moved id {expected_text[0]};" in verified.stderr
        assert "retire" in verified.stderr and "mint" in verified.stderr

    diff = run("diff", previous, current, "--json")
    assert diff.returncode == 1, diff.stderr
    assert json.loads(diff.stdout)["ids"]["moved"] == expected
    text = run("diff", previous, current)
    assert text.returncode == 1, text.stderr
    ids_section = text.stdout.split("== ids ==\n")[1].split("==", 1)[0]
    assert ("moved:" in ids_section) == bool(expected)
    for line in expected_text:
        assert line in ids_section


def test_removed_membership_id_requires_retirement(tmp_path, membership_case):
    # Regression: verify compares ident only, allowing a removed var_set pid to
    # disappear without retirement. Both retired and unretired twins use real builds;
    # these memberships are absent from the older moved-ident cases. Explicit v1
    # candidates catch retirement becoming optional past the freeze.
    fixture, _, curation, _ = membership_case

    def run(*args):
        return subprocess.run(
            [REG_BUILD, *args],
            cwd=REPO,
            capture_output=True,
            text=True,
            check=False,
        )

    def build(out, version="1.0.0"):
        built = run("lock", "--inputs", fixture / "inputs", "--curation", curation)
        assert built.returncode == 0, built.stderr
        built = run(
            "build",
            "--inputs",
            fixture / "inputs",
            "--curation",
            curation,
            "--out",
            out,
            "--catalog-version",
            version,
        )
        assert built.returncode == 0, built.stderr

    previous = tmp_path / "previous.db"
    build(previous)
    statements = curation / "registers/demo/r/curation.toml"
    text = statements.read_text()
    text = text.replace('sets = ["concept"]', "sets = []")
    statements.write_text(text)
    current = tmp_path / "current.db"
    build(current)
    refused = run("verify", current, "--previous", previous)
    pid = "demo/r/concept"
    assert refused.returncode == 1, refused.stderr
    assert "previous release:" in refused.stderr and pid in refused.stderr
    # Regression: enforcing v1 retirement rules on a major-zero candidate refuses
    # this removed membership. The readable golden pins its reported id and count.
    build(current, version="0.68.0")
    reported = run("verify", current, "--previous", previous)
    assert reported.returncode == 0, reported.stderr
    assert reported.stdout == (fixture / "pre-v1-expected.txt").read_text()
    (curation / "retired.toml").write_text(
        f'"{pid}" = {{ kind = "set", in = "1.0.0", reason = "withdrawn" }}\n'
    )
    build(current)
    accepted = run("verify", current, "--previous", previous)
    assert accepted.returncode == 0, accepted.stderr
