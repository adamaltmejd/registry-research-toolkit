"""Curation source fixtures → located lock refusal, before catalog publication."""

from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
from pathlib import Path

import pytest
from reader_artifacts import REG_BUILD, REPO


def test_assignments_refuse_concurrent_folds_at_lock(tmp_path):
    # Regression: assignment resolution accepts concurrent different folds in one
    # table, leaving only artifact verify to reject the resulting segment. The
    # one-day overlap must refuse beside nonconcurrent and cross-table allowed twins.
    case = Path(__file__).parent / "cases/lock/concurrent-fold-assignment"
    expected = json.loads((case / "expected.json").read_text())
    curation = tmp_path / "curation"
    shutil.copytree(case / "curation", curation)
    statements = curation / "registers/demo/assignment/curation.toml"
    command = [
        REG_BUILD,
        "lock",
        "--inputs",
        case / "inputs",
        "--curation",
        curation,
    ]
    statements.write_text((case / "allowed.toml").read_text())
    allowed = subprocess.run(
        command, cwd=REPO, capture_output=True, text=True, check=False
    )
    assert allowed.returncode == 0, allowed.stderr

    statements.write_text((case / "refused.toml").read_text())
    refused = subprocess.run(
        command, cwd=REPO, capture_output=True, text=True, check=False
    )
    assert refused.returncode == expected["exit_code"]
    # Bystander assignments at lines 1 and 7 must not receive this refusal.
    assert [
        "registers/" + line.split("registers/", 1)[1]
        for line in refused.stderr.splitlines()
        if "D-11:" in line
    ] == expected["located_errors"]


@pytest.mark.parametrize("change", ["singleton", "unknown-tag", "retired-set"])
def test_membership_refusals(tmp_path, membership_case, change):
    # Regression: one final member (even repeated), an unknown tag, or a retired
    # var_set pid is accepted. Existing assignment cases never create memberships.
    # The allowed twin has two distinct variables in different same-register tables;
    # fixture TOML and expected.json supply the located refusal and public name.
    fixture, overlay, curation, expected = membership_case

    def run(command):
        return subprocess.run(
            [
                REG_BUILD,
                command,
                "--inputs",
                fixture / "inputs",
                "--curation",
                curation,
                *(["--out", tmp_path / "catalog.db"] if command == "build" else []),
            ],
            cwd=REPO,
            capture_output=True,
            text=True,
            check=False,
        )

    allowed = run("lock")
    assert allowed.returncode == 0, allowed.stderr
    if change == "retired-set":
        shutil.copyfile(overlay / "retired.toml", curation / "retired.toml")
    else:
        shutil.copyfile(
            overlay / f"{change}.toml", curation / "registers/demo/r/curation.toml"
        )
        if change == "singleton":
            locked = run("lock")
            assert locked.returncode == 0, locked.stderr
    refusal = expected[change]
    refused = run(refusal["command"])
    assert refused.returncode == 1, refused.stderr
    assert f"{refusal['location']}: {refusal['code']}:" in refused.stderr
    assert refusal["pid"] in refused.stderr


def test_vocabulary_wording_needs_no_relock(tmp_path, membership_case):
    # Regression: vocabulary text becomes a premise/digest dependency, is omitted
    # from the build hash, or the emitter uses slugs instead of Swedish metadata.
    # The fixture's label/definition are the oracle, not the emitter's output.
    fixture, _, curation, expected = membership_case

    def run(command, out=None):
        result = subprocess.run(
            [
                REG_BUILD,
                command,
                "--inputs",
                fixture / "inputs",
                "--curation",
                curation,
                *(["--out", out] if out else []),
            ],
            cwd=REPO,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr

    run("lock")
    lock = curation / "registers/demo/r/curation.lock"
    original_lock = lock.read_bytes()
    current = tmp_path / "catalog.db"
    run("build", current)
    with sqlite3.connect(current) as db:
        assert (
            list(
                db.execute(
                    "SELECT n.body, d.body FROM var_set s "
                    "JOIN text n ON n.id=s.name_text "
                    "JOIN text d ON d.id=s.description_text WHERE s.pid='tag/income'"
                ).fetchone()
            )
            == expected["tag_text"]
        )
        original_hash = db.execute(
            "SELECT value FROM meta WHERE key='curation_digest'"
        ).fetchone()
    vocabulary = curation / "tag-vocabulary.toml"
    vocabulary.write_text(
        vocabulary.read_text().replace(
            expected["tag_text"][1], expected["reworded_definition"]
        )
    )
    run("build", current)
    run("lock")
    assert lock.read_bytes() == original_lock
    with sqlite3.connect(current) as db:
        assert (
            db.execute("SELECT value FROM meta WHERE key='curation_digest'").fetchone()
            != original_hash
        )
