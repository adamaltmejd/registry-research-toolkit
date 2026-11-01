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


def test_inherited_identity_uses_remaining_ledger_anchors(tmp_path):
    # Regression: the alphabetically first ledger id owns the whole default source
    # key, so assigning its earlier spelling silently absorbs the later spelling.
    # Existing assignment cases use different source keys. The dated anchor must
    # also identify the year after its day; fixture source/ledger supply the oracle.
    case = Path(__file__).parent / "cases/lock/ledger-inheritance"
    expected = json.loads((case / "expected.json").read_text())
    curation = tmp_path / "curation"
    shutil.copytree(case / "curation", curation)
    catalog = tmp_path / "catalog.db"
    for command in ("lock", "build"):
        result = subprocess.run(
            [
                REG_BUILD,
                command,
                "--inputs",
                case / "inputs",
                "--curation",
                curation,
                *(["--release", "--out", catalog] if command == "build" else []),
            ],
            cwd=REPO,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
    with sqlite3.connect(catalog) as db:
        assert [
            list(row)
            for row in db.execute(
                "SELECT v.pid, s.exact, date(o.start * 86400, 'unixepoch') "
                "FROM occurrence o JOIN variable v ON v.id=o.variable_id "
                "JOIN spelling s ON s.id=o.spelling_id ORDER BY o.start"
            )
        ] == expected


@pytest.mark.parametrize("change", ["singleton", "unknown-tag", "retired-set"])
def test_membership_refusals(tmp_path, membership_case, change):
    # Regression: source occurrences are counted as set members instead of final
    # variables, an unknown tag is accepted, or a retired var_set pid is accepted.
    # Existing assignment cases never create memberships. The singleton uses three
    # nonconcurrent source occurrences resolving to the same final variable.
    # The allowed twin has two distinct variables in different same-register tables;
    # fixture TOML and expected.json supply the located refusal and public name.
    fixture, overlay, curation, expected = membership_case

    def run(command):
        return subprocess.run(
            [
                REG_BUILD,
                command,
                "--inputs",
                fixture / ("grown" if change == "singleton" else "inputs"),
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


def test_variable_premise_tracks_matches_not_occurrence_fields(
    tmp_path, membership_case
):
    # Regression: variable metadata locks occurrence/container field values, repeats
    # large container descriptions, or retains only a match count and misses a
    # same-count locator replacement. Existing vocabulary wording coverage never
    # changes source rows. Source overlays and expected JSON supply the allowed
    # field drift, entry-size ceiling, matched-row counts and located refusal.
    fixture, overlay, curation, expected = membership_case
    expected = expected["variable_premise"]
    shutil.copyfile(
        overlay / "variable-level.toml", curation / "registers/demo/r/curation.toml"
    )
    inputs = tmp_path / "inputs"
    shutil.copytree(fixture / "inputs", inputs)
    source = inputs / "frame/registers.jsonl"
    report = tmp_path / "report.json"
    shutil.copyfile(overlay / "container-baseline.jsonl", source)

    def run(command):
        return subprocess.run(
            [
                REG_BUILD,
                command,
                "--inputs",
                inputs,
                "--curation",
                curation,
                *(
                    [
                        "--release",
                        "--out",
                        tmp_path / "catalog.db",
                        "--report",
                        report,
                    ]
                    if command == "build"
                    else []
                ),
            ],
            cwd=REPO,
            capture_output=True,
            text=True,
            check=False,
        )

    locked = run("lock")
    assert locked.returncode == 0, locked.stderr
    lock = json.loads((curation / "registers/demo/r/curation.lock").read_text())
    assert all(
        len(json.dumps({key: value}, separators=(",", ":")).encode())
        <= expected["max_entry_bytes"]
        for key, value in lock["statements"].items()
    )
    shutil.copyfile(overlay / "containers-changed.jsonl", source)
    containers = run("build")
    assert containers.returncode == 0, containers.stderr
    (tmp_path / "catalog.db").unlink()
    shutil.copyfile(overlay / "fields-changed.jsonl", source)
    allowed = run("build")
    assert allowed.returncode == 0, allowed.stderr
    (tmp_path / "catalog.db").unlink()

    shutil.copyfile(overlay / "match-replaced.jsonl", source)
    replaced = run("build")
    assert replaced.returncode == 1, replaced.stderr
    refusals = "\n".join(
        detail
        for refusal in json.loads(report.read_text())["refusals"]
        for detail in refusal["details"]
    )
    assert f"{expected['location']}: {expected['code']}:" in refusals
    assert expected["subject"] in refusals
    count = expected["original_count"]
    assert f"old count {count}, new count {count}" in refusals
    shutil.copyfile(fixture / "grown/frame/registers.jsonl", source)
    grown = run("build")
    assert grown.returncode == 1, grown.stderr
    refusals = "\n".join(
        detail
        for refusal in json.loads(report.read_text())["refusals"]
        for detail in refusal["details"]
    )
    assert f"{expected['location']}: {expected['code']}:" in refusals
    assert expected["subject"] in refusals
    assert f"old count {count}, new count {expected['grown_count']}" in refusals

    # Regression: a v1 lock is silently accepted rather than requiring a re-lock.
    shutil.copyfile(overlay / "v1.lock", curation / "registers/demo/r/curation.lock")
    obsolete = run("build")
    assert obsolete.returncode == 1, obsolete.stderr
    assert "unsupported premise lock version 1" in obsolete.stderr
    assert "run reg-build lock" in obsolete.stderr
