"""Raw inputs and curation produce one complete refusal report before publication."""

from __future__ import annotations

import json
import subprocess

from reader_artifacts import CASES, REG_BUILD, REPO


def test_release_collects_all_refusals_before_publication(tmp_path):
    # Fails if release curation or any register's first ledger refusal returns before
    # the report, loses later ids/registers, or publishes despite a refusal. No existing
    # conformance case exercises a refused build; expected ids/counts come from this
    # fixture's source and ledger, including two split and unplaced ids per register.
    fixture = CASES / "build-report"
    expected = json.loads((fixture / "expected.json").read_text())
    command = [
        REG_BUILD,
        "build",
        "--inputs",
        fixture / "inputs",
        "--curation",
        fixture / "curation",
    ]
    dev_report = tmp_path / "dev.json"
    dev = subprocess.run(
        [*command, "--out", tmp_path / "dev.db", "--report", dev_report],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=False,
    )
    assert dev.returncode == 0, dev.stderr
    previous_catalog = b"previous catalog must survive a refused build\n"
    catalog = tmp_path / "catalog.db"
    catalog.write_bytes(previous_catalog)
    release_report = tmp_path / "release.json"
    release = subprocess.run(
        [
            *command,
            "--release",
            "--out",
            catalog,
            "--report",
            release_report,
        ],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=False,
    )
    assert release.returncode == 1, release.stderr
    assert release_report.is_file(), release.stderr
    assert catalog.read_bytes() == previous_catalog
    report = json.loads(release_report.read_text())
    normal_report = json.loads(dev_report.read_text())
    assert report["registers"] == expected["registers"]
    refusals = report.pop("refusals")
    assert normal_report.pop("refusals") == []
    assert report == normal_report

    counts = {}
    for refusal in refusals:
        register = refusal["register"] or "curation"
        counts.setdefault(register, {})[refusal["kind"]] = len(refusal["details"])
        assert (
            f"{register}: {len(refusal['details'])} {refusal['kind']}" in release.stderr
        )
        if refusal["kind"] == "unminted-variable":
            assert refusal["details"] == expected["registers"][register]["unminted"]
        elif refusal["kind"].startswith("ledger-id-"):
            key = (
                "split_ids" if refusal["kind"] == "ledger-id-split" else "unplaced_ids"
            )
            for ident, detail in zip(
                expected["registers"][register][key], refusal["details"], strict=True
            ):
                assert f'ledger id "{ident}"' in detail
                assert f"/registers/{register}/ids.toml:" in detail
                assert refusal["kind"] in detail
        elif refusal["kind"] == "curation":
            assert any(
                expected["curation_refusals"][register] in detail
                for detail in refusal["details"]
            )
    assert counts == expected["refusal_counts"]
    assert str(release_report) in release.stderr
    for detail in (
        "fresh",
        "u-missing-one",
        "u-missing-two",
        "z-split-one",
        "z-split-two",
        "premise-missing",
    ):
        assert detail not in release.stderr
