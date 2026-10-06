"""Sparse SCB value-role checkouts: index flags, cone rules and hydration."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from _csv_fixtures import (
    hydrate_scb_values,
    scb_values_role,
    sparsify_scb_values,
    write_input_bundle,
    write_scb_input,
    write_scb_snapshot,
)
from _snapshot_fixtures import git
from reg_meta_build.input_snapshot import (
    CatalogBundleSelection,
    ScbSnapshotSelection,
    SnapshotError,
    SnapshotMaterializationError,
    open_input_bundle,
    open_scb_snapshot,
    restore_snapshot,
    verify_input_bundle,
    verify_snapshot,
)

PROTOTYPE_CLI = Path(__file__).resolve().parents[2] / "scripts/prototype_scb_inputs.py"


def _git_index_evidence(repo: Path) -> tuple[bytes, bytes]:
    index_path = repo / git(repo, "rev-parse", "--git-path", "index")
    index_bytes = index_path.read_bytes()
    flags = subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "-c",
            "core.sparseCheckout=false",
            "ls-files",
            "--sparse",
            "-v",
            "--stage",
            "-z",
        ],
        check=True,
        capture_output=True,
        env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"},
    ).stdout
    assert index_path.read_bytes() == index_bytes
    return index_bytes, flags


def _cold_value_role_paths(selection: CatalogBundleSelection) -> list[str]:
    """Committed normalized files under the SCB value role, repository-relative."""
    role = scb_values_role(selection)
    return sorted(
        path
        for path in git(
            selection.path.parent, "ls-tree", "-r", "--name-only", "HEAD"
        ).splitlines()
        if path.startswith(f"{role}/")
    )


def test_sparse_value_role_preserves_declared_source_and_requires_hydration_for_proof(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "source"
    scb_dir = write_scb_input(input_dir)
    selection = write_input_bundle(tmp_path / "accepted", input_dir)
    complete = open_input_bundle(selection)
    source_hash = complete.snapshot.raw_sha256("Vardemangder.csv")
    role = scb_values_role(selection)
    cold_paths = sorted((complete.snapshot.root / "files/Vardemangder.csv").rglob("*"))
    committed_cold = {
        path
        for path in git(
            selection.path.parent, "ls-tree", "-r", "--name-only", "HEAD"
        ).splitlines()
        if path.startswith(f"{role}/")
    }
    assert committed_cold

    saved_directories = sparsify_scb_values(selection)
    assert all(not path.exists() for path in cold_paths)

    bundle = open_input_bundle(selection)
    assert bundle.snapshot.has_file("Vardemangder.csv")
    assert bundle.snapshot.raw_sha256("Vardemangder.csv") == source_hash
    assert (
        source_hash
        == hashlib.sha256((scb_dir / "Vardemangder.csv").read_bytes()).hexdigest()
    )
    assert {
        path
        for path in git(
            selection.path.parent, "ls-tree", "-r", "--name-only", "HEAD"
        ).splitlines()
        if path.startswith(f"{role}/")
    } == committed_cold

    for action in (
        bundle.snapshot.open_vardemangder,
        lambda: verify_input_bundle(selection),
        lambda: verify_snapshot(bundle.snapshot.root),
        lambda: restore_snapshot(bundle.snapshot.root, tmp_path / "restored"),
    ):
        with pytest.raises(SnapshotMaterializationError) as exc_info:
            action()
        assert str(selection.path.parent) in str(exc_info.value)
        assert selection.input_commit in str(exc_info.value)
        assert role in exc_info.value.hydration_action
    assert not (tmp_path / "restored").exists()

    hydrate_scb_values(selection)
    hydrated = open_input_bundle(selection)
    hydrated.snapshot.require_vardemangder_materialized()
    assert verify_snapshot(hydrated.snapshot.root) == hydrated.snapshot.manifest
    assert git(selection.path.parent, "rev-parse", "HEAD") == selection.input_commit
    assert not git(selection.path.parent, "status", "--porcelain=v1")

    restored_directories = sparsify_scb_values(selection)
    assert restored_directories == saved_directories
    open_input_bundle(selection)
    assert git(selection.path.parent, "rev-parse", "HEAD") == selection.input_commit
    assert not git(selection.path.parent, "status", "--porcelain=v1")


@pytest.mark.parametrize("index_representation", ("full", "preserved-condensed"))
def test_legitimate_sparse_reads_preserve_index_and_config_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    index_representation: str,
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    selection = write_input_bundle(tmp_path / "accepted", input_dir)
    complete = open_input_bundle(selection)
    snapshot_selection = ScbSnapshotSelection(
        path=complete.snapshot.root,
        input_commit=selection.input_commit,
        manifest_sha256=complete.manifest.scb_manifest_sha256,
    )
    directories = sparsify_scb_values(selection)
    repo = selection.path.parent
    index_path = repo / git(repo, "rev-parse", "--git-path", "index")
    full_index_bytes = index_path.read_bytes()
    if index_representation == "preserved-condensed":
        subprocess.run(
            [
                "git",
                "-C",
                str(repo),
                "sparse-checkout",
                "set",
                "--cone",
                "--sparse-index",
                "--stdin",
            ],
            input="".join(f"{path}\n" for path in directories),
            check=True,
            text=True,
        )
        condensed_index_bytes = index_path.read_bytes()
        assert condensed_index_bytes != full_index_bytes
        assert git(repo, "config", "--worktree", "--get", "index.sparse") == "true"
        git(repo, "config", "--worktree", "index.sparse", "false")
        assert index_path.read_bytes() == condensed_index_bytes
    assert git(repo, "config", "--worktree", "--get", "index.sparse") == "false"

    monkeypatch.delenv("GIT_OPTIONAL_LOCKS", raising=False)
    assert "GIT_OPTIONAL_LOCKS" not in os.environ
    config_path = repo / git(repo, "rev-parse", "--git-path", "config.worktree")
    before = _git_index_evidence(repo)
    config_bytes = config_path.read_bytes()
    assert any(
        entry.startswith(b"S ") and b"Vardemangder.csv/" in entry
        for entry in before[1].split(b"\0")
    )

    assert not open_scb_snapshot(snapshot_selection).vardemangder_materialized
    assert _git_index_evidence(repo) == before
    assert config_path.read_bytes() == config_bytes
    assert not open_input_bundle(selection).snapshot.vardemangder_materialized
    assert _git_index_evidence(repo) == before
    assert config_path.read_bytes() == config_bytes
    with pytest.raises(SnapshotMaterializationError):
        verify_snapshot(snapshot_selection.path)
    assert _git_index_evidence(repo) == before
    assert config_path.read_bytes() == config_bytes


@pytest.mark.parametrize("boundary", ("snapshot", "bundle", "proof"))
@pytest.mark.parametrize(
    "invalid_state", ("warm-hot-skip", "hydrated-hot-skip", "cold-replacements")
)
def test_sparse_boundaries_reject_without_normalizing_raw_index_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
    invalid_state: str,
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    selection = write_input_bundle(tmp_path / "accepted", input_dir)
    complete = open_input_bundle(selection)
    repo = selection.path.parent
    snapshot_selection = ScbSnapshotSelection(
        path=complete.snapshot.root,
        input_commit=selection.input_commit,
        manifest_sha256=complete.manifest.scb_manifest_sha256,
    )
    snapshot_relative = complete.snapshot.root.relative_to(repo).as_posix()
    cold = _cold_value_role_paths(selection)
    cold_payloads = {path: (repo / path).read_bytes() for path in cold}
    hot_item = next(
        item
        for item in complete.snapshot.manifest.files
        if item.name == "Registerinformation.csv"
    )
    hot = f"{snapshot_relative}/{hot_item.records[0].path}"
    sparsify_scb_values(selection)

    if invalid_state == "hydrated-hot-skip":
        hydrate_scb_values(selection)
    if invalid_state.endswith("hot-skip"):
        git(repo, "update-index", "--skip-worktree", hot)
        assert (repo / hot).is_file()
        expected = "outside the SCB Vardemangder role"
    else:
        for relative, payload in cold_payloads.items():
            path = repo / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)
        expected = "marked omitted by the accepted Git index"

    monkeypatch.delenv("GIT_OPTIONAL_LOCKS", raising=False)
    assert "GIT_OPTIONAL_LOCKS" not in os.environ
    before = _git_index_evidence(repo)
    expected_skips = (hot,) if invalid_state.endswith("hot-skip") else tuple(cold)
    assert all(
        any(
            entry.startswith(b"S ") and entry.endswith(b"\t" + relative.encode("utf-8"))
            for entry in before[1].split(b"\0")
        )
        for relative in expected_skips
    )
    trace = tmp_path / "git-trace.log"
    monkeypatch.setenv("GIT_TRACE", str(trace))
    actions = {
        "snapshot": lambda: open_scb_snapshot(snapshot_selection),
        "bundle": lambda: open_input_bundle(selection),
        "proof": lambda: verify_snapshot(snapshot_selection.path),
    }
    with pytest.raises(SnapshotError, match=expected):
        actions[boundary]()
    if invalid_state.endswith("hot-skip"):
        # The raw index rejection precedes any Git status run.
        assert not any(
            " status " in line
            for line in trace.read_text(encoding="utf-8").splitlines()
        )
    monkeypatch.delenv("GIT_TRACE")
    assert _git_index_evidence(repo) == before


def test_sparse_bundle_verifier_cli_reports_materialization_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from reg_meta.errors import EXIT_CONFIG

    from reg_meta_build import cli as cli_module

    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    selection = write_input_bundle(tmp_path / "accepted", input_dir)
    sparsify_scb_values(selection)

    exit_code = cli_module.run(
        [
            "verify-input-bundle",
            "--input-bundle",
            str(selection.path),
            "--input-commit",
            selection.input_commit,
            "--input-manifest-sha256",
            selection.manifest_sha256,
        ]
    )

    assert exit_code == EXIT_CONFIG
    error = json.loads(capsys.readouterr().out)["error"]
    assert error["code"] == "scb_snapshot_materialization_required"
    assert selection.input_commit in error["message"]
    assert "sparse-checkout add --stdin" in error["remediation"]


def test_prototype_verify_cli_reports_the_sparse_hydration_action(
    tmp_path: Path,
) -> None:
    selection = write_scb_snapshot(
        tmp_path / "accepted", write_scb_input(tmp_path / "source")
    )
    sparsify_scb_values(selection)

    process = subprocess.run(
        [sys.executable, str(PROTOTYPE_CLI), "verify", str(selection.path)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert process.returncode == 2, process.stdout
    assert process.stdout == ""
    assert process.stderr.startswith("error: ")
    for detail in (
        str(selection.path.parent),
        selection.input_commit,
        scb_values_role(selection),
        "sparse-checkout add --stdin",
    ):
        assert detail in process.stderr


@pytest.mark.parametrize(
    "invalid_layout",
    (
        "partial-cold",
        "missing-hot",
        "assume-unchanged-hot",
        "loose-cold-replacement",
        "complete-loose-cold-replacement",
        "non-cone",
        "sparse-index",
        "missing-sibling",
    ),
)
def test_sparse_value_role_rejects_incorrect_index_and_rule_state(
    tmp_path: Path, invalid_layout: str
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    selection = write_input_bundle(tmp_path / "accepted", input_dir)
    repo = selection.path.parent
    if invalid_layout == "missing-sibling":
        support = repo / "support" / "evidence.txt"
        support.parent.mkdir()
        support.write_text("accepted support\n", encoding="utf-8")
        git(repo, "add", ".")
        git(repo, "commit", "-q", "-m", "accepted support")
        selection = CatalogBundleSelection(
            selection.path,
            git(repo, "rev-parse", "HEAD"),
            selection.manifest_sha256,
        )
    bundle = open_input_bundle(selection)
    snapshot_relative = bundle.snapshot.root.relative_to(repo).as_posix()
    manifest = bundle.snapshot.manifest
    cold = _cold_value_role_paths(selection)
    hot_item = next(
        item for item in manifest.files if item.name == "Registerinformation.csv"
    )
    hot = f"{snapshot_relative}/{hot_item.records[0].path}"

    if invalid_layout == "partial-cold":
        git(repo, "update-index", "--skip-worktree", cold[0])
        (repo / cold[0]).unlink()
        expected = "complete SCB Vardemangder role"
    elif invalid_layout == "missing-hot":
        git(repo, "update-index", "--skip-worktree", hot)
        (repo / hot).unlink()
        expected = "outside the SCB Vardemangder role"
    elif invalid_layout == "assume-unchanged-hot":
        git(repo, "update-index", "--assume-unchanged", hot)
        path = repo / hot
        payload = bytearray(path.read_bytes())
        payload[0] = ord("A") if payload[0] != ord("A") else ord("B")
        path.write_bytes(payload)
        assert not git(repo, "status", "--porcelain=v1")
        expected = "assume-unchanged"
    else:
        cold_path = repo / cold[0]
        cold_payloads = {path: (repo / path).read_bytes() for path in cold}
        directories = sparsify_scb_values(selection)
        if invalid_layout == "loose-cold-replacement":
            cold_path.parent.mkdir(parents=True, exist_ok=True)
            cold_path.write_bytes(cold_payloads[cold[0]])
            expected = "marked omitted by the accepted Git index"
        elif invalid_layout == "complete-loose-cold-replacement":
            for relative, payload in cold_payloads.items():
                path = repo / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(payload)
            expected = "marked omitted by the accepted Git index"
        elif invalid_layout == "non-cone":
            git(repo, "config", "--worktree", "core.sparseCheckoutCone", "false")
            expected = "cone mode"
        elif invalid_layout == "sparse-index":
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(repo),
                    "sparse-checkout",
                    "set",
                    "--cone",
                    "--sparse-index",
                    "--stdin",
                ],
                input="".join(f"{path}\n" for path in directories),
                check=True,
                text=True,
            )
            expected = "normal full Git index"
        else:
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(repo),
                    "sparse-checkout",
                    "set",
                    "--cone",
                    "--no-sparse-index",
                    "--stdin",
                ],
                input="".join(
                    f"{path}\n"
                    for path in directories
                    if not path.startswith("support")
                ),
                check=True,
                text=True,
            )
            expected = "outside the SCB Vardemangder role"

    with pytest.raises(SnapshotError, match=expected):
        open_input_bundle(selection)
