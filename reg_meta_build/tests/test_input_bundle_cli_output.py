"""CLI JSON --output confinement for the input-bundle commands.

`reg-meta-build --output` must never land inside the accepted input repository.
The rejection comes before the command runs: a selection whose pin is wrong still
reports the output conflict, not the command's own selection error, and nothing is
written to the requested path, the bundle checkout or the candidate directory.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import write_input_bundle, write_scb_input
from reg_meta.db import DB_FILENAME
from reg_meta.errors import EXIT_CONFIG
from reg_meta_build.cli import run
from reg_meta_build.input_snapshot import open_input_bundle

if TYPE_CHECKING:
    from pathlib import Path

_COMMANDS = ("prepare-input-bundle", "verify-input-bundle", "inspect-source-records")


def _command_args(
    command: str, input_dir: Path, bundle, selection, *, pin: str
) -> list[str]:
    if command == "prepare-input-bundle":
        return [
            command,
            "--input-dir",
            str(input_dir),
            "--scb-snapshot",
            str(bundle.snapshot.root),
            "--scb-input-commit",
            pin,
            "--scb-manifest-sha256",
            bundle.manifest.scb_manifest_sha256,
            "--output-dir",
            str(bundle.repository / "candidate"),
        ]
    return [
        command,
        "--input-bundle",
        str(bundle.root),
        "--input-commit",
        pin,
        "--input-manifest-sha256",
        selection.manifest_sha256,
    ]


@pytest.fixture()
def accepted(tmp_path: Path):
    input_dir = tmp_path / "input"
    write_scb_input(input_dir)
    selection = write_input_bundle(tmp_path / "accepted", input_dir)
    return input_dir, selection, open_input_bundle(selection)


@pytest.mark.parametrize("command", _COMMANDS)
@pytest.mark.parametrize(
    ("destination_kind", "valid_pin"),
    (("tracked", True), ("untracked", False)),
    ids=("tracked-valid-pin", "untracked-wrong-pin"),
)
def test_output_inside_input_repository_is_rejected_before_the_command_runs(
    accepted,
    capsys: pytest.CaptureFixture[str],
    command: str,
    destination_kind: str,
    valid_pin: bool,
) -> None:
    input_dir, selection, bundle = accepted
    repository = bundle.repository
    output = (
        (
            bundle.snapshot.root / "manifest.json"
            if command == "prepare-input-bundle"
            else bundle.root / "catalog-bundle.json"
        )
        if destination_kind == "tracked"
        else repository / "results" / f"{command}.json"
    )
    original_output = output.read_bytes() if output.exists() else None
    assert (original_output is None) == (destination_kind == "untracked")
    pin = selection.input_commit if valid_pin else "0" * 40
    args = _command_args(command, input_dir, bundle, selection, pin=pin)

    exit_code = run(["--output", str(output), *args])

    assert exit_code == EXIT_CONFIG
    error = json.loads(capsys.readouterr().out)["error"]
    assert error["code"] == "catalog_input_output_conflict"
    assert "must stay outside the accepted input repository" in error["message"]
    assert str(output) in error["message"]
    if original_output is None:
        assert not output.exists()
    else:
        assert output.read_bytes() == original_output
    assert not (repository / "results").exists()
    assert not (repository / "candidate").exists()
    open_input_bundle(selection)

    if not valid_pin:
        # Without --output the same wrong pin is the command's own error, so the
        # conflict above was reported before the command ran.
        assert run(args) == EXIT_CONFIG
        own_error = json.loads(capsys.readouterr().out)["error"]
        assert own_error["code"] != "catalog_input_output_conflict"
        assert not (repository / "results").exists()
        assert not (repository / "candidate").exists()


class TestBundleCommandOutputProtection:
    @pytest.mark.parametrize(
        "command",
        (
            "prepare-input-bundle",
            "verify-input-bundle",
            "inspect-source-records",
        ),
        ids=("prepare", "verify", "inspect"),
    )
    def test_missing_bundle_selection_suppresses_tracked_and_untracked_output(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        command: str,
    ) -> None:
        from reg_meta.errors import EXIT_CONFIG
        from reg_meta_build.input_snapshot import open_input_bundle

        from reg_meta_build import cli as cli_mod

        input_dir = tmp_path / "input"
        write_scb_input(input_dir)
        selection = write_input_bundle(tmp_path / "accepted", input_dir)
        bundle = open_input_bundle(selection)
        repository = bundle.repository
        tracked_output = bundle.root / "catalog-bundle.json"
        tracked_bytes = tracked_output.read_bytes()
        db_dir = tmp_path / "db"
        db_dir.mkdir()
        published = db_dir / DB_FILENAME
        published_bytes = b"EXISTING-CATALOG"
        published.write_bytes(published_bytes)
        missing_selection = tmp_path / "missing-selection"
        candidate = repository / "candidate"

        if command == "prepare-input-bundle":
            command_args = [
                command,
                "--input-dir",
                str(input_dir),
                "--scb-snapshot",
                str(missing_selection),
                "--scb-input-commit",
                "0" * 40,
                "--scb-manifest-sha256",
                "0" * 64,
                "--output-dir",
                str(candidate),
            ]
        else:
            command_args = [
                command,
                "--input-bundle",
                str(missing_selection),
                "--input-commit",
                "0" * 40,
                "--input-manifest-sha256",
                "0" * 64,
            ]

        for output in (
            tracked_output,
            repository / "results" / f"{command}.json",
        ):
            exit_code = cli_mod.run(["--output", str(output), *command_args])

            assert exit_code == EXIT_CONFIG
            error = json.loads(capsys.readouterr().out)["error"]
            assert error["code"] == "catalog_input_bundle_invalid"
            assert "not found" in error["message"]
            assert tracked_output.read_bytes() == tracked_bytes
            assert published.read_bytes() == published_bytes
            assert not (repository / "results").exists()
            assert not candidate.exists()
            open_input_bundle(selection)
