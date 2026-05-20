"""Tests for CLI overwrite/force/warn-and-keep behavior."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from mock_data_wizard.cli import build_parser, main

from .conftest import MINIMAL_STATS


# -- `ui` subcommand parsing ----------------------------------------------


def test_ui_subcommand_returns_frozen_message(capsys):
    """The ``ui`` subcommand is a stub pending §15 step 7 deletion of
    the local editor + server + Svelte UI. It accepts no positional or
    flag arguments and exits with code 2 + a frozen-message hint."""
    rc = main(["ui"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "frozen" in err.lower()
    assert "project_data.json" in err


def test_ui_subcommand_takes_no_arguments():
    """Pre-step-4 the ``ui`` subcommand carried project_dir + a sheet
    of host/port flags; the stub drops them all."""
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["ui", "/some/path"])


def _setup(tmp_path: Path) -> tuple[Path, Path]:
    stats_path = tmp_path / "mock_data_stats.json"
    stats_path.write_text(json.dumps(MINIMAL_STATS), encoding="utf-8")
    out_dir = tmp_path / "mock_data"
    out_dir.mkdir()
    (out_dir / "stale.csv").write_text("old data")
    return stats_path, out_dir


def test_yes_keeps_stale_by_default(tmp_path: Path, capsys):
    """`-y` proceeds without prompting; stale files are kept (warn-and-keep default)."""
    stats_path, out_dir = _setup(tmp_path)
    rc = main(
        [
            "generate",
            "--stats",
            str(stats_path),
            "--output-dir",
            str(out_dir),
            "--no-reg-meta",
            "-y",
        ]
    )
    assert rc == 0
    # Stale file is still on disk
    assert (out_dir / "stale.csv").exists()
    # Mock CSV was produced
    assert (out_dir / "persons.csv").exists()
    # User saw the stale-files warning
    err = capsys.readouterr().err
    assert "stale" in err.lower()


def test_force_overwrites_and_removes_stale(tmp_path: Path):
    stats_path, out_dir = _setup(tmp_path)
    rc = main(
        [
            "generate",
            "--stats",
            str(stats_path),
            "--output-dir",
            str(out_dir),
            "--no-reg-meta",
            "--force",
        ]
    )
    assert rc == 0
    assert not (out_dir / "stale.csv").exists()
    assert (out_dir / "persons.csv").exists()
    assert (out_dir / "manifest.json").exists()


def test_yes_and_force_overwrites(tmp_path: Path):
    stats_path, out_dir = _setup(tmp_path)
    rc = main(
        [
            "generate",
            "--stats",
            str(stats_path),
            "--output-dir",
            str(out_dir),
            "--no-reg-meta",
            "-y",
            "--force",
        ]
    )
    assert rc == 0
    assert not (out_dir / "stale.csv").exists()
    assert (out_dir / "persons.csv").exists()


def test_force_on_empty_dir_works(tmp_path: Path):
    stats_path = tmp_path / "mock_data_stats.json"
    stats_path.write_text(json.dumps(MINIMAL_STATS), encoding="utf-8")
    out_dir = tmp_path / "mock_data"
    rc = main(
        [
            "generate",
            "--stats",
            str(stats_path),
            "--output-dir",
            str(out_dir),
            "--no-reg-meta",
            "--force",
            "-y",
        ]
    )
    assert rc == 0
    assert (out_dir / "persons.csv").exists()
