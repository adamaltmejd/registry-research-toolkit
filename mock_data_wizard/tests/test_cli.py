"""Tests for CLI overwrite/force behavior."""

from __future__ import annotations

import json
from pathlib import Path

from mock_data_wizard.cli import main

from conftest import MINIMAL_STATS


def _setup(tmp_path: Path) -> tuple[Path, Path]:
    stats_path = tmp_path / "stats.json"
    stats_path.write_text(json.dumps(MINIMAL_STATS), encoding="utf-8")
    out_dir = tmp_path / "mock_data"
    out_dir.mkdir()
    (out_dir / "stale.csv").write_text("old data")
    return stats_path, out_dir


def test_yes_without_force_errors_when_dir_exists(tmp_path: Path):
    stats_path, out_dir = _setup(tmp_path)
    rc = main(
        [
            "generate",
            "--stats",
            str(stats_path),
            "--output-dir",
            str(out_dir),
            "--no-regmeta",
            "-y",
        ]
    )
    assert rc == 1
    # Stale file should still be there (nothing was overwritten)
    assert (out_dir / "stale.csv").exists()


def test_force_overwrites_and_removes_stale(tmp_path: Path):
    stats_path, out_dir = _setup(tmp_path)
    rc = main(
        [
            "generate",
            "--stats",
            str(stats_path),
            "--output-dir",
            str(out_dir),
            "--no-regmeta",
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
            "--no-regmeta",
            "-y",
            "--force",
        ]
    )
    assert rc == 0
    assert not (out_dir / "stale.csv").exists()
    assert (out_dir / "persons.csv").exists()


def test_force_on_empty_dir_works(tmp_path: Path):
    stats_path = tmp_path / "stats.json"
    stats_path.write_text(json.dumps(MINIMAL_STATS), encoding="utf-8")
    out_dir = tmp_path / "mock_data"
    rc = main(
        [
            "generate",
            "--stats",
            str(stats_path),
            "--output-dir",
            str(out_dir),
            "--no-regmeta",
            "--force",
            "-y",
        ]
    )
    assert rc == 0
    assert (out_dir / "persons.csv").exists()
