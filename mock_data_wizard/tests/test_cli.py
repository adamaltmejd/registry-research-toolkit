"""Tests for CLI overwrite/force/warn-and-keep behavior."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

# conftest.py inserts reg_monabundle/tests onto sys.path (it is imported first
# by pytest), so the shared Model A spec builder resolves here.
from _project_data_fixtures import make_project_data, write_project_data  # noqa: E402
from mock_data_wizard.cli import main

from .conftest import MINIMAL_STATS

if TYPE_CHECKING:
    from pathlib import Path

# -- build-bundle --project-data error handling ---------------------------


def test_build_bundle_project_data_malformed_json_clean_error(tmp_path: Path, capsys):
    """Hand-editing project_data.json is the common local workflow;
    a JSON syntax error must surface as ``Error: ...`` not a traceback."""
    bad = tmp_path / "project_data.json"
    bad.write_text("{this is not valid json", encoding="utf-8")
    rc = main(
        [
            "build-bundle",
            "--output",
            str(tmp_path / "bundle.py"),
            "--project-data",
            str(bad),
        ]
    )
    assert rc == 1
    err = capsys.readouterr().err
    assert "Error:" in err
    assert "not valid JSON" in err
    assert "Traceback" not in err


def test_build_bundle_project_data_duplicate_keys_clean_error(tmp_path: Path, capsys):
    """``_reject_duplicate_keys`` raises ValueError (not JSONDecodeError)
    from inside ``json.load``; the dup-key path must also land in the
    friendly ``Error: ...`` branch."""
    bad = tmp_path / "project_data.json"
    bad.write_text(
        '{"schema_version": "1.0.0", "schema_version": "2.0.0", '
        '"steward": "global", "reg_meta_version": "test", '
        '"name": "x", "sources": [], "panels": []}',
        encoding="utf-8",
    )
    rc = main(
        [
            "build-bundle",
            "--output",
            str(tmp_path / "bundle.py"),
            "--project-data",
            str(bad),
        ]
    )
    assert rc == 1
    err = capsys.readouterr().err
    assert "Error:" in err
    assert "duplicate key" in err
    assert "Traceback" not in err


def test_build_bundle_project_data_invalid_schema_clean_error(tmp_path: Path, capsys):
    """Structural validation failures (missing required field, bad
    types, composite key, etc.) likewise route through the friendly
    ``Error: ...`` path rather than a ValueError traceback."""
    bad = tmp_path / "project_data.json"
    # Drop the required ``steward`` field — structural validator raises.
    bad.write_text(
        json.dumps(
            {
                "schema_version": "1.0.0",
                "reg_meta_version": "test",
                "name": "x",
                "sources": [],
                "panels": [],
            }
        ),
        encoding="utf-8",
    )
    rc = main(
        [
            "build-bundle",
            "--output",
            str(tmp_path / "bundle.py"),
            "--project-data",
            str(bad),
        ]
    )
    assert rc == 1
    err = capsys.readouterr().err
    assert "Error:" in err
    assert "failed validation" in err
    assert "Traceback" not in err


def test_build_bundle_model_a_spec_succeeds(tmp_path: Path, capsys):
    """Happy path: a Model A project_data.json (3-seg binding FQIDs,
    register_variant + period, a reg_monabundle.binding_options block keyed by
    a 3-seg variable FQID) builds a bundle cleanly. Pins the mdw
    cli → reg_monabundle.runtime.spec contract against an accidental v0.x
    reintroduction — the error-path tests above only prove rejection (A3.2)."""
    spec = make_project_data(
        sources=[
            {
                "name": "lisa_2020",
                "register_variant": "scb/lisa/individer-15plus",
                "period": 2020,
                "bindings": [
                    {"display_name": "LopNr", "type": "id", "id_subtype": "integer"},
                    {
                        "variable": "scb/lisa/kon",
                        "display_name": "Kon",
                        "type": "categorical",
                    },
                ],
            }
        ],
        reg_monabundle={"binding_options": {"scb/lisa/kon": {"suppress_k": 25}}},
    )
    spec_path = write_project_data(tmp_path, spec)
    out = tmp_path / "bundle.py"
    rc = main(["build-bundle", "--output", str(out), "--project-data", str(spec_path)])
    assert rc == 0, capsys.readouterr().err
    assert out.is_file() and out.stat().st_size > 0
    # The Model A spec is embedded (3-seg binding FQID present).
    assert "scb/lisa/kon" in out.read_text(encoding="utf-8")


def test_build_bundle_step4_unsupported_spec_fails_fast(tmp_path: Path, capsys):
    """A datetime binding is structurally VALID (reg_schema accepts it) but the
    mdw runtime can't execute it. The build must fail fast at the step-4
    capability gate, not embed a bundle that dies deep in the runner on MONA
    (review P2 on #157 — A3.4 moved that gate off the build path)."""
    spec = make_project_data(
        sources=[
            {
                "name": "a.csv",
                "register_variant": "scb/test/_default",
                "period": 2020,
                "bindings": [
                    {
                        "variable": "scb/test/ts",
                        "display_name": "Ts",
                        "type": "datetime",
                    }
                ],
            }
        ],
    )
    spec_path = write_project_data(tmp_path, spec)
    out = tmp_path / "bundle.py"
    rc = main(["build-bundle", "--output", str(out), "--project-data", str(spec_path)])
    assert rc == 1
    err = capsys.readouterr().err
    assert "failed validation" in err
    assert "datetime" in err
    assert not out.exists()  # nothing embedded


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
