"""Tests for CLI overwrite/force/warn-and-keep behavior."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from mock_data_wizard.cli import build_parser, main

from .conftest import MINIMAL_STATS


# -- `ui` subcommand parsing ----------------------------------------------


def test_ui_subcommand_parses_minimum_args():
    parser = build_parser()
    args = parser.parse_args(["ui", "/tmp/proj"])
    assert args.command == "ui"
    assert args.project_dir == "/tmp/proj"
    assert args.host == "127.0.0.1"
    assert args.port == 8765
    assert args.unsafe_host is False
    assert args.no_browser is False
    assert args.db_path is None


def test_ui_subcommand_overrides():
    parser = build_parser()
    args = parser.parse_args(
        [
            "ui",
            "/tmp/proj",
            "--port",
            "9000",
            "--host",
            "::1",
            "--no-browser",
            "--db-path",
            "/tmp/reg_meta.db",
        ]
    )
    assert args.port == 9000
    assert args.host == "::1"
    assert args.no_browser is True
    assert args.db_path == "/tmp/reg_meta.db"


def test_ui_requires_project_dir():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["ui"])


def test_ui_rejects_non_loopback_without_unsafe(tmp_path: Path, capsys):
    """Concrete safety gate: 0.0.0.0 without --unsafe-host fails fast."""
    rc = main(["ui", str(tmp_path), "--host", "0.0.0.0", "--no-browser"])
    assert rc == 2
    captured = capsys.readouterr()
    assert "refusing to bind" in captured.err


def test_ui_brackets_ipv6_url(tmp_path: Path, capsys, monkeypatch):
    """`--host ::1` must print/open `http://[::1]:PORT/`, not the
    raw `http://::1:PORT/` (which is invalid because of the colon
    collision)."""
    from mock_data_wizard import server as server_mod

    # Build the server, capture the URL, then trigger KeyboardInterrupt
    # in serve_forever so the CLI exits without blocking.
    captured_url: list[str] = []

    real_open = server_mod.build_server

    def _wrap(config: server_mod.ServerConfig):
        httpd = real_open(config)
        original = httpd.serve_forever

        def _capture_and_stop(*a, **kw):
            captured_url.append(httpd.server_address[0])
            raise KeyboardInterrupt

        httpd.serve_forever = _capture_and_stop  # type: ignore[method-assign]
        # Restore for downstream cleanup; serve_forever raises immediately.
        del original
        return httpd

    monkeypatch.setattr(server_mod, "build_server", _wrap)
    rc = main(["ui", str(tmp_path), "--host", "::1", "--no-browser", "--port", "0"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "http://[::1]:" in out, f"expected bracketed IPv6 URL in: {out!r}"


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
