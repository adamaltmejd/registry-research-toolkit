"""Tests for mock_data_wizard.update (version parsing, PyPI lookup, CLI wiring)."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

from mock_data_wizard import update
from mock_data_wizard.cli import main


class TestParseVersion:
    def test_final_release(self):
        assert update.parse_version("0.4.0") == (0, 4, 0, 0, 0)

    def test_alpha(self):
        assert update.parse_version("0.5.0a1") == (0, 5, 0, -1, 1)

    def test_dev(self):
        assert update.parse_version("0.5.0.dev3") == (0, 5, 0, -2, 3)

    def test_strips_v_prefix(self):
        assert update.parse_version("v1.2.3") == (1, 2, 3, 0, 0)

    def test_unparseable_returns_lowest(self):
        assert update.parse_version("garbage") == (0, 0, 0, -99, 0)

    @pytest.mark.parametrize(
        "older, newer",
        [
            ("0.4.0.dev1", "0.4.0a1"),
            ("0.4.0a1", "0.4.0"),
            ("0.4.0", "0.5.0"),
            ("0.4.0.dev1", "0.4.0.dev2"),
            ("0.4.0", "1.0.0"),
        ],
    )
    def test_ordering(self, older: str, newer: str):
        assert update.parse_version(older) < update.parse_version(newer)


@pytest.fixture
def isolated_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect the update-check cache to tmp_path so tests don't touch ~/."""
    monkeypatch.setenv("MOCK_DATA_WIZARD_STATE", str(tmp_path))
    return tmp_path


class TestUpdateChecker:
    def test_returns_newer_version(
        self, isolated_state: Path, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setattr(update, "__version__", "0.1.0")
        monkeypatch.setattr(
            update, "fetch_pypi_latest_version", lambda *, timeout=10: "0.2.0"
        )
        checker = update.UpdateChecker()
        assert checker.get_newer_version(timeout=5) == "0.2.0"
        assert checker.completed

    def test_returns_none_when_up_to_date(
        self, isolated_state: Path, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setattr(update, "__version__", "0.2.0")
        monkeypatch.setattr(
            update, "fetch_pypi_latest_version", lambda *, timeout=10: "0.2.0"
        )
        checker = update.UpdateChecker()
        assert checker.get_newer_version(timeout=5) is None
        assert checker.completed

    def test_writes_cache_on_fresh_check(
        self, isolated_state: Path, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setattr(update, "__version__", "0.1.0")
        monkeypatch.setattr(
            update, "fetch_pypi_latest_version", lambda *, timeout=10: "0.2.0"
        )
        checker = update.UpdateChecker()
        checker.get_newer_version(timeout=5)
        cache = json.loads((isolated_state / ".update_check").read_text())
        assert cache["latest_version"] == "0.2.0"

    def test_reads_cache_when_fresh(
        self, isolated_state: Path, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setattr(update, "__version__", "0.1.0")
        cache_path = isolated_state / ".update_check"
        cache_path.write_text(
            json.dumps(
                {"timestamp": __import__("time").time(), "latest_version": "0.9.0"}
            )
        )

        def _should_not_be_called(*, timeout: float = 10) -> str:
            raise AssertionError("PyPI hit despite fresh cache")

        monkeypatch.setattr(update, "fetch_pypi_latest_version", _should_not_be_called)
        checker = update.UpdateChecker()
        assert checker.get_newer_version(timeout=5) == "0.9.0"

    def test_network_failure_leaves_cache_untouched(
        self, isolated_state: Path, monkeypatch: pytest.MonkeyPatch
    ):
        def _boom(*, timeout: float = 10) -> str:
            raise RuntimeError("offline")

        monkeypatch.setattr(update, "__version__", "0.1.0")
        monkeypatch.setattr(update, "fetch_pypi_latest_version", _boom)
        checker = update.UpdateChecker()
        assert checker.get_newer_version(timeout=5) is None
        assert not (isolated_state / ".update_check").exists()


def _proc(returncode: int, stdout: str = "", stderr: str = "") -> Any:
    class _P:
        pass

    p = _P()
    p.returncode = returncode
    p.stdout = stdout
    p.stderr = stderr
    return p


class TestRunUpdate:
    def test_already_up_to_date(
        self, isolated_state: Path, monkeypatch: pytest.MonkeyPatch, capsys
    ):
        monkeypatch.setattr(update, "__version__", "0.4.0")
        monkeypatch.setattr(
            update, "fetch_pypi_latest_version", lambda *, timeout=10: "0.4.0"
        )
        called = []
        monkeypatch.setattr(
            subprocess, "run", lambda *a, **kw: called.append(a) or _proc(0)
        )
        assert update.run_update() == 0
        assert called == []  # no upgrade attempted
        assert "Already up to date" in capsys.readouterr().err

    def test_upgrades_when_newer(
        self, isolated_state: Path, monkeypatch: pytest.MonkeyPatch, capsys
    ):
        monkeypatch.setattr(update, "__version__", "0.4.0")
        monkeypatch.setattr(
            update, "fetch_pypi_latest_version", lambda *, timeout=10: "0.5.0"
        )
        calls: list[tuple[Any, ...]] = []

        def _fake_run(*args: Any, **kwargs: Any) -> Any:
            calls.append(args)
            return _proc(0, stdout="Upgraded mock-data-wizard from 0.4.0 to 0.5.0\n")

        monkeypatch.setattr(subprocess, "run", _fake_run)
        assert update.run_update() == 0
        assert calls and "uv" in calls[0][0][0]
        assert "Upgrading package: v0.4.0 → v0.5.0" in capsys.readouterr().err

    def test_reports_no_upgrade_when_uv_nothing_to_upgrade(
        self, isolated_state: Path, monkeypatch: pytest.MonkeyPatch, capsys
    ):
        monkeypatch.setattr(update, "__version__", "0.4.0")
        monkeypatch.setattr(
            update, "fetch_pypi_latest_version", lambda *, timeout=10: "0.5.0"
        )
        monkeypatch.setattr(
            subprocess,
            "run",
            lambda *a, **kw: _proc(0, stdout="Nothing to upgrade\n"),
        )
        assert update.run_update() == 0
        err = capsys.readouterr().err
        assert "nothing to upgrade" in err.lower()

    def test_uv_missing(
        self, isolated_state: Path, monkeypatch: pytest.MonkeyPatch, capsys
    ):
        monkeypatch.setattr(update, "__version__", "0.4.0")
        monkeypatch.setattr(
            update, "fetch_pypi_latest_version", lambda *, timeout=10: "0.5.0"
        )

        def _raise(*a: Any, **kw: Any) -> Any:
            raise FileNotFoundError("uv")

        monkeypatch.setattr(subprocess, "run", _raise)
        assert update.run_update() == update.EXIT_CONFIG
        assert "uv is not installed" in capsys.readouterr().err


class TestCliWiring:
    def test_update_subcommand_calls_run_update(
        self, isolated_state: Path, monkeypatch: pytest.MonkeyPatch
    ):
        called: list[int] = []

        def _fake_update() -> int:
            called.append(1)
            return 0

        monkeypatch.setattr("mock_data_wizard.update.run_update", _fake_update)
        assert main(["update"]) == 0
        assert called == [1]

    def test_version_flag_prints_version(
        self, isolated_state: Path, monkeypatch: pytest.MonkeyPatch, capsys
    ):
        monkeypatch.setattr(
            "mock_data_wizard.update.fetch_pypi_latest_version",
            lambda *, timeout=10: "0.4.0",
        )
        rc = main(["--version"])
        assert rc == 0
        err = capsys.readouterr().err
        assert "mock-data-wizard v" in err
