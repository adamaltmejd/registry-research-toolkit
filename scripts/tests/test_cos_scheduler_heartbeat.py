"""Tests for the chief-of-staff foreground heartbeat wrapper."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "cos_scheduler_heartbeat.sh"


def _exe(path: Path, body: str) -> Path:
    path.write_text(body, encoding="utf-8")
    path.chmod(0o755)
    return path


def _clean_env() -> dict[str, str]:
    env = os.environ.copy()
    for key in list(env):
        if key.startswith("COS_"):
            env.pop(key)
    return env


def test_requires_thread_id(tmp_path: Path) -> None:
    tick = _exe(tmp_path / "tick", "#!/usr/bin/env bash\nexit 0\n")

    result = subprocess.run(
        [str(SCRIPT), "--tick-bin", str(tick), "--max-ticks", "1"],
        capture_output=True,
        text=True,
        check=False,
        env=_clean_env(),
    )

    assert result.returncode == 2
    assert "missing THREAD_ID" in result.stderr


def test_single_tick_uses_positional_thread(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    tick_log = tmp_path / "tick.log"
    tick = _exe(
        tmp_path / "tick",
        f"#!/usr/bin/env bash\nprintf '%s\\n' \"$@\" > {tick_log}\n",
    )

    result = subprocess.run(
        [
            str(SCRIPT),
            "thread-1",
            "--repo",
            str(repo),
            "--tick-bin",
            str(tick),
            "--max-ticks",
            "1",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=_clean_env(),
    )

    assert result.returncode == 0
    assert "chief-of-staff scheduler tick 1" in result.stdout
    assert "reached max ticks (1)" in result.stdout
    assert tick_log.read_text(encoding="utf-8").splitlines() == [
        "--repo",
        str(repo),
        "--thread",
        "thread-1",
    ]


def test_forwards_tick_options(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    state_file = tmp_path / "state.json"
    tick_log = tmp_path / "tick.log"
    tick = _exe(
        tmp_path / "tick",
        f"#!/usr/bin/env bash\nprintf '%s\\n' \"$@\" > {tick_log}\n",
    )

    result = subprocess.run(
        [
            str(SCRIPT),
            "--thread",
            "thread-1",
            "--repo",
            str(repo),
            "--prompt",
            "Run one COS tick",
            "--state-file",
            str(state_file),
            "--wake-backend",
            "exec",
            "--app-wake-bin",
            "/tmp/fake-app-wake.py",
            "--wake-timeout",
            "123",
            "--codex-bin",
            "/tmp/fake-codex",
            "--uv-bin",
            "/tmp/fake-uv",
            "--tick-bin",
            str(tick),
            "--max-ticks",
            "1",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=_clean_env(),
    )

    assert result.returncode == 0
    assert tick_log.read_text(encoding="utf-8").splitlines() == [
        "--repo",
        str(repo),
        "--thread",
        "thread-1",
        "--prompt",
        "Run one COS tick",
        "--state-file",
        str(state_file),
        "--wake-backend",
        "exec",
        "--app-wake-bin",
        "/tmp/fake-app-wake.py",
        "--wake-timeout",
        "123",
        "--codex-bin",
        "/tmp/fake-codex",
        "--uv-bin",
        "/tmp/fake-uv",
    ]


def test_thread_can_come_from_env(tmp_path: Path) -> None:
    tick_log = tmp_path / "tick.log"
    tick = _exe(
        tmp_path / "tick",
        f"#!/usr/bin/env bash\nprintf '%s\\n' \"$@\" > {tick_log}\n",
    )
    env = _clean_env()
    env["COS_THREAD_ID"] = "thread-env"

    result = subprocess.run(
        [str(SCRIPT), "--tick-bin", str(tick), "--max-ticks", "1"],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 0
    assert "--thread\nthread-env" in tick_log.read_text(encoding="utf-8")


def test_invalid_interval_is_rejected(tmp_path: Path) -> None:
    tick = _exe(tmp_path / "tick", "#!/usr/bin/env bash\nexit 0\n")

    result = subprocess.run(
        [
            str(SCRIPT),
            "thread-1",
            "--interval",
            "0",
            "--tick-bin",
            str(tick),
            "--max-ticks",
            "1",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=_clean_env(),
    )

    assert result.returncode == 2
    assert "--interval must be a positive integer" in result.stderr


def test_tick_failure_stops_loop(tmp_path: Path) -> None:
    tick = _exe(tmp_path / "tick", "#!/usr/bin/env bash\nexit 7\n")

    result = subprocess.run(
        [
            str(SCRIPT),
            "thread-1",
            "--tick-bin",
            str(tick),
            "--max-ticks",
            "1",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=_clean_env(),
    )

    assert result.returncode == 7
    assert "tick failed with exit 7" in result.stderr
