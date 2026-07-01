"""Tests for the chief-of-staff scheduler wrapper."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "cos_scheduler_tick.sh"


def _exe(path: Path, body: str) -> Path:
    path.write_text(body, encoding="utf-8")
    path.chmod(0o755)
    return path


def test_idle_preflight_does_not_wake(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    preflight_log = tmp_path / "preflight.log"
    preflight = _exe(
        tmp_path / "preflight",
        f"#!/usr/bin/env bash\nprintf '%s\\n' \"$@\" > {preflight_log}\nexit 0\n",
    )
    codex_log = tmp_path / "codex.log"
    codex = _exe(
        tmp_path / "codex",
        f"#!/usr/bin/env bash\nprintf '%s\\n' \"$@\" > {codex_log}\n",
    )

    result = subprocess.run(
        [
            str(SCRIPT),
            "--repo",
            str(repo),
            "--thread",
            "thread-1",
            "--preflight-bin",
            str(preflight),
            "--codex-bin",
            str(codex),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert result.stdout == ""
    assert result.stderr == ""
    assert not codex_log.exists()
    assert preflight_log.read_text(encoding="utf-8").splitlines() == [
        "--canonical",
        str(repo),
    ]


def test_wake_dry_run_prints_codex_resume_command(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    preflight_log = tmp_path / "preflight.log"
    preflight = _exe(
        tmp_path / "preflight",
        f"""#!/usr/bin/env bash
printf '%s\\n' "$@" > {preflight_log}
printf '{{"wake": true}}\\n'
exit 10
""",
    )

    result = subprocess.run(
        [
            str(SCRIPT),
            "--dry-run",
            "--repo",
            str(repo),
            "--thread",
            "thread-1",
            "--prompt",
            "Run one COS tick",
            "--preflight-bin",
            str(preflight),
            "--codex-bin",
            "/tmp/fake-codex",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 10
    assert '{"wake": true}' in result.stdout
    assert "cos-scheduler: dry-run would run:" in result.stdout
    assert "exec -C" in result.stdout
    assert "resume thread-1" in result.stdout
    assert preflight_log.read_text(encoding="utf-8").splitlines() == [
        "--canonical",
        str(repo),
        "--dry-run",
    ]


def test_wake_invokes_codex_exec_resume(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    preflight = _exe(
        tmp_path / "preflight",
        "#!/usr/bin/env bash\nprintf '{\"wake\": true}\\n'\nexit 10\n",
    )
    codex_log = tmp_path / "codex.log"
    codex = _exe(
        tmp_path / "codex",
        f"#!/usr/bin/env bash\nprintf '%s\\n' \"$@\" > {codex_log}\n",
    )

    result = subprocess.run(
        [
            str(SCRIPT),
            "--repo",
            str(repo),
            "--thread",
            "thread-1",
            "--prompt",
            "Run one COS tick",
            "--preflight-bin",
            str(preflight),
            "--codex-bin",
            str(codex),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert '{"wake": true}' in result.stdout
    assert "waking chief-of-staff thread thread-1" in result.stderr
    assert codex_log.read_text(encoding="utf-8").splitlines() == [
        "exec",
        "-C",
        str(repo),
        "resume",
        "thread-1",
        "Run one COS tick",
    ]


def test_preflight_setup_error_is_reported(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    preflight = _exe(
        tmp_path / "preflight",
        "#!/usr/bin/env bash\necho setup failed >&2\nexit 2\n",
    )

    env = os.environ.copy()
    env["COS_THREAD_ID"] = "thread-1"
    result = subprocess.run(
        [
            str(SCRIPT),
            "--repo",
            str(repo),
            "--preflight-bin",
            str(preflight),
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 2
    assert "setup failed" in result.stderr
