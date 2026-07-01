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
        "--dry-run",
    ]


def test_wake_dry_run_prints_app_server_command(tmp_path: Path) -> None:
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
    assert "cos_app_server_wake.py" in result.stdout
    assert "--thread thread-1" in result.stdout
    assert preflight_log.read_text(encoding="utf-8").splitlines() == [
        "--canonical",
        str(repo),
        "--dry-run",
    ]


def test_wake_invokes_app_server_backend_by_default(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    preflight_log = tmp_path / "preflight.log"
    preflight = _exe(
        tmp_path / "preflight",
        f"""#!/usr/bin/env bash
printf '%s\\n' "$@" >> {preflight_log}
printf -- '---\\n' >> {preflight_log}
printf '{{"wake": true}}\\n'
exit 10
""",
    )
    uv_log = tmp_path / "uv.log"
    uv = _exe(
        tmp_path / "uv",
        f"#!/usr/bin/env bash\nprintf '%s\\n' \"$@\" > {uv_log}\n",
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
            "--uv-bin",
            str(uv),
            "--app-wake-bin",
            "/tmp/fake-app-wake.py",
            "--codex-bin",
            "/tmp/fake-codex",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert '{"wake": true}' in result.stdout
    assert result.stderr == ""
    assert uv_log.read_text(encoding="utf-8").splitlines() == [
        "run",
        "--no-project",
        "python",
        "/tmp/fake-app-wake.py",
        "--repo",
        str(repo),
        "--thread",
        "thread-1",
        "--prompt",
        "Run one COS tick",
        "--codex-bin",
        "/tmp/fake-codex",
        "--timeout",
        "3600",
    ]
    assert preflight_log.read_text(encoding="utf-8").splitlines() == [
        "--canonical",
        str(repo),
        "--dry-run",
        "---",
        "--canonical",
        str(repo),
        "---",
    ]


def test_wake_invokes_codex_exec_resume(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    preflight_log = tmp_path / "preflight.log"
    preflight = _exe(
        tmp_path / "preflight",
        f"""#!/usr/bin/env bash
printf '%s\\n' "$@" >> {preflight_log}
printf -- '---\\n' >> {preflight_log}
printf '{{"wake": true}}\\n'
exit 10
""",
    )
    codex_log = tmp_path / "codex.log"
    codex = _exe(
        tmp_path / "codex",
        f"""#!/usr/bin/env bash
printf '%s\\n' "$@" > {codex_log}
printf 'codex progress\\n' >&2
printf 'final COS report\\n'
""",
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
            "--wake-backend",
            "exec",
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
    assert "final COS report" in result.stdout
    assert result.stderr == ""
    assert codex_log.read_text(encoding="utf-8").splitlines() == [
        "exec",
        "-C",
        str(repo),
        "resume",
        "thread-1",
        "Run one COS tick",
    ]
    assert preflight_log.read_text(encoding="utf-8").splitlines() == [
        "--canonical",
        str(repo),
        "--dry-run",
        "---",
        "--canonical",
        str(repo),
        "---",
    ]


def test_failed_wake_does_not_commit_preflight_state(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    preflight_log = tmp_path / "preflight.log"
    preflight = _exe(
        tmp_path / "preflight",
        f"""#!/usr/bin/env bash
printf '%s\\n' "$@" >> {preflight_log}
printf -- '---\\n' >> {preflight_log}
printf '{{"wake": true}}\\n'
exit 10
""",
    )
    codex = _exe(
        tmp_path / "codex",
        "#!/usr/bin/env bash\necho codex failed >&2\nexit 4\n",
    )

    result = subprocess.run(
        [
            str(SCRIPT),
            "--repo",
            str(repo),
            "--thread",
            "thread-1",
            "--wake-backend",
            "exec",
            "--preflight-bin",
            str(preflight),
            "--codex-bin",
            str(codex),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 4
    assert "codex failed" in result.stderr
    assert preflight_log.read_text(encoding="utf-8").splitlines() == [
        "--canonical",
        str(repo),
        "--dry-run",
        "---",
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
