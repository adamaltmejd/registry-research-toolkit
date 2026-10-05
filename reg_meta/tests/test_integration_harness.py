"""Unit regressions for the native packaging harness; no runtime proof."""

from __future__ import annotations

import fcntl
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest
import test_integration as harness


def _patch_popen(monkeypatch, run):
    """Adapt run-shaped fakes to the Popen process root the harness drives.

    Patches only stdlib process roots (`subprocess.Popen`, `os.killpg`), so the
    harness's own argv construction, timeout handling and redaction all run.
    """

    class Process:
        def __init__(self, argv, **kwargs):
            self.argv = argv
            self.kwargs = kwargs
            self.returncode = None
            self.pid = 12345
            self.stdout = None
            self.stderr = None
            self.expired = None

        def communicate(self, timeout):
            if self.expired is not None:
                self.returncode = -15
                return _text(self.expired.stdout), _text(self.expired.stderr)
            try:
                result = run(self.argv, timeout=timeout, **self.kwargs)
            except subprocess.TimeoutExpired as error:
                self.expired = error
                raise
            self.returncode = result.returncode
            return result.stdout, result.stderr

    monkeypatch.setattr(subprocess, "Popen", Process)
    monkeypatch.setattr(os, "killpg", lambda *args: None)


def _text(output):
    return output.decode() if isinstance(output, bytes) else output or ""


def _runtime_args(argv):
    """The runtime subcommand argv, without the executable and storage flags."""
    return argv[8:] if "--remote=false" in argv else argv[1:]


@pytest.mark.parametrize(
    "platform,binary,probe,output",
    [
        (
            "darwin",
            "container",
            ["system", "status", "--format", "json"],
            '{"status":"running"}',
        ),
        ("linux", "podman", ["info"], "rootless: true"),
    ],
)
def test_runtime_selection(monkeypatch, platform, binary, probe, output):
    monkeypatch.setattr(sys, "platform", platform)
    monkeypatch.setattr(shutil, "which", lambda name: f"/native/{name}")
    calls = []

    def run(argv, **kwargs):
        calls.append((argv[0], argv[1:], kwargs["timeout"]))
        return subprocess.CompletedProcess(argv, 0, output, "")

    _patch_popen(monkeypatch, run)
    runtime = harness._resolve_runtime()
    assert runtime.apple == (platform == "darwin")
    assert calls == [(f"/native/{binary}", probe, 10)]


@pytest.mark.parametrize(
    "platform,binary", [("darwin", "container"), ("linux", "podman")]
)
def test_missing_runtime_fails_without_fallback(monkeypatch, platform, binary):
    monkeypatch.setattr(sys, "platform", platform)
    calls = []
    monkeypatch.setattr(shutil, "which", lambda name: calls.append(name))
    with pytest.raises(pytest.fail.Exception, match=f"{binary} not available on PATH"):
        harness._resolve_runtime()
    assert calls == [binary]


@pytest.mark.parametrize(
    "platform,code,output,remedy",
    [
        ("darwin", 0, '{"status":"stopped"}', "container system start"),
        ("darwin", 0, "unavailable plugins", "container system start"),
        ("linux", 125, "", "rootless user namespace/storage"),
    ],
)
def test_unhealthy_runtime_fails_actionably(
    monkeypatch, platform, code, output, remedy
):
    monkeypatch.setattr(sys, "platform", platform)
    monkeypatch.setattr(shutil, "which", lambda name: f"/native/{name}")
    _patch_popen(
        monkeypatch,
        lambda argv, **k: subprocess.CompletedProcess(
            argv, code, output, "probe error"
        ),
    )
    with pytest.raises(pytest.fail.Exception, match=remedy):
        harness._resolve_runtime()


@pytest.mark.parametrize("apple", [True, False])
@pytest.mark.parametrize("outcome", [0, 17, "timeout"])
def test_run_isolated_cleanup_and_credentials(monkeypatch, apple, outcome):
    monkeypatch.setenv("GITHUB_TOKEN", "test-secret-one")
    monkeypatch.setenv("GH_TOKEN", "test-secret-two")
    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        if argv[1] == "run":
            if outcome == "timeout":
                raise subprocess.TimeoutExpired(
                    argv,
                    kwargs["timeout"],
                    output=b"test-secret-one",
                    stderr=b"test-secret-two",
                )
            return subprocess.CompletedProcess(
                argv, outcome, "test-secret-one", "test-secret-two"
            )
        return subprocess.CompletedProcess(argv, 0, "", "")

    _patch_popen(monkeypatch, run)
    runtime = harness.NativeRuntime("/native/runtime", apple)
    names = []
    for _ in range(2):
        if outcome == "timeout":
            with pytest.raises(
                pytest.fail.Exception, match="timed out after 8s"
            ) as error:
                runtime.run("mode-worker-session", "reg-meta --help", timeout=8)
            assert "test-secret" not in str(error.value)
        else:
            result = runtime.run("mode-worker-session", "reg-meta --help", timeout=8)
            assert result.returncode == outcome
            assert result.stdout == result.stderr == "[REDACTED]"
        argv, cleanup = calls[-2:]
        name = argv[argv.index("--name") + 1]
        names.append(name)
        assert name.startswith("reg-meta-integration-run-")
        assert len(name) <= 63
        assert argv[4:8] == ["--env", "GITHUB_TOKEN", "--env", "GH_TOKEN"]
        assert "test-secret" not in " ".join(argv)
        assert cleanup == (
            ["/native/runtime", "delete", "--force", name]
            if apple
            else ["/native/runtime", "rm", "--force", "--ignore", name]
        )
    assert names[0] != names[1]


@pytest.mark.parametrize(
    "mode,expected",
    [
        ("registry", {"reg_meta", "pyproject.toml"}),
        ("workspace", {"reg_meta", "reg_schema"}),
    ],
)
@pytest.mark.parametrize("outcome", [0, 42, "timeout"])
@pytest.mark.parametrize("apple", [True, False])
def test_image_context_and_cleanup(
    monkeypatch, tmp_path, mode, expected, outcome, apple
):
    for name in ("reg_meta", "reg_schema"):
        (tmp_path / name / "tests").mkdir(parents=True)
        (tmp_path / name / "pyproject.toml").write_text("project")
        (tmp_path / name / "tests" / "ignored.py").write_text("excluded")
    (tmp_path / "pyproject.toml").write_text("workspace")
    # Serve the checkout paths the harness copies from this synthetic tree, at
    # the filesystem boundary, so the harness's own copy ignore rules still run.
    copytree, copy2 = shutil.copytree, shutil.copy2
    monkeypatch.setattr(
        shutil,
        "copytree",
        lambda src, dst, **k: copytree(tmp_path / Path(src).name, dst, **k),
    )
    monkeypatch.setattr(
        shutil, "copy2", lambda src, dst: copy2(tmp_path / Path(src).name, dst)
    )
    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw3")
    calls = []
    tags = []

    def run(argv, **kwargs):
        args = _runtime_args(argv)
        calls.append(args)
        if args[0] == "build":
            ctx = kwargs["cwd"]
            assert {p.name for p in ctx.iterdir()} == expected | {"Dockerfile"}
            assert not (ctx / "reg_meta" / "tests").exists()
            text = (ctx / "Dockerfile").read_text()
            assert text == harness.DOCKERFILE_PREAMBLE + harness.INSTALL_STEP[mode]
            tag = args[args.index("-t") + 1]
            tags.append(tag)
            assert tag.startswith(f"reg-meta-integration-test-{mode}-gw3-")
            if outcome == "timeout":
                raise subprocess.TimeoutExpired(argv, kwargs["timeout"])
            return subprocess.CompletedProcess(
                argv, outcome, "build output", "build error"
            )
        return subprocess.CompletedProcess(argv, 0, "", "")

    _patch_popen(monkeypatch, run)
    runtime = harness.NativeRuntime("/native/runtime", apple)
    for _ in range(2):
        fixture = harness._installed_image(runtime, mode)
        if outcome == 0:
            assert next(fixture) == tags[-1]
            assert calls[-1][0] == "build"
            with pytest.raises(StopIteration):
                next(fixture)
        else:
            with pytest.raises(
                pytest.fail.Exception if outcome == "timeout" else AssertionError
            ):
                next(fixture)
        assert calls[-1] == (
            ["image", "delete", "--force", tags[-1]]
            if apple
            else ["rmi", "--ignore", "--no-prune", tags[-1]]
        )
    assert len(set(tags)) == 2


def test_cleanup_failure_is_reported(monkeypatch):
    _patch_popen(
        monkeypatch,
        lambda argv, **k: subprocess.CompletedProcess(argv, 1, "", "permission denied"),
    )
    with pytest.raises(AssertionError, match="Cleanup failed.*owned-name"):
        harness.NativeRuntime("container", True).remove("owned-name")


def test_apple_absent_container_is_benign(monkeypatch):
    _patch_popen(
        monkeypatch,
        lambda argv, **k: subprocess.CompletedProcess(
            argv, 1, "", "notFound: container with ID owned-name not found"
        ),
    )
    harness.NativeRuntime("container", True).remove("owned-name")


def test_apple_builder_lock_has_timeout(monkeypatch, tmp_path):
    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))
    ticks = iter([0, 301])
    monkeypatch.setattr(time, "monotonic", lambda: next(ticks))

    def busy(*args):
        raise BlockingIOError()

    monkeypatch.setattr(fcntl, "flock", busy)
    with (
        pytest.raises(pytest.fail.Exception, match="builder lock busy for 300s"),
        harness._apple_build_lock(),
    ):
        pytest.fail("must not acquire busy builder lock")


@pytest.mark.parametrize("timeout", [False, True])
def test_command_and_cleanup_failure_both_reported(monkeypatch, timeout):
    monkeypatch.setenv("GITHUB_TOKEN", "test-secret")

    def run(argv, **kwargs):
        if argv[1] == "run":
            if timeout:
                raise subprocess.TimeoutExpired(
                    argv,
                    8,
                    output=b"original output",
                    stderr=b"test-secret original error",
                )
            return subprocess.CompletedProcess(
                argv, 17, "original output", "test-secret original error"
            )
        return subprocess.CompletedProcess(argv, 1, "", "cleanup denied")

    _patch_popen(monkeypatch, run)
    with pytest.raises(pytest.fail.Exception) as error:
        harness.NativeRuntime("container", True).run("image", "command", timeout=8)
    message = str(error.value)
    assert ("timed out after 8s" if timeout else "Command exited 17") in message
    assert "original output" in message
    assert "original error" in message
    assert "cleanup denied" in message
    assert "test-secret" not in message


@pytest.mark.parametrize("update_exit", [0, 10])
def test_release_update_preserves_failure_and_query_json(
    monkeypatch, tmp_path, update_exit
):
    script = tmp_path / "reg-meta"
    script.write_text(
        "#!/bin/sh\n"
        'if [ "$1" = update ]; then\n'
        '    echo \'{"update":"diagnostic"}\'\n'
        f"    exit {update_exit}\n"
        "fi\n"
        'echo \'{"results":[{"name":"kommun"}]}\'\n'
    )
    script.chmod(0o755)
    environment = {**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}"}
    results = []
    # Captured before patching: the fake runs the container's shell command as
    # a real host process standing in for the container.
    popen = subprocess.Popen

    def run(argv, **kwargs):
        if argv[1] == "run":
            with popen(
                ["sh", "-c", argv[-1]],
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            ) as process:
                stdout, stderr = process.communicate()
            result = subprocess.CompletedProcess(
                argv, process.returncode, stdout, stderr
            )
            results.append(result)
            return result
        return subprocess.CompletedProcess(argv, 0, "", "")

    _patch_popen(monkeypatch, run)
    runtime = harness.NativeRuntime("unit", False)
    if update_exit:
        with pytest.raises(AssertionError, match=r"Pipeline failed \(exit 10\)"):
            harness.test_update_and_query(runtime, "unit-image")
        assert results[0].returncode == 10
        assert results[0].stdout == ""
        assert '"update":"diagnostic"' in results[0].stderr
    else:
        harness.test_update_and_query(runtime, "unit-image")
        assert results[0].stdout == '{"results":[{"name":"kommun"}]}\n'
        assert results[0].stderr == ""


@pytest.mark.parametrize("failure", ["exit", "timeout"])
def test_linux_fixture_storage_cleanup_after_build_failure(
    monkeypatch, tmp_path, failure
):
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(shutil, "which", lambda name: name)
    calls = []
    containers = ["a" * 64]

    def run(argv, **kwargs):
        assert argv[:2] == ["podman", "--remote=false"]
        args = _runtime_args(argv)
        calls.append(args)
        if args[0] == "build":
            if failure == "timeout":
                raise subprocess.TimeoutExpired(argv, kwargs["timeout"])
            return subprocess.CompletedProcess(argv, 17, "", "build error")
        if args[0] == "ps":
            return subprocess.CompletedProcess(argv, 0, "\n".join(containers), "")
        if args[0] == "rm":
            containers.clear()
        return subprocess.CompletedProcess(argv, 0, "", "")

    _patch_popen(monkeypatch, run)
    fixture = harness.runtime._get_wrapped_function()()
    runtime = next(fixture)
    storage = runtime.storage
    assert storage is not None and storage.exists()
    try:
        with pytest.raises(
            pytest.fail.Exception if failure == "timeout" else AssertionError
        ):
            next(harness._built_image(runtime, tmp_path, "owned-failed-tag"))
    finally:
        fixture.close()
    assert calls[0] == ["info"]
    assert calls[1][0] == "build"
    assert calls[2:] == [
        ["ps", "--all", "--external", "--quiet", "--no-trunc"],
        ["rm", "--force", "--ignore", "a" * 64],
        ["rmi", "--ignore", "--no-prune", "owned-failed-tag"],
        ["ps", "--all", "--external", "--quiet", "--no-trunc"],
        ["rmi", "--all", "--force", "--ignore"],
    ]
    assert not storage.exists()


def test_invalid_external_container_ids_are_not_removed(monkeypatch, tmp_path):
    calls = []

    def run(argv, **kwargs):
        calls.append(_runtime_args(argv))
        return subprocess.CompletedProcess(argv, 0, "untrusted invalid id", "")

    _patch_popen(monkeypatch, run)
    with pytest.raises(AssertionError, match="Invalid container IDs"):
        harness.NativeRuntime("podman", False, tmp_path).remove_external_containers()
    assert len(calls) == 1 and calls[0][0] == "ps"


def test_every_private_podman_command_uses_owned_storage(monkeypatch, tmp_path):
    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, "", "")

    _patch_popen(monkeypatch, run)
    runtime = harness.NativeRuntime("podman", False, tmp_path)
    runtime.command(["info"])
    runtime.command(["build", "-t", "own-tag", "."])
    runtime.command(["run", "own-tag", "true"])
    runtime.remove("own-container")
    runtime.remove("own-tag", image=True)
    runtime.cleanup_storage()
    assert all(
        argv[:8]
        == [
            "podman",
            "--remote=false",
            "--root",
            str(tmp_path / "root"),
            "--runroot",
            str(tmp_path / "runroot"),
            "--tmpdir",
            str(tmp_path / "tmp"),
        ]
        for argv in calls
    )


def test_native_cancellation_precedes_forced_kill(monkeypatch):

    events = []

    class Process:
        pid = 12345
        returncode = None
        stdout = stderr = None

        def communicate(self, timeout):
            events.append(("wait", timeout))
            if len(events) < 5:
                raise subprocess.TimeoutExpired(
                    "build", timeout, output=b"build output"
                )
            self.returncode = -9
            return "build output", ""

    monkeypatch.setattr(subprocess, "Popen", lambda *a, **k: Process())
    monkeypatch.setattr(os, "killpg", lambda pid, sig: events.append(("signal", sig)))
    with pytest.raises(pytest.fail.Exception, match="timed out after 2s"):
        harness.NativeRuntime("podman", False).command(["build"], timeout=2)
    assert events == [
        ("wait", 2),
        ("signal", signal.SIGTERM),
        ("wait", 10),
        ("signal", signal.SIGKILL),
        ("wait", 10),
    ]
