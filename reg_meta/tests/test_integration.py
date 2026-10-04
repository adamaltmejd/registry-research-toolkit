"""Integration test: full install-and-query pipeline in a native container.

Not run by default. Requires Apple Container on macOS or Podman on Linux, and `--install-mode registry` (the default)
additionally requires reg_meta's declared dependencies to be resolvable from
PyPI. The `release`-marked test also needs a published GitHub release asset.

    pytest --run-integration reg_meta/tests/test_integration.py
    pytest --run-integration --install-mode workspace reg_meta/tests/test_integration.py

The two modes answer two different questions and neither substitutes for the
other:

  - `registry` is the PUBLICATION boundary. reg_schema is kept out of the build
    context and reg_meta installs with `--no-sources`, so a green run means the
    `reg-schema` floor in reg_meta's published metadata actually resolved from
    the index. It is red for as long as the sibling release is owed, which is
    exactly what it is for — a reg_meta wheel must not reach PyPI before it.
  - `workspace` is the SOURCE-COHERENCE boundary. It builds this checkout's
    reg_schema and reg_meta wheels in the same pinned container and installs
    both, so main can carry a coherent unpublished sibling bump and still get a
    hard packaging gate on every push. A green run is NOT evidence that
    reg-schema is on PyPI.
"""

from __future__ import annotations

import fcntl
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import textwrap
import time
from contextlib import contextmanager, nullcontext, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]

# Both external images below carry a readable exact tag PLUS its immutable
# multi-arch manifest digest, matching the workspace baseline
# (reg_webapp/Dockerfile). Floating `python:3.14-slim` /
# `uv:latest` made this test drift with whatever uv shipped that week, which is
# how uv's rejection of the workspace source (see INSTALL_STEP's `registry`
# note) landed as a surprise failure; bump both halves deliberately, with the
# rest of the baseline.
DOCKERFILE_PREAMBLE = textwrap.dedent("""\
    FROM python:3.14.7-slim-bookworm@sha256:9ab8d9c8514b44f90cf0029dd42fdd7e9e211e639c8b995304cc04568dee900f

    COPY --from=ghcr.io/astral-sh/uv:0.12.11@sha256:79c6f4776b851471cc73b7d21d0cc834bb94383c292e83640d27eff512864df7 /uv /usr/local/bin/uv

    WORKDIR /src
    COPY . .

    RUN uv venv /opt/venv
    ENV VIRTUAL_ENV=/opt/venv
    ENV PATH="/opt/venv/bin:$PATH"
""")

# The install step is the whole difference between the modes.
#
# registry: `--no-sources` makes uv ignore the root pyproject's
# `[tool.uv.sources]` and resolve reg_meta's dependencies from the registry
# instead. The root pyproject stays in the context precisely so the install is
# proven to hold in the presence of the workspace config — the answer to
# `reg-schema = { workspace = true }` is to ignore the source, not to copy
# reg_schema in, which would install the local tree and hide whether reg_meta's
# published metadata resolves at all. There is deliberately no local-wheel or
# alternate-index fallback here: a fallback would turn this gate green while the
# published wheel stayed uninstallable.
#
# workspace: both wheels are built from this checkout and installed together in
# one resolution, so reg_meta's `reg-schema` floor is satisfied by the local
# 3.x wheel while pydantic/zstandard still come from the index — a normal
# resolution, not `--no-deps`.
INSTALL_STEP: dict[str, str] = {
    "registry": textwrap.dedent("""\
        RUN uv pip install --no-sources "./reg_meta"
    """),
    "workspace": textwrap.dedent("""\
        RUN uv build --wheel --out-dir /wheels ./reg_schema \\
            && uv build --wheel --out-dir /wheels ./reg_meta \\
            && uv pip install /wheels/reg_schema-*.whl /wheels/reg_meta-*.whl
    """),
}

# Minimal per-mode build context, copied into the temp context dir verbatim
# (directories with copytree, files with copy2). registry pairs reg_meta with
# the workspace root pyproject for the reason above; workspace ships neither it
# nor the other members, so uv sees two standalone projects rather than a
# partial workspace. `tests`/`test_corpus` are excluded from the copy on top of
# the usual build droppings: neither reaches the wheel (both packages are
# src-layout), and letting them in busts the `COPY` layer — and so re-runs the
# whole install — on the test-only edits this gate fires for most often.
CONTEXT_PATHS: dict[str, tuple[str, ...]] = {
    "registry": ("reg_meta", "pyproject.toml"),
    "workspace": ("reg_meta", "reg_schema"),
}
CONTEXT_IGNORE = shutil.ignore_patterns(
    "__pycache__", "*.pyc", ".pytest_cache", "*.egg-info", "tests", "test_corpus"
)

IMAGE_TAG = "reg-meta-integration-test"

# PEP 610: pip/uv write `direct_url.json` into the .dist-info of a distribution
# installed from a local path or URL, and omit it for one resolved by NAME. It
# is therefore installed-tree evidence of HOW reg_schema got there — unlike the
# install command's text, which proves only what we asked for.
SCHEMA_PROVENANCE_PY = (
    "import importlib.metadata as m; "
    'print(m.distribution("reg-schema").read_text("direct_url.json") or "")'
)
VERSIONS_PY = (
    "import reg_meta, reg_schema; print(reg_meta.__version__, reg_schema.__version__)"
)


@pytest.fixture(scope="module")
def install_mode(request: pytest.FixtureRequest) -> str:
    """Which installation boundary this run exercises (root conftest option)."""
    return request.config.getoption("--install-mode")


def _redact(output: str | bytes | None) -> str:
    text = (
        output.decode(errors="replace") if isinstance(output, bytes) else output or ""
    )
    for var in ("GITHUB_TOKEN", "GH_TOKEN"):
        if value := os.environ.get(var):
            text = text.replace(value, "[REDACTED]")
    return text


@dataclass(frozen=True)
class NativeRuntime:
    executable: str
    apple: bool
    storage: Path | None = None

    def command(
        self, args: list[str], *, timeout: int = 60, cwd: Path | None = None
    ) -> subprocess.CompletedProcess[str]:
        storage_flags = (
            [
                "--remote=false",
                "--root",
                str(self.storage / "root"),
                "--runroot",
                str(self.storage / "runroot"),
                "--tmpdir",
                str(self.storage / "tmp"),
            ]
            if self.storage is not None
            else []
        )
        argv = [self.executable, *storage_flags, *args]
        try:
            process = subprocess.Popen(
                argv,
                cwd=cwd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
            )
            try:
                try:
                    stdout, stderr = process.communicate(timeout=timeout)
                except subprocess.TimeoutExpired:
                    # Give native cancellation handlers time to stop the build.
                    # Signal only this CLI's new process group, never the service.
                    for sig in (signal.SIGTERM, signal.SIGKILL):
                        with suppress(ProcessLookupError):
                            os.killpg(process.pid, sig)
                        try:
                            stdout, stderr = process.communicate(timeout=10)
                            break
                        except subprocess.TimeoutExpired as error:
                            if sig == signal.SIGKILL:
                                if process.stdout is not None:
                                    process.stdout.close()
                                if process.stderr is not None:
                                    process.stderr.close()
                                stdout, stderr = (
                                    _redact(error.stdout),
                                    _redact(error.stderr),
                                )
                    pytest.fail(
                        f"{Path(self.executable).name} {args[0]} timed out after {timeout}s; "
                        f"sent bounded native cancellation:\n{_redact(stdout)}{_redact(stderr)}",
                        pytrace=False,
                    )
            finally:
                # Avoid Popen's context-manager wait(), which has no deadline.
                if process.returncode is None:
                    with suppress(ProcessLookupError):
                        os.killpg(process.pid, signal.SIGKILL)
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        pytest.fail(
                            f"{args[0]} client PID {process.pid} did not exit after SIGKILL; "
                            "native cleanup cannot be confirmed",
                            pytrace=False,
                        )
                if process.stdout is not None:
                    process.stdout.close()
                if process.stderr is not None:
                    process.stderr.close()
        except OSError as error:
            pytest.fail(f"Cannot execute {self.executable}: {error}", pytrace=False)
        return subprocess.CompletedProcess(
            argv, process.returncode, _redact(stdout), _redact(stderr)
        )

    def remove_external_containers(self) -> None:
        if self.storage is None:
            return
        result = self.command(["ps", "--all", "--external", "--quiet", "--no-trunc"])
        assert result.returncode == 0, (
            f"Cannot inspect owned build containers:\n{result.stdout}{result.stderr}"
        )
        ids = result.stdout.split()
        assert all(re.fullmatch(r"[0-9a-f]{64}", id_) for id_ in ids), (
            f"Invalid container IDs from owned storage: {result.stdout}"
        )
        if ids:
            result = self.command(["rm", "--force", "--ignore", *ids], timeout=30)
            assert result.returncode == 0, (
                f"Cannot remove owned build containers:\n{result.stdout}{result.stderr}"
            )

    def cleanup_storage(self) -> None:
        if self.storage is None:
            return
        self.remove_external_containers()
        # --all is confined to this module's private store. It cannot remove
        # another packaging worker's images, cache, containers, or networks.
        result = self.command(["rmi", "--all", "--force", "--ignore"], timeout=30)
        assert result.returncode == 0, (
            f"Cannot remove owned images/cache:\n{result.stdout}{result.stderr}"
        )

    def remove(self, name: str, *, image: bool = False) -> None:
        if self.apple:
            args = ["image", "delete"] if image else ["delete"]
            args += ["--force", name]  # image --force only ignores missing tags
        else:
            # rmi --force would delete other containers sharing the image ID.
            args = (
                ["rmi", "--ignore", "--no-prune", name]
                if image
                else ["rm", "--force", "--ignore", name]
            )
        result = self.command(args, timeout=30)
        # Apple's container delete lacks Podman's --ignore. A failed start can
        # leave no container to delete; only that exact missing name is benign.
        absent = (
            self.apple
            and not image
            and "notFound:" in result.stderr
            and name in result.stderr
        )
        assert result.returncode == 0 or absent, (
            f"Cleanup failed for {name}:\n{result.stdout}{result.stderr}"
        )

    def run(
        self, image: str, cmd: str, *, timeout: int = 60
    ) -> subprocess.CompletedProcess[str]:
        # Keep IDs short enough for Apple Container. A fresh UUID isolates
        # each run regardless of its image mode/worker/session.
        name = f"reg-meta-integration-run-{uuid4().hex}"
        env_flags = [
            flag
            for var in ("GITHUB_TOKEN", "GH_TOKEN")
            if os.environ.get(var)
            for flag in ("--env", var)
        ]
        # Both CLIs inherit --env NAME from the host. Never put values in argv.
        result = None
        failure = None
        try:
            result = self.command(
                ["run", "--name", name, *env_flags, image, "sh", "-c", cmd],
                timeout=timeout,
            )
            return result
        except BaseException as error:
            failure = error
            raise
        finally:
            try:
                self.remove(name)
            except (AssertionError, pytest.fail.Exception) as cleanup_error:
                original = (
                    str(failure)
                    if failure is not None
                    else f"Command exited {result.returncode}:\n{result.stdout}{result.stderr}"
                    if result is not None
                    else "Command did not return"
                )
                pytest.fail(
                    _redact(f"{original}\nCleanup failed: {cleanup_error}"),
                    pytrace=False,
                )


@contextmanager
def _apple_build_lock() -> Iterator[None]:
    # Apple has one shared builder. Serialize this harness across workers and
    # invocations; never stop/restart it or remove other projects' resources.
    path = Path(tempfile.gettempdir()) / "reg-meta-integration-apple-builder.lock"
    with path.open("a") as lock:
        deadline = time.monotonic() + 300
        while True:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    pytest.fail(
                        "Apple Container builder lock busy for 300s; wait for the other packaging run and retry",
                        pytrace=False,
                    )
                time.sleep(0.1)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def _resolve_runtime(storage: Path | None = None) -> NativeRuntime:
    if sys.platform not in {"darwin", "linux"}:
        pytest.fail(
            "Package integration requires macOS (Apple Container) or Linux (Podman)",
            pytrace=False,
        )
    apple = sys.platform == "darwin"
    binary = "container" if apple else "podman"
    path = shutil.which(binary)
    if not path:
        pytest.fail(
            f"{binary} not available on PATH; install {'Apple Container' if apple else 'Podman (preferably rootless)'} and retry",
            pytrace=False,
        )
    runtime = NativeRuntime(path, apple, storage)
    probe = ["system", "status", "--format", "json"] if apple else ["info"]
    result = runtime.command(probe, timeout=10)
    healthy = result.returncode == 0
    if healthy and apple:
        try:
            healthy = json.loads(result.stdout).get("status") == "running"
        except ValueError, AttributeError:
            healthy = False
    if not healthy:
        remedy = (
            "run 'container system start'"
            if apple
            else "check 'podman info' and rootless user namespace/storage configuration"
        )
        pytest.fail(
            f"{binary} runtime unavailable; {remedy} and retry:\n{result.stdout}{result.stderr}",
            pytrace=False,
        )
    return runtime


@pytest.fixture(scope="module")
def runtime() -> Iterator[NativeRuntime]:
    """Native runtime, with wholly owned Linux storage even after build timeout."""
    with tempfile.TemporaryDirectory(prefix="reg-meta-runtime-") as storage:
        resolved = _resolve_runtime(Path(storage) if sys.platform == "linux" else None)
        try:
            yield resolved
        finally:
            resolved.cleanup_storage()


def _built_image(
    runtime: NativeRuntime, context: Path, tag: str, *, timeout: int = 300
) -> Iterator[str]:
    lock = _apple_build_lock() if runtime.apple else nullcontext()
    with lock:
        try:
            flags = (
                ["--progress", "plain"] if runtime.apple else ["--force-rm", "--layers"]
            )
            result = runtime.command(
                ["build", *flags, "-t", tag, "."], cwd=context, timeout=timeout
            )
            assert result.returncode == 0, (
                f"Container build failed (exit {result.returncode}):\n{result.stdout}{result.stderr}"
            )
        except BaseException as error:
            try:
                runtime.remove_external_containers()
                runtime.remove(tag, image=True)
            except (AssertionError, pytest.fail.Exception) as cleanup_error:
                pytest.fail(
                    _redact(f"{error}\nBuild cleanup failed: {cleanup_error}"),
                    pytrace=False,
                )
            raise
    try:
        yield tag
    finally:
        runtime.remove(tag, image=True)


def _installed_image(runtime: NativeRuntime, install_mode: str) -> Iterator[str]:
    """Build only the requested installation boundary, with isolated resources."""
    worker = os.environ.get("PYTEST_XDIST_WORKER", "serial")
    tag = f"{IMAGE_TAG}-{install_mode}-{worker}-{uuid4().hex}"
    with tempfile.TemporaryDirectory() as ctx_str:
        ctx = Path(ctx_str)
        for name in CONTEXT_PATHS[install_mode]:
            src = REPO_ROOT / name
            if src.is_dir():
                shutil.copytree(src, ctx / name, ignore=CONTEXT_IGNORE)
            else:
                shutil.copy2(src, ctx / name)
        (ctx / "Dockerfile").write_text(
            DOCKERFILE_PREAMBLE + INSTALL_STEP[install_mode]
        )
        yield from _built_image(runtime, ctx, tag)


@pytest.fixture(scope="module")
def image(runtime: NativeRuntime, install_mode: str) -> Iterator[str]:
    yield from _installed_image(runtime, install_mode)


def test_install_and_cli_help(runtime: NativeRuntime, image: str):
    """Package installs cleanly and CLI is functional."""
    result = runtime.run(image, "reg-meta --help")
    assert result.returncode == 0, (
        f"Container command failed (exit {result.returncode}):\n{result.stdout}{result.stderr}"
    )
    # reg_meta's CLI prints its custom help to stderr (cli.py `_print_help`, with
    # `add_help=False` on the parser), so assert over the combined stream rather
    # than pinning stdout — robust whichever way help is routed.
    help_text = result.stdout + result.stderr
    assert "search" in help_text
    assert "update" in help_text


def test_version_importable(runtime: NativeRuntime, image: str):
    """Both installed packages import and report their version."""
    result = runtime.run(image, f"python -c {shlex.quote(VERSIONS_PY)}")
    assert result.returncode == 0, (
        f"Container command failed (exit {result.returncode}):\n{result.stdout}{result.stderr}"
    )
    assert result.stdout.strip()
    assert len(result.stdout.split()) == 2, result.stdout


def test_installed_dependencies_are_satisfied(runtime: NativeRuntime, image: str):
    """`uv pip check`: every installed distribution's requirements are met.

    In registry mode this is the publication guarantee in its observable form —
    reg_meta's `reg-schema` floor was met by a distribution uv actually
    resolved from the index. In workspace mode it is the compatibility check
    between the two wheels this checkout just built.
    """
    result = runtime.run(image, "uv pip check")
    assert result.returncode == 0, (
        f"uv pip check failed:\n{result.stdout}{result.stderr}"
    )


def test_installed_schema_provenance(
    runtime: NativeRuntime, image: str, install_mode: str
):
    """The installed reg_schema was installed the way its mode requires.

    Workspace mode's has a `direct_url.json` naming the local wheel built in
    the container; registry mode's has none, because uv resolved `reg-schema`
    by NAME. A named resolution is all PEP 610 records — not which index served
    it: an extra index, or `--no-index --find-links`, would leave no
    `direct_url.json` either. "The registry" means PyPI here because of the
    setup — clean container, default index, no index configuration in the image
    or the install step — not because of this assertion.

    What this does catch is a direct local-wheel or URL install of reg_schema
    sneaking into registry mode, which would leave the install command's text
    unchanged while making the mode's guarantee vacuous.
    """
    result = runtime.run(image, f"python -c {shlex.quote(SCHEMA_PROVENANCE_PY)}")
    assert result.returncode == 0, (
        f"Container command failed (exit {result.returncode}):\n{result.stdout}{result.stderr}"
    )
    direct_url = result.stdout.strip()
    if install_mode == "workspace":
        url = json.loads(direct_url)["url"]
        assert url.startswith("file://") and url.endswith(".whl"), direct_url
    else:
        assert not direct_url, (
            f"reg-schema was installed directly, not resolved by name: {direct_url}"
        )


@pytest.mark.release
def test_update_and_query(runtime: NativeRuntime, image: str):
    """Full pipeline: update (downloads DB) from GitHub Releases and run a query.

    Carries the `release` marker on top of the module-level `integration` mark, so
    it needs BOTH --run-integration AND --run-release. The pre-push hook passes
    only the former (so this is skipped — a push isn't blocked when a release is
    merely owed); a post-release / scheduled CI job passes both and runs it as a
    hard gate, where a compatible published asset is guaranteed to exist. That job
    runs in `registry` mode, so the container it queries from is the published
    consumer's install, not a locally built one."""
    cmd = textwrap.dedent("""\
        update_output=$(mktemp) || exit $?
        if reg-meta update --yes > "$update_output"; then
            rm -f "$update_output"
        else
            status=$?
            cat "$update_output" >&2
            rm -f "$update_output"
            exit "$status"
        fi
        reg-meta --format json search --query kommun --field datacolumn
    """)
    result = runtime.run(image, cmd, timeout=600)
    assert result.returncode == 0, (
        f"Pipeline failed (exit {result.returncode}):\n{result.stderr}"
    )

    payload = json.loads(result.stdout)
    results = payload.get("results", payload.get("data", {}).get("results", []))
    assert len(results) > 0, "Expected search results for 'kommun'"


@pytest.mark.parametrize("failure", ["exit", "timeout"])
def test_native_build_failure_cleanup(
    runtime: NativeRuntime, image: str, tmp_path: Path, failure: str
):
    """Actual failed/cancelled RUN leaves no owned containers or image tag."""
    marker = f"NATIVE_TIMEOUT_READY_{uuid4().hex}"
    step = "exit 17" if failure == "exit" else f"echo {marker}; sleep 60"
    tag = f"reg-meta-integration-lifecycle-{uuid4().hex}"
    (tmp_path / "Dockerfile").write_text(f"FROM {image}\nRUN {step}\n")
    exception = AssertionError if failure == "exit" else pytest.fail.Exception
    match = "Container build failed" if failure == "exit" else "timed out after 8s"
    with pytest.raises(exception, match=match) as error:
        next(
            _built_image(runtime, tmp_path, tag, timeout=60 if failure == "exit" else 8)
        )
    if failure == "timeout":
        # Match emitted RUN output, not merely the displayed instruction text.
        assert re.search(rf"(?m)^(?:#\d+ [0-9.]+ )?{marker}\r?$", str(error.value)), (
            str(error.value)
        )
    inspection = runtime.command(["image", "inspect", tag])
    assert inspection.returncode != 0, "Failed build left its target image tag"
    if not runtime.apple:
        listing = runtime.command(
            ["ps", "--all", "--external", "--quiet", "--no-trunc"]
        )
        assert listing.returncode == 0, listing.stderr
        assert not listing.stdout.strip(), listing.stdout
