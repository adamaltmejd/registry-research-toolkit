"""Integration test: full install-and-query pipeline in a Docker container.

Not run by default. Requires Docker and a published GitHub release.

    pytest -m integration reg_meta/tests/test_integration.py
"""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
import textwrap
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]

DOCKERFILE = textwrap.dedent("""\
    FROM python:3.14-slim

    COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

    WORKDIR /src
    COPY . .

    RUN uv venv /opt/venv
    ENV VIRTUAL_ENV=/opt/venv
    ENV PATH="/opt/venv/bin:$PATH"
    RUN uv pip install "./reg_meta"
""")

IMAGE_TAG = "reg-meta-integration-test"


@pytest.fixture(scope="module")
def docker() -> str:
    """Resolve a working Docker daemon, or FAIL the test.

    This fixture only runs when integration tests are opted into via
    ``--run-integration`` (the root conftest skips them at collection otherwise),
    so opting in IS the assertion that Docker is available: a missing binary or a
    stopped daemon is a hard failure here, not a skip. The pre-push hook
    (.pre-commit-config.yaml) passes ``--run-integration``, which is what makes a
    running Docker daemon a required pre-push gate — start Docker and push again
    rather than bypassing with ``--no-verify``.
    """
    path = shutil.which("docker")
    if not path:
        pytest.fail("Docker not available (binary not found on PATH)", pytrace=False)
    result = subprocess.run([path, "info"], capture_output=True, timeout=10)
    if result.returncode != 0:
        pytest.fail("Docker daemon not running — start Docker and retry", pytrace=False)
    return path


@pytest.fixture(scope="module")
def image(docker: str) -> str:
    """Build a Docker image with reg_meta installed from local source."""
    with tempfile.TemporaryDirectory() as ctx_str:
        ctx = Path(ctx_str)

        # Minimal build context: only reg_meta package + workspace root config
        shutil.copytree(
            REPO_ROOT / "reg_meta",
            ctx / "reg_meta",
            ignore=shutil.ignore_patterns(
                "__pycache__", "*.pyc", ".pytest_cache", "*.egg-info"
            ),
        )
        shutil.copy2(REPO_ROOT / "pyproject.toml", ctx)
        (ctx / "Dockerfile").write_text(DOCKERFILE)

        result = subprocess.run(
            [docker, "build", "-t", IMAGE_TAG, "."],
            cwd=ctx,
            capture_output=True,
            text=True,
            timeout=300,
        )
        assert result.returncode == 0, f"Docker build failed:\n{result.stderr}"

    yield IMAGE_TAG

    subprocess.run([docker, "rmi", IMAGE_TAG], capture_output=True, timeout=30)


def _docker_run(
    docker: str, image: str, cmd: str, *, timeout: int = 60
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [docker, "run", "--rm", image, "sh", "-c", cmd],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def test_install_and_cli_help(docker: str, image: str):
    """Package installs cleanly and CLI is functional."""
    result = _docker_run(docker, image, "reg-meta --help")
    assert result.returncode == 0
    assert "search" in result.stdout
    assert "update" in result.stdout


def test_version_importable(docker: str, image: str):
    """Package version is importable."""
    result = _docker_run(
        docker, image, "python -c 'import reg_meta; print(reg_meta.__version__)'"
    )
    assert result.returncode == 0
    assert result.stdout.strip()


@pytest.mark.release
def test_update_and_query(docker: str, image: str):
    """Full pipeline: update (downloads DB) from GitHub Releases and run a query.

    Carries the `release` marker on top of the module-level `integration` mark, so
    it needs BOTH --run-integration AND --run-release. The pre-push hook passes
    only the former (so this is skipped — a push isn't blocked when a release is
    merely owed); a post-release / scheduled CI job passes both and runs it as a
    hard gate, where a compatible published asset is guaranteed to exist."""
    cmd = (
        "reg-meta update --yes > /dev/null"
        " && reg-meta --format json search --query kommun --datacolumn"
    )
    result = _docker_run(docker, image, cmd, timeout=600)
    assert result.returncode == 0, (
        f"Pipeline failed (exit {result.returncode}):\n{result.stderr}"
    )

    payload = json.loads(result.stdout)
    results = payload.get("results", payload.get("data", {}).get("results", []))
    assert len(results) > 0, "Expected search results for 'kommun'"
